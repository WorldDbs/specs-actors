package power

import (
	"fmt"
	"reflect"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"github.com/filecoin-project/specs-actors/v3/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v3/actors/util/smoothing"
)

// genesis power in bytes = 750,000 GiB
var InitialQAPowerEstimatePosition = big.Mul(big.NewInt(750_000), big.NewInt(1<<30))

// max chain throughput in bytes per epoch = 120 ProveCommits / epoch = 3,840 GiB
var InitialQAPowerEstimateVelocity = big.Mul(big.NewInt(3_840), big.NewInt(1<<30))

// Bitwidth of CronEventQueue HAMT determined empirically from mutation
// patterns and projections of mainnet data.
const CronQueueHamtBitwidth = 6

// Bitwidth of CronEventQueue AMT determined empirically from mutation
// patterns and projections of mainnet data.
const CronQueueAmtBitwidth = 6

// Bitwidth of ProofValidationBatch AMT determined empirically from mutation
// pattersn and projections of mainnet data.
const ProofValidationBatchAmtBitwidth = 4

type State struct {
	TotalRawBytePower abi.StoragePower
	// TotalBytesCommitted includes claims from miners below min power threshold
	TotalBytesCommitted  abi.StoragePower
	TotalQualityAdjPower abi.StoragePower
	// TotalQABytesCommitted includes claims from miners below min power threshold
	TotalQABytesCommitted abi.StoragePower
	TotalPledgeCollateral abi.TokenAmount

	// These fields are set once per epoch in the previous cron tick and used
	// for consistent values across a single epoch's state transition.
	ThisEpochRawBytePower     abi.StoragePower
	ThisEpochQualityAdjPower  abi.StoragePower
	ThisEpochPledgeCollateral abi.TokenAmount
	ThisEpochQAPowerSmoothed  smoothing.FilterEstimate

	MinerCount int64
	// Number of miners having proven the minimum consensus power.
	MinerAboveMinPowerCount int64

	// A queue of events to be triggered by cron, indexed by epoch.
	CronEventQueue cid.Cid // Multimap, (HAMT[ChainEpoch]AMT[CronEvent])

	// First epoch in which a cron task may be stored.
	// Cron will iterate every epoch between this and the current epoch inclusively to find tasks to execute.
	FirstCronEpoch abi.ChainEpoch

	// Claimed power for each miner.
	Claims cid.Cid // Map, HAMT[address]Claim

	ProofValidationBatch *cid.Cid // Multimap, (HAMT[Address]AMT[SealVerifyInfo])
}

type Claim struct {
	// Miner's proof type used to determine minimum miner size
	WindowPoStProofType abi.RegisteredPoStProof

	// Sum of raw byte power for a miner's sectors.
	RawBytePower abi.StoragePower

	// Sum of quality adjusted power for a miner's sectors.
	QualityAdjPower abi.StoragePower
}

type CronEvent struct {
	MinerAddr       addr.Address
	CallbackPayload []byte
}

func ConstructState(store adt.Store) (*State, error) {
	emptyClaimsMapCid, err := adt.StoreEmptyMap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty map: %w", err)
	}
	emptyCronQueueMMapCid, err := adt.StoreEmptyMultimap(store, CronQueueHamtBitwidth, CronQueueAmtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty multimap: %w", err)
	}

	return &State{
		TotalRawBytePower:         abi.NewStoragePower(0),
		TotalBytesCommitted:       abi.NewStoragePower(0),
		TotalQualityAdjPower:      abi.NewStoragePower(0),
		TotalQABytesCommitted:     abi.NewStoragePower(0),
		TotalPledgeCollateral:     abi.NewTokenAmount(0),
		ThisEpochRawBytePower:     abi.NewStoragePower(0),
		ThisEpochQualityAdjPower:  abi.NewStoragePower(0),
		ThisEpochPledgeCollateral: abi.NewTokenAmount(0),
		ThisEpochQAPowerSmoothed:  smoothing.NewEstimate(InitialQAPowerEstimatePosition, InitialQAPowerEstimateVelocity),
		FirstCronEpoch:            0,
		CronEventQueue:            emptyCronQueueMMapCid,
		Claims:                    emptyClaimsMapCid,
		MinerCount:                0,
		MinerAboveMinPowerCount:   0,
	}, nil
}

// MinerNominalPowerMeetsConsensusMinimum is used to validate Election PoSt
// winners outside the chain state. If the miner has over a threshold of power
// the miner meets the minimum.  If the network is a below a threshold of
// miners and has power > zero the miner meets the minimum.
func (st *State) MinerNominalPowerMeetsConsensusMinimum(s adt.Store, miner addr.Address) (bool, error) { //nolint:deadcode,unused
	claims, err := adt.AsMap(s, st.Claims, builtin.DefaultHamtBitwidth)
	if err != nil {
		return false, xerrors.Errorf("failed to load claims: %w", err)
	}

	claim, ok, err := getClaim(claims, miner)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, errors.Errorf("no claim for actor %v", miner)
	}

	minerNominalPower := claim.RawBytePower
	minerMinPower, err := builtin.ConsensusMinerMinPower(claim.WindowPoStProofType)
	if err != nil {
		return false, errors.Wrap(err, "could not get miner min power from proof type")
	}

	// if miner is larger than min power requirement, we're set
	if minerNominalPower.GreaterThanEqual(minerMinPower) {
		return true, nil
	}

	// otherwise, if ConsensusMinerMinMiners miners meet min power requirement, return false
	if st.MinerAboveMinPowerCount >= ConsensusMinerMinMiners {
		return false, nil
	}

	// If fewer than ConsensusMinerMinMiners over threshold miner can win a block with non-zero power
	return minerNominalPower.GreaterThan(abi.NewStoragePower(0)), nil
}

// Parameters may be negative to subtract.
func (st *State) AddToClaim(s adt.Store, miner addr.Address, power abi.StoragePower, qapower abi.StoragePower) error {
	claims, err := adt.AsMap(s, st.Claims, builtin.DefaultHamtBitwidth)
	if err != nil {
		return xerrors.Errorf("failed to load claims: %w", err)
	}

	if err := st.addToClaim(claims, miner, power, qapower); err != nil {
		return xerrors.Errorf("failed to add claim: %w", err)
	}

	st.Claims, err = claims.Root()
	if err != nil {
		return xerrors.Errorf("failed to flush claims: %w", err)
	}

	return nil
}

func (st *State) GetClaim(s adt.Store, a addr.Address) (*Claim, bool, error) {
	claims, err := adt.AsMap(s, st.Claims, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, false, xerrors.Errorf("failed to load claims: %w", err)
	}
	return getClaim(claims, a)
}

func (st *State) addToClaim(claims *adt.Map, miner addr.Address, power abi.StoragePower, qapower abi.StoragePower) error {
	oldClaim, ok, err := getClaim(claims, miner)
	if err != nil {
		return fmt.Errorf("failed to get claim: %w", err)
	}
	if !ok {
		return exitcode.ErrNotFound.Wrapf("no claim for actor %v", miner)
	}

	// TotalBytes always update directly
	st.TotalQABytesCommitted = big.Add(st.TotalQABytesCommitted, qapower)
	st.TotalBytesCommitted = big.Add(st.TotalBytesCommitted, power)

	newClaim := Claim{
		WindowPoStProofType: oldClaim.WindowPoStProofType,
		RawBytePower:        big.Add(oldClaim.RawBytePower, power),
		QualityAdjPower:     big.Add(oldClaim.QualityAdjPower, qapower),
	}

	minPower, err := builtin.ConsensusMinerMinPower(oldClaim.WindowPoStProofType)
	if err != nil {
		return fmt.Errorf("could not get consensus miner min power: %w", err)
	}

	prevBelow := oldClaim.RawBytePower.LessThan(minPower)
	stillBelow := newClaim.RawBytePower.LessThan(minPower)

	if prevBelow && !stillBelow {
		// just passed min miner size
		st.MinerAboveMinPowerCount++
		st.TotalQualityAdjPower = big.Add(st.TotalQualityAdjPower, newClaim.QualityAdjPower)
		st.TotalRawBytePower = big.Add(st.TotalRawBytePower, newClaim.RawBytePower)
	} else if !prevBelow && stillBelow {
		// just went below min miner size
		st.MinerAboveMinPowerCount--
		st.TotalQualityAdjPower = big.Sub(st.TotalQualityAdjPower, oldClaim.QualityAdjPower)
		st.TotalRawBytePower = big.Sub(st.TotalRawBytePower, oldClaim.RawBytePower)
	} else if !prevBelow && !stillBelow {
		// Was above the threshold, still above
		st.TotalQualityAdjPower = big.Add(st.TotalQualityAdjPower, qapower)
		st.TotalRawBytePower = big.Add(st.TotalRawBytePower, power)
	}

	if newClaim.RawBytePower.LessThan(big.Zero()) {
		return xerrors.Errorf("negative claimed raw byte power: %v", newClaim.RawBytePower)
	}
	if newClaim.QualityAdjPower.LessThan(big.Zero()) {
		return xerrors.Errorf("negative claimed quality adjusted power: %v", newClaim.QualityAdjPower)
	}
	if st.MinerAboveMinPowerCount < 0 {
		return xerrors.Errorf("negative number of miners larger than min: %v", st.MinerAboveMinPowerCount)
	}
	return setClaim(claims, miner, &newClaim)
}

func (st *State) updateStatsForNewMiner(windowPoStProof abi.RegisteredPoStProof) error {
	minPower, err := builtin.ConsensusMinerMinPower(windowPoStProof)
	if err != nil {
		return fmt.Errorf("could not get consensus miner min power: %w", err)
	}

	if minPower.LessThanEqual(big.Zero()) {
		st.MinerAboveMinPowerCount++
	}
	return nil
}

func (st *State) deleteClaim(claims *adt.Map, miner addr.Address) (bool, error) {
	// Note: this flow loads the claim multiple times, unnecessarily.
	// We should refactor to use claims.Pop().
	oldClaim, ok, err := getClaim(claims, miner)
	if err != nil {
		return false, fmt.Errorf("failed to get claim: %w", err)
	}
	if !ok {
		return false, nil // no record, we're done
	}

	// subtract from stats as if we were simply removing power
	err = st.addToClaim(claims, miner, oldClaim.RawBytePower.Neg(), oldClaim.QualityAdjPower.Neg())
	if err != nil {
		return false, fmt.Errorf("failed to subtract miner power before deleting claim: %w", err)
	}

	// delete claim from state to invalidate miner
	return true, claims.Delete(abi.AddrKey(miner))
}

func getClaim(claims *adt.Map, a addr.Address) (*Claim, bool, error) {
	var out Claim
	found, err := claims.Get(abi.AddrKey(a), &out)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to get claim for address %v", a)
	}
	if !found {
		return nil, false, nil
	}
	return &out, true, nil
}

func (st *State) addPledgeTotal(amount abi.TokenAmount) {
	st.TotalPledgeCollateral = big.Add(st.TotalPledgeCollateral, amount)
}

func (st *State) appendCronEvent(events *adt.Multimap, epoch abi.ChainEpoch, event *CronEvent) error {
	// if event is in past, alter FirstCronEpoch so it will be found.
	if epoch < st.FirstCronEpoch {
		st.FirstCronEpoch = epoch
	}

	if err := events.Add(epochKey(epoch), event); err != nil {
		return xerrors.Errorf("failed to store cron event at epoch %v for miner %v: %w", epoch, event, err)
	}

	return nil
}

func (st *State) updateSmoothedEstimate(delta abi.ChainEpoch) {
	filterQAPower := smoothing.LoadFilter(st.ThisEpochQAPowerSmoothed, smoothing.DefaultAlpha, smoothing.DefaultBeta)
	st.ThisEpochQAPowerSmoothed = filterQAPower.NextEstimate(st.ThisEpochQualityAdjPower, delta)
}

func loadCronEvents(mmap *adt.Multimap, epoch abi.ChainEpoch) ([]CronEvent, error) {
	var events []CronEvent
	var ev CronEvent
	err := mmap.ForEach(epochKey(epoch), &ev, func(i int64) error {
		events = append(events, ev)
		return nil
	})
	return events, err
}

func setClaim(claims *adt.Map, a addr.Address, claim *Claim) error {
	if claim.RawBytePower.LessThan(big.Zero()) {
		return xerrors.Errorf("negative claim raw power %v", claim.RawBytePower)
	}
	if claim.QualityAdjPower.LessThan(big.Zero()) {
		return xerrors.Errorf("negative claim quality-adjusted power %v", claim.QualityAdjPower)
	}
	if err := claims.Put(abi.AddrKey(a), claim); err != nil {
		return xerrors.Errorf("failed to put claim with address %s power %v: %w", a, claim, err)
	}
	return nil
}

// CurrentTotalPower returns current power values accounting for minimum miner
// and minimum power
func CurrentTotalPower(st *State) (abi.StoragePower, abi.StoragePower) {
	if st.MinerAboveMinPowerCount < ConsensusMinerMinMiners {
		return st.TotalBytesCommitted, st.TotalQABytesCommitted
	}
	return st.TotalRawBytePower, st.TotalQualityAdjPower
}

func epochKey(e abi.ChainEpoch) abi.Keyer {
	return abi.IntKey(int64(e))
}

func init() {
	// Check that ChainEpoch is indeed a signed integer to confirm that epochKey is making the right interpretation.
	var e abi.ChainEpoch
	if reflect.TypeOf(e).Kind() != reflect.Int64 {
		panic("incorrect chain epoch encoding")
	}

}
