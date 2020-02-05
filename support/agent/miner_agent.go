package agent

import (
	"container/heap"
	"crypto/sha256"
	"fmt"
	"math/rand"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/pkg/errors"

	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v5/actors/runtime/proof"
)

type MinerAgentConfig struct {
	PrecommitRate    float64                 // average number of PreCommits per epoch
	ProofType        abi.RegisteredSealProof // seal proof type for this miner
	StartingBalance  abi.TokenAmount         // initial actor balance for miner actor
	FaultRate        float64                 // rate at which committed sectors go faulty (faults per committed sector per epoch)
	RecoveryRate     float64                 // rate at which faults are recovered (recoveries per fault per epoch)
	MinMarketBalance abi.TokenAmount         // balance below which miner will top up funds in market actor
	MaxMarketBalance abi.TokenAmount         // balance to which miner will top up funds in market actor
	UpgradeSectors   bool                    // if true, miner will replace sectors without deals with sectors that do
}

type MinerAgent struct {
	Config        MinerAgentConfig // parameters used to define miner prior to creation
	Owner         address.Address
	Worker        address.Address
	IDAddress     address.Address
	RobustAddress address.Address

	// Stats
	UpgradedSectors uint64

	// These slices are used to track counts and for random selections
	// all committed sectors (including sectors pending proof validation) that are not faulty and have not expired
	liveSectors []uint64
	// all sectors expected to be faulty
	faultySectors []uint64
	// all sectors that contain no deals (committed capacity sectors)
	ccSectors []uint64

	// deals made by this agent that need to be published
	pendingDeals []market.ClientDealProposal
	// deals made by this agent that need to be published
	dealsPendingInclusion []pendingDeal

	// priority queue used to trigger actions at future epochs
	operationSchedule *opQueue
	// which sector belongs to which deadline/partition
	deadlines [miner.WPoStPeriodDeadlines][]partition
	// iterator to time PreCommit events according to rate
	preCommitEvents *RateIterator
	// iterator to time faults events according to rate
	faultEvents *RateIterator
	// iterator to time recoveries according to rate
	recoveryEvents *RateIterator
	// tracks which sector number to use next
	nextSectorNumber abi.SectorNumber
	// tracks funds expected to be locked for miner deal collateral
	expectedMarketBalance abi.TokenAmount
	// random numnber generator provided by sim
	rnd *rand.Rand
}

func NewMinerAgent(owner address.Address, worker address.Address, idAddress address.Address, robustAddress address.Address,
	rndSeed int64, config MinerAgentConfig,
) *MinerAgent {
	rnd := rand.New(rand.NewSource(rndSeed))
	return &MinerAgent{
		Config:        config,
		Owner:         owner,
		Worker:        worker,
		IDAddress:     idAddress,
		RobustAddress: robustAddress,

		operationSchedule:     &opQueue{},
		preCommitEvents:       NewRateIterator(config.PrecommitRate, rnd.Int63()),
		expectedMarketBalance: big.Zero(),

		// fault rate is the configured fault rate times the number of live sectors or zero.
		faultEvents: NewRateIterator(0.0, rnd.Int63()),
		// recovery rate is the configured recovery rate times the number of faults or zero.
		recoveryEvents: NewRateIterator(0.0, rnd.Int63()),
		rnd:            rnd, // rng for this miner isolated from original source
	}
}

func (ma *MinerAgent) Tick(s SimState) ([]message, error) {
	var messages []message

	// act on scheduled operations
	for _, op := range ma.operationSchedule.PopOpsUntil(s.GetEpoch()) {
		switch o := op.action.(type) {
		case proveCommitAction:
			messages = append(messages, ma.createProveCommit(s.GetEpoch(), o.sectorNumber, o.committedCapacity, o.upgrade))
		case registerSectorAction:
			err := ma.registerSector(s, o.sectorNumber, o.committedCapacity, o.upgrade)
			if err != nil {
				return nil, err
			}
		case proveDeadlineAction:
			msgs, err := ma.submitPoStForDeadline(s, o.dlIdx)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msgs...)
		case recoverSectorAction:
			msgs, err := ma.delayedRecoveryMessage(o.dlIdx, o.pIdx, o.sectorNumber)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msgs...)
		case syncDeadlineStateAction:
			if err := ma.syncMinerState(s, o.dlIdx); err != nil {
				return nil, err
			}
		}
	}

	// Start PreCommits. PreCommits are triggered with a Poisson distribution at the PreCommit rate.
	// This permits multiple PreCommits per epoch while also allowing multiple epochs to pass
	// between PreCommits. For now always assume we have enough funds for the PreCommit deposit.
	if err := ma.preCommitEvents.Tick(func() error {
		// can't create precommit if in fee debt
		mSt, err := s.MinerState(ma.IDAddress)
		if err != nil {
			return err
		}
		feeDebt, err := mSt.FeeDebt(s.Store())
		if err != nil {
			return err
		}
		if feeDebt.GreaterThan(big.Zero()) {
			return nil
		}

		msg, err := ma.createPreCommit(s, s.GetEpoch())
		if err != nil {
			return err
		}
		messages = append(messages, msg)
		return nil
	}); err != nil {
		return nil, err
	}

	// Fault sectors.
	// Rate must be multiplied by the number of live sectors
	faultRate := ma.Config.FaultRate * float64(len(ma.liveSectors))
	if err := ma.faultEvents.TickWithRate(faultRate, func() error {
		msgs, err := ma.createFault(s)
		if err != nil {
			return err
		}
		messages = append(messages, msgs...)
		return nil
	}); err != nil {
		return nil, err
	}

	// Recover sectors.
	// Rate must be multiplied by the number of faulty sectors
	recoveryRate := ma.Config.RecoveryRate * float64(len(ma.faultySectors))
	if err := ma.recoveryEvents.TickWithRate(recoveryRate, func() error {
		msgs, err := ma.createRecovery(s)
		if err != nil {
			return err
		}
		messages = append(messages, msgs...)
		return nil
	}); err != nil {
		return nil, err
	}

	// publish pending deals
	messages = append(messages, ma.publishStorageDeals()...)

	// add market balance if needed
	messages = append(messages, ma.updateMarketBalance()...)

	return messages, nil
}

///////////////////////////////////
//
//  DealProvider methods
//
///////////////////////////////////

var _ DealProvider = (*MinerAgent)(nil)

func (ma *MinerAgent) Address() address.Address {
	return ma.IDAddress
}

func (ma *MinerAgent) DealRange(currentEpoch abi.ChainEpoch) (abi.ChainEpoch, abi.ChainEpoch) {
	// maximum sector start and maximum expiration
	return currentEpoch + miner.MaxProveCommitDuration[ma.Config.ProofType] + miner.MinSectorExpiration,
		currentEpoch + miner.MaxSectorExpirationExtension
}

func (ma *MinerAgent) CreateDeal(proposal market.ClientDealProposal) {
	ma.expectedMarketBalance = big.Sub(ma.expectedMarketBalance, proposal.Proposal.ProviderCollateral)
	ma.pendingDeals = append(ma.pendingDeals, proposal)
}

func (ma *MinerAgent) AvailableCollateral() abi.TokenAmount {
	return ma.expectedMarketBalance
}

///////////////////////////////////
//
//  Message Generation
//
///////////////////////////////////

// create PreCommit message and activation trigger
func (ma *MinerAgent) createPreCommit(s SimState, currentEpoch abi.ChainEpoch) (message, error) {
	// go ahead and choose when we're going to activate this sector
	sectorActivation := ma.sectorActivation(currentEpoch)
	sectorNumber := ma.nextSectorNumber
	ma.nextSectorNumber++

	expiration := ma.sectorExpiration(currentEpoch)
	dealIds, expiration := ma.fillSectorWithPendingDeals(expiration)
	ma.pendingDeals = nil

	// create sector with all deals the miner has made but not yet included
	params := miner.PreCommitSectorParams{
		DealIDs:       dealIds,
		SealProof:     ma.Config.ProofType,
		SectorNumber:  sectorNumber,
		SealedCID:     sectorSealCID(ma.rnd),
		SealRandEpoch: currentEpoch - 1,
		Expiration:    expiration,
	}

	// upgrade sector if upgrades are on, this sector has deals, and we have a cc sector
	isUpgrade := ma.Config.UpgradeSectors && len(dealIds) > 0 && len(ma.ccSectors) > 0
	if isUpgrade {
		var upgradeNumber uint64
		upgradeNumber, ma.ccSectors = PopRandom(ma.ccSectors, ma.rnd)

		// prevent sim from attempting to upgrade to sector with shorter duration
		sinfo, err := ma.sectorInfo(s, upgradeNumber)
		if err != nil {
			return message{}, err
		}
		if sinfo.Expiration() > expiration {
			params.Expiration = sinfo.Expiration()
		}

		dlInfo, pIdx, err := ma.dlInfoForSector(s, upgradeNumber)
		if err != nil {
			return message{}, err
		}

		params.ReplaceCapacity = true
		params.ReplaceSectorNumber = abi.SectorNumber(upgradeNumber)
		params.ReplaceSectorDeadline = dlInfo.Index
		params.ReplaceSectorPartition = pIdx
		ma.UpgradedSectors++
	}

	// assume PreCommit succeeds and schedule prove commit
	ma.operationSchedule.ScheduleOp(sectorActivation, proveCommitAction{
		sectorNumber:      sectorNumber,
		committedCapacity: ma.Config.UpgradeSectors && len(dealIds) == 0,
		upgrade:           isUpgrade,
	})

	return message{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.PreCommitSector,
		Params: &params,
	}, nil
}

// create prove commit message
func (ma *MinerAgent) createProveCommit(epoch abi.ChainEpoch, sectorNumber abi.SectorNumber, committedCapacity bool, upgrade bool) message {
	params := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}

	// register an op for next epoch (after batch prove) to schedule a post for the sector
	ma.operationSchedule.ScheduleOp(epoch+1, registerSectorAction{
		sectorNumber:      sectorNumber,
		committedCapacity: committedCapacity,
		upgrade:           upgrade,
	})

	return message{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: &params,
	}
}

// Fault a sector.
// This chooses a sector from live sectors and then either declares the recovery
// or adds it as a fault
func (ma *MinerAgent) createFault(v SimState) ([]message, error) {
	// opt out if no live sectors
	if len(ma.liveSectors) == 0 {
		return nil, nil
	}

	// choose a live sector to go faulty
	var faultNumber uint64
	faultNumber, ma.liveSectors = PopRandom(ma.liveSectors, ma.rnd)
	ma.faultySectors = append(ma.faultySectors, faultNumber)

	// avoid trying to upgrade a faulty sector
	ma.ccSectors = filterSlice(ma.ccSectors, map[uint64]bool{faultNumber: true})

	faultDlInfo, pIdx, err := ma.dlInfoForSector(v, faultNumber)
	if err != nil {
		return nil, err
	}

	parts := ma.deadlines[faultDlInfo.Index]
	if pIdx >= uint64(len(parts)) {
		return nil, errors.Errorf("sector %d in deadline %d has unregistered partition %d",
			faultNumber, faultDlInfo.Index, pIdx)
	}
	parts[pIdx].faults.Set(faultNumber)

	// If it's too late, skip fault rather than declaring it
	if faultDlInfo.FaultCutoffPassed() {
		parts[pIdx].toBeSkipped.Set(faultNumber)
		return nil, nil
	}

	// for now, just send a message per fault rather than trying to batch them
	faultParams := miner.DeclareFaultsParams{
		Faults: []miner.FaultDeclaration{{
			Deadline:  faultDlInfo.Index,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{faultNumber}),
		}},
	}

	return []message{{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.DeclareFaults,
		Params: &faultParams,
	}}, nil
}

// Recover a sector.
// This chooses a sector from faulty sectors and then either declare the recovery or schedule one for later
func (ma *MinerAgent) createRecovery(v SimState) ([]message, error) {
	// opt out if no faulty sectors
	if len(ma.faultySectors) == 0 {
		return nil, nil
	}

	// choose a faulty sector to recover
	var recoveryNumber uint64
	recoveryNumber, ma.faultySectors = PopRandom(ma.faultySectors, ma.rnd)

	recoveryDlInfo, pIdx, err := ma.dlInfoForSector(v, recoveryNumber)
	if err != nil {
		return nil, err
	}

	parts := ma.deadlines[recoveryDlInfo.Index]
	if pIdx >= uint64(len(parts)) {
		return nil, errors.Errorf("recovered sector %d in deadline %d has unregistered partition %d",
			recoveryNumber, recoveryDlInfo.Index, pIdx)
	}
	if set, err := parts[pIdx].faults.IsSet(recoveryNumber); err != nil {
		return nil, errors.Errorf("could not check if %d in deadline %d partition %d is faulty",
			recoveryNumber, recoveryDlInfo.Index, pIdx)
	} else if !set {
		return nil, errors.Errorf("recovery %d in deadline %d partition %d was not a fault",
			recoveryNumber, recoveryDlInfo.Index, pIdx)
	}

	// If it's too late, schedule recovery rather than declaring it
	if recoveryDlInfo.FaultCutoffPassed() {
		ma.operationSchedule.ScheduleOp(recoveryDlInfo.Close, recoverSectorAction{
			dlIdx:        recoveryDlInfo.Index,
			pIdx:         pIdx,
			sectorNumber: abi.SectorNumber(recoveryNumber),
		})
		return nil, nil
	}

	return ma.recoveryMessage(recoveryDlInfo.Index, pIdx, abi.SectorNumber(recoveryNumber))
}

// prove sectors in deadline
func (ma *MinerAgent) submitPoStForDeadline(v SimState, dlIdx uint64) ([]message, error) {
	var partitions []miner.PoStPartition
	for pIdx, part := range ma.deadlines[dlIdx] {
		if live, err := bitfield.SubtractBitField(part.sectors, part.faults); err != nil {
			return nil, err
		} else if empty, err := live.IsEmpty(); err != nil {
			return nil, err
		} else if !empty {
			partitions = append(partitions, miner.PoStPartition{
				Index:   uint64(pIdx),
				Skipped: part.toBeSkipped,
			})

			part.toBeSkipped = bitfield.New()
		}
	}

	// schedule post-deadline state synchronization and next PoSt
	if err := ma.scheduleSyncAndNextProof(v, dlIdx); err != nil {
		return nil, err
	}

	// submitPoSt only if we have something to prove
	if len(partitions) == 0 {
		return nil, nil
	}

	postProofType, err := ma.Config.ProofType.RegisteredWindowPoStProof()
	if err != nil {
		return nil, err
	}

	params := miner.SubmitWindowedPoStParams{
		Deadline:   dlIdx,
		Partitions: partitions,
		Proofs: []proof.PoStProof{{
			PoStProof:  postProofType,
			ProofBytes: []byte{},
		}},
		ChainCommitEpoch: v.GetEpoch() - 1,
		ChainCommitRand:  []byte("not really random"),
	}

	return []message{{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: &params,
	}}, nil
}

// create a deal proposal message and notify provider of deal
func (ma *MinerAgent) publishStorageDeals() []message {
	if len(ma.pendingDeals) == 0 {
		return []message{}
	}

	params := market.PublishStorageDealsParams{
		Deals: ma.pendingDeals,
	}
	ma.pendingDeals = nil

	return []message{{
		From:   ma.Worker,
		To:     builtin.StorageMarketActorAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMarket.PublishStorageDeals,
		Params: &params,
		ReturnHandler: func(_ SimState, _ message, ret cbor.Marshaler) error {
			// add returned deal ids to be included within sectors
			publishReturn, ok := ret.(*market.PublishStorageDealsReturn)
			if !ok {
				return errors.Errorf("create miner return has wrong type: %v", ret)
			}

			for idx, dealId := range publishReturn.IDs {
				ma.dealsPendingInclusion = append(ma.dealsPendingInclusion, pendingDeal{
					id:   dealId,
					size: params.Deals[idx].Proposal.PieceSize,
					ends: params.Deals[idx].Proposal.EndEpoch,
				})
			}
			return nil
		},
	}}
}

func (ma *MinerAgent) updateMarketBalance() []message {
	if ma.expectedMarketBalance.GreaterThanEqual(ma.Config.MinMarketBalance) {
		return []message{}
	}

	balanceToAdd := big.Sub(ma.Config.MaxMarketBalance, ma.expectedMarketBalance)

	return []message{{
		From:   ma.Worker,
		To:     builtin.StorageMarketActorAddr,
		Value:  balanceToAdd,
		Method: builtin.MethodsMarket.AddBalance,
		Params: &ma.IDAddress,

		// update in return handler to prevent deals before the miner has balance
		ReturnHandler: func(_ SimState, _ message, _ cbor.Marshaler) error {
			ma.expectedMarketBalance = ma.Config.MaxMarketBalance
			return nil
		},
	}}
}

////////////////////////////////////////////////
//
//  Misc methods
//
////////////////////////////////////////////////

// looks up sector deadline and partition so we can start adding it to PoSts
func (ma *MinerAgent) registerSector(v SimState, sectorNumber abi.SectorNumber, committedCapacity bool, upgrade bool) error {
	mSt, err := v.MinerState(ma.IDAddress)
	if err != nil {
		return err
	}

	// first check for sector
	if found, err := mSt.HasSectorNo(v.Store(), sectorNumber); err != nil {
		return err
	} else if !found {
		fmt.Printf("failed to register sector %d, did proof verification fail?\n", sectorNumber)
		return nil
	}

	dlIdx, pIdx, err := mSt.FindSector(v.Store(), sectorNumber)
	if err != nil {
		return err
	}

	if len(ma.deadlines[dlIdx]) == 0 {
		err := ma.scheduleSyncAndNextProof(v, dlIdx)
		if err != nil {
			return err
		}
	}

	if upgrade {
		ma.UpgradedSectors++
	}

	ma.liveSectors = append(ma.liveSectors, uint64(sectorNumber))
	if committedCapacity {
		ma.ccSectors = append(ma.ccSectors, uint64(sectorNumber))
	}

	// pIdx should be sequential, but add empty partitions just in case
	for pIdx >= uint64(len(ma.deadlines[dlIdx])) {
		ma.deadlines[dlIdx] = append(ma.deadlines[dlIdx], partition{
			sectors:     bitfield.New(),
			toBeSkipped: bitfield.New(),
			faults:      bitfield.New(),
		})
	}
	ma.deadlines[dlIdx][pIdx].sectors.Set(uint64(sectorNumber))
	return nil
}

// schedule a proof within the deadline's bounds
func (ma *MinerAgent) scheduleSyncAndNextProof(v SimState, dlIdx uint64) error {
	mSt, err := v.MinerState(ma.IDAddress)
	if err != nil {
		return err
	}

	// find next proving window for this deadline
	provingPeriodStart, err := mSt.ProvingPeriodStart(v.Store())
	if err != nil {
		return err
	}
	deadlineStart := provingPeriodStart + abi.ChainEpoch(dlIdx)*miner.WPoStChallengeWindow
	if deadlineStart-miner.WPoStChallengeWindow < v.GetEpoch() {
		deadlineStart += miner.WPoStProvingPeriod
	}
	deadlineClose := deadlineStart + miner.WPoStChallengeWindow

	ma.operationSchedule.ScheduleOp(deadlineClose, syncDeadlineStateAction{dlIdx: dlIdx})

	proveAt := deadlineStart + abi.ChainEpoch(ma.rnd.Int63n(int64(deadlineClose-deadlineStart)))
	ma.operationSchedule.ScheduleOp(proveAt, proveDeadlineAction{dlIdx: dlIdx})

	return nil
}

// Fill sector with deals
// This is a naive packing algorithm that adds pieces in order received.
func (ma *MinerAgent) fillSectorWithPendingDeals(expiration abi.ChainEpoch) ([]abi.DealID, abi.ChainEpoch) {
	var dealIDs []abi.DealID

	sectorSize, err := ma.Config.ProofType.SectorSize()
	if err != nil {
		panic(err)
	}

	// pieces are aligned so that each starts at the first multiple of its piece size >= the next empty slot.
	// just stop when we find one that doesn't fit in the sector. Assume pieces can't have zero size
	loc := uint64(0)
	for _, piece := range ma.dealsPendingInclusion {
		size := uint64(piece.size)
		loc = ((loc + size - 1) / size) * size // round loc up to the next multiple of size
		if loc+size > uint64(sectorSize) {
			break
		}

		dealIDs = append(dealIDs, piece.id)
		if piece.ends > expiration {
			expiration = piece.ends
		}

		loc += size
	}

	// remove ids we've added from pending
	ma.dealsPendingInclusion = ma.dealsPendingInclusion[len(dealIDs):]

	return dealIDs, expiration
}

// ensure recovery hasn't expired since it was scheduled
func (ma *MinerAgent) delayedRecoveryMessage(dlIdx uint64, pIdx uint64, recoveryNumber abi.SectorNumber) ([]message, error) {
	part := ma.deadlines[dlIdx][pIdx]
	if expired, err := part.expired.IsSet(uint64(recoveryNumber)); err != nil {
		return nil, err
	} else if expired {
		// just ignore this recovery if expired
		return nil, nil
	}

	return ma.recoveryMessage(dlIdx, pIdx, recoveryNumber)
}

func (ma *MinerAgent) recoveryMessage(dlIdx uint64, pIdx uint64, recoveryNumber abi.SectorNumber) ([]message, error) {
	// assume this message succeeds
	ma.liveSectors = append(ma.liveSectors, uint64(recoveryNumber))
	part := ma.deadlines[dlIdx][pIdx]
	part.faults.Unset(uint64(recoveryNumber))

	recoverParams := miner.DeclareFaultsRecoveredParams{
		Recoveries: []miner.RecoveryDeclaration{{
			Deadline:  dlIdx,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(recoveryNumber)}),
		}},
	}

	return []message{{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.DeclareFaultsRecovered,
		Params: &recoverParams,
	}}, nil
}

// This function updates all sectors in deadline that have newly expired
func (ma *MinerAgent) syncMinerState(s SimState, dlIdx uint64) error {
	mSt, err := s.MinerState(ma.IDAddress)
	if err != nil {
		return err
	}

	dl, err := mSt.LoadDeadlineState(s.Store(), dlIdx)
	if err != nil {
		return err
	}

	// update sector state for all partitions in deadline
	var allNewExpired []bitfield.BitField
	for pIdx, part := range ma.deadlines[dlIdx] {
		partState, err := dl.LoadPartition(s.Store(), uint64(pIdx))
		if err != nil {
			return err
		}
		newExpired, err := bitfield.IntersectBitField(part.sectors, partState.Terminated())
		if err != nil {
			return err
		}

		if empty, err := newExpired.IsEmpty(); err != nil {
			return err
		} else if !empty {
			err := part.expireSectors(newExpired)
			if err != nil {
				return err
			}
			allNewExpired = append(allNewExpired, newExpired)
		}
	}

	// remove newly expired sectors from miner agent state to prevent choosing them in the future.
	toRemoveBF, err := bitfield.MultiMerge(allNewExpired...)
	if err != nil {
		return err
	}

	toRemove, err := toRemoveBF.AllMap(uint64(ma.nextSectorNumber))
	if err != nil {
		return err
	}

	if len(toRemove) > 0 {
		ma.liveSectors = filterSlice(ma.liveSectors, toRemove)
		ma.faultySectors = filterSlice(ma.faultySectors, toRemove)
		ma.ccSectors = filterSlice(ma.ccSectors, toRemove)
	}
	return nil
}

func filterSlice(ns []uint64, toRemove map[uint64]bool) []uint64 {
	var nextLive []uint64
	for _, sn := range ns {
		_, expired := toRemove[sn]
		if !expired {
			nextLive = append(nextLive, sn)
		}
	}
	return nextLive
}

func (ma *MinerAgent) sectorInfo(v SimState, sectorNumber uint64) (SimSectorInfo, error) {
	mSt, err := v.MinerState(ma.IDAddress)
	if err != nil {
		return nil, err
	}

	sector, err := mSt.LoadSectorInfo(v.Store(), sectorNumber)
	if err != nil {
		return nil, err
	}
	return sector, nil
}

func (ma *MinerAgent) dlInfoForSector(v SimState, sectorNumber uint64) (*dline.Info, uint64, error) {
	mSt, err := v.MinerState(ma.IDAddress)
	if err != nil {
		return nil, 0, err
	}

	dlIdx, pIdx, err := mSt.FindSector(v.Store(), abi.SectorNumber(sectorNumber))
	if err != nil {
		return nil, 0, err
	}

	dlInfo, err := mSt.DeadlineInfo(v.Store(), v.GetEpoch())
	if err != nil {
		return nil, 0, err
	}
	sectorDLInfo := miner.NewDeadlineInfo(dlInfo.PeriodStart, dlIdx, v.GetEpoch()).NextNotElapsed()
	return sectorDLInfo, pIdx, nil
}

// create a random valid sector expiration
func (ma *MinerAgent) sectorExpiration(currentEpoch abi.ChainEpoch) abi.ChainEpoch {
	// Require sector lifetime meets minimum by assuming activation happens at last epoch permitted for seal proof
	// to meet the constraints imposed in PreCommit.
	minExp := currentEpoch + miner.MaxProveCommitDuration[ma.Config.ProofType] + miner.MinSectorExpiration
	// Require duration of sector from now does not exceed the maximum sector extension. This constraint
	// is also imposed by PreCommit, and along with the first constraint define the bounds for a valid
	// expiration of a new sector.
	maxExp := currentEpoch + miner.MaxSectorExpirationExtension

	// generate a uniformly distributed expiration in the valid range.
	return minExp + abi.ChainEpoch(ma.rnd.Int63n(int64(maxExp-minExp)))
}

// Generate a sector activation over the range of acceptable values.
// The range varies widely from 150 - 3030 epochs after precommit.
// Assume differences in hardware and contention in the miner's sealing queue create a uniform distribution
// over the acceptable range
func (ma *MinerAgent) sectorActivation(preCommitAt abi.ChainEpoch) abi.ChainEpoch {
	minActivation := preCommitAt + miner.PreCommitChallengeDelay + 1
	maxActivation := preCommitAt + miner.MaxProveCommitDuration[ma.Config.ProofType]
	return minActivation + abi.ChainEpoch(ma.rnd.Int63n(int64(maxActivation-minActivation)))
}

// create a random seal CID
func sectorSealCID(rnd *rand.Rand) cid.Cid {
	data := make([]byte, 10)
	_, err := rnd.Read(data)
	if err != nil {
		panic(err)
	}

	sum := sha256.Sum256(data)
	hash, err := mh.Encode(sum[:], miner.SealedCIDPrefix.MhType)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(miner.SealedCIDPrefix.Codec, hash)
}

/////////////////////////////////////////////
//
//  Internal data structures
//
/////////////////////////////////////////////

// tracks state relevant to each partition
type partition struct {
	sectors     bitfield.BitField // sector numbers of all sectors that have not expired
	toBeSkipped bitfield.BitField // sector numbers of sectors to be skipped next PoSt
	faults      bitfield.BitField // sector numbers of sectors believed to be faulty
	expired     bitfield.BitField // sector number of sectors believed to have expired
}

func (part *partition) expireSectors(newExpired bitfield.BitField) error {
	var err error
	part.sectors, err = bitfield.SubtractBitField(part.sectors, newExpired)
	if err != nil {
		return err
	}
	part.faults, err = bitfield.SubtractBitField(part.faults, newExpired)
	if err != nil {
		return err
	}
	part.toBeSkipped, err = bitfield.SubtractBitField(part.toBeSkipped, newExpired)
	if err != nil {
		return err
	}
	part.expired, err = bitfield.MergeBitFields(part.expired, newExpired)
	if err != nil {
		return err
	}
	return nil
}

type minerOp struct {
	epoch  abi.ChainEpoch
	action interface{}
}

type proveCommitAction struct {
	sectorNumber      abi.SectorNumber
	committedCapacity bool
	upgrade           bool
}

type registerSectorAction struct {
	sectorNumber      abi.SectorNumber
	committedCapacity bool
	upgrade           bool
}

type recoverSectorAction struct {
	dlIdx        uint64
	pIdx         uint64
	sectorNumber abi.SectorNumber
}

type proveDeadlineAction struct {
	dlIdx uint64
}

type syncDeadlineStateAction struct {
	dlIdx uint64
}

type pendingDeal struct {
	id   abi.DealID
	size abi.PaddedPieceSize
	ends abi.ChainEpoch
}

/////////////////////////////////////////////
//
//  opQueue priority queue for scheduling
//
/////////////////////////////////////////////

type opQueue struct {
	ops []minerOp
}

var _ heap.Interface = (*opQueue)(nil)

// add an op to schedule
func (o *opQueue) ScheduleOp(epoch abi.ChainEpoch, action interface{}) {
	heap.Push(o, minerOp{
		epoch:  epoch,
		action: action,
	})
}

// get operations for up to and including current epoch
func (o *opQueue) PopOpsUntil(epoch abi.ChainEpoch) []minerOp {
	var ops []minerOp

	for !o.IsEmpty() && o.NextEpoch() <= epoch {
		next := heap.Pop(o).(minerOp)
		ops = append(ops, next)
	}
	return ops
}

func (o *opQueue) NextEpoch() abi.ChainEpoch {
	return o.ops[0].epoch
}

func (o *opQueue) IsEmpty() bool {
	return len(o.ops) == 0
}

func (o *opQueue) Len() int {
	return len(o.ops)
}

func (o *opQueue) Less(i, j int) bool {
	return o.ops[i].epoch < o.ops[j].epoch
}

func (o *opQueue) Swap(i, j int) {
	o.ops[i], o.ops[j] = o.ops[j], o.ops[i]
}

func (o *opQueue) Push(x interface{}) {
	o.ops = append(o.ops, x.(minerOp))
}

func (o *opQueue) Pop() interface{} {
	op := o.ops[len(o.ops)-1]
	o.ops = o.ops[:len(o.ops)-1]
	return op
}
