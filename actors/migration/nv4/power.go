package nv4

import (
	"context"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	power0 "github.com/filecoin-project/specs-actors/actors/builtin/power"
	states0 "github.com/filecoin-project/specs-actors/actors/states"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	smoothing2 "github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
)

type powerMigrator struct {
	actorsIn     *states0.Tree
	powerUpdates *PowerUpdates
}

func (m powerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (*StateMigrationResult, error) {
	var inState power0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	cronEventsRoot, err := m.updateCronEvents(ctx, store, inState.CronEventQueue, m.powerUpdates)
	if err != nil {
		return nil, xerrors.Errorf("could not update cron events: %w", err)
	}

	cronEventsRoot, err = m.migrateCronEvents(ctx, store, cronEventsRoot)
	if err != nil {
		return nil, xerrors.Errorf("cron events: %w", err)
	}

	claimsRoot, err := m.updateClaims(ctx, store, inState.Claims, m.powerUpdates)
	if err != nil {
		return nil, xerrors.Errorf("claims: %w", err)
	}

	claimsRoot, err = m.migrateClaims(ctx, store, claimsRoot)
	if err != nil {
		return nil, xerrors.Errorf("claims: %w", err)
	}

	outState := power2.State{
		TotalRawBytePower:         inState.TotalRawBytePower,
		TotalBytesCommitted:       inState.TotalBytesCommitted,
		TotalQualityAdjPower:      inState.TotalQualityAdjPower,
		TotalQABytesCommitted:     inState.TotalQABytesCommitted,
		TotalPledgeCollateral:     inState.TotalPledgeCollateral,
		ThisEpochRawBytePower:     inState.ThisEpochRawBytePower,
		ThisEpochQualityAdjPower:  inState.ThisEpochQualityAdjPower,
		ThisEpochPledgeCollateral: inState.ThisEpochPledgeCollateral,
		ThisEpochQAPowerSmoothed:  smoothing2.FilterEstimate(*inState.ThisEpochQAPowerSmoothed),
		MinerCount:                inState.MinerCount,
		MinerAboveMinPowerCount:   inState.MinerAboveMinPowerCount,
		CronEventQueue:            cronEventsRoot,
		FirstCronEpoch:            inState.FirstCronEpoch,
		Claims:                    claimsRoot,
		ProofValidationBatch:      nil, // Set nil at the end of every epoch in cron handler
	}

	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (m *powerMigrator) updateCronEvents(ctx context.Context, store cbor.IpldStore, cronRoot cid.Cid, powerUpdates *PowerUpdates) (cid.Cid, error) {
	crons, err := adt0.AsMultimap(adt0.WrapStore(ctx, store), cronRoot)
	if err != nil {
		return cid.Undef, err
	}

	for epoch, cronEvents := range powerUpdates.crons { // nolint:nomaprange
		for _, event := range cronEvents {
			if err := crons.Add(abi.IntKey(int64(epoch)), &event); err != nil {
				return cid.Undef, err
			}
		}
	}

	return crons.Root()
}

func (m *powerMigrator) migrateCronEvents(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value (an AMT[CronEvent] root) is identical.
	// The AMT queues may contain miner0.CronEventWorkerKeyChange, but these will be ignored by the miner
	// actor so are safe to leave behind.
	var _ = power0.CronEvent(power2.CronEvent{})

	return migrateHAMTRaw(ctx, store, root)
}

func (m *powerMigrator) updateClaims(ctx context.Context, store cbor.IpldStore, root cid.Cid, updates *PowerUpdates) (cid.Cid, error) {
	claims, err := adt0.AsMap(adt0.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}

	for addr, claim := range updates.claims { // nolint:nomaprange
		if err := claims.Put(abi.AddrKey(addr), &claim); err != nil {
			return cid.Undef, err
		}
	}

	return claims.Root()
}

func (m *powerMigrator) migrateClaims(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	inMap, err := adt0.AsMap(adt0.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outMap := adt2.MakeEmptyMap(adt2.WrapStore(ctx, store))

	var inClaim power0.Claim
	if err = inMap.ForEach(&inClaim, func(key string) error {
		// look up seal proof type from miner actor
		a, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		minerActor, found, err := m.actorsIn.GetActor(address.Address(a))
		if err != nil {
			return err
		}
		if !found {
			return xerrors.Errorf("claim exists for miner %s but miner not in state tree", a)
		}
		var minerState miner0.State
		if err := store.Get(ctx, minerActor.Head, &minerState); err != nil {
			return err
		}
		info, err := minerState.GetInfo(adt0.WrapStore(ctx, store))
		if err != nil {
			return err
		}

		outClaim := power2.Claim{
			SealProofType:   info.SealProofType,
			RawBytePower:    inClaim.RawBytePower,
			QualityAdjPower: inClaim.QualityAdjPower,
		}
		return outMap.Put(StringKey(key), &outClaim)
	}); err != nil {
		return cid.Undef, err
	}

	return outMap.Root()
}
