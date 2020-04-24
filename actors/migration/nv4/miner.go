package nv4

import (
	"context"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/big"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type minerMigrator struct {
}

func (m minerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (*StateMigrationResult, error) {
	// first correct issues with miners due problems in old code
	result, err := m.CorrectState(ctx, store, head, info.priorEpoch, info.address)
	if err != nil {
		return nil, err
	}
	result.Transfer = big.Zero()

	var inState miner0.State
	if err := store.Get(ctx, result.NewHead, &inState); err != nil {
		return nil, err
	}

	infoCid, err := m.migrateInfo(ctx, store, inState.Info)
	if err != nil {
		return nil, xerrors.Errorf("info: %w", err)
	}

	vestingFunds, err := m.migrateVestingFunds(ctx, store, inState.VestingFunds)
	if err != nil {
		return nil, xerrors.Errorf("vesting funds: %w", err)
	}

	precommitsRoot, err := m.migratePreCommittedSectors(ctx, store, inState.PreCommittedSectors)
	if err != nil {
		return nil, xerrors.Errorf("precommitted sectors: %w", err)
	}

	precommitExpiryRoot, err := m.migrateBitfieldQueue(ctx, store, inState.PreCommittedSectorsExpiry)
	if err != nil {
		return nil, xerrors.Errorf("precommit expiry queue: %w", err)
	}

	allocatedSectors, err := m.migrateAllocatedSectors(ctx, store, inState.AllocatedSectors)
	if err != nil {
		return nil, xerrors.Errorf("allocated sectors: %w", err)
	}

	sectorsRoot, err := m.migrateSectors(ctx, store, inState.Sectors)
	if err != nil {
		return nil, xerrors.Errorf("sectors: %w", err)
	}

	deadlinesRoot, err := m.migrateDeadlines(ctx, store, inState.Deadlines)
	if err != nil {
		return nil, xerrors.Errorf("deadlines: %w", err)
	}

	// To preserve v2 Balance Invariant that actor balance > initial pledge + pre commit deposit + locked funds
	// we must transfer burned funds to all states in IP debt to cover their balance requirements.
	// We can maintain the fee deduction by setting the fee debt to the transferred value.
	debt := big.Zero()
	// We need to calculate this explicitly without using miner state functions because
	// miner state invariants are violated in v1 chain state so state functions panic
	minerLiabilities := big.Sum(inState.LockedFunds, inState.PreCommitDeposits, inState.InitialPledgeRequirement)
	availableBalance := big.Sub(info.balance, minerLiabilities)
	if availableBalance.LessThan(big.Zero()) {
		debt = availableBalance.Neg() // debt must always be positive
		result.Transfer = debt
	}

	outState := miner2.State{
		Info:                      infoCid,
		PreCommitDeposits:         inState.PreCommitDeposits,
		LockedFunds:               inState.LockedFunds,
		VestingFunds:              vestingFunds,
		FeeDebt:                   debt,
		InitialPledge:             inState.InitialPledgeRequirement,
		PreCommittedSectors:       precommitsRoot,
		PreCommittedSectorsExpiry: precommitExpiryRoot,
		AllocatedSectors:          allocatedSectors,
		Sectors:                   sectorsRoot,
		ProvingPeriodStart:        inState.ProvingPeriodStart,
		CurrentDeadline:           inState.CurrentDeadline,
		Deadlines:                 deadlinesRoot,
		EarlyTerminations:         inState.EarlyTerminations,
	}

	newHead, err := store.Put(ctx, &outState)
	result.NewHead = newHead
	return result, err
}

func (m *minerMigrator) migrateInfo(ctx context.Context, store cbor.IpldStore, c cid.Cid) (cid.Cid, error) {
	var oldInfo miner0.MinerInfo
	err := store.Get(ctx, c, &oldInfo)
	if err != nil {
		return cid.Undef, err
	}
	var newWorkerKey *miner2.WorkerKeyChange
	if oldInfo.PendingWorkerKey != nil {
		newPWK := miner2.WorkerKeyChange(*oldInfo.PendingWorkerKey)
		newWorkerKey = &newPWK
	}
	newInfo := miner2.MinerInfo{
		Owner:                      oldInfo.Owner,
		Worker:                     oldInfo.Worker,
		ControlAddresses:           oldInfo.ControlAddresses,
		PendingWorkerKey:           newWorkerKey,
		PeerId:                     oldInfo.PeerId,
		Multiaddrs:                 oldInfo.Multiaddrs,
		SealProofType:              oldInfo.SealProofType,
		SectorSize:                 oldInfo.SectorSize,
		WindowPoStPartitionSectors: oldInfo.WindowPoStPartitionSectors,
		ConsensusFaultElapsed:      -1, // New
	}
	return store.Put(ctx, &newInfo)
}

func (m *minerMigrator) migrateVestingFunds(_ context.Context, _ cbor.IpldStore, c cid.Cid) (cid.Cid, error) {
	// VestingFunds has a single element, a slice of VestingFund, which is unchanged between versions.
	var _ = miner2.VestingFund(miner0.VestingFund{})

	return c, nil
}

func (m *minerMigrator) migratePreCommittedSectors(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	inMap, err := adt0.AsMap(adt0.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outMap := adt2.MakeEmptyMap(adt2.WrapStore(ctx, store))

	var inSPCOCI miner0.SectorPreCommitOnChainInfo
	if err = inMap.ForEach(&inSPCOCI, func(key string) error {
		// Identical, but Go refuses to convert nested types.
		out := miner2.SectorPreCommitOnChainInfo{
			Info:               miner2.SectorPreCommitInfo(inSPCOCI.Info),
			PreCommitDeposit:   inSPCOCI.PreCommitDeposit,
			PreCommitEpoch:     inSPCOCI.PreCommitEpoch,
			DealWeight:         inSPCOCI.DealWeight,
			VerifiedDealWeight: inSPCOCI.VerifiedDealWeight,
		}
		return outMap.Put(StringKey(key), &out)
	}); err != nil {
		return cid.Undef, err
	}

	return outMap.Root()
}

func (m *minerMigrator) migrateBitfieldQueue(_ context.Context, _ cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT[Epoch]BitField is unchanged
	return root, nil
}

func (m *minerMigrator) migrateAllocatedSectors(_ context.Context, _ cbor.IpldStore, c cid.Cid) (cid.Cid, error) {
	// Bitfield is unchanged
	return c, nil
}

func (m *minerMigrator) migrateSectors(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	inArray, err := adt0.AsArray(adt0.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outArray := adt2.MakeEmptyArray(adt2.WrapStore(ctx, store))

	var inSector miner0.SectorOnChainInfo
	if err = inArray.ForEach(&inSector, func(i int64) error {
		outSector := miner2.SectorOnChainInfo{
			SectorNumber:          inSector.SectorNumber,
			SealProof:             inSector.SealProof,
			SealedCID:             inSector.SealedCID,
			DealIDs:               inSector.DealIDs,
			Activation:            inSector.Activation,
			Expiration:            inSector.Expiration,
			DealWeight:            inSector.DealWeight,
			VerifiedDealWeight:    inSector.VerifiedDealWeight,
			InitialPledge:         inSector.InitialPledge,
			ExpectedDayReward:     inSector.ExpectedDayReward,
			ExpectedStoragePledge: inSector.ExpectedStoragePledge,
			ReplacedSectorAge:     0,          // New in v2
			ReplacedDayReward:     big.Zero(), // New in v2
		}

		return outArray.Set(uint64(i), &outSector)
	}); err != nil {
		return cid.Undef, err
	}

	return outArray.Root()
}

func (m *minerMigrator) migrateDeadlines(ctx context.Context, store cbor.IpldStore, deadlines cid.Cid) (cid.Cid, error) {
	var inDeadlines miner0.Deadlines
	err := store.Get(ctx, deadlines, &inDeadlines)
	if err != nil {
		return cid.Undef, err
	}

	if miner0.WPoStPeriodDeadlines != miner2.WPoStPeriodDeadlines {
		return cid.Undef, xerrors.Errorf("unexpected WPoStPeriodDeadlines changed from %d to %d",
			miner0.WPoStPeriodDeadlines, miner2.WPoStPeriodDeadlines)
	}

	outDeadlines := miner2.Deadlines{Due: [miner2.WPoStPeriodDeadlines]cid.Cid{}}

	for i, c := range inDeadlines.Due {
		var inDeadline miner0.Deadline
		if err = store.Get(ctx, c, &inDeadline); err != nil {
			return cid.Undef, err
		}

		partitions, err := m.migratePartitions(ctx, store, inDeadline.Partitions)
		if err != nil {
			return cid.Undef, xerrors.Errorf("partitions: %w", err)
		}

		expirationEpochs, err := m.migrateBitfieldQueue(ctx, store, inDeadline.ExpirationsEpochs)
		if err != nil {
			return cid.Undef, xerrors.Errorf("bitfield queue: %w", err)
		}

		outDeadline := miner2.Deadline{
			Partitions:        partitions,
			ExpirationsEpochs: expirationEpochs,
			PostSubmissions:   inDeadline.PostSubmissions,
			EarlyTerminations: inDeadline.EarlyTerminations,
			LiveSectors:       inDeadline.LiveSectors,
			TotalSectors:      inDeadline.TotalSectors,
			FaultyPower:       miner2.PowerPair(inDeadline.FaultyPower),
		}

		outDlCid, err := store.Put(ctx, &outDeadline)
		if err != nil {
			return cid.Undef, err
		}
		outDeadlines.Due[i] = outDlCid
	}

	return store.Put(ctx, &outDeadlines)
}

func (m *minerMigrator) migratePartitions(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT[PartitionNumber]Partition
	inArray, err := adt0.AsArray(adt0.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outArray := adt2.MakeEmptyArray(adt2.WrapStore(ctx, store))

	var inPartition miner0.Partition
	if err = inArray.ForEach(&inPartition, func(i int64) error {
		expirationEpochs, err := m.migrateExpirationQueue(ctx, store, inPartition.ExpirationsEpochs)
		if err != nil {
			return xerrors.Errorf("expiration queue: %w", err)
		}

		earlyTerminated, err := m.migrateBitfieldQueue(ctx, store, inPartition.EarlyTerminated)
		if err != nil {
			return xerrors.Errorf("early termination queue: %w", err)
		}

		outPartition := miner2.Partition{
			Sectors:           inPartition.Sectors,
			Unproven:          bitfield.New(),
			Faults:            inPartition.Faults,
			Recoveries:        inPartition.Recoveries,
			Terminated:        inPartition.Terminated,
			ExpirationsEpochs: expirationEpochs,
			EarlyTerminated:   earlyTerminated,
			LivePower:         miner2.PowerPair(inPartition.LivePower),
			UnprovenPower:     miner2.NewPowerPairZero(),
			FaultyPower:       miner2.PowerPair(inPartition.FaultyPower),
			RecoveringPower:   miner2.PowerPair(inPartition.RecoveringPower),
		}

		return outArray.Set(uint64(i), &outPartition)
	}); err != nil {
		return cid.Undef, err
	}

	return outArray.Root()
}

func (m *minerMigrator) migrateExpirationQueue(_ context.Context, _ cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The AMT[ChainEpoch]ExpirationSet is unchanged, though we can't statically show this because the
	// ExpirationSet has nested structures, which Go refuses to equate.
	return root, nil
}
