package nv10

import (
	"context"

	"github.com/filecoin-project/go-bitfield"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"

	builtin3 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	miner3 "github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
	adt3 "github.com/filecoin-project/specs-actors/v4/actors/util/adt"
)

type minerMigrator struct{}

func (m minerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState miner2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	infoOut, err := m.migrateInfo(ctx, store, inState.Info)
	if err != nil {
		return nil, err
	}
	preCommittedSectorsOut, err := migrateHAMTRaw(ctx, store, inState.PreCommittedSectors, builtin3.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}
	preCommittedSectorsExpiryOut, err := migrateAMTRaw(ctx, store, inState.PreCommittedSectorsExpiry, miner3.PrecommitExpiryAmtBitwidth)
	if err != nil {
		return nil, err
	}

	sectorsOut, err := in.cache.Load(SectorsRootKey(inState.Sectors), func() (cid.Cid, error) {
		return migrateAMTRaw(ctx, store, inState.Sectors, miner3.SectorsAmtBitwidth)
	})
	if err != nil {
		return nil, err
	}

	deadlinesOut, err := m.migrateDeadlines(ctx, store, in.cache, inState.Deadlines)
	if err != nil {
		return nil, err
	}

	outState := miner3.State{
		Info:                      infoOut,
		PreCommitDeposits:         inState.PreCommitDeposits,
		LockedFunds:               inState.LockedFunds,
		VestingFunds:              inState.VestingFunds,
		FeeDebt:                   inState.FeeDebt,
		InitialPledge:             inState.InitialPledge,
		PreCommittedSectors:       preCommittedSectorsOut,
		PreCommittedSectorsExpiry: preCommittedSectorsExpiryOut,
		AllocatedSectors:          inState.AllocatedSectors,
		Sectors:                   sectorsOut,
		ProvingPeriodStart:        inState.ProvingPeriodStart,
		CurrentDeadline:           inState.CurrentDeadline,
		Deadlines:                 deadlinesOut,
		EarlyTerminations:         inState.EarlyTerminations,
	}
	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m minerMigrator) migratedCodeCID() cid.Cid {
	return builtin3.StorageMinerActorCodeID
}

func (m *minerMigrator) migrateInfo(ctx context.Context, store cbor.IpldStore, c cid.Cid) (cid.Cid, error) {
	var oldInfo miner2.MinerInfo
	err := store.Get(ctx, c, &oldInfo)
	if err != nil {
		return cid.Undef, err
	}

	var newWorkerKeyChange *miner3.WorkerKeyChange
	if oldInfo.PendingWorkerKey != nil {
		newWorkerKeyChange = &miner3.WorkerKeyChange{
			NewWorker:   oldInfo.PendingWorkerKey.NewWorker,
			EffectiveAt: oldInfo.PendingWorkerKey.EffectiveAt,
		}
	}

	windowPoStProof, err := oldInfo.SealProofType.RegisteredWindowPoStProof()
	if err != nil {
		return cid.Undef, err
	}

	newInfo := miner3.MinerInfo{
		Owner:                      oldInfo.Owner,
		Worker:                     oldInfo.Worker,
		ControlAddresses:           oldInfo.ControlAddresses,
		PendingWorkerKey:           newWorkerKeyChange,
		PeerId:                     oldInfo.PeerId,
		Multiaddrs:                 oldInfo.Multiaddrs,
		WindowPoStProofType:        windowPoStProof,
		SectorSize:                 oldInfo.SectorSize,
		WindowPoStPartitionSectors: oldInfo.WindowPoStPartitionSectors,
		ConsensusFaultElapsed:      oldInfo.ConsensusFaultElapsed,
		PendingOwnerAddress:        oldInfo.PendingOwnerAddress,
	}
	return store.Put(ctx, &newInfo)
}

func (m *minerMigrator) migrateDeadlines(ctx context.Context, store cbor.IpldStore, cache MigrationCache, deadlines cid.Cid) (cid.Cid, error) {
	var inDeadlines miner2.Deadlines
	err := store.Get(ctx, deadlines, &inDeadlines)
	if err != nil {
		return cid.Undef, err
	}

	if miner2.WPoStPeriodDeadlines != miner3.WPoStPeriodDeadlines {
		return cid.Undef, xerrors.Errorf("unexpected WPoStPeriodDeadlines changed from %d to %d",
			miner2.WPoStPeriodDeadlines, miner3.WPoStPeriodDeadlines)
	}

	outDeadlines := miner3.Deadlines{Due: [miner3.WPoStPeriodDeadlines]cid.Cid{}}

	// Start from an empty template to zero-initialize new fields.
	deadlineTemplate, err := miner3.ConstructDeadline(adt3.WrapStore(ctx, store))
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to construct new deadline template")
	}

	for i, c := range inDeadlines.Due {
		outDlCid, err := cache.Load(DeadlineKey(c), func() (cid.Cid, error) {
			var inDeadline miner2.Deadline
			if err = store.Get(ctx, c, &inDeadline); err != nil {
				return cid.Undef, err
			}

			partitions, err := m.migratePartitions(ctx, store, inDeadline.Partitions)
			if err != nil {
				return cid.Undef, xerrors.Errorf("partitions: %w", err)
			}

			expirationEpochs, err := migrateAMTRaw(ctx, store, inDeadline.ExpirationsEpochs, miner3.DeadlineExpirationAmtBitwidth)
			if err != nil {
				return cid.Undef, xerrors.Errorf("bitfield queue: %w", err)
			}

			outDeadline := *deadlineTemplate
			outDeadline.Partitions = partitions
			outDeadline.ExpirationsEpochs = expirationEpochs
			outDeadline.PartitionsPoSted = inDeadline.PostSubmissions
			outDeadline.EarlyTerminations = inDeadline.EarlyTerminations
			outDeadline.LiveSectors = inDeadline.LiveSectors
			outDeadline.TotalSectors = inDeadline.TotalSectors
			outDeadline.FaultyPower = miner3.PowerPair(inDeadline.FaultyPower)

			// If there are no live sectors in this partition, zero out the "partitions
			// posted" bitfield. This corrects a state issue where:
			// 1. A proof is submitted and a partition is marked as proven.
			// 2. All sectors in a deadline are terminated during the challenge window.
			// 3. The end of deadline logic is skipped because there are no live sectors.
			// This bug has been fixed in actors v3 (no terminations allowed during the
			// challenge window) but the state still needs to be fixed.
			// See: https://github.com/filecoin-project/specs-actors/issues/1348
			if outDeadline.LiveSectors == 0 {
				outDeadline.PartitionsPoSted = bitfield.New()
			}

			return store.Put(ctx, &outDeadline)
		})
		if err != nil {
			return cid.Undef, err
		}

		outDeadlines.Due[i] = outDlCid
	}

	return store.Put(ctx, &outDeadlines)
}

func (m *minerMigrator) migratePartitions(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT[PartitionNumber]Partition
	inArray, err := adt2.AsArray(adt2.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outArray, err := adt3.MakeEmptyArray(adt2.WrapStore(ctx, store), miner3.DeadlinePartitionsAmtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	var inPartition miner2.Partition
	if err = inArray.ForEach(&inPartition, func(i int64) error {
		expirationEpochs, err := migrateAMTRaw(ctx, store, inPartition.ExpirationsEpochs, miner3.PartitionExpirationAmtBitwidth)
		if err != nil {
			return xerrors.Errorf("expiration queue: %w", err)
		}

		earlyTerminated, err := migrateAMTRaw(ctx, store, inPartition.EarlyTerminated, miner3.PartitionEarlyTerminationArrayAmtBitwidth)
		if err != nil {
			return xerrors.Errorf("early termination queue: %w", err)
		}

		outPartition := miner3.Partition{
			Sectors:           inPartition.Sectors,
			Unproven:          inPartition.Unproven,
			Faults:            inPartition.Faults,
			Recoveries:        inPartition.Recoveries,
			Terminated:        inPartition.Terminated,
			ExpirationsEpochs: expirationEpochs,
			EarlyTerminated:   earlyTerminated,
			LivePower:         miner3.PowerPair(inPartition.LivePower),
			UnprovenPower:     miner3.PowerPair(inPartition.UnprovenPower),
			FaultyPower:       miner3.PowerPair(inPartition.FaultyPower),
			RecoveringPower:   miner3.PowerPair(inPartition.RecoveringPower),
		}

		return outArray.Set(uint64(i), &outPartition)
	}); err != nil {
		return cid.Undef, err
	}

	return outArray.Root()
}
