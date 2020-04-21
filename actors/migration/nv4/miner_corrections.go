package nv4

import (
	"bytes"
	"context"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	miner "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	power0 "github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

func (m *minerMigrator) CorrectState(ctx context.Context, store cbor.IpldStore, head cid.Cid,
	priorEpoch abi.ChainEpoch, a addr.Address) (*StateMigrationResult, error) {
	result := &StateMigrationResult{
		NewHead:  head,
		Transfer: big.Zero(),
	}
	epoch := priorEpoch + 1
	var st miner.State
	if err := store.Get(ctx, head, &st); err != nil {
		return nil, err
	}

	// If the miner's proving period hasn't started yet, it's a new v0
	// miner.
	//
	// 1. There's no need to fix any state.
	// 2. We definitely don't want to reschedule the proving period
	//    start/deadlines.
	if st.ProvingPeriodStart > epoch {
		return result, nil
	}

	adtStore := adt.WrapStore(ctx, store)

	sectors, err := miner.LoadSectors(adtStore, st.Sectors)
	if err != nil {
		return nil, err
	}

	info, err := st.GetInfo(adtStore)
	if err != nil {
		return nil, err
	}

	powerClaimSuspect, err := m.correctForCCUpgradeThenFaultIssue(ctx, store, &st, sectors, epoch, info.SectorSize)
	if err != nil {
		return nil, err
	}

	if powerClaimSuspect {
		claimUpdate, err := m.computeClaim(ctx, adtStore, &st, a)
		if err != nil {
			return nil, err
		}
		if claimUpdate != nil {
			result.PowerUpdates = append(result.PowerUpdates, claimUpdate)
		}

		cronUpdate, err := m.computeProvingPeriodCron(a, &st, epoch, adtStore)
		if err != nil {
			return nil, err
		}
		if cronUpdate != nil {
			result.PowerUpdates = append(result.PowerUpdates, cronUpdate)
		}
	}

	newHead, err := store.Put(ctx, &st)
	result.NewHead = newHead
	return result, err
}

func (m *minerMigrator) correctForCCUpgradeThenFaultIssue(
	ctx context.Context, store cbor.IpldStore, st *miner.State, sectors miner.Sectors, epoch abi.ChainEpoch,
	sectorSize abi.SectorSize,
) (bool, error) {

	quantSpec := st.QuantSpecEveryDeadline()

	deadlines, err := st.LoadDeadlines(adt.WrapStore(ctx, store))
	if err != nil {
		return false, err
	}

	missedProvingPeriodCron := false
	for st.ProvingPeriodStart+miner.WPoStProvingPeriod <= epoch {
		st.ProvingPeriodStart += miner.WPoStProvingPeriod
		missedProvingPeriodCron = true
	}
	expectedDeadlline := uint64((epoch - st.ProvingPeriodStart) / miner.WPoStChallengeWindow)
	if expectedDeadlline != st.CurrentDeadline {
		st.CurrentDeadline = expectedDeadlline
		missedProvingPeriodCron = true
	}

	deadlinesModified := false
	err = deadlines.ForEach(adt.WrapStore(ctx, store), func(dlIdx uint64, deadline *miner.Deadline) error {
		partitions, err := adt.AsArray(adt.WrapStore(ctx, store), deadline.Partitions)
		if err != nil {
			return err
		}

		alteredPartitions := make(map[uint64]miner.Partition)
		allFaultyPower := miner.NewPowerPairZero()
		var part miner.Partition
		err = partitions.ForEach(&part, func(partIdx int64) error {
			exq, err := miner.LoadExpirationQueue(adt.WrapStore(ctx, store), part.ExpirationsEpochs, quantSpec)
			if err != nil {
				return err
			}

			exqRoot, stats, err := m.correctExpirationQueue(exq, sectors, part.Terminated, part.Faults, sectorSize)
			if err != nil {
				return err
			}

			// if unmodified, we're done
			if exqRoot.Equals(cid.Undef) {
				return nil
			}

			if !part.ExpirationsEpochs.Equals(exqRoot) {
				part.ExpirationsEpochs = exqRoot
				alteredPartitions[uint64(partIdx)] = part
			}

			if !part.LivePower.Equals(stats.totalActivePower.Add(stats.totalFaultyPower)) {
				part.LivePower = stats.totalActivePower.Add(stats.totalFaultyPower)
				alteredPartitions[uint64(partIdx)] = part
			}
			if !part.FaultyPower.Equals(stats.totalFaultyPower) {
				part.FaultyPower = stats.totalFaultyPower
				alteredPartitions[uint64(partIdx)] = part
			}
			if missedProvingPeriodCron {
				part.Recoveries = bitfield.New()
				part.RecoveringPower = miner.NewPowerPairZero()
				alteredPartitions[uint64(partIdx)] = part
			}
			allFaultyPower = allFaultyPower.Add(part.FaultyPower)

			return nil
		})
		if err != nil {
			return err
		}

		// if we've failed to update at last proving period, expect post submissions to contain bits it shouldn't
		if missedProvingPeriodCron {
			deadline.PostSubmissions = bitfield.New()
			if err := deadlines.UpdateDeadline(adt.WrapStore(ctx, store), dlIdx, deadline); err != nil {
				return err
			}
			deadlinesModified = true
		}

		// if partitions have been updates, record that in deadline
		if len(alteredPartitions) > 0 {
			for partIdx, part := range alteredPartitions { // nolint:nomaprange
				if err := partitions.Set(partIdx, &part); err != nil {
					return err
				}
			}

			deadline.Partitions, err = partitions.Root()
			if err != nil {
				return err
			}

			deadline.FaultyPower = allFaultyPower

			if err := deadlines.UpdateDeadline(adt.WrapStore(ctx, store), dlIdx, deadline); err != nil {
				return err
			}
			deadlinesModified = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	if !deadlinesModified {
		return false, nil
	}

	if err = st.SaveDeadlines(adt.WrapStore(ctx, store), deadlines); err != nil {
		return false, err
	}

	return true, err
}

func (m *minerMigrator) computeProvingPeriodCron(a addr.Address, st *miner.State, epoch abi.ChainEpoch, store adt.Store) (*cronUpdate, error) {
	var buf bytes.Buffer
	payload := &miner.CronEventPayload{
		EventType: miner.CronEventProvingDeadline,
	}
	err := payload.MarshalCBOR(&buf)
	if err != nil {
		return nil, err
	}

	dlInfo := st.DeadlineInfo(epoch)
	return &cronUpdate{
		epoch: dlInfo.Last(),
		event: power0.CronEvent{
			MinerAddr:       a,
			CallbackPayload: buf.Bytes(),
		},
	}, nil
}

func (m *minerMigrator) computeClaim(ctx context.Context, store adt.Store, st *miner.State, a addr.Address) (*claimUpdate, error) {
	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		return nil, err
	}

	activePower := miner.NewPowerPairZero()
	err = deadlines.ForEach(store, func(dlIdx uint64, dl *miner.Deadline) error {
		partitions, err := dl.PartitionsArray(store)
		if err != nil {
			return err
		}

		var part miner.Partition
		return partitions.ForEach(&part, func(pIdx int64) error {
			activePower = activePower.Add(part.LivePower.Sub(part.FaultyPower))
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return &claimUpdate{
		addr: a,
		claim: power0.Claim{
			RawBytePower:    activePower.Raw,
			QualityAdjPower: activePower.QA,
		},
	}, nil
}

type expirationQueueStats struct {
	// total of all active power in the expiration queue
	totalActivePower miner.PowerPair
	// total of all faulty power in the expiration queue
	totalFaultyPower miner.PowerPair
}

// Updates the expiration queue by correcting any duplicate entries and their fallout.
// If no changes need to be made cid.Undef will be returned.
// Returns the new root of the expiration queue
func (m *minerMigrator) correctExpirationQueue(exq miner.ExpirationQueue, sectors miner.Sectors,
	allTerminated bitfield.BitField, allFaults bitfield.BitField, sectorSize abi.SectorSize,
) (cid.Cid, expirationQueueStats, error) {
	// processed expired sectors includes all terminated and all sectors seen in earlier expiration sets
	processedExpiredSectors := allTerminated
	expirationSetPowerSuspect := false

	var exs miner.ExpirationSet

	// Check for faults that need to be erased.
	// Erased faults will be removed from bitfields and the power will be recomputed
	// in the subsequent loop.
	err := exq.ForEach(&exs, func(epoch int64) error { //nolint:nomaprange
		// Detect sectors that are present in this expiration set as "early", but that
		// have already terminated or duplicate a prior entry in the queue, and thus will
		// be terminated before this entry is processed. The sector was rescheduled here
		// upon fault, but the entry is stale and should not exist.
		modified := false
		earlyDuplicates, err := bitfield.IntersectBitField(exs.EarlySectors, processedExpiredSectors)
		if err != nil {
			return err
		} else if empty, err := earlyDuplicates.IsEmpty(); err != nil {
			return err
		} else if !empty {
			modified = true
			exs.EarlySectors, err = bitfield.SubtractBitField(exs.EarlySectors, earlyDuplicates)
			if err != nil {
				return err
			}
		}

		// Detect sectors that are terminating on time, but have either already terminated or duplicate
		// an entry in the queue. The sector might be faulty, but were expiring here anyway so not
		// rescheduled as "early".
		onTimeDuplicates, err := bitfield.IntersectBitField(exs.OnTimeSectors, processedExpiredSectors)
		if err != nil {
			return err
		} else if empty, err := onTimeDuplicates.IsEmpty(); err != nil {
			return err
		} else if !empty {
			modified = true
			exs.OnTimeSectors, err = bitfield.SubtractBitField(exs.OnTimeSectors, onTimeDuplicates)
			if err != nil {
				return err
			}
		}

		if modified {
			expirationSetPowerSuspect = true
			exs2, err := copyES(exs)
			if err != nil {
				return err
			}
			if err := exq.Set(uint64(epoch), &exs2); err != nil {
				return err
			}
		}

		// Record all sectors that would be terminated after this queue entry is processed.
		if processedExpiredSectors, err = bitfield.MultiMerge(processedExpiredSectors, exs.EarlySectors, exs.OnTimeSectors); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return cid.Undef, expirationQueueStats{}, err
	}

	// If we didn't find any duplicate sectors, we're done.
	if !expirationSetPowerSuspect {
		return cid.Undef, expirationQueueStats{}, nil
	}

	partitionActivePower := miner.NewPowerPairZero()
	partitionFaultyPower := miner.NewPowerPairZero()
	err = exq.ForEach(&exs, func(epoch int64) error {
		modified, activePower, faultyPower, err := correctExpirationSetPowerAndPledge(&exs, sectors, allFaults, sectorSize)
		if err != nil {
			return err
		}
		partitionActivePower = partitionActivePower.Add(activePower)
		partitionFaultyPower = partitionFaultyPower.Add(faultyPower)

		if modified {
			exs2, err := copyES(exs)
			if err != nil {
				return err
			}
			if err := exq.Set(uint64(epoch), &exs2); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return cid.Undef, expirationQueueStats{}, err
	}

	expirationQueueRoot, err := exq.Root()
	if err != nil {
		return cid.Undef, expirationQueueStats{}, err
	}

	return expirationQueueRoot, expirationQueueStats{
		partitionActivePower,
		partitionFaultyPower,
	}, nil
}

// Recompute active and faulty power for an expiration set.
// The active power for an expiration set should be the sum of the power of all its active sectors,
// where active means all sectors not labeled as a fault in the partition. Similarly, faulty power
// is the sum of faulty sectors.
// If a sector has been rescheduled from ES3 to both ES1 as active and ES2
// as a fault, we expect it to be labeled as a fault in the partition. We have already
// removed the sector from ES2, so this correction should move its active power to faulty power in ES1
// because it is labeled as a fault, remove its power altogether from ES2 because its been removed from
// ES2's bitfields, and correct the double subtraction of power from ES3.
func correctExpirationSetPowerAndPledge(exs *miner.ExpirationSet, sectors miner.Sectors,
	allFaults bitfield.BitField, sectorSize abi.SectorSize,
) (bool, miner.PowerPair, miner.PowerPair, error) {

	modified := false

	allSectors, err := bitfield.MergeBitFields(exs.OnTimeSectors, exs.EarlySectors)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}

	// correct errors in active power
	activeSectors, err := bitfield.SubtractBitField(allSectors, allFaults)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	as, err := sectors.Load(activeSectors)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	activePower := miner.PowerForSectors(sectorSize, as)

	if !activePower.Equals(exs.ActivePower) {
		exs.ActivePower = activePower
		modified = true
	}

	// correct errors in faulty power
	faultySectors, err := bitfield.IntersectBitField(allSectors, allFaults)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	fs, err := sectors.Load(faultySectors)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	faultyPower := miner.PowerForSectors(sectorSize, fs)

	if !faultyPower.Equals(exs.FaultyPower) {
		exs.FaultyPower = faultyPower
		modified = true
	}

	// correct errors in pledge
	expectedPledge := big.Zero()
	ots, err := sectors.Load(exs.OnTimeSectors)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	for _, sector := range ots {
		expectedPledge = big.Add(expectedPledge, sector.InitialPledge)
	}
	if !expectedPledge.Equals(exs.OnTimePledge) {
		exs.OnTimePledge = expectedPledge
		modified = true
	}

	return modified, activePower, faultyPower, nil
}

func copyES(in miner.ExpirationSet) (miner.ExpirationSet, error) {
	ots, err := in.OnTimeSectors.Copy()
	if err != nil {
		return miner.ExpirationSet{}, err
	}

	es, err := in.EarlySectors.Copy()
	if err != nil {
		return miner.ExpirationSet{}, err
	}

	return miner.ExpirationSet{
		OnTimeSectors: ots,
		EarlySectors:  es,
		OnTimePledge:  in.OnTimePledge,
		ActivePower:   in.ActivePower,
		FaultyPower:   in.FaultyPower,
	}, nil
}
