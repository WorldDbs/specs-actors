package miner_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/ipld"
)

func TestPartitions(t *testing.T) {
	sectors := []*miner.SectorOnChainInfo{
		testSector(2, 1, 50, 60, 1000),
		testSector(3, 2, 51, 61, 1001),
		testSector(7, 3, 52, 62, 1002),
		testSector(8, 4, 53, 63, 1003),
		testSector(11, 5, 54, 64, 1004),
		testSector(13, 6, 55, 65, 1005),
	}
	sectorSize := abi.SectorSize(32 << 30)

	quantSpec := miner.NewQuantSpec(4, 1)

	setup := func(t *testing.T) (adt.Store, *miner.Partition) {
		store := ipld.NewADTStore(context.Background())
		partition := emptyPartition(t, store)

		power, err := partition.AddSectors(store, sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))

		return store, partition
	}

	t.Run("adds sectors and reports sector stats", func(t *testing.T) {
		store, partition := setup(t)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf())

		// assert sectors have been arranged into 3 groups
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4)},
			{expiration: 13, sectors: bf(5, 6)},
		})
	})

	t.Run("doesn't add sectors twice", func(t *testing.T) {
		store, partition := setup(t)

		_, err := partition.AddSectors(store, sectors[:1], sectorSize, quantSpec)
		require.EqualError(t, err, "not all added sectors are new")
	})

	t.Run("adds faults", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		faultSet := bf(4, 5)
		_, power, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, faultSet))
		assert.True(t, expectedFaultyPower.Equals(power))

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5), bf(), bf())

		// moves faulty sectors after expiration to earlier group
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4, 5)},
			{expiration: 13, sectors: bf(6)},
		})
	})

	t.Run("re-adding faults is a no-op", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		faultSet := bf(4, 5)
		_, power, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, faultSet))
		assert.True(t, expectedFaultyPower.Equals(power))

		faultSet = bf(5, 6)
		newFaults, power, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(3), sectorSize, quantSpec)
		require.NoError(t, err)
		assertBitfieldEquals(t, newFaults, 6)
		expectedFaultyPower = miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(6)))
		assert.True(t, expectedFaultyPower.Equals(power))

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(), bf())

		// moves newly-faulty sector
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2, 6)},
			{expiration: 9, sectors: bf(3, 4, 5)},
		})
	})

	t.Run("fails to add faults for missing sectors", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		faultSet := bf(99)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not all sectors are assigned to the partition")
	})

	t.Run("adds recoveries", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5), bf())
	})

	t.Run("remove recoveries", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// declaring no faults doesn't do anything.
		newFaults, _, err := partition.DeclareFaults(store, sectorArr, bf(), abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)
		assertBitfieldEmpty(t, newFaults) // no new faults.

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5), bf())

		// removing sector 5 alters recovery set and recovery power
		newFaults, _, err = partition.DeclareFaults(store, sectorArr, bf(5), abi.ChainEpoch(10), sectorSize, quantSpec)
		require.NoError(t, err)
		assertBitfieldEmpty(t, newFaults) // these faults aren't new.

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4), bf())
	})

	t.Run("recovers faults", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		recoveryPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, recoverSet))
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// mark recoveries as recovered recover sectors
		recoveredPower, err := partition.RecoverFaults(store, sectorArr, sectorSize, quantSpec)
		require.NoError(t, err)

		// recovered power should equal power of recovery sectors
		assert.True(t, recoveryPower.Equals(recoveredPower))

		// state should be as if recovered sectors were never faults
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(6), bf(), bf())

		// restores recovered expirations to original state (unrecovered sector 6 still expires early)
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4, 6)},
			{expiration: 13, sectors: bf(5)},
		})
	})

	t.Run("faulty power recovered exactly once", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 3, 4 and 5 as recoveries. 3 is not faulty so it's skipped
		recoverSet := bf(3, 4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		recoveringPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, faultSet))
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, faultSet)
		require.NoError(t, err)
		assert.True(t, partition.RecoveringPower.Equals(recoveringPower))

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5, 6), bf())
	})

	t.Run("missing sectors are not recovered", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// try to add 99 as a recovery but it's not in the partition
		err := partition.DeclareFaultsRecovered(sectorArr, sectorSize, bf(99))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not all sectors are assigned to the partition")
	})

	t.Run("reschedules expirations", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// Mark sector 2 faulty, we should skip it when rescheudling
		faultSet := bf(2)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// reschedule
		moved, err := partition.RescheduleExpirations(store, sectorsArr(t, store, sectors), 18, bf(2, 4, 6), sectorSize, quantSpec)
		require.NoError(t, err)

		// Make sure we moved the right ones.
		assertBitfieldEquals(t, moved, 4, 6)

		// We need to change the actual sector infos so our queue validation works.
		rescheduled := rescheduleSectors(t, 18, sectors, bf(4, 6))

		// partition power and sector categorization should remain the same
		assertPartitionState(t, store, partition, quantSpec, sectorSize, rescheduled, bf(1, 2, 3, 4, 5, 6), bf(2), bf(), bf())

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3)},
			{expiration: 13, sectors: bf(5)},
			{expiration: 21, sectors: bf(4, 6)},
		})
	})

	t.Run("replace sectors", func(t *testing.T) {
		store, partition := setup(t)

		// remove 3 sectors starting with 2
		oldSectors := sectors[1:4]
		oldSectorPower := miner.PowerForSectors(sectorSize, oldSectors)
		oldSectorPledge := int64(1001 + 1002 + 1003)

		// replace 1 and add 2 new sectors
		newSectors := []*miner.SectorOnChainInfo{
			testSector(10, 2, 150, 260, 3000),
			testSector(10, 7, 151, 261, 3001),
			testSector(18, 8, 152, 262, 3002),
		}
		newSectorPower := miner.PowerForSectors(sectorSize, newSectors)
		newSectorPledge := int64(3000 + 3001 + 3002)

		powerDelta, pledgeDelta, err := partition.ReplaceSectors(store, oldSectors, newSectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPowerDelta := newSectorPower.Sub(oldSectorPower)
		assert.True(t, expectedPowerDelta.Equals(powerDelta))
		assert.Equal(t, abi.NewTokenAmount(newSectorPledge-oldSectorPledge), pledgeDelta)

		// partition state should contain new sectors and not old sectors
		allSectors := append(newSectors, sectors[:1]...)
		allSectors = append(allSectors, sectors[4:]...)
		assertPartitionState(t, store, partition, quantSpec, sectorSize, allSectors, bf(1, 2, 5, 6, 7, 8), bf(), bf(), bf())

		// sector 2 should be moved, 3 and 4 should be removed, and 7 and 8 added
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1)},
			{expiration: 13, sectors: bf(2, 5, 6, 7)},
			{expiration: 21, sectors: bf(8)},
		})
	})

	t.Run("replace sectors errors when attempting to replace inactive sector", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// fault sector 2
		faultSet := bf(2)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// remove 3 sectors starting with 2
		oldSectors := sectors[1:4]

		// replace sector 2
		newSectors := []*miner.SectorOnChainInfo{
			testSector(10, 2, 150, 260, 3000),
		}

		_, _, err = partition.ReplaceSectors(store, oldSectors, newSectors, sectorSize, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "refusing to replace inactive sectors")
	})

	t.Run("terminate sectors", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// fault sector 3, 4, 5 and 6
		faultSet := bf(3, 4, 5, 6)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// mark 4and 5 as a recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// now terminate 1, 3 and 5
		terminations := bf(1, 3, 5)
		terminationEpoch := abi.ChainEpoch(3)
		removed, err := partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedActivePower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(1)))
		assert.True(t, expectedActivePower.Equals(removed.ActivePower))
		expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(3, 5)))
		assert.True(t, expectedFaultyPower.Equals(removed.FaultyPower))

		// expect partition state to no longer reflect power and pledge from terminated sectors and terminations to contain new sectors
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 6), bf(4), terminations)

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(2)},
			{expiration: 9, sectors: bf(4, 6)},
		})

		// sectors should be added to early termination bitfield queue
		queue, err := miner.LoadBitfieldQueue(store, partition.EarlyTerminated, miner.NoQuantization)
		require.NoError(t, err)

		ExpectBQ().
			Add(terminationEpoch, 1, 3, 5).
			Equals(t, queue)
	})

	t.Run("terminate non-existent sectors", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		terminations := bf(99)
		terminationEpoch := abi.ChainEpoch(3)
		_, err := partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.EqualError(t, err, "can only terminate live sectors")
	})

	t.Run("terminate already terminated sector", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		terminations := bf(1)
		terminationEpoch := abi.ChainEpoch(3)

		// First termination works.
		removed, err := partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.NoError(t, err)
		expectedActivePower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(1)))
		assert.True(t, expectedActivePower.Equals(removed.ActivePower))
		assert.True(t, removed.FaultyPower.Equals(miner.NewPowerPairZero()))
		count, err := removed.Count()
		require.NoError(t, err)
		assert.EqualValues(t, 1, count)

		// Second termination fails
		_, err = partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.EqualError(t, err, "can only terminate live sectors")
	})

	t.Run("mark terminated sectors as faulty", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		terminations := bf(1)
		terminationEpoch := abi.ChainEpoch(3)

		// Termination works.
		_, err := partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.NoError(t, err)

		// Fault declaration for terminated sectors fails.
		newFaults, _, err := partition.DeclareFaults(store, sectorArr, terminations, abi.ChainEpoch(5), sectorSize, quantSpec)
		require.NoError(t, err)
		empty, err := newFaults.IsEmpty()
		require.NoError(t, err)
		require.True(t, empty)
	})

	t.Run("pop expiring sectors", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// add one fault with an early termination
		faultSet := bf(4)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(2), sectorSize, quantSpec)
		require.NoError(t, err)

		// pop first expiration set
		expireEpoch := abi.ChainEpoch(5)
		expset, err := partition.PopExpiredSectors(store, expireEpoch, quantSpec)
		require.NoError(t, err)

		assertBitfieldEquals(t, expset.OnTimeSectors, 1, 2)
		assertBitfieldEquals(t, expset.EarlySectors, 4)
		assert.Equal(t, abi.NewTokenAmount(1000+1001), expset.OnTimePledge)

		// active power only contains power from non-faulty sectors
		assert.True(t, expset.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[:2])))

		// faulty power comes from early termination
		assert.True(t, expset.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[3:4])))

		// expect sectors to be moved to terminations
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf(1, 2, 4))

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 9, sectors: bf(3)},
			{expiration: 13, sectors: bf(5, 6)},
		})

		// sectors should be added to early termination bitfield queue
		queue, err := miner.LoadBitfieldQueue(store, partition.EarlyTerminated, miner.NoQuantization)
		require.NoError(t, err)

		// only early termination appears in bitfield queue
		ExpectBQ().
			Add(expireEpoch, 4).
			Equals(t, queue)
	})

	t.Run("pop expiring sectors errors if a recovery exists", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		_, _, err := partition.DeclareFaults(store, sectorArr, bf(5), abi.ChainEpoch(2), sectorSize, quantSpec)
		require.NoError(t, err)

		// add a recovery
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, bf(5))
		require.NoError(t, err)

		// pop first expiration set
		expireEpoch := abi.ChainEpoch(5)
		_, err = partition.PopExpiredSectors(store, expireEpoch, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected recoveries while processing expirations")
	})

	t.Run("records missing PoSt", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// record entire partition as faulted
		newFaultPower, failedRecoveryPower, err := partition.RecordMissedPost(store, abi.ChainEpoch(6), quantSpec)
		require.NoError(t, err)

		expectedNewFaultPower := miner.PowerForSectors(sectorSize, sectors[:3])
		assert.True(t, expectedNewFaultPower.Equals(newFaultPower))

		expectedFailedRecoveryPower := miner.PowerForSectors(sectorSize, sectors[3:5])
		assert.True(t, expectedFailedRecoveryPower.Equals(failedRecoveryPower))

		// everything is now faulty
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(1, 2, 3, 4, 5, 6), bf(), bf())

		// everything not in first expiration group is now in second because fault expiration quantized to 9
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4, 5, 6)},
		})
	})

	t.Run("pops early terminations", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// fault sector 3, 4, 5 and 6
		faultSet := bf(3, 4, 5, 6)
		_, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// mark 4and 5 as a recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// now terminate 1, 3 and 5
		terminations := bf(1, 3, 5)
		terminationEpoch := abi.ChainEpoch(3)
		_, err = partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.NoError(t, err)

		// pop first termination
		result, hasMore, err := partition.PopEarlyTerminations(store, 1)
		require.NoError(t, err)

		// expect first sector to be in early terminations
		assertBitfieldEquals(t, result.Sectors[terminationEpoch], 1)

		// expect more results
		assert.True(t, hasMore)

		// expect terminations to still contain 3 and 5
		queue, err := miner.LoadBitfieldQueue(store, partition.EarlyTerminated, miner.NoQuantization)
		require.NoError(t, err)

		// only early termination appears in bitfield queue
		ExpectBQ().
			Add(terminationEpoch, 3, 5).
			Equals(t, queue)

		// pop the rest
		result, hasMore, err = partition.PopEarlyTerminations(store, 5)
		require.NoError(t, err)

		// expect 3 and 5
		assertBitfieldEquals(t, result.Sectors[terminationEpoch], 3, 5)

		// expect no more results
		assert.False(t, hasMore)

		// expect early terminations to be empty
		queue, err = miner.LoadBitfieldQueue(store, partition.EarlyTerminated, miner.NoQuantization)
		require.NoError(t, err)
		ExpectBQ().Equals(t, queue)
	})

	t.Run("test max sectors", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		partition := emptyPartition(t, store)

		proofType := abi.RegisteredSealProof_StackedDrg32GiBV1
		sectorSize, err := proofType.SectorSize()
		require.NoError(t, err)
		partitionSectors, err := builtin.SealProofWindowPoStPartitionSectors(proofType)
		require.NoError(t, err)

		manySectors := make([]*miner.SectorOnChainInfo, partitionSectors)
		ids := make([]uint64, partitionSectors)
		for i := range manySectors {
			id := uint64((i + 1) << 50)
			ids[i] = id
			manySectors[i] = testSector(int64(i+1), int64(id), 50, 60, 1000)
		}
		sectorNos := bf(ids...)

		power, err := partition.AddSectors(store, manySectors, sectorSize, miner.NoQuantization)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, manySectors)
		assert.True(t, expectedPower.Equals(power))

		assertPartitionState(
			t, store, partition,
			miner.NoQuantization, sectorSize, manySectors,
			sectorNos, bf(), bf(), bf(),
		)

		// Make sure we can still encode and decode.
		var buf bytes.Buffer
		err = partition.MarshalCBOR(&buf)
		require.NoError(t, err)

		var newPartition miner.Partition
		err = newPartition.UnmarshalCBOR(&buf)
		require.NoError(t, err)

		assertPartitionState(
			t, store, &newPartition,
			miner.NoQuantization, sectorSize, manySectors,
			sectorNos, bf(), bf(), bf(),
		)

	})
}

type expectExpirationGroup struct {
	expiration abi.ChainEpoch
	sectors    bitfield.BitField
}

func assertPartitionExpirationQueue(t *testing.T, store adt.Store, partition *miner.Partition, quant miner.QuantSpec, groups []expectExpirationGroup) {
	queue, err := miner.LoadExpirationQueue(store, partition.ExpirationsEpochs, quant)
	require.NoError(t, err)

	for _, group := range groups {
		requireNoExpirationGroupsBefore(t, group.expiration, queue)
		set, err := queue.PopUntil(group.expiration)
		require.NoError(t, err)

		// we pnly care whether the sectors are in the queue or not. ExpirationQueue tests can deal with early or on time.
		allSectors, err := bitfield.MergeBitFields(set.OnTimeSectors, set.EarlySectors)
		require.NoError(t, err)
		assertBitfieldsEqual(t, group.sectors, allSectors)
	}
}

func checkPartitionInvariants(t *testing.T,
	store adt.Store,
	partition *miner.Partition,
	quant miner.QuantSpec,
	sectorSize abi.SectorSize,
	sectors []*miner.SectorOnChainInfo,
) {
	live, err := partition.LiveSectors()
	require.NoError(t, err)

	active, err := partition.ActiveSectors()
	require.NoError(t, err)

	liveSectors := selectSectors(t, sectors, live)

	// Validate power
	faultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, partition.Faults))
	assert.True(t, partition.FaultyPower.Equals(faultyPower), "faulty power was %v, expected %v", partition.FaultyPower, faultyPower)
	recoveringPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, partition.Recoveries))
	assert.True(t, partition.RecoveringPower.Equals(recoveringPower), "recovering power was %v, expected %v", partition.RecoveringPower, recoveringPower)
	livePower := miner.PowerForSectors(sectorSize, liveSectors)
	assert.True(t, partition.LivePower.Equals(livePower), "live power was %v, expected %v", partition.LivePower, livePower)
	activePower := livePower.Sub(faultyPower)
	partitionActivePower := partition.ActivePower()
	assert.True(t, partitionActivePower.Equals(activePower), "active power was %v, expected %v", partitionActivePower, activePower)

	// All recoveries are faults.
	contains, err := util.BitFieldContainsAll(partition.Faults, partition.Recoveries)
	require.NoError(t, err)
	assert.True(t, contains)

	// All faults are live.
	contains, err = util.BitFieldContainsAll(live, partition.Faults)
	require.NoError(t, err)
	assert.True(t, contains)

	// All terminated sectors are part of the partition.
	contains, err = util.BitFieldContainsAll(partition.Sectors, partition.Terminated)
	require.NoError(t, err)
	assert.True(t, contains)

	// Live has no terminated sectors
	contains, err = util.BitFieldContainsAny(live, partition.Terminated)
	require.NoError(t, err)
	assert.False(t, contains)

	// Live contains active sectors
	contains, err = util.BitFieldContainsAll(live, active)
	require.NoError(t, err)
	assert.True(t, contains)

	// Active contains no faults
	contains, err = util.BitFieldContainsAny(active, partition.Faults)
	require.NoError(t, err)
	assert.False(t, contains)

	// Ok, now validate that the expiration queue makes sense.
	{
		seenSectors := make(map[abi.SectorNumber]bool)

		expQ, err := miner.LoadExpirationQueue(store, partition.ExpirationsEpochs, quant)
		require.NoError(t, err)

		var exp miner.ExpirationSet
		err = expQ.ForEach(&exp, func(epoch int64) error {
			require.Equal(t, quant.QuantizeUp(abi.ChainEpoch(epoch)), abi.ChainEpoch(epoch))

			all, err := bitfield.MergeBitFields(exp.OnTimeSectors, exp.EarlySectors)
			require.NoError(t, err)
			active, err := bitfield.SubtractBitField(all, partition.Faults)
			require.NoError(t, err)
			faulty, err := bitfield.IntersectBitField(all, partition.Faults)
			require.NoError(t, err)

			activeSectors := selectSectors(t, liveSectors, active)
			faultySectors := selectSectors(t, liveSectors, faulty)
			onTimeSectors := selectSectors(t, liveSectors, exp.OnTimeSectors)
			earlySectors := selectSectors(t, liveSectors, exp.EarlySectors)

			// Validate that expiration only contains valid sectors.
			contains, err := util.BitFieldContainsAll(partition.Faults, exp.EarlySectors)
			require.NoError(t, err)
			assert.True(t, contains, "all early expirations must be faulty")

			contains, err = util.BitFieldContainsAll(live, exp.OnTimeSectors)
			require.NoError(t, err)
			assert.True(t, contains, "all expirations must be live")

			// Validate that sectors are only contained in one
			// epoch, and that they're contained in a valid epoch.
			for _, sector := range onTimeSectors {
				assert.False(t, seenSectors[sector.SectorNumber], "sector already seen")
				seenSectors[sector.SectorNumber] = true
				actualEpoch := quant.QuantizeUp(sector.Expiration)
				assert.Equal(t, actualEpoch, abi.ChainEpoch(epoch))
			}

			for _, sector := range earlySectors {
				assert.False(t, seenSectors[sector.SectorNumber], "sector already seen")
				seenSectors[sector.SectorNumber] = true
				actualEpoch := quant.QuantizeUp(sector.Expiration)
				assert.Less(t, epoch, int64(actualEpoch))
			}

			// Validate power and pledge.
			activePower := miner.PowerForSectors(sectorSize, activeSectors)
			assert.True(t, exp.ActivePower.Equals(activePower))

			faultyPower := miner.PowerForSectors(sectorSize, faultySectors)
			assert.True(t, exp.FaultyPower.Equals(faultyPower))

			onTimePledge := big.Zero()
			for _, sector := range onTimeSectors {
				onTimePledge = big.Add(onTimePledge, sector.InitialPledge)
			}
			assert.Equal(t, onTimePledge, exp.OnTimePledge)

			return nil
		})
		require.NoError(t, err)
	}

	// Now make sure the early termination queue makes sense.
	{
		seenSectors := make(map[uint64]bool)

		earlyQ, err := miner.LoadBitfieldQueue(store, partition.EarlyTerminated, miner.NoQuantization)
		require.NoError(t, err)

		err = earlyQ.ForEach(func(epoch abi.ChainEpoch, bf bitfield.BitField) error {
			return bf.ForEach(func(i uint64) error {
				assert.False(t, seenSectors[i], "sector already seen")
				seenSectors[i] = true
				return nil
			})
		})
		require.NoError(t, err)

		earlyTerms := bf()
		for bit := range seenSectors {
			earlyTerms.Set(bit)
		}

		contains, err := util.BitFieldContainsAll(partition.Terminated, earlyTerms)
		require.NoError(t, err)
		require.True(t, contains)
	}
}

func assertPartitionState(t *testing.T,
	store adt.Store,
	partition *miner.Partition,
	quant miner.QuantSpec,
	sectorSize abi.SectorSize,
	sectors []*miner.SectorOnChainInfo,
	allSectorIds bitfield.BitField,
	faults bitfield.BitField,
	recovering bitfield.BitField,
	terminations bitfield.BitField) {

	assertBitfieldsEqual(t, faults, partition.Faults)
	assertBitfieldsEqual(t, recovering, partition.Recoveries)
	assertBitfieldsEqual(t, terminations, partition.Terminated)
	assertBitfieldsEqual(t, allSectorIds, partition.Sectors)

	checkPartitionInvariants(t, store, partition, quant, sectorSize, sectors)
}

func bf(secNos ...uint64) bitfield.BitField {
	return bitfield.NewFromSet(secNos)
}

func selectSectors(t *testing.T, sectors []*miner.SectorOnChainInfo, field bitfield.BitField) []*miner.SectorOnChainInfo {
	toInclude, err := field.AllMap(miner.SectorsMax)
	require.NoError(t, err)

	included := []*miner.SectorOnChainInfo{}
	for _, s := range sectors {
		if !toInclude[uint64(s.SectorNumber)] {
			continue
		}
		included = append(included, s)
		delete(toInclude, uint64(s.SectorNumber))
	}
	assert.Empty(t, toInclude, "expected additional sectors")
	return included
}

func emptyPartition(t *testing.T, store adt.Store) *miner.Partition {
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	return miner.ConstructPartition(root)
}

func rescheduleSectors(t *testing.T, target abi.ChainEpoch, sectors []*miner.SectorOnChainInfo, filter bitfield.BitField) []*miner.SectorOnChainInfo {
	toReschedule, err := filter.AllMap(miner.SectorsMax)
	require.NoError(t, err)
	output := make([]*miner.SectorOnChainInfo, len(sectors))
	for i, sector := range sectors {
		cpy := *sector
		if toReschedule[uint64(cpy.SectorNumber)] {
			cpy.Expiration = target
		}
		output[i] = &cpy
	}
	return output
}
