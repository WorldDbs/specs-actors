package miner_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
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

	setupUnproven := func(t *testing.T) (adt.Store, *miner.Partition) {
		store := ipld.NewADTStore(context.Background())
		partition := emptyPartition(t, store)

		power, err := partition.AddSectors(store, false, sectors, sectorSize, quantSpec)
		require.NoError(t, err)
		require.True(t, power.IsZero())

		return store, partition
	}

	setup := func(t *testing.T) (adt.Store, *miner.Partition) {
		store, partition := setupUnproven(t)

		power := partition.ActivateUnproven()

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))

		return store, partition
	}

	t.Run("adds sectors then activates unproven", func(t *testing.T) {
		_, partition := setupUnproven(t)

		power := partition.ActivateUnproven()
		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))
	})

	t.Run("adds sectors and reports sector stats", func(t *testing.T) {
		store, partition := setup(t)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf(), bf())

		// assert sectors have been arranged into 3 groups
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4)},
			{expiration: 13, sectors: bf(5, 6)},
		})
	})

	t.Run("doesn't add sectors twice", func(t *testing.T) {
		store, partition := setup(t)

		_, err := partition.AddSectors(store, false, sectors[:1], sectorSize, quantSpec)
		require.EqualError(t, err, "not all added sectors are new")
	})

	for _, proven := range []bool{true, false} {
		t.Run(fmt.Sprintf("adds faults (proven:%v)", proven), func(t *testing.T) {
			store, partition := setupUnproven(t)
			if proven {
				_ = partition.ActivateUnproven()
			}
			sectorArr := sectorsArr(t, store, sectors)

			faultSet := bf(4, 5)
			_, powerDelta, newFaultyPower, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
			require.NoError(t, err)

			expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, faultSet))
			expectedPowerDelta := miner.NewPowerPairZero()
			if proven {
				expectedPowerDelta = expectedFaultyPower.Neg()
			}
			assert.True(t, expectedFaultyPower.Equals(newFaultyPower))
			assert.True(t, powerDelta.Equals(expectedPowerDelta))

			sectorNos := bf(1, 2, 3, 4, 5, 6)
			unprovenSet := bf(1, 2, 3, 6) // faults are no longer "unproven", just faulty.
			if proven {
				unprovenSet = bf()
			}

			assertPartitionState(
				t, store, partition, quantSpec, sectorSize, sectors,
				sectorNos, faultSet, bf(), bf(), unprovenSet,
			)

			// moves faulty sectors after expiration to earlier group
			assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
				{expiration: 5, sectors: bf(1, 2)},
				{expiration: 9, sectors: bf(3, 4, 5)},
				{expiration: 13, sectors: bf(6)},
			})
		})
	}

	t.Run("re-adding faults is a no-op", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		faultSet := bf(4, 5)
		_, powerDelta, newFaultyPower, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, faultSet))
		assert.True(t, expectedFaultyPower.Equals(newFaultyPower))
		assert.True(t, powerDelta.Equals(expectedFaultyPower.Neg()))

		faultSet = bf(5, 6)
		newFaults, powerDelta, newFaultyPower, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(3), sectorSize, quantSpec)
		require.NoError(t, err)
		assertBitfieldEquals(t, newFaults, 6)
		expectedFaultyPower = miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(6)))
		assert.True(t, expectedFaultyPower.Equals(newFaultyPower))
		assert.True(t, powerDelta.Equals(expectedFaultyPower.Neg()))

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(), bf(), bf())

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
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not all sectors are assigned to the partition")
	})

	t.Run("adds recoveries", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5), bf(), bf())
	})

	t.Run("remove recoveries", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// declaring no faults doesn't do anything.
		newFaults, _, _, err := partition.DeclareFaults(store, sectorArr, bf(), abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)
		assertBitfieldEmpty(t, newFaults) // no new faults.

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5), bf(), bf())

		// removing sector 5 alters recovery set and recovery power
		newFaults, _, _, err = partition.DeclareFaults(store, sectorArr, bf(5), abi.ChainEpoch(10), sectorSize, quantSpec)
		require.NoError(t, err)
		assertBitfieldEmpty(t, newFaults) // these faults aren't new.

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4), bf(), bf())
	})

	t.Run("recovers faults", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
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
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(6), bf(), bf(), bf())

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
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 3, 4 and 5 as recoveries. 3 is not faulty so it's skipped
		recoverSet := bf(3, 4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		recoveringPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, faultSet))
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, faultSet)
		require.NoError(t, err)
		assert.True(t, partition.RecoveringPower.Equals(recoveringPower))

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5, 6), bf(), bf())
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

		unprovenSector := testSector(13, 7, 55, 65, 1006)
		allSectors := append(sectors[:len(sectors):len(sectors)], unprovenSector)
		sectorArr := sectorsArr(t, store, allSectors)

		// Mark sector 2 faulty, we should skip it when rescheduling
		faultSet := bf(2)
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// Add an unproven sector. We _should_ reschedule the expiration.
		// This is fine as we don't allow actually _expiring_ sectors
		// while there are unproven sectors.
		powerDelta, err := partition.AddSectors(
			store, false,
			[]*miner.SectorOnChainInfo{unprovenSector},
			sectorSize, quantSpec,
		)
		require.NoError(t, err)
		require.True(t, powerDelta.IsZero()) // no power for unproven sectors.

		// reschedule
		replaced, err := partition.RescheduleExpirations(store, sectorArr, 18, bf(2, 4, 6, 7), sectorSize, quantSpec)
		require.NoError(t, err)

		// Assert we returned the sector infos of the replaced sectors
		assert.Len(t, replaced, 3)
		sort.Slice(replaced, func(i, j int) bool {
			return replaced[i].SectorNumber < replaced[j].SectorNumber
		})
		assert.Equal(t, abi.SectorNumber(4), replaced[0].SectorNumber)
		assert.Equal(t, abi.SectorNumber(6), replaced[1].SectorNumber)
		assert.Equal(t, abi.SectorNumber(7), replaced[2].SectorNumber)

		// We need to change the actual sector infos so our queue validation works.
		rescheduled := rescheduleSectors(t, 18, allSectors, bf(4, 6, 7))

		// partition power and sector categorization should remain the same
		assertPartitionState(t, store, partition, quantSpec, sectorSize, rescheduled, bf(1, 2, 3, 4, 5, 6, 7), bf(2), bf(), bf(), bf(7))

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3)},
			{expiration: 13, sectors: bf(5)},
			{expiration: 21, sectors: bf(4, 6, 7)},
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
		assertPartitionState(t, store, partition, quantSpec, sectorSize, allSectors, bf(1, 2, 5, 6, 7, 8), bf(), bf(), bf(), bf())

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
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
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

	t.Run("replace sectors errors when attempting to unproven sector", func(t *testing.T) {
		store, partition := setupUnproven(t)

		// remove 3 sectors starting with 2
		oldSectors := sectors[1:4]

		// replace sector 2
		newSectors := []*miner.SectorOnChainInfo{
			testSector(10, 2, 150, 260, 3000),
		}

		_, _, err := partition.ReplaceSectors(store, oldSectors, newSectors, sectorSize, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "refusing to replace inactive sectors")
	})

	t.Run("terminate sectors", func(t *testing.T) {
		store, partition := setup(t)

		unprovenSector := testSector(13, 7, 55, 65, 1006)
		allSectors := append(sectors[:len(sectors):len(sectors)], unprovenSector)
		sectorArr := sectorsArr(t, store, allSectors)

		// Add an unproven sector.
		powerDelta, err := partition.AddSectors(
			store, false,
			[]*miner.SectorOnChainInfo{unprovenSector},
			sectorSize, quantSpec,
		)
		require.NoError(t, err)
		require.True(t, powerDelta.IsZero()) // no power for unproven sectors.

		// fault sector 3, 4, 5 and 6
		faultSet := bf(3, 4, 5, 6)
		_, _, _, err = partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// mark 4and 5 as a recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// now terminate 1, 3, 5, and 7
		terminations := bf(1, 3, 5, 7)
		terminationEpoch := abi.ChainEpoch(3)
		removed, err := partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedActivePower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(1)))
		assert.True(t, expectedActivePower.Equals(removed.ActivePower))
		expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(3, 5)))
		assert.True(t, expectedFaultyPower.Equals(removed.FaultyPower))

		// expect partition state to no longer reflect power and pledge from terminated sectors and terminations to contain new sectors
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6, 7), bf(4, 6), bf(4), terminations, bf())

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(2)},
			{expiration: 9, sectors: bf(4, 6)},
		})

		// sectors should be added to early termination bitfield queue
		queue, err := miner.LoadBitfieldQueue(store, partition.EarlyTerminated, miner.NoQuantization)
		require.NoError(t, err)

		ExpectBQ().
			Add(terminationEpoch, 1, 3, 5, 7).
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
		newFaults, _, _, err := partition.DeclareFaults(store, sectorArr, terminations, abi.ChainEpoch(5), sectorSize, quantSpec)
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
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(2), sectorSize, quantSpec)
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
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf(1, 2, 4), bf())

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

		_, _, _, err := partition.DeclareFaults(store, sectorArr, bf(5), abi.ChainEpoch(2), sectorSize, quantSpec)
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

	t.Run("pop expiring sectors errors if a unproven sectors exist", func(t *testing.T) {
		store, partition := setupUnproven(t)

		// pop first expiration set
		expireEpoch := abi.ChainEpoch(5)
		_, err := partition.PopExpiredSectors(store, expireEpoch, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot pop expired sectors from a partition with unproven sectors")
	})

	t.Run("records missing PoSt", func(t *testing.T) {
		store, partition := setup(t)

		unprovenSector := testSector(13, 7, 55, 65, 1006)
		allSectors := append(sectors[:len(sectors):len(sectors)], unprovenSector)
		sectorArr := sectorsArr(t, store, allSectors)

		// Add an unproven sector.
		powerDelta, err := partition.AddSectors(
			store, false,
			[]*miner.SectorOnChainInfo{unprovenSector},
			sectorSize, quantSpec,
		)
		require.NoError(t, err)
		require.True(t, powerDelta.IsZero()) // no power for unproven sectors.

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, _, err = partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		// record entire partition as faulted
		powerDelta, penalizedPower, newFaultyPower, err := partition.RecordMissedPost(store, abi.ChainEpoch(6), quantSpec)
		require.NoError(t, err)

		expectedNewFaultPower := miner.PowerForSectors(sectorSize, append(allSectors[:3:3], allSectors[6]))
		assert.True(t, expectedNewFaultPower.Equals(newFaultyPower))

		// 6 has always been faulty, so we shouldn't be penalized for it (except ongoing).
		expectedPenalizedPower := miner.PowerForSectors(sectorSize, allSectors).
			Sub(miner.PowerForSector(sectorSize, allSectors[5]))
		assert.True(t, expectedPenalizedPower.Equals(penalizedPower))

		// We should lose power for sectors 1-3.
		expectedPowerDelta := miner.PowerForSectors(sectorSize, allSectors[:3]).Neg()
		assert.True(t, expectedPowerDelta.Equals(powerDelta))

		// everything is now faulty
		assertPartitionState(t, store, partition, quantSpec, sectorSize, allSectors, bf(1, 2, 3, 4, 5, 6, 7), bf(1, 2, 3, 4, 5, 6, 7), bf(), bf(), bf())

		// everything not in first expiration group is now in second because fault expiration quantized to 9
		assertPartitionExpirationQueue(t, store, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4, 5, 6, 7)},
		})
	})

	t.Run("pops early terminations", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// fault sector 3, 4, 5 and 6
		faultSet := bf(3, 4, 5, 6)
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
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

		proofType := abi.RegisteredSealProof_StackedDrg32GiBV1_1
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

		power, err := partition.AddSectors(store, false, manySectors, sectorSize, miner.NoQuantization)
		require.NoError(t, err)

		assert.True(t, power.IsZero()) // not activated

		assertPartitionState(
			t, store, partition,
			miner.NoQuantization, sectorSize, manySectors,
			sectorNos, bf(), bf(), bf(), sectorNos,
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
			sectorNos, bf(), bf(), bf(), sectorNos,
		)

	})
}

func TestRecordSkippedFaults(t *testing.T) {
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
	exp := abi.ChainEpoch(100)

	setup := func(t *testing.T) (adt.Store, *miner.Partition) {
		store := ipld.NewADTStore(context.Background())
		partition := emptyPartition(t, store)

		power, err := partition.AddSectors(store, true, sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))

		return store, partition
	}

	t.Run("fail if ALL declared sectors are NOT in the partition", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		skipped := bitfield.NewFromSet([]uint64{1, 100})

		powerDelta, newFaulty, retractedRecovery, newFaults, err := partition.RecordSkippedFaults(
			store, sectorArr, sectorSize, quantSpec, exp, skipped,
		)
		require.Error(t, err)
		require.EqualValues(t, exitcode.ErrIllegalArgument, exitcode.Unwrap(err, exitcode.Ok))
		require.EqualValues(t, miner.NewPowerPairZero(), newFaulty)
		require.EqualValues(t, miner.NewPowerPairZero(), retractedRecovery)
		require.EqualValues(t, miner.NewPowerPairZero(), powerDelta)
		require.False(t, newFaults)
	})

	t.Run("already faulty and terminated sectors are ignored", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// terminate 1 AND 2
		terminations := bf(1, 2)
		terminationEpoch := abi.ChainEpoch(3)
		_, err := partition.TerminateSectors(store, sectorArr, terminationEpoch, terminations, sectorSize, quantSpec)
		require.NoError(t, err)
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(), bf(), terminations, bf())

		// declare 4 & 5 as faulty
		faultSet := bf(4, 5)
		_, _, _, err = partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)
		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), faultSet, bf(), terminations, bf())

		// record skipped faults such that some of them are already faulty/terminated
		skipped := bitfield.NewFromSet([]uint64{1, 2, 3, 4, 5})
		powerDelta, newFaultPower, retractedPower, newFaults, err := partition.RecordSkippedFaults(store, sectorArr, sectorSize, quantSpec, exp, skipped)
		require.NoError(t, err)
		require.EqualValues(t, miner.NewPowerPairZero(), retractedPower)
		expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(3)))
		require.EqualValues(t, expectedFaultyPower, newFaultPower)
		require.EqualValues(t, powerDelta, newFaultPower.Neg())
		require.True(t, newFaults)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(3, 4, 5), bf(), bf(1, 2), bf())
	})

	t.Run("recoveries are retracted without being marked as new faulty power", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		// make 4, 5 and 6 faulty
		faultSet := bf(4, 5, 6)
		_, _, _, err := partition.DeclareFaults(store, sectorArr, faultSet, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bf(4, 5)
		err = partition.DeclareFaultsRecovered(sectorArr, sectorSize, recoverSet)
		require.NoError(t, err)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5), bf(), bf())

		// record skipped faults such that some of them have been marked as recovered
		skipped := bitfield.NewFromSet([]uint64{1, 4, 5})
		powerDelta, newFaultPower, recoveryPower, newFaults, err := partition.RecordSkippedFaults(store, sectorArr, sectorSize, quantSpec, exp, skipped)
		require.NoError(t, err)
		require.True(t, newFaults)

		// only 1 is marked for fault power as 4 & 5 are recovering
		expectedFaultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(1)))
		require.EqualValues(t, expectedFaultyPower, newFaultPower)
		require.EqualValues(t, expectedFaultyPower.Neg(), powerDelta)

		// 4 & 5 are marked for recovery power
		expectedRecoveryPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(4, 5)))
		require.EqualValues(t, expectedRecoveryPower, recoveryPower)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(1, 4, 5, 6), bf(), bf(), bf())
	})

	t.Run("successful when skipped fault set is empty", func(t *testing.T) {
		store, partition := setup(t)
		sectorArr := sectorsArr(t, store, sectors)

		powerDelta, newFaultPower, recoveryPower, newFaults, err := partition.RecordSkippedFaults(store, sectorArr, sectorSize, quantSpec, exp, bf())
		require.NoError(t, err)
		require.EqualValues(t, miner.NewPowerPairZero(), newFaultPower)
		require.EqualValues(t, miner.NewPowerPairZero(), recoveryPower)
		require.EqualValues(t, miner.NewPowerPairZero(), powerDelta)
		require.False(t, newFaults)

		assertPartitionState(t, store, partition, quantSpec, sectorSize, sectors, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf(), bf())
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

func assertPartitionState(t *testing.T,
	store adt.Store,
	partition *miner.Partition,
	quant miner.QuantSpec,
	sectorSize abi.SectorSize,
	sectors []*miner.SectorOnChainInfo,
	allSectorIds bitfield.BitField,
	faults bitfield.BitField,
	recovering bitfield.BitField,
	terminations bitfield.BitField,
	unproven bitfield.BitField,
) {

	assertBitfieldsEqual(t, faults, partition.Faults)
	assertBitfieldsEqual(t, recovering, partition.Recoveries)
	assertBitfieldsEqual(t, terminations, partition.Terminated)
	assertBitfieldsEqual(t, unproven, partition.Unproven)
	assertBitfieldsEqual(t, allSectorIds, partition.Sectors)

	msgs := &builtin.MessageAccumulator{}
	_ = miner.CheckPartitionStateInvariants(partition, store, quant, sectorSize, sectorsAsMap(sectors), msgs)
	assert.True(t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))
}

func bf(secNos ...uint64) bitfield.BitField {
	return bitfield.NewFromSet(secNos)
}

func selectSectors(t *testing.T, sectors []*miner.SectorOnChainInfo, field bitfield.BitField) []*miner.SectorOnChainInfo {
	toInclude, err := field.AllMap(miner.AddressedSectorsMax)
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
	toReschedule, err := filter.AllMap(miner.AddressedSectorsMax)
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

func sectorsAsMap(sectors []*miner.SectorOnChainInfo) map[abi.SectorNumber]*miner.SectorOnChainInfo {
	m := map[abi.SectorNumber]*miner.SectorOnChainInfo{}
	for _, s := range sectors {
		m[s.SectorNumber] = s
	}
	return m
}
