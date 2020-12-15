package miner_test

import (
	"context"
	"strings"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v4/actors/builtin"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v4/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v4/support/ipld"
)

func TestDeadlines(t *testing.T) {
	sectors := []*miner.SectorOnChainInfo{
		testSector(2, 1, 50, 60, 1000),
		testSector(3, 2, 51, 61, 1001),
		testSector(7, 3, 52, 62, 1002),
		testSector(8, 4, 53, 63, 1003),

		testSector(8, 5, 54, 64, 1004),
		testSector(11, 6, 55, 65, 1005),
		testSector(13, 7, 56, 66, 1006),
		testSector(8, 8, 57, 67, 1007),

		testSector(8, 9, 58, 68, 1008),
	}
	extraSectors := []*miner.SectorOnChainInfo{
		testSector(8, 10, 58, 68, 1008),
	}
	allSectors := append(sectors, extraSectors...)

	sectorSize := abi.SectorSize(32 << 30)
	quantSpec := miner.NewQuantSpec(4, 1)
	partitionSize := uint64(4)

	dlState := expectedDeadlineState{
		quant:         quantSpec,
		partitionSize: partitionSize,
		sectorSize:    sectorSize,
		sectors:       allSectors,
	}

	sectorPower := func(t *testing.T, sectorNos ...uint64) miner.PowerPair {
		return miner.PowerForSectors(sectorSize, selectSectors(t, allSectors, bf(sectorNos...)))
	}

	//
	// Define some basic test scenarios that build one each other.
	//

	// Adds sectors, and proves them if requested.
	//
	// Partition 1: sectors 1, 2, 3, 4
	// Partition 2: sectors 5, 6, 7, 8
	// Partition 3: sectors 9
	addSectors := func(t *testing.T, store adt.Store, dl *miner.Deadline, prove bool) {
		power := miner.PowerForSectors(sectorSize, sectors)
		activatedPower, err := dl.AddSectors(store, partitionSize, false, sectors, sectorSize, quantSpec)
		require.NoError(t, err)
		assert.True(t, activatedPower.Equals(power))

		dlState.withUnproven(1, 2, 3, 4, 5, 6, 7, 8, 9).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)

		if !prove {
			return
		}

		sectorArr := sectorsArr(t, store, sectors)

		// Prove everything
		result, err := dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 0, []miner.PoStPartition{{Index: 0}, {Index: 1}, {Index: 2}})
		require.NoError(t, err)
		require.True(t, result.PowerDelta.Equals(power))

		faultyPower, recoveryPower, err := dl.ProcessDeadlineEnd(store, quantSpec, 0)
		require.NoError(t, err)
		require.True(t, faultyPower.IsZero())
		require.True(t, recoveryPower.IsZero())

		dlState.withPartitions(
			bf(1, 2, 3, 4),
			bf(5, 6, 7, 8),
			bf(9),
		).assert(t, store, dl)
	}

	// Adds sectors according to addSectors, then terminates them:
	//
	// From partition 0: sectors 1 & 3
	// From partition 1: sectors 6
	addThenTerminate := func(t *testing.T, store adt.Store, dl *miner.Deadline, proveFirst bool) {
		addSectors(t, store, dl, proveFirst)

		removedPower, err := dl.TerminateSectors(store, sectorsArr(t, store, sectors), 15, miner.PartitionSectorMap{
			0: bf(1, 3),
			1: bf(6),
		}, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.NewPowerPairZero()
		unproven := []uint64{2, 4, 5, 7, 8, 9} // not 1, 3, 6
		if proveFirst {
			unproven = nil
			expectedPower = sectorPower(t, 1, 3, 6)
		}
		require.True(t, expectedPower.Equals(removedPower), "dlState to remove power for terminated sectors")

		dlState.withTerminations(1, 3, 6).
			withUnproven(unproven...).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	}

	// Adds and terminates sectors according to the previous two functions,
	// then pops early terminations.
	addThenTerminateThenPopEarly := func(t *testing.T, store adt.Store, dl *miner.Deadline) {
		addThenTerminate(t, store, dl, true)

		earlyTerminations, more, err := dl.PopEarlyTerminations(store, 100, 100)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Equal(t, uint64(2), earlyTerminations.PartitionsProcessed)
		assert.Equal(t, uint64(3), earlyTerminations.SectorsProcessed)
		assert.Len(t, earlyTerminations.Sectors, 1)
		assertBitfieldEquals(t, earlyTerminations.Sectors[15], 1, 3, 6)

		// Popping early terminations doesn't affect the terminations bitfield.
		dlState.withTerminations(1, 3, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	}

	// Runs the above scenarios, then removes partition 0.
	addThenTerminateThenRemovePartition := func(t *testing.T, store adt.Store, dl *miner.Deadline) {
		addThenTerminateThenPopEarly(t, store, dl)

		live, dead, removedPower, err := dl.RemovePartitions(store, bf(0), quantSpec)
		require.NoError(t, err, "should have removed partitions")
		assertBitfieldEquals(t, live, 2, 4)
		assertBitfieldEquals(t, dead, 1, 3)
		livePower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, live))
		require.True(t, livePower.Equals(removedPower))

		dlState.withTerminations(6).
			withPartitions(
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	}

	// Adds sectors according to addSectors, then marks sectors 1, 5, 6
	// faulty, expiring at epoch 9.
	//
	// Sector 5 will expire on-time at epoch 9 while 6 will expire early at epoch 9.
	addThenMarkFaulty := func(t *testing.T, store adt.Store, dl *miner.Deadline, proveFirst bool) {
		addSectors(t, store, dl, proveFirst)

		// Mark faulty.
		powerDelta, err := dl.RecordFaults(
			store, sectorsArr(t, store, sectors), sectorSize, quantSpec, 9,
			map[uint64]bitfield.BitField{
				0: bf(1),
				1: bf(5, 6),
			},
		)
		require.NoError(t, err)

		expectedPower := miner.NewPowerPairZero()
		unproven := []uint64{2, 3, 4, 7, 8, 9} // not 1, 5, 6
		if proveFirst {
			unproven = nil
			expectedPower = sectorPower(t, 1, 5, 6)
		}
		assert.True(t, powerDelta.Equals(expectedPower.Neg()))

		dlState.withFaults(1, 5, 6).
			withUnproven(unproven...).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	}

	// Test the basic scenarios (technically, we could just run the final one).

	t.Run("adds sectors", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addSectors(t, store, dl, false)
	})

	t.Run("adds sectors and proves", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addSectors(t, store, dl, true)
	})

	t.Run("terminates sectors", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addThenTerminate(t, store, dl, true)
	})

	t.Run("terminates unproven sectors", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addThenTerminate(t, store, dl, false)
	})

	t.Run("pops early terminations", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenTerminateThenPopEarly(t, store, dl)
	})

	t.Run("removes partitions", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenTerminateThenRemovePartition(t, store, dl)
	})

	t.Run("marks faulty", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenMarkFaulty(t, store, dl, true)
	})

	t.Run("marks unproven sectors faulty", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenMarkFaulty(t, store, dl, false)
	})

	//
	// Now, build on these basic scenarios with some "what ifs".
	//

	t.Run("cannot remove partitions with early terminations", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addThenTerminate(t, store, dl, false)

		_, _, _, err := dl.RemovePartitions(store, bf(0), quantSpec)
		require.Error(t, err, "should have failed to remove a partition with early terminations")
	})

	t.Run("can pop early terminations in multiple steps", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addThenTerminate(t, store, dl, true)

		var result miner.TerminationResult

		// process 1 sector, 2 partitions (should pop 1 sector)
		result1, hasMore, err := dl.PopEarlyTerminations(store, 2, 1)
		require.NoError(t, err)
		require.True(t, hasMore)
		require.NoError(t, result.Add(result1))

		// process 2 sectors, 1 partitions (should pop 1 sector)
		result2, hasMore, err := dl.PopEarlyTerminations(store, 2, 1)
		require.NoError(t, err)
		require.True(t, hasMore)
		require.NoError(t, result.Add(result2))

		// process 1 sectors, 1 partitions (should pop 1 sector)
		result3, hasMore, err := dl.PopEarlyTerminations(store, 1, 1)
		require.NoError(t, err)
		require.False(t, hasMore)
		require.NoError(t, result.Add(result3))

		assert.Equal(t, uint64(3), result.PartitionsProcessed)
		assert.Equal(t, uint64(3), result.SectorsProcessed)
		assert.Len(t, result.Sectors, 1)
		assertBitfieldEquals(t, result.Sectors[15], 1, 3, 6)

		// Popping early terminations doesn't affect the terminations bitfield.
		dlState.withTerminations(1, 3, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	})

	t.Run("cannot remove missing partition", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenTerminateThenRemovePartition(t, store, dl)

		_, _, _, err := dl.RemovePartitions(store, bf(2), quantSpec)
		require.Error(t, err, "should have failed to remove missing partition")
	})

	t.Run("removing no partitions does nothing", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenTerminateThenPopEarly(t, store, dl)

		live, dead, removedPower, err := dl.RemovePartitions(store, bf(), quantSpec)
		require.NoError(t, err, "should not have failed to remove no partitions")
		require.True(t, removedPower.IsZero())
		assertBitfieldEquals(t, live)
		assertBitfieldEquals(t, dead)

		// Popping early terminations doesn't affect the terminations bitfield.
		dlState.withTerminations(1, 3, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	})

	t.Run("fails to remove partitions with faulty sectors", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		addThenMarkFaulty(t, store, dl, false)

		// Try to remove a partition with faulty sectors.
		_, _, _, err := dl.RemovePartitions(store, bf(1), quantSpec)
		require.Error(t, err, "should have failed to remove a partition with faults")
	})

	t.Run("terminate proven & faulty", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenMarkFaulty(t, store, dl, true) // 1, 5, 6 faulty

		sectorArr := sectorsArr(t, store, sectors)
		removedPower, err := dl.TerminateSectors(store, sectorArr, 15, miner.PartitionSectorMap{
			0: bf(1, 3),
			1: bf(6),
		}, sectorSize, quantSpec)
		require.NoError(t, err)

		// Sector 3 active, 1, 6 faulty
		expectedPowerLoss := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(3)))
		require.True(t, expectedPowerLoss.Equals(removedPower), "dlState to remove power for terminated sectors")

		dlState.withTerminations(1, 3, 6).
			withFaults(5).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	})

	t.Run("terminate unproven & faulty", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenMarkFaulty(t, store, dl, false) // 1, 5, 6 faulty

		sectorArr := sectorsArr(t, store, sectors)
		removedPower, err := dl.TerminateSectors(store, sectorArr, 15, miner.PartitionSectorMap{
			0: bf(1, 3),
			1: bf(6),
		}, sectorSize, quantSpec)
		require.NoError(t, err)

		// Sector 3 unproven, 1, 6 faulty
		require.True(t, removedPower.Equals(miner.NewPowerPairZero()), "should remove no power")

		dlState.withTerminations(1, 3, 6).
			withUnproven(2, 4, 7, 8, 9). // not 1, 3, 5, & 6
			withFaults(5).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	})

	t.Run("fails to terminate missing sector", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenMarkFaulty(t, store, dl, false) // 1, 5, 6 faulty

		sectorArr := sectorsArr(t, store, sectors)
		_, err := dl.TerminateSectors(store, sectorArr, 15, miner.PartitionSectorMap{
			0: bf(6),
		}, sectorSize, quantSpec)
		require.Error(t, err)
		require.Contains(t, err.Error(), "can only terminate live sectors")
	})

	t.Run("fails to terminate missing partition", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenMarkFaulty(t, store, dl, false) // 1, 5, 6 faulty

		sectorArr := sectorsArr(t, store, sectors)
		_, err := dl.TerminateSectors(store, sectorArr, 15, miner.PartitionSectorMap{
			4: bf(6),
		}, sectorSize, quantSpec)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to find partition 4")
	})

	t.Run("fails to terminate already terminated sector", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenTerminate(t, store, dl, false) // terminates 1, 3, & 6

		sectorArr := sectorsArr(t, store, sectors)
		_, err := dl.TerminateSectors(store, sectorArr, 15, miner.PartitionSectorMap{
			0: bf(1, 2),
		}, sectorSize, quantSpec)
		require.Error(t, err)
		require.Contains(t, err.Error(), "can only terminate live sectors")
	})

	t.Run("faulty sectors expire", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		// Mark sectors 5 & 6 faulty, expiring at epoch 9.
		addThenMarkFaulty(t, store, dl, true)

		// We expect all sectors but 7 to have expired at this point.
		exp, err := dl.PopExpiredSectors(store, 9, quantSpec)
		require.NoError(t, err)

		onTimeExpected := bf(1, 2, 3, 4, 5, 8, 9)
		earlyExpected := bf(6)

		assertBitfieldsEqual(t, onTimeExpected, exp.OnTimeSectors)
		assertBitfieldsEqual(t, earlyExpected, exp.EarlySectors)

		dlState.withTerminations(1, 2, 3, 4, 5, 6, 8, 9).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)

		// Check early terminations.
		earlyTerminations, more, err := dl.PopEarlyTerminations(store, 100, 100)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Equal(t, uint64(1), earlyTerminations.PartitionsProcessed)
		assert.Equal(t, uint64(1), earlyTerminations.SectorsProcessed)
		assert.Len(t, earlyTerminations.Sectors, 1)
		assertBitfieldEquals(t, earlyTerminations.Sectors[9], 6)

		// Popping early terminations doesn't affect the terminations bitfield.
		dlState.withTerminations(1, 2, 3, 4, 5, 6, 8, 9).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	})

	t.Run("cannot pop expired sectors before proving", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		// Add sectors, but don't prove.
		addSectors(t, store, dl, false)

		// Try to pop some expirations.
		_, err := dl.PopExpiredSectors(store, 9, quantSpec)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot pop expired sectors from a partition with unproven sectors")
	})

	t.Run("post all the things", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		addSectors(t, store, dl, true)

		// add an inactive sector
		power, err := dl.AddSectors(store, partitionSize, false, extraSectors, sectorSize, quantSpec)
		require.NoError(t, err)
		expectedPower := miner.PowerForSectors(sectorSize, extraSectors)
		assert.True(t, expectedPower.Equals(power))

		sectorArr := sectorsArr(t, store, allSectors)

		postResult1, err := dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 0, Skipped: bf()},
			{Index: 1, Skipped: bf()},
		})
		require.NoError(t, err)
		assertBitfieldEquals(t, postResult1.Sectors, 1, 2, 3, 4, 5, 6, 7, 8)
		assertEmptyBitfield(t, postResult1.IgnoredSectors)
		require.True(t, postResult1.NewFaultyPower.Equals(miner.NewPowerPairZero()))
		require.True(t, postResult1.RetractedRecoveryPower.Equals(miner.NewPowerPairZero()))
		require.True(t, postResult1.RecoveredPower.Equals(miner.NewPowerPairZero()))

		// First two partitions posted
		dlState.withPosts(0, 1).
			withUnproven(10).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9, 10),
			).assert(t, store, dl)

		postResult2, err := dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 2, Skipped: bf()},
		})
		require.NoError(t, err)
		assertBitfieldEquals(t, postResult2.Sectors, 9, 10)
		assertEmptyBitfield(t, postResult2.IgnoredSectors)
		require.True(t, postResult2.NewFaultyPower.Equals(miner.NewPowerPairZero()))
		require.True(t, postResult2.RetractedRecoveryPower.Equals(miner.NewPowerPairZero()))
		require.True(t, postResult2.RecoveredPower.Equals(miner.NewPowerPairZero()))
		// activate sector 10
		require.True(t, postResult2.PowerDelta.Equals(sectorPower(t, 10)))

		// All 3 partitions posted, unproven sector 10 proven and power activated.
		dlState.withPosts(0, 1, 2).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9, 10),
			).assert(t, store, dl)

		powerDelta, penalizedPower, err := dl.ProcessDeadlineEnd(store, quantSpec, 13)
		require.NoError(t, err)

		// No power delta for successful post.
		require.True(t, powerDelta.IsZero())
		require.True(t, penalizedPower.IsZero())

		// Everything back to normal.
		dlState.withPartitions(
			bf(1, 2, 3, 4),
			bf(5, 6, 7, 8),
			bf(9, 10),
		).assert(t, store, dl)
	})

	t.Run("post with unproven, faults, recoveries, and retracted recoveries", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		// Marks sectors 1 (partition 0), 5 & 6 (partition 1) as faulty.
		addThenMarkFaulty(t, store, dl, true)

		// add an inactive sector
		power, err := dl.AddSectors(store, partitionSize, false, extraSectors, sectorSize, quantSpec)
		require.NoError(t, err)
		expectedPower := miner.PowerForSectors(sectorSize, extraSectors)
		assert.True(t, expectedPower.Equals(power))

		sectorArr := sectorsArr(t, store, allSectors)

		// Declare sectors 1 & 6 recovered.
		require.NoError(t, dl.DeclareFaultsRecovered(store, sectorArr, sectorSize, map[uint64]bitfield.BitField{
			0: bf(1),
			1: bf(6),
		}))

		// We're now recovering 1 & 6.
		dlState.withRecovering(1, 6).
			withFaults(1, 5, 6).
			withUnproven(10).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9, 10),
			).assert(t, store, dl)

		// Prove partitions 0 & 1, skipping sectors 1 & 7.
		postResult, err := dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 0, Skipped: bf(1)},
			{Index: 1, Skipped: bf(7)},
		})

		require.NoError(t, err)
		// 1, 5, and 7 are expected to be faulty.
		// - 1 should have recovered but didn't (retracted)
		// - 5 was already marked faulty.
		// - 7 is newly faulty.
		// - 6 has recovered.
		assertBitfieldEquals(t, postResult.Sectors, 1, 2, 3, 4, 5, 6, 7, 8)
		assertBitfieldEquals(t, postResult.IgnoredSectors, 1, 5, 7)
		// sector 7 is newly faulty
		require.True(t, postResult.NewFaultyPower.Equals(sectorPower(t, 7)))
		// we failed to recover 1 (retracted)
		require.True(t, postResult.RetractedRecoveryPower.Equals(sectorPower(t, 1)))
		// we recovered 6
		require.True(t, postResult.RecoveredPower.Equals(sectorPower(t, 6)))
		// no power delta from these deadlines.
		require.True(t, postResult.PowerDelta.IsZero())

		// First two partitions should be posted.
		dlState.withPosts(0, 1).
			withFaults(1, 5, 7).
			withUnproven(10).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9, 10),
			).assert(t, store, dl)

		powerDelta, penalizedPower, err := dl.ProcessDeadlineEnd(store, quantSpec, 13)
		require.NoError(t, err)

		expFaultPower := sectorPower(t, 9, 10)
		expPowerDelta := sectorPower(t, 9).Neg()

		// Sector 9 wasn't proven.
		require.True(t, powerDelta.Equals(expPowerDelta))
		// No new changes to recovering power.
		require.True(t, penalizedPower.Equals(expFaultPower))

		// Posts taken care of.
		// Unproven now faulty.
		dlState.withFaults(1, 5, 7, 9, 10).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9, 10),
			).assert(t, store, dl)
	})

	t.Run("post with skipped unproven", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		addSectors(t, store, dl, true)

		// add an inactive sector
		power, err := dl.AddSectors(store, partitionSize, false, extraSectors, sectorSize, quantSpec)
		require.NoError(t, err)
		expectedPower := miner.PowerForSectors(sectorSize, extraSectors)
		assert.True(t, expectedPower.Equals(power))

		sectorArr := sectorsArr(t, store, allSectors)

		postResult1, err := dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 0, Skipped: bf()},
			{Index: 1, Skipped: bf()},
			{Index: 2, Skipped: bf(10)},
		})

		require.NoError(t, err)
		assertBitfieldEquals(t, postResult1.Sectors, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		assertBitfieldEquals(t, postResult1.IgnoredSectors, 10)
		require.True(t, postResult1.NewFaultyPower.Equals(sectorPower(t, 10)))
		require.True(t, postResult1.PowerDelta.IsZero()) // not proven yet.
		require.True(t, postResult1.RetractedRecoveryPower.IsZero())
		require.True(t, postResult1.RecoveredPower.IsZero())

		// All posted
		dlState.withPosts(0, 1, 2).
			withFaults(10).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9, 10),
			).assert(t, store, dl)

		powerDelta, penalizedPower, err := dl.ProcessDeadlineEnd(store, quantSpec, 13)
		require.NoError(t, err)

		// All posts submitted, no power delta, no extra penalties.
		require.True(t, powerDelta.IsZero())
		// Penalize for skipped sector 10.
		require.True(t, penalizedPower.IsZero())

		// Everything back to normal, except that we have a fault.
		dlState.withFaults(10).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9, 10),
			).assert(t, store, dl)
	})

	t.Run("post missing partition", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		addSectors(t, store, dl, true)

		// add an inactive sector
		power, err := dl.AddSectors(store, partitionSize, false, extraSectors, sectorSize, quantSpec)
		require.NoError(t, err)
		expectedPower := miner.PowerForSectors(sectorSize, extraSectors)
		assert.True(t, expectedPower.Equals(power))

		sectorArr := sectorsArr(t, store, allSectors)

		_, err = dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 0, Skipped: bf()},
			{Index: 3, Skipped: bf()},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such partition")
	})

	t.Run("post partition twice", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		addSectors(t, store, dl, true)

		// add an inactive sector
		power, err := dl.AddSectors(store, partitionSize, false, extraSectors, sectorSize, quantSpec)
		require.NoError(t, err)
		expectedPower := miner.PowerForSectors(sectorSize, extraSectors)
		assert.True(t, expectedPower.Equals(power))

		sectorArr := sectorsArr(t, store, allSectors)

		_, err = dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 0, Skipped: bf()},
			{Index: 0, Skipped: bf()},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate partitions proven")
	})

	t.Run("retract recoveries", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		// Marks sectors 1 (partition 0), 5 & 6 (partition 1) as faulty.
		addThenMarkFaulty(t, store, dl, true)

		sectorArr := sectorsArr(t, store, sectors)

		// Declare sectors 1 & 6 recovered.
		require.NoError(t, dl.DeclareFaultsRecovered(store, sectorArr, sectorSize, map[uint64]bitfield.BitField{
			0: bf(1),
			1: bf(6),
		}))

		// Retract recovery for sector 1.
		powerDelta, err := dl.RecordFaults(store, sectorArr, sectorSize, quantSpec, 13, map[uint64]bitfield.BitField{
			0: bf(1),
		})

		// We're just retracting a recovery, this doesn't count as a new fault.
		require.NoError(t, err)
		require.True(t, powerDelta.Equals(miner.NewPowerPairZero()))

		// We're now recovering 6.
		dlState.withRecovering(6).
			withFaults(1, 5, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)

		// Prove all partitions.
		postResult, err := dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 0, Skipped: bf()},
			{Index: 1, Skipped: bf()},
			{Index: 2, Skipped: bf()},
		})

		require.NoError(t, err)
		// 1 & 5 are still faulty
		assertBitfieldEquals(t, postResult.Sectors, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		assertBitfieldEquals(t, postResult.IgnoredSectors, 1, 5)
		// All faults were declared.
		require.True(t, postResult.NewFaultyPower.Equals(miner.NewPowerPairZero()))
		// we didn't fail to recover anything.
		require.True(t, postResult.RetractedRecoveryPower.Equals(miner.NewPowerPairZero()))
		// we recovered 6.
		require.True(t, postResult.RecoveredPower.Equals(sectorPower(t, 6)))

		// First two partitions should be posted.
		dlState.withPosts(0, 1, 2).
			withFaults(1, 5).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)

		newFaultyPower, failedRecoveryPower, err := dl.ProcessDeadlineEnd(store, quantSpec, 13)
		require.NoError(t, err)

		// No power changes.
		require.True(t, newFaultyPower.Equals(miner.NewPowerPairZero()))
		require.True(t, failedRecoveryPower.Equals(miner.NewPowerPairZero()))

		// Posts taken care of.
		dlState.withFaults(1, 5).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	})

	t.Run("reschedule expirations", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		sectorArr := sectorsArr(t, store, sectors)

		// Marks sectors 1 (partition 0), 5 & 6 (partition 1) as faulty.
		addThenMarkFaulty(t, store, dl, true)

		// Try to reschedule two sectors, only the 7 (non faulty) should succeed.
		replaced, err := dl.RescheduleSectorExpirations(store, sectorArr, 1, miner.PartitionSectorMap{
			1: bf(6, 7, 99), // 99 should be skipped, it doesn't exist.
			5: bf(100),      // partition 5 doesn't exist.
			2: bf(),         // empty bitfield should be fine.
		}, sectorSize, quantSpec)
		require.NoError(t, err)

		assert.Len(t, replaced, 1)

		exp, err := dl.PopExpiredSectors(store, 1, quantSpec)
		require.NoError(t, err)

		sector7 := selectSectors(t, sectors, bf(7))[0]

		dlState.withFaults(1, 5, 6).
			withTerminations(7).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
		assertBitfieldEmpty(t, exp.EarlySectors)
		assertBitfieldEquals(t, exp.OnTimeSectors, 7)
		assert.True(t, exp.ActivePower.Equals(miner.PowerForSector(sectorSize, sector7)))
		assert.True(t, exp.FaultyPower.IsZero())
		assert.True(t, exp.OnTimePledge.Equals(sector7.InitialPledge))
	})

	t.Run("cannot declare faults in missing partitions", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addSectors(t, store, dl, true)
		sectorArr := sectorsArr(t, store, allSectors)

		// Declare sectors 1 & 6 faulty.
		_, err := dl.RecordFaults(store, sectorArr, sectorSize, quantSpec, 17, map[uint64]bitfield.BitField{
			0: bf(1),
			4: bf(6),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such partition 4")
	})

	t.Run("cannot declare faults recovered in missing partitions", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		// Marks sectors 1 (partition 0), 5 & 6 (partition 1) as faulty.
		addThenMarkFaulty(t, store, dl, true)
		sectorArr := sectorsArr(t, store, allSectors)

		// Declare sectors 1 & 6 faulty.
		err := dl.DeclareFaultsRecovered(store, sectorArr, sectorSize, map[uint64]bitfield.BitField{
			0: bf(1),
			4: bf(6),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such partition 4")
	})
}

func emptyDeadline(t *testing.T, store adt.Store) *miner.Deadline {
	dl, err := miner.ConstructDeadline(store)
	require.NoError(t, err)
	return dl
}

// Helper type for validating deadline state.
//
// All methods take and the state by _value_ so one can (and should) construct a
// sane base-state.
type expectedDeadlineState struct {
	quant         miner.QuantSpec
	sectorSize    abi.SectorSize
	partitionSize uint64
	sectors       []*miner.SectorOnChainInfo

	faults       bitfield.BitField
	recovering   bitfield.BitField
	terminations bitfield.BitField
	unproven     bitfield.BitField
	posts        bitfield.BitField

	partitionSectors []bitfield.BitField
}

//nolint:unused
func (s expectedDeadlineState) withQuantSpec(quant miner.QuantSpec) expectedDeadlineState {
	s.quant = quant
	return s
}

//nolint:unused
func (s expectedDeadlineState) withFaults(faults ...uint64) expectedDeadlineState {
	s.faults = bf(faults...)
	return s
}

//nolint:unused
func (s expectedDeadlineState) withRecovering(recovering ...uint64) expectedDeadlineState {
	s.recovering = bf(recovering...)
	return s
}

//nolint:unused
func (s expectedDeadlineState) withTerminations(terminations ...uint64) expectedDeadlineState {
	s.terminations = bf(terminations...)
	return s
}

//nolint:unused
func (s expectedDeadlineState) withUnproven(unproven ...uint64) expectedDeadlineState {
	s.unproven = bf(unproven...)
	return s
}

//nolint:unused
func (s expectedDeadlineState) withPosts(posts ...uint64) expectedDeadlineState {
	s.posts = bf(posts...)
	return s
}

//nolint:unused
func (s expectedDeadlineState) withPartitions(partitions ...bitfield.BitField) expectedDeadlineState {
	s.partitionSectors = partitions
	return s
}

// Assert that the deadline's state matches the expected state.
func (s expectedDeadlineState) assert(t *testing.T, store adt.Store, dl *miner.Deadline) {
	_, faults, recoveries, terminations, unproven := checkDeadlineInvariants(
		t, store, dl, s.quant, s.sectorSize, s.sectors,
	)

	assertBitfieldsEqual(t, s.faults, faults)
	assertBitfieldsEqual(t, s.recovering, recoveries)
	assertBitfieldsEqual(t, s.terminations, terminations)
	assertBitfieldsEqual(t, s.unproven, unproven)
	assertBitfieldsEqual(t, s.posts, dl.PartitionsPoSted)

	partitions, err := dl.PartitionsArray(store)
	require.NoError(t, err)
	require.Equal(t, uint64(len(s.partitionSectors)), partitions.Length(), "unexpected number of partitions")
	for i, partSectors := range s.partitionSectors {
		var partition miner.Partition
		found, err := partitions.Get(uint64(i), &partition)
		require.NoError(t, err)
		require.True(t, found)
		assertBitfieldsEqual(t, partSectors, partition.Sectors)
	}
}

// check the deadline's invariants, returning all contained sectors, faults,
// recoveries, terminations, and partition/sector assignments.
func checkDeadlineInvariants(
	t *testing.T, store adt.Store, dl *miner.Deadline,
	quant miner.QuantSpec, ssize abi.SectorSize,
	sectors []*miner.SectorOnChainInfo,
) (
	allSectors bitfield.BitField,
	allFaults bitfield.BitField,
	allRecoveries bitfield.BitField,
	allTerminations bitfield.BitField,
	allUnproven bitfield.BitField,
) {
	msgs := &builtin.MessageAccumulator{}
	summary := miner.CheckDeadlineStateInvariants(dl, store, quant, ssize, sectorsAsMap(sectors), msgs)
	assert.True(t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))

	allSectors = summary.AllSectors
	allFaults = summary.FaultySectors
	allRecoveries = summary.RecoveringSectors
	allTerminations = summary.TerminatedSectors
	allUnproven = summary.UnprovenSectors
	return
}
