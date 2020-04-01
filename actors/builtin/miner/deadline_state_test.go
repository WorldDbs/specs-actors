package miner_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/ipld"
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

	sectorSize := abi.SectorSize(32 << 30)
	quantSpec := miner.NewQuantSpec(4, 1)
	partitionSize := uint64(4)

	dlState := expectedDeadlineState{
		quant:         quantSpec,
		partitionSize: partitionSize,
		sectorSize:    sectorSize,
		sectors:       sectors,
	}

	sectorPower := func(t *testing.T, sectorNos ...uint64) miner.PowerPair {
		return miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(sectorNos...)))
	}

	//
	// Define some basic test scenarios that build one each other.
	//

	// Adds sectors
	//
	// Partition 1: sectors 1, 2, 3, 4
	// Partition 2: sectors 5, 6, 7, 8
	// Partition 3: sectors 9
	addSectors := func(t *testing.T, store adt.Store, dl *miner.Deadline) {
		power, err := dl.AddSectors(store, partitionSize, sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))
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
	addThenTerminate := func(t *testing.T, store adt.Store, dl *miner.Deadline) {
		addSectors(t, store, dl)

		removedPower, err := dl.TerminateSectors(store, sectorsArr(t, store, sectors), 15, miner.PartitionSectorMap{
			0: bf(1, 3),
			1: bf(6),
		}, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := sectorPower(t, 1, 3, 6)
		require.True(t, expectedPower.Equals(removedPower), "dlState to remove power for terminated sectors")

		dlState.withTerminations(1, 3, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	}

	// Adds and terminates sectors according to the previous two functions,
	// then pops early terminations.
	addThenTerminateThenPopEarly := func(t *testing.T, store adt.Store, dl *miner.Deadline) {
		addThenTerminate(t, store, dl)

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
	addThenMarkFaulty := func(t *testing.T, store adt.Store, dl *miner.Deadline) {
		addSectors(t, store, dl)

		// Mark faulty.
		faultyPower, err := dl.DeclareFaults(
			store, sectorsArr(t, store, sectors), sectorSize, quantSpec, 9,
			map[uint64]bitfield.BitField{
				0: bf(1),
				1: bf(5, 6),
			},
		)
		require.NoError(t, err)

		expectedPower := sectorPower(t, 1, 5, 6)
		assert.True(t, faultyPower.Equals(expectedPower))

		dlState.withFaults(1, 5, 6).
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
		addSectors(t, store, dl)
	})

	t.Run("terminates sectors", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addThenTerminate(t, store, dl)
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

		addThenMarkFaulty(t, store, dl)
	})

	//
	// Now, build on these basic scenarios with some "what ifs".
	//

	t.Run("cannot remove partitions with early terminations", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addThenTerminate(t, store, dl)

		_, _, _, err := dl.RemovePartitions(store, bf(0), quantSpec)
		require.Error(t, err, "should have failed to remove a partition with early terminations")
	})

	t.Run("can pop early terminations in multiple steps", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)
		addThenTerminate(t, store, dl)

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
		addThenMarkFaulty(t, store, dl)

		// Try to remove a partition with faulty sectors.
		_, _, _, err := dl.RemovePartitions(store, bf(1), quantSpec)
		require.Error(t, err, "should have failed to remove a partition with faults")
	})

	t.Run("terminate faulty", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		addThenMarkFaulty(t, store, dl) // 1, 5, 6 faulty

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

	t.Run("faulty sectors expire", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		// Mark sectors 5 & 6 faulty, expiring at epoch 9.
		addThenMarkFaulty(t, store, dl)

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

	t.Run("post all the things", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())

		dl := emptyDeadline(t, store)
		addSectors(t, store, dl)

		sectorArr := sectorsArr(t, store, sectors)

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
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)

		postResult2, err := dl.RecordProvenSectors(store, sectorArr, sectorSize, quantSpec, 13, []miner.PoStPartition{
			{Index: 1, Skipped: bf()}, // ignore already posted partitions
			{Index: 2, Skipped: bf()},
		})
		require.NoError(t, err)
		assertBitfieldEquals(t, postResult2.Sectors, 9)
		assertEmptyBitfield(t, postResult2.IgnoredSectors)
		require.True(t, postResult2.NewFaultyPower.Equals(miner.NewPowerPairZero()))
		require.True(t, postResult2.RetractedRecoveryPower.Equals(miner.NewPowerPairZero()))
		require.True(t, postResult2.RecoveredPower.Equals(miner.NewPowerPairZero()))

		// All 3 partitions posted
		dlState.withPosts(0, 1, 2).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)

		newFaultyPower, failedRecoveryPower, err := dl.ProcessDeadlineEnd(store, quantSpec, 13)
		require.NoError(t, err)

		// No power change on successful post.
		require.True(t, newFaultyPower.Equals(miner.NewPowerPairZero()))
		require.True(t, failedRecoveryPower.Equals(miner.NewPowerPairZero()))

		// Everything back to normal.
		dlState.withPartitions(
			bf(1, 2, 3, 4),
			bf(5, 6, 7, 8),
			bf(9),
		).assert(t, store, dl)
	})

	t.Run("post with faults, recoveries, and retracted recoveries", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		// Marks sectors 1 (partition 0), 5 & 6 (partition 1) as faulty.
		addThenMarkFaulty(t, store, dl)

		sectorArr := sectorsArr(t, store, sectors)

		// Declare sectors 1 & 6 recovered.
		require.NoError(t, dl.DeclareFaultsRecovered(store, sectorArr, sectorSize, map[uint64]bitfield.BitField{
			0: bf(1),
			1: bf(6),
		}))

		// We're now recovering 1 & 6.
		dlState.withRecovering(1, 6).
			withFaults(1, 5, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
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

		// First two partitions should be posted.
		dlState.withPosts(0, 1).
			withFaults(1, 5, 7).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)

		newFaultyPower, failedRecoveryPower, err := dl.ProcessDeadlineEnd(store, quantSpec, 13)
		require.NoError(t, err)

		// Sector 9 wasn't proven.
		require.True(t, newFaultyPower.Equals(sectorPower(t, 9)))
		// No new changes to recovering power.
		require.True(t, failedRecoveryPower.Equals(miner.NewPowerPairZero()))

		// Posts taken care of.
		dlState.withFaults(1, 5, 7, 9).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, store, dl)
	})

	t.Run("retract recoveries", func(t *testing.T) {
		store := ipld.NewADTStore(context.Background())
		dl := emptyDeadline(t, store)

		// Marks sectors 1 (partition 0), 5 & 6 (partition 1) as faulty.
		addThenMarkFaulty(t, store, dl)

		sectorArr := sectorsArr(t, store, sectors)

		// Declare sectors 1 & 6 recovered.
		require.NoError(t, dl.DeclareFaultsRecovered(store, sectorArr, sectorSize, map[uint64]bitfield.BitField{
			0: bf(1),
			1: bf(6),
		}))

		// Retract recovery for sector 1.
		faultyPower, err := dl.DeclareFaults(store, sectorArr, sectorSize, quantSpec, 13, map[uint64]bitfield.BitField{
			0: bf(1),
		})

		// We're just retracting a recovery, this doesn't count as a new fault.
		require.NoError(t, err)
		require.True(t, faultyPower.Equals(miner.NewPowerPairZero()))

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
		addThenMarkFaulty(t, store, dl)

		// Try to reschedule two sectors, only the 7 (non faulty) should succeed.
		err := dl.RescheduleSectorExpirations(store, sectorArr, 1, miner.PartitionSectorMap{
			1: bf(6, 7, 99), // 99 should be skipped, it doesn't exist.
			5: bf(100),      // partition 5 doesn't exist.
			2: bf(),         // empty bitfield should be fine.
		}, sectorSize, quantSpec)
		require.NoError(t, err)

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
}

func emptyDeadline(t *testing.T, store adt.Store) *miner.Deadline {
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	return miner.ConstructDeadline(root)
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
	_, faults, recoveries, terminations, partitions := checkDeadlineInvariants(
		t, store, dl, s.quant, s.sectorSize, s.partitionSize, s.sectors,
	)

	assertBitfieldsEqual(t, s.faults, faults)
	assertBitfieldsEqual(t, s.recovering, recoveries)
	assertBitfieldsEqual(t, s.terminations, terminations)
	assertBitfieldsEqual(t, s.posts, dl.PostSubmissions)

	require.Equal(t, len(s.partitionSectors), len(partitions), "unexpected number of partitions")

	for i, partSectors := range s.partitionSectors {
		assertBitfieldsEqual(t, partSectors, partitions[i])
	}
}

// check the deadline's invariants, returning all contained sectors, faults,
// recoveries, terminations, and partition/sector assignments.
func checkDeadlineInvariants(
	t *testing.T, store adt.Store, dl *miner.Deadline,
	quant miner.QuantSpec, ssize abi.SectorSize, partitionSize uint64,
	sectors []*miner.SectorOnChainInfo,
) (
	allSectors bitfield.BitField,
	allFaults bitfield.BitField,
	allRecoveries bitfield.BitField,
	allTerminations bitfield.BitField,
	partitionSectors []bitfield.BitField,
) {
	partitions, err := dl.PartitionsArray(store)
	require.NoError(t, err)

	expectedDeadlineExpQueue := make(map[abi.ChainEpoch][]uint64)
	var partitionsWithEarlyTerminations []uint64

	allSectors = bitfield.NewFromSet(nil)
	allFaults = bitfield.NewFromSet(nil)
	allRecoveries = bitfield.NewFromSet(nil)
	allTerminations = bitfield.NewFromSet(nil)
	allFaultyPower := miner.NewPowerPairZero()

	expectPartIndex := int64(0)
	var partition miner.Partition
	err = partitions.ForEach(&partition, func(partIdx int64) error {
		// Assert sequential partitions.
		require.Equal(t, expectPartIndex, partIdx)
		expectPartIndex++

		partitionSectors = append(partitionSectors, partition.Sectors)

		contains, err := util.BitFieldContainsAny(allSectors, partition.Sectors)
		require.NoError(t, err)
		require.False(t, contains, "duplicate sectors in deadline")

		allSectors, err = bitfield.MergeBitFields(allSectors, partition.Sectors)
		require.NoError(t, err)

		allFaults, err = bitfield.MergeBitFields(allFaults, partition.Faults)
		require.NoError(t, err)

		allRecoveries, err = bitfield.MergeBitFields(allRecoveries, partition.Recoveries)
		require.NoError(t, err)

		allTerminations, err = bitfield.MergeBitFields(allTerminations, partition.Terminated)
		require.NoError(t, err)

		allFaultyPower = allFaultyPower.Add(partition.FaultyPower)

		// 1. This will check things like "recoveries is a subset of
		//    sectors" so we don't need to check that here.
		// 2. We are intentionally calling this and not
		//    assertPartitionState. We'll check sector assignment to
		//    deadline outside.
		checkPartitionInvariants(t, store, &partition, quant, ssize, sectors)

		earlyTerminated, err := adt.AsArray(store, partition.EarlyTerminated)
		require.NoError(t, err)
		if earlyTerminated.Length() > 0 {
			partitionsWithEarlyTerminations = append(partitionsWithEarlyTerminations, uint64(partIdx))
		}

		// The partition's expiration queue is already tested by the
		// partition tests.
		//
		// Here, we're making sure it's consistent with the deadline's queue.
		q, err := adt.AsArray(store, partition.ExpirationsEpochs)
		require.NoError(t, err)
		err = q.ForEach(nil, func(epoch int64) error {
			require.Equal(t, quant.QuantizeUp(abi.ChainEpoch(epoch)), abi.ChainEpoch(epoch))
			expectedDeadlineExpQueue[abi.ChainEpoch(epoch)] = append(
				expectedDeadlineExpQueue[abi.ChainEpoch(epoch)],
				uint64(partIdx),
			)
			return nil
		})
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	allSectorsCount, err := allSectors.Count()
	require.NoError(t, err)

	deadSectorsCount, err := allTerminations.Count()
	require.NoError(t, err)

	require.Equal(t, dl.LiveSectors, allSectorsCount-deadSectorsCount)
	require.Equal(t, dl.TotalSectors, allSectorsCount)
	require.True(t, allFaultyPower.Equals(dl.FaultyPower))

	// Validate expiration queue. The deadline expiration queue is a
	// superset of the partition expiration queues because we never remove
	// from it.
	{
		expirationEpochs, err := adt.AsArray(store, dl.ExpirationsEpochs)
		require.NoError(t, err)
		for epoch, partitions := range expectedDeadlineExpQueue {
			var bf bitfield.BitField
			found, err := expirationEpochs.Get(uint64(epoch), &bf)
			require.NoError(t, err)
			require.True(t, found, "expected to find partitions with expirations at epoch %d", epoch)
			for _, p := range partitions {
				present, err := bf.IsSet(p)
				require.NoError(t, err)
				assert.True(t, present, "expected partition %d to be present in deadline expiration queue at epoch %d", p, epoch)
			}
		}
	}

	// Validate early terminations.
	assertBitfieldEquals(t, dl.EarlyTerminations, partitionsWithEarlyTerminations...)

	// returns named values.
	return
}
