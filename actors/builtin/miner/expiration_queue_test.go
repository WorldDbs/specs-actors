package miner_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestExpirationSet(t *testing.T) {
	onTimeSectors := bitfield.NewFromSet([]uint64{5, 8, 9})
	earlySectors := bitfield.NewFromSet([]uint64{2, 3})
	onTimePledge := abi.NewTokenAmount(1000)
	activePower := miner.NewPowerPair(abi.NewStoragePower(1<<13), abi.NewStoragePower(1<<14))
	faultyPower := miner.NewPowerPair(abi.NewStoragePower(1<<11), abi.NewStoragePower(1<<12))

	t.Run("adds sectors and power to empty set", func(t *testing.T) {
		set := miner.NewExpirationSetEmpty()

		err := set.Add(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 8, 9)
		assertBitfieldEquals(t, set.EarlySectors, 2, 3)
		assert.Equal(t, onTimePledge, set.OnTimePledge)
		assert.True(t, activePower.Equals(set.ActivePower))
		assert.True(t, faultyPower.Equals(set.FaultyPower))

		count, err := set.Count()
		require.NoError(t, err)
		assert.EqualValues(t, 5, count)
	})

	t.Run("adds sectors and power to non-empty set", func(t *testing.T) {
		set := miner.NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		err := set.Add(
			bitfield.NewFromSet([]uint64{6, 7, 11}),
			bitfield.NewFromSet([]uint64{1, 4}),
			abi.NewTokenAmount(300),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<13)), abi.NewStoragePower(3*(1<<14))),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
		)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 6, 7, 8, 9, 11)
		assertBitfieldEquals(t, set.EarlySectors, 1, 2, 3, 4)
		assert.Equal(t, abi.NewTokenAmount(1300), set.OnTimePledge)
		active := miner.NewPowerPair(abi.NewStoragePower(1<<15), abi.NewStoragePower(1<<16))
		assert.True(t, active.Equals(set.ActivePower))
		faulty := miner.NewPowerPair(abi.NewStoragePower(1<<13), abi.NewStoragePower(1<<14))
		assert.True(t, faulty.Equals(set.FaultyPower))
	})

	t.Run("removes sectors and power set", func(t *testing.T) {
		set := miner.NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		err := set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(800),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 8)
		assertBitfieldEquals(t, set.EarlySectors, 3)
		assert.Equal(t, abi.NewTokenAmount(200), set.OnTimePledge)
		active := miner.NewPowerPair(abi.NewStoragePower(1<<11), abi.NewStoragePower(1<<12))
		assert.True(t, active.Equals(set.ActivePower))
		faulty := miner.NewPowerPair(abi.NewStoragePower(1<<9), abi.NewStoragePower(1<<10))
		assert.True(t, faulty.Equals(set.FaultyPower))
	})

	t.Run("remove fails when pledge underflows", func(t *testing.T) {
		set := miner.NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		err := set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(1200),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pledge underflow")
	})

	t.Run("remove fails to remove sectors it does not contain", func(t *testing.T) {
		set := miner.NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		// remove unknown active sector 12
		err := set.Remove(
			bitfield.NewFromSet([]uint64{12}),
			bitfield.NewFromSet([]uint64{}),
			abi.NewTokenAmount(0),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not contained")

		// remove faulty sector 8, that is active in the set
		err = set.Remove(
			bitfield.NewFromSet([]uint64{0}),
			bitfield.NewFromSet([]uint64{8}),
			abi.NewTokenAmount(0),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not contained")
	})

	t.Run("remove fails when active or fault qa power underflows", func(t *testing.T) {
		set := miner.NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		// active removed power > active power
		err := set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(200),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<12)), abi.NewStoragePower(3*(1<<13))),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "power underflow")

		set = miner.NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		// faulty removed power > faulty power
		err = set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(200),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			miner.NewPowerPair(abi.NewStoragePower(3*(1<<10)), abi.NewStoragePower(3*(1<<11))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "power underflow")
	})

	t.Run("set is empty when all sectors removed", func(t *testing.T) {
		set := miner.NewExpirationSetEmpty()

		empty, err := set.IsEmpty()
		require.NoError(t, err)
		assert.True(t, empty)

		count, err := set.Count()
		require.NoError(t, err)
		assert.Zero(t, count)

		err = set.Add(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)
		require.NoError(t, err)

		empty, err = set.IsEmpty()
		require.NoError(t, err)
		assert.False(t, empty)

		err = set.Remove(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)
		require.NoError(t, err)

		empty, err = set.IsEmpty()
		require.NoError(t, err)
		assert.True(t, empty)

		count, err = set.Count()
		require.NoError(t, err)
		assert.Zero(t, count)
	})
}

func TestExpirationQueue(t *testing.T) {
	sectors := []*miner.SectorOnChainInfo{
		testSector(2, 1, 50, 60, 1000),
		testSector(3, 2, 51, 61, 1001),
		testSector(7, 3, 52, 62, 1002),
		testSector(8, 4, 53, 63, 1003),
		testSector(11, 5, 54, 64, 1004),
		testSector(13, 6, 55, 65, 1005),
	}
	sectorSize := abi.SectorSize(32 * 1 << 30)

	t.Run("added sectors can be popped off queue", func(t *testing.T) {
		queue := emptyExpirationQueue(t)
		secNums, power, pledge, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)
		assertBitfieldEquals(t, secNums, 1, 2, 3, 4, 5, 6)
		assert.True(t, power.Equals(miner.PowerForSectors(sectorSize, sectors)))
		assert.Equal(t, abi.NewTokenAmount(6015), pledge)

		// default test quantizing of 1 means every sector is in its own expriation set
		assert.Equal(t, len(sectors), int(queue.Length()))

		_, err = queue.Root()
		require.NoError(t, err)

		// pop off sectors up to and including epoch 8
		set, err := queue.PopUntil(7)
		require.NoError(t, err)

		// only 3 sectors remain
		assert.Equal(t, 3, int(queue.Length()))

		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2, 3)
		assertBitfieldEmpty(t, set.EarlySectors)

		activePower := miner.PowerForSectors(sectorSize, sectors[:3])
		faultyPower := miner.NewPowerPairZero()

		assert.Equal(t, big.NewInt(3003), set.OnTimePledge) // sum of first 3 sector pledges
		assert.True(t, activePower.Equals(set.ActivePower))
		assert.True(t, faultyPower.Equals(set.FaultyPower))

		// pop off rest up to and including epoch 8
		set, err = queue.PopUntil(20)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 4, 5, 6)
		assertBitfieldEmpty(t, set.EarlySectors)

		assert.Equal(t, big.NewInt(3012), set.OnTimePledge) // sum of last 3 sector pledges
		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[3:])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))

		// queue is now empty
		assert.Equal(t, 0, int(queue.Length()))
	})

	t.Run("quantizes added sectors by expiration", func(t *testing.T) {
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(5, 3))
		secNums, power, pledge, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)
		assertBitfieldEquals(t, secNums, 1, 2, 3, 4, 5, 6)
		assert.True(t, power.Equals(miner.PowerForSectors(sectorSize, sectors)))
		assert.Equal(t, abi.NewTokenAmount(6015), pledge)

		// work around caching issues in amt
		_, err = queue.Root()
		require.NoError(t, err)

		// quantizing spec means sectors should be grouped into 3 sets expiring at 3, 8 and 13
		assert.Equal(t, 3, int(queue.Length()))

		// set popped before first quantized sector should be empty
		set, err := queue.PopUntil(2)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 3, int(queue.Length()))

		// first 2 sectors will be in first set popped off at quantization offset (3)
		set, err = queue.PopUntil(3)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2)
		assert.Equal(t, 2, int(queue.Length()))

		_, err = queue.Root()
		require.NoError(t, err)

		// no sectors will be popped off in quantization interval
		set, err = queue.PopUntil(7)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 2, int(queue.Length()))

		// next 2 sectors will be in first set popped off after quantization interval (8)
		set, err = queue.PopUntil(8)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 3, 4)
		assert.Equal(t, 1, int(queue.Length()))

		_, err = queue.Root()
		require.NoError(t, err)

		// no sectors will be popped off in quantization interval
		set, err = queue.PopUntil(12)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 1, int(queue.Length()))

		// rest of sectors will be in first set popped off after quantization interval (13)
		set, err = queue.PopUntil(13)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 5, 6)
		assert.Equal(t, 0, int(queue.Length()))
	})

	t.Run("reschedules sectors to expire later", func(t *testing.T) {
		queue := emptyExpirationQueue(t)
		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		err = queue.RescheduleExpirations(abi.ChainEpoch(20), sectors[:3], sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// expect 3 rescheduled sectors to be bundled into 1 set
		assert.Equal(t, 4, int(queue.Length()))

		// rescheduled sectors are no longer scheduled before epoch 8
		set, err := queue.PopUntil(7)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 4, int(queue.Length()))

		// pop off sectors before new expiration and expect only the rescheduled set to remain
		_, err = queue.PopUntil(19)
		require.NoError(t, err)
		assert.Equal(t, 1, int(queue.Length()))

		// pop off rescheduled sectors
		set, err = queue.PopUntil(20)
		require.NoError(t, err)
		assert.Equal(t, 0, int(queue.Length()))

		// expect all sector stats from first 3 sectors to belong to new expiration set
		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2, 3)
		assertBitfieldEmpty(t, set.EarlySectors)

		assert.Equal(t, big.NewInt(3003), set.OnTimePledge)
		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[:3])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))
	})

	t.Run("reschedules sectors as faults", func(t *testing.T) {
		// Create 3 expiration sets with 2 sectors apiece
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// Fault middle sectors to expire at epoch 6
		// This faults one sector from the first set, all of the second set and one from the third.
		// Faulting at epoch 6 means the first 3 will expire on time, but the last will be early and
		// moved to the second set
		powerDelta, err := queue.RescheduleAsFaults(abi.ChainEpoch(6), sectors[1:5], sectorSize)
		require.NoError(t, err)
		assert.True(t, powerDelta.Equals(miner.PowerForSectors(sectorSize, sectors[1:5])))

		_, err = queue.Root()
		require.NoError(t, err)

		// expect first set to contain first two sectors but with the seconds power moved to faulty power
		requireNoExpirationGroupsBefore(t, 5, queue)
		set, err := queue.PopUntil(5)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2)
		assertBitfieldEmpty(t, set.EarlySectors)

		assert.Equal(t, big.NewInt(2001), set.OnTimePledge)
		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[0:1])))
		assert.True(t, set.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[1:2])))

		// expect the second set to have all faulty power and now contain 5th sector as an early sector
		requireNoExpirationGroupsBefore(t, 9, queue)
		set, err = queue.PopUntil(9)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 3, 4)
		assertBitfieldEquals(t, set.EarlySectors, 5)

		// pledge is kept from original 2 sectors. Pledge from new early sector is NOT added.
		assert.Equal(t, big.NewInt(2005), set.OnTimePledge)

		assert.True(t, set.ActivePower.Equals(miner.NewPowerPairZero()))
		assert.True(t, set.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[2:5])))

		// expect last set to only contain non faulty sector
		requireNoExpirationGroupsBefore(t, 13, queue)
		set, err = queue.PopUntil(13)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 6)
		assertBitfieldEmpty(t, set.EarlySectors)

		// Pledge from sector moved from this set is dropped
		assert.Equal(t, big.NewInt(1005), set.OnTimePledge)

		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[5:])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))
	})

	t.Run("reschedules all sectors as faults", func(t *testing.T) {
		// Create expiration 3 sets with 2 sectors apiece
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// Fault all sectors
		// This converts the first 2 sets to faults and adds the 3rd set as early sectors to the second set
		err = queue.RescheduleAllAsFaults(abi.ChainEpoch(6))
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// expect first set to contain first two sectors but with the seconds power moved to faulty power
		requireNoExpirationGroupsBefore(t, 5, queue)
		set, err := queue.PopUntil(5)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2) // sectors are unmoved
		assertBitfieldEmpty(t, set.EarlySectors)

		assert.Equal(t, big.NewInt(2001), set.OnTimePledge) // pledge is same

		// active power is converted to fault power
		assert.True(t, set.ActivePower.Equals(miner.NewPowerPairZero()))
		assert.True(t, set.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[:2])))

		// expect the second set to have all faulty power and now contain 5th and 6th sectors as an early sectors
		requireNoExpirationGroupsBefore(t, 9, queue)
		set, err = queue.PopUntil(9)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 3, 4)
		assertBitfieldEquals(t, set.EarlySectors, 5, 6)

		// pledge is kept from original 2 sectors. Pledge from new early sectors is NOT added.
		assert.Equal(t, big.NewInt(2005), set.OnTimePledge)

		// fault power is all power for sectors previously in the first and second sets
		assert.True(t, set.ActivePower.Equals(miner.NewPowerPairZero()))
		assert.True(t, set.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[2:])))

		// expect last set to only contain non faulty sector
		requireNoExpirationGroupsBefore(t, 13, queue)
		set, err = queue.PopUntil(13)
		require.NoError(t, err)

		assertBitfieldEmpty(t, set.OnTimeSectors)
		assertBitfieldEmpty(t, set.EarlySectors)

		// all pledge is dropped
		assert.Equal(t, big.Zero(), set.OnTimePledge)

		assert.True(t, set.ActivePower.Equals(miner.NewPowerPairZero()))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))
	})

	t.Run("reschedule expirations then reschedule as fault", func(t *testing.T) {
		// Create expiration 3 sets with 2 sectors apiece
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// reschedule 2 from second group to first
		toReschedule := []*miner.SectorOnChainInfo{sectors[2]}
		err = queue.RescheduleExpirations(2, toReschedule, sectorSize)
		require.NoError(t, err)

		// now reschedule one sector in first group and another in second group as faults to expire in first set
		faults := []*miner.SectorOnChainInfo{sectors[1], sectors[2]}
		power, err := queue.RescheduleAsFaults(4, faults, sectorSize)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, faults)
		assert.Equal(t, expectedPower, power)

		// expect 0, 1, 2, 3 in first group
		set, err := queue.PopUntil(5)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2, 3)
		assert.Equal(t, miner.PowerForSectors(sectorSize, []*miner.SectorOnChainInfo{sectors[0]}), set.ActivePower)
		assert.Equal(t, expectedPower, set.FaultyPower)

		// expect rest to come later
		set, err = queue.PopUntil(20)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 4, 5, 6)
		assert.Equal(t, miner.PowerForSectors(sectorSize, []*miner.SectorOnChainInfo{sectors[3], sectors[4], sectors[5]}), set.ActivePower)
		assert.Equal(t, miner.NewPowerPairZero(), set.FaultyPower)
	})

	t.Run("reschedule recover restores all sector stats", func(t *testing.T) {
		// Create expiration 3 sets with 2 sectors apiece
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// Fault middle sectors to expire at epoch 6 to put sectors in a state
		// described in "reschedules sectors as faults"
		_, err = queue.RescheduleAsFaults(abi.ChainEpoch(6), sectors[1:5], sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// mark faulted sectors as recovered
		recovered, err := queue.RescheduleRecovered(sectors[1:5], sectorSize)
		require.NoError(t, err)
		assert.True(t, recovered.Equals(miner.PowerForSectors(sectorSize, sectors[1:5])))

		// expect first set to contain first two sectors with active power
		requireNoExpirationGroupsBefore(t, 5, queue)
		set, err := queue.PopUntil(5)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2)
		assertBitfieldEmpty(t, set.EarlySectors)

		// pledge from both sectors
		assert.Equal(t, big.NewInt(2001), set.OnTimePledge)

		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[:2])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))

		// expect second set to have lost early sector 5 and have active power just from 3 and 4
		requireNoExpirationGroupsBefore(t, 9, queue)
		set, err = queue.PopUntil(9)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 3, 4)
		assertBitfieldEmpty(t, set.EarlySectors)

		// pledge is kept from original 2 sectors
		assert.Equal(t, big.NewInt(2005), set.OnTimePledge)

		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[2:4])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))

		// expect sector 5 to be returned to last setu
		requireNoExpirationGroupsBefore(t, 13, queue)
		set, err = queue.PopUntil(13)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 6)
		assertBitfieldEmpty(t, set.EarlySectors)

		// Pledge from sector 5 is restored
		assert.Equal(t, big.NewInt(2009), set.OnTimePledge)

		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[4:])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))
	})

	t.Run("replaces sectors with new sectors", func(t *testing.T) {
		// Create expiration 3 sets
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))

		// add sectors to each set
		_, _, _, err := queue.AddActiveSectors([]*miner.SectorOnChainInfo{sectors[0], sectors[1], sectors[3], sectors[5]}, sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// remove all from first set, replace second set, and append to third
		toRemove := []*miner.SectorOnChainInfo{sectors[0], sectors[1], sectors[3]}
		toAdd := []*miner.SectorOnChainInfo{sectors[2], sectors[4]}
		removed, added, powerDelta, pledgeDelta, err := queue.ReplaceSectors(
			toRemove,
			toAdd,
			sectorSize)
		require.NoError(t, err)
		assertBitfieldEquals(t, removed, 1, 2, 4)
		assertBitfieldEquals(t, added, 3, 5)
		addedPower := miner.PowerForSectors(sectorSize, toAdd)
		assert.True(t, powerDelta.Equals(addedPower.Sub(miner.PowerForSectors(sectorSize, toRemove))))
		assert.Equal(t, abi.NewTokenAmount(1002+1004-1000-1001-1003), pledgeDelta)

		// first set is gone
		requireNoExpirationGroupsBefore(t, 9, queue)

		// second set is replaced
		set, err := queue.PopUntil(9)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 3)
		assertBitfieldEmpty(t, set.EarlySectors)

		// pledge and power is only from sector 3
		assert.Equal(t, big.NewInt(1002), set.OnTimePledge)
		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[2:3])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))

		// last set appends sector 6
		requireNoExpirationGroupsBefore(t, 13, queue)
		set, err = queue.PopUntil(13)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 6)
		assertBitfieldEmpty(t, set.EarlySectors)

		// pledge and power are some of old and new sectors
		assert.Equal(t, big.NewInt(2009), set.OnTimePledge)
		assert.True(t, set.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[4:])))
		assert.True(t, set.FaultyPower.Equals(miner.NewPowerPairZero()))
	})

	t.Run("removes sectors", func(t *testing.T) {
		// add all sectors into 3 sets
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// put queue in a state where some sectors are early and some are faulty
		_, err = queue.RescheduleAsFaults(abi.ChainEpoch(6), sectors[1:6], sectorSize)
		require.NoError(t, err)

		_, err = queue.Root()
		require.NoError(t, err)

		// remove an active sector from first set, faulty sector and early faulty sector from second set,
		toRemove := []*miner.SectorOnChainInfo{sectors[0], sectors[3], sectors[4], sectors[5]}

		// and only sector from last set
		faults := bitfield.NewFromSet([]uint64{4, 5, 6})

		// label the last as recovering
		recovering := bitfield.NewFromSet([]uint64{6})
		removed, recoveringPower, err := queue.RemoveSectors(toRemove, faults, recovering, sectorSize)
		require.NoError(t, err)

		// assert all return values are correct
		assertBitfieldEquals(t, removed.OnTimeSectors, 1, 4)
		assertBitfieldEquals(t, removed.EarlySectors, 5, 6)
		assert.Equal(t, abi.NewTokenAmount(1000+1003), removed.OnTimePledge) // only on-time sectors
		assert.True(t, removed.ActivePower.Equals(miner.PowerForSectors(sectorSize, []*miner.SectorOnChainInfo{sectors[0]})))
		assert.True(t, removed.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[3:6])))
		assert.True(t, recoveringPower.Equals(miner.PowerForSectors(sectorSize, sectors[5:6])))

		// assert queue state is as expected

		// only faulty sector 2 is found in first set
		requireNoExpirationGroupsBefore(t, 5, queue)
		set, err := queue.PopUntil(5)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 2)
		assertBitfieldEmpty(t, set.EarlySectors)
		assert.Equal(t, abi.NewTokenAmount(1001), set.OnTimePledge)
		assert.True(t, set.ActivePower.Equals(miner.NewPowerPairZero()))
		assert.True(t, set.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[1:2])))

		// only faulty on-time sector 3 is found in second set
		requireNoExpirationGroupsBefore(t, 9, queue)
		set, err = queue.PopUntil(9)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 3)
		assertBitfieldEmpty(t, set.EarlySectors)
		assert.Equal(t, abi.NewTokenAmount(1002), set.OnTimePledge)
		assert.True(t, set.ActivePower.Equals(miner.NewPowerPairZero()))
		assert.True(t, set.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[2:3])))

		// no further sets remain
		requireNoExpirationGroupsBefore(t, 20, queue)
	})

	t.Run("adding no sectors leaves the queue empty", func(t *testing.T) {
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		_, _, _, err := queue.AddActiveSectors(nil, sectorSize)
		require.NoError(t, err)
		assert.Zero(t, queue.Length())
	})

	t.Run("rescheduling no expirations leaves the queue empty", func(t *testing.T) {
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		err := queue.RescheduleExpirations(10, nil, sectorSize)
		require.NoError(t, err)
		assert.Zero(t, queue.Length())
	})

	t.Run("rescheduling no expirations as faults leaves the queue empty", func(t *testing.T) {
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))

		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		// all sectors already expire before epoch 15, nothing should change.
		length := queue.Length()
		_, err = queue.RescheduleAsFaults(15, sectors, sectorSize)
		require.NoError(t, err)
		assert.Equal(t, length, queue.Length())
	})

	t.Run("rescheduling all expirations as faults leaves the queue empty if it was empty", func(t *testing.T) {
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))

		_, _, _, err := queue.AddActiveSectors(sectors, sectorSize)
		require.NoError(t, err)

		// all sectors already expire before epoch 15, nothing should change.
		length := queue.Length()
		err = queue.RescheduleAllAsFaults(15)
		require.NoError(t, err)
		assert.Equal(t, length, queue.Length())
	})

	t.Run("rescheduling no sectors as recovered leaves the queue empty", func(t *testing.T) {
		queue := emptyExpirationQueueWithQuantizing(t, miner.NewQuantSpec(4, 1))
		_, err := queue.RescheduleRecovered(nil, sectorSize)
		require.NoError(t, err)
		assert.Zero(t, queue.Length())
	})
}

func testSector(expiration, number, weight, vweight, pledge int64) *miner.SectorOnChainInfo {
	return &miner.SectorOnChainInfo{
		Expiration:         abi.ChainEpoch(expiration),
		SectorNumber:       abi.SectorNumber(number),
		DealWeight:         big.NewInt(weight),
		VerifiedDealWeight: big.NewInt(vweight),
		InitialPledge:      abi.NewTokenAmount(pledge),
		SealedCID:          tutil.MakeCID(fmt.Sprintf("commR-%d", number), &miner.SealedCIDPrefix),
	}
}

func requireNoExpirationGroupsBefore(t *testing.T, epoch abi.ChainEpoch, queue miner.ExpirationQueue) {
	_, err := queue.Root()
	require.NoError(t, err)

	set, err := queue.PopUntil(epoch - 1)
	require.NoError(t, err)
	empty, err := set.IsEmpty()
	require.NoError(t, err)
	require.True(t, empty)
}

func emptyExpirationQueueWithQuantizing(t *testing.T, quant miner.QuantSpec) miner.ExpirationQueue {
	rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	queue, err := miner.LoadExpirationQueue(store, root, quant)
	require.NoError(t, err)
	return queue
}

func emptyExpirationQueue(t *testing.T) miner.ExpirationQueue {
	return emptyExpirationQueueWithQuantizing(t, miner.NoQuantization)
}
