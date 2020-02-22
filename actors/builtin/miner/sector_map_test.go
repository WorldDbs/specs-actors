package miner_test

import (
	"errors"
	"math"
	"sort"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
)

func TestDeadlineSectorMap(t *testing.T) {
	dm := make(miner.DeadlineSectorMap)
	dlCount := uint64(10)
	partCount := uint64(5)
	for dlIdx := uint64(0); dlIdx < dlCount; dlIdx++ {
		for partIdx := uint64(0); partIdx < partCount; partIdx++ {
			assert.NoError(t, dm.Add(dlIdx, partIdx, bf(dlIdx*partCount+partIdx)))
		}
	}

	err := dm.ForEach(func(dlIdx uint64, partitions miner.PartitionSectorMap) error {
		assert.Equal(t, dm[dlIdx], partitions)
		return partitions.ForEach(func(partIdx uint64, sectorNos bitfield.BitField) error {
			assert.Equal(t, partitions[partIdx], sectorNos)
			assertBitfieldEquals(t, sectorNos, dlIdx*partCount+partIdx)
			return nil
		})
	})
	require.NoError(t, err)

	// check all counts.
	parts, sectors, err := dm.Count()
	require.NoError(t, err)
	assert.Equal(t, parts, partCount*dlCount)
	assert.Equal(t, sectors, partCount*dlCount)
	assert.Error(t, dm.Check(1, 1))
	assert.Error(t, dm.Check(100, 1))
	assert.Error(t, dm.Check(1, 100))
	assert.NoError(t, dm.Check(partCount*dlCount, partCount*dlCount))

	// Merge a sector in.
	require.NoError(t, dm.Add(0, 0, bf(1000)))
	assertBitfieldEquals(t, dm[0][0], 0, 1000)
	assert.Error(t, dm.Check(partCount*dlCount, partCount*dlCount))
	assert.NoError(t, dm.Check(partCount*dlCount, partCount*dlCount+1))
}

func TestDeadlineSectorMapError(t *testing.T) {
	dm := make(miner.DeadlineSectorMap)
	dlCount := uint64(10)
	partCount := uint64(5)
	for dlIdx := uint64(0); dlIdx < dlCount; dlIdx++ {
		for partIdx := uint64(0); partIdx < partCount; partIdx++ {
			assert.NoError(t, dm.Add(dlIdx, partIdx, bf(dlIdx*partCount+partIdx)))
		}
	}

	expErr := errors.New("foobar")

	err := dm.ForEach(func(dlIdx uint64, partitions miner.PartitionSectorMap) error {
		return partitions.ForEach(func(partIdx uint64, sectorNos bitfield.BitField) error {
			return expErr
		})
	})
	require.Equal(t, expErr, err)
}

func TestDeadlineSectorMapValues(t *testing.T) {
	dm := make(miner.DeadlineSectorMap)
	assert.NoError(t, dm.AddValues(0, 1, 0, 1, 2, 3))

	assertBitfieldEquals(t, dm[0][1], 0, 1, 2, 3)
}

func TestPartitionSectorMapValues(t *testing.T) {
	pm := make(miner.PartitionSectorMap)
	assert.NoError(t, pm.AddValues(0, 0, 1, 2, 3))

	assertBitfieldEquals(t, pm[0], 0, 1, 2, 3)
}

func TestDeadlineSectorMapOverflow(t *testing.T) {
	dm := make(miner.DeadlineSectorMap)
	dlCount := uint64(10)
	bf := seq(t, 0, math.MaxUint64)
	for dlIdx := uint64(0); dlIdx < dlCount; dlIdx++ {
		assert.NoError(t, dm.Add(dlIdx, 0, bf))
	}

	_, _, err := dm[0].Count()
	require.NoError(t, err)

	_, _, err = dm.Count()
	require.Error(t, err)
}

func TestPartitionSectorMapOverflow(t *testing.T) {
	pm := make(miner.PartitionSectorMap)
	partCount := uint64(2)
	bf := seq(t, 0, math.MaxUint64)
	for partIdx := uint64(0); partIdx < partCount; partIdx++ {
		assert.NoError(t, pm.Add(partIdx, bf))
	}

	_, _, err := pm.Count()
	require.Error(t, err)
}

func TestDeadlineSectorMapEmpty(t *testing.T) {
	var dm miner.DeadlineSectorMap
	partitions, sectors, err := dm.Count()
	require.NoError(t, err)
	require.Zero(t, partitions)
	require.Zero(t, sectors)

	require.NoError(t, dm.ForEach(func(dlIdx uint64, pm miner.PartitionSectorMap) error {
		require.Fail(t, "should not iterate over an empty map")
		return nil
	}))
	require.Empty(t, dm.Deadlines())
}

func TestPartitionSectorMapEmpty(t *testing.T) {
	var pm miner.PartitionSectorMap

	partitions, sectors, err := pm.Count()
	require.NoError(t, err)
	require.Zero(t, partitions)
	require.Zero(t, sectors)

	require.NoError(t, pm.ForEach(func(dlIdx uint64, sectorNos bitfield.BitField) error {
		require.Fail(t, "should not iterate over an empty map")
		return nil
	}))
	require.Empty(t, pm.Partitions())
}

func TestDeadlineSectorMapSorted(t *testing.T) {
	dm := make(miner.DeadlineSectorMap)
	for i := uint64(47); i > 0; i-- {
		require.NoError(t, dm.AddValues(i, 0, 0))
	}
	dls := dm.Deadlines()
	require.True(t, sort.SliceIsSorted(dls, func(i, j int) bool {
		return dls[i] < dls[j]
	}))
}

func TestPartitionSectorMapSorted(t *testing.T) {
	pm := make(miner.PartitionSectorMap)
	for i := uint64(100); i > 0; i-- {
		require.NoError(t, pm.AddValues(i, 0))
	}
	pms := pm.Partitions()
	require.True(t, sort.SliceIsSorted(pms, func(i, j int) bool {
		return pms[i] < pms[j]
	}))
}
