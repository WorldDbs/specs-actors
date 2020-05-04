package miner_test

import (
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
)

func TestTerminationResult(t *testing.T) {
	var result miner.TerminationResult
	require.True(t, result.IsEmpty())
	err := result.ForEach(func(epoch abi.ChainEpoch, sectors bitfield.BitField) error {
		require.FailNow(t, "unreachable")
		return nil
	})
	require.NoError(t, err)

	resultA := miner.TerminationResult{
		Sectors: map[abi.ChainEpoch]bitfield.BitField{
			3: bitfield.NewFromSet([]uint64{9}),
			0: bitfield.NewFromSet([]uint64{1, 2, 4}),
			2: bitfield.NewFromSet([]uint64{3, 5, 7}),
		},
		PartitionsProcessed: 1,
		SectorsProcessed:    7,
	}
	require.False(t, resultA.IsEmpty())
	resultB := miner.TerminationResult{
		Sectors: map[abi.ChainEpoch]bitfield.BitField{
			1: bitfield.NewFromSet([]uint64{12}),
			0: bitfield.NewFromSet([]uint64{10}),
		},
		PartitionsProcessed: 1,
		SectorsProcessed:    2,
	}
	require.False(t, resultB.IsEmpty())
	require.NoError(t, result.Add(resultA))
	require.NoError(t, result.Add(resultB))
	require.False(t, result.IsEmpty())
	expected := miner.TerminationResult{
		Sectors: map[abi.ChainEpoch]bitfield.BitField{
			2: bitfield.NewFromSet([]uint64{3, 5, 7}),
			0: bitfield.NewFromSet([]uint64{1, 2, 4, 10}),
			1: bitfield.NewFromSet([]uint64{12}),
			3: bitfield.NewFromSet([]uint64{9}),
		},
		PartitionsProcessed: 2,
		SectorsProcessed:    9,
	}
	require.Equal(t, expected.SectorsProcessed, result.SectorsProcessed)
	require.Equal(t, expected.PartitionsProcessed, result.PartitionsProcessed)
	require.Equal(t, len(expected.Sectors), len(result.Sectors))

	expectedEpoch := abi.ChainEpoch(0)
	err = result.ForEach(func(epoch abi.ChainEpoch, actualBf bitfield.BitField) error {
		require.Equal(t, expectedEpoch, epoch)
		expectedEpoch++
		expectedBf, ok := expected.Sectors[epoch]
		require.True(t, ok)
		expectedNos, err := expectedBf.All(1000)
		require.NoError(t, err)
		actualNos, err := actualBf.All(1000)
		require.NoError(t, err)
		require.Equal(t, expectedNos, actualNos)
		return nil
	})
	require.NoError(t, err)

	// partitions = 2, sectors = 9
	require.False(t, result.BelowLimit(2, 9))
	require.False(t, result.BelowLimit(3, 9))
	require.False(t, result.BelowLimit(3, 8))
	require.False(t, result.BelowLimit(2, 10))
	require.True(t, result.BelowLimit(3, 10))
}
