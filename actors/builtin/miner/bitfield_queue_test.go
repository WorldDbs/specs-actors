package miner_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
)

func TestBitfieldQueue(t *testing.T) {
	t.Run("adds values to empty queue", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		values := []uint64{1, 2, 3, 4}
		epoch := abi.ChainEpoch(42)
		require.NoError(t, queue.AddToQueueValues(epoch, values...))

		ExpectBQ().
			Add(epoch, values...).
			Equals(t, queue)
	})

	t.Run("adds bitfield to empty queue", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		values := []uint64{1, 2, 3, 4}
		epoch := abi.ChainEpoch(42)

		require.NoError(t, queue.AddToQueue(epoch, bitfield.NewFromSet(values)))

		ExpectBQ().
			Add(epoch, values...).
			Equals(t, queue)
	})

	t.Run("quantizes added epochs according to quantization spec", func(t *testing.T) {
		queue := emptyBitfieldQueueWithQuantizing(t, miner.NewQuantSpec(5, 3))

		for _, val := range []uint64{0, 2, 3, 4, 7, 8, 9} {
			require.NoError(t, queue.AddToQueueValues(abi.ChainEpoch(val), val))
		}

		// expect values to only be set on quantization boundaries
		ExpectBQ().
			Add(abi.ChainEpoch(3), 0, 2, 3).
			Add(abi.ChainEpoch(8), 4, 7, 8).
			Add(abi.ChainEpoch(13), 9).
			Equals(t, queue)
	})

	t.Run("quantizes added epochs according to quantization spec", func(t *testing.T) {
		queue := emptyBitfieldQueueWithQuantizing(t, miner.NewQuantSpec(5, 3))

		for _, val := range []uint64{0, 2, 3, 4, 7, 8, 9} {
			err := queue.AddToQueueValues(abi.ChainEpoch(val), val)
			require.NoError(t, err)
		}

		// expect values to only be set on quantization boundaries
		ExpectBQ().
			Add(abi.ChainEpoch(3), 0, 2, 3).
			Add(abi.ChainEpoch(8), 4, 7, 8).
			Add(abi.ChainEpoch(13), 9).
			Equals(t, queue)
	})

	t.Run("merges values withing same epoch", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch := abi.ChainEpoch(42)

		require.NoError(t, queue.AddToQueueValues(epoch, 1, 3))
		require.NoError(t, queue.AddToQueueValues(epoch, 2, 4))

		ExpectBQ().
			Add(epoch, 1, 2, 3, 4).
			Equals(t, queue)
	})

	t.Run("adds values to different epochs", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch1 := abi.ChainEpoch(42)
		epoch2 := abi.ChainEpoch(93)

		require.NoError(t, queue.AddToQueueValues(epoch1, 1, 3))
		require.NoError(t, queue.AddToQueueValues(epoch2, 2, 4))

		ExpectBQ().
			Add(epoch1, 1, 3).
			Add(epoch2, 2, 4).
			Equals(t, queue)
	})

	t.Run("PouUntil from empty queue returns empty bitfield", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		// TODO: broken pending https://github.com/filecoin-project/go-amt-ipld/issues/18
		//emptyQueue, err := queue.Root()
		//require.NoError(t, err)

		next, modified, err := queue.PopUntil(42)
		require.NoError(t, err)
		assert.False(t, modified)

		// no values are returned
		count, err := next.Count()
		require.NoError(t, err)
		assert.Equal(t, 0, int(count))

		// queue is still empty
		//root, err := queue.Root()
		//assert.Equal(t, emptyQueue, root)
	})

	t.Run("PopUntil does nothing if 'until' parameter before first value", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch1 := abi.ChainEpoch(42)
		epoch2 := abi.ChainEpoch(93)

		require.NoError(t, queue.AddToQueueValues(epoch1, 1, 3))
		require.NoError(t, queue.AddToQueueValues(epoch2, 2, 4))

		next, modified, err := queue.PopUntil(epoch1 - 1)
		assert.False(t, modified)
		require.NoError(t, err)
		assert.False(t, modified)

		// no values are returned
		count, err := next.Count()
		require.NoError(t, err)
		assert.Equal(t, 0, int(count))

		// queue remains the same
		ExpectBQ().
			Add(epoch1, 1, 3).
			Add(epoch2, 2, 4).
			Equals(t, queue)
	})

	t.Run("PopUntil removes and returns entries before and including target epoch", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch1 := abi.ChainEpoch(42)
		epoch2 := abi.ChainEpoch(93)
		epoch3 := abi.ChainEpoch(94)
		epoch4 := abi.ChainEpoch(203)

		require.NoError(t, queue.AddToQueueValues(epoch1, 1, 3))
		require.NoError(t, queue.AddToQueueValues(epoch2, 5))
		require.NoError(t, queue.AddToQueueValues(epoch3, 6, 7, 8))
		require.NoError(t, queue.AddToQueueValues(epoch4, 2, 4))

		// Required to ensure queue is in a sane state for PopUntil
		_, err := queue.Root()
		require.NoError(t, err)

		next, modified, err := queue.PopUntil(epoch2)
		require.NoError(t, err)
		// modified should be true to indicate queue has changed
		assert.True(t, modified)

		// values from first two epochs are returned
		assertBitfieldEquals(t, next, 1, 3, 5)

		// queue only contains remaining values
		ExpectBQ().
			Add(epoch3, 6, 7, 8).
			Add(epoch4, 2, 4).
			Equals(t, queue)

		// subsequent call to epoch less than next does nothing.
		next, modified, err = queue.PopUntil(epoch3 - 1)
		require.NoError(t, err)
		assert.False(t, modified)

		// no values are returned
		assertBitfieldEquals(t, next, []uint64{}...)

		// queue only contains remaining values
		ExpectBQ().
			Add(epoch3, 6, 7, 8).
			Add(epoch4, 2, 4).
			Equals(t, queue)

		// popping the rest of the queue gets the rest of the values
		next, modified, err = queue.PopUntil(epoch4)
		require.NoError(t, err)
		assert.True(t, modified)

		// rest of values are returned
		assertBitfieldEquals(t, next, 2, 4, 6, 7, 8)

		// queue is now empty
		ExpectBQ().
			Equals(t, queue)
	})

	t.Run("cuts elements", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch1 := abi.ChainEpoch(42)
		epoch2 := abi.ChainEpoch(93)

		require.NoError(t, queue.AddToQueueValues(epoch1, 1, 2, 3, 4, 99))
		require.NoError(t, queue.AddToQueueValues(epoch2, 5, 6))

		require.NoError(t, queue.Cut(bitfield.NewFromSet([]uint64{2, 4, 5, 6})))

		ExpectBQ().
			Add(epoch1, 1, 2, 95). // 3 shifts down to 2, 99 down to 95
			Equals(t, queue)
	})

}

func emptyBitfieldQueueWithQuantizing(t *testing.T, quant miner.QuantSpec) miner.BitfieldQueue {
	rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	queue, err := miner.LoadBitfieldQueue(store, root, quant)
	require.NoError(t, err)
	return queue
}

func emptyBitfieldQueue(t *testing.T) miner.BitfieldQueue {
	return emptyBitfieldQueueWithQuantizing(t, miner.NoQuantization)
}

type bqExpectation struct {
	expected map[abi.ChainEpoch][]uint64
}

func ExpectBQ() *bqExpectation {
	return &bqExpectation{expected: make(map[abi.ChainEpoch][]uint64)}
}

func (bqe *bqExpectation) Add(epoch abi.ChainEpoch, values ...uint64) *bqExpectation {
	bqe.expected[epoch] = values
	return bqe
}

func (bqe *bqExpectation) Equals(t *testing.T, q miner.BitfieldQueue) {
	// ensure cached changes are ready to be iterated
	_, err := q.Root()
	require.NoError(t, err)

	assert.Equal(t, uint64(len(bqe.expected)), q.Length())

	err = q.ForEach(func(epoch abi.ChainEpoch, bf bitfield.BitField) error {
		values, ok := bqe.expected[epoch]
		require.True(t, ok)

		assertBitfieldEquals(t, bf, values...)
		return nil
	})
	require.NoError(t, err)
}
