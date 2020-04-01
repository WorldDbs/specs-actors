package miner

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
)

func TestQuantizeUp(t *testing.T) {
	t.Run("no quantization", func(t *testing.T) {
		q := NoQuantization
		assert.Equal(t, abi.ChainEpoch(0), q.QuantizeUp(0))
		assert.Equal(t, abi.ChainEpoch(1), q.QuantizeUp(1))
		assert.Equal(t, abi.ChainEpoch(2), q.QuantizeUp(2))
		assert.Equal(t, abi.ChainEpoch(123456789), q.QuantizeUp(123456789))
	})
	t.Run("zero offset", func(t *testing.T) {
		assert.Equal(t, abi.ChainEpoch(50), quantizeUp(42, 10, 0))
		assert.Equal(t, abi.ChainEpoch(16000), quantizeUp(16000, 100, 0))
		assert.Equal(t, abi.ChainEpoch(0), quantizeUp(-5, 10, 0))
		assert.Equal(t, abi.ChainEpoch(-50), quantizeUp(-50, 10, 0))
		assert.Equal(t, abi.ChainEpoch(-50), quantizeUp(-53, 10, 0))
	})

	t.Run("non zero offset", func(t *testing.T) {
		assert.Equal(t, abi.ChainEpoch(6), quantizeUp(4, 5, 1))
		assert.Equal(t, abi.ChainEpoch(1), quantizeUp(0, 5, 1))
		assert.Equal(t, abi.ChainEpoch(-4), quantizeUp(-6, 5, 1))
		assert.Equal(t, abi.ChainEpoch(4), quantizeUp(2, 10, 4))
	})

	t.Run("offset seed bigger than unit is normalized", func(t *testing.T) {
		assert.Equal(t, abi.ChainEpoch(13), quantizeUp(9, 5, 28)) // offset should be 3
		assert.Equal(t, abi.ChainEpoch(10000), quantizeUp(10000, 100, 2000000))
	})
}
