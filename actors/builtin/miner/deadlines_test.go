package miner_test

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
)

func TestProvingPeriodDeadlines(t *testing.T) {

	t.Run("quantization spec rounds to the next deadline", func(t *testing.T) {
		periodStart := abi.ChainEpoch(2)
		curr := periodStart + miner.WPoStProvingPeriod
		d := miner.NewDeadlineInfo(periodStart, 10, curr)
		quant := miner.QuantSpecForDeadline(d)
		assert.Equal(t, d.NextNotElapsed().Last(), quant.QuantizeUp(curr))
	})
}

func TestDeadlineInfoFromOffsetAndEpoch(t *testing.T) {

	// All proving periods equivalent mod WPoStProving period should give equivalent
	// dlines for a given epoch. Only the offset property should matter
	t.Run("Offset and epoch invariant checking", func(t *testing.T) {
		pp := abi.ChainEpoch(1972)
		ppThree := abi.ChainEpoch(1972 + 2880*3)
		ppMillion := abi.ChainEpoch(1972 + 2880*10e6)

		epochs := []abi.ChainEpoch{4, 2000, 400000, 5000000}
		for _, epoch := range epochs {
			dlineA := miner.NewDeadlineInfoFromOffsetAndEpoch(pp, epoch)
			dlineB := miner.NewDeadlineInfoFromOffsetAndEpoch(ppThree, epoch)
			dlineC := miner.NewDeadlineInfoFromOffsetAndEpoch(ppMillion, epoch)

			assert.Equal(t, *dlineA, *dlineB)
			assert.Equal(t, *dlineB, *dlineC)
		}
	})
	t.Run("sanity checks", func(t *testing.T) {
		offset := abi.ChainEpoch(7)
		start := abi.ChainEpoch(2880*103) + offset
		// epoch 2880*103 + offset we are in deadline 0, pp start = 2880*103 + offset
		dline := miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start)
		assert.Equal(t, uint64(0), dline.Index)
		assert.Equal(t, start, dline.PeriodStart)

		// epoch 2880*103 + offset + WPoStChallengeWindow - 1 we are in deadline 0
		dline = miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start+miner.WPoStChallengeWindow-1)
		assert.Equal(t, uint64(0), dline.Index)
		assert.Equal(t, start, dline.PeriodStart)

		// epoch 2880*103 + offset + WPoStChallengeWindow we are in deadline 1
		dline = miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start+miner.WPoStChallengeWindow)
		assert.Equal(t, uint64(1), dline.Index)
		assert.Equal(t, start, dline.PeriodStart)

		// epoch 2880*103 + offset + 40*WPoStChallengeWindow we are in deadline 40
		dline = miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start+40*miner.WPoStChallengeWindow)
		assert.Equal(t, uint64(40), dline.Index)
		assert.Equal(t, start, dline.PeriodStart)

		// epoch 2880*103 + offset + 40*WPoStChallengeWindow - 1 we are in deadline 39
		dline = miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start+40*miner.WPoStChallengeWindow-1)
		assert.Equal(t, uint64(39), dline.Index)
		assert.Equal(t, start, dline.PeriodStart)

		// epoch 2880*103 + offset + 40*WPoStChallengeWindow + 1 we are in deadline 40
		dline = miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start+40*miner.WPoStChallengeWindow+1)
		assert.Equal(t, uint64(40), dline.Index)
		assert.Equal(t, start, dline.PeriodStart)

		// epoch 2880*103 + offset + WPoStPeriodDeadlines*WPoStChallengeWindow -1 we are in deadline WPoStPeriodDeadlines - 1
		dline = miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start+abi.ChainEpoch(miner.WPoStPeriodDeadlines)*miner.WPoStChallengeWindow-abi.ChainEpoch(1))
		assert.Equal(t, uint64(miner.WPoStPeriodDeadlines-1), dline.Index)
		assert.Equal(t, start, dline.PeriodStart)

		// epoch 2880*103 + offset + WPoStPeriodDeadlines*WPoStChallengeWindow + 1 we are in deadline 0, pp start = 2880*104 + offset
		dline = miner.NewDeadlineInfoFromOffsetAndEpoch(offset, start+abi.ChainEpoch(miner.WPoStPeriodDeadlines)*miner.WPoStChallengeWindow)
		assert.Equal(t, uint64(0), dline.Index)
		assert.Equal(t, start+miner.WPoStProvingPeriod, dline.PeriodStart)

	})
}
