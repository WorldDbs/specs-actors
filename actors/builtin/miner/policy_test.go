package miner_test

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
)

func TestQuality(t *testing.T) {
	// Quality of space with no deals. This doesn't depend on either the sector size or duration.
	emptyQuality := big.NewIntUnsigned(1 << builtin.SectorQualityPrecision)
	// Quality space filled with non-verified deals.
	dealQuality := big.Mul(emptyQuality, big.Div(builtin.DealWeightMultiplier, builtin.QualityBaseMultiplier))
	// Quality space filled with verified deals.
	verifiedQuality := big.Mul(emptyQuality, big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier))

	t.Run("quality is independent of size and duration", func(t *testing.T) {
		for _, size := range []abi.SectorSize{1, 10, 1 << 10, 32 << 30, 1 << 40} {
			for _, duration := range []abi.ChainEpoch{1, 10, 1000, 1000 * builtin.EpochsInDay} {
				sectorWeight := weight(size, duration)
				assertEqual(t, emptyQuality, miner.QualityForWeight(size, duration, big.Zero(), big.Zero()))
				assertEqual(t, dealQuality, miner.QualityForWeight(size, duration, sectorWeight, big.Zero()))
				assertEqual(t, verifiedQuality, miner.QualityForWeight(size, duration, big.Zero(), sectorWeight))
			}
		}
	})

	t.Run("quality scales with verified weight proportion", func(t *testing.T) {
		sectorSize := abi.SectorSize(1 << 40)
		sectorDuration := abi.ChainEpoch(1_000_000) // ~350 days
		sectorWeight := weight(sectorSize, sectorDuration)

		for _, verifiedSpace := range []abi.SectorSize{0, 1, 1 << 10, 2 << 20, 5 << 20, 1 << 30, sectorSize - 1, sectorSize} {
			emptyWeight := weight(sectorSize-verifiedSpace, sectorDuration)
			verifiedWeight := weight(verifiedSpace, sectorDuration)
			assertEqual(t, big.Sub(sectorWeight, emptyWeight), verifiedWeight)

			// Expect sector quality to be a weighted sum of base and verified quality.
			eq := big.Mul(emptyWeight, emptyQuality)
			vq := big.Mul(verifiedWeight, verifiedQuality)
			expectedQuality := big.Div(big.Sum(eq, vq), sectorWeight)
			assertEqual(t, expectedQuality, miner.QualityForWeight(sectorSize, sectorDuration, big.Zero(), verifiedWeight))
		}
	})
}

func TestPower(t *testing.T) {
	t.Run("empty sector has power equal to size", func(t *testing.T) {
		for _, size := range []abi.SectorSize{1, 10, 1 << 10, 32 << 30, 1 << 40} {
			for _, duration := range []abi.ChainEpoch{1, 10, 1000, 1000 * builtin.EpochsInDay} {
				expectedPower := big.NewInt(int64(size))
				assert.Equal(t, expectedPower, miner.QAPowerForWeight(size, duration, big.Zero(), big.Zero()))
			}
		}
	})

	t.Run("verified sector has power a multiple of size", func(t *testing.T) {
		// Assumes the multiplier is an integer.
		verifiedMultiplier := big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier).Int64()
		for _, size := range []abi.SectorSize{1, 10, 1 << 10, 32 << 30, 1 << 40} {
			for _, duration := range []abi.ChainEpoch{1, 10, 1000, 1000 * builtin.EpochsInDay} {
				verifiedWeight := weight(size, duration)
				expectedPower := big.NewInt(int64(size) * verifiedMultiplier)
				assert.Equal(t, expectedPower, miner.QAPowerForWeight(size, duration, big.Zero(), verifiedWeight))
			}
		}
	})

	t.Run("verified weight adds proportional power", func(t *testing.T) {
		sectorSize := abi.SectorSize(1 << 40)
		sectorDuration := abi.ChainEpoch(180 * builtin.SecondsInDay)
		sectorWeight := weight(sectorSize, sectorDuration)

		fullyEmptyPower := big.NewInt(int64(sectorSize))
		fullyVerifiedPower := big.Div(big.Mul(big.NewInt(int64(sectorSize)), builtin.VerifiedDealWeightMultiplier), builtin.QualityBaseMultiplier)

		maxError := big.NewInt(1 << builtin.SectorQualityPrecision)

		for _, verifiedSpace := range []abi.SectorSize{0, 1, 1 << 10, 5 << 20, 32 << 30, sectorSize - 1, sectorSize} {
			for _, verifiedDuration := range []abi.ChainEpoch{0, 1, sectorDuration / 2, sectorDuration - 1, sectorDuration} {
				verifiedWeight := weight(verifiedSpace, verifiedDuration)
				emptyWeight := big.Sub(sectorWeight, verifiedWeight)

				// Expect sector power to be a weighted sum of base and verified power.
				ep := big.Mul(emptyWeight, fullyEmptyPower)
				vp := big.Mul(verifiedWeight, fullyVerifiedPower)
				expectedPower := big.Div(big.Sum(ep, vp), sectorWeight)
				power := miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), verifiedWeight)
				powerError := big.Sub(expectedPower, power)
				assert.True(t, powerError.LessThanEqual(maxError))
			}
		}
	})

	t.Run("demonstrate standard sectors", func(t *testing.T) {
		sectorDuration := abi.ChainEpoch(180 * builtin.EpochsInDay)
		vmul := big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier).Int64()

		{
			// 32 GiB
			sectorSize := abi.SectorSize(32 << 30)
			sectorWeight := weight(sectorSize, sectorDuration)

			assert.Equal(t, big.NewInt(1*32<<30), miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), big.Zero()))
			assert.Equal(t, big.NewInt(vmul*32<<30), miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), sectorWeight))
			halfVerifiedPower := big.NewInt((1 * 32 << 30 / 2) + (vmul * 32 << 30 / 2))
			assert.Equal(t, halfVerifiedPower, miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), big.Div(sectorWeight, big.NewInt(2))))
		}
		{
			// 64 GiB
			sectorSize := abi.SectorSize(64 << 30)
			sectorWeight := weight(sectorSize, sectorDuration)

			assert.Equal(t, big.NewInt(1*64<<30), miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), big.Zero()))
			assert.Equal(t, big.NewInt(vmul*64<<30), miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), sectorWeight))
			halfVerifiedPower := big.NewInt((1 * 64 << 30 / 2) + (vmul * 64 << 30 / 2))
			assert.Equal(t, halfVerifiedPower, miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), big.Div(sectorWeight, big.NewInt(2))))
		}
		{
			// 1 TiB
			sectorSize := abi.SectorSize(1 << 40)
			sectorWeight := weight(sectorSize, sectorDuration)

			assert.Equal(t, big.NewInt(1*1<<40), miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), big.Zero()))
			assert.Equal(t, big.NewInt(vmul*1<<40), miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), sectorWeight))
			halfVerifiedPower := big.NewInt((1 * 1 << 40 / 2) + (vmul * 1 << 40 / 2))
			assert.Equal(t, halfVerifiedPower, miner.QAPowerForWeight(sectorSize, sectorDuration, big.Zero(), big.Div(sectorWeight, big.NewInt(2))))
		}
	})
}

func weight(size abi.SectorSize, duration abi.ChainEpoch) big.Int {
	return big.Mul(big.NewIntUnsigned(uint64(size)), big.NewInt(int64(duration)))
}

func assertEqual(t *testing.T, a, b big.Int) {
	// Zero does not have a canonical representation, so check that explicitly.
	if !(a.IsZero() && b.IsZero()) {
		assert.Equal(t, a, b)
	}
}
