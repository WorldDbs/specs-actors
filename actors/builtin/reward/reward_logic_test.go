package reward

import (
	"bytes"
	"fmt"
	gbig "math/big"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/xorcare/golden"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/math"
)

func q128ToF(x big.Int) float64 {
	q128 := new(gbig.Int).SetInt64(1)
	q128 = q128.Lsh(q128, math.Precision128)
	res, _ := new(gbig.Rat).SetFrac(x.Int, q128).Float64()
	return res
}

func TestComputeRTeta(t *testing.T) {
	baselinePowerAt := func(epoch abi.ChainEpoch) abi.StoragePower {
		return big.Mul(big.NewInt(int64(epoch+1)), big.NewInt(2048))
	}

	assert.Equal(t, 0.5, q128ToF(ComputeRTheta(1, baselinePowerAt(1), big.NewInt(2048+2*2048*0.5), big.NewInt(2048+2*2048))))
	assert.Equal(t, 0.25, q128ToF(ComputeRTheta(1, baselinePowerAt(1), big.NewInt(2048+2*2048*0.25), big.NewInt(2048+2*2048))))

	cumsum15 := big.NewInt(0)
	for i := abi.ChainEpoch(0); i < 16; i++ {
		cumsum15 = big.Add(cumsum15, baselinePowerAt(i))
	}
	assert.Equal(t, 15.25, q128ToF(ComputeRTheta(16,
		baselinePowerAt(16),
		big.Add(cumsum15, big.Div(baselinePowerAt(16), big.NewInt(4))),
		big.Add(cumsum15, baselinePowerAt(16)))))
}

func TestBaselineReward(t *testing.T) {
	step := gbig.NewInt(5000)
	step = step.Lsh(step, math.Precision128)
	step = step.Sub(step, gbig.NewInt(77777777777)) // offset from full integers

	delta := gbig.NewInt(1)
	delta = delta.Lsh(delta, math.Precision128)
	delta = delta.Sub(delta, gbig.NewInt(33333333333)) // offset from full integers

	prevTheta := new(gbig.Int)
	theta := new(gbig.Int).Set(delta)

	b := &bytes.Buffer{}
	b.WriteString("t0, t1, y\n")
	simple := computeReward(0, big.Zero(), big.Zero(), DefaultSimpleTotal, DefaultBaselineTotal)

	for i := 0; i < 512; i++ {
		reward := computeReward(0, big.NewFromGo(prevTheta), big.NewFromGo(theta), DefaultSimpleTotal, DefaultBaselineTotal)
		reward = big.Sub(reward, simple)
		fmt.Fprintf(b, "%s,%s,%s\n", prevTheta, theta, reward.Int)
		prevTheta = prevTheta.Add(prevTheta, step)
		theta = theta.Add(theta, step)
	}

	golden.Assert(t, b.Bytes())
}

func TestSimpleReward(t *testing.T) {
	b := &bytes.Buffer{}
	b.WriteString("x, y\n")
	for i := int64(0); i < 512; i++ {
		x := i * 5000
		reward := computeReward(abi.ChainEpoch(x), big.Zero(), big.Zero(), DefaultSimpleTotal, DefaultBaselineTotal)
		fmt.Fprintf(b, "%d,%s\n", x, reward.Int)
	}

	golden.Assert(t, b.Bytes())
}

func TestBaselineRewardGrowth(t *testing.T) {

	baselineInYears := func(start abi.StoragePower, x abi.ChainEpoch) abi.StoragePower {
		baseline := start
		for i := abi.ChainEpoch(0); i < x*builtin.EpochsInYear; i++ {
			baseline = BaselinePowerFromPrev(baseline)
		}
		return baseline
	}

	// Baseline reward should have 100% growth rate
	// This implies that for every year x, the baseline function should be:
	// StartVal * 2^x.
	//
	// Error values for 1 years of growth were determined empirically with latest
	// baseline power construction to set bounds in this test in order to
	// 1. throw a test error if function changes and percent error goes up
	// 2. serve as documentation of current error bounds
	type growthTestCase struct {
		StartVal abi.StoragePower
		ErrBound float64
	}
	cases := []growthTestCase{
		// 1 byte
		{
			abi.NewStoragePower(1),
			1,
		},
		// GiB
		{
			abi.NewStoragePower(1 << 30),
			1e-3,
		},
		// TiB
		{
			abi.NewStoragePower(1 << 40),
			1e-6,
		},
		// PiB
		{
			abi.NewStoragePower(1 << 50),
			1e-8,
		},
		// EiB
		{
			BaselineInitialValue,
			1e-8,
		},
		// ZiB
		{
			big.Lsh(big.NewInt(1), 70),
			1e-8,
		},
		// non power of 2 ~ 1 EiB
		{
			abi.NewStoragePower(513633559722596517),
			1e-8,
		},
	}
	for _, testCase := range cases {
		years := int64(1)
		end := baselineInYears(testCase.StartVal, abi.ChainEpoch(1))

		multiplier := big.Exp(big.NewInt(2), big.NewInt(years)) // keeping this generalized in case we want to test more years
		expected := big.Mul(testCase.StartVal, multiplier)
		diff := big.Sub(expected, end)

		perrFrac := gbig.NewRat(1, 1).SetFrac(diff.Int, expected.Int)
		perr, _ := perrFrac.Float64()

		assert.Less(t, perr, testCase.ErrBound)
	}
}
