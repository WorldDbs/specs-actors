package smoothing_test

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/math"
	"github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
)

// project of cumsum ratio is equal to cumsum of ratio of projections
func TestCumSumRatioProjection(t *testing.T) {
	t.Run("constant estimate", func(t *testing.T) {
		numEstimate := smoothing.TestingConstantEstimate(big.NewInt(4e6))
		denomEstimate := smoothing.TestingConstantEstimate(big.NewInt(1))
		// 4e6/1 over 1000 epochs should give us 4e9
		csr := smoothing.ExtrapolatedCumSumOfRatio(abi.ChainEpoch(1000), abi.ChainEpoch(0), numEstimate, denomEstimate)
		csr = big.Rsh(csr, math.Precision128)
		assert.Equal(t, big.NewInt(4e9), csr)

		// if we change t0 nothing should change because velocity is 0
		csr2 := smoothing.ExtrapolatedCumSumOfRatio(abi.ChainEpoch(1000), abi.ChainEpoch(1e15), numEstimate, denomEstimate)
		csr2 = big.Rsh(csr2, math.Precision128)
		assert.Equal(t, csr, csr2)

		// 1e12 / 200e12 for 100 epochs should give ratio of 1/2
		numEstimate = smoothing.TestingConstantEstimate(big.NewInt(1e12))
		denomEstimate = smoothing.TestingConstantEstimate(big.NewInt(200e12))
		csrFrac := smoothing.ExtrapolatedCumSumOfRatio(abi.ChainEpoch(100), abi.ChainEpoch(0), numEstimate, denomEstimate)
		// If we didn't return Q.128 we'd just get zero
		assert.Equal(t, big.Zero(), big.Rsh(csrFrac, math.Precision128))
		// multiply by 10k and we'll get 5k
		// note: this is a bit sensative to input, lots of numbers approach from below
		// (...99999) and so truncating division takes us off by one
		product := big.Mul(csrFrac, big.Lsh(big.NewInt(10000), math.Precision128)) // Q.256
		assert.Equal(t, big.NewInt(5000), big.Rsh(product, 2*math.Precision128))
	})

	// Q.128 cumsum of ratio using the trapezoid rule
	iterativeCumSumOfRatio := func(num, denom smoothing.FilterEstimate, t0, delta abi.ChainEpoch) big.Int {
		ratio := big.Zero() // Q.128
		for i := abi.ChainEpoch(0); i < delta; i++ {
			numEpsilon := num.Extrapolate(t0 + i)                   // Q.256
			denomEpsilon := denom.Extrapolate(t0 + i)               // Q.256
			denomEpsilon = big.Rsh(denomEpsilon, math.Precision128) // Q.256 => Q.128
			epsilon := big.Div(numEpsilon, denomEpsilon)            // Q.256 / Q.128 => Q.128
			if i != abi.ChainEpoch(0) && i != delta-1 {
				epsilon = big.Mul(big.NewInt(2), epsilon) // Q.128 * Q.0 => Q.128
			}
			ratio = big.Sum(ratio, epsilon)
		}
		ratio = big.Div(ratio, big.NewInt(2)) // Q.128 / Q.0 => Q.128
		return ratio
	}

	// millionths of error difference
	// This error value was set after empirically seeing values in this range
	//
	// Note 1: when cumsum taken over small numbers of epochs error is much worse
	// Note 2: since both methods are approximations with error this is not a
	// measurement of analytic method's error, it is a sanity check that the
	// two methods give similar results
	errBound := big.NewInt(350)

	assertErrBound := func(t *testing.T, num, denom smoothing.FilterEstimate, delta, t0 abi.ChainEpoch, errBound big.Int) {
		t.Helper()
		analytic := smoothing.ExtrapolatedCumSumOfRatio(delta, t0, num, denom)
		iterative := iterativeCumSumOfRatio(num, denom, t0, delta)
		actualErr := perMillionError(analytic, iterative)
		assert.True(t, actualErr.LessThan(errBound),
			"expected %d, actual %d (error %d > %d)",
			iterative, analytic, actualErr, errBound)
	}

	t.Run("both positive velocity", func(t *testing.T) {
		numEstimate := smoothing.TestingEstimate(big.NewInt(111), big.NewInt(33))
		denomEstimate := smoothing.TestingEstimate(big.NewInt(3456), big.NewInt(8))
		delta := abi.ChainEpoch(10000)
		t0 := abi.ChainEpoch(0)
		assertErrBound(t, numEstimate, denomEstimate, delta, t0, errBound)
	})

	t.Run("flipped signs", func(t *testing.T) {
		numEstimate := smoothing.TestingEstimate(big.NewInt(1e6), big.NewInt(-100))
		denomEstimate := smoothing.TestingEstimate(big.NewInt(7e4), big.NewInt(1000))
		delta := abi.ChainEpoch(100000)
		t0 := abi.ChainEpoch(0)
		assertErrBound(t, numEstimate, denomEstimate, delta, t0, errBound)
	})

	t.Run("values in range we care about for BR", func(t *testing.T) {
		tensOfFIL := big.Mul(abi.NewTokenAmount(1e18), big.NewInt(50))
		oneFILPerSecond := big.NewInt(25)
		fourFILPerSecond := big.NewInt(100)
		slowMoney := smoothing.TestingEstimate(tensOfFIL, oneFILPerSecond)
		fastMoney := smoothing.TestingEstimate(tensOfFIL, fourFILPerSecond)

		tensOfEiBs := big.Mul(abi.NewStoragePower(1e18), big.NewInt(10))
		thousandsOfEiBs := big.Mul(abi.NewStoragePower(1e18), big.NewInt(2e4))

		oneBytePerEpochVelocity := big.NewInt(1)
		tenPiBsPerDayVelocity := big.Div(big.NewInt(10<<50), big.NewInt(int64(builtin.EpochsInDay)))
		oneEiBPerDayVelocity := big.Div(big.NewInt(1<<60), big.NewInt(int64(builtin.EpochsInDay)))

		delta := abi.ChainEpoch(builtin.EpochsInDay)
		t0 := abi.ChainEpoch(0)
		{
			// low power low velocity
			power := smoothing.TestingEstimate(tensOfEiBs, oneBytePerEpochVelocity)
			assertErrBound(t, slowMoney, power, delta, t0, errBound)
			assertErrBound(t, fastMoney, power, delta, t0, errBound)
		}

		{
			// low power mid velocity
			power := smoothing.TestingEstimate(tensOfEiBs, tenPiBsPerDayVelocity)
			assertErrBound(t, slowMoney, power, delta, t0, errBound)
			assertErrBound(t, fastMoney, power, delta, t0, errBound)
		}

		{
			// low power high velocity
			power := smoothing.TestingEstimate(tensOfEiBs, oneEiBPerDayVelocity)
			assertErrBound(t, slowMoney, power, delta, t0, errBound)
			assertErrBound(t, fastMoney, power, delta, t0, errBound)
		}

		{
			// high power low velocity
			power := smoothing.TestingEstimate(thousandsOfEiBs, oneBytePerEpochVelocity)
			assertErrBound(t, slowMoney, power, delta, t0, errBound)
			assertErrBound(t, fastMoney, power, delta, t0, errBound)
		}
		{
			// high power mid velocity
			power := smoothing.TestingEstimate(thousandsOfEiBs, tenPiBsPerDayVelocity)
			assertErrBound(t, slowMoney, power, delta, t0, errBound)
			assertErrBound(t, fastMoney, power, delta, t0, errBound)
		}
		{
			// high power high velocity
			power := smoothing.TestingEstimate(thousandsOfEiBs, oneEiBPerDayVelocity)
			assertErrBound(t, slowMoney, power, delta, t0, errBound)
			assertErrBound(t, fastMoney, power, delta, t0, errBound)
		}
	})

}

// Millionths of difference between val1 and val2
// (val1 - val2) / val1 * 1e6
// all inputs Q.128, output Q.0
func perMillionError(val1, val2 big.Int) big.Int {
	diff := big.Sub(val1, val2)

	diff = big.Lsh(diff, math.Precision128)                // Q.128 => Q.256
	perMillion := big.Div(diff, val1)                      // Q.256 / Q.128 => Q.128
	million := big.Lsh(big.NewInt(1e6), math.Precision128) // Q.0 => Q.128

	perMillion = big.Mul(perMillion, million) // Q.128 * Q.128 => Q.256
	if perMillion.LessThan(big.Zero()) {
		perMillion = perMillion.Neg()
	}
	return big.Rsh(perMillion, 2*math.Precision128)
}
