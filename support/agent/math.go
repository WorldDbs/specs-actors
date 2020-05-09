package agent

import (
	"math"
	big2 "math/big"
	"math/rand"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
)

var DisbursedAmount = big.Mul(big.NewInt(41e6), big.NewInt(1e18))

// RateIterator can be used to model poisson process (a process with discreet events occurring at
// arbitrary times with a specified average rate). It's Tick function must be called at regular
// intervals with a function that will be called zero or more times to produce the event distribution
// at the correct rate.
type RateIterator struct {
	rnd            *rand.Rand
	rate           float64
	nextOccurrence float64
}

func NewRateIterator(rate float64, seed int64) *RateIterator {
	rnd := rand.New(rand.NewSource(seed))
	next := 1.0
	if rate > 0.0 {
		next += poissonDelay(rnd.Float64(), rate)
	}

	return &RateIterator{
		rnd:  rnd,
		rate: rate,

		// choose first event in next tick
		nextOccurrence: next,
	}
}

// simulate random occurrences by calling the given function once for each event that would land in this epoch.
// The function will be called `rate` times on average, but may be called zero or many times in any Tick.
func (ri *RateIterator) Tick(f func() error) error {
	// wait until we have a positive rate before doing anything
	if ri.rate <= 0.0 {
		return nil
	}

	// next tick becomes this tick
	ri.nextOccurrence -= 1.0

	// choose events can call function until event occurs in next tick
	for ri.nextOccurrence < 1.0 {
		err := f()
		if err != nil {
			return err
		}

		// Choose next event
		// Note the argument to Log is <= 1, so the right side is always negative and nextOccurrence increases
		ri.nextOccurrence += poissonDelay(ri.rnd.Float64(), ri.rate)
	}
	return nil
}

// TickWithRate permits a variable rate.
// If the rate has changed, it will compute a new next occurrence before running tick.
// This prevents having to wait a long time to recognize a change from a very slow rate to a higher one.
func (ri *RateIterator) TickWithRate(rate float64, f func() error) error {
	// recompute next occurrence if rate has changed
	if ri.rate != rate && rate > 0.0 {
		ri.nextOccurrence = 1.0 + poissonDelay(ri.rnd.Float64(), rate)
	}
	ri.rate = rate

	return ri.Tick(f)
}

// Compute a poisson distributed delay that produces (on average) a given rate.
// The uniformRnd is a real number uniformly distributed in [0, 1).
// The rate is the average number of events expected per epoch and may be greater or less than 1 but not zero.
func poissonDelay(uniformRnd float64, rate float64) float64 {
	return -math.Log(1.0-uniformRnd) / rate
}

///////////////////////////////////////
//
//  Win Count
//
///////////////////////////////////////

// This is the Filecoin algorithm for winning a ticket within a block with the tickets replaced
// with random numbers. It lets miners win according to a Poisson distribution with rate
// proportional to the miner's fraction of network power.
func WinCount(minerPower abi.StoragePower, totalPower abi.StoragePower, random float64) uint64 {
	E := big2.NewRat(5, 1)
	lambdaR := new(big2.Rat)
	lambdaR.SetFrac(minerPower.Int, totalPower.Int)
	lambdaR.Mul(lambdaR, E)
	lambda, _ := lambdaR.Float64()

	rhs := 1 - poissonPMF(lambda, 0)

	winCount := uint64(0)
	for rhs > random {
		winCount++
		rhs -= poissonPMF(lambda, winCount)
	}
	return winCount
}

//////////////////////////////////////////
//
//  Misc
//
//////////////////////////////////////////

// this panics if list is empty.
func PopRandom(list []uint64, rnd *rand.Rand) (uint64, []uint64) {
	idx := rnd.Int63n(int64(len(list)))
	result := list[idx]
	list[idx] = list[len(list)-1]
	return result, list[:len(list)-1]
}

func poissonPMF(lambda float64, k uint64) float64 {
	fk := float64(k)
	return (math.Exp(-lambda) * math.Pow(lambda, fk)) / fact(fk)
}

func fact(k float64) float64 {
	fact := 1.0
	for i := 2.0; i <= k; i += 1.0 {
		fact *= i
	}
	return fact
}
