package states

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

// Checks for miner election eligibility.
// A miner must satisfy conditions on both the immediate parent state, as well as state at the
// Winning PoSt election lookback.

// Tests whether a miner is eligible to win an election given the immediately prior state of the
// miner and system actors.
func MinerEligibleForElection(store adt.Store, mstate *miner.State, pstate *power.State, maddr addr.Address, currEpoch abi.ChainEpoch) (bool, error) {
	// Non-empty power claim.
	if claim, found, err := pstate.GetClaim(store, maddr); err != nil {
		return false, err
	} else if !found {
		return false, err
	} else if claim.QualityAdjPower.LessThanEqual(big.Zero()) {
		return false, err
	}

	// No fee debt.
	if !mstate.IsDebtFree() {
		return false, nil
	}

	// No active consensus faults.
	if mInfo, err := mstate.GetInfo(store); err != nil {
		return false, err
	} else if miner.ConsensusFaultActive(mInfo, currEpoch) {
		return false, nil
	}

	return true, nil
}

// Tests whether a miner is eligible for election given a Winning PoSt lookback state.
// The power state must be the state of the power actor at Winning PoSt lookback epoch.
func MinerPoStLookbackEligibleForElection(store adt.Store, pstate *power.State, mAddr addr.Address) (bool, error) {
	// Minimum power requirements.
	return pstate.MinerNominalPowerMeetsConsensusMinimum(store, mAddr)
}