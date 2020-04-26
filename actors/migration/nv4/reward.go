package nv4

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	reward0 "github.com/filecoin-project/specs-actors/actors/builtin/reward"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	reward2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	math2 "github.com/filecoin-project/specs-actors/v2/actors/util/math"
	smoothing2 "github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
)

var (
	// Q.128 coefficients used to compute new effective network time
	// (g*Tau is the BaselineExponent, B0 is the BaselineInitialValue)
	gTauOverB0  big.Int
	oneOverGTau big.Int
)

func init() {
	constStrs := []string{
		"77669179383316",
		"516058975841646949912583456815941211282956743",
	}
	constBigs := math2.Parse(constStrs)
	gTauOverB0 = big.NewFromGo(constBigs[0]) // Q.128
	oneOverGTau = big.NewFromGo(constBigs[1])
}

type rewardMigrator struct {
	actorsOut *states.Tree
}

func (m rewardMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, migInfo MigrationInfo) (*StateMigrationResult, error) {
	var inState reward0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	// Get migrated power actor's ThisEpochRawBytePower
	outPowerAct, found, err := m.actorsOut.GetActor(builtin2.StoragePowerActorAddr)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, xerrors.Errorf("could not find power actor in migrated state")
	}
	var outPowerSt power2.State
	if err := store.Get(ctx, outPowerAct.Head, &outPowerSt); err != nil {
		return nil, xerrors.Errorf("failed to read migrated power actor state %w", err)
	}
	thisEpochRawBytePower := outPowerSt.ThisEpochRawBytePower

	one := big.Lsh(big.NewInt(1), math2.Precision128) // Q.128
	// Set baseline to the value it would be at the end of the prior epoch if
	// current parameters had been in place at network start
	outThisEpochBaselinePower := math2.ExpBySquaring(reward2.BaselineExponent, int64(migInfo.priorEpoch)) // Q.128
	outThisEpochBaselinePower = big.Mul(reward2.BaselineInitialValue, outThisEpochBaselinePower)          // Q.0 * Q.128 => Q.128
	outThisEpochBaselinePower = big.Rsh(outThisEpochBaselinePower, math2.Precision128)                    // Q.128 => Q.0

	// Set effective network power to the value it would be at the end of the
	// prior epoch if the current parameters had been in place at network start
	cumSumRealized := big.Lsh(inState.CumsumRealized, math2.Precision128) // Q.0 => Q.128
	acc0 := big.Mul(cumSumRealized, gTauOverB0)                           // Q.128 * Q.128 => Q.256
	acc0 = big.Rsh(acc0, math2.Precision128)                              // Q.256 => Q.128
	acc0 = big.Add(acc0, one)
	acc0 = math2.Ln(acc0)
	acc0 = big.Mul(acc0, oneOverGTau)                               // Q.128 * Q.128 => Q.256
	postUpgradeTheta := big.Rsh(acc0, math2.Precision128)           // Q.256 => Q.128
	effNetworkTime := big.Rsh(postUpgradeTheta, math2.Precision128) // Q.128 => Q.0
	effNetworkTime = big.Add(effNetworkTime, big.NewInt(1))
	if !effNetworkTime.Int.IsInt64() {
		return nil, xerrors.Errorf("new effective network time chain epoch out of bounds %v", effNetworkTime)
	}
	outEffectiveNetworkTime := abi.ChainEpoch(effNetworkTime.Int.Int64())

	// Set effective baseline power given the new effective network time and
	// baseline function.
	outEffectiveBaselinePower := math2.ExpBySquaring(reward2.BaselineExponent, int64(outEffectiveNetworkTime)) // Q.128
	outEffectiveBaselinePower128 := big.Mul(reward2.BaselineInitialValue, outEffectiveBaselinePower)           // Q.0 * Q.128 => Q.128
	outEffectiveBaselinePower = big.Rsh(outEffectiveBaselinePower128, math2.Precision128)                      // Q.128 => Q.0

	// Set cumsum baseline given new effective network time and baseline function
	outCumSumBaseline := big.Sub(outEffectiveBaselinePower128, big.Lsh(reward2.BaselineInitialValue, math2.Precision128)) // Q.128 - Q.128 => Q.128
	outCumSumBaseline = big.Mul(outCumSumBaseline, oneOverGTau)                                                           // Q.128 * Q.128 => Q.256
	outCumSumBaseline = big.Rsh(outCumSumBaseline, 2*math2.Precision128)                                                  // Q.256 => Q.0

	// Reduce baseline total so that the amount of tokens remaining to be minted is held fixed across the upgrade
	preUpgradeTheta := reward2.ComputeRTheta(inState.EffectiveNetworkTime, inState.EffectiveBaselinePower, inState.CumsumRealized, inState.CumsumBaseline) // Q.128
	thetaDiff := big.Sub(preUpgradeTheta, postUpgradeTheta)
	baselineAdjustmentExp := big.Mul(reward2.Lambda, thetaDiff)                // Q.128 * Q.128 => Q.256
	baselineAdjustmentExp = big.Rsh(baselineAdjustmentExp, math2.Precision128) // Q.256 => Q.128
	baselineAdjustment := big.NewFromGo(math2.ExpNeg(baselineAdjustmentExp.Int))
	outBaselineTotal := big.Mul(reward0.BaselineTotal, baselineAdjustment) // Q.0 * Q.128 => Q.128
	outBaselineTotal = big.Rsh(outBaselineTotal, math2.Precision128)       // Q.128 => Q.0

	// Set reward filter postiion and velocity values

	// Filter position is set by evaluating the closed form expression for expected
	// block reward at a particular epoch assuming that the new baseline function
	// had been in place at network start
	acc1 := big.Mul(reward2.BaselineInitialValue, oneOverGTau) // Q.0 * Q.128 => Q.128
	acc1 = big.Add(acc1, big.Lsh(inState.CumsumRealized, math2.Precision128))
	rawBytePower256 := big.Lsh(thisEpochRawBytePower, 2*math2.Precision128) // Q.0 => Q.256
	acc1 = big.Div(rawBytePower256, acc1)                                   // Q.256 / Q.128 => Q.128
	acc1 = big.Add(acc1, one)
	acc1 = math2.Ln(acc1)
	acc1 = big.Lsh(acc1, math2.Precision128)                         // Q.128 => Q.256
	acc1 = big.Div(acc1, big.Lsh(big.NewInt(6), math2.Precision128)) // Q.256 / Q.128 => Q.128
	acc1 = big.NewFromGo(math2.ExpNeg(acc1.Int))
	acc1 = big.Sub(one, acc1)
	acc2 := big.Mul(postUpgradeTheta, reward2.Lambda) // Q.128 * Q.128 => Q.256
	acc2 = big.Rsh(acc2, math2.Precision128)          // Q.256 => Q.128
	acc2 = big.NewFromGo(math2.ExpNeg(acc2.Int))
	acc1 = big.Mul(acc1, acc2)                                             //Q.128 * Q.128 => Q.256
	acc1 = big.Rsh(acc1, math2.Precision128)                               // Q.256 => Q.128
	acc1 = big.Mul(acc1, outBaselineTotal)                                 // Q.128 * Q.0 => Q.128
	acc3 := big.Mul(big.NewInt(int64(migInfo.priorEpoch)), reward2.Lambda) // Q.0 * Q.128 => Q.128
	acc3 = big.NewFromGo(math2.ExpNeg(acc3.Int))
	acc3 = big.Mul(acc3, reward0.SimpleTotal)  // Q.128 * Q.0 => Q.128
	acc3 = big.Mul(acc3, reward2.ExpLamSubOne) // Q.128 * Q.128 => Q.256
	acc3 = big.Rsh(acc3, math2.Precision128)   // Q.256 => Q.128
	outRewardSmoothedPosition := big.Add(acc1, acc3)

	// Filter velocity precision is less important. Hard coded value of 31 x 10^51 is sufficient
	outRewardSmoothedVelocity := big.Mul(
		big.NewInt(31),
		big.Exp(big.NewInt(10), big.NewInt(51)),
	) // Q.128
	outThisEpochRewardSmoothed := smoothing2.FilterEstimate{
		PositionEstimate: outRewardSmoothedPosition,
		VelocityEstimate: outRewardSmoothedVelocity,
	}

	outState := reward2.State{
		CumsumBaseline:          outCumSumBaseline,
		CumsumRealized:          inState.CumsumRealized,
		EffectiveNetworkTime:    outEffectiveNetworkTime,
		EffectiveBaselinePower:  outEffectiveBaselinePower,
		ThisEpochReward:         inState.ThisEpochReward,
		ThisEpochRewardSmoothed: outThisEpochRewardSmoothed,
		ThisEpochBaselinePower:  outThisEpochBaselinePower,
		Epoch:                   inState.Epoch,
		TotalStoragePowerReward: inState.TotalMined,
		SimpleTotal:             reward0.SimpleTotal,
		BaselineTotal:           outBaselineTotal,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}
