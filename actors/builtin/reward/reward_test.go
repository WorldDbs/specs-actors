package reward_test

import (
	"context"
	"testing"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, reward.Actor{})
}

const EpochZeroReward = "36266264293777134739"

func TestConstructor(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}

	t.Run("construct with 0 power", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower := abi.NewStoragePower(0)
		actor.constructAndVerify(rt, &startRealizedPower)
		st := getState(rt)
		assert.Equal(t, abi.ChainEpoch(0), st.Epoch)
		assert.Equal(t, abi.NewStoragePower(0), st.CumsumRealized)
		assert.Equal(t, big.MustFromString(EpochZeroReward), st.ThisEpochReward)
		epochZeroBaseline := big.Sub(reward.BaselineInitialValue, big.NewInt(1)) // account for rounding error of one byte during construction
		assert.Equal(t, epochZeroBaseline, st.ThisEpochBaselinePower)
		assert.Equal(t, reward.BaselineInitialValue, st.EffectiveBaselinePower)
	})
	t.Run("construct with less power than baseline", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower := big.Lsh(abi.NewStoragePower(1), 39)
		actor.constructAndVerify(rt, &startRealizedPower)
		st := getState(rt)
		assert.Equal(t, abi.ChainEpoch(0), st.Epoch)
		assert.Equal(t, startRealizedPower, st.CumsumRealized)

		assert.NotEqual(t, big.Zero(), st.ThisEpochReward)
	})
	t.Run("construct with more power than baseline", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower := reward.BaselineInitialValue
		actor.constructAndVerify(rt, &startRealizedPower)
		st := getState(rt)
		rwrd := st.ThisEpochReward

		// start with 2x power
		rt = mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower = big.Mul(reward.BaselineInitialValue, big.NewInt(2))
		actor.constructAndVerify(rt, &startRealizedPower)
		newSt := getState(rt)
		// Reward value is the same; realized power impact on reward is capped at baseline
		assert.Equal(t, rwrd, newSt.ThisEpochReward)
	})

}

func TestAwardBlockReward(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}
	winner := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("rejects gas reward exceeding balance", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(0)
		actor.constructAndVerify(rt, &startRealizedPower)

		rt.SetBalance(abi.NewTokenAmount(9))
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			gasReward := big.NewInt(10)
			actor.awardBlockReward(rt, winner, big.Zero(), gasReward, 1, big.Zero())
		})
	})

	t.Run("rejects negative penalty or reward", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(0)
		actor.constructAndVerify(rt, &startRealizedPower)

		rt.SetBalance(abi.NewTokenAmount(1e18))
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			penalty := big.NewInt(-1)
			actor.awardBlockReward(rt, winner, penalty, big.Zero(), 1, big.Zero())
		})
		rt.Reset()
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			gasReward := big.NewInt(-1)
			actor.awardBlockReward(rt, winner, big.Zero(), gasReward, 1, big.Zero())
		})
	})

	t.Run("rejects zero wincount", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(0)
		actor.constructAndVerify(rt, &startRealizedPower)

		rt.SetBalance(abi.NewTokenAmount(1e18))
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.awardBlockReward(rt, winner, big.Zero(), big.Zero(), 0, big.Zero())
		})
		rt.Reset()
	})

	t.Run("pays reward and tracks penalty", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(0)
		actor.constructAndVerify(rt, &startRealizedPower)

		rt.SetBalance(big.Mul(big.NewInt(1e9), abi.NewTokenAmount(1e18)))
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		penalty := big.NewInt(100)
		gasReward := big.NewInt(200)
		expectedReward := big.Sum(big.Div(big.MustFromString(EpochZeroReward), big.NewInt(5)), gasReward)
		actor.awardBlockReward(rt, winner, penalty, gasReward, 1, expectedReward)
		rt.Reset()
	})

	t.Run("pays out current balance when reward exceeds total balance", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(1)
		actor.constructAndVerify(rt, &startRealizedPower)

		// Total reward is a huge number, upon writing ~1e18, so 300 should be way less
		smallReward := abi.NewTokenAmount(300)
		penalty := abi.NewTokenAmount(100)
		rt.SetBalance(smallReward)
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)

		minerPenalty := big.Mul(big.NewInt(reward.PenaltyMultiplier), penalty)
		expectedParams := builtin.ApplyRewardParams{Reward: smallReward, Penalty: minerPenalty}
		rt.ExpectSend(winner, builtin.MethodsMiner.ApplyRewards, &expectedParams, smallReward, nil, 0)
		rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
			Miner:     winner,
			Penalty:   penalty,
			GasReward: big.Zero(),
			WinCount:  1,
		})
		rt.Verify()
	})

	t.Run("TotalStoragePowerReward tracks correctly", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(1)
		actor.constructAndVerify(rt, &startRealizedPower)
		miner := tutil.NewIDAddr(t, 1000)

		st := getState(rt)
		assert.Equal(t, big.Zero(), st.TotalStoragePowerReward)
		st.ThisEpochReward = abi.NewTokenAmount(5000)
		rt.ReplaceState(st)
		// enough balance to pay 3 full rewards and one partial
		totalPayout := abi.NewTokenAmount(3500)
		rt.SetBalance(totalPayout)

		// award normalized by expected leaders is 1000
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(1000))
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(1000))
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(1000))
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(500)) // partial payout when balance below reward

		newState := getState(rt)
		assert.Equal(t, totalPayout, newState.TotalStoragePowerReward)

	})

	t.Run("funds are sent to the burnt funds actor if sending locked funds to miner fails", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(1)
		actor.constructAndVerify(rt, &startRealizedPower)
		miner := tutil.NewIDAddr(t, 1000)
		st := getState(rt)
		assert.Equal(t, big.Zero(), st.TotalStoragePowerReward)
		st.ThisEpochReward = abi.NewTokenAmount(5000)
		rt.ReplaceState(st)
		// enough balance to pay 3 full rewards and one partial
		totalPayout := abi.NewTokenAmount(3500)
		rt.SetBalance(totalPayout)

		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		expectedReward := big.NewInt(1000)
		penalty := big.Zero()
		expectedParams := builtin.ApplyRewardParams{Reward: expectedReward, Penalty: penalty}
		rt.ExpectSend(miner, builtin.MethodsMiner.ApplyRewards, &expectedParams, expectedReward, nil, exitcode.ErrForbidden)
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectedReward, nil, exitcode.Ok)

		rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
			Miner:     miner,
			Penalty:   big.Zero(),
			GasReward: big.Zero(),
			WinCount:  1,
		})

		rt.Verify()
	})
}

func TestThisEpochReward(t *testing.T) {
	t.Run("successfully fetch reward for this epoch", func(t *testing.T) {
		actor := rewardHarness{reward.Actor{}, t}
		builder := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
		rt := builder.Build(t)
		power := abi.NewStoragePower(1 << 50)
		actor.constructAndVerify(rt, &power)

		resp := actor.thisEpochReward(rt)
		st := getState(rt)

		require.EqualValues(t, st.ThisEpochBaselinePower, resp.ThisEpochBaselinePower)
		require.EqualValues(t, st.ThisEpochRewardSmoothed, resp.ThisEpochRewardSmoothed)
	})
}

func TestSuccessiveKPIUpdates(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}
	builder := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
	rt := builder.Build(t)
	power := abi.NewStoragePower(1 << 50)
	actor.constructAndVerify(rt, &power)

	rt.SetEpoch(abi.ChainEpoch(1))
	actor.updateNetworkKPI(rt, &power)

	rt.SetEpoch(abi.ChainEpoch(2))
	actor.updateNetworkKPI(rt, &power)

	rt.SetEpoch(abi.ChainEpoch(3))
	actor.updateNetworkKPI(rt, &power)

}

type rewardHarness struct {
	reward.Actor
	t testing.TB
}

func (h *rewardHarness) constructAndVerify(rt *mock.Runtime, currRawPower *abi.StoragePower) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, currRawPower)
	assert.Nil(h.t, ret)
	rt.Verify()

}

func (h *rewardHarness) updateNetworkKPI(rt *mock.Runtime, currRawPower *abi.StoragePower) {
	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	ret := rt.Call(h.UpdateNetworkKPI, currRawPower)
	assert.Nil(h.t, ret)
	rt.Verify()
}

func (h *rewardHarness) awardBlockReward(rt *mock.Runtime, miner address.Address, penalty, gasReward abi.TokenAmount, winCount int64, expectedPayment abi.TokenAmount) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	// expect penalty multiplier
	minerPenalty := big.Mul(big.NewInt(reward.PenaltyMultiplier), penalty)
	expectedParams := builtin.ApplyRewardParams{Reward: expectedPayment, Penalty: minerPenalty}
	rt.ExpectSend(miner, builtin.MethodsMiner.ApplyRewards, &expectedParams, expectedPayment, nil, 0)

	rt.Call(h.AwardBlockReward, &reward.AwardBlockRewardParams{
		Miner:     miner,
		Penalty:   penalty,
		GasReward: gasReward,
		WinCount:  winCount,
	})
	rt.Verify()
}

func (h *rewardHarness) thisEpochReward(rt *mock.Runtime) *reward.ThisEpochRewardReturn {
	rt.ExpectValidateCallerAny()

	ret := rt.Call(h.ThisEpochReward, nil)
	rt.Verify()

	resp, ok := ret.(*reward.ThisEpochRewardReturn)
	require.True(h.t, ok)
	return resp
}

func getState(rt *mock.Runtime) *reward.State {
	var st reward.State
	rt.GetState(&st)
	return &st
}
