package power_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	cid "github.com/ipfs/go-cid"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	mineract "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, power.Actor{})
}

func TestConstruction(t *testing.T) {
	actor := newHarness(t)
	owner := tutil.NewIDAddr(t, 101)
	miner := tutil.NewIDAddr(t, 103)
	actr := tutil.NewActorAddr(t, "actor")

	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
	})

	t.Run("create miner", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMiner(rt, owner, owner, miner, actr, abi.PeerID("miner"), []abi.Multiaddrs{{1}}, abi.RegisteredSealProof_StackedDrg2KiBV1, abi.NewTokenAmount(10))

		var st power.State
		rt.GetState(&st)
		assert.Equal(t, int64(1), st.MinerCount)
		assert.Equal(t, abi.NewStoragePower(0), st.TotalQualityAdjPower)
		assert.Equal(t, abi.NewStoragePower(0), st.TotalRawBytePower)
		assert.Equal(t, int64(0), st.MinerAboveMinPowerCount)

		claim, err := adt.AsMap(adt.AsStore(rt), st.Claims)
		assert.NoError(t, err)
		keys, err := claim.CollectKeys()
		require.NoError(t, err)
		assert.Equal(t, 1, len(keys))
		var actualClaim power.Claim
		found, err_ := claim.Get(asKey(keys[0]), &actualClaim)
		require.NoError(t, err_)
		assert.True(t, found)
		assert.Equal(t, power.Claim{big.Zero(), big.Zero()}, actualClaim) // miner has not proven anything

		verifyEmptyMap(t, rt, st.CronEventQueue)
	})
}

func TestCreateMinerFailures(t *testing.T) {
	owner := tutil.NewIDAddr(t, 101)
	peer := abi.PeerID("miner")
	mAddr := []abi.Multiaddrs{{1}}
	sealProofType := abi.RegisteredSealProof_StackedDrg2KiBV1

	t.Run("fails when caller is not of signable type", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		rt.SetCaller(owner, builtin.StorageMinerActorCodeID)
		rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.CreateMiner, &power.CreateMinerParams{})
		})
		rt.Verify()
	})

	t.Run("fails if send to Init Actor fails", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		createMinerParams := &power.CreateMinerParams{
			Owner:         owner,
			Worker:        owner,
			SealProofType: sealProofType,
			Peer:          peer,
			Multiaddrs:    mAddr,
		}

		// owner send CreateMiner to Actor
		rt.SetCaller(owner, builtin.AccountActorCodeID)
		rt.SetReceived(abi.NewTokenAmount(10))
		rt.SetBalance(abi.NewTokenAmount(10))
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)

		msgParams := &initact.ExecParams{
			CodeCID:           builtin.StorageMinerActorCodeID,
			ConstructorParams: initCreateMinerBytes(t, owner, owner, peer, mAddr, sealProofType),
		}
		expRet :=  initact.ExecReturn{
			IDAddress:  tutil.NewIDAddr(t, 1475),
			RobustAddress: tutil.NewActorAddr(t, "test"),
		}
		rt.ExpectSend(builtin.InitActorAddr, builtin.MethodsInit.Exec, msgParams, abi.NewTokenAmount(10),
			&expRet, exitcode.ErrInsufficientFunds)

		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			rt.Call(ac.CreateMiner, createMinerParams)
		})
	})
}

func TestUpdateClaimedPowerFailures(t *testing.T) {
	rawDelta := big.NewInt(100)
	qaDelta := big.NewInt(200)
	miner := tutil.NewIDAddr(t, 101)

	t.Run("fails if caller is not a StorageMinerActor", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		params := power.UpdateClaimedPowerParams{
			RawByteDelta:         rawDelta,
			QualityAdjustedDelta: qaDelta,
		}
		rt.SetCaller(miner, builtin.SystemActorCodeID)
		rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.UpdateClaimedPower, &params)
		})

		rt.Verify()
	})

	t.Run("fails if claim does not exist for caller", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		params := power.UpdateClaimedPowerParams{
			RawByteDelta:         rawDelta,
			QualityAdjustedDelta: qaDelta,
		}
		rt.SetCaller(miner, builtin.StorageMinerActorCodeID)
		rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			rt.Call(ac.UpdateClaimedPower, &params)
		})

		rt.Verify()
	})
}

func TestEnrollCronEpoch(t *testing.T) {
	miner := tutil.NewIDAddr(t, 101)

	t.Run("enroll multiple events", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		e1 := abi.ChainEpoch(1)

		// enroll event with miner 1
		p1 := []byte("hello")
		ac.enrollCronEvent(rt, miner, e1, p1)

		events := ac.getEnrolledCronTicks(rt, e1)

		evt := events[0]
		require.EqualValues(t, p1, evt.CallbackPayload)
		require.EqualValues(t, miner, evt.MinerAddr)

		// enroll another event with the same miner
		p2 := []byte("hello2")
		ac.enrollCronEvent(rt, miner, e1, p2)
		events = ac.getEnrolledCronTicks(rt, e1)
		evt = events[0]
		require.EqualValues(t, p1, evt.CallbackPayload)
		require.EqualValues(t, miner, evt.MinerAddr)

		evt = events[1]
		require.EqualValues(t, p2, evt.CallbackPayload)
		require.EqualValues(t, miner, evt.MinerAddr)

		// enroll another event with a different miner for a different epoch
		e2 := abi.ChainEpoch(2)
		p3 := []byte("test")
		miner2 := tutil.NewIDAddr(t, 501)
		ac.enrollCronEvent(rt, miner2, e2, p3)
		events = ac.getEnrolledCronTicks(rt, e2)
		evt = events[0]
		require.EqualValues(t, p3, evt.CallbackPayload)
		require.EqualValues(t, miner2, evt.MinerAddr)
	})

	t.Run("enroll for an epoch before the current epoch", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		// current epoch is 5
		current := abi.ChainEpoch(5)
		rt.SetEpoch(current)

		// enroll event with miner at epoch=2
		p1 := []byte("hello")
		e1 := abi.ChainEpoch(2)
		ac.enrollCronEvent(rt, miner, e1, p1)
		events := ac.getEnrolledCronTicks(rt, e1)
		evt := events[0]
		require.EqualValues(t, p1, evt.CallbackPayload)
		require.EqualValues(t, miner, evt.MinerAddr)
		st := getState(rt)
		require.EqualValues(t, abi.ChainEpoch(0), st.FirstCronEpoch)

		// enroll event with miner at epoch=1
		p2 := []byte("hello2")
		e2 := abi.ChainEpoch(1)
		ac.enrollCronEvent(rt, miner, e2, p2)
		events = ac.getEnrolledCronTicks(rt, e2)
		evt = events[0]
		require.EqualValues(t, p2, evt.CallbackPayload)
		require.EqualValues(t, miner, evt.MinerAddr)
		require.EqualValues(t, abi.ChainEpoch(0), st.FirstCronEpoch)
	})

	t.Run("fails if epoch is negative", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			ac.enrollCronEvent(rt, miner, abi.ChainEpoch(-1), []byte("payload"))
		})
	})
}

func TestOnConsensusFault(t *testing.T) {
	miner := tutil.NewIDAddr(t, 101)
	owner := tutil.NewIDAddr(t, 102)
	smallPowerUnit := big.NewInt(1_000_000)
	powerUnit := power.ConsensusMinerMinPower
	zeroPledge := abi.NewTokenAmount(0)

	t.Run("qaPower was below threshold before fault", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner)
		ac.updateClaimedPower(rt, miner, smallPowerUnit, smallPowerUnit)

		st := getState(rt)
		require.True(t, st.TotalRawBytePower.IsZero())
		require.True(t, st.TotalQualityAdjPower.IsZero())
		require.EqualValues(t, 0, st.MinerAboveMinPowerCount)
		require.EqualValues(t, smallPowerUnit, st.TotalBytesCommitted)
		require.EqualValues(t, smallPowerUnit, st.TotalQABytesCommitted)

		ac.onConsensusFault(rt, miner, &zeroPledge)

		st = getState(rt)
		require.True(t, st.TotalRawBytePower.IsZero())
		require.True(t, st.TotalQualityAdjPower.IsZero())
		require.EqualValues(t, 0, st.MinerAboveMinPowerCount)
		require.True(t, st.TotalQABytesCommitted.IsZero())
		require.True(t, st.TotalBytesCommitted.IsZero())
	})

	t.Run("qaPower was above threshold before fault and successfully reduces pledged amount", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner)
		ac.updateClaimedPower(rt, miner, powerUnit, powerUnit)

		st := getState(rt)
		require.EqualValues(t, powerUnit, st.TotalRawBytePower)
		require.EqualValues(t, powerUnit, st.TotalQualityAdjPower)
		require.EqualValues(t, 1, st.MinerAboveMinPowerCount)
		require.EqualValues(t, powerUnit, st.TotalBytesCommitted)
		require.EqualValues(t, powerUnit, st.TotalQABytesCommitted)

		delta := abi.NewTokenAmount(100)
		ac.updatePledgeTotal(rt, miner, delta)

		slash := abi.NewTokenAmount(50)
		ac.onConsensusFault(rt, miner, &slash)

		st = getState(rt)
		require.True(t, st.TotalRawBytePower.IsZero())
		require.True(t, st.TotalQualityAdjPower.IsZero())
		require.EqualValues(t, 0, st.MinerAboveMinPowerCount)
		require.True(t, st.TotalQABytesCommitted.IsZero())
		require.True(t, st.TotalBytesCommitted.IsZero())
		require.EqualValues(t, big.Sub(delta, slash), st.TotalPledgeCollateral)
	})

	t.Run("fails if total pledged amount goes below zero after fault", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner)
		amt := big.NewInt(10)

		rt.ExpectAssertionFailure("pledged amount cannot be negative", func() {
			ac.onConsensusFault(rt, miner, &amt)
		})
	})

	t.Run("fails if caller is not a StorageMinerActor", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		rt.SetCaller(miner, builtin.SystemActorCodeID)
		rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
		pledge := abi.NewTokenAmount(10)

		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.OnConsensusFault, &pledge)
		})

		rt.Verify()
	})

	t.Run("fails if claim does not exist for caller", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		pledge := abi.NewTokenAmount(10)

		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			ac.onConsensusFault(rt, miner, &pledge)
		})

		rt.Verify()
	})
}

func TestPowerAndPledgeAccounting(t *testing.T) {
	actor := newHarness(t)
	owner := tutil.NewIDAddr(t, 101)
	miner1 := tutil.NewIDAddr(t, 111)
	miner2 := tutil.NewIDAddr(t, 112)
	miner3 := tutil.NewIDAddr(t, 113)
	miner4 := tutil.NewIDAddr(t, 114)
	miner5 := tutil.NewIDAddr(t, 115)

	// These tests use the min power for consensus to check the accounting above and below that value.
	powerUnit := power.ConsensusMinerMinPower
	mul := func(a big.Int, b int64) big.Int {
		return big.Mul(a, big.NewInt(b))
	}
	div := func(a big.Int, b int64) big.Int {
		return big.Div(a, big.NewInt(b))
	}
	smallPowerUnit := big.NewInt(1_000_000)
	require.True(t, smallPowerUnit.LessThan(powerUnit), "power.CosensusMinerMinPower has changed requiring update to this test")
	// Subtests implicitly rely on ConsensusMinerMinMiners = 3
	require.Equal(t, 3, power.ConsensusMinerMinMiners)

	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("power & pledge accounted below threshold", func(t *testing.T) {

		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)

		ret := actor.currentPowerTotal(rt)
		assert.Equal(t, big.Zero(), ret.RawBytePower)
		assert.Equal(t, big.Zero(), ret.QualityAdjPower)
		assert.Equal(t, big.Zero(), ret.PledgeCollateral)

		// Add power for miner1
		actor.updateClaimedPower(rt, miner1, smallPowerUnit, mul(smallPowerUnit, 2))
		actor.expectTotalPowerEager(rt, smallPowerUnit, mul(smallPowerUnit, 2))
		assert.Equal(t, big.Zero(), ret.PledgeCollateral)

		// Add power and pledge for miner2
		actor.updateClaimedPower(rt, miner2, smallPowerUnit, smallPowerUnit)
		actor.updatePledgeTotal(rt, miner1, abi.NewTokenAmount(1e6))
		actor.expectTotalPowerEager(rt, mul(smallPowerUnit, 2), mul(smallPowerUnit, 3))
		actor.expectTotalPledgeEager(rt, abi.NewTokenAmount(1e6))

		rt.Verify()

		// Verify claims in state.
		var st power.State
		rt.GetState(&st)
		claim1 := actor.getClaim(rt, miner1)
		require.Equal(t, smallPowerUnit, claim1.RawBytePower)
		require.Equal(t, mul(smallPowerUnit, 2), claim1.QualityAdjPower)

		claim2 := actor.getClaim(rt, miner2)
		require.Equal(t, smallPowerUnit, claim2.RawBytePower)
		require.Equal(t, smallPowerUnit, claim2.QualityAdjPower)

		// Subtract power and some pledge for miner2
		actor.updateClaimedPower(rt, miner2, smallPowerUnit.Neg(), smallPowerUnit.Neg())
		actor.updatePledgeTotal(rt, miner2, abi.NewTokenAmount(1e5).Neg())
		actor.expectTotalPowerEager(rt, mul(smallPowerUnit, 1), mul(smallPowerUnit, 2))
		actor.expectTotalPledgeEager(rt, abi.NewTokenAmount(9e5))

		rt.GetState(&st)
		claim2 = actor.getClaim(rt, miner2)
		require.Equal(t, big.Zero(), claim2.RawBytePower)
		require.Equal(t, big.Zero(), claim2.QualityAdjPower)
	})

	t.Run("power accounting crossing threshold", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.createMinerBasic(rt, owner, owner, miner4)
		actor.createMinerBasic(rt, owner, owner, miner5)

		actor.updateClaimedPower(rt, miner1, div(smallPowerUnit, 2), smallPowerUnit)
		actor.updateClaimedPower(rt, miner2, div(smallPowerUnit, 2), smallPowerUnit)
		actor.updateClaimedPower(rt, miner3, div(smallPowerUnit, 2), smallPowerUnit)

		actor.updateClaimedPower(rt, miner4, div(powerUnit, 2), powerUnit)
		actor.updateClaimedPower(rt, miner5, div(powerUnit, 2), powerUnit)

		// Below threshold small miner power is counted
		expectedTotalBelow := big.Sum(mul(smallPowerUnit, 3), mul(powerUnit, 2))
		actor.expectTotalPowerEager(rt, div(expectedTotalBelow, 2), expectedTotalBelow)

		// Above threshold (power.ConsensusMinerMinMiners = 3) small miner power is ignored
		delta := big.Sub(powerUnit, smallPowerUnit)
		actor.updateClaimedPower(rt, miner3, div(delta, 2), delta)
		expectedTotalAbove := mul(powerUnit, 3)
		actor.expectTotalPowerEager(rt, div(expectedTotalAbove, 2), expectedTotalAbove)

		st := getState(rt)
		assert.Equal(t, int64(3), st.MinerAboveMinPowerCount)

		// Less than 3 miners above threshold again small miner power is counted again

		actor.updateClaimedPower(rt, miner3, div(delta.Neg(), 2), delta.Neg())
		actor.expectTotalPowerEager(rt, div(expectedTotalBelow, 2), expectedTotalBelow)
	})

	t.Run("all of one miner's power disappears when that miner dips below min power threshold", func(t *testing.T) {
		// Setup four miners above threshold
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.createMinerBasic(rt, owner, owner, miner4)

		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner2, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner3, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner4, powerUnit, powerUnit)

		expectedTotal := mul(powerUnit, 4)
		actor.expectTotalPowerEager(rt, expectedTotal, expectedTotal)

		// miner4 dips just below threshold
		actor.updateClaimedPower(rt, miner4, smallPowerUnit.Neg(), smallPowerUnit.Neg())

		expectedTotal = mul(powerUnit, 3)
		actor.expectTotalPowerEager(rt, expectedTotal, expectedTotal)
	})

	t.Run("threshold only depends on qa power, not raw byte", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)

		actor.updateClaimedPower(rt, miner1, powerUnit, big.Zero())
		actor.updateClaimedPower(rt, miner2, powerUnit, big.Zero())
		actor.updateClaimedPower(rt, miner3, powerUnit, big.Zero())
		st := getState(rt)
		assert.Equal(t, int64(0), st.MinerAboveMinPowerCount)

		actor.updateClaimedPower(rt, miner1, big.Zero(), powerUnit)
		actor.updateClaimedPower(rt, miner2, big.Zero(), powerUnit)
		actor.updateClaimedPower(rt, miner3, big.Zero(), powerUnit)
		st = getState(rt)
		assert.Equal(t, int64(3), st.MinerAboveMinPowerCount)
	})

	t.Run("qa power is above threshold before and after update", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// update claim so qa is above threshold
		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.updateClaimedPower(rt, miner1, mul(powerUnit, 3), mul(powerUnit, 3))
		st := getState(rt)
		require.EqualValues(t, mul(powerUnit, 3), st.TotalQualityAdjPower)
		require.EqualValues(t, mul(powerUnit, 3), st.TotalRawBytePower)

		// update such that it's above threshold again
		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		st = getState(rt)
		require.EqualValues(t, mul(powerUnit, 4), st.TotalQualityAdjPower)
		require.EqualValues(t, mul(powerUnit, 4), st.TotalRawBytePower)
	})

	t.Run("slashing miner that is already below minimum does not impact power", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)

		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner2, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner3, powerUnit, powerUnit)

		// create small miner
		actor.createMinerBasic(rt, owner, owner, miner4)

		actor.updateClaimedPower(rt, miner4, smallPowerUnit, smallPowerUnit)

		actor.expectTotalPowerEager(rt, mul(powerUnit, 3), mul(powerUnit, 3))

		// fault small miner
		zeroPledge := abi.NewTokenAmount(0)
		actor.onConsensusFault(rt, miner4, &zeroPledge)

		// power unchanged
		actor.expectTotalPowerEager(rt, mul(powerUnit, 3), mul(powerUnit, 3))

	})

	t.Run("slashing miner correctly updates power", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// create three miners
		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner2, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner3, powerUnit, powerUnit)

		// create a fourth miner to go above the consensus limit.
		actor.createMinerBasic(rt, owner, owner, miner4)
		actor.updateClaimedPower(rt, miner4, big.Mul(powerUnit, big.NewInt(2)), powerUnit)
		actor.expectTotalPowerEager(rt, mul(powerUnit, 5), mul(powerUnit, 4))

		// fault the fourth miner
		zeroPledge := abi.NewTokenAmount(0)
		actor.onConsensusFault(rt, miner4, &zeroPledge)

		// power of the fourth miner is removed
		actor.expectTotalPowerEager(rt, mul(powerUnit, 3), mul(powerUnit, 3))
	})
}

func TestCron(t *testing.T) {
	actor := newHarness(t)
	miner1 := tutil.NewIDAddr(t, 101)
	miner2 := tutil.NewIDAddr(t, 102)
	owner := tutil.NewIDAddr(t, 103)

	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("calls reward actor", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		expectedPower := big.NewInt(0)
		rt.SetEpoch(1)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, abi.NewTokenAmount(0), nil, 0)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)

		rt.ExpectBatchVerifySeals(nil, nil, nil)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
	})

	t.Run("test amount sent to reward actor and state change", func(t *testing.T) {
		powerUnit := power.ConsensusMinerMinPower
		miner3 := tutil.NewIDAddr(t, 103)
		miner4 := tutil.NewIDAddr(t, 104)

		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.createMinerBasic(rt, owner, owner, miner4)
		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)

		expectedPower := big.Mul(big.NewInt(4), powerUnit)

		delta := abi.NewTokenAmount(1)
		actor.updatePledgeTotal(rt, miner1, delta)
		actor.onEpochTickEnd(rt, 0, expectedPower, nil, nil)

		st := getState(rt)
		require.EqualValues(t, delta, st.ThisEpochPledgeCollateral)
		require.EqualValues(t, expectedPower, st.ThisEpochQualityAdjPower)
		require.EqualValues(t, expectedPower, st.ThisEpochRawBytePower)
	})

	t.Run("event scheduled in null round called next round", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		//  0 - genesis
		//  1 - block - registers events
		//  2 - null  - has event
		//  3 - null
		//  4 - block - has event

		rt.SetEpoch(1)
		actor.enrollCronEvent(rt, miner1, 2, []byte{0x1, 0x3})
		actor.enrollCronEvent(rt, miner2, 4, []byte{0x2, 0x3})

		expectedRawBytePower := big.NewInt(0)
		rt.SetEpoch(4)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes([]byte{0x1, 0x3}), big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes([]byte{0x2, 0x3}), big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedRawBytePower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
	})

	t.Run("event scheduled in past called next round", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// run cron once to put it in a clean state at epoch 4
		rt.SetEpoch(4)
		expectedRawBytePower := big.NewInt(0)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedRawBytePower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)

		rt.ExpectBatchVerifySeals(nil, nil, nil)

		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()

		// enroll a cron task at epoch 2 (which is in the past)
		actor.enrollCronEvent(rt, miner1, 2, []byte{0x1, 0x3})

		// run cron again in the future
		rt.SetEpoch(6)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes([]byte{0x1, 0x3}), big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedRawBytePower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()

		// assert used cron events are cleaned up
		st := getState(rt)

		mmap, err := adt.AsMultimap(rt.AdtStore(), st.CronEventQueue)
		require.NoError(t, err)

		var ev power.CronEvent
		err = mmap.ForEach(abi.IntKey(int64(2)), &ev, func(i int64) error {
			t.Errorf("Unexpected bitfield at epoch %d", i)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("fails to enroll if epoch is negative", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// enroll a cron task at epoch 2 (which is in the past)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "epoch -2 cannot be less than zero", func() {
			actor.enrollCronEvent(rt, miner1, -2, []byte{0x1, 0x3})
		})
	})

	t.Run("handles failed call", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(1)
		actor.enrollCronEvent(rt, miner1, 2, []byte{})
		actor.enrollCronEvent(rt, miner2, 2, []byte{})

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)

		rawPow := power.ConsensusMinerMinPower
		qaPow := rawPow
		actor.updateClaimedPower(rt, miner1, rawPow, qaPow)
		actor.expectTotalPowerEager(rt, rawPow, qaPow)

		expectedPower := big.NewInt(0)
		rt.SetEpoch(2)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		// First send fails
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes(nil), big.Zero(), nil, exitcode.ErrIllegalState)
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		// Subsequent one still invoked
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes(nil), big.Zero(), nil, exitcode.Ok)
		// Reward actor still invoked
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()

		// expect cron failure was logged
		rt.ExpectLogsContain("OnDeferredCronEvent failed for miner")

		newPow := actor.currentPowerTotal(rt)
		assert.Equal(t, abi.NewStoragePower(0), newPow.RawBytePower)
		assert.Equal(t, abi.NewStoragePower(0), newPow.QualityAdjPower)

		// Next epoch, only the reward actor is invoked
		rt.SetEpoch(3)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
	})
}

func TestSubmitPoRepForBulkVerify(t *testing.T) {
	actor := newHarness(t)
	miner := tutil.NewIDAddr(t, 101)
	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("registers porep and charges gas", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		commR := tutil.MakeCID("commR", &mineract.SealedCIDPrefix)
		commD := tutil.MakeCID("commD", &market.PieceCIDPrefix)
		sealInfo := &proof.SealVerifyInfo{
			SealedCID:   commR,
			UnsealedCID: commD,
		}
		actor.submitPoRepForBulkVerify(rt, miner, sealInfo)
		rt.ExpectGasCharged(power.GasOnSubmitVerifySeal)
		st := getState(rt)
		store := rt.AdtStore()
		require.NotNil(t, st.ProofValidationBatch)
		mmap, err := adt.AsMultimap(store, *st.ProofValidationBatch)
		require.NoError(t, err)
		arr, found, err := mmap.Get(abi.AddrKey(miner))
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, uint64(1), arr.Length())
		var storedSealInfo proof.SealVerifyInfo
		found, err = arr.Get(0, &storedSealInfo)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, commR, storedSealInfo.SealedCID)
	})

	t.Run("aborts when too many poreps", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		sealInfo := func(i int) *proof.SealVerifyInfo {
			var sealInfo proof.SealVerifyInfo
			sealInfo.SealedCID = tutil.MakeCID(fmt.Sprintf("commR-%d", i), &mineract.SealedCIDPrefix)
			sealInfo.UnsealedCID = tutil.MakeCID(fmt.Sprintf("commD-%d", i), &market.PieceCIDPrefix)
			return &sealInfo
		}

		// Adding MaxMinerProveCommitsPerEpoch works without error
		for i := 0; i < power.MaxMinerProveCommitsPerEpoch; i++ {
			actor.submitPoRepForBulkVerify(rt, miner, sealInfo(i))
		}

		rt.ExpectAbort(power.ErrTooManyProveCommits, func() {
			actor.submitPoRepForBulkVerify(rt, miner, sealInfo(power.MaxMinerProveCommitsPerEpoch))
		})

		// Gas only charged for successful submissions
		rt.ExpectGasCharged(power.GasOnSubmitVerifySeal * power.MaxMinerProveCommitsPerEpoch)
	})
}

func TestCronBatchProofVerifies(t *testing.T) {
	sealInfo := func(i int) *proof.SealVerifyInfo {
		var sealInfo proof.SealVerifyInfo
		sealInfo.SealedCID = tutil.MakeCID(fmt.Sprintf("commR-%d", i), &mineract.SealedCIDPrefix)
		sealInfo.UnsealedCID = tutil.MakeCID(fmt.Sprintf("commD-%d", i), &market.PieceCIDPrefix)
		sealInfo.SectorID = abi.SectorID{Number: abi.SectorNumber(i)}
		return &sealInfo
	}

	miner1 := tutil.NewIDAddr(t, 101)
	info := sealInfo(0)
	info1 := sealInfo(1)
	info2 := sealInfo(2)
	info3 := sealInfo(3)
	info4 := sealInfo(101)
	info5 := sealInfo(200)
	info6 := sealInfo(201)
	info7 := sealInfo(300)
	info8 := sealInfo(301)

	t.Run("success with one miner and one confirmed sector", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.submitPoRepForBulkVerify(rt, miner1, info)

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info}}
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info.Number}}}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)
	})

	t.Run("success with one miner and multiple confirmed sectors", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)
		ac.submitPoRepForBulkVerify(rt, miner1, info3)

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info2, *info3}}
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info1.Number, info2.Number, info3.Number}}}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)
	})

	t.Run("duplicate sector numbers are ignored for a miner", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)

		// duplicates will be sent to the batch verify call
		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info1, *info2}}

		// however, duplicates will not be sent to the miner as confirmed
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info1.Number, info2.Number}}}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)
	})

	t.Run("success with multiple miners and multiple confirmed sectors and assert expected power", func(t *testing.T) {
		miner2 := tutil.NewIDAddr(t, 102)
		miner3 := tutil.NewIDAddr(t, 103)
		miner4 := tutil.NewIDAddr(t, 104)

		rt, ac := basicPowerSetup(t)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)

		ac.submitPoRepForBulkVerify(rt, miner2, info3)
		ac.submitPoRepForBulkVerify(rt, miner2, info4)

		ac.submitPoRepForBulkVerify(rt, miner3, info5)
		ac.submitPoRepForBulkVerify(rt, miner3, info6)

		ac.submitPoRepForBulkVerify(rt, miner4, info7)
		ac.submitPoRepForBulkVerify(rt, miner4, info8)

		// TODO Because read order of keys in a multi-map is not as per insertion order,
		// we have to move around the expected sends
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info1.Number, info2.Number}},
			{miner3, []abi.SectorNumber{info5.Number, info6.Number}},
			{miner4, []abi.SectorNumber{info7.Number, info8.Number}},
			{miner2, []abi.SectorNumber{info3.Number, info4.Number}}}

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info2},
			miner2: {*info3, *info4},
			miner3: {*info5, *info6},
			miner4: {*info7, *info8}}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)
	})

	t.Run("success when no confirmed sector", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.onEpochTickEnd(rt, 0, big.Zero(), nil, nil)
	})

	t.Run("verification for one sector fails but others succeeds for a miner", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)
		ac.submitPoRepForBulkVerify(rt, miner1, info3)

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info2, *info3}}

		res := map[addr.Address][]bool{
			miner1: []bool{true, false, true},
		}

		// send will only be for the first and third sector as the middle sector will fail verification
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info1.Number, info3.Number}}}

		// expect sends for confirmed sectors
		for _, cs := range cs {
			param := &builtin.ConfirmSectorProofsParams{Sectors: cs.sectorNums}
			rt.ExpectSend(cs.miner, builtin.MethodsMiner.ConfirmSectorProofsValid, param, abi.NewTokenAmount(0), nil, 0)
		}

		rt.ExpectBatchVerifySeals(infos, res, nil)
		power := big.Zero()
		//expect power sends to reward actor
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &power, abi.NewTokenAmount(0), nil, 0)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)

		rt.SetEpoch(0)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)

		rt.Call(ac.OnEpochTickEnd, nil)
		rt.Verify()
	})

	t.Run("fails if batch verify seals fails", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)
		ac.submitPoRepForBulkVerify(rt, miner1, info3)

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info2, *info3}}

		rt.ExpectBatchVerifySeals(infos, batchVerifyDefaultOutput(infos), fmt.Errorf("fail"))
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)

		rt.SetEpoch(abi.ChainEpoch(0))
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)

		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			rt.Call(ac.Actor.OnEpochTickEnd, nil)
		})
		rt.Verify()
	})
}

//
// Misc. Utility Functions
//

type key string

func asKey(in string) abi.Keyer {
	return key(in)
}

func verifyEmptyMap(t testing.TB, rt *mock.Runtime, cid cid.Cid) {
	mapChecked, err := adt.AsMap(adt.AsStore(rt), cid)
	assert.NoError(t, err)
	keys, err := mapChecked.CollectKeys()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

type spActorHarness struct {
	power.Actor
	t        *testing.T
	minerSeq int
}

func newHarness(t *testing.T) *spActorHarness {
	return &spActorHarness{
		Actor: power.Actor{},
		t:     t,
	}
}

func (h *spActorHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Actor.Constructor, nil)
	assert.Nil(h.t, ret)
	rt.Verify()

	var st power.State

	rt.GetState(&st)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalRawBytePower)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalBytesCommitted)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalQualityAdjPower)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalQABytesCommitted)
	assert.Equal(h.t, abi.NewTokenAmount(0), st.TotalPledgeCollateral)
	assert.Equal(h.t, abi.NewStoragePower(0), st.ThisEpochRawBytePower)
	assert.Equal(h.t, abi.NewStoragePower(0), st.ThisEpochQualityAdjPower)
	assert.Equal(h.t, abi.NewTokenAmount(0), st.ThisEpochPledgeCollateral)
	assert.Equal(h.t, abi.ChainEpoch(0), st.FirstCronEpoch)
	assert.Equal(h.t, int64(0), st.MinerCount)
	assert.Equal(h.t, int64(0), st.MinerAboveMinPowerCount)

	verifyEmptyMap(h.t, rt, st.Claims)
	verifyEmptyMap(h.t, rt, st.CronEventQueue)
}

type confirmedSectorSend struct {
	miner      addr.Address
	sectorNums []abi.SectorNumber
}

func (h *spActorHarness) onEpochTickEnd(rt *mock.Runtime, currEpoch abi.ChainEpoch, expectedRawPower abi.StoragePower,
	confirmedSectors []confirmedSectorSend, infos map[addr.Address][]proof.SealVerifyInfo) {

	// expect sends for confirmed sectors
	for _, cs := range confirmedSectors {
		param := &builtin.ConfirmSectorProofsParams{Sectors: cs.sectorNums}
		rt.ExpectSend(cs.miner, builtin.MethodsMiner.ConfirmSectorProofsValid, param, abi.NewTokenAmount(0), nil, 0)
	}

	rt.ExpectBatchVerifySeals(infos, batchVerifyDefaultOutput(infos), nil)
	//expect power sends to reward actor
	rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedRawPower, abi.NewTokenAmount(0), nil, 0)
	rt.ExpectValidateCallerAddr(builtin.CronActorAddr)

	rt.SetEpoch(currEpoch)
	rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)

	rt.Call(h.Actor.OnEpochTickEnd, nil)
	rt.Verify()

	st := getState(rt)
	require.Nil(h.t, st.ProofValidationBatch)
}

func (h *spActorHarness) createMiner(rt *mock.Runtime, owner, worker, miner, robust addr.Address, peer abi.PeerID,
	multiaddrs []abi.Multiaddrs, sealProofType abi.RegisteredSealProof, value abi.TokenAmount) {

	st := getState(rt)
	prevMinerCount := st.MinerCount

	createMinerParams := &power.CreateMinerParams{
		Owner:         owner,
		Worker:        worker,
		SealProofType: sealProofType,
		Peer:          peer,
		Multiaddrs:    multiaddrs,
	}

	// owner send CreateMiner to Actor
	rt.SetCaller(owner, builtin.AccountActorCodeID)
	rt.SetReceived(value)
	rt.SetBalance(value)
	rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)

	createMinerRet := &power.CreateMinerReturn{
		IDAddress:     miner,  // miner actor id address
		RobustAddress: robust, // should be long miner actor address
	}

	msgParams := &initact.ExecParams{
		CodeCID:           builtin.StorageMinerActorCodeID,
		ConstructorParams: initCreateMinerBytes(h.t, owner, worker, peer, multiaddrs, sealProofType),
	}
	rt.ExpectSend(builtin.InitActorAddr, builtin.MethodsInit.Exec, msgParams, value, createMinerRet, 0)
	rt.Call(h.Actor.CreateMiner, createMinerParams)
	rt.Verify()

	cl := h.getClaim(rt, miner)
	require.True(h.t, cl.RawBytePower.IsZero())
	require.True(h.t, cl.QualityAdjPower.IsZero())
	require.EqualValues(h.t, prevMinerCount+1, getState(rt).MinerCount)

}

func (h *spActorHarness) getClaim(rt *mock.Runtime, a addr.Address) *power.Claim {
	var st power.State
	rt.GetState(&st)

	claims, err := adt.AsMap(adt.AsStore(rt), st.Claims)
	require.NoError(h.t, err)

	var out power.Claim
	found, err := claims.Get(abi.AddrKey(a), &out)
	require.NoError(h.t, err)
	require.True(h.t, found)

	return &out
}

func (h *spActorHarness) getEnrolledCronTicks(rt *mock.Runtime, epoch abi.ChainEpoch) []power.CronEvent {
	var st power.State
	rt.GetState(&st)

	events, err := adt.AsMultimap(adt.AsStore(rt), st.CronEventQueue)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load cron events")

	evts, found, err := events.Get(abi.IntKey(int64(epoch)))
	require.NoError(h.t, err)
	require.True(h.t, found)

	cronEvt := &power.CronEvent{}
	var cronEvents []power.CronEvent
	err = evts.ForEach(cronEvt, func(i int64) error {
		cronEvents = append(cronEvents, *cronEvt)
		return nil
	})
	require.NoError(h.t, err)

	return cronEvents
}

func basicPowerSetup(t *testing.T) (*mock.Runtime, *spActorHarness) {
	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
	rt := builder.Build(t)
	h := newHarness(t)
	h.constructAndVerify(rt)

	return rt, h
}

func (h *spActorHarness) createMinerBasic(rt *mock.Runtime, owner, worker, miner addr.Address) {
	label := strconv.Itoa(h.minerSeq)
	actrAddr := tutil.NewActorAddr(h.t, label)
	h.minerSeq += 1
	h.createMiner(rt, owner, worker, miner, actrAddr, abi.PeerID(label), nil, abi.RegisteredSealProof_StackedDrg2KiBV1, big.Zero())
}

func (h *spActorHarness) updateClaimedPower(rt *mock.Runtime, miner addr.Address, rawDelta, qaDelta abi.StoragePower) {
	prevCl := h.getClaim(rt, miner)

	params := power.UpdateClaimedPowerParams{
		RawByteDelta:         rawDelta,
		QualityAdjustedDelta: qaDelta,
	}
	rt.SetCaller(miner, builtin.StorageMinerActorCodeID)
	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
	rt.Call(h.UpdateClaimedPower, &params)
	rt.Verify()

	cl := h.getClaim(rt, miner)
	expectedRaw := big.Add(prevCl.RawBytePower, rawDelta)
	expectedAdjusted := big.Add(prevCl.QualityAdjPower, qaDelta)
	if expectedRaw.IsZero() {
		require.True(h.t, cl.RawBytePower.IsZero())
	} else {
		require.EqualValues(h.t, big.Add(prevCl.RawBytePower, rawDelta), cl.RawBytePower)
	}

	if expectedAdjusted.IsZero() {
		require.True(h.t, cl.QualityAdjPower.IsZero())
	} else {
		require.EqualValues(h.t, big.Add(prevCl.QualityAdjPower, qaDelta), cl.QualityAdjPower)
	}
}

func (h *spActorHarness) updatePledgeTotal(rt *mock.Runtime, miner addr.Address, delta abi.TokenAmount) {
	st := getState(rt)
	prev := st.TotalPledgeCollateral

	rt.SetCaller(miner, builtin.StorageMinerActorCodeID)
	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
	rt.Call(h.UpdatePledgeTotal, &delta)
	rt.Verify()

	st = getState(rt)
	new := st.TotalPledgeCollateral
	require.EqualValues(h.t, big.Add(prev, delta), new)
}

func (h *spActorHarness) currentPowerTotal(rt *mock.Runtime) *power.CurrentTotalPowerReturn {
	rt.ExpectValidateCallerAny()
	ret := rt.Call(h.CurrentTotalPower, nil).(*power.CurrentTotalPowerReturn)
	rt.Verify()
	return ret
}

func (h *spActorHarness) enrollCronEvent(rt *mock.Runtime, miner addr.Address, epoch abi.ChainEpoch, payload []byte) {
	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
	rt.SetCaller(miner, builtin.StorageMinerActorCodeID)
	rt.Call(h.Actor.EnrollCronEvent, &power.EnrollCronEventParams{
		EventEpoch: epoch,
		Payload:    payload,
	})
	rt.Verify()

}

func (h *spActorHarness) onConsensusFault(rt *mock.Runtime, minerAddr addr.Address, pledgeAmount *abi.TokenAmount) {
	st := getState(rt)
	prevMinerCount := st.MinerCount
	prevPledged := st.TotalPledgeCollateral

	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
	rt.SetCaller(minerAddr, builtin.StorageMinerActorCodeID)
	rt.Call(h.Actor.OnConsensusFault, pledgeAmount)
	rt.Verify()

	// verify that miner claim is erased from state, miner is removed and pledged amount is updated
	st = getState(rt)
	claims, err := adt.AsMap(adt.AsStore(rt), st.Claims)
	require.NoError(h.t, err)
	var out power.Claim
	found, err := claims.Get(abi.AddrKey(minerAddr), &out)
	require.NoError(h.t, err)
	require.False(h.t, found)

	require.EqualValues(h.t, prevMinerCount-1, st.MinerCount)

	require.EqualValues(h.t, big.Sub(prevPledged, *pledgeAmount), st.TotalPledgeCollateral)
}

func (h *spActorHarness) submitPoRepForBulkVerify(rt *mock.Runtime, minerAddr addr.Address, sealInfo *proof.SealVerifyInfo) {
	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
	rt.SetCaller(minerAddr, builtin.StorageMinerActorCodeID)
	rt.Call(h.Actor.SubmitPoRepForBulkVerify, sealInfo)
	rt.Verify()
}

func (h *spActorHarness) expectTotalPowerEager(rt *mock.Runtime, expectedRaw, expectedQA abi.StoragePower) {
	st := getState(rt)

	rawBytePower, qualityAdjPower := power.CurrentTotalPower(st)
	assert.Equal(h.t, expectedRaw, rawBytePower)
	assert.Equal(h.t, expectedQA, qualityAdjPower)
}

func (h *spActorHarness) expectTotalPledgeEager(rt *mock.Runtime, expectedPledge abi.TokenAmount) {
	st := getState(rt)
	assert.Equal(h.t, expectedPledge, st.TotalPledgeCollateral)
}

func initCreateMinerBytes(t testing.TB, owner, worker addr.Address, peer abi.PeerID, multiaddrs []abi.Multiaddrs, sealProofType abi.RegisteredSealProof) []byte {
	params := &power.MinerConstructorParams{
		OwnerAddr:     owner,
		WorkerAddr:    worker,
		SealProofType: sealProofType,
		PeerId:        peer,
		Multiaddrs:    multiaddrs,
	}

	buf := new(bytes.Buffer)
	require.NoError(t, params.MarshalCBOR(buf))
	return buf.Bytes()
}

func (s key) Key() string {
	return string(s)
}

func getState(rt *mock.Runtime) *power.State {
	var st power.State
	rt.GetState(&st)
	return &st
}

func batchVerifyDefaultOutput(vis map[addr.Address][]proof.SealVerifyInfo) map[addr.Address][]bool {
	out := make(map[addr.Address][]bool)
	for k, v := range vis { //nolint:nomaprange
		validations := make([]bool, len(v))
		for i := range validations {
			validations[i] = true
		}
		out[k] = validations
	}
	return out
}
