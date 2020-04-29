package power_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/filecoin-project/go-state-types/network"
	"strconv"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	cid "github.com/ipfs/go-cid"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	mineract "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
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
		actor.checkState(rt)
	})

	t.Run("create miner", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMiner(rt, owner, owner, miner, actr, abi.PeerID("miner"), []abi.Multiaddrs{{1}}, abi.RegisteredSealProof_StackedDrg32GiBV1_1, abi.NewTokenAmount(10))

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
		assert.Equal(t, power.Claim{abi.RegisteredSealProof_StackedDrg32GiBV1_1, big.Zero(), big.Zero()}, actualClaim) // miner has not proven anything

		verifyEmptyMap(t, rt, st.CronEventQueue)
		actor.checkState(rt)
	})
}

func TestCreateMinerFailures(t *testing.T) {
	owner := tutil.NewIDAddr(t, 101)
	peer := abi.PeerID("miner")
	mAddr := []abi.Multiaddrs{{1}}
	sealProofType := abi.RegisteredSealProof_StackedDrg2KiBV1_1

	t.Run("fails when caller is not of signable type", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		rt.SetCaller(owner, builtin.StorageMinerActorCodeID)
		rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
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
		expRet := initact.ExecReturn{
			IDAddress:     tutil.NewIDAddr(t, 1475),
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

		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
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
	owner := tutil.NewBLSAddr(t, 0)
	miner := tutil.NewIDAddr(t, 101)

	t.Run("enroll multiple events", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner)
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
		ac.createMinerBasic(rt, owner, owner, miner2)
		ac.enrollCronEvent(rt, miner2, e2, p3)
		events = ac.getEnrolledCronTicks(rt, e2)
		evt = events[0]
		require.EqualValues(t, p3, evt.CallbackPayload)
		require.EqualValues(t, miner2, evt.MinerAddr)
		ac.checkState(rt)
	})

	t.Run("enroll for an epoch before the current epoch", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner)

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
		ac.checkState(rt)
	})

	t.Run("fails if epoch is negative", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			ac.enrollCronEvent(rt, miner, abi.ChainEpoch(-1), []byte("payload"))
		})
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
	powerUnit, err := builtin.ConsensusMinerMinPower(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
	require.NoError(t, err)

	mul := func(a big.Int, b int64) big.Int {
		return big.Mul(a, big.NewInt(b))
	}
	div := func(a big.Int, b int64) big.Int {
		return big.Div(a, big.NewInt(b))
	}
	smallPowerUnit := big.NewInt(1_000_000)
	require.True(t, smallPowerUnit.LessThan(powerUnit), "power.ConsensusMinerMinPower has changed requiring update to this test")
	// Subtests implicitly rely on ConsensusMinerMinMiners = 3
	require.Equal(t, 4, power.ConsensusMinerMinMiners, "power.ConsensusMinerMinMiners has changed requiring update to this test")

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
		actor.checkState(rt)
	})

	t.Run("after version 7, new miner updates MinerAboveMinPowerCount", func(t *testing.T) {
		for _, test := range []struct {
			version        network.Version
			proof          abi.RegisteredSealProof
			expectedMiners int64
		}{{
			version:        network.Version6,
			proof:          abi.RegisteredSealProof_StackedDrg2KiBV1, // 2K sectors have zero consensus minimum
			expectedMiners: 0,
		}, {
			version:        network.Version6,
			proof:          abi.RegisteredSealProof_StackedDrg32GiBV1,
			expectedMiners: 0,
		}, {
			version:        network.Version7,
			proof:          abi.RegisteredSealProof_StackedDrg2KiBV1, // 2K sectors have zero consensus minimum
			expectedMiners: 1,
		}, {
			version:        network.Version7,
			proof:          abi.RegisteredSealProof_StackedDrg32GiBV1,
			expectedMiners: 0,
		}} {
			rt := builder.WithNetworkVersion(test.version).Build(t)
			actor.constructAndVerify(rt)
			actor.sealProof = test.proof
			actor.createMinerBasic(rt, owner, owner, miner1)

			st := getState(rt)
			assert.Equal(t, test.expectedMiners, st.MinerAboveMinPowerCount)
		}
	})

	t.Run("power accounting crossing threshold", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.createMinerBasic(rt, owner, owner, miner4)
		actor.createMinerBasic(rt, owner, owner, miner5)

		// Use qa power 10x raw power to show it's not being used for threshold calculations.
		actor.updateClaimedPower(rt, miner1, smallPowerUnit, mul(smallPowerUnit, 10))
		actor.updateClaimedPower(rt, miner2, smallPowerUnit, mul(smallPowerUnit, 10))

		actor.updateClaimedPower(rt, miner3, powerUnit, mul(powerUnit, 10))
		actor.updateClaimedPower(rt, miner4, powerUnit, mul(powerUnit, 10))
		actor.updateClaimedPower(rt, miner5, powerUnit, mul(powerUnit, 10))

		// Below threshold small miner power is counted
		expectedTotalBelow := big.Sum(mul(smallPowerUnit, 2), mul(powerUnit, 3))
		actor.expectTotalPowerEager(rt, expectedTotalBelow, mul(expectedTotalBelow, 10))

		// Above threshold (power.ConsensusMinerMinMiners = 4) small miner power is ignored
		delta := big.Sub(powerUnit, smallPowerUnit)
		actor.updateClaimedPower(rt, miner2, delta, mul(delta, 10))
		expectedTotalAbove := mul(powerUnit, 4)
		actor.expectTotalPowerEager(rt, expectedTotalAbove, mul(expectedTotalAbove, 10))

		st := getState(rt)
		assert.Equal(t, int64(4), st.MinerAboveMinPowerCount)

		// Less than 4 miners above threshold again small miner power is counted again
		actor.updateClaimedPower(rt, miner4, delta.Neg(), mul(delta.Neg(), 10))
		actor.expectTotalPowerEager(rt, expectedTotalBelow, mul(expectedTotalBelow, 10))
		actor.checkState(rt)
	})

	t.Run("all of one miner's power disappears when that miner dips below min power threshold", func(t *testing.T) {
		// Setup four miners above threshold
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.createMinerBasic(rt, owner, owner, miner4)
		actor.createMinerBasic(rt, owner, owner, miner5)

		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner2, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner3, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner4, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner5, powerUnit, powerUnit)

		expectedTotal := mul(powerUnit, 5)
		actor.expectTotalPowerEager(rt, expectedTotal, expectedTotal)

		// miner4 dips just below threshold
		actor.updateClaimedPower(rt, miner4, smallPowerUnit.Neg(), smallPowerUnit.Neg())

		expectedTotal = mul(powerUnit, 4)
		actor.expectTotalPowerEager(rt, expectedTotal, expectedTotal)
		actor.checkState(rt)
	})

	t.Run("consensus minimum power depends on proof type", func(t *testing.T) {
		// Setup four miners above threshold
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// create 4 miners that meet minimum
		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.createMinerBasic(rt, owner, owner, miner4)

		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner2, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner3, powerUnit, powerUnit)
		actor.updateClaimedPower(rt, miner4, powerUnit, powerUnit)

		actor.expectMinersAboveMinPower(rt, 4)
		expectedTotal := mul(powerUnit, 4)
		actor.expectTotalPowerEager(rt, expectedTotal, expectedTotal)

		// miner 5 uses 64GiB sectors and has a higher minimum
		actor.createMiner(rt, owner, owner, miner5, tutil.NewActorAddr(t, "m5"), abi.PeerID("m5"),
			nil, abi.RegisteredSealProof_StackedDrg64GiBV1_1, big.Zero())

		power64Unit, err := builtin.ConsensusMinerMinPower(abi.RegisteredSealProof_StackedDrg64GiBV1_1)
		require.NoError(t, err)
		assert.True(t, power64Unit.GreaterThan(powerUnit))

		// below limit actors power is not added
		actor.updateClaimedPower(rt, miner5, powerUnit, powerUnit)
		actor.expectMinersAboveMinPower(rt, 4)
		actor.expectTotalPowerEager(rt, expectedTotal, expectedTotal)

		// just below limit
		delta := big.Subtract(power64Unit, powerUnit, big.NewInt(1))
		actor.updateClaimedPower(rt, miner5, delta, delta)
		actor.expectMinersAboveMinPower(rt, 4)
		actor.expectTotalPowerEager(rt, expectedTotal, expectedTotal)

		// at limit power is added
		actor.updateClaimedPower(rt, miner5, big.NewInt(1), big.NewInt(1))
		actor.expectMinersAboveMinPower(rt, 5)
		newExpectedTotal := big.Add(expectedTotal, power64Unit)
		actor.expectTotalPowerEager(rt, newExpectedTotal, newExpectedTotal)
		actor.checkState(rt)
	})

	t.Run("threshold only depends on raw power, not qa power", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)
		actor.createMinerBasic(rt, owner, owner, miner3)
		actor.createMinerBasic(rt, owner, owner, miner4)

		actor.updateClaimedPower(rt, miner1, div(powerUnit, 2), powerUnit)
		actor.updateClaimedPower(rt, miner2, div(powerUnit, 2), powerUnit)
		actor.updateClaimedPower(rt, miner3, div(powerUnit, 2), powerUnit)
		st := getState(rt)
		assert.Equal(t, int64(0), st.MinerAboveMinPowerCount)

		actor.updateClaimedPower(rt, miner1, div(powerUnit, 2), powerUnit)
		actor.updateClaimedPower(rt, miner2, div(powerUnit, 2), powerUnit)
		actor.updateClaimedPower(rt, miner3, div(powerUnit, 2), powerUnit)
		st = getState(rt)
		assert.Equal(t, int64(3), st.MinerAboveMinPowerCount)
		actor.checkState(rt)
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
		actor.checkState(rt)
	})

	t.Run("claimed power is externally available", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.updateClaimedPower(rt, miner1, powerUnit, powerUnit)
		st := getState(rt)

		claim, found, err := st.GetClaim(rt.AdtStore(), miner1)
		require.NoError(t, err)
		require.True(t, found)

		assert.Equal(t, powerUnit, claim.RawBytePower)
		assert.Equal(t, powerUnit, claim.QualityAdjPower)
		actor.checkState(rt)
	})
}

func TestUpdatePledgeTotal(t *testing.T) {
	// most coverage of update pledge total is in accounting test above

	actor := newHarness(t)
	owner := tutil.NewIDAddr(t, 101)
	miner := tutil.NewIDAddr(t, 111)
	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("update pledge total aborts if miner has no claim", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		actor.createMinerBasic(rt, owner, owner, miner)

		// explicitly delete miner claim
		actor.deleteClaim(rt, miner)

		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "unknown miner", func() {
			actor.updatePledgeTotal(rt, miner, abi.NewTokenAmount(1e6))
		})
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
		actor.checkState(rt)
	})

	t.Run("test amount sent to reward actor and state change", func(t *testing.T) {
		powerUnit, err := builtin.ConsensusMinerMinPower(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		require.NoError(t, err)

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
		actor.checkState(rt)
	})

	t.Run("event scheduled in null round called next round", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)

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
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, builtin.CBORBytes([]byte{0x1, 0x3}), big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, builtin.CBORBytes([]byte{0x2, 0x3}), big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedRawBytePower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("event scheduled in past called next round", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		actor.createMinerBasic(rt, owner, owner, miner1)

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
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, builtin.CBORBytes([]byte{0x1, 0x3}), big.Zero(), nil, exitcode.Ok)
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
		actor.checkState(rt)
	})

	t.Run("fails to enroll if epoch is negative", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// enroll a cron task at epoch 2 (which is in the past)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "epoch -2 cannot be less than zero", func() {
			actor.enrollCronEvent(rt, miner1, -2, []byte{0x1, 0x3})
		})
	})

	t.Run("skips invocation if miner has no claim", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(1)
		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)

		actor.enrollCronEvent(rt, miner1, 2, []byte{})
		actor.enrollCronEvent(rt, miner2, 2, []byte{})

		// explicitly delete miner 1's claim
		actor.deleteClaim(rt, miner1)

		rt.SetEpoch(2)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)

		// process batch verifies first
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		// only expect second deferred cron event call
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, builtin.CBORBytes(nil), big.Zero(), nil, exitcode.Ok)

		// Reward actor still invoked
		expectedPower := big.NewInt(0)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()

		// expect cron skip was logged
		rt.ExpectLogsContain("skipping cron event for unknown miner t0101")
		actor.checkState(rt)
	})

	t.Run("handles failed call", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(1)
		actor.createMinerBasic(rt, owner, owner, miner1)
		actor.createMinerBasic(rt, owner, owner, miner2)

		actor.enrollCronEvent(rt, miner1, 2, []byte{})
		actor.enrollCronEvent(rt, miner2, 2, []byte{})

		rawPow, err := builtin.ConsensusMinerMinPower(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
		require.NoError(t, err)

		qaPow := rawPow
		actor.updateClaimedPower(rt, miner1, rawPow, qaPow)
		actor.expectTotalPowerEager(rt, rawPow, qaPow)
		actor.expectMinersAboveMinPower(rt, 1)

		expectedPower := big.NewInt(0)
		rt.SetEpoch(2)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)

		// process batch verifies first
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		// First send fails
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, builtin.CBORBytes(nil), big.Zero(), nil, exitcode.ErrIllegalState)

		// Subsequent one still invoked
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, builtin.CBORBytes(nil), big.Zero(), nil, exitcode.Ok)
		// Reward actor still invoked
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()

		// expect cron failure was logged
		rt.ExpectLogsContain("OnDeferredCronEvent failed for miner")

		// expect power stats to be decremented due to claim deletion
		actor.expectTotalPowerEager(rt, big.Zero(), big.Zero())
		actor.expectMinersAboveMinPower(rt, 0)

		// miner's claim is removed
		st := getState(rt)
		_, found, err := st.GetClaim(rt.AdtStore(), miner1)
		require.NoError(t, err)
		assert.False(t, found)

		// Next epoch, only the reward actor is invoked
		rt.SetEpoch(3)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.ExpectBatchVerifySeals(nil, nil, nil)

		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("failed call decrements miner count at network version 7", func(t *testing.T) {
		for _, test := range []struct {
			version            network.Version
			versionLabel       string
			expectedMinerCount int64
		}{{
			version:            network.Version6,
			versionLabel:       "Version6",
			expectedMinerCount: 1,
		}, {
			version:            network.Version7,
			versionLabel:       "Version7",
			expectedMinerCount: 0,
		}} {
			rt := builder.WithNetworkVersion(test.version).Build(t)
			rt.NetworkVersion()
			actor.constructAndVerify(rt)

			rt.SetEpoch(1)
			actor.createMinerBasic(rt, owner, owner, miner1)

			// expect one miner
			st := getState(rt)
			assert.Equal(t, int64(1), st.MinerCount)

			actor.enrollCronEvent(rt, miner1, 1, []byte{})

			rt.SetEpoch(2)
			rt.ExpectValidateCallerAddr(builtin.CronActorAddr)

			// process batch verifies first
			rt.ExpectBatchVerifySeals(nil, nil, nil)

			// send fails
			rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, builtin.CBORBytes(nil), big.Zero(), nil, exitcode.ErrIllegalState)

			// Reward actor still invoked
			power := abi.NewStoragePower(0)
			rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &power, big.Zero(), nil, exitcode.Ok)
			rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
			rt.Call(actor.Actor.OnEpochTickEnd, nil)
			rt.Verify()

			// expect cron failure was logged
			rt.ExpectLogsContain("OnDeferredCronEvent failed for miner")

			// miner count has been changed or not depending on version
			st = getState(rt)
			assert.Equal(t, test.expectedMinerCount, st.MinerCount)
		}
	})
}

func TestSubmitPoRepForBulkVerify(t *testing.T) {
	actor := newHarness(t)
	miner := tutil.NewIDAddr(t, 101)
	owner := tutil.NewIDAddr(t, 101)
	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("registers porep and charges gas", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		actor.createMinerBasic(rt, owner, owner, miner)
		commR := tutil.MakeCID("commR", &mineract.SealedCIDPrefix)
		commD := tutil.MakeCID("commD", &market.PieceCIDPrefix)
		sealInfo := &proof.SealVerifyInfo{
			SealProof:   actor.sealProof,
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
		actor.checkState(rt)
	})

	t.Run("aborts when too many poreps", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		actor.createMinerBasic(rt, owner, owner, miner)

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

	t.Run("aborts when miner has no claim", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		actor.createMinerBasic(rt, owner, owner, miner)
		commR := tutil.MakeCID("commR", &mineract.SealedCIDPrefix)
		commD := tutil.MakeCID("commD", &market.PieceCIDPrefix)
		sealInfo := &proof.SealVerifyInfo{
			SealedCID:   commR,
			UnsealedCID: commD,
		}

		// delete miner
		actor.deleteClaim(rt, miner)

		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "unknown miner", func() {
			actor.submitPoRepForBulkVerify(rt, miner, sealInfo)
		})
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
	owner := tutil.NewIDAddr(t, 102)
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
		ac.createMinerBasic(rt, owner, owner, miner1)

		ac.submitPoRepForBulkVerify(rt, miner1, info)

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info}}
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info.Number}}}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)
		ac.checkState(rt)
	})

	t.Run("success with one miner and multiple confirmed sectors", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner1)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)
		ac.submitPoRepForBulkVerify(rt, miner1, info3)

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info2, *info3}}
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info1.Number, info2.Number, info3.Number}}}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)
		ac.checkState(rt)
	})

	t.Run("duplicate sector numbers are ignored for a miner", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner1)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)

		// duplicates will be sent to the batch verify call
		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info1, *info2}}

		// however, duplicates will not be sent to the miner as confirmed
		cs := []confirmedSectorSend{{miner1, []abi.SectorNumber{info1.Number, info2.Number}}}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)
		ac.checkState(rt)
	})

	t.Run("skips verify if miner has no claim", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner1)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)

		// now explicitly delete miner's claim
		ac.deleteClaim(rt, miner1)

		// all infos will be skipped
		infos := map[addr.Address][]proof.SealVerifyInfo{}

		// nothing will be sent to miner
		cs := []confirmedSectorSend{}

		ac.onEpochTickEnd(rt, 0, big.Zero(), cs, infos)

		// expect cron failure was logged
		rt.ExpectLogsContain("skipping batch verifies for unknown miner t0101")
		ac.checkState(rt)
	})

	t.Run("success with multiple miners and multiple confirmed sectors and assert expected power", func(t *testing.T) {
		miner2 := tutil.NewIDAddr(t, 102)
		miner3 := tutil.NewIDAddr(t, 103)
		miner4 := tutil.NewIDAddr(t, 104)

		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner1)
		ac.createMinerBasic(rt, owner, owner, miner2)
		ac.createMinerBasic(rt, owner, owner, miner3)
		ac.createMinerBasic(rt, owner, owner, miner4)

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
		ac.checkState(rt)
	})

	t.Run("success when no confirmed sector", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.onEpochTickEnd(rt, 0, big.Zero(), nil, nil)
		ac.checkState(rt)
	})

	t.Run("verification for one sector fails but others succeeds for a miner", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner1)

		ac.submitPoRepForBulkVerify(rt, miner1, info1)
		ac.submitPoRepForBulkVerify(rt, miner1, info2)
		ac.submitPoRepForBulkVerify(rt, miner1, info3)

		infos := map[addr.Address][]proof.SealVerifyInfo{miner1: {*info1, *info2, *info3}}

		res := map[addr.Address][]bool{
			miner1: {true, false, true},
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
		ac.checkState(rt)
	})

	t.Run("fails if batch verify seals fails", func(t *testing.T) {
		rt, ac := basicPowerSetup(t)
		ac.createMinerBasic(rt, owner, owner, miner1)

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
	t         *testing.T
	minerSeq  int
	sealProof abi.RegisteredSealProof
}

func newHarness(t *testing.T) *spActorHarness {
	return &spActorHarness{
		Actor:     power.Actor{},
		t:         t,
		sealProof: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
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

func (h *spActorHarness) deleteClaim(rt *mock.Runtime, a addr.Address) {
	st := getState(rt)
	claims, err := adt.AsMap(adt.AsStore(rt), st.Claims)
	require.NoError(h.t, err)
	err = claims.Delete(abi.AddrKey(a))
	require.NoError(h.t, err)
	st.Claims, err = claims.Root()
	require.NoError(h.t, err)
	rt.ReplaceState(st)
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
	h.createMiner(rt, owner, worker, miner, actrAddr, abi.PeerID(label), nil, h.sealProof, big.Zero())
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

func (h *spActorHarness) expectMinersAboveMinPower(rt *mock.Runtime, count int64) {
	st := getState(rt)
	assert.Equal(h.t, count, st.MinerAboveMinPowerCount)
}

func (h *spActorHarness) expectTotalPledgeEager(rt *mock.Runtime, expectedPledge abi.TokenAmount) {
	st := getState(rt)
	assert.Equal(h.t, expectedPledge, st.TotalPledgeCollateral)
}

func (h *spActorHarness) checkState(rt *mock.Runtime) {
	st := getState(rt)
	_, msgs := power.CheckStateInvariants(st, rt.AdtStore())
	assert.True(h.t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))
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
