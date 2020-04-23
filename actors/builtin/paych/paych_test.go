package paych_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	. "github.com/filecoin-project/specs-actors/v2/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, Actor{})
}

func TestPaymentChannelActor_Constructor(t *testing.T) {
	ctx := context.Background()
	paychAddr := tutil.NewIDAddr(t, 100)
	payerAddr := tutil.NewIDAddr(t, 101)
	payeeAddr := tutil.NewIDAddr(t, 102)
	callerAddr := tutil.NewIDAddr(t, 102)

	actor := pcActorHarness{Actor{}, t, paychAddr, payerAddr, payeeAddr}

	t.Run("can create a payment channel actor", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, paychAddr).
			WithCaller(callerAddr, builtin.InitActorCodeID).
			WithActorType(payerAddr, builtin.AccountActorCodeID).
			WithActorType(payeeAddr, builtin.AccountActorCodeID)
		rt := builder.Build(t)
		actor.constructAndVerify(t, rt, payerAddr, payeeAddr)
		actor.checkState(rt)
	})

	t.Run("creates a payment channel actor after resolving non-ID addresses to ID addresses", func(t *testing.T) {
		payerAddr := tutil.NewIDAddr(t, 101)
		payerNonId := tutil.NewBLSAddr(t, 102)

		payeeAddr := tutil.NewIDAddr(t, 103)
		payeeNonId := tutil.NewBLSAddr(t, 104)

		builder := mock.NewBuilder(ctx, paychAddr).
			WithCaller(callerAddr, builtin.InitActorCodeID).
			WithActorType(payerAddr, builtin.AccountActorCodeID).
			WithActorType(payeeAddr, builtin.AccountActorCodeID)
		rt := builder.Build(t)
		rt.AddIDAddress(payerNonId, payerAddr)
		rt.AddIDAddress(payeeNonId, payeeAddr)

		actor.constructAndVerify(t, rt, payerNonId, payeeNonId)
		actor.checkState(rt)
	})

	nonAccountCodeID := builtin.MultisigActorCodeID
	testCases := []struct {
		desc        string
		fromCode    cid.Cid
		fromAddr    addr.Address
		toCode      cid.Cid
		toAddr      addr.Address
		expExitCode exitcode.ExitCode
	}{
		{"fails if target (to) is not account actor",
			builtin.AccountActorCodeID,
			payerAddr,
			nonAccountCodeID,
			payeeAddr,
			exitcode.ErrForbidden,
		}, {"fails if sender (from) is not account actor",
			nonAccountCodeID,
			payerAddr,
			builtin.AccountActorCodeID,
			payeeAddr,
			exitcode.ErrForbidden,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			builder := mock.NewBuilder(ctx, paychAddr).
				WithCaller(callerAddr, builtin.InitActorCodeID).
				WithActorType(paychAddr, builtin.PaymentChannelActorCodeID).
				WithActorType(payerAddr, tc.toCode).
				WithActorType(payeeAddr, tc.fromCode)
			rt := builder.Build(t)
			rt.ExpectValidateCallerType(builtin.InitActorCodeID)
			rt.ExpectAbort(tc.expExitCode, func() {
				rt.Call(actor.Constructor, &ConstructorParams{To: tc.toAddr, From: tc.fromAddr})
			})
		})
	}

	t.Run("fails if sender addr is not resolvable to ID address", func(t *testing.T) {
		to := tutil.NewIDAddr(t, 101)
		nonIdAddr := tutil.NewBLSAddr(t, 501)

		rt := mock.NewBuilder(ctx, paychAddr).
			WithCaller(callerAddr, builtin.InitActorCodeID).
			WithActorType(to, builtin.AccountActorCodeID).Build(t)

		rt.ExpectSend(nonIdAddr, builtin.MethodSend, nil, abi.NewTokenAmount(0), nil, exitcode.Ok)
		rt.ExpectValidateCallerType(builtin.InitActorCodeID)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			rt.Call(actor.Constructor, &ConstructorParams{From: nonIdAddr, To: to})
		})
		rt.Verify()
	})

	t.Run("fails if target addr is not resolvable to ID address", func(t *testing.T) {
		from := tutil.NewIDAddr(t, 5555)
		nonIdAddr := tutil.NewBLSAddr(t, 501)

		rt := mock.NewBuilder(ctx, paychAddr).
			WithCaller(callerAddr, builtin.InitActorCodeID).
			WithActorType(from, builtin.AccountActorCodeID).Build(t)

		rt.ExpectSend(nonIdAddr, builtin.MethodSend, nil, abi.NewTokenAmount(0), nil, exitcode.Ok)
		rt.ExpectValidateCallerType(builtin.InitActorCodeID)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			rt.Call(actor.Constructor, &ConstructorParams{From: from, To: nonIdAddr})
		})
		rt.Verify()
	})

	t.Run("fails if actor does not exist with: no code for address", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, paychAddr).
			WithCaller(callerAddr, builtin.InitActorCodeID).
			WithActorType(payerAddr, builtin.AccountActorCodeID)
		rt := builder.Build(t)
		rt.ExpectValidateCallerType(builtin.InitActorCodeID)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.Constructor, &ConstructorParams{To: paychAddr})
		})
	})
}

func TestPaymentChannelActor_CreateLane(t *testing.T) {
	ctx := context.Background()
	initActorAddr := tutil.NewIDAddr(t, 100)
	paychNonId := tutil.NewBLSAddr(t, 201)
	paychAddr := tutil.NewIDAddr(t, 101)
	payerAddr := tutil.NewIDAddr(t, 102)
	payeeAddr := tutil.NewIDAddr(t, 103)
	payChBalance := abi.NewTokenAmount(9)

	actor := pcActorHarness{Actor{}, t, paychAddr, payerAddr, payeeAddr}
	sig := &crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte("doesn't matter")}

	testCases := []struct {
		desc       string
		targetCode cid.Cid

		balance  int64
		received int64
		epoch    int64

		tlmin int64
		tlmax int64
		lane  uint64
		nonce uint64
		amt   int64

		paymentChannel addr.Address

		secretPreimage []byte
		sig            *crypto.Signature
		verifySig      bool
		expExitCode    exitcode.ExitCode
	}{
		{desc: "succeeds", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: paychAddr, epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: true,
			expExitCode: exitcode.Ok},
		{desc: "fails if channel address does not match address on the signed voucher", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: tutil.NewIDAddr(t, 210), epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if address on the signed voucher cannot be resolved to ID address", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: tutil.NewBLSAddr(t, 1), epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "succeeds if address on the signed voucher can be resolved to channel ID address", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: paychNonId, epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: true,
			expExitCode: exitcode.Ok},
		{desc: "fails if balance too low", targetCode: builtin.AccountActorCodeID,
			amt: 10, paymentChannel: paychAddr, epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if new send balance is negative", targetCode: builtin.AccountActorCodeID,
			amt: -1, paymentChannel: paychAddr, epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if signature not valid", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: paychAddr, epoch: 1, tlmin: 1, tlmax: 0,
			sig: nil, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if too early for voucher", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: paychAddr, epoch: 1, tlmin: 10, tlmax: 0,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if beyond TimeLockMax", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: paychAddr, epoch: 10, tlmin: 1, tlmax: 5,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if signature not verified", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: paychAddr, epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: false,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if SigningBytes fails", targetCode: builtin.AccountActorCodeID,
			amt: 1, paymentChannel: paychAddr, epoch: 1, tlmin: 1, tlmax: 0,
			sig: sig, verifySig: true,
			secretPreimage: make([]byte, 2<<21),
			expExitCode:    exitcode.ErrIllegalArgument},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			hasher := func(data []byte) [32]byte { return [32]byte{} }

			builder := mock.NewBuilder(ctx, paychAddr).
				WithBalance(payChBalance, abi.NewTokenAmount(tc.received)).
				WithEpoch(abi.ChainEpoch(tc.epoch)).
				WithCaller(initActorAddr, builtin.InitActorCodeID).
				WithActorType(payeeAddr, builtin.AccountActorCodeID).
				WithActorType(payerAddr, builtin.AccountActorCodeID).
				WithHasher(hasher)

			rt := builder.Build(t)
			rt.AddIDAddress(paychNonId, paychAddr)
			actor.constructAndVerify(t, rt, payerAddr, payeeAddr)

			sv := SignedVoucher{
				ChannelAddr:    tc.paymentChannel,
				TimeLockMin:    abi.ChainEpoch(tc.tlmin),
				TimeLockMax:    abi.ChainEpoch(tc.tlmax),
				Lane:           tc.lane,
				Nonce:          tc.nonce,
				Amount:         big.NewInt(tc.amt),
				Signature:      tc.sig,
				SecretPreimage: tc.secretPreimage,
			}
			ucp := &UpdateChannelStateParams{Sv: sv}

			rt.SetCaller(payeeAddr, tc.targetCode)
			rt.ExpectValidateCallerAddr(payerAddr, payeeAddr)
			if tc.sig != nil && tc.secretPreimage == nil {
				var result error
				if !tc.verifySig {
					result = fmt.Errorf("bad signature")
				}
				rt.ExpectVerifySignature(*sv.Signature, payerAddr, voucherBytes(t, &sv), result)
			}

			if tc.expExitCode == exitcode.Ok {
				rt.Call(actor.UpdateChannelState, ucp)
				var st State
				rt.GetState(&st)
				lstates, err := adt.AsArray(adt.AsStore(rt), st.LaneStates)
				assert.NoError(t, err)
				assert.Equal(t, uint64(1), lstates.Length())

				var ls LaneState
				found, err := lstates.Get(sv.Lane, &ls)
				assert.True(t, found)
				assert.NoError(t, err)

				assert.Equal(t, sv.Amount, ls.Redeemed)
				assert.Equal(t, sv.Nonce, ls.Nonce)
				actor.checkState(rt)
			} else {
				rt.ExpectAbort(tc.expExitCode, func() {
					rt.Call(actor.UpdateChannelState, ucp)
				})
				// verify state unchanged; no lane was created
				verifyInitialState(t, rt, payerAddr, payeeAddr)
			}
			rt.Verify()
		})
	}
}

func assertLaneStatesLength(t *testing.T, rt *mock.Runtime, rcid cid.Cid, l int) {
	t.Helper()
	arr, err := adt.AsArray(adt.AsStore(rt), rcid)
	assert.NoError(t, err)
	assert.Equal(t, arr.Length(), uint64(l))
}

func constructLaneStateAMT(t *testing.T, rt *mock.Runtime, lss []*LaneState) cid.Cid {
	t.Helper()
	arr := adt.MakeEmptyArray(adt.AsStore(rt))
	for i, ls := range lss {
		err := arr.Set(uint64(i), ls)
		assert.NoError(t, err)
	}

	c, err := arr.Root()
	assert.NoError(t, err)

	return c
}

func getLaneState(t *testing.T, rt *mock.Runtime, rcid cid.Cid, lane uint64) *LaneState {
	arr, err := adt.AsArray(adt.AsStore(rt), rcid)
	assert.NoError(t, err)

	var out LaneState
	found, err := arr.Get(lane, &out)
	assert.NoError(t, err)
	assert.True(t, found)

	return &out
}

func TestActor_UpdateChannelStateRedeem(t *testing.T) {
	ctx := context.Background()
	newVoucherAmt := big.NewInt(9)

	t.Run("redeeming voucher updates correctly with one lane", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, ctx, 1)
		var st1 State
		rt.GetState(&st1)

		expLs := LaneState{
			Redeemed: newVoucherAmt,
			Nonce:    2,
		}

		ucp := &UpdateChannelStateParams{Sv: *sv}
		ucp.Sv.Amount = newVoucherAmt

		// Sending to same lane updates the lane with "new" state
		rt.SetCaller(actor.payee, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payer, voucherBytes(t, &ucp.Sv), nil)
		ret := rt.Call(actor.UpdateChannelState, ucp)
		require.Nil(t, ret)
		rt.Verify()

		expState := State{
			From:            st1.From,
			To:              st1.To,
			ToSend:          newVoucherAmt,
			SettlingAt:      st1.SettlingAt,
			MinSettleHeight: st1.MinSettleHeight,
			LaneStates:      constructLaneStateAMT(t, rt, []*LaneState{&expLs}),
		}
		verifyState(t, rt, 1, expState)
		actor.checkState(rt)
	})

	t.Run("redeems voucher for correct lane", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, ctx, 3)
		var st1, st2 State
		rt.GetState(&st1)

		initialAmt := st1.ToSend

		ucp := &UpdateChannelStateParams{Sv: *sv}
		ucp.Sv.Amount = newVoucherAmt
		ucp.Sv.Lane = 1

		lsToUpdate := getLaneState(t, rt, st1.LaneStates, ucp.Sv.Lane)
		ucp.Sv.Nonce = lsToUpdate.Nonce + 1

		// Sending to same lane updates the lane with "new" state
		rt.SetCaller(actor.payee, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payer, voucherBytes(t, &ucp.Sv), nil)
		ret := rt.Call(actor.UpdateChannelState, ucp)
		require.Nil(t, ret)
		rt.Verify()

		rt.GetState(&st2)
		lUpdated := getLaneState(t, rt, st2.LaneStates, ucp.Sv.Lane)

		bDelta := big.Sub(ucp.Sv.Amount, lsToUpdate.Redeemed)
		expToSend := big.Add(initialAmt, bDelta)
		assert.Equal(t, expToSend, st2.ToSend)
		assert.Equal(t, ucp.Sv.Amount, lUpdated.Redeemed)
		assert.Equal(t, ucp.Sv.Nonce, lUpdated.Nonce)
		actor.checkState(rt)
	})

	t.Run("redeeming voucher fails on nonce reuse", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, ctx, 1)
		var st1 State
		rt.GetState(&st1)

		ucp := &UpdateChannelStateParams{Sv: *sv}
		// requireCreateChannelWithLanes creates a lane with nonce = 1.
		// reusing that should fail
		ucp.Sv.Nonce = 1
		ucp.Sv.Amount = newVoucherAmt

		rt.SetCaller(actor.payee, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payer, voucherBytes(t, &ucp.Sv), nil)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})

		rt.Verify()
		actor.checkState(rt)
	})
}

func TestActor_UpdateChannelStateMergeSuccess(t *testing.T) {
	// Check that a lane merge correctly updates lane states
	numLanes := 3
	rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), numLanes)

	var st1 State
	rt.GetState(&st1)
	rt.SetCaller(st1.From, builtin.AccountActorCodeID)

	mergeToID := uint64(0)
	mergeTo := getLaneState(t, rt, st1.LaneStates, mergeToID)
	mergeFromID := uint64(1)
	mergeFrom := getLaneState(t, rt, st1.LaneStates, mergeFromID)

	// Note sv.Amount = 3
	sv.Lane = mergeToID
	mergeNonce := mergeTo.Nonce + 10

	merges := []Merge{{Lane: mergeFromID, Nonce: mergeNonce}}
	sv.Merges = merges

	ucp := &UpdateChannelStateParams{Sv: *sv}
	rt.ExpectValidateCallerAddr(st1.From, st1.To)
	rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payee, voucherBytes(t, &ucp.Sv), nil)
	ret := rt.Call(actor.UpdateChannelState, ucp)
	require.Nil(t, ret)
	rt.Verify()

	expMergeTo := LaneState{Redeemed: sv.Amount, Nonce: sv.Nonce}
	expMergeFrom := LaneState{Redeemed: mergeFrom.Redeemed, Nonce: mergeNonce}

	// calculate ToSend amount
	redeemed := big.Add(mergeFrom.Redeemed, mergeTo.Redeemed)
	expDelta := big.Sub(sv.Amount, redeemed)
	expSendAmt := big.Add(st1.ToSend, expDelta)

	// last lane should be unchanged
	expState := st1
	expState.ToSend = expSendAmt
	expState.LaneStates = constructLaneStateAMT(t, rt, []*LaneState{&expMergeTo, &expMergeFrom, getLaneState(t, rt, st1.LaneStates, 2)})
	verifyState(t, rt, numLanes, expState)
	actor.checkState(rt)
}

func TestActor_UpdateChannelStateMergeFailure(t *testing.T) {
	testCases := []struct {
		name                           string
		balance                        int64
		lane, voucherNonce, mergeNonce uint64
		expExitCode                    exitcode.ExitCode
	}{
		{
			name: "fails: merged lane in voucher has outdated nonce, cannot redeem",
			lane: 1, voucherNonce: 10, mergeNonce: 1,
			expExitCode: exitcode.ErrIllegalArgument,
		},
		{
			name: "fails: voucher has an outdated nonce, cannot redeem",
			lane: 1, voucherNonce: 0, mergeNonce: 10,
			expExitCode: exitcode.ErrIllegalArgument,
		},
		{
			name: "fails: not enough funds in channel to cover voucher",
			lane: 1, balance: 1, voucherNonce: 10, mergeNonce: 10,
			expExitCode: exitcode.ErrIllegalArgument,
		},
		{
			name: "fails: voucher cannot merge lanes into its own lane",
			lane: 0, voucherNonce: 10, mergeNonce: 10,
			expExitCode: exitcode.ErrIllegalArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 2)
			if tc.balance > 0 {
				rt.SetBalance(abi.NewTokenAmount(tc.balance))
			}

			var st1 State
			rt.GetState(&st1)
			mergeToID := uint64(0)
			mergeFromID := uint64(tc.lane)

			sv.Lane = mergeToID
			sv.Nonce = tc.voucherNonce
			merges := []Merge{{Lane: mergeFromID, Nonce: tc.mergeNonce}}
			sv.Merges = merges
			ucp := &UpdateChannelStateParams{Sv: *sv}

			rt.SetCaller(st1.From, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerAddr(st1.From, st1.To)
			rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payee, voucherBytes(t, &ucp.Sv), nil)
			rt.ExpectAbort(tc.expExitCode, func() {
				rt.Call(actor.UpdateChannelState, ucp)
			})
			rt.Verify()

		})
	}
	t.Run("When lane doesn't exist, fails with: voucher specifies invalid merge lane 999", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 2)

		var st1 State
		rt.GetState(&st1)
		mergeToID := uint64(0)
		mergeFromID := uint64(999)

		sv.Lane = mergeToID
		sv.Nonce = 10
		merges := []Merge{{Lane: mergeFromID, Nonce: sv.Nonce}}
		sv.Merges = merges
		ucp := &UpdateChannelStateParams{Sv: *sv}

		rt.SetCaller(st1.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payee, voucherBytes(t, &ucp.Sv), nil)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})
		rt.Verify()
	})

	t.Run("Lane ID over max fails", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 1)

		var st1 State
		rt.GetState(&st1)
		sv.Lane = MaxLane + 1
		sv.Nonce++
		sv.Amount = abi.NewTokenAmount(100)
		ucp := &UpdateChannelStateParams{Sv: *sv}
		rt.SetCaller(st1.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payee, voucherBytes(t, &ucp.Sv), nil)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})
		rt.Verify()
	})
}

func TestActor_UpdateChannelStateExtra(t *testing.T) {
	mnum := builtin.MethodsPaych.UpdateChannelState
	fakeParams := cbg.CborBoolTrue
	expSendParams := &cbg.Deferred{Raw: fakeParams}
	otherAddr := tutil.NewIDAddr(t, 104)
	ex := &ModVerifyParams{
		Actor:  otherAddr,
		Method: mnum,
		Data:   fakeParams,
	}

	t.Run("Succeeds if extra call succeeds", func(t *testing.T) {
		rt, actor1, sv1 := requireCreateChannelWithLanes(t, context.Background(), 1)
		var st1 State
		rt.GetState(&st1)
		rt.SetCaller(st1.From, builtin.AccountActorCodeID)

		ucp := &UpdateChannelStateParams{Sv: *sv1}
		ucp.Sv.Extra = ex

		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, st1.To, voucherBytes(t, &ucp.Sv), nil)
		rt.ExpectSend(otherAddr, mnum, expSendParams, big.Zero(), nil, exitcode.Ok)
		rt.Call(actor1.UpdateChannelState, ucp)
		rt.Verify()
		actor1.checkState(rt)
	})
	t.Run("If Extra call fails, fails with: spend voucher verification failed", func(t *testing.T) {
		rt, actor1, sv1 := requireCreateChannelWithLanes(t, context.Background(), 1)
		var st1 State
		rt.GetState(&st1)
		rt.SetCaller(st1.From, builtin.AccountActorCodeID)

		ucp := &UpdateChannelStateParams{Sv: *sv1}
		ucp.Sv.Extra = ex

		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		rt.ExpectSend(otherAddr, mnum, expSendParams, big.Zero(), nil, exitcode.ErrIllegalArgument)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, st1.To, voucherBytes(t, &ucp.Sv), nil)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor1.UpdateChannelState, ucp)
		})
		rt.Verify()
	})
}

func TestActor_UpdateChannelStateSettling(t *testing.T) {
	rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 1)

	ep := abi.ChainEpoch(10)
	rt.SetEpoch(ep)
	var st State
	rt.GetState(&st)

	rt.SetCaller(st.From, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(st.From, st.To)
	rt.Call(actor.Settle, nil)

	expSettlingAt := ep + SettleDelay
	rt.GetState(&st)
	require.Equal(t, expSettlingAt, st.SettlingAt)
	require.Equal(t, abi.ChainEpoch(0), st.MinSettleHeight)

	ucp := &UpdateChannelStateParams{Sv: *sv}

	testCases := []struct {
		name                                               string
		minSettleHeight, expSettlingAt, expMinSettleHeight abi.ChainEpoch
		//expExitCode                                        exitcode.ExitCode
	}{
		{name: "No change",
			minSettleHeight: 0, expMinSettleHeight: st.MinSettleHeight,
			expSettlingAt: st.SettlingAt},
		{name: "Updates MinSettleHeight only",
			minSettleHeight: abi.ChainEpoch(2), expMinSettleHeight: abi.ChainEpoch(2),
			expSettlingAt: st.SettlingAt},
		{name: "SettlingAt unchanged even after MinSettleHeight is changed because it is greater than MinSettleHeight",
			minSettleHeight: abi.ChainEpoch(12), expMinSettleHeight: abi.ChainEpoch(12),
			expSettlingAt: st.SettlingAt},
		{name: "SettlingAt changes after MinSettleHeight is changed because it is less than MinSettleHeight",
			minSettleHeight: st.SettlingAt + 1, expMinSettleHeight: st.SettlingAt + 1,
			expSettlingAt: st.SettlingAt + 1},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var newSt State
			ucp.Sv.MinSettleHeight = tc.minSettleHeight
			rt.ExpectValidateCallerAddr(st.From, st.To)
			rt.ExpectVerifySignature(*ucp.Sv.Signature, st.To, voucherBytes(t, &ucp.Sv), nil)
			rt.Call(actor.UpdateChannelState, ucp)
			rt.GetState(&newSt)
			assert.Equal(t, tc.expSettlingAt, newSt.SettlingAt)
			assert.Equal(t, tc.expMinSettleHeight, newSt.MinSettleHeight)
			ucp.Sv.Nonce = ucp.Sv.Nonce + 1
			actor.checkState(rt)
		})
	}
}

func TestActor_UpdateChannelStateSecretPreimage(t *testing.T) {
	t.Run("Succeeds with correct secret", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 1)
		var st State
		rt.GetState(&st)

		rt.SetHasher(func(data []byte) [32]byte {
			aux := []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
			var res [32]byte
			copy(res[:], aux)
			copy(res[:], data)
			return res
		})
		ucp := &UpdateChannelStateParams{
			Sv:     *sv,
			Secret: []byte("Profesr"),
		}
		ucp.Sv.SecretPreimage = []byte("ProfesrXXXXXXXXXXXXXXXXXXXXXXXXX")
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, st.To, voucherBytes(t, &ucp.Sv), nil)
		rt.Call(actor.UpdateChannelState, ucp)
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("If bad secret preimage, fails with: incorrect secret!", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 1)
		var st State
		rt.GetState(&st)
		ucp := &UpdateChannelStateParams{
			Sv:     *sv,
			Secret: []byte("Profesr"),
		}
		ucp.Sv.SecretPreimage = append([]byte("Magneto"), make([]byte, 25)...)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, st.To, voucherBytes(t, &ucp.Sv), nil)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})
		rt.Verify()
	})
}

func TestActor_Settle(t *testing.T) {
	ep := abi.ChainEpoch(10)

	t.Run("Settle adjusts SettlingAt", func(t *testing.T) {
		rt, actor, _ := requireCreateChannelWithLanes(t, context.Background(), 1)
		rt.SetEpoch(ep)
		var st State
		rt.GetState(&st)

		rt.SetCaller(st.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.Call(actor.Settle, nil)

		expSettlingAt := ep + SettleDelay
		rt.GetState(&st)
		assert.Equal(t, expSettlingAt, st.SettlingAt)
		assert.Equal(t, abi.ChainEpoch(0), st.MinSettleHeight)
		actor.checkState(rt)
	})

	t.Run("settle fails if called twice: channel already settling", func(t *testing.T) {
		rt, actor, _ := requireCreateChannelWithLanes(t, context.Background(), 1)
		rt.SetEpoch(ep)
		var st State
		rt.GetState(&st)

		rt.SetCaller(st.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.Call(actor.Settle, nil)

		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			rt.Call(actor.Settle, nil)
		})
	})

	t.Run("Settle changes SettleHeight again if MinSettleHeight is less", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 1)
		rt.SetEpoch(ep)
		var st State
		rt.GetState(&st)

		// UpdateChannelState to increase MinSettleHeight only
		ucp := &UpdateChannelStateParams{Sv: *sv}
		ucp.Sv.MinSettleHeight = (ep + SettleDelay) + 1

		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, st.To, voucherBytes(t, &ucp.Sv), nil)
		rt.Call(actor.UpdateChannelState, ucp)

		var newSt State
		rt.GetState(&newSt)
		// SettlingAt should remain the same.
		require.Equal(t, abi.ChainEpoch(0), newSt.SettlingAt)
		require.Equal(t, ucp.Sv.MinSettleHeight, newSt.MinSettleHeight)

		// Settle.
		rt.SetCaller(st.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.Call(actor.Settle, nil)

		// SettlingAt should = MinSettleHeight, not epoch + SettleDelay.
		rt.GetState(&newSt)
		assert.Equal(t, ucp.Sv.MinSettleHeight, newSt.SettlingAt)
		actor.checkState(rt)
	})

	t.Run("Voucher invalid after settling", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 1)
		rt.SetEpoch(ep)
		var st State
		rt.GetState(&st)

		rt.SetCaller(st.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.Call(actor.Settle, nil)

		rt.GetState(&st)
		rt.SetEpoch(st.SettlingAt + 40)
		ucp := &UpdateChannelStateParams{Sv: *sv}
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.ExpectVerifySignature(*ucp.Sv.Signature, actor.payee, voucherBytes(t, &ucp.Sv), nil)
		rt.ExpectAbort(ErrChannelStateUpdateAfterSettled, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})

	})
}

func TestActor_Collect(t *testing.T) {
	t.Run("Happy path", func(t *testing.T) {
		rt, actor, _ := requireCreateChannelWithLanes(t, context.Background(), 1)
		currEpoch := abi.ChainEpoch(10)
		rt.SetEpoch(currEpoch)
		var st State
		rt.GetState(&st)

		// Settle.
		rt.SetCaller(st.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.Call(actor.Settle, nil)

		rt.GetState(&st)
		require.EqualValues(t, SettleDelay+currEpoch, st.SettlingAt)
		rt.ExpectValidateCallerAddr(st.From, st.To)

		// "wait" for SettlingAt epoch
		rt.SetEpoch(st.SettlingAt + 1)

		rt.ExpectSend(st.To, builtin.MethodSend, nil, st.ToSend, nil, exitcode.Ok)

		// Collect.
		rt.SetCaller(st.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.ExpectDeleteActor(st.From)
		res := rt.Call(actor.Collect, nil)
		assert.Nil(t, res)
		actor.checkState(rt)
	})

	testCases := []struct {
		name                                           string
		expSendToCode, expSendFromCode, expCollectExit exitcode.ExitCode
		dontSettle                                     bool
	}{
		{name: "fails if not settling with: payment channel not settling or settled", dontSettle: true, expCollectExit: exitcode.ErrForbidden},
		{name: "fails if Failed to send funds to `To`", expSendToCode: exitcode.ErrIllegalArgument, expCollectExit: exitcode.ErrIllegalArgument},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rt, actor, _ := requireCreateChannelWithLanes(t, context.Background(), 1)
			currEpoch := abi.ChainEpoch(10)
			rt.SetEpoch(currEpoch)
			var st State
			rt.GetState(&st)

			if !tc.dontSettle {
				rt.SetCaller(st.From, builtin.AccountActorCodeID)
				rt.ExpectValidateCallerAddr(st.From, st.To)
				rt.Call(actor.Settle, nil)
				rt.GetState(&st)
				require.Equal(t, SettleDelay+currEpoch, st.SettlingAt)
			}

			// "wait" for SettlingAt epoch
			rt.SetEpoch(st.SettlingAt + 1)

			rt.ExpectSend(st.To, builtin.MethodSend, nil, st.ToSend, nil, tc.expSendToCode)

			// Collect.
			rt.SetCaller(st.From, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerAddr(st.From, st.To)
			rt.ExpectAbort(tc.expCollectExit, func() {
				rt.Call(actor.Collect, nil)
			})
		})
	}
}

type pcActorHarness struct {
	Actor
	t testing.TB

	addr  addr.Address
	payer addr.Address
	payee addr.Address
}

type laneParams struct {
	epochNum    int64
	from, to    addr.Address
	amt         big.Int
	lane, nonce uint64
	merges      []Merge
}

func requireCreateChannelWithLanes(t *testing.T, ctx context.Context, numLanes int) (*mock.Runtime, *pcActorHarness, *SignedVoucher) {
	paychAddr := tutil.NewIDAddr(t, 100)
	payerAddr := tutil.NewIDAddr(t, 102)
	payeeAddr := tutil.NewIDAddr(t, 103)
	balance := abi.NewTokenAmount(100000)
	received := abi.NewTokenAmount(0)

	curEpoch := 2
	hasher := func(data []byte) [32]byte { return [32]byte{} }

	builder := mock.NewBuilder(ctx, paychAddr).
		WithBalance(balance, received).
		WithEpoch(abi.ChainEpoch(curEpoch)).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID).
		WithActorType(payerAddr, builtin.AccountActorCodeID).
		WithActorType(payeeAddr, builtin.AccountActorCodeID).
		WithHasher(hasher)

	actor := pcActorHarness{Actor{}, t, paychAddr, payerAddr, payeeAddr}

	rt := builder.Build(t)
	actor.constructAndVerify(t, rt, payerAddr, payeeAddr)

	var lastSv *SignedVoucher
	for i := 0; i < numLanes; i++ {
		amt := big.NewInt(int64(i + 1))
		lastSv = requireAddNewLane(t, rt, &actor, laneParams{
			epochNum: int64(curEpoch),
			from:     payerAddr,
			to:       payeeAddr,
			amt:      amt,
			lane:     uint64(i),
			nonce:    uint64(i + 1),
		})
	}
	return rt, &actor, lastSv
}

func requireAddNewLane(t *testing.T, rt *mock.Runtime, actor *pcActorHarness, params laneParams) *SignedVoucher {
	sig := &crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte{0, 1, 2, 3, 4, 5, 6, 7}}
	tl := abi.ChainEpoch(params.epochNum)
	sv := SignedVoucher{ChannelAddr: actor.addr, TimeLockMin: tl, TimeLockMax: math.MaxInt64, Lane: params.lane, Nonce: params.nonce, Amount: params.amt, Signature: sig, Merges: params.merges}
	ucp := &UpdateChannelStateParams{Sv: sv}

	rt.SetCaller(params.from, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(params.from, params.to)
	rt.ExpectVerifySignature(*sv.Signature, actor.payee, voucherBytes(t, &sv), nil)
	ret := rt.Call(actor.UpdateChannelState, ucp)
	require.Nil(t, ret)
	rt.Verify()
	sv.Nonce = sv.Nonce + 1
	return &sv
}

func (h *pcActorHarness) constructAndVerify(t *testing.T, rt *mock.Runtime, sender, receiver addr.Address) {
	params := &ConstructorParams{To: receiver, From: sender}

	rt.ExpectValidateCallerType(builtin.InitActorCodeID)
	ret := rt.Call(h.Actor.Constructor, params)
	assert.Nil(h.t, ret)
	rt.Verify()

	senderId, ok := rt.GetIdAddr(sender)
	require.True(h.t, ok)

	receiverId, ok := rt.GetIdAddr(receiver)
	require.True(h.t, ok)

	verifyInitialState(t, rt, senderId, receiverId)
}

func (h *pcActorHarness) checkState(rt *mock.Runtime) {
	var st State
	rt.GetState(&st)
	_, msgs := CheckStateInvariants(&st, rt.AdtStore(), rt.Balance())
	assert.True(h.t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))
}

func verifyInitialState(t *testing.T, rt *mock.Runtime, sender, receiver addr.Address) {
	var st State
	rt.GetState(&st)
	emptyArrCid, err := adt.MakeEmptyArray(adt.AsStore(rt)).Root()
	assert.NoError(t, err)
	expectedState := State{From: sender, To: receiver, ToSend: abi.NewTokenAmount(0), LaneStates: emptyArrCid}
	verifyState(t, rt, -1, expectedState)
}

func verifyState(t *testing.T, rt *mock.Runtime, expLanes int, expectedState State) {
	var st State
	rt.GetState(&st)
	assert.Equal(t, expectedState.To, st.To)
	assert.Equal(t, expectedState.From, st.From)
	assert.Equal(t, expectedState.MinSettleHeight, st.MinSettleHeight)
	assert.Equal(t, expectedState.SettlingAt, st.SettlingAt)
	assert.Equal(t, expectedState.ToSend, st.ToSend)
	if expLanes >= 0 {
		assertLaneStatesLength(t, rt, st.LaneStates, expLanes)
		assert.True(t, reflect.DeepEqual(expectedState.LaneStates, st.LaneStates))
	} else {
		ecid, err := adt.MakeEmptyArray(adt.AsStore(rt)).Root()
		assert.NoError(t, err)
		assert.Equal(t, st.LaneStates, ecid)
	}
}

func voucherBytes(t *testing.T, sv *SignedVoucher) []byte {
	bytes, err := sv.SigningBytes()
	require.NoError(t, err)
	return bytes
}
