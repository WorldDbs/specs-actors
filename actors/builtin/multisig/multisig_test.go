package multisig_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/minio/blake2b-simd"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, multisig.Actor{})
}

func TestConstruction(t *testing.T) {
	actor := multisig.Actor{}

	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	anneNonId := tutil.NewBLSAddr(t, 1)

	bob := tutil.NewIDAddr(t, 102)
	bobNonId := tutil.NewBLSAddr(t, 2)

	charlie := tutil.NewIDAddr(t, 103)

	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		startEpoch := abi.ChainEpoch(100)
		unlockDuration := abi.ChainEpoch(200)

		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob, charlie},
			NumApprovalsThreshold: 2,
			UnlockDuration:        unlockDuration,
			StartEpoch:            startEpoch,
		}

		rt.SetReceived(abi.NewTokenAmount(100))
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		ret := rt.Call(actor.Constructor, &params)
		assert.Nil(t, ret)
		rt.Verify()

		var st multisig.State
		rt.GetState(&st)
		assert.Equal(t, params.Signers, st.Signers)
		assert.Equal(t, params.NumApprovalsThreshold, st.NumApprovalsThreshold)
		assert.Equal(t, abi.NewTokenAmount(100), st.InitialBalance)
		assert.Equal(t, unlockDuration, st.UnlockDuration)
		assert.Equal(t, startEpoch, st.StartEpoch)
		txns, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		assert.NoError(t, err)
		keys, err := txns.CollectKeys()
		require.NoError(t, err)
		assert.Empty(t, keys)

		assertStateInvariants(t, rt, &st)
	})

	t.Run("construction by resolving signers to ID addresses", func(t *testing.T) {
		rt := builder.Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anneNonId, bobNonId, charlie},
			NumApprovalsThreshold: 2,
			UnlockDuration:        0,
		}
		rt.AddIDAddress(anneNonId, anne)
		rt.AddIDAddress(bobNonId, bob)

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		ret := rt.Call(actor.Constructor, &params)
		assert.Nil(t, ret)
		rt.Verify()

		var st multisig.State
		rt.GetState(&st)
		require.Equal(t, []addr.Address{anne, bob, charlie}, st.Signers)

		assertStateInvariants(t, rt, &st)
	})

	t.Run("construction with vesting", func(t *testing.T) {
		rt := builder.WithEpoch(1234).Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob, charlie},
			NumApprovalsThreshold: 3,
			UnlockDuration:        100,
			StartEpoch:            1234,
		}
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		ret := rt.Call(actor.Constructor, &params)
		assert.Nil(t, ret)
		rt.Verify()

		var st multisig.State
		rt.GetState(&st)
		assert.Equal(t, params.Signers, st.Signers)
		assert.Equal(t, params.NumApprovalsThreshold, st.NumApprovalsThreshold)
		assert.Equal(t, abi.NewTokenAmount(0), st.InitialBalance)
		assert.Equal(t, abi.ChainEpoch(100), st.UnlockDuration)
		assert.Equal(t, abi.ChainEpoch(1234), st.StartEpoch)

		// assert no transactions
		empty, err := adt.MakeEmptyMap(rt.AdtStore()).Root()
		require.NoError(t, err)
		assert.Equal(t, empty, st.PendingTxns)

		assertStateInvariants(t, rt, &st)
	})

	t.Run("fail to construct multisig actor with 0 signers", func(t *testing.T) {
		rt := builder.Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{},
			NumApprovalsThreshold: 1,
			UnlockDuration:        1,
		}
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()

	})

	t.Run("fail to construct multisig actor with more than max signers", func(t *testing.T) {
		rt := builder.Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{},
			NumApprovalsThreshold: 1,
			UnlockDuration:        1,
		}
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()
	})

	t.Run("fail to construct multisig with more approvals than signers", func(t *testing.T) {
		rt := builder.Build(t)
		signers := make([]addr.Address, multisig.SignersMax+1)
		for i := range signers {
			signers[i] = tutil.NewIDAddr(t, uint64(101+i))
		}
		params := multisig.ConstructorParams{
			Signers:               signers,
			NumApprovalsThreshold: 4,
			UnlockDuration:        1,
		}
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "cannot add more than 256 signers", func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()
	})

	t.Run("fail to construct multisig if a signer is not resolvable to an ID address", func(t *testing.T) {
		builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
		rt := builder.Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anneNonId, bob, charlie},
			NumApprovalsThreshold: 2,
			UnlockDuration:        1,
		}
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectSend(anneNonId, builtin.MethodSend, nil, abi.NewTokenAmount(0), nil, exitcode.Ok)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()
	})

	t.Run("fail to construct multisig with duplicate signers(all ID addresses)", func(t *testing.T) {
		rt := builder.Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob, bob},
			NumApprovalsThreshold: 2,
			UnlockDuration:        0,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()
	})

	t.Run("fail to construct multisig with duplicate signers(ID & non-ID addresses)", func(t *testing.T) {
		rt := builder.Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bobNonId, bob},
			NumApprovalsThreshold: 2,
			UnlockDuration:        0,
		}

		rt.AddIDAddress(bobNonId, bob)
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()
	})
}

func TestVesting(t *testing.T) {
	actor := msActorHarness{multisig.Actor{}, t}
	startEpoch := abi.ChainEpoch(0)

	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	bob := tutil.NewIDAddr(t, 102)
	charlie := tutil.NewIDAddr(t, 103)
	darlene := tutil.NewIDAddr(t, 103)

	const unlockDuration = abi.ChainEpoch(10)
	var multisigInitialBalance = abi.NewTokenAmount(100)

	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID).
		WithEpoch(0).
		WithBalance(multisigInitialBalance, multisigInitialBalance).
		WithHasher(blake2b.Sum256)

	t.Run("happy path full vesting", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 2, unlockDuration, startEpoch, anne, bob, charlie)
		rt.SetReceived(big.Zero())

		// anne proposes that darlene receives `multisgiInitialBalance` FIL.
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, darlene, multisigInitialBalance, builtin.MethodSend, nil, nil)

		// bob approves anne's transaction too soon
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.approveOK(rt, 0, proposalHashData, nil)
		})
		rt.Reset()

		// Advance the epoch s.t. all funds are unlocked.
		rt.SetEpoch(0 + unlockDuration)
		// expect darlene to receive the transaction proposed by anne.
		rt.ExpectSend(darlene, builtin.MethodSend, nil, multisigInitialBalance, nil, exitcode.Ok)
		actor.approveOK(rt, 0, proposalHashData, nil)
		actor.checkState(rt)
	})

	t.Run("partial vesting propose to send half the actor balance when the epoch is half the unlock duration", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 2, 10, startEpoch, anne, bob, charlie)
		rt.SetReceived(big.Zero())

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, darlene, big.Div(multisigInitialBalance, big.NewInt(2)), builtin.MethodSend, nil, nil)

		// set the current balance of the multisig actor to its InitialBalance amount
		rt.SetEpoch(0 + unlockDuration/2)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(darlene, builtin.MethodSend, nil, big.Div(multisigInitialBalance, big.NewInt(2)), nil, exitcode.Ok)
		actor.approveOK(rt, 0, proposalHashData, nil)
		actor.checkState(rt)
	})

	t.Run("propose and autoapprove transaction above locked amount fails", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 1, unlockDuration, startEpoch, anne, bob, charlie)
		rt.SetReceived(big.Zero())

		// this propose will fail since it would send more than the required locked balance and num approvals == 1
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, darlene, abi.NewTokenAmount(100), builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// this will pass since sending below the locked amount is permitted
		rt.SetEpoch(1)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectSend(darlene, builtin.MethodSend, nil, abi.NewTokenAmount(10), nil, 0)
		actor.proposeOK(rt, darlene, abi.NewTokenAmount(10), builtin.MethodSend, nil, nil)
		actor.checkState(rt)
	})

	t.Run("fail to vest more than locked amount", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 2, unlockDuration, startEpoch, anne, bob, charlie)
		rt.SetReceived(big.Zero())

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, darlene, big.Div(multisigInitialBalance, big.NewInt(2)), builtin.MethodSend, nil, nil)

		// this propose will fail since it would send more than the required locked balance.
		rt.SetEpoch(1)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.approve(rt, 0, proposalHashData, nil)
		})
	})

	t.Run("avoid truncating division", func(t *testing.T) {
		rt := builder.Build(t)

		lockedBalance := big.NewInt(int64(unlockDuration) - 1) // Balance < duration
		rt.SetReceived(lockedBalance)
		rt.SetBalance(lockedBalance)
		actor.constructAndVerify(rt, 1, unlockDuration, startEpoch, anne)
		rt.SetReceived(big.Zero())

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		// Expect nothing vested yet
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.proposeOK(rt, anne, big.NewInt(1), builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Expect nothing (<1 unit) vested after 1 epoch
		rt.SetEpoch(1)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.proposeOK(rt, anne, big.NewInt(1), builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Expect 1 unit available after 2 epochs
		rt.SetEpoch(2)
		rt.ExpectSend(anne, builtin.MethodSend, nil, big.NewInt(1), nil, exitcode.Ok)
		actor.proposeOK(rt, anne, big.NewInt(1), builtin.MethodSend, nil, nil)
		rt.SetBalance(lockedBalance)

		// Do not expect full vesting before full duration has elapsed
		rt.SetEpoch(unlockDuration - 1)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.proposeOK(rt, anne, lockedBalance, builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Expect all but one unit available after all but one epochs
		rt.ExpectSend(anne, builtin.MethodSend, nil, big.Sub(lockedBalance, big.NewInt(1)), nil, exitcode.Ok)
		actor.proposeOK(rt, anne, big.Sub(lockedBalance, big.NewInt(1)), builtin.MethodSend, nil, nil)
		rt.SetBalance(lockedBalance)

		// Expect everything after exactly the right epochs
		rt.SetBalance(lockedBalance)
		rt.SetEpoch(unlockDuration)
		rt.ExpectSend(anne, builtin.MethodSend, nil, lockedBalance, nil, exitcode.Ok)
		actor.proposeOK(rt, anne, lockedBalance, builtin.MethodSend, nil, nil)
		actor.checkState(rt)
	})

	t.Run("sending zero ok when nothing vested", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, 1, unlockDuration, startEpoch, anne)
		rt.SetReceived(big.Zero())

		sendAmount := abi.NewTokenAmount(0)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectSend(bob, builtin.MethodSend, nil, sendAmount, nil, 0)
		actor.proposeOK(rt, bob, sendAmount, builtin.MethodSend, nil, nil)
		actor.checkState(rt)
	})

	t.Run("sending zero ok when lockup exceeds balance", func(t *testing.T) {
		rt := builder.Build(t)
		rt.SetReceived(big.Zero())
		rt.SetBalance(big.Zero())
		actor.constructAndVerify(rt, 1, 0, startEpoch, anne)

		// Lock up funds the actor doesn't have yet.
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.lockBalance(rt, startEpoch, unlockDuration, abi.NewTokenAmount(10))

		// Make a transaction that transfers no value.
		sendAmount := abi.NewTokenAmount(0)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectSend(bob, builtin.MethodSend, nil, sendAmount, nil, 0)
		actor.proposeOK(rt, bob, sendAmount, builtin.MethodSend, nil, nil)

		// Verify that sending any value is prevented
		sendAmount = abi.NewTokenAmount(1)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, bob, sendAmount, builtin.MethodSend, nil, nil)
		})
	})
}

func TestPropose(t *testing.T) {
	actor := msActorHarness{multisig.Actor{}, t}
	startEpoch := abi.ChainEpoch(0)

	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	bob := tutil.NewIDAddr(t, 102)
	chuck := tutil.NewIDAddr(t, 103)

	const noUnlockDuration = abi.ChainEpoch(0)
	var sendValue = abi.NewTokenAmount(10)
	var fakeParams = builtin.CBORBytes([]byte{1, 2, 3, 4})
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose", func(t *testing.T) {
		const numApprovals = uint64(2)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, sendValue, builtin.MethodSend, fakeParams, nil)

		// the transaction remains awaiting second approval
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})
		actor.checkState(rt)
	})

	t.Run("propose with threshold met", func(t *testing.T) {
		const numApprovals = uint64(1)

		rt := builder.WithBalance(abi.NewTokenAmount(20), abi.NewTokenAmount(0)).Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.ExpectSend(chuck, builtin.MethodSend, fakeParams, sendValue, nil, 0)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, sendValue, builtin.MethodSend, fakeParams, nil)

		// the transaction has been sent and cleaned up
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("propose with threshold and non-empty return value", func(t *testing.T) {
		const numApprovals = uint64(1)

		rt := builder.WithBalance(abi.NewTokenAmount(20), abi.NewTokenAmount(0)).Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		proposeRet := miner.GetControlAddressesReturn{
			Owner:  tutil.NewIDAddr(t, 1),
			Worker: tutil.NewIDAddr(t, 2),
		}
		rt.ExpectSend(chuck, builtin.MethodsMiner.ControlAddresses, fakeParams, sendValue, &proposeRet, 0)

		rt.SetCaller(anne, builtin.AccountActorCodeID)

		var out miner.GetControlAddressesReturn
		actor.proposeOK(rt, chuck, sendValue, builtin.MethodsMiner.ControlAddresses, fakeParams, &out)
		// assert ProposeReturn.Ret can be marshaled into the expected structure.
		assert.Equal(t, proposeRet, out)

		// the transaction has been sent and cleaned up
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("fail propose with threshold met and insufficient balance", func(t *testing.T) {
		const numApprovals = uint64(1)
		rt := builder.WithBalance(abi.NewTokenAmount(0), abi.NewTokenAmount(0)).Build(t)
		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, chuck, sendValue, builtin.MethodSend, fakeParams, nil)
		})
		rt.Reset()

		// proposal failed since it should have but failed to immediately execute.
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("fail propose from non-signer", func(t *testing.T) {
		// non-signer address
		richard := tutil.NewIDAddr(t, 105)
		const numApprovals = uint64(2)

		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			_ = actor.propose(rt, chuck, sendValue, builtin.MethodSend, fakeParams, nil)
		})
		rt.Reset()

		// the transaction is not persisted
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})
}

func TestApprove(t *testing.T) {
	actor := msActorHarness{multisig.Actor{}, t}
	startEpoch := abi.ChainEpoch(0)

	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	bob := tutil.NewIDAddr(t, 102)
	chuck := tutil.NewIDAddr(t, 103)

	const noUnlockDuration = abi.ChainEpoch(0)
	const numApprovals = uint64(2)
	const txnID = int64(0)
	const fakeMethod = abi.MethodNum(42)
	var sendValue = abi.NewTokenAmount(10)
	var fakeParams = builtin.CBORBytes([]byte{1, 2, 3, 4})
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID).
		WithHasher(blake2b.Sum256)

	t.Run("simple propose and approval", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})

		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(chuck, fakeMethod, fakeParams, sendValue, nil, 0)
		actor.approveOK(rt, txnID, proposalHashData, nil)

		// Transaction should be removed from actor state after send
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("approve with non-empty return value", func(t *testing.T) {
		const numApprovals = uint64(2)

		rt := builder.WithBalance(abi.NewTokenAmount(20), abi.NewTokenAmount(0)).Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, builtin.MethodsMiner.ControlAddresses, fakeParams, nil)

		approveRet := miner.GetControlAddressesReturn{
			Owner:  tutil.NewIDAddr(t, 1),
			Worker: tutil.NewIDAddr(t, 2),
		}

		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(chuck, builtin.MethodsMiner.ControlAddresses, fakeParams, sendValue, &approveRet, 0)
		var out miner.GetControlAddressesReturn
		actor.approveOK(rt, txnID, proposalHashData, &out)
		// assert approveRet.Ret can be marshaled into the expected structure.
		assert.Equal(t, approveRet, out)

		// the transaction has been sent and cleaned up
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("approval works if enough funds have been unlocked for the transaction", func(t *testing.T) {
		rt := builder.Build(t)
		unlockDuration := abi.ChainEpoch(20)
		startEpoch := abi.ChainEpoch(10)
		sendValue := abi.NewTokenAmount(20)

		rt.SetReceived(sendValue)
		actor.constructAndVerify(rt, numApprovals, unlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHash := actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})

		rt.SetEpoch(startEpoch + 20)
		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(chuck, fakeMethod, fakeParams, sendValue, nil, 0)

		// as the (current epoch - startepoch) = 20 is  equal to unlock duration, all initial funds must have been vested and available to spend
		actor.approveOK(rt, txnID, proposalHash, nil)
		actor.checkState(rt)
	})

	t.Run("fail approval if current balance is less than the transaction value", func(t *testing.T) {
		rt := builder.Build(t)
		numApprovals := uint64(1)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetBalance(big.Sub(sendValue, big.NewInt(1)))
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectAbortContainsMessage(exitcode.ErrInsufficientFunds, "insufficient funds unlocked: current balance 9 less than amount to spend 10", func() {
			_ = actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams, nil)
		})
	})

	t.Run("fail approval if enough unlocked balance not available", func(t *testing.T) {
		rt := builder.Build(t)
		unlockDuration := abi.ChainEpoch(20)
		startEpoch := abi.ChainEpoch(10)
		sendValue := abi.NewTokenAmount(20)

		rt.SetReceived(sendValue)
		actor.constructAndVerify(rt, numApprovals, unlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHash := actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})

		rt.SetEpoch(startEpoch + 5)
		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		// expected locked amount at epoch=startEpoch + 5 would be 15.
		// however, remaining funds if this transactions is approved would be 0.
		rt.ExpectAbortContainsMessage(exitcode.ErrInsufficientFunds, "insufficient funds unlocked: balance 0 if spent 20 would be less than locked amount 15",
			func() {
				actor.approveOK(rt, txnID, proposalHash, nil)
			})
	})

	t.Run("fail approval with bad proposal hash", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})

		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(chuck, fakeMethod, fakeParams, sendValue, nil, 0)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			proposalHashData := makeProposalHash(t, &multisig.Transaction{
				To:       chuck,
				Value:    sendValue,
				Method:   fakeMethod,
				Params:   fakeParams,
				Approved: []addr.Address{bob}, // mismatch
			})
			_ = actor.approve(rt, txnID, proposalHashData, nil)
		})
	})

	t.Run("accept approval with no proposal hash", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, 0, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})

		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(chuck, fakeMethod, fakeParams, sendValue, nil, 0)
		actor.approveOK(rt, txnID, nil, nil)

		// Transaction should be removed from actor state after send
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})
	t.Run("fail approve transaction more than once", func(t *testing.T) {
		const numApprovals = uint64(2)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, builtin.MethodSend, fakeParams, nil)

		// anne is going to approve it twice and fail, poor anne.
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			_ = actor.approve(rt, txnID, proposalHashData, nil)
		})
		rt.Reset()

		// Transaction still exists
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})
		actor.checkState(rt)
	})

	t.Run("fail approve transaction that does not exist", func(t *testing.T) {
		const dneTxnID = int64(1)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, builtin.MethodSend, fakeParams, nil)

		// bob is going to approve a transaction that doesn't exist.
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			_ = actor.approve(rt, dneTxnID, proposalHashData, nil)
		})
		rt.Reset()

		// Transaction was not removed from store.
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})
		actor.checkState(rt)
	})

	t.Run("fail to approve transaction by non-signer", func(t *testing.T) {
		// non-signer address
		richard := tutil.NewIDAddr(t, 105)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, builtin.MethodSend, fakeParams, nil)

		// richard is going to approve a transaction they are not a signer for.
		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			_ = actor.approve(rt, txnID, proposalHashData, nil)
		})
		rt.Reset()

		// Transaction was not removed from store.
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})
		actor.checkState(rt)
	})

	t.Run("proposed transaction is approved by proposer if number of approvers has already crossed threshold", func(t *testing.T) {
		rt := builder.Build(t)
		const newThreshold = 1
		signers := []addr.Address{anne, bob}
		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHash := actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		// reduce the threshold so the transaction is already approved
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.changeNumApprovalsThreshold(rt, newThreshold)

		// even if anne calls for an approval again(duplicate approval), transaction is executed because the threshold has been met.
		rt.ExpectSend(chuck, fakeMethod, fakeParams, sendValue, nil, 0)
		rt.SetBalance(sendValue)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.approveOK(rt, txnID, proposalHash, nil)

		// Transaction should be removed from actor state after send
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("approve transaction if number of approvers has already crossed threshold even if we attempt a duplicate approval", func(t *testing.T) {
		rt := builder.Build(t)
		const numApprovals = 3
		const newThreshold = 2
		signers := []addr.Address{anne, bob, chuck}
		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHash := actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		// bob approves the transaction (number of approvals is now two but threshold is three)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		actor.approveOK(rt, txnID, proposalHash, nil)

		// reduce the threshold so the transaction is already approved
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.changeNumApprovalsThreshold(rt, newThreshold)

		// even if bob calls for an approval again(duplicate approval), transaction is executed because the threshold has been met.
		rt.ExpectSend(chuck, fakeMethod, fakeParams, sendValue, nil, 0)
		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		actor.approveOK(rt, txnID, proposalHash, nil)

		// Transaction should be removed from actor state after send
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("approve transaction if number of approvers has already crossed threshold and ensure non-signatory cannot approve a transaction", func(t *testing.T) {
		rt := builder.Build(t)
		const newThreshold = 1
		signers := []addr.Address{anne, bob}
		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHash := actor.proposeOK(rt, chuck, sendValue, fakeMethod, fakeParams, nil)

		// reduce the threshold so the transaction is already approved
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.changeNumApprovalsThreshold(rt, newThreshold)

		// alice cannot approve the transaction as alice is not a signatory
		alice := tutil.NewIDAddr(t, 104)
		rt.SetCaller(alice, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			_ = actor.approve(rt, txnID, proposalHash, nil)
		})
		rt.Reset()

		// bob attempts to approve the transaction but it gets approved without
		// processing his approval as it the threshold has been met.
		rt.ExpectSend(chuck, fakeMethod, fakeParams, sendValue, nil, 0)
		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		actor.approveOK(rt, txnID, proposalHash, nil)

		// Transaction should be removed from actor state after send
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})
}

func TestCancel(t *testing.T) {
	actor := msActorHarness{multisig.Actor{}, t}
	startEpoch := abi.ChainEpoch(0)

	richard := tutil.NewIDAddr(t, 104)
	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	bob := tutil.NewIDAddr(t, 102)
	chuck := tutil.NewIDAddr(t, 103)

	const noUnlockDuration = abi.ChainEpoch(0)
	const numApprovals = uint64(2)
	const txnID = int64(0)
	const fakeMethod = abi.MethodNum(42)
	var sendValue = abi.NewTokenAmount(10)
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID).
		WithHasher(blake2b.Sum256)

	t.Run("simple propose and cancel", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, fakeMethod, nil, nil)

		// anne cancels their transaction
		rt.SetBalance(sendValue)
		actor.cancel(rt, txnID, proposalHashData)

		// Transaction should be removed from actor state after cancel
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})

	t.Run("fail cancel with bad proposal hash", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, sendValue, fakeMethod, nil, nil)

		// anne cancels their transaction
		rt.SetBalance(sendValue)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			proposalHashData := makeProposalHash(t, &multisig.Transaction{
				To:       bob, // mismatched To
				Value:    sendValue,
				Method:   fakeMethod,
				Params:   nil,
				Approved: []addr.Address{chuck},
			})
			actor.cancel(rt, txnID, proposalHashData)
		})
	})

	t.Run("signer fails to cancel transaction from another signer", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, fakeMethod, nil, nil)

		// bob (a signer) fails to cancel anne's transaction because bob didn't create it, nice try bob.
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.cancel(rt, txnID, proposalHashData)
		})
		rt.Reset()

		// Transaction should remain after invalid cancel
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   nil,
			Approved: []addr.Address{anne},
		})
		actor.checkState(rt)
	})

	t.Run("fail to cancel transaction when not signer", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, 0, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, fakeMethod, nil, nil)

		// richard (not a signer) fails to cancel anne's transaction because richard isn't a signer, go away richard.
		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.cancel(rt, txnID, proposalHashData)
		})
		rt.Reset()

		// Transaction should remain after invalid cancel
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   nil,
			Approved: []addr.Address{anne},
		})
		actor.checkState(rt)
	})

	t.Run("fail to cancel a transaction that does not exist", func(t *testing.T) {
		rt := builder.Build(t)
		const dneTxnID = int64(1)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction ID: 0
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		proposalHashData := actor.proposeOK(rt, chuck, sendValue, fakeMethod, nil, nil)

		// anne fails to cancel a transaction that does not exists ID: 1 (dneTxnID)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.cancel(rt, dneTxnID, proposalHashData)
		})
		rt.Reset()

		// Transaction should remain after invalid cancel
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   nil,
			Approved: []addr.Address{anne},
		})
		actor.checkState(rt)
	})

	t.Run("subsequent approver replaces removed proposer as owner", func(t *testing.T) {
		rt := builder.Build(t)
		const numApprovals = 3
		signers := []addr.Address{anne, bob, chuck}

		txnId := int64(0)
		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction ID: 0
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, sendValue, fakeMethod, nil, nil)

		// bob approves the transaction -> he is the second approver
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		actor.approveOK(rt, txnId, nil, nil)

		// remove anne as a signer, now bob is the "proposer"
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.removeSigner(rt, anne, true)

		// anne fails to cancel a transaction - she is not a signer
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.cancel(rt, txnID, nil)
		})

		// even after anne is restored as a signer, she's not the proposer any more
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.addSigner(rt, anne, true)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.cancel(rt, txnID, nil)
		})

		// Transaction should remain after invalid cancel
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   nil,
			Approved: []addr.Address{bob}, // Anne's approval is gone
		})

		// bob can cancel the transaction
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		actor.cancel(rt, txnID, nil)
		actor.checkState(rt)
	})
}

type addSignerTestCase struct {
	desc string

	idAddrsMapping   map[addr.Address]addr.Address
	initialSigners   []addr.Address
	initialApprovals uint64

	addSigner addr.Address
	increase  bool

	expectSigners   []addr.Address
	expectApprovals uint64
	code            exitcode.ExitCode
}

func TestAddSigner(t *testing.T) {
	actor := msActorHarness{multisig.Actor{}, t}
	startEpoch := abi.ChainEpoch(0)

	multisigWalletAdd := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	bob := tutil.NewIDAddr(t, 102)
	chuck := tutil.NewIDAddr(t, 103)
	chuckNonId := tutil.NewBLSAddr(t, 1)

	const noUnlockDuration = abi.ChainEpoch(0)

	testCases := []addSignerTestCase{
		{
			desc: "happy path add signer",

			initialSigners:   []addr.Address{anne, bob},
			initialApprovals: uint64(2),

			addSigner: chuck,
			increase:  false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: uint64(2),
			code:            exitcode.Ok,
		},
		{
			desc: "add signer and increase threshold",

			initialSigners:   []addr.Address{anne, bob},
			initialApprovals: uint64(2),

			addSigner: chuck,
			increase:  true,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: uint64(3),
			code:            exitcode.Ok,
		},
		{
			desc: "fail to add signer than already exists",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(3),

			addSigner: chuck,
			increase:  false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: uint64(3),
			code:            exitcode.ErrForbidden,
		},
		{
			desc: "fail to add signer with ID address that already exists(even though we ONLY have the non ID address as an approver)",

			idAddrsMapping:   map[addr.Address]addr.Address{chuckNonId: chuck},
			initialSigners:   []addr.Address{anne, bob, chuckNonId},
			initialApprovals: uint64(3),

			addSigner: chuck,
			increase:  false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: uint64(3),
			code:            exitcode.ErrForbidden,
		},
		{
			desc:             "fail to add signer with non-ID address that already exists(even though we ONLY have the ID address as an approver)",
			idAddrsMapping:   map[addr.Address]addr.Address{chuckNonId: chuck},
			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(3),

			addSigner: chuckNonId,
			increase:  false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: uint64(3),
			code:            exitcode.ErrForbidden,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			builder := mock.NewBuilder(context.Background(), multisigWalletAdd).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
			rt := builder.Build(t)
			for src, target := range tc.idAddrsMapping {
				rt.AddIDAddress(src, target)
			}

			actor.constructAndVerify(rt, tc.initialApprovals, noUnlockDuration, startEpoch, tc.initialSigners...)

			rt.SetCaller(multisigWalletAdd, builtin.MultisigActorCodeID)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.addSigner(rt, tc.addSigner, tc.increase)
				})
			} else {
				actor.addSigner(rt, tc.addSigner, tc.increase)
				var st multisig.State
				rt.GetState(&st)
				assert.Equal(t, tc.expectSigners, st.Signers)
				assert.Equal(t, tc.expectApprovals, st.NumApprovalsThreshold)
				actor.checkState(rt)
			}
			rt.Verify()
		})
	}
}

type removeSignerTestCase struct {
	desc string

	initialSigners   []addr.Address
	initialApprovals uint64

	removeSigner addr.Address
	decrease     bool

	expectSigners   []addr.Address
	expectApprovals uint64
	code            exitcode.ExitCode
}

func TestRemoveSigner(t *testing.T) {
	startEpoch := abi.ChainEpoch(0)
	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	anneNonID := tutil.NewBLSAddr(t, 1)

	bob := tutil.NewIDAddr(t, 102)
	chuck := tutil.NewIDAddr(t, 103)
	richard := tutil.NewIDAddr(t, 104)

	const noUnlockDuration = abi.ChainEpoch(0)

	actor := msActorHarness{multisig.Actor{}, t}
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	testCases := []removeSignerTestCase{
		{
			desc: "happy path remove signer",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(2),

			removeSigner: chuck,
			decrease:     false,

			expectSigners:   []addr.Address{anne, bob},
			expectApprovals: uint64(2),
			code:            exitcode.Ok,
		},
		{
			desc: "remove signer and decrease threshold",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(2),

			removeSigner: chuck,
			decrease:     true,

			expectSigners:   []addr.Address{anne, bob},
			expectApprovals: uint64(1),
			code:            exitcode.Ok,
		},
		{
			desc:             "remove signer when multi-sig is created with an ID address and then removed using it's non-ID address",
			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(2),

			removeSigner: anneNonID,
			decrease:     true,

			expectSigners:   []addr.Address{bob, chuck},
			expectApprovals: uint64(1),
			code:            exitcode.Ok,
		},
		{
			desc:             "remove signer when multi-sig is created with a non-ID address and then removed using it's ID address",
			initialSigners:   []addr.Address{anneNonID, bob, chuck},
			initialApprovals: uint64(2),

			removeSigner: anne,
			decrease:     true,

			expectSigners:   []addr.Address{bob, chuck},
			expectApprovals: uint64(1),
			code:            exitcode.Ok,
		},
		{
			desc:             "remove signer when multi-sig is created with a non-ID address and then removed using it's non-ID address",
			initialSigners:   []addr.Address{anneNonID, bob, chuck},
			initialApprovals: uint64(2),

			removeSigner: anneNonID,
			decrease:     true,

			expectSigners:   []addr.Address{bob, chuck},
			expectApprovals: uint64(1),
			code:            exitcode.Ok,
		},
		{
			desc:             "remove signer when multi-sig is created with a ID address and then removed using it's ID address",
			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(2),

			removeSigner: anne,
			decrease:     true,

			expectSigners:   []addr.Address{bob, chuck},
			expectApprovals: uint64(1),
			code:            exitcode.Ok,
		},
		{
			desc: "fail remove signer if decrease set to false and number of signers below threshold",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(3),

			removeSigner: chuck,
			decrease:     false,

			expectSigners:   []addr.Address{anne, bob},
			expectApprovals: uint64(2),
			code:            exitcode.ErrIllegalArgument,
		},
		{
			desc: "remove signer from single singer list",

			initialSigners:   []addr.Address{anne},
			initialApprovals: uint64(1),

			removeSigner: anne,
			decrease:     false,

			expectSigners:   nil,
			expectApprovals: uint64(1),
			code:            exitcode.ErrForbidden,
		},
		{
			desc: "fail to remove non-signer",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(2),

			removeSigner: richard,
			decrease:     false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: uint64(2),
			code:            exitcode.ErrForbidden,
		},
		{
			desc: "fail to remove a signer and decrease approvals below 1",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: uint64(1),

			removeSigner: anne,
			decrease:     true,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: uint64(1),
			code:            exitcode.ErrIllegalArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)
			rt.AddIDAddress(anneNonID, anne)

			actor.constructAndVerify(rt, tc.initialApprovals, noUnlockDuration, startEpoch, tc.initialSigners...)

			rt.SetCaller(receiver, builtin.MultisigActorCodeID)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.removeSigner(rt, tc.removeSigner, tc.decrease)
				})
			} else {
				actor.removeSigner(rt, tc.removeSigner, tc.decrease)
				var st multisig.State
				rt.GetState(&st)
				assert.Equal(t, tc.expectSigners, st.Signers)
				assert.Equal(t, tc.expectApprovals, st.NumApprovalsThreshold)
				actor.checkState(rt)
			}
			rt.Verify()
		})
	}

	t.Run("remove signer removes approvals", func(t *testing.T) {
		rt := builder.Build(t)
		signers := []addr.Address{anne, bob, chuck}

		actor.constructAndVerify(rt, 3, noUnlockDuration, startEpoch, signers...)

		// Anne proposes a tx
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, big.Zero(), builtin.MethodSend, nil, nil)
		// Bob approves
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		actor.approveOK(rt, 0, nil, nil)

		// Bob proposes a tx
		actor.proposeOK(rt, chuck, big.Zero(), builtin.MethodSend, nil, nil)
		// Anne approves
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.approveOK(rt, 1, nil, nil)

		// Anne is removed, threshold dropped to 2 of 2
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.removeSigner(rt, anne, true)

		// Anne's approval is removed from each transaction.
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    big.Zero(),
			Method:   builtin.MethodSend,
			Params:   nil,
			Approved: []addr.Address{bob},
		}, multisig.Transaction{
			To:       chuck,
			Value:    big.Zero(),
			Method:   builtin.MethodSend,
			Params:   nil,
			Approved: []addr.Address{bob},
		})
		actor.checkState(rt)
	})

	t.Run("remove signer deletes solo proposals", func(t *testing.T) {
		rt := builder.Build(t)
		rt.AddIDAddress(anneNonID, anne)
		signers := []addr.Address{anne, bob, chuck}

		actor.constructAndVerify(rt, 2, noUnlockDuration, startEpoch, signers...)

		// Anne proposes a tx
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, big.Zero(), builtin.MethodSend, nil, nil)

		// Anne is removed
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.removeSigner(rt, anneNonID, false) // Remove via non-ID address.

		// Transaction is gone.
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})
}

type swapTestCase struct {
	initialSigner []addr.Address
	desc          string
	to            addr.Address
	from          addr.Address
	expect        []addr.Address
	code          exitcode.ExitCode
}

func TestSwapSigners(t *testing.T) {
	startEpoch := abi.ChainEpoch(0)

	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)

	bob := tutil.NewIDAddr(t, 102)
	bobNonId := tutil.NewBLSAddr(t, 1)

	chuck := tutil.NewIDAddr(t, 103)
	darlene := tutil.NewIDAddr(t, 104)

	const noUnlockDuration = abi.ChainEpoch(0)
	const numApprovals = uint64(1)

	actor := msActorHarness{multisig.Actor{}, t}
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	testCases := []swapTestCase{
		{
			desc:          "happy path signer swap",
			initialSigner: []addr.Address{anne, bob},
			to:            chuck,
			from:          bob,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.Ok,
		},
		{
			desc:          "swap signer when multi-sig is created with it's ID address but we ask for a swap with it's non-ID address",
			initialSigner: []addr.Address{anne, bob},
			to:            chuck,
			from:          bobNonId,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.Ok,
		},
		{
			desc:          "swap signer when multi-sig is created with it's non-ID address but we ask for a swap with it's ID address",
			initialSigner: []addr.Address{anne, bobNonId},
			to:            chuck,
			from:          bob,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.Ok,
		},
		{
			desc:          "swap signer when multi-sig is created with it's non-ID address and we ask for a swap with it's non-ID address",
			initialSigner: []addr.Address{anne, bobNonId},
			to:            chuck,
			from:          bobNonId,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.Ok,
		},
		{
			desc:          "swap signer when multi-sig is created with it's ID address and we ask for a swap with it's ID address",
			initialSigner: []addr.Address{anne, bob},
			to:            chuck,
			from:          bob,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.Ok,
		},
		{
			desc:          "fail to swap when from signer not found",
			initialSigner: []addr.Address{anne, bob},
			to:            chuck,
			from:          darlene,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.ErrForbidden,
		},
		{
			desc:          "fail to swap when to signer already present",
			initialSigner: []addr.Address{anne, bob},
			to:            bob,
			from:          anne,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.ErrIllegalArgument,
		},
		{
			desc:          "fail to swap when to signer ID address already present(even though we have the non-ID address)",
			initialSigner: []addr.Address{anne, bobNonId},
			to:            bob,
			from:          anne,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.ErrIllegalArgument,
		},
		{
			desc:          "fail to swap when to signer non-ID address already present(even though we have the ID address)",
			initialSigner: []addr.Address{anne, bob},
			to:            bobNonId,
			from:          anne,
			expect:        []addr.Address{anne, chuck},
			code:          exitcode.ErrIllegalArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)
			rt.AddIDAddress(bobNonId, bob)

			actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, tc.initialSigner...)

			rt.SetCaller(receiver, builtin.MultisigActorCodeID)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.swapSigners(rt, tc.from, tc.to)
				})
			} else {
				actor.swapSigners(rt, tc.from, tc.to)
				var st multisig.State
				rt.GetState(&st)
				assert.Equal(t, tc.expect, st.Signers)
				actor.checkState(rt)
			}
			rt.Verify()
		})
	}

	t.Run("swap signer removes approvals", func(t *testing.T) {
		rt := builder.Build(t)
		signers := []addr.Address{anne, bob, chuck}

		actor.constructAndVerify(rt, 3, noUnlockDuration, startEpoch, signers...)

		// Anne proposes a tx
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, big.Zero(), builtin.MethodSend, nil, nil)
		// Bob approves
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		actor.approveOK(rt, 0, nil, nil)

		// Bob proposes a tx
		actor.proposeOK(rt, chuck, big.Zero(), builtin.MethodSend, nil, nil)
		// Anne approves
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.approveOK(rt, 1, nil, nil)

		// Anne is swapped for darlene
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.swapSigners(rt, anne, darlene)

		// Anne's approval is removed from each transaction.
		actor.assertTransactions(rt, multisig.Transaction{
			To:       chuck,
			Value:    big.Zero(),
			Method:   builtin.MethodSend,
			Params:   nil,
			Approved: []addr.Address{bob},
		}, multisig.Transaction{
			To:       chuck,
			Value:    big.Zero(),
			Method:   builtin.MethodSend,
			Params:   nil,
			Approved: []addr.Address{bob},
		})
		actor.checkState(rt)
	})

	t.Run("swap signer deletes solo proposals", func(t *testing.T) {
		rt := builder.Build(t)
		signers := []addr.Address{anne, bob}

		actor.constructAndVerify(rt, 2, noUnlockDuration, startEpoch, signers...)

		// Anne proposes a tx
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, big.Zero(), builtin.MethodSend, nil, nil)

		// Anne is swapped
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.swapSigners(rt, anne, chuck)

		// Transaction is gone.
		actor.assertTransactions(rt)
		actor.checkState(rt)
	})
}

type thresholdTestCase struct {
	desc             string
	initialThreshold uint64
	setThreshold     uint64
	code             exitcode.ExitCode
}

func TestChangeThreshold(t *testing.T) {
	actor := msActorHarness{multisig.Actor{}, t}
	startEpoch := abi.ChainEpoch(0)

	multisigWalletAdd := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	bob := tutil.NewIDAddr(t, 102)
	chuck := tutil.NewIDAddr(t, 103)

	const noUnlockDuration = abi.ChainEpoch(0)
	var initialSigner = []addr.Address{anne, bob, chuck}

	testCases := []thresholdTestCase{
		{
			desc:             "happy path decrease threshold",
			initialThreshold: 2,
			setThreshold:     1,
			code:             exitcode.Ok,
		},
		{
			desc:             "happy path simple increase threshold",
			initialThreshold: 2,
			setThreshold:     3,
			code:             exitcode.Ok,
		},
		{
			desc:             "fail to set threshold to zero",
			initialThreshold: 2,
			setThreshold:     0,
			code:             exitcode.ErrIllegalArgument,
		},
		{
			desc:             "fail to set threshold above number of signers",
			initialThreshold: 2,
			setThreshold:     uint64(len(initialSigner) + 1),
			code:             exitcode.ErrIllegalArgument,
		},
	}

	builder := mock.NewBuilder(context.Background(), multisigWalletAdd).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)

			actor.constructAndVerify(rt, tc.initialThreshold, noUnlockDuration, startEpoch, initialSigner...)

			rt.SetCaller(multisigWalletAdd, builtin.MultisigActorCodeID)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.changeNumApprovalsThreshold(rt, tc.setThreshold)
				})
			} else {
				actor.changeNumApprovalsThreshold(rt, tc.setThreshold)
				var st multisig.State
				rt.GetState(&st)
				assert.Equal(t, tc.setThreshold, st.NumApprovalsThreshold)
				actor.checkState(rt)
			}
			rt.Verify()
		})
	}

	t.Run("transaction can be re-approved and executed after threshold lowered", func(t *testing.T) {
		fakeMethod := abi.MethodNum(42)
		numApprovals := uint64(2)

		var sendValue = abi.NewTokenAmount(10)
		receiver := tutil.NewIDAddr(t, 100)
		rt := builder.Build(t)
		signers := []addr.Address{anne, bob, chuck}

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, startEpoch, signers...)

		// anne proposes a transaction ID: 0
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.proposeOK(rt, chuck, sendValue, fakeMethod, nil, nil)

		// lower approver threshold. transaction is technically approved, but will not be executed yet.
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.changeNumApprovalsThreshold(rt, 1)

		// anne may re-approve causing transaction to be executed
		rt.ExpectSend(chuck, fakeMethod, nil, sendValue, nil, 0)
		rt.SetBalance(sendValue)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		actor.approveOK(rt, 0, nil, nil)
		actor.checkState(rt)
	})
}

func TestLockBalance(t *testing.T) {
	actor := msActorHarness{multisig.Actor{}, t}
	receiver := tutil.NewIDAddr(t, 100)
	anne := tutil.NewIDAddr(t, 101)
	bob := tutil.NewIDAddr(t, 102)

	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID).
		WithEpoch(0).
		WithHasher(blake2b.Sum256)

	t.Run("retroactive vesting", func(t *testing.T) {
		rt := builder.Build(t)

		// Create empty multisig
		rt.SetEpoch(100)
		actor.constructAndVerify(rt, 1, 0, 0, anne)

		// Some time later, initialize vesting
		rt.SetEpoch(200)
		vestStart := abi.ChainEpoch(0)
		lockAmount := abi.NewTokenAmount(100_000)
		vestDuration := abi.ChainEpoch(1000)
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.lockBalance(rt, vestStart, vestDuration, lockAmount)

		rt.SetEpoch(300)
		vested := abi.NewTokenAmount(30_000) // Since vestStart
		rt.SetCaller(anne, builtin.AccountActorCodeID)

		// Fail to spend balance the multisig doesn't have
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, bob, vested, builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Fail to spend more than the vested amount
		rt.SetBalance(lockAmount)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, bob, big.Add(vested, big.NewInt(1)), builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Can fully spend the vested amount
		rt.ExpectSend(bob, builtin.MethodSend, nil, vested, nil, exitcode.Ok)
		actor.proposeOK(rt, bob, vested, builtin.MethodSend, nil, nil)

		// Can't spend more
		rt.SetBalance(big.Sub(lockAmount, vested))
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, bob, abi.NewTokenAmount(1), builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Later, can spend the rest
		rt.SetEpoch(vestStart + vestDuration)
		rested := big.NewInt(70_000)
		rt.ExpectSend(bob, builtin.MethodSend, nil, rested, nil, exitcode.Ok)
		actor.proposeOK(rt, bob, rested, builtin.MethodSend, nil, nil)
		actor.checkState(rt)
	})

	t.Run("prospective vesting", func(t *testing.T) {
		rt := builder.Build(t)

		// Create empty multisig
		rt.SetEpoch(100)
		actor.constructAndVerify(rt, 1, 0, 0, anne)

		// Some time later, initialize vesting
		rt.SetEpoch(200)
		vestStart := abi.ChainEpoch(1000)
		lockAmount := abi.NewTokenAmount(100_000)
		vestDuration := abi.ChainEpoch(1000)
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.lockBalance(rt, vestStart, vestDuration, lockAmount)

		rt.SetEpoch(300)
		rt.SetCaller(anne, builtin.AccountActorCodeID)

		// Oversupply the wallet, allow spending the oversupply.
		rt.SetBalance(big.Add(lockAmount, abi.NewTokenAmount(1)))
		rt.ExpectSend(bob, builtin.MethodSend, nil, abi.NewTokenAmount(1), nil, exitcode.Ok)
		actor.proposeOK(rt, bob, abi.NewTokenAmount(1), builtin.MethodSend, nil, nil)

		// Fail to spend locked funds before vesting starts
		rt.SetBalance(lockAmount)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, bob, abi.NewTokenAmount(1), builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Can spend partially vested amount
		rt.SetEpoch(vestStart + 200)
		vested := abi.NewTokenAmount(20_000)
		rt.ExpectSend(bob, builtin.MethodSend, nil, vested, nil, exitcode.Ok)
		actor.proposeOK(rt, bob, vested, builtin.MethodSend, nil, nil)

		// Can't spend more
		rt.SetBalance(big.Sub(lockAmount, vested))
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			_ = actor.propose(rt, bob, abi.NewTokenAmount(1), builtin.MethodSend, nil, nil)
		})
		rt.Reset()

		// Later, can spend the rest
		rt.SetEpoch(vestStart + vestDuration)
		rested := big.NewInt(80_000)
		rt.ExpectSend(bob, builtin.MethodSend, nil, rested, nil, exitcode.Ok)
		actor.proposeOK(rt, bob, rested, builtin.MethodSend, nil, nil)
		actor.checkState(rt)
	})

	t.Run("can't alter vesting", func(t *testing.T) {
		rt := builder.Build(t)

		// Create empty multisig
		rt.SetEpoch(100)
		actor.constructAndVerify(rt, 1, 0, 0, anne)

		// Initialize vesting from zero
		vestStart := abi.ChainEpoch(0)
		lockAmount := abi.NewTokenAmount(100_000)
		vestDuration := abi.ChainEpoch(1000)
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)
		actor.lockBalance(rt, vestStart, vestDuration, lockAmount)

		// Can't change vest start
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.lockBalance(rt, vestStart-1, vestDuration, lockAmount)
		})
		rt.Reset()

		// Can't change lock duration
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.lockBalance(rt, vestStart, vestDuration-1, lockAmount)
		})
		rt.Reset()

		// Can't change locked amount
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.lockBalance(rt, vestStart, vestDuration, big.Sub(lockAmount, big.NewInt(1)))
		})
		rt.Reset()
	})

	t.Run("can't alter vesting from construction", func(t *testing.T) {
		rt := builder.Build(t)

		// Create empty multisig with vesting
		startEpoch := abi.ChainEpoch(100)
		unlockDuration := abi.ChainEpoch(1000)
		actor.constructAndVerify(rt, 1, unlockDuration, startEpoch, anne)

		// Can't change vest start
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.lockBalance(rt, startEpoch-1, abi.ChainEpoch(unlockDuration), big.Zero())
		})
		rt.Reset()
	})
	
	t.Run("checks preconditions", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 1, 0, 0, anne)
		vestStart := abi.ChainEpoch(0)
		lockAmount := abi.NewTokenAmount(100_000)
		vestDuration := abi.ChainEpoch(1000)
		rt.SetCaller(receiver, builtin.MultisigActorCodeID)

		// Disallow negative duration (though negative start epoch is allowed).
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.lockBalance(rt, vestStart, abi.ChainEpoch(-1), lockAmount)
		})

		// After version 7, disallow negative amount.
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.lockBalance(rt, vestStart, vestDuration, abi.NewTokenAmount(-1))
		})

		// Before version 7, allow negative amount.
		rt.SetNetworkVersion(network.Version6)
		actor.lockBalance(rt, vestStart, vestDuration, abi.NewTokenAmount(-1))
	})
}

//
// Helper methods for calling multisig actor methods
//

type msActorHarness struct {
	a multisig.Actor
	t testing.TB
}

func (h *msActorHarness) constructAndVerify(rt *mock.Runtime, numApprovalsThresh uint64, unlockDuration abi.ChainEpoch, startEpoch abi.ChainEpoch, signers ...addr.Address) {
	constructParams := multisig.ConstructorParams{
		Signers:               signers,
		NumApprovalsThreshold: numApprovalsThresh,
		UnlockDuration:        unlockDuration,
		StartEpoch:            startEpoch,
	}

	rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
	ret := rt.Call(h.a.Constructor, &constructParams)
	assert.Nil(h.t, ret)
	rt.Verify()
}

func (h *msActorHarness) propose(rt *mock.Runtime, to addr.Address, value abi.TokenAmount, method abi.MethodNum, params []byte, out cbor.Unmarshaler) exitcode.ExitCode {
	rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
	proposeParams := &multisig.ProposeParams{
		To:     to,
		Value:  value,
		Method: method,
		Params: params,
	}
	ret := rt.Call(h.a.Propose, proposeParams)
	rt.Verify()

	proposeReturn, ok := ret.(*multisig.ProposeReturn)
	if !ok {
		h.t.Fatalf("unexpected type returned from call to Propose")
	}
	// if the transaction was applied and a return value is expected deserialize it to the out parameter
	if proposeReturn.Applied {
		if out != nil {
			require.NoError(h.t, out.UnmarshalCBOR(bytes.NewReader(proposeReturn.Ret)))
		}
	}
	return proposeReturn.Code
}

// returns the proposal hash
func (h *msActorHarness) proposeOK(rt *mock.Runtime, to addr.Address, value abi.TokenAmount, method abi.MethodNum, params []byte, out cbor.Unmarshaler) []byte {
	code := h.propose(rt, to, value, method, params, out)
	if code != exitcode.Ok {
		h.t.Fatalf("unexpected exitcode %d from propose", code)
	}

	proposalHashData, err := multisig.ComputeProposalHash(&multisig.Transaction{
		To:       to,
		Value:    value,
		Method:   method,
		Params:   params,
		Approved: []addr.Address{rt.Caller()},
	}, blake2b.Sum256)
	require.NoError(h.t, err)

	return proposalHashData
}

func (h *msActorHarness) approve(rt *mock.Runtime, txnID int64, proposalParams []byte, out cbor.Unmarshaler) exitcode.ExitCode {
	rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
	ret := rt.Call(h.a.Approve, &multisig.TxnIDParams{
		ID:           multisig.TxnID(txnID),
		ProposalHash: proposalParams,
	})
	rt.Verify()
	approveReturn, ok := ret.(*multisig.ApproveReturn)
	if !ok {
		h.t.Fatalf("unexpected type returned from call to Approve")
	}
	// if the transaction was applied and a return value is expected deserialize it to the out parameter
	if approveReturn.Applied {
		if out != nil {
			require.NoError(h.t, out.UnmarshalCBOR(bytes.NewReader(approveReturn.Ret)))
		}
	}
	return approveReturn.Code
}

func (h *msActorHarness) approveOK(rt *mock.Runtime, txnID int64, proposalParams []byte, out cbor.Unmarshaler) {
	code := h.approve(rt, txnID, proposalParams, out)
	if code != exitcode.Ok {
		h.t.Fatalf("unexpected exitcode %d from approve", code)
	}
}

func (h *msActorHarness) cancel(rt *mock.Runtime, txnID int64, proposalParams []byte) {
	rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
	rt.Call(h.a.Cancel, &multisig.TxnIDParams{
		ID:           multisig.TxnID(txnID),
		ProposalHash: proposalParams,
	})
	rt.Verify()
}

func (h *msActorHarness) addSigner(rt *mock.Runtime, signer addr.Address, increase bool) {
	rt.ExpectValidateCallerAddr(rt.Receiver())
	rt.Call(h.a.AddSigner, &multisig.AddSignerParams{
		Signer:   signer,
		Increase: increase,
	})
	rt.Verify()
}

func (h *msActorHarness) removeSigner(rt *mock.Runtime, signer addr.Address, decrease bool) {
	rt.ExpectValidateCallerAddr(rt.Receiver())
	rt.Call(h.a.RemoveSigner, &multisig.RemoveSignerParams{
		Signer:   signer,
		Decrease: decrease,
	})
	rt.Verify()
}

func (h *msActorHarness) swapSigners(rt *mock.Runtime, oldSigner, newSigner addr.Address) {
	rt.ExpectValidateCallerAddr(rt.Receiver())
	rt.Call(h.a.SwapSigner, &multisig.SwapSignerParams{
		From: oldSigner,
		To:   newSigner,
	})
	rt.Verify()
}

func (h *msActorHarness) changeNumApprovalsThreshold(rt *mock.Runtime, newThreshold uint64) {
	rt.ExpectValidateCallerAddr(rt.Receiver())
	rt.Call(h.a.ChangeNumApprovalsThreshold, &multisig.ChangeNumApprovalsThresholdParams{
		NewThreshold: newThreshold,
	})
	rt.Verify()
}

func (h *msActorHarness) lockBalance(rt *mock.Runtime, start, duration abi.ChainEpoch, amount abi.TokenAmount) {
	rt.ExpectValidateCallerAddr(rt.Receiver())
	rt.Call(h.a.LockBalance, &multisig.LockBalanceParams{
		StartEpoch:     start,
		UnlockDuration: duration,
		Amount:         amount,
	})
	rt.Verify()
}

func (h *msActorHarness) assertTransactions(rt *mock.Runtime, expected ...multisig.Transaction) {
	var st multisig.State
	rt.GetState(&st)

	txns, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
	assert.NoError(h.t, err)
	keys, err := txns.CollectKeys()
	assert.NoError(h.t, err)

	require.Equal(h.t, len(expected), len(keys))
	for i, k := range keys {
		var actual multisig.Transaction
		found, err_ := txns.Get(asKey(k), &actual)
		require.NoError(h.t, err_)
		assert.True(h.t, found)
		assert.Equal(h.t, expected[i], actual)
	}
}

func (h *msActorHarness) checkState(rt *mock.Runtime) {
	var st multisig.State
	rt.GetState(&st)
	assertStateInvariants(h.t, rt, &st)
}

func assertStateInvariants(t testing.TB, rt *mock.Runtime, st *multisig.State) {
	_, msgs := multisig.CheckStateInvariants(st, rt.AdtStore())
	assert.True(t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))
}

func makeProposalHash(t *testing.T, txn *multisig.Transaction) []byte {
	proposalHashData, err := multisig.ComputeProposalHash(txn, blake2b.Sum256)
	require.NoError(t, err)
	return proposalHashData
}

type key string

func (s key) Key() string {
	return string(s)
}

func asKey(in string) abi.Keyer {
	return key(in)
}
