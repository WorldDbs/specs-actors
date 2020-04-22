package test

import (
	"bytes"
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
)

func TestV5MultisigDeleteSigner1Of2(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	v, err := v.WithNetworkVersion(network.Version5)
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	multisigParams := multisig.ConstructorParams{
		Signers:               addrs,
		NumApprovalsThreshold: 1,
	}

	paramBuf := new(bytes.Buffer)
	err = multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	removeParams := multisig.RemoveSignerParams{
		Signer:   addrs[0],
		Decrease: false,
	}

	paramBuf = new(bytes.Buffer)
	err = removeParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeRemoveSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.RemoveSigner,
		Params: paramBuf.Bytes(),
	}
	// address 0 fails when trying to execute the transaction removing address 0
	_, code := v.ApplyMessage(addrs[0], multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeRemoveSignerParams)
	assert.Equal(t, exitcode.ErrIllegalState, code)
	// address 1 succeeds when trying to execute the transaction removing address 0
	vm.ApplyOk(t, v, addrs[1], multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeRemoveSignerParams)
}

func TestV5MultisigDeleteSelf2Of3RemovedIsProposer(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	v, err := v.WithNetworkVersion(network.Version5)
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 3, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	multisigParams := multisig.ConstructorParams{
		Signers:               addrs,
		NumApprovalsThreshold: 2,
	}

	paramBuf := new(bytes.Buffer)
	err = multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	removeParams := multisig.RemoveSignerParams{
		Signer:   addrs[0],
		Decrease: false,
	}

	paramBuf = new(bytes.Buffer)
	err = removeParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeRemoveSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.RemoveSigner,
		Params: paramBuf.Bytes(),
	}

	// first proposal goes ok and should have txnid = 0
	vm.ApplyOk(t, v, addrs[0], multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeRemoveSignerParams)
	// approval goes through
	approveRemoveSignerParams := multisig.TxnIDParams{ID: multisig.TxnID(0)}
	vm.ApplyOk(t, v, addrs[1], multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveRemoveSignerParams)

	// txnid not found when third approval gets processed indicating that the transaction has gone through successfully
	_, code := v.ApplyMessage(addrs[2], multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveRemoveSignerParams)
	assert.Equal(t, exitcode.ErrNotFound, code)

}

func TestV5MultisigDeleteSelf2Of3RemovedIsApprover(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	v, err := v.WithNetworkVersion(network.Version5)
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 3, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	multisigParams := multisig.ConstructorParams{
		Signers:               addrs,
		NumApprovalsThreshold: 2,
	}

	paramBuf := new(bytes.Buffer)
	err = multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	removeParams := multisig.RemoveSignerParams{
		Signer:   addrs[0],
		Decrease: false,
	}

	paramBuf = new(bytes.Buffer)
	err = removeParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeRemoveSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.RemoveSigner,
		Params: paramBuf.Bytes(),
	}

	// first proposal goes ok and should have txnid = 0
	vm.ApplyOk(t, v, addrs[1], multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeRemoveSignerParams)
	// approval goes through
	approveRemoveSignerParams := multisig.TxnIDParams{ID: multisig.TxnID(0)}
	vm.ApplyOk(t, v, addrs[0], multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveRemoveSignerParams)

	// txnid not found when third approval gets processed indicating that the transaction has gone through successfully
	_, code := v.ApplyMessage(addrs[2], multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveRemoveSignerParams)
	assert.Equal(t, exitcode.ErrNotFound, code)

}

func TestV5MultisigDeleteSelf2Of2(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	v, err := v.WithNetworkVersion(network.Version5)
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	multisigParams := multisig.ConstructorParams{
		Signers:               addrs,
		NumApprovalsThreshold: 2,
	}

	paramBuf := new(bytes.Buffer)
	err = multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	removeParams := multisig.RemoveSignerParams{
		Signer:   addrs[0],
		Decrease: true,
	}

	paramBuf = new(bytes.Buffer)
	err = removeParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	// first proposal goes ok and should have txnid = 0
	proposeRemoveSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.RemoveSigner,
		Params: paramBuf.Bytes(),
	}
	vm.ApplyOk(t, v, addrs[0], multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeRemoveSignerParams)
	// approval goes through
	approveRemoveSignerParams := multisig.TxnIDParams{ID: multisig.TxnID(0)}
	vm.ApplyOk(t, v, addrs[1], multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveRemoveSignerParams)

	// txnid not found when another approval gets processed indicating that the transaction has gone through successfully
	_, code := v.ApplyMessage(addrs[1], multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveRemoveSignerParams)
	assert.Equal(t, exitcode.ErrNotFound, code)
}

func TestV5MultisigSwapsSelf1Of2(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	v, err := v.WithNetworkVersion(network.Version5)
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 3, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)
	alice := addrs[0]
	bob := addrs[1]
	chuck := addrs[2]

	multisigParams := multisig.ConstructorParams{
		Signers:               []address.Address{alice, bob},
		NumApprovalsThreshold: 1,
	}

	paramBuf := new(bytes.Buffer)
	err = multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	swapParams := multisig.SwapSignerParams{
		From: alice,
		To:   chuck,
	}

	paramBuf = new(bytes.Buffer)
	err = swapParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeSwapSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.SwapSigner,
		Params: paramBuf.Bytes(),
	}

	// alice fails when trying to execute the transaction swapping alice for chuck
	_, code := v.ApplyMessage(alice, multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeSwapSignerParams)
	assert.Equal(t, exitcode.ErrIllegalState, code)
	// bob succeeds when trying to execute the transaction swapping alice for chuck
	vm.ApplyOk(t, v, bob, multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeSwapSignerParams)

}

func TestV5MultisigSwapsSelf2Of3(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	v, err := v.WithNetworkVersion(network.Version5)
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)
	alice := addrs[0]
	bob := addrs[1]
	chuck := addrs[2]
	dinesh := addrs[3]

	multisigParams := multisig.ConstructorParams{
		Signers:               []address.Address{alice, bob, chuck},
		NumApprovalsThreshold: 2,
	}

	paramBuf := new(bytes.Buffer)
	err = multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	// Case 1 swapped out is proposer (swap alice for dinesh, alice is removed)
	swapParams := multisig.SwapSignerParams{
		From: alice,
		To:   dinesh,
	}

	paramBuf = new(bytes.Buffer)
	err = swapParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeSwapSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.SwapSigner,
		Params: paramBuf.Bytes(),
	}

	// proposal from swapped address goes ok and should have txnid = 0
	vm.ApplyOk(t, v, alice, multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeSwapSignerParams)

	// approval goes through
	approveSwapSignerParams := multisig.TxnIDParams{ID: multisig.TxnID(0)}
	vm.ApplyOk(t, v, bob, multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveSwapSignerParams)

	// Case 2 swapped out is approver (swap dinesh for alice, dinesh is removed)
	swapParams2 := multisig.SwapSignerParams{
		From: dinesh,
		To:   alice,
	}

	paramBuf2 := new(bytes.Buffer)
	err = swapParams2.MarshalCBOR(paramBuf2)
	require.NoError(t, err)

	proposeSwapSignerParams2 := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.SwapSigner,
		Params: paramBuf2.Bytes(),
	}

	// proposal from regular address goes ok txnid = 1
	vm.ApplyOk(t, v, bob, multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeSwapSignerParams2)

	// approve from swapped address goes ok
	approveSwapSignerParams2 := multisig.TxnIDParams{ID: multisig.TxnID(1)}
	vm.ApplyOk(t, v, dinesh, multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveSwapSignerParams2)

}

func TestMultisigDeleteSigner1Of2(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	multisigParams := multisig.ConstructorParams{
		Signers:               addrs,
		NumApprovalsThreshold: 1,
	}

	paramBuf := new(bytes.Buffer)
	err := multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	removeParams := multisig.RemoveSignerParams{
		Signer:   addrs[0],
		Decrease: false,
	}

	paramBuf = new(bytes.Buffer)
	err = removeParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeRemoveSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.RemoveSigner,
		Params: paramBuf.Bytes(),
	}
	// address 0 succeeds when trying to execute the transaction removing address 0
	vm.ApplyOk(t, v, addrs[0], multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeRemoveSignerParams)
}

func TestMultisigSwapsSelf1Of2(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 3, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)
	alice := addrs[0]
	bob := addrs[1]
	chuck := addrs[2]

	multisigParams := multisig.ConstructorParams{
		Signers:               []address.Address{alice, bob},
		NumApprovalsThreshold: 1,
	}

	paramBuf := new(bytes.Buffer)
	err := multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	swapParams := multisig.SwapSignerParams{
		From: alice,
		To:   chuck,
	}

	paramBuf = new(bytes.Buffer)
	err = swapParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeSwapSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.SwapSigner,
		Params: paramBuf.Bytes(),
	}

	// alice succeeds when trying to execute the transaction swapping alice for chuck
	vm.ApplyOk(t, v, alice, multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeSwapSignerParams)

}
