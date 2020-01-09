package vm_test

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/go-state-types/rt"
	vm2 "github.com/filecoin-project/specs-actors/v2/support/vm"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	"github.com/filecoin-project/specs-actors/v4/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/v4/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v4/actors/runtime"
	"github.com/filecoin-project/specs-actors/v4/actors/states"
	"github.com/filecoin-project/specs-actors/v4/actors/util/adt"
)

// VM is a simplified message execution framework for the purposes of testing inter-actor communication.
// The VM maintains actor state and can be used to simulate message validation for a single block or tipset.
// The VM does not track gas charges, provide working syscalls, validate message nonces and many other things
// that a compliant VM needs to do.
type VM struct {
	ctx   context.Context
	store adt.Store

	currentEpoch   abi.ChainEpoch
	networkVersion network.Version

	ActorImpls  ActorImplLookup
	stateRoot   cid.Cid  // The last committed root.
	actors      *adt.Map // The current (not necessarily committed) root node.
	actorsDirty bool

	emptyObject  cid.Cid
	callSequence uint64

	logs            []string
	invocationStack []*Invocation
	invocations     []*Invocation

	statsSource   StatsSource
	statsByMethod StatsByCall

	circSupply abi.TokenAmount
}

// VM types

// type ActorImplLookup map[cid.Cid]runtime.VMActor
type ActorImplLookup vm2.ActorImplLookup

type InternalMessage struct {
	from   address.Address
	to     address.Address
	value  abi.TokenAmount
	method abi.MethodNum
	params interface{}
}

type Invocation struct {
	Msg            *InternalMessage
	Exitcode       exitcode.ExitCode
	Ret            cbor.Marshaler
	SubInvocations []*Invocation
}

// NewVM creates a new runtime for executing messages.
func NewVM(ctx context.Context, actorImpls ActorImplLookup, store adt.Store) *VM {
	actors, err := adt.MakeEmptyMap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		panic(err)
	}
	actorRoot, err := actors.Root()
	if err != nil {
		panic(err)
	}

	emptyObject, err := store.Put(context.TODO(), []struct{}{})
	if err != nil {
		panic(err)
	}

	return &VM{
		ctx:            ctx,
		ActorImpls:     actorImpls,
		store:          store,
		actors:         actors,
		stateRoot:      actorRoot,
		actorsDirty:    false,
		emptyObject:    emptyObject,
		networkVersion: network.VersionMax,
		statsByMethod:  make(StatsByCall),
		circSupply:     big.Mul(big.NewInt(1e9), big.NewInt(1e18)),
	}
}

// NewVM creates a new runtime for executing messages.
func NewVMAtEpoch(ctx context.Context, actorImpls ActorImplLookup, store adt.Store, stateRoot cid.Cid, epoch abi.ChainEpoch) (*VM, error) {
	actors, err := adt.AsMap(store, stateRoot, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	emptyObject, err := store.Put(context.TODO(), []struct{}{})
	if err != nil {
		panic(err)
	}

	return &VM{
		ctx:            ctx,
		ActorImpls:     actorImpls,
		currentEpoch:   epoch,
		store:          store,
		actors:         actors,
		stateRoot:      stateRoot,
		actorsDirty:    false,
		emptyObject:    emptyObject,
		networkVersion: network.VersionMax,
		statsByMethod:  make(StatsByCall),
		circSupply:     big.Mul(big.NewInt(1e9), big.NewInt(1e18)),
	}, nil
}

func (vm *VM) WithEpoch(epoch abi.ChainEpoch) (*VM, error) {
	_, err := vm.checkpoint()
	if err != nil {
		return nil, err
	}

	actors, err := adt.AsMap(vm.store, vm.stateRoot, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	return &VM{
		ctx:            vm.ctx,
		ActorImpls:     vm.ActorImpls,
		store:          vm.store,
		actors:         actors,
		stateRoot:      vm.stateRoot,
		actorsDirty:    false,
		emptyObject:    vm.emptyObject,
		currentEpoch:   epoch,
		networkVersion: vm.networkVersion,
		statsSource:    vm.statsSource,
		statsByMethod:  make(StatsByCall),
		circSupply:     vm.circSupply,
	}, nil
}

func (vm *VM) WithNetworkVersion(nv network.Version) (*VM, error) {
	_, err := vm.checkpoint()
	if err != nil {
		return nil, err
	}

	actors, err := adt.AsMap(vm.store, vm.stateRoot, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	return &VM{
		ctx:            vm.ctx,
		ActorImpls:     vm.ActorImpls,
		store:          vm.store,
		actors:         actors,
		stateRoot:      vm.stateRoot,
		actorsDirty:    false,
		emptyObject:    vm.emptyObject,
		currentEpoch:   vm.currentEpoch,
		networkVersion: nv,
		statsSource:    vm.statsSource,
		statsByMethod:  make(StatsByCall),
		circSupply:     vm.circSupply,
	}, nil
}

func (vm *VM) rollback(root cid.Cid) error {
	var err error
	vm.actors, err = adt.AsMap(vm.store, root, builtin.DefaultHamtBitwidth)
	if err != nil {
		return errors.Wrapf(err, "failed to load node for %s", root)
	}

	// reset the root node
	vm.stateRoot = root
	vm.actorsDirty = false
	return nil
}

func (vm *VM) GetActor(a address.Address) (*states.Actor, bool, error) {
	na, found := vm.NormalizeAddress(a)
	if !found {
		return nil, false, nil
	}
	var act states.Actor
	found, err := vm.actors.Get(abi.AddrKey(na), &act)
	return &act, found, err
}

// SetActor sets the the actor to the given value whether it previously existed or not.
//
// This method will not check if the actor previously existed, it will blindly overwrite it.
func (vm *VM) setActor(_ context.Context, key address.Address, a *states.Actor) error {
	if err := vm.actors.Put(abi.AddrKey(key), a); err != nil {
		return errors.Wrap(err, "setting actor in state tree failed")
	}
	vm.actorsDirty = true
	return nil
}

// SetActorState stores the state and updates the addressed actor
func (vm *VM) SetActorState(ctx context.Context, key address.Address, state cbor.Marshaler) error {
	stateCid, err := vm.store.Put(ctx, state)
	if err != nil {
		return err
	}
	a, found, err := vm.GetActor(key)
	if err != nil {
		return err
	}
	if !found {
		return errors.Errorf("could not find actor %s to set state", key)
	}
	a.Head = stateCid
	return vm.setActor(ctx, key, a)
}

// deleteActor remove the actor from the storage.
//
// This method will NOT return an error if the actor was not found.
// This behaviour is based on a principle that some store implementations might not be able to determine
// whether something exists before deleting it.
func (vm *VM) deleteActor(_ context.Context, key address.Address) error {
	found, err := vm.actors.TryDelete(abi.AddrKey(key))
	vm.actorsDirty = found
	return err
}

func (vm *VM) checkpoint() (cid.Cid, error) {
	// commit the vm state
	root, err := vm.actors.Root()
	if err != nil {
		return cid.Undef, err
	}
	vm.stateRoot = root
	vm.actorsDirty = false

	return root, nil
}

func (vm *VM) NormalizeAddress(addr address.Address) (address.Address, bool) {
	// short-circuit if the address is already an ID address
	if addr.Protocol() == address.ID {
		return addr, true
	}

	// resolve the target address via the InitActor, and attempt to load state.
	initActorEntry, found, err := vm.GetActor(builtin.InitActorAddr)
	if err != nil {
		panic(errors.Wrapf(err, "failed to load init actor"))
	}
	if !found {
		panic(errors.Wrapf(err, "no init actor"))
	}

	// get a view into the actor state
	var state init_.State
	if err := vm.store.Get(vm.ctx, initActorEntry.Head, &state); err != nil {
		panic(err)
	}

	idAddr, found, err := state.ResolveAddress(vm.store, addr)
	if err != nil {
		panic(err)
	}
	return idAddr, found
}

// ApplyMessage applies the message to the current state.
func (vm *VM) ApplyMessage(from, to address.Address, value abi.TokenAmount, method abi.MethodNum, params interface{}) (cbor.Marshaler, exitcode.ExitCode) {
	// This method does not actually execute the message itself,
	// but rather deals with the pre/post processing of a message.
	// (see: `invocationContext.invoke()` for the dispatch and execution)

	// load actor from global state
	fromID, ok := vm.NormalizeAddress(from)
	if !ok {
		return nil, exitcode.SysErrSenderInvalid
	}

	fromActor, found, err := vm.GetActor(fromID)
	if err != nil {
		panic(err)
	}
	if !found {
		// Execution error; sender does not exist at time of message execution.
		return nil, exitcode.SysErrSenderInvalid
	}

	// checkpoint state
	// Even if the message fails, the following accumulated changes will be applied:
	// - CallSeqNumber increment
	// - sender balance withheld
	priorRoot, err := vm.checkpoint()
	if err != nil {
		panic(err)
	}

	// send
	// 1. build internal message
	// 2. build invocation context
	// 3. process the msg

	topLevel := topLevelContext{
		originatorStableAddress: from,
		// this should be nonce, but we only care that it creates a unique stable address
		originatorCallSeq:    vm.callSequence,
		newActorAddressCount: 0,
		statsSource:          vm.statsSource,
		circSupply:           vm.circSupply,
	}
	vm.callSequence++

	// build internal msg
	imsg := InternalMessage{
		from:   fromID,
		to:     to,
		value:  value,
		method: method,
		params: params,
	}

	// build invocation context
	ctx := newInvocationContext(vm, &topLevel, imsg, fromActor, vm.emptyObject)

	// 3. invoke
	ret, exitCode := ctx.invoke()

	// record stats
	vm.statsByMethod.MergeStats(ctx.toActor.Code, imsg.method, ctx.stats)

	// Roll back all state if the receipt's exit code is not ok.
	// This is required in addition to rollback within the invocation context since top level messages can fail for
	// more reasons than internal ones. Invocation context still needs its own rollback so actors can recover and
	// proceed from a nested call failure.
	if exitCode != exitcode.Ok {
		if err := vm.rollback(priorRoot); err != nil {
			panic(err)
		}
	} else {
		// persist changes from final invocation if call is ok
		if _, err := vm.checkpoint(); err != nil {
			panic(err)
		}

	}

	return ret.inner, exitCode
}

func (vm *VM) StateRoot() cid.Cid {
	return vm.stateRoot
}

func (vm *VM) GetState(addr address.Address, out cbor.Unmarshaler) error {
	act, found, err := vm.GetActor(addr)
	if err != nil {
		return err
	}
	if !found {
		return errors.Errorf("actor %v not found", addr)
	}
	return vm.store.Get(vm.ctx, act.Head, out)
}

func (vm *VM) GetStateTree() (*states.Tree, error) {
	root, err := vm.checkpoint()
	if err != nil {
		return nil, err
	}

	return states.LoadTree(vm.store, root)
}

func (vm *VM) GetTotalActorBalance() (abi.TokenAmount, error) {
	tree, err := vm.GetStateTree()
	if err != nil {
		return big.Zero(), err
	}
	total := big.Zero()
	err = tree.ForEach(func(_ address.Address, actor *states.Actor) error {
		total = big.Add(total, actor.Balance)
		return nil
	})
	if err != nil {
		return big.Zero(), err
	}
	return total, nil
}

func (vm *VM) Store() adt.Store {
	return vm.store
}

// Get the chain epoch for this vm
func (vm *VM) GetEpoch() abi.ChainEpoch {
	return vm.currentEpoch
}

// Get call stats
func (vm *VM) GetCallStats() map[MethodKey]*CallStats {
	return vm.statsByMethod
}

// Set the FIL circulating supply passed to actors through runtime
func (vm *VM) SetCirculatingSupply(supply abi.TokenAmount) {
	vm.circSupply = supply
}

// Set the FIL circulating supply passed to actors through runtime
func (vm *VM) GetCirculatingSupply() abi.TokenAmount {
	return vm.circSupply
}

func (vm *VM) GetActorImpls() map[cid.Cid]rt.VMActor {
	return vm.ActorImpls
}

// transfer debits money from one account and credits it to another.
// avoid calling this method with a zero amount else it will perform unnecessary actor loading.
//
// WARNING: this method will panic if the the amount is negative, accounts dont exist, or have inssuficient funds.
//
// Note: this is not idiomatic, it follows the Spec expectations for this method.
func (vm *VM) transfer(debitFrom address.Address, creditTo address.Address, amount abi.TokenAmount) (*states.Actor, *states.Actor) {
	// allow only for positive amounts
	if amount.LessThan(abi.NewTokenAmount(0)) {
		panic("unreachable: negative funds transfer not allowed")
	}

	ctx := context.Background()

	// retrieve debit account
	fromActor, found, err := vm.GetActor(debitFrom)
	if err != nil {
		panic(err)
	}
	if !found {
		panic(fmt.Errorf("unreachable: debit account not found. %s", err))
	}

	// check that account has enough balance for transfer
	if fromActor.Balance.LessThan(amount) {
		panic("unreachable: insufficient balance on debit account")
	}

	// debit funds
	fromActor.Balance = big.Sub(fromActor.Balance, amount)
	if err := vm.setActor(ctx, debitFrom, fromActor); err != nil {
		panic(err)
	}

	// retrieve credit account
	toActor, found, err := vm.GetActor(creditTo)
	if err != nil {
		panic(err)
	}
	if !found {
		panic(fmt.Errorf("unreachable: credit account not found. %s", err))
	}

	// credit funds
	toActor.Balance = big.Add(toActor.Balance, amount)
	if err := vm.setActor(ctx, creditTo, toActor); err != nil {
		panic(err)
	}
	return toActor, fromActor
}

func (vm *VM) getActorImpl(code cid.Cid) runtime.VMActor {
	actorImpl, ok := vm.ActorImpls[code]
	if !ok {
		vm.Abortf(exitcode.SysErrInvalidReceiver, "actor implementation not found for Exitcode %v", code)
	}
	return actorImpl
}

//
// stats
//

func (vm *VM) SetStatsSource(s StatsSource) {
	vm.statsSource = s
}

func (vm *VM) GetStatsSource() StatsSource {
	return vm.statsSource
}

func (vm *VM) StoreReads() uint64 {
	if vm.statsSource != nil {
		return vm.statsSource.ReadCount()
	}
	return 0
}

func (vm *VM) StoreWrites() uint64 {
	if vm.statsSource != nil {
		return vm.statsSource.WriteCount()
	}
	return 0
}

func (vm *VM) StoreReadBytes() uint64 {
	if vm.statsSource != nil {
		return vm.statsSource.ReadSize()
	}
	return 0
}

func (vm *VM) StoreWriteBytes() uint64 {
	if vm.statsSource != nil {
		return vm.statsSource.WriteSize()
	}
	return 0
}

//
// invocation tracking
//

func (vm *VM) startInvocation(msg *InternalMessage) {
	invocation := Invocation{Msg: msg}
	if len(vm.invocationStack) > 0 {
		parent := vm.invocationStack[len(vm.invocationStack)-1]
		parent.SubInvocations = append(parent.SubInvocations, &invocation)
	} else {
		vm.invocations = append(vm.invocations, &invocation)
	}
	vm.invocationStack = append(vm.invocationStack, &invocation)
}

func (vm *VM) endInvocation(code exitcode.ExitCode, ret cbor.Marshaler) {
	curIndex := len(vm.invocationStack) - 1
	current := vm.invocationStack[curIndex]
	current.Exitcode = code
	current.Ret = ret

	vm.invocationStack = vm.invocationStack[:curIndex]
}

func (vm *VM) Invocations() []*Invocation {
	return vm.invocations
}

func (vm *VM) LastInvocation() *Invocation {
	return vm.invocations[len(vm.invocations)-1]
}

//
// implement runtime.Runtime for VM
//

func (vm *VM) Log(_ rt.LogLevel, msg string, args ...interface{}) {
	vm.logs = append(vm.logs, fmt.Sprintf(msg, args...))
}

func (vm *VM) GetLogs() []string {
	return vm.logs
}

type abort struct {
	code exitcode.ExitCode
	msg  string
}

func (vm *VM) Abortf(errExitCode exitcode.ExitCode, msg string, args ...interface{}) {
	panic(abort{errExitCode, fmt.Sprintf(msg, args...)})
}

//
// implement runtime.MessageInfo for InternalMessage
//

var _ runtime.Message = (*InternalMessage)(nil)

// ValueReceived implements runtime.MessageInfo.
func (msg InternalMessage) ValueReceived() abi.TokenAmount {
	return msg.value
}

// Caller implements runtime.MessageInfo.
func (msg InternalMessage) Caller() address.Address {
	return msg.from
}

// Receiver implements runtime.MessageInfo.
func (msg InternalMessage) Receiver() address.Address {
	return msg.to
}
