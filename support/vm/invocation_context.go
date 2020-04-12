package vm_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime/debug"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/go-state-types/rt"
	"github.com/ipfs/go-cid"
	"github.com/minio/blake2b-simd"
	"github.com/pkg/errors"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	"github.com/filecoin-project/specs-actors/v2/support/testing"
)

var EmptyObjectCid cid.Cid

// Context for an individual message invocation, including inter-actor sends.
type invocationContext struct {
	rt               *VM
	topLevel         *topLevelContext
	msg              InternalMessage // The message being processed
	fromActor        *states.Actor   // The immediate calling actor
	toActor          *states.Actor   // The actor to which message is addressed
	emptyObject      cid.Cid
	allowSideEffects bool
	callerValidated  bool
	// Maps (references to) loaded state objs to their expected cid.
	// Used for detecting modifications to state outside of transactions.
	stateUsedObjs map[cbor.Marshaler]cid.Cid
	stats         *CallStats
}

// Context for a top-level invocation sequence
type topLevelContext struct {
	originatorStableAddress address.Address // Stable (public key) address of the top-level message sender.
	originatorCallSeq       uint64          // Call sequence number of the top-level message.
	newActorAddressCount    uint64          // Count of calls to NewActorAddress (mutable).
	statsSource             StatsSource     // optional source of external statistics that can be used to profile calls
	circSupply              abi.TokenAmount // default or externally specified circulating FIL supply
}

func newInvocationContext(rt *VM, topLevel *topLevelContext, msg InternalMessage, fromActor *states.Actor, emptyObject cid.Cid) invocationContext {
	// Note: the toActor and stateHandle are loaded during the `invoke()`
	return invocationContext{
		rt:               rt,
		topLevel:         topLevel,
		msg:              msg,
		fromActor:        fromActor,
		toActor:          nil,
		emptyObject:      emptyObject,
		allowSideEffects: true,
		callerValidated:  false,
		stateUsedObjs:    map[cbor.Marshaler]cid.Cid{},
		stats:            NewCallStats(topLevel.statsSource),
	}
}

var _ runtime.StateHandle = (*invocationContext)(nil)

func (ic *invocationContext) loadState(obj cbor.Unmarshaler) cid.Cid {
	// The actor must be loaded from store every time since the state may have changed via a different state handle
	// (e.g. in a recursive call).
	actr := ic.loadActor()
	c := actr.Head
	if !c.Defined() {
		ic.Abortf(exitcode.SysErrorIllegalActor, "failed to load undefined state, must construct first")
	}
	err := ic.rt.store.Get(ic.rt.ctx, c, obj)
	if err != nil {
		panic(errors.Wrapf(err, "failed to load state for actor %s, CID %s", ic.msg.to, c))
	}
	return c
}

func (ic *invocationContext) loadActor() *states.Actor {
	actr, found, err := ic.rt.GetActor(ic.msg.to)
	if err != nil {
		panic(err)
	}
	if !found {
		panic(fmt.Errorf("failed to find actor %s for state", ic.msg.to))
	}
	return actr
}

func (ic *invocationContext) storeActor(actr *states.Actor) {
	err := ic.rt.setActor(ic.rt.ctx, ic.msg.to, actr)
	if err != nil {
		panic(err)
	}
}

/////////////////////////////////////////////
//          Runtime methods
/////////////////////////////////////////////

var _ runtime.Runtime = (*invocationContext)(nil)

// Store implements runtime.Runtime.
func (ic *invocationContext) StoreGet(c cid.Cid, o cbor.Unmarshaler) bool {
	sw := &storeWrapper{s: ic.rt.store, rt: ic.rt}
	return sw.StoreGet(c, o)
}

func (ic *invocationContext) StorePut(x cbor.Marshaler) cid.Cid {
	sw := &storeWrapper{s: ic.rt.store, rt: ic.rt}
	return sw.StorePut(x)
}

// These methods implement
// ValueReceived implements runtime.Message
func (ic *invocationContext) ValueReceived() abi.TokenAmount {
	return ic.msg.ValueReceived()
}

// Caller implements runtime.Message
func (ic *invocationContext) Caller() address.Address {
	return ic.msg.Caller()
}

// Receiver implements runtime.Message
func (ic *invocationContext) Receiver() address.Address {
	return ic.msg.Receiver()
}

func (ic *invocationContext) StateCreate(obj cbor.Marshaler) {
	actr := ic.loadActor()
	if actr.Head.Defined() && !ic.emptyObject.Equals(actr.Head) {
		ic.Abortf(exitcode.SysErrorIllegalActor, "failed to construct actor state: already initialized")
	}
	c, err := ic.rt.store.Put(ic.rt.ctx, obj)
	if err != nil {
		ic.Abortf(exitcode.ErrIllegalState, "failed to create actor state")
	}
	actr.Head = c
	ic.storeActor(actr)
	ic.stateUsedObjs[obj] = c // Track the expected CID of the object.
}

// Readonly is the implementation of the ActorStateHandle interface.
func (ic *invocationContext) StateReadonly(obj cbor.Unmarshaler) {
	// Load state to obj.
	c := ic.loadState(obj)
	ic.stateUsedObjs[obj.(cbor.Marshaler)] = c // Track the expected CID of the object.
}

// Transaction is the implementation of the ActorStateHandle interface.
func (ic *invocationContext) StateTransaction(obj cbor.Er, f func()) {
	if obj == nil {
		ic.Abortf(exitcode.SysErrorIllegalActor, "Must not pass nil to Transaction()")
	}
	ic.checkStateObjectsUnmodified()

	// Load state to obj.
	ic.loadState(obj)

	// Call user code allowing mutation but not side-effects
	ic.allowSideEffects = false
	f()
	ic.allowSideEffects = true

	c := ic.replace(obj)
	ic.stateUsedObjs[obj] = c // Track the expected CID of the object.
}

func (ic *invocationContext) VerifySignature(signature crypto.Signature, signer address.Address, plaintext []byte) error {
	return ic.Syscalls().VerifySignature(signature, signer, plaintext)
}

func (ic *invocationContext) HashBlake2b(data []byte) [32]byte {
	return ic.Syscalls().HashBlake2b(data)
}

func (ic *invocationContext) ComputeUnsealedSectorCID(reg abi.RegisteredSealProof, pieces []abi.PieceInfo) (cid.Cid, error) {
	return ic.Syscalls().ComputeUnsealedSectorCID(reg, pieces)
}

func (ic *invocationContext) VerifySeal(vi proof.SealVerifyInfo) error {
	return ic.Syscalls().VerifySeal(vi)
}

func (ic *invocationContext) BatchVerifySeals(vis map[address.Address][]proof.SealVerifyInfo) (map[address.Address][]bool, error) {
	return ic.Syscalls().BatchVerifySeals(vis)
}

func (ic *invocationContext) VerifyPoSt(vi proof.WindowPoStVerifyInfo) error {
	return ic.Syscalls().VerifyPoSt(vi)
}

func (ic *invocationContext) VerifyConsensusFault(h1, h2, extra []byte) (*runtime.ConsensusFault, error) {
	return ic.Syscalls().VerifyConsensusFault(h1, h2, extra)
}

func (ic *invocationContext) NetworkVersion() network.Version {
	return ic.rt.networkVersion
}

func (ic *invocationContext) CurrEpoch() abi.ChainEpoch {
	return ic.rt.currentEpoch
}

func (ic *invocationContext) CurrentBalance() abi.TokenAmount {
	// load balance
	act, found, err := ic.rt.GetActor(ic.msg.to)
	if err != nil {
		ic.Abortf(exitcode.ErrIllegalState, "could not load to actor %v: %v", ic.msg.to, err)
	} else if !found {
		ic.Abortf(exitcode.ErrIllegalState, "could not find to actor %v", ic.msg.to)
	}
	return act.Balance
}

func (ic *invocationContext) GetActorCodeCID(a address.Address) (cid.Cid, bool) {
	entry, found, err := ic.rt.GetActor(a)
	if !found {
		return cid.Undef, false
	}
	if err != nil {
		panic(err)
	}
	return entry.Code, true
}

func (ic *invocationContext) GetRandomnessFromBeacon(_ crypto.DomainSeparationTag, _ abi.ChainEpoch, _ []byte) abi.Randomness {
	return []byte("not really random")
}

func (ic *invocationContext) GetRandomnessFromTickets(_ crypto.DomainSeparationTag, _ abi.ChainEpoch, _ []byte) abi.Randomness {
	return []byte("not really random")
}

func (ic *invocationContext) ValidateImmediateCallerAcceptAny() {
	ic.assertf(!ic.callerValidated, "caller has been double validated")
	ic.callerValidated = true
}

func (ic *invocationContext) ValidateImmediateCallerIs(addrs ...address.Address) {
	ic.assertf(!ic.callerValidated, "caller has been double validated")
	ic.callerValidated = true
	for _, addr := range addrs {
		if ic.msg.from == addr {
			return
		}
	}
	ic.Abortf(exitcode.ErrForbidden, "caller address %v forbidden, allowed: %v", ic.msg.from, addrs)
}

func (ic *invocationContext) ValidateImmediateCallerType(types ...cid.Cid) {
	ic.assertf(!ic.callerValidated, "caller has been double validated")
	ic.callerValidated = true
	for _, t := range types {
		if t.Equals(ic.fromActor.Code) {
			return
		}
	}
	ic.Abortf(exitcode.ErrForbidden, "caller type %v forbidden, allowed: %v", ic.fromActor.Code, types)
}

func (ic *invocationContext) Abortf(errExitCode exitcode.ExitCode, msg string, args ...interface{}) {
	ic.rt.Abortf(errExitCode, msg, args...)
}

func (ic *invocationContext) assertf(condition bool, msg string, args ...interface{}) {
	if !condition {
		panic(fmt.Errorf(msg, args...))
	}
}

func (ic *invocationContext) ResolveAddress(address address.Address) (address.Address, bool) {
	return ic.rt.NormalizeAddress(address)
}

func (ic *invocationContext) NewActorAddress() address.Address {
	var buf bytes.Buffer

	b1, err := ic.topLevel.originatorStableAddress.Marshal()
	if err != nil {
		panic(err)
	}
	_, err = buf.Write(b1)
	if err != nil {
		panic(err)
	}

	err = binary.Write(&buf, binary.BigEndian, ic.topLevel.originatorCallSeq)
	if err != nil {
		panic(err)
	}

	err = binary.Write(&buf, binary.BigEndian, ic.topLevel.newActorAddressCount)
	if err != nil {
		panic(err)
	}

	actorAddress, err := address.NewActorAddress(buf.Bytes())
	if err != nil {
		panic(err)
	}
	return actorAddress
}

// Send implements runtime.InvocationContext.
func (ic *invocationContext) Send(toAddr address.Address, methodNum abi.MethodNum, params cbor.Marshaler, value abi.TokenAmount, out cbor.Er) (errcode exitcode.ExitCode) {
	// check if side-effects are allowed
	if !ic.allowSideEffects {
		ic.Abortf(exitcode.SysErrorIllegalActor, "Calling Send() is not allowed during side-effect lock")
	}
	from := ic.msg.to
	fromActor, found, err := ic.rt.GetActor(from)
	if err != nil {
		ic.Abortf(exitcode.ErrIllegalState, "could not retrieve send actor %v for internal send: %v", from, err)
	} else if !found {
		ic.Abortf(exitcode.ErrIllegalState, "could not find send actor %v for internal send", from)
	}

	newMsg := InternalMessage{
		from:   from,
		to:     toAddr,
		value:  value,
		method: methodNum,
		params: params,
	}

	newCtx := newInvocationContext(ic.rt, ic.topLevel, newMsg, fromActor, ic.emptyObject)
	ret, code := newCtx.invoke()

	ic.stats.MergeSubStat(newCtx.toActor.Code, newMsg.method, newCtx.stats)

	err = ret.Into(out)
	if err != nil {
		ic.Abortf(exitcode.ErrSerialization, "failed to serialize send return value into output parameter")
	}
	return code
}

// CreateActor implements runtime.ExtendedInvocationContext.
func (ic *invocationContext) CreateActor(codeID cid.Cid, addr address.Address) {
	act, ok := ic.rt.ActorImpls[codeID]
	if !ok {
		ic.Abortf(exitcode.SysErrorIllegalArgument, "Can only create built-in actors.")
	}

	if rt.IsSingletonActor(act) {
		ic.Abortf(exitcode.SysErrorIllegalArgument, "Can only have one instance of singleton actors.")
	}

	ic.rt.Log(rt.DEBUG, "creating actor, friendly-name: %s, Exitcode: %s, addr: %s\n", builtin.ActorNameByCode(codeID), codeID, addr)

	// Check existing address. If nothing there, create empty actor.
	//
	// Note: we are storing the actors by ActorID *address*
	_, found, err := ic.rt.GetActor(addr)
	if err != nil {
		panic(err)
	}
	if found {
		ic.Abortf(exitcode.SysErrorIllegalArgument, "Actor address already exists")
	}

	newActor := &states.Actor{
		Head:    ic.emptyObject,
		Code:    codeID,
		Balance: abi.NewTokenAmount(0),
	}
	if err := ic.rt.setActor(ic.rt.ctx, addr, newActor); err != nil {
		panic(err)
	}
}

// deleteActor implements runtime.ExtendedInvocationContext.
func (ic *invocationContext) DeleteActor(beneficiary address.Address) {
	receiver := ic.msg.to
	receiverActor, found, err := ic.rt.GetActor(receiver)
	if err != nil {
		panic(err)
	}
	if !found {
		ic.Abortf(exitcode.SysErrorIllegalActor, "delete non-existent actor %v", receiverActor)
	}

	// Transfer any remaining balance to the beneficiary.
	// This looks like it could cause a problem with gas refund going to a non-existent actor, but the gas payer
	// is always an account actor, which cannot be the receiver of this message.
	if receiverActor.Balance.GreaterThan(big.Zero()) {
		ic.rt.transfer(receiver, beneficiary, receiverActor.Balance)
	}

	if err := ic.rt.deleteActor(ic.rt.ctx, receiver); err != nil {
		panic(err)
	}
}

func (ic *invocationContext) TotalFilCircSupply() abi.TokenAmount {
	return ic.topLevel.circSupply
}

func (ic *invocationContext) Context() context.Context {
	return ic.rt.ctx
}

func (ic *invocationContext) ChargeGas(_ string, _ int64, _ int64) {
	// no-op
}

// Starts a new tracing span. The span must be End()ed explicitly, typically with a deferred invocation.
func (ic *invocationContext) StartSpan(_ string) func() {
	return fakeTraceSpanEnd
}

// Provides the system call interface.
func (ic *invocationContext) Syscalls() runtime.Syscalls {
	return fakeSyscalls{receiver: ic.msg.to, epoch: ic.rt.currentEpoch}
}

// Note events that may make debugging easier
func (ic *invocationContext) Log(level rt.LogLevel, msg string, args ...interface{}) {
	ic.rt.Log(level, msg, args...)
}

type returnWrapper struct {
	inner cbor.Marshaler
}

func (r returnWrapper) Into(o cbor.Unmarshaler) error {
	if r.inner == nil {
		return fmt.Errorf("failed to unmarshal nil return (did you mean abi.Empty?)")
	}
	b := bytes.Buffer{}
	if err := r.inner.MarshalCBOR(&b); err != nil {
		return err
	}
	return o.UnmarshalCBOR(&b)
}

/////////////////////////////////////////////
//          Fake syscalls
/////////////////////////////////////////////

type fakeSyscalls struct {
	receiver address.Address
	epoch    abi.ChainEpoch
}

func (s fakeSyscalls) VerifySignature(_ crypto.Signature, _ address.Address, _ []byte) error {
	return nil
}

func (s fakeSyscalls) HashBlake2b(b []byte) [32]byte {
	return blake2b.Sum256(b)
}

func (s fakeSyscalls) ComputeUnsealedSectorCID(_ abi.RegisteredSealProof, _ []abi.PieceInfo) (cid.Cid, error) {
	return testing.MakeCID("presealedSectorCID", nil), nil
}

func (s fakeSyscalls) VerifySeal(_ proof.SealVerifyInfo) error {
	return nil
}

func (s fakeSyscalls) BatchVerifySeals(vi map[address.Address][]proof.SealVerifyInfo) (map[address.Address][]bool, error) {
	res := map[address.Address][]bool{}
	for addr, infos := range vi { //nolint:nomaprange
		verified := make([]bool, len(infos))
		for i := range infos {
			// everyone wins
			verified[i] = true
		}
		res[addr] = verified
	}
	return res, nil
}

func (s fakeSyscalls) VerifyPoSt(_ proof.WindowPoStVerifyInfo) error {
	return nil
}

func (s fakeSyscalls) VerifyConsensusFault(_, _, _ []byte) (*runtime.ConsensusFault, error) {
	return &runtime.ConsensusFault{
		Target: s.receiver,
		Epoch:  s.epoch - 1,
		Type:   runtime.ConsensusFaultDoubleForkMining,
	}, nil
}

/////////////////////////////////////////////
//          Fake trace span
/////////////////////////////////////////////

func fakeTraceSpanEnd() {
}

/////////////////////////////////////////////
//          storeWrapper
/////////////////////////////////////////////

type storeWrapper struct {
	s  adt.Store
	rt *VM
}

func (s storeWrapper) StoreGet(c cid.Cid, o cbor.Unmarshaler) bool {
	err := s.s.Get(s.rt.ctx, c, o)
	// assume all errors are not found errors (bad assumption, but ok for testing)
	return err == nil
}

func (s storeWrapper) StorePut(x cbor.Marshaler) cid.Cid {
	c, err := s.s.Put(s.rt.ctx, x)
	if err != nil {
		s.rt.Abortf(exitcode.ErrIllegalState, "could not put object in store")
	}
	return c
}

/////////////////////////////////////////////
//          invocation
/////////////////////////////////////////////

// runtime aborts are trapped by invoke, it will always return an exit code.
func (ic *invocationContext) invoke() (ret returnWrapper, errcode exitcode.ExitCode) {
	// Checkpoint state, for restoration on rollback
	// Note that changes prior to invocation (sequence number bump and gas prepayment) persist even if invocation fails.
	priorRoot, err := ic.rt.checkpoint()
	if err != nil {
		panic(err)
	}

	ic.rt.startInvocation(&ic.msg)

	// Install handler for abort, which rolls back all state changes from this and any nested invocations.
	// This is the only path by which a non-OK exit code may be returned.
	defer func() {
		ic.stats.Capture()

		if r := recover(); r != nil {
			if err := ic.rt.rollback(priorRoot); err != nil {
				panic(err)
			}
			switch r := r.(type) {
			case abort:
				ic.rt.Log(rt.WARN, "Abort during actor execution. errMsg: %v exitCode: %d sender: %v receiver; %v method: %d value %v",
					r, r.code, ic.msg.from, ic.msg.to, ic.msg.method, ic.msg.value)
				ic.rt.endInvocation(r.code, abi.Empty)
				ret = returnWrapper{abi.Empty} // The Empty here should never be used, but slightly safer than zero value.
				errcode = r.code
				return
			default:
				// do not trap unknown panics
				debug.PrintStack()
				panic(r)
			}
		}
	}()

	// pre-dispatch
	// 1. load target actor
	// 2. transfer optional funds
	// 3. short-circuit _Send_ method
	// 4. load target actor code
	// 5. create target state handle
	// assert from address is an ID address.
	if ic.msg.from.Protocol() != address.ID {
		panic("bad Exitcode: sender address MUST be an ID address at invocation time")
	}

	// 2. load target actor
	// Note: we replace the "to" address with the normalized version
	ic.toActor, ic.msg.to = ic.resolveTarget(ic.msg.to)

	// 3. transfer funds carried by the msg
	if !ic.msg.value.NilOrZero() {
		if ic.msg.value.LessThan(big.Zero()) {
			ic.Abortf(exitcode.SysErrForbidden, "attempt to transfer negative value %s from %s to %s",
				ic.msg.value, ic.msg.from, ic.msg.to)
		}
		if ic.fromActor.Balance.LessThan(ic.msg.value) {
			ic.Abortf(exitcode.SysErrInsufficientFunds, "sender %s insufficient balance %s to transfer %s to %s",
				ic.msg.from, ic.fromActor.Balance, ic.msg.value, ic.msg.to)
		}
		ic.toActor, ic.fromActor = ic.rt.transfer(ic.msg.from, ic.msg.to, ic.msg.value)
	}

	// 4. if we are just sending funds, there is nothing else to do.
	if ic.msg.method == builtin.MethodSend {
		ic.rt.endInvocation(exitcode.Ok, abi.Empty)
		return returnWrapper{abi.Empty}, exitcode.Ok
	}

	// 5. load target actor code
	actorImpl := ic.rt.getActorImpl(ic.toActor.Code)

	// dispatch
	out, err := ic.dispatch(actorImpl, ic.msg.method, ic.msg.params)
	if err != nil {
		ic.Abortf(exitcode.SysErrInvalidMethod, "could not dispatch method")
	}

	// assert output implements expected interface
	var marsh cbor.Marshaler = abi.Empty
	if out != nil {
		var ok bool
		marsh, ok = out.(cbor.Marshaler)
		if !ok {
			ic.Abortf(exitcode.SysErrorIllegalActor, "Returned value is not a CBORMarshaler")
		}
	}
	ret = returnWrapper{inner: marsh}

	ic.checkStateObjectsUnmodified()

	// 3. success!
	ic.rt.endInvocation(exitcode.Ok, marsh)
	return ret, exitcode.Ok
}

func (ic *invocationContext) dispatch(actor runtime.VMActor, method abi.MethodNum, arg interface{}) (interface{}, error) {
	// get method signature
	exports := actor.Exports()

	// get method entry
	methodIdx := (uint64)(method)
	if len(exports) <= (int)(methodIdx) {
		return nil, fmt.Errorf("method undefined. method: %d, Exitcode: %s", method, actor.Code())
	}
	entry := exports[methodIdx]
	if entry == nil {
		return nil, fmt.Errorf("method undefined. method: %d, Exitcode: %s", method, actor.Code())
	}

	ventry := reflect.ValueOf(entry)

	// build args to pass to the method
	args := []reflect.Value{
		// the ctx will be automatically coerced
		reflect.ValueOf(ic),
	}

	t := ventry.Type().In(1)
	if arg == nil {
		args = append(args, reflect.New(t).Elem())
	} else if raw, ok := arg.([]byte); ok {
		obj, err := decodeBytes(t, raw)
		if err != nil {
			return nil, err
		}
		args = append(args, reflect.ValueOf(obj))
	} else if raw, ok := arg.(builtin.CBORBytes); ok {
		obj, err := decodeBytes(t, raw)
		if err != nil {
			return nil, err
		}
		args = append(args, reflect.ValueOf(obj))
	} else {
		args = append(args, reflect.ValueOf(arg))
	}

	// invoke the method
	out := ventry.Call(args)

	// Note: we only support single objects being returned
	if len(out) > 1 {
		return nil, fmt.Errorf("actor method returned more than one object. method: %d, Exitcode: %s", method, actor.Code())
	}

	// method returns unit
	// Note: we need to check for `IsNill()` here because Go doesnt work if you do `== nil` on the interface
	if len(out) == 0 || (out[0].Kind() != reflect.Struct && out[0].IsNil()) {
		return nil, nil
	}

	// forward return
	return out[0].Interface(), nil
}

// resolveTarget loads and actor and returns its ActorID address.
//
// If the target actor does not exist, and the target address is a pub-key address,
// a new account actor will be created.
// Otherwise, this method will abort execution.
func (ic *invocationContext) resolveTarget(target address.Address) (*states.Actor, address.Address) {
	// resolve the target address via the InitActor, and attempt to load state.
	initActorEntry, found, err := ic.rt.GetActor(builtin.InitActorAddr)
	if err != nil {
		panic(err)
	}
	if !found {
		ic.Abortf(exitcode.SysErrSenderInvalid, "init actor not found")
	}

	if target == builtin.InitActorAddr {
		return initActorEntry, target
	}

	// get a view into the actor state
	var state init_.State
	if err := ic.rt.store.Get(ic.rt.ctx, initActorEntry.Head, &state); err != nil {
		panic(err)
	}

	// lookup the ActorID based on the address
	targetIDAddr, found, err := state.ResolveAddress(ic.rt.store, target)
	created := false
	if err != nil {
		panic(err)
	} else if !found {
		if target.Protocol() != address.SECP256K1 && target.Protocol() != address.BLS {
			// Don't implicitly create an account actor for an address without an associated key.
			ic.Abortf(exitcode.SysErrInvalidReceiver, "cannot create account for address type")
		}

		targetIDAddr, err = state.MapAddressToNewID(ic.rt.store, target)
		if err != nil {
			panic(err)
		}
		// store new state
		initHead, err := ic.rt.store.Put(ic.rt.ctx, &state)
		if err != nil {
			panic(err)
		}
		// update init actor
		initActorEntry.Head = initHead
		if err := ic.rt.setActor(ic.rt.ctx, builtin.InitActorAddr, initActorEntry); err != nil {
			panic(err)
		}

		ic.CreateActor(builtin.AccountActorCodeID, targetIDAddr)

		// call constructor on account
		newMsg := InternalMessage{
			from:   builtin.SystemActorAddr,
			to:     targetIDAddr,
			value:  big.Zero(),
			method: builtin.MethodsAccount.Constructor,
			// use original address as constructor params
			// Note: constructor takes a pointer
			params: &target,
		}

		newCtx := newInvocationContext(ic.rt, ic.topLevel, newMsg, nil, ic.emptyObject)
		_, code := newCtx.invoke()

		ic.stats.MergeSubStat(builtin.InitActorCodeID, builtin.MethodsAccount.Constructor, newCtx.stats)

		if code.IsError() {
			// we failed to construct an account actor..
			ic.Abortf(code, "failed to construct account actor")
		}

		created = true
	}

	// load actor
	targetActor, found, err := ic.rt.GetActor(targetIDAddr)
	if err != nil {
		panic(err)
	}
	if !found && created {
		panic(fmt.Errorf("unreachable: actor is supposed to exist but it does not. addr: %s, idAddr: %s", target, targetIDAddr))
	}
	if !found {
		ic.Abortf(exitcode.SysErrInvalidReceiver, "actor at address %s registered but not found", targetIDAddr.String())
	}

	return targetActor, targetIDAddr
}

func (ic *invocationContext) replace(obj cbor.Marshaler) cid.Cid {
	actr, found, err := ic.rt.GetActor(ic.msg.to)
	if err != nil {
		panic(err)
	}
	if !found {
		ic.rt.Abortf(exitcode.ErrIllegalState, "failed to find actor %s for state", ic.msg.to)
	}
	c, err := ic.rt.store.Put(ic.rt.ctx, obj)
	if err != nil {
		ic.rt.Abortf(exitcode.ErrIllegalState, "could not save new state")
	}
	actr.Head = c
	err = ic.rt.setActor(ic.rt.ctx, ic.msg.to, actr)
	if err != nil {
		ic.rt.Abortf(exitcode.ErrIllegalState, "could not save actor %s", ic.msg.to)
	}
	return c
}

// Checks that state objects weren't modified outside of transaction.
func (ic *invocationContext) checkStateObjectsUnmodified() {
	for obj, expectedKey := range ic.stateUsedObjs { // nolint:nomaprange
		// Recompute the CID of the object and check it's the same as was recorded
		// when the object was loaded.
		finalKey, _, err := ipld.MarshalCBOR(obj)
		if err != nil {
			ic.Abortf(exitcode.SysErrorIllegalActor, "error marshalling state object for validation: %v", err)
		}
		if finalKey != expectedKey {
			ic.Abortf(exitcode.SysErrorIllegalActor, "State mutated outside of transaction scope")
		}
	}
}

func decodeBytes(t reflect.Type, argBytes []byte) (interface{}, error) {
	// decode arg1 (this is the payload for the actor method)
	v := reflect.New(t)

	// This would be better fixed in then encoding library.
	obj := v.Elem().Interface()
	if _, ok := obj.(cbor.Unmarshaler); !ok {
		return nil, errors.New("method argument cannot be decoded")
	}

	buf := bytes.NewBuffer(argBytes)
	auxv := reflect.New(t.Elem())
	obj = auxv.Interface()

	unmarsh := obj.(cbor.Unmarshaler)
	if err := unmarsh.UnmarshalCBOR(buf); err != nil {
		return nil, err
	}
	return unmarsh, nil
}
