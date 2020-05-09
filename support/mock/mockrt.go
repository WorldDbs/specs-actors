package mock

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	goruntime "runtime"
	"runtime/debug"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/go-state-types/rt"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
)

// A mock runtime for unit testing of actors in isolation.
// The mock allows direct specification of the runtime context as observable by an actor, supports
// the storage interface, and mocks out side-effect-inducing calls.
type Runtime struct {
	// Execution context
	ctx               context.Context
	epoch             abi.ChainEpoch
	networkVersion    network.Version
	receiver          addr.Address
	caller            addr.Address
	callerType        cid.Cid
	miner             addr.Address
	valueReceived     abi.TokenAmount
	idAddresses       map[addr.Address]addr.Address
	actorCodeCIDs     map[addr.Address]cid.Cid
	newActorAddr      addr.Address
	circulatingSupply abi.TokenAmount

	// Actor state
	state   cid.Cid
	balance abi.TokenAmount

	// VM implementation
	store         map[cid.Cid][]byte
	inCall        bool
	inTransaction bool
	// Maps (references to) loaded state objs to their expected cid.
	// Used for detecting modifications to state outside of transactions.
	stateUsedObjs map[cbor.Marshaler]cid.Cid
	// Syscalls
	hashfunc func(data []byte) [32]byte

	// Expectations
	t                              testing.TB
	expectValidateCallerAny        bool
	expectValidateCallerAddr       []addr.Address
	expectValidateCallerType       []cid.Cid
	expectRandomnessBeacon         []*expectRandomness
	expectRandomnessTickets        []*expectRandomness
	expectSends                    []*expectedMessage
	expectVerifySigs               []*expectVerifySig
	expectCreateActor              *expectCreateActor
	expectVerifySeal               *expectVerifySeal
	expectComputeUnsealedSectorCID *expectComputeUnsealedSectorCID
	expectVerifyPoSt               *expectVerifyPoSt
	expectVerifyConsensusFault     *expectVerifyConsensusFault
	expectDeleteActor              *addr.Address
	expectBatchVerifySeals         *expectBatchVerifySeals

	logs []string
	// Gas charged explicitly through rt.ChargeGas. Note: most charges are implicit
	gasCharged int64
}

type expectBatchVerifySeals struct {
	in  map[addr.Address][]proof.SealVerifyInfo
	out map[addr.Address][]bool
	err error
}

type expectRandomness struct {
	// Expected parameters.
	tag     crypto.DomainSeparationTag
	epoch   abi.ChainEpoch
	entropy []byte
	// Result.
	out abi.Randomness
}

type expectedMessage struct {
	// expectedMessage values
	to     addr.Address
	method abi.MethodNum
	params cbor.Marshaler
	value  abi.TokenAmount

	// returns from applying expectedMessage
	sendReturn cbor.Er
	exitCode   exitcode.ExitCode
}

type expectVerifySig struct {
	// Expected arguments
	sig       crypto.Signature
	signer    addr.Address
	plaintext []byte
	// Result
	result error
}

type expectVerifySeal struct {
	seal   proof.SealVerifyInfo
	result error
}

type expectComputeUnsealedSectorCID struct {
	reg       abi.RegisteredSealProof
	pieces    []abi.PieceInfo
	cid       cid.Cid
	resultErr error
}

type expectVerifyPoSt struct {
	post   proof.WindowPoStVerifyInfo
	result error
}

func (m *expectedMessage) Equal(to addr.Address, method abi.MethodNum, params cbor.Marshaler, value abi.TokenAmount) bool {
	// avoid nil vs. zero/empty discrepancies that would disappear in serialization
	paramBuf1 := new(bytes.Buffer)
	if m.params != nil {
		m.params.MarshalCBOR(paramBuf1) // nolint: errcheck
	}
	paramBuf2 := new(bytes.Buffer)
	if params != nil {
		params.MarshalCBOR(paramBuf2) // nolint: errcheck
	}

	return m.to == to && m.method == method && m.value.Equals(value) && bytes.Equal(paramBuf1.Bytes(), paramBuf2.Bytes())
}

func (m *expectedMessage) String() string {
	return fmt.Sprintf("to: %v method: %v value: %v params: %v sendReturn: %v exitCode: %v", m.to, m.method, m.value, m.params, m.sendReturn, m.exitCode)
}

type expectCreateActor struct {
	// Expected parameters
	codeId  cid.Cid
	address addr.Address
}

type expectVerifyConsensusFault struct {
	requireCorrectInput bool
	BlockHeader1        []byte
	BlockHeader2        []byte
	BlockHeaderExtra    []byte

	Fault *runtime.ConsensusFault
	Err   error
}

var _ runtime.Runtime = &Runtime{}
var _ runtime.StateHandle = &Runtime{}
var typeOfRuntimeInterface = reflect.TypeOf((*runtime.Runtime)(nil)).Elem()
var typeOfCborUnmarshaler = reflect.TypeOf((*cbor.Unmarshaler)(nil)).Elem()
var typeOfCborMarshaler = reflect.TypeOf((*cbor.Marshaler)(nil)).Elem()

///// Implementation of the runtime API /////

func (rt *Runtime) NetworkVersion() network.Version {
	return rt.networkVersion
}

func (rt *Runtime) CurrEpoch() abi.ChainEpoch {
	rt.requireInCall()
	return rt.epoch
}

func (rt *Runtime) ValidateImmediateCallerAcceptAny() {
	rt.requireInCall()
	if !rt.expectValidateCallerAny {
		rt.failTest("unexpected validate-caller-any")
	}
	rt.expectValidateCallerAny = false
}

func (rt *Runtime) ValidateImmediateCallerIs(addrs ...addr.Address) {
	rt.requireInCall()
	rt.checkArgument(len(addrs) > 0, "addrs must be non-empty")
	// Check and clear expectations.
	if len(rt.expectValidateCallerAddr) == 0 {
		rt.failTest("unexpected validate caller addrs")
		return
	}
	if !reflect.DeepEqual(rt.expectValidateCallerAddr, addrs) {
		rt.failTest("unexpected validate caller addrs %v, expected %+v", addrs, rt.expectValidateCallerAddr)
		return
	}
	defer func() {
		rt.expectValidateCallerAddr = nil
	}()

	// Implement method.
	for _, expected := range addrs {
		if rt.caller == expected {
			return
		}
	}
	rt.Abortf(exitcode.SysErrForbidden, "caller address %v forbidden, allowed: %v", rt.caller, addrs)
}

func (rt *Runtime) ValidateImmediateCallerType(types ...cid.Cid) {
	rt.requireInCall()
	rt.checkArgument(len(types) > 0, "types must be non-empty")

	// Check and clear expectations.
	if len(rt.expectValidateCallerType) == 0 {
		rt.failTest("unexpected validate caller code")
	}
	if !reflect.DeepEqual(rt.expectValidateCallerType, types) {
		rt.failTest("unexpected validate caller code %v, expected %+v", types, rt.expectValidateCallerType)
	}
	defer func() {
		rt.expectValidateCallerType = nil
	}()

	// Implement method.
	for _, expected := range types {
		if rt.callerType.Equals(expected) {
			return
		}
	}
	rt.Abortf(exitcode.SysErrForbidden, "caller type %v forbidden, allowed: %v", rt.callerType, types)
}

func (rt *Runtime) CurrentBalance() abi.TokenAmount {
	rt.requireInCall()
	return rt.balance
}

func (rt *Runtime) ResolveAddress(address addr.Address) (ret addr.Address, ok bool) {
	rt.requireInCall()
	if address.Protocol() == addr.ID {
		return address, true
	}
	resolved, ok := rt.idAddresses[address]
	return resolved, ok
}

func (rt *Runtime) GetIdAddr(raw addr.Address) (addr.Address, bool) {
	if raw.Protocol() == addr.ID {
		return raw, true
	}
	a, found := rt.idAddresses[raw]
	return a, found
}

func (rt *Runtime) GetActorCodeCID(addr addr.Address) (ret cid.Cid, ok bool) {
	rt.requireInCall()
	ret, ok = rt.actorCodeCIDs[addr]
	return
}

func (rt *Runtime) GetRandomnessFromBeacon(tag crypto.DomainSeparationTag, epoch abi.ChainEpoch, entropy []byte) abi.Randomness {
	rt.requireInCall()
	if len(rt.expectRandomnessBeacon) == 0 {
		rt.failTestNow("unexpected call to get randomness for tag %v, epoch %v", tag, epoch)
	}

	if epoch > rt.epoch {
		rt.failTestNow("attempt to get randomness from future\n"+
			"         requested epoch: %d greater than current epoch %d\n", epoch, rt.epoch)
	}

	exp := rt.expectRandomnessBeacon[0]
	if tag != exp.tag || epoch != exp.epoch || !bytes.Equal(entropy, exp.entropy) {
		rt.failTest("unexpected get randomness\n"+
			"         tag: %d, epoch: %d, entropy: %v\n"+
			"expected tag: %d, epoch: %d, entropy: %v", tag, epoch, entropy, exp.tag, exp.epoch, exp.entropy)
	}
	defer func() {
		rt.expectRandomnessBeacon = rt.expectRandomnessBeacon[1:]
	}()
	return exp.out
}

func (rt *Runtime) GetRandomnessFromTickets(tag crypto.DomainSeparationTag, epoch abi.ChainEpoch, entropy []byte) abi.Randomness {
	rt.requireInCall()
	if len(rt.expectRandomnessTickets) == 0 {
		rt.failTestNow("unexpected call to get randomness for tag %v, epoch %v", tag, epoch)
	}

	if epoch > rt.epoch {
		rt.failTestNow("attempt to get randomness from future\n"+
			"         requested epoch: %d greater than current epoch %d\n", epoch, rt.epoch)
	}

	exp := rt.expectRandomnessTickets[0]
	if tag != exp.tag || epoch != exp.epoch || !bytes.Equal(entropy, exp.entropy) {
		rt.failTest("unexpected get randomness\n"+
			"         tag: %d, epoch: %d, entropy: %v\n"+
			"expected tag: %d, epoch: %d, entropy: %v", tag, epoch, entropy, exp.tag, exp.epoch, exp.entropy)
	}
	defer func() {
		rt.expectRandomnessTickets = rt.expectRandomnessTickets[1:]
	}()
	return exp.out
}

func (rt *Runtime) Send(toAddr addr.Address, methodNum abi.MethodNum, params cbor.Marshaler, value abi.TokenAmount, out cbor.Er) exitcode.ExitCode {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	if len(rt.expectSends) == 0 {
		rt.failTestNow("unexpected send to: %v method: %v, value: %v, params: %v", toAddr, methodNum, value, params)
	}
	exp := rt.expectSends[0]

	if !exp.Equal(toAddr, methodNum, params, value) {
		toName := "unknown"
		toMeth := "unknown"
		expToName := "unknown"
		expToMeth := "unknown"
		if code, ok := rt.GetActorCodeCID(toAddr); ok && builtin.IsBuiltinActor(code) {
			toName = builtin.ActorNameByCode(code)
			toMeth = getMethodName(code, methodNum)
		}
		if code, ok := rt.GetActorCodeCID(exp.to); ok && builtin.IsBuiltinActor(code) {
			expToName = builtin.ActorNameByCode(code)
			expToMeth = getMethodName(code, exp.method)
		}

		rt.failTestNow("unexpected send\n"+
			"          to: %s (%s) method: %d (%s) value: %v params: %v\n"+
			"Expected  to: %s (%s) method: %d (%s) value: %v params: %v",
			toAddr, toName, methodNum, toMeth, value, params, exp.to, expToName, exp.method, expToMeth, exp.value, exp.params)
	}

	if value.GreaterThan(rt.balance) {
		rt.Abortf(exitcode.SysErrSenderStateInvalid, "cannot send value: %v exceeds balance: %v", value, rt.balance)
	}

	// pop the expectedMessage from the queue and modify the mockrt balance to reflect the send.
	defer func() {
		rt.expectSends = rt.expectSends[1:]
		rt.balance = big.Sub(rt.balance, value)
	}()

	// populate the output argument
	var buf bytes.Buffer
	err := exp.sendReturn.MarshalCBOR(&buf)
	if err != nil {
		rt.failTestNow("error serializing expected send return: %v", err)
	}
	err = out.UnmarshalCBOR(&buf)
	if err != nil {
		rt.failTestNow("error deserializing send return bytes to output param: %v", err)
	}

	return exp.exitCode
}

func (rt *Runtime) NewActorAddress() addr.Address {
	rt.requireInCall()
	if rt.newActorAddr == addr.Undef {
		rt.failTestNow("unexpected call to new actor address")
	}
	defer func() { rt.newActorAddr = addr.Undef }()
	return rt.newActorAddr
}

func (rt *Runtime) CreateActor(codeId cid.Cid, address addr.Address) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	exp := rt.expectCreateActor
	if exp != nil {
		if !exp.codeId.Equals(codeId) || exp.address != address {
			rt.failTest("unexpected create actor, code: %s, address: %s; expected code: %s, address: %s",
				codeId, address, exp.codeId, exp.address)
		}
		defer func() {
			rt.expectCreateActor = nil
		}()
		return
	}
	rt.failTestNow("unexpected call to create actor")
}

func (rt *Runtime) DeleteActor(addr addr.Address) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	if rt.expectDeleteActor == nil {
		rt.failTestNow("unexpected call to delete actor %s", addr.String())
	}

	if *rt.expectDeleteActor != addr {
		rt.failTestNow("attempt to delete wrong actor. Expected %s, got %s.", rt.expectDeleteActor.String(), addr.String())
	}
	rt.expectDeleteActor = nil
}

func (rt *Runtime) TotalFilCircSupply() abi.TokenAmount {
	return rt.circulatingSupply
}

func (rt *Runtime) Abortf(errExitCode exitcode.ExitCode, msg string, args ...interface{}) {
	rt.requireInCall()
	rt.t.Logf("Mock Runtime Abort ExitCode: %v Reason: %s", errExitCode, fmt.Sprintf(msg, args...))
	panic(abort{errExitCode, fmt.Sprintf(msg, args...)})
}

func (rt *Runtime) Context() context.Context {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	return rt.ctx
}

func (rt *Runtime) StartSpan(_ string) func() {
	rt.requireInCall()
	return func() {}
}

func (rt *Runtime) checkArgument(predicate bool, msg string, args ...interface{}) {
	if !predicate {
		rt.Abortf(exitcode.SysErrorIllegalArgument, msg, args...)
	}
}

///// Store implementation /////

// Gets raw data from the state. This function will extract inline data from the
// CID to better mimic what filecoin implementations should do.
func (rt *Runtime) get(c cid.Cid) ([]byte, bool) {
	prefix := c.Prefix()
	if prefix.Codec != cid.DagCBOR {
		rt.Abortf(exitcode.ErrSerialization, "tried to fetch a non-cbor object: %s", c)
	}

	var data []byte
	if prefix.MhType == mh.IDENTITY {
		decoded, err := mh.Decode(c.Hash())
		if err != nil {
			rt.Abortf(exitcode.ErrSerialization, "failed to parse identity cid %s: %s", c, err)
		}
		data = decoded.Digest
	} else if stored, found := rt.store[c]; found {
		data = stored
	} else {
		return nil, false
	}
	return data, true
}

// Puts raw data into the state, but only if it's not "inlined" into the CID.
func (rt *Runtime) put(c cid.Cid, data []byte) {
	if c.Prefix().MhType != mh.IDENTITY {
		rt.store[c] = data
	}
}

func (rt *Runtime) StoreGet(c cid.Cid, o cbor.Unmarshaler) bool {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	data, found := rt.get(c)
	if found {
		err := o.UnmarshalCBOR(bytes.NewReader(data))
		if err != nil {
			rt.Abortf(exitcode.ErrSerialization, err.Error())
		}
	}

	return found
}

func (rt *Runtime) StorePut(o cbor.Marshaler) cid.Cid {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	key, data, err := ipld.MarshalCBOR(o)
	if err != nil {
		rt.Abortf(exitcode.ErrSerialization, err.Error())
	}
	rt.put(key, data)
	return key
}

///// Message implementation /////

func (rt *Runtime) BlockMiner() addr.Address {
	return rt.miner
}

func (rt *Runtime) Caller() addr.Address {
	return rt.caller
}

func (rt *Runtime) Receiver() addr.Address {
	return rt.receiver
}

func (rt *Runtime) ValueReceived() abi.TokenAmount {
	return rt.valueReceived
}

///// State handle implementation /////

func (rt *Runtime) StateCreate(obj cbor.Marshaler) {
	if rt.state.Defined() {
		rt.Abortf(exitcode.SysErrorIllegalActor, "state already constructed")
	}
	rt.state = rt.StorePut(obj)
	// Track the expected CID of the object.
	rt.stateUsedObjs[obj] = rt.state
}

func (rt *Runtime) StateReadonly(st cbor.Unmarshaler) {
	found := rt.StoreGet(rt.state, st)
	if !found {
		panic(fmt.Sprintf("actor state not found: %v", rt.state))
	}
	// Track the expected CID of the object.
	rt.stateUsedObjs[st.(cbor.Marshaler)] = rt.state
}

func (rt *Runtime) StateTransaction(st cbor.Er, f func()) {
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "nested transaction")
	}
	rt.checkStateObjectsUnmodified()
	rt.StateReadonly(st)
	rt.inTransaction = true
	defer func() { rt.inTransaction = false }()
	f()
	rt.state = rt.StorePut(st)
	// Track the expected CID of the object.
	rt.stateUsedObjs[st] = rt.state
}

///// Syscalls implementation /////

func (rt *Runtime) VerifySignature(sig crypto.Signature, signer addr.Address, plaintext []byte) error {
	if len(rt.expectVerifySigs) == 0 {
		rt.failTest("unexpected signature verification sig: %v, signer: %s, plaintext: %v", sig, signer, plaintext)
	}

	exp := rt.expectVerifySigs[0]
	if exp != nil {
		if !exp.sig.Equals(&sig) || exp.signer != signer || !bytes.Equal(exp.plaintext, plaintext) {
			rt.failTest("unexpected signature verification\n"+
				"         sig: %v, signer: %s, plaintext: %v\n"+
				"expected sig: %v, signer: %s, plaintext: %v",
				sig, signer, plaintext, exp.sig, exp.signer, exp.plaintext)
		}
		defer func() {
			rt.expectVerifySigs = rt.expectVerifySigs[1:]
		}()
		return exp.result
	}
	rt.failTestNow("unexpected syscall to verify signature %v, signer %s, plaintext %v", sig, signer, plaintext)
	return nil
}

func (rt *Runtime) HashBlake2b(data []byte) [32]byte {
	return rt.hashfunc(data)
}

func (rt *Runtime) ComputeUnsealedSectorCID(reg abi.RegisteredSealProof, pieces []abi.PieceInfo) (cid.Cid, error) {
	exp := rt.expectComputeUnsealedSectorCID
	if exp != nil {
		if !reflect.DeepEqual(exp.reg, reg) {
			rt.failTest("unexpected ComputeUnsealedSectorCID proof, expected: %v, got: %v", exp.reg, reg)
		}
		if !reflect.DeepEqual(exp.pieces, pieces) {
			rt.failTest("unexpected ComputeUnsealedSectorCID pieces, expected: %v, got: %v", exp.pieces, pieces)
		}

		defer func() {
			rt.expectComputeUnsealedSectorCID = nil
		}()
		return exp.cid, exp.resultErr
	}
	rt.failTestNow("unexpected syscall to ComputeUnsealedSectorCID %v", reg)
	return cid.Cid{}, nil
}

func (rt *Runtime) VerifySeal(seal proof.SealVerifyInfo) error {
	exp := rt.expectVerifySeal
	if exp != nil {
		if !reflect.DeepEqual(exp.seal, seal) {
			rt.failTest("unexpected seal verification\n"+
				"        : %v\n"+
				"expected: %v",
				seal, exp.seal)
		}
		defer func() {
			rt.expectVerifySeal = nil
		}()
		return exp.result
	}
	rt.failTestNow("unexpected syscall to verify seal %v", seal)
	return nil
}

func (rt *Runtime) ExpectBatchVerifySeals(in map[addr.Address][]proof.SealVerifyInfo, out map[addr.Address][]bool, err error) {
	rt.expectBatchVerifySeals = &expectBatchVerifySeals{
		in, out, err,
	}
}

func (rt *Runtime) BatchVerifySeals(vis map[addr.Address][]proof.SealVerifyInfo) (map[addr.Address][]bool, error) {
	exp := rt.expectBatchVerifySeals
	if exp != nil {
		if len(vis) != len(exp.in) {
			rt.failTest("length mismatch, expected: %v, actual: %v", exp.in, vis)
		}

		for key, value := range exp.in { //nolint:nomaprange
			v, ok := vis[key]
			if !ok {
				rt.failTest("address %v expected but not found", key)
			}

			if len(v) != len(value) {
				rt.failTest("sector info length mismatch for address %v, \n expected: %v, \n actual: %v", key, value, v)
			}
			for i, info := range value {
				if v[i].SealedCID != info.SealedCID {
					rt.failTest("sealed cid does not match for address %v", key)
				}

				if v[i].UnsealedCID != info.UnsealedCID {
					rt.failTest("unsealed cid does not match for address %v", key)
				}
			}

			delete(exp.in, key)
		}

		if len(exp.in) != 0 {
			rt.failTest("addresses in expected map absent in actual: %v", exp.in)
		}
		defer func() {
			rt.expectBatchVerifySeals = nil
		}()
		return exp.out, exp.err
	}
	rt.failTestNow("unexpected syscall to batch verify seals with %v", vis)
	return nil, nil
}

func (rt *Runtime) VerifyPoSt(vi proof.WindowPoStVerifyInfo) error {
	exp := rt.expectVerifyPoSt
	if exp != nil {
		if !reflect.DeepEqual(exp.post, vi) {
			rt.failTest("unexpected PoSt verification\n"+
				"        : %v\n"+
				"expected: %v",
				vi, exp.post)
		}
		defer func() {
			rt.expectVerifyPoSt = nil
		}()
		return exp.result
	}
	rt.failTestNow("unexpected syscall to verify PoSt %v", vi)
	return nil
}

func (rt *Runtime) VerifyConsensusFault(h1, h2, extra []byte) (*runtime.ConsensusFault, error) {
	if rt.expectVerifyConsensusFault == nil {
		rt.failTestNow("Unexpected syscall VerifyConsensusFault")
		return nil, nil
	}

	if rt.expectVerifyConsensusFault.requireCorrectInput {
		if !bytes.Equal(h1, rt.expectVerifyConsensusFault.BlockHeader1) {
			rt.failTest("block header 1 does not equal expected block header 1 (%v != %v)", h1, rt.expectVerifyConsensusFault.BlockHeader1)
		}
		if !bytes.Equal(h2, rt.expectVerifyConsensusFault.BlockHeader2) {
			rt.failTest("block header 2 does not equal expected block header 2 (%v != %v)", h2, rt.expectVerifyConsensusFault.BlockHeader2)
		}
		if !bytes.Equal(extra, rt.expectVerifyConsensusFault.BlockHeaderExtra) {
			rt.failTest("block header extra does not equal expected block header extra (%v != %v)", extra, rt.expectVerifyConsensusFault.BlockHeaderExtra)
		}
	}

	fault := rt.expectVerifyConsensusFault.Fault
	err := rt.expectVerifyConsensusFault.Err
	rt.expectVerifyConsensusFault = nil
	return fault, err
}

func (rt *Runtime) Log(level rt.LogLevel, msg string, args ...interface{}) {
	rt.logs = append(rt.logs, fmt.Sprintf(msg, args...))
}

///// Trace span implementation /////

type TraceSpan struct {
}

func (t TraceSpan) End() {
	// no-op
}

type abort struct {
	code exitcode.ExitCode
	msg  string
}

func (a abort) String() string {
	return fmt.Sprintf("abort(%v): %s", a.code, a.msg)
}

///// Inspection facilities /////

func (rt *Runtime) AdtStore() adt.Store {
	return adt.AsStore(rt)
}

func (rt *Runtime) StateRoot() cid.Cid {
	return rt.state
}

func (rt *Runtime) GetState(o cbor.Unmarshaler) {
	data, found := rt.get(rt.state)
	if !found {
		rt.failTestNow("can't find state at root %v", rt.state) // something internal is messed up
	}
	err := o.UnmarshalCBOR(bytes.NewReader(data))
	if err != nil {
		rt.failTestNow("error loading state: %v", err)
	}
}

func (rt *Runtime) Balance() abi.TokenAmount {
	return rt.balance
}

func (rt *Runtime) Epoch() abi.ChainEpoch {
	return rt.epoch
}

///// Mocking facilities /////

func (rt *Runtime) SetCaller(address addr.Address, actorType cid.Cid) {
	rt.caller = address
	rt.callerType = actorType
	rt.actorCodeCIDs[address] = actorType
}

func (rt *Runtime) SetAddressActorType(address addr.Address, actorType cid.Cid) {
	rt.actorCodeCIDs[address] = actorType
}

func (rt *Runtime) SetBalance(amt abi.TokenAmount) {
	rt.balance = amt
}

func (rt *Runtime) SetReceived(amt abi.TokenAmount) {
	rt.valueReceived = amt
}

func (rt *Runtime) SetNetworkVersion(v network.Version) {
	rt.networkVersion = v
}

func (rt *Runtime) SetEpoch(epoch abi.ChainEpoch) {
	rt.epoch = epoch
}

func (rt *Runtime) ReplaceState(o cbor.Marshaler) {
	rt.state = rt.StorePut(o)
}

func (rt *Runtime) SetCirculatingSupply(amt abi.TokenAmount) {
	rt.circulatingSupply = amt
}

func (rt *Runtime) AddIDAddress(src addr.Address, target addr.Address) {
	rt.require(target.Protocol() == addr.ID, "target must use ID address protocol")
	rt.idAddresses[src] = target
}

func (rt *Runtime) SetNewActorAddress(actAddr addr.Address) {
	rt.require(actAddr.Protocol() == addr.Actor, "new actor address must be protocol: Actor, got protocol: %v", actAddr.Protocol())
	rt.newActorAddr = actAddr
}

func (rt *Runtime) ExpectValidateCallerAny() {
	rt.expectValidateCallerAny = true
}

func (rt *Runtime) ExpectValidateCallerAddr(addrs ...addr.Address) {
	rt.require(len(addrs) > 0, "addrs must be non-empty")
	rt.expectValidateCallerAddr = addrs[:]
}

func (rt *Runtime) ExpectValidateCallerType(types ...cid.Cid) {
	rt.require(len(types) > 0, "types must be non-empty")
	rt.expectValidateCallerType = types[:]
}

func (rt *Runtime) ExpectGetRandomnessBeacon(tag crypto.DomainSeparationTag, epoch abi.ChainEpoch, entropy []byte, out abi.Randomness) {
	rt.expectRandomnessBeacon = append(rt.expectRandomnessBeacon, &expectRandomness{
		tag:     tag,
		epoch:   epoch,
		entropy: entropy,
		out:     out,
	})
}

func (rt *Runtime) ExpectGetRandomnessTickets(tag crypto.DomainSeparationTag, epoch abi.ChainEpoch, entropy []byte, out abi.Randomness) {
	rt.expectRandomnessTickets = append(rt.expectRandomnessTickets, &expectRandomness{
		tag:     tag,
		epoch:   epoch,
		entropy: entropy,
		out:     out,
	})
}

func (rt *Runtime) ExpectSend(toAddr addr.Address, methodNum abi.MethodNum, params cbor.Marshaler, value abi.TokenAmount, ret cbor.Er, exitCode exitcode.ExitCode) {
	// Adapt nil to Empty as convenience for the caller (otherwise we would require non-nil here).
	if ret == nil {
		ret = abi.Empty
	}
	rt.expectSends = append(rt.expectSends, &expectedMessage{
		to:         toAddr,
		method:     methodNum,
		params:     params,
		value:      value,
		sendReturn: ret,
		exitCode:   exitCode,
	})
}

func (rt *Runtime) ExpectVerifySignature(sig crypto.Signature, signer addr.Address, plaintext []byte, result error) {
	rt.expectVerifySigs = append(rt.expectVerifySigs, &expectVerifySig{
		sig:       sig,
		signer:    signer,
		plaintext: plaintext,
		result:    result,
	})
}

func (rt *Runtime) ExpectCreateActor(codeId cid.Cid, address addr.Address) {
	rt.expectCreateActor = &expectCreateActor{
		codeId:  codeId,
		address: address,
	}
}

func (rt *Runtime) ExpectDeleteActor(beneficiary addr.Address) {
	rt.expectDeleteActor = &beneficiary
}

func (rt *Runtime) SetHasher(f func(data []byte) [32]byte) {
	rt.hashfunc = f
}

func (rt *Runtime) ExpectVerifySeal(seal proof.SealVerifyInfo, result error) {
	rt.expectVerifySeal = &expectVerifySeal{
		seal:   seal,
		result: result,
	}
}

func (rt *Runtime) ExpectComputeUnsealedSectorCID(reg abi.RegisteredSealProof, pieces []abi.PieceInfo, cid cid.Cid, err error) {
	rt.expectComputeUnsealedSectorCID = &expectComputeUnsealedSectorCID{
		reg, pieces, cid, err,
	}
}

func (rt *Runtime) ExpectVerifyPoSt(post proof.WindowPoStVerifyInfo, result error) {
	rt.expectVerifyPoSt = &expectVerifyPoSt{
		post:   post,
		result: result,
	}
}

func (rt *Runtime) ExpectVerifyConsensusFault(h1, h2, extra []byte, result *runtime.ConsensusFault, resultErr error) {
	rt.expectVerifyConsensusFault = &expectVerifyConsensusFault{
		requireCorrectInput: true,
		BlockHeader1:        h1,
		BlockHeader2:        h2,
		BlockHeaderExtra:    extra,
		Fault:               result,
		Err:                 resultErr,
	}
}

// Verifies that expected calls were received, and resets all expectations.
func (rt *Runtime) Verify() {
	rt.t.Helper()
	if rt.expectValidateCallerAny {
		rt.failTest("expected ValidateCallerAny, not received")
	}
	if len(rt.expectValidateCallerAddr) > 0 {
		rt.failTest("missing expected ValidateCallerAddr %v", rt.expectValidateCallerAddr)
	}
	if len(rt.expectValidateCallerType) > 0 {
		rt.failTest("missing expected ValidateCallerType %v", rt.expectValidateCallerType)
	}
	if len(rt.expectRandomnessBeacon) > 0 {
		rt.failTest("missing expected beacon randomness %v", rt.expectRandomnessBeacon)
	}
	if len(rt.expectRandomnessTickets) > 0 {
		rt.failTest("missing expected ticket randomness %v", rt.expectRandomnessTickets)
	}
	if len(rt.expectSends) > 0 {
		rt.failTest("missing expected send %v", rt.expectSends)
	}
	if len(rt.expectVerifySigs) > 0 {
		rt.failTest("missing expected verify signature %v", rt.expectVerifySigs)
	}
	if rt.expectCreateActor != nil {
		rt.failTest("missing expected create actor with code %s, address %s",
			rt.expectCreateActor.codeId, rt.expectCreateActor.address)
	}

	if rt.expectVerifySeal != nil {
		rt.failTest("missing expected verify seal with %v", rt.expectVerifySeal.seal)
	}

	if rt.expectBatchVerifySeals != nil {
		rt.failTest("missing expected batch verify seals with %v", rt.expectBatchVerifySeals)
	}

	if rt.expectComputeUnsealedSectorCID != nil {
		rt.failTest("missing expected ComputeUnsealedSectorCID with %v", rt.expectComputeUnsealedSectorCID)
	}

	if rt.expectVerifyConsensusFault != nil {
		rt.failTest("missing expected verify consensus fault")
	}
	if rt.expectDeleteActor != nil {
		rt.failTest("missing expected delete actor with address %s", rt.expectDeleteActor.String())
	}

	rt.Reset()
}

// Resets expectations
func (rt *Runtime) Reset() {
	rt.expectValidateCallerAny = false
	rt.expectValidateCallerAddr = nil
	rt.expectValidateCallerType = nil
	rt.expectRandomnessBeacon = nil
	rt.expectRandomnessTickets = nil
	rt.expectSends = nil
	rt.expectCreateActor = nil
	rt.expectVerifySigs = nil
	rt.expectVerifySeal = nil
	rt.expectBatchVerifySeals = nil
	rt.expectComputeUnsealedSectorCID = nil
}

// Calls f() expecting it to invoke Runtime.Abortf() with a specified exit code.
func (rt *Runtime) ExpectAbort(expected exitcode.ExitCode, f func()) {
	rt.ExpectAbortContainsMessage(expected, "", f)
}

// Calls f() expecting it to invoke Runtime.Abortf() with a specified exit code and message.
func (rt *Runtime) ExpectAbortContainsMessage(expected exitcode.ExitCode, substr string, f func()) {
	rt.t.Helper()
	prevState := rt.state

	defer func() {
		rt.t.Helper()
		r := recover()
		if r == nil {
			rt.failTest("expected abort with code %v but call succeeded", expected)
			return
		}
		a, ok := r.(abort)
		if !ok {
			panic(r)
		}
		if a.code != expected {
			rt.failTest("abort expected code %v, got %v %s", expected, a.code, a.msg)
		}
		if substr != "" {
			if !strings.Contains(a.msg, substr) {
				rt.failTest("abort expected message\n'%s'\nto contain\n'%s'\n", a.msg, substr)
			}
		}
		// Roll back state change.
		rt.state = prevState
	}()
	f()
}

func (rt *Runtime) ExpectAssertionFailure(expected string, f func()) {
	rt.t.Helper()
	prevState := rt.state

	defer func() {
		r := recover()
		if r == nil {
			rt.failTest("expected panic with message %v but call succeeded", expected)
			return
		}
		a, ok := r.(abort)
		if ok {
			rt.failTest("expected panic with message %v but got abort %v", expected, a)
			return
		}
		p, ok := r.(string)
		if !ok {
			panic(r)
		}
		if p != expected {
			rt.failTest("expected panic with message \"%v\" but got message \"%v\"", expected, p)
		}
		// Roll back state change.
		rt.state = prevState
	}()
	f()
}

func (rt *Runtime) ExpectLogsContain(substr string) {
	for _, msg := range rt.logs {
		if strings.Contains(msg, substr) {
			return
		}
	}
	rt.failTest("logs contain %d message(s) and do not contain \"%s\"", len(rt.logs), substr)
}

func (rt *Runtime) ClearLogs() {
	rt.logs = []string{}
}

func (rt *Runtime) ExpectGasCharged(gas int64) {
	if gas != rt.gasCharged {
		rt.failTest("expected gas charged: %d, actual gas charged: %d", gas, rt.gasCharged)
	}
}

func (rt *Runtime) Call(method interface{}, params interface{}) interface{} {
	meth := reflect.ValueOf(method)
	rt.verifyExportedMethodType(meth)

	// There's no panic recovery here. If an abort is expected, this call will be inside an ExpectAbort block.
	// If not expected, the panic will escape and cause the test to fail.

	rt.inCall = true
	rt.stateUsedObjs = map[cbor.Marshaler]cid.Cid{}
	defer func() {
		rt.inCall = false
		rt.stateUsedObjs = nil
	}()
	var arg reflect.Value
	if params != nil {
		arg = reflect.ValueOf(params)
	} else {
		arg = reflect.ValueOf(abi.Empty)
	}
	ret := meth.Call([]reflect.Value{reflect.ValueOf(rt), arg})
	rt.checkStateObjectsUnmodified()
	return ret[0].Interface()
}

// Checks that state objects weren't modified outside of transaction.
func (rt *Runtime) checkStateObjectsUnmodified() {
	for obj, expectedKey := range rt.stateUsedObjs { // nolint:nomaprange
		// Recompute the CID of the object and check it's the same as was recorded
		// when the object was loaded.
		finalKey, _, err := ipld.MarshalCBOR(obj)
		if err != nil {
			rt.Abortf(exitcode.SysErrorIllegalActor, "error marshalling state object for validation: %v", err)
		}
		if finalKey != expectedKey {
			rt.Abortf(exitcode.SysErrorIllegalActor, "State mutated outside of transaction scope")
		}
	}
}

func (rt *Runtime) verifyExportedMethodType(meth reflect.Value) {
	rt.t.Helper()
	t := meth.Type()
	rt.require(t.Kind() == reflect.Func, "%v is not a function", meth)
	rt.require(t.NumIn() == 2, "exported method %v must have two parameters, got %v", meth, t.NumIn())
	rt.require(t.In(0) == typeOfRuntimeInterface, "exported method first parameter must be runtime, got %v", t.In(0))
	rt.require(t.In(1).Kind() == reflect.Ptr, "exported method second parameter must be pointer to params, got %v", t.In(1))
	rt.require(t.In(1).Implements(typeOfCborUnmarshaler), "exported method second parameter must be CBOR-unmarshalable params, got %v", t.In(1))
	rt.require(t.NumOut() == 1, "exported method must return a single value")
	rt.require(t.Out(0).Implements(typeOfCborMarshaler), "exported method must return CBOR-marshalable value")
}

func (rt *Runtime) requireInCall() {
	rt.t.Helper()
	rt.require(rt.inCall, "invalid runtime invocation outside of method call")
}

func (rt *Runtime) require(predicate bool, msg string, args ...interface{}) {
	rt.t.Helper()
	if !predicate {
		rt.failTestNow(msg, args...)
	}
}

func (rt *Runtime) failTest(msg string, args ...interface{}) {
	rt.t.Helper()
	rt.t.Logf(msg, args...)
	rt.t.Logf("%s", debug.Stack())
	rt.t.Fail()
}

func (rt *Runtime) failTestNow(msg string, args ...interface{}) {
	rt.t.Helper()
	rt.t.Logf(msg, args...)
	rt.t.Logf("%s", debug.Stack())
	rt.t.FailNow()
}

func (rt *Runtime) ChargeGas(_ string, gas, _ int64) {
	rt.gasCharged += gas
}

func getMethodName(code cid.Cid, num abi.MethodNum) string {
	for _, actor := range exported.BuiltinActors() {
		if actor.Code().Equals(code) {
			exports := actor.Exports()
			if len(exports) <= int(num) {
				return "<invalid>"
			}
			meth := exports[num]
			if meth == nil {
				return "<invalid>"
			}
			name := goruntime.FuncForPC(reflect.ValueOf(meth).Pointer()).Name()
			name = strings.TrimSuffix(name, "-fm")
			lastDot := strings.LastIndexByte(name, '.')
			name = name[lastDot+1:]
			return name
		}
	}
	return "<unknown actor>"
}
