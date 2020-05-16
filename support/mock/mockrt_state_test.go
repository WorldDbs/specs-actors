package mock

import (
	"context"
	"io"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

type FakeActor struct{}

func (a FakeActor) Exports() []interface{} {
	return []interface{}{
		1: a.Constructor,
		2: a.ReadOnlyState,
		3: a.TransactionState,
		4: a.TransactionStateTwice,
	}
}

func (a FakeActor) Code() cid.Cid {
	builder := cid.V1Builder{Codec: cid.Raw, MhType: mh.IDENTITY}
	c, err := builder.Sum([]byte("fake"))
	if err != nil {
		panic(err)
	}
	return c
}

func (a FakeActor) State() cbor.Er {
	return new(State)
}

type State struct {
	Value int64
}

func (s State) MarshalCBOR(w io.Writer) error {
	cint := cbg.CborInt(s.Value)
	return cint.MarshalCBOR(w)
}

func (s State) UnmarshalCBOR(r io.Reader) error {
	var cint cbg.CborInt
	err := cint.UnmarshalCBOR(r)
	s.Value = int64(cint)
	return err
}

var _ runtime.VMActor = FakeActor{}


func (a FakeActor) Constructor(rt runtime.Runtime, mutate *cbg.CborBool) *abi.EmptyValue {
	st := State{Value: 0}
	rt.StateCreate(&st)
	if *mutate {
		st.Value = 1
	}
	return nil
}

func (a FakeActor) ReadOnlyState(rt runtime.Runtime, mutate *cbg.CborBool) *abi.EmptyValue {
	rt.ValidateImmediateCallerAcceptAny()
	var st State
	rt.StateReadonly(&st)
	if *mutate {
		// Illegal mutation of read-only state
		st.Value = st.Value + 1
	}
	return nil
}

func (a FakeActor) TransactionState(rt runtime.Runtime, mutate *cbg.CborBool) *abi.EmptyValue {
	rt.ValidateImmediateCallerAcceptAny()
	var st State
	rt.StateTransaction(&st, func() {
		st.Value = st.Value + 1
	})
	if *mutate {
		// Illegal mutation of state outside transaction.
		st.Value = st.Value + 1
	}
	return nil
}

func (a FakeActor) TransactionStateTwice(rt runtime.Runtime, mutate *cbg.CborBool) *abi.EmptyValue {
	rt.ValidateImmediateCallerAcceptAny()
	var st State
	rt.StateTransaction(&st, func() {
		st.Value = st.Value + 1
	})
	if *mutate {
		// Illegal mutation of state outside transaction.
		st.Value = st.Value + 1
	}
	rt.StateTransaction(&st, func() {
		if *mutate {
			panic("Can't get here")
		}
	})
	return nil
}


func TestIllegalStateModifications(t *testing.T) {
	actor := FakeActor{}
	receiver := tutil.NewIDAddr(t, 100)
	builder := NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("construction", func(t *testing.T) {
		rt := builder.Build(t)
		mutate := cbg.CborBool(false)
		rt.Call(actor.Constructor, &mutate)
	})

	t.Run("mutation after construction forbidden", func(t *testing.T) {
		rt := builder.Build(t)
		mutate := cbg.CborBool(true)
		rt.ExpectAbort(exitcode.SysErrorIllegalActor, func() {
			rt.Call(actor.Constructor, &mutate)
		})
	})

	t.Run("readonly", func(t *testing.T) {
		rt := builder.Build(t)
		mutate := cbg.CborBool(false)
		rt.Call(actor.Constructor, &mutate)

		rt.ExpectValidateCallerAny()
		rt.Call(actor.ReadOnlyState, &mutate)

		mutate = true
		rt.ExpectValidateCallerAny()
		rt.ExpectAbort(exitcode.SysErrorIllegalActor, func() {
			rt.Call(actor.ReadOnlyState, &mutate)
		})
	})

	t.Run("transaction", func(t *testing.T) {
		rt := builder.Build(t)
		mutate := cbg.CborBool(false)
		rt.Call(actor.Constructor, &mutate)

		rt.ExpectValidateCallerAny()
		rt.Call(actor.TransactionState, &mutate)

		mutate = true
		rt.ExpectValidateCallerAny()
		rt.ExpectAbort(exitcode.SysErrorIllegalActor, func() {
			rt.Call(actor.TransactionState, &mutate)
		})
	})

	t.Run("transaction twice", func(t *testing.T) {
		rt := builder.Build(t)
		mutate := cbg.CborBool(false)
		rt.Call(actor.Constructor, &mutate)

		rt.ExpectValidateCallerAny()
		rt.Call(actor.TransactionStateTwice, &mutate)

		mutate = true
		rt.ExpectValidateCallerAny()
		rt.ExpectAbort(exitcode.SysErrorIllegalActor, func() {
			rt.Call(actor.TransactionStateTwice, &mutate)
		})
	})
}

