package cron

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/cbor"
	cron0 "github.com/filecoin-project/specs-actors/actors/builtin/cron"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
)

// The cron actor is a built-in singleton that sends messages to other registered actors at the end of each epoch.
type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.EpochTick,
	}
}

func (a Actor) Code() cid.Cid {
	return builtin.CronActorCodeID
}

func (a Actor) IsSingleton() bool {
	return true
}

func (a Actor) State() cbor.Er {
	return new(State)
}

var _ runtime.VMActor = Actor{}

//type ConstructorParams struct {
//	Entries []Entry
//}
type ConstructorParams = cron0.ConstructorParams

type EntryParam = cron0.Entry

func (a Actor) Constructor(rt runtime.Runtime, params *ConstructorParams) *abi.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	entries := make([]Entry, len(params.Entries))
	for i, e := range params.Entries {
		entries[i] = Entry(e) // Identical
	}
	rt.StateCreate(ConstructState(entries))
	return nil
}

// Invoked by the system after all other messages in the epoch have been processed.
func (a Actor) EpochTick(rt runtime.Runtime, _ *abi.EmptyValue) *abi.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	var st State
	rt.StateReadonly(&st)
	for _, entry := range st.Entries {
		_ = rt.Send(entry.Receiver, entry.MethodNum, nil, abi.NewTokenAmount(0), &builtin.Discard{})
		// Any error and return value are ignored.
	}

	return nil
}
