package account

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
)

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		1: a.Constructor,
		2: a.PubkeyAddress,
	}
}

func (a Actor) Code() cid.Cid {
	return builtin.AccountActorCodeID
}

func (a Actor) State() cbor.Er {
	return new(State)
}

var _ runtime.VMActor = Actor{}

type State struct {
	Address addr.Address
}

func (a Actor) Constructor(rt runtime.Runtime, address *addr.Address) *abi.EmptyValue {
	// Account actors are created implicitly by sending a message to a pubkey-style address.
	// This constructor is not invoked by the InitActor, but by the system.
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	switch address.Protocol() {
	case addr.SECP256K1:
	case addr.BLS:
		break // ok
	default:
		rt.Abortf(exitcode.ErrIllegalArgument, "address must use BLS or SECP protocol, got %v", address.Protocol())
	}
	st := State{Address: *address}
	rt.StateCreate(&st)
	return nil
}

// Fetches the pubkey-type address from this actor.
func (a Actor) PubkeyAddress(rt runtime.Runtime, _ *abi.EmptyValue) *addr.Address {
	rt.ValidateImmediateCallerAcceptAny()
	var st State
	rt.StateReadonly(&st)
	return &st.Address
}
