package system_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/system"
	"github.com/filecoin-project/specs-actors/v3/support/mock"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, system.Actor{})
}

func TestConstruction(t *testing.T) {
	rt := mock.NewBuilder(builtin.SystemActorAddr).Build(t)
	a := system.Actor{}

	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	rt.SetCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
	rt.Call(a.Constructor, nil)
	rt.Verify()

	var st system.State
	rt.GetState(&st)

	require.Equal(t, system.State{}, st)
}
