package cron_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/cron"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, cron.Actor{})
}

func TestConstructor(t *testing.T) {
	actor := cronHarness{cron.Actor{}, t}

	receiver := tutil.NewIDAddr(t, 100)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("construct with empty entries", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)

		var st cron.State
		rt.GetState(&st)
		var nilCronEntries = []cron.Entry(nil)
		assert.Equal(t, nilCronEntries, st.Entries)

		actor.checkState(rt)
	})

	t.Run("construct with non-empty entries", func(t *testing.T) {
		rt := builder.Build(t)

		var entryParams = []cron.EntryParam{
			{Receiver: tutil.NewIDAddr(t, 1001), MethodNum: abi.MethodNum(1001)},
			{Receiver: tutil.NewIDAddr(t, 1002), MethodNum: abi.MethodNum(1002)},
			{Receiver: tutil.NewIDAddr(t, 1003), MethodNum: abi.MethodNum(1003)},
			{Receiver: tutil.NewIDAddr(t, 1004), MethodNum: abi.MethodNum(1004)},
		}
		actor.constructAndVerify(rt, entryParams...)

		var st cron.State
		rt.GetState(&st)
		expectedEntries := make([]cron.Entry, len(entryParams))
		for i, e := range entryParams {
			expectedEntries[i] = cron.Entry(e)
		}
		assert.Equal(t, expectedEntries, st.Entries)

		actor.checkState(rt)
	})
}

func TestEpochTick(t *testing.T) {
	actor := cronHarness{cron.Actor{}, t}

	receiver := tutil.NewIDAddr(t, 100)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("epoch tick with empty entries", func(t *testing.T) {
		rt := builder.Build(t)

		var nilCronEntries = []cron.EntryParam(nil)
		actor.constructAndVerify(rt, nilCronEntries...)
		actor.epochTickAndVerify(rt)
		actor.checkState(rt)
	})

	t.Run("epoch tick with non-empty entries", func(t *testing.T) {
		rt := builder.Build(t)

		entry1 := cron.EntryParam{Receiver: tutil.NewIDAddr(t, 1001), MethodNum: abi.MethodNum(1001)}
		entry2 := cron.EntryParam{Receiver: tutil.NewIDAddr(t, 1002), MethodNum: abi.MethodNum(1002)}
		entry3 := cron.EntryParam{Receiver: tutil.NewIDAddr(t, 1003), MethodNum: abi.MethodNum(1003)}
		entry4 := cron.EntryParam{Receiver: tutil.NewIDAddr(t, 1004), MethodNum: abi.MethodNum(1004)}

		actor.constructAndVerify(rt, entry1, entry2, entry3, entry4)
		// exit code should not matter
		rt.ExpectSend(entry1.Receiver, entry1.MethodNum, nil, big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(entry2.Receiver, entry2.MethodNum, nil, big.Zero(), nil, exitcode.ErrIllegalArgument)
		rt.ExpectSend(entry3.Receiver, entry3.MethodNum, nil, big.Zero(), nil, exitcode.ErrInsufficientFunds)
		rt.ExpectSend(entry4.Receiver, entry4.MethodNum, nil, big.Zero(), nil, exitcode.ErrForbidden)
		actor.epochTickAndVerify(rt)

		actor.checkState(rt)
	})

	t.Run("built-in entries", func(t *testing.T) {
		bie := cron.BuiltInEntries()
		assert.True(t, len(bie) > 0)
	})
}

type cronHarness struct {
	cron.Actor
	t testing.TB
}

func (h *cronHarness) constructAndVerify(rt *mock.Runtime, entries ...cron.EntryParam) {
	params := cron.ConstructorParams{Entries: entries}
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, &params)
	assert.Nil(h.t, ret)
	rt.Verify()
}

func (h *cronHarness) epochTickAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.EpochTick, nil)
	assert.Nil(h.t, ret)
	rt.Verify()
}

func (h *cronHarness) checkState(rt *mock.Runtime) {
	var st cron.State
	rt.GetState(&st)
	_, msgs := cron.CheckStateInvariants(&st, rt.AdtStore())
	assert.True(h.t, msgs.IsEmpty())
}
