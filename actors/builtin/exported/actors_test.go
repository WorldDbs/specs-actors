package exported

import (
	"reflect"
	goruntime "runtime"
	"strings"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/cron"
	init_ "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/system"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	"github.com/ipfs/go-cid"

	"github.com/stretchr/testify/require"
)

func TestKnownActors(t *testing.T) {
	// Test all known actors. This ensures we:
	// * Export all the right actors.
	// * Don't get any method mismatches.

	// We can't test this in the builtin package due to cyclic imports, so
	// we test it here.
	builtins := BuiltinActors()
	actorInfos := []struct {
		actor   runtime.VMActor
		code    cid.Cid
		methods interface{}
	}{
		{account.Actor{}, builtin.AccountActorCodeID, builtin.MethodsAccount},
		{cron.Actor{}, builtin.CronActorCodeID, builtin.MethodsCron},
		{init_.Actor{}, builtin.InitActorCodeID, builtin.MethodsInit},
		{market.Actor{}, builtin.StorageMarketActorCodeID, builtin.MethodsMarket},
		{miner.Actor{}, builtin.StorageMinerActorCodeID, builtin.MethodsMiner},
		{multisig.Actor{}, builtin.MultisigActorCodeID, builtin.MethodsMultisig},
		{paych.Actor{}, builtin.PaymentChannelActorCodeID, builtin.MethodsPaych},
		{power.Actor{}, builtin.StoragePowerActorCodeID, builtin.MethodsPower},
		{reward.Actor{}, builtin.RewardActorCodeID, builtin.MethodsReward},
		{system.Actor{}, builtin.SystemActorCodeID, nil},
		{verifreg.Actor{}, builtin.VerifiedRegistryActorCodeID, builtin.MethodsVerifiedRegistry},
	}
	require.Equal(t, len(builtins), len(actorInfos))
	for i, info := range actorInfos {
		// check exported actors.
		require.Equal(t, info.actor, builtins[i])

		// check codes.
		require.Equal(t, info.code, info.actor.Code())

		// check methods.
		exports := info.actor.Exports()
		if info.methods == nil {
			continue
		}
		methodsVal := reflect.ValueOf(info.methods)
		methodsTyp := methodsVal.Type()
		require.Equal(t, len(exports)-1, methodsVal.NumField())
		require.Nil(t, exports[0]) // send.
		for i, m := range exports {
			if i == 0 {
				// send
				require.Nil(t, m)
				continue
			}
			expectedVal := methodsVal.Field(i - 1)
			expectedName := methodsTyp.Field(i - 1).Name

			require.Equal(t, expectedVal.Interface().(abi.MethodNum), abi.MethodNum(i))

			if m == nil {
				// not send, must be deprecated.
				require.True(t, strings.HasPrefix(expectedName, "Deprecated"))
				continue
			}

			name := goruntime.FuncForPC(reflect.ValueOf(m).Pointer()).Name()
			name = strings.TrimSuffix(name, "-fm")
			lastDot := strings.LastIndexByte(name, '.')
			name = name[lastDot+1:]
			require.Equal(t, expectedName, name)
		}
	}
}
