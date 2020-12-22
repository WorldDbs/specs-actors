package test_test

import (
	"context"
	"strings"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/rt"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	power3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/power"
	vm3 "github.com/filecoin-project/specs-actors/v3/support/vm"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	builtin "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	exported "github.com/filecoin-project/specs-actors/v4/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v4/actors/migration/nv12"
	"github.com/filecoin-project/specs-actors/v4/actors/states"
	vm "github.com/filecoin-project/specs-actors/v4/support/vm"
)

func TestEmptyMinersStopCronAfterMigration(t *testing.T) {
	ctx := context.Background()
	log := nv12.TestLogger{TB: t}
	v := vm3.NewVMWithSingletons(ctx, t, ipld2.NewSyncBlockStoreInMemory())
	addrs := vm3.CreateAccounts(ctx, t, v, 110, big.Mul(big.NewInt(100_000), vm3.FIL), 93837779)

	// create empty miners
	minerAddrs := make([]address.Address, 100)
	for i := 0; i < 100; i++ {
		worker := addrs[i]
		minerBalance := big.Mul(big.NewInt(10_000), vm3.FIL)

		params := power3.CreateMinerParams{
			Owner:               worker,
			Worker:              worker,
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			Peer:                abi.PeerID("fake peer id"),
		}
		ret := vm3.ApplyOk(t, v, worker, builtin3.StoragePowerActorAddr, minerBalance, builtin3.MethodsPower.CreateMiner, &params)
		createRet, ok := ret.(*power3.CreateMinerReturn)
		require.True(t, ok)
		minerAddrs[i] = createRet.IDAddress
	}
	// run network for a few proving periods
	stop := v.GetEpoch() + abi.ChainEpoch(10_000)
	v = AdvanceToEpochWithCronV3(t, v, stop)

	// migrate
	nextRoot, err := nv12.MigrateStateTree(ctx, v.Store(), v.StateRoot(), v.GetEpoch(), nv12.Config{MaxWorkers: 1}, log, nv12.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}
	v4, err := vm.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch())
	require.NoError(t, err)

	// check that all miners are cronning
	stateTree, err := v4.GetStateTree()
	require.NoError(t, err)
	err = stateTree.ForEach(func(addr address.Address, act *states.Actor) error {
		if act.Code.Equals(builtin.StorageMinerActorCodeID) {
			var mSt miner.State
			err := v4.GetState(addr, &mSt)
			require.NoError(t, err)
			assert.True(t, mSt.DeadlineCronActive)
		}
		return nil
	})
	require.NoError(t, err)

	// empty miners stop cronning within 1 proving period
	v4 = AdvanceToEpochWithCron(t, v4, v4.GetEpoch()+miner.WPoStProvingPeriod)

	// check invariants
	stateTree, err = v4.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v4.GetTotalActorBalance()
	require.NoError(t, err)
	msgs, err := states.CheckStateInvariants(stateTree, totalBalance, v4.GetEpoch()-1)
	require.NoError(t, err)

	assert.Equal(t, 0, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))

	// check that no miners are cronning
	err = stateTree.ForEach(func(addr address.Address, act *states.Actor) error {
		if act.Code.Equals(builtin.StorageMinerActorCodeID) {
			var mSt miner.State
			err := v4.GetState(addr, &mSt)
			require.NoError(t, err)
			assert.False(t, mSt.DeadlineCronActive)
		}
		return nil
	})
	require.NoError(t, err)
}

// Advances to given epoch running cron for all epochs up to but not including this epoch.
// This utility didn't exist in v3 vm utils so copying here after the fact to handle migrations.
func AdvanceToEpochWithCronV3(t *testing.T, v *vm3.VM, stop abi.ChainEpoch) *vm3.VM {
	currEpoch := v.GetEpoch()
	var err error
	for currEpoch < stop {
		_, code := v.ApplyMessage(builtin3.SystemActorAddr, builtin3.CronActorAddr, big.Zero(), builtin3.MethodsCron.EpochTick, nil)
		require.Equal(t, exitcode.Ok, code)
		currEpoch += 1
		v, err = v.WithEpoch(currEpoch)
		require.NoError(t, err)
	}
	return v
}

// Advances to given epoch running cron for all epochs up to but not including this epoch.
func AdvanceToEpochWithCron(t *testing.T, v *vm.VM, stop abi.ChainEpoch) *vm.VM {
	currEpoch := v.GetEpoch()
	var err error
	for currEpoch < stop {
		_, code := v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
		require.Equal(t, exitcode.Ok, code)
		currEpoch += 1
		v, err = v.WithEpoch(currEpoch)
		require.NoError(t, err)
	}
	return v
}
