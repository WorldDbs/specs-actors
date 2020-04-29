package test_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/actors/builtin/cron"
	initactor "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/builtin/system"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/actors/states"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/actors/migration/nv4"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestParallelMigrationCalls(t *testing.T) {
	// Construct simple v0 state tree over a locking store
	ctx := context.Background()
	syncStore := ipld2.NewSyncADTStore(ctx)
	initializeActor := func(ctx context.Context, t *testing.T, actors *states.Tree, state cbor.Marshaler, code cid.Cid, a addr.Address, balance abi.TokenAmount) {
		stateCID, err := actors.Store.Put(ctx, state)
		require.NoError(t, err)
		actor := &states.Actor{
			Head:    stateCID,
			Code:    code,
			Balance: balance,
		}
		err = actors.SetActor(a, actor)
		require.NoError(t, err)
	}
	actorsIn, err := states.NewTree(syncStore)
	require.NoError(t, err)

	emptyMapCID, err := adt.MakeEmptyMap(syncStore).Root()
	require.NoError(t, err)
	emptyArrayCID, err := adt.MakeEmptyArray(syncStore).Root()
	require.NoError(t, err)
	emptyMultimapCID, err := adt.MakeEmptyMultimap(syncStore).Root()
	require.NoError(t, err)

	initializeActor(ctx, t, actorsIn, &system.State{}, builtin.SystemActorCodeID, builtin.SystemActorAddr, big.Zero())

	initState := initactor.ConstructState(emptyMapCID, "scenarios")
	initializeActor(ctx, t, actorsIn, initState, builtin.InitActorCodeID, builtin.InitActorAddr, big.Zero())

	rewardState := reward.ConstructState(abi.NewStoragePower(0))
	initializeActor(ctx, t, actorsIn, rewardState, builtin.RewardActorCodeID, builtin.RewardActorAddr, builtin.TotalFilecoin)

	cronState := cron.ConstructState(cron.BuiltInEntries())
	initializeActor(ctx, t, actorsIn, cronState, builtin.CronActorCodeID, builtin.CronActorAddr, big.Zero())

	powerState := power.ConstructState(emptyMapCID, emptyMultimapCID)
	initializeActor(ctx, t, actorsIn, powerState, builtin.StoragePowerActorCodeID, builtin.StoragePowerActorAddr, big.Zero())

	marketState := market.ConstructState(emptyArrayCID, emptyMapCID, emptyMultimapCID)
	initializeActor(ctx, t, actorsIn, marketState, builtin.StorageMarketActorCodeID, builtin.StorageMarketActorAddr, big.Zero())

	// this will need to be replaced with the address of a multisig actor for the verified registry to be tested accurately
	verifregRoot, err := addr.NewIDAddress(80)
	require.NoError(t, err)
	initializeActor(ctx, t, actorsIn, &account.State{Address: verifregRoot}, builtin.AccountActorCodeID, verifregRoot, big.Zero())
	vrState := verifreg.ConstructState(emptyMapCID, verifregRoot)
	initializeActor(ctx, t, actorsIn, vrState, builtin.VerifiedRegistryActorCodeID, builtin.VerifiedRegistryActorAddr, big.Zero())

	// burnt funds
	initializeActor(ctx, t, actorsIn, &account.State{Address: builtin.BurntFundsActorAddr}, builtin.AccountActorCodeID, builtin.BurntFundsActorAddr, big.Zero())

	startRoot, err := actorsIn.Flush()
	require.NoError(t, err)

	// Migrate to v2

	endRootSerial, err := nv4.MigrateStateTree(ctx, syncStore, startRoot, abi.ChainEpoch(0), nv4.Config{MaxWorkers: 1})
	require.NoError(t, err)

	// Migrate in parallel
	var endRootParallel1, endRootParallel2 cid.Cid
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		var err1 error
		endRootParallel1, err1 = nv4.MigrateStateTree(ctx, syncStore, startRoot, abi.ChainEpoch(0), nv4.Config{MaxWorkers: 2})
		return err1
	})
	grp.Go(func() error {
		var err2 error
		endRootParallel2, err2 = nv4.MigrateStateTree(ctx, syncStore, startRoot, abi.ChainEpoch(0), nv4.Config{MaxWorkers: 2})
		return err2
	})
	require.NoError(t, grp.Wait())
	assert.Equal(t, endRootSerial, endRootParallel1)
	assert.Equal(t, endRootParallel1, endRootParallel2)
}
