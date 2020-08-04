package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	vm2 "github.com/filecoin-project/specs-actors/v2/support/vm"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/specs-actors/v3/actors/migration/nv10"
)

func TestParallelMigrationCalls(t *testing.T) {
	// Construct simple prior state tree over a synchronized store
	ctx := context.Background()
	log := nv10.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	vm := vm2.NewVMWithSingletons(ctx, t, bs)

	// Run migration
	adtStore := adt2.WrapStore(ctx, cbor.NewCborStore(bs))
	startRoot := vm.StateRoot()
	endRootSerial, err := nv10.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv10.Config{MaxWorkers: 1}, log, nv10.NewMemMigrationCache())
	require.NoError(t, err)

	// Migrate in parallel
	var endRootParallel1, endRootParallel2 cid.Cid
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		var err1 error
		endRootParallel1, err1 = nv10.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv10.Config{MaxWorkers: 2}, log, nv10.NewMemMigrationCache())
		return err1
	})
	grp.Go(func() error {
		var err2 error
		endRootParallel2, err2 = nv10.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv10.Config{MaxWorkers: 2}, log, nv10.NewMemMigrationCache())
		return err2
	})
	require.NoError(t, grp.Wait())
	assert.Equal(t, endRootSerial, endRootParallel1)
	assert.Equal(t, endRootParallel1, endRootParallel2)
}
