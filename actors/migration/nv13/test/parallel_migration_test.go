package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	vm4 "github.com/filecoin-project/specs-actors/v4/support/vm"
	"github.com/filecoin-project/specs-actors/v5/actors/migration/nv13"
	adt5 "github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestParallelMigrationCalls(t *testing.T) {
	// Construct simple prior state tree over a synchronized store
	ctx := context.Background()
	log := nv13.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	vm := vm4.NewVMWithSingletons(ctx, t, bs)

	// Run migration
	adtStore := adt5.WrapStore(ctx, cbor.NewCborStore(bs))
	startRoot := vm.StateRoot()
	endRootSerial, err := nv13.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv13.Config{MaxWorkers: 1}, log, nv13.NewMemMigrationCache())
	require.NoError(t, err)

	// Migrate in parallel
	var endRootParallel1, endRootParallel2 cid.Cid
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		var err1 error
		endRootParallel1, err1 = nv13.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv13.Config{MaxWorkers: 2}, log, nv13.NewMemMigrationCache())
		return err1
	})
	grp.Go(func() error {
		var err2 error
		endRootParallel2, err2 = nv13.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv13.Config{MaxWorkers: 2}, log, nv13.NewMemMigrationCache())
		return err2
	})
	require.NoError(t, grp.Wait())
	assert.Equal(t, endRootSerial, endRootParallel1)
	assert.Equal(t, endRootParallel1, endRootParallel2)
}
