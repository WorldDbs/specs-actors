package nv4

import (
	"context"

	"github.com/filecoin-project/go-state-types/big"
	init0 "github.com/filecoin-project/specs-actors/actors/builtin/init"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
)

type initMigrator struct {
}

func (m initMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ MigrationInfo) (*StateMigrationResult, error) {
	var inState init0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	// Migrate address resolution map
	addrMapRoot, err := m.migrateAddrs(ctx, store, inState.AddressMap)
	if err != nil {
		return nil, xerrors.Errorf("migrate addrs: %w", err)
	}

	outState := init2.State{
		AddressMap:  addrMapRoot,
		NextID:      inState.NextID,
		NetworkName: inState.NetworkName,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (m *initMigrator) migrateAddrs(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type (Address) is identical.
	return migrateHAMTRaw(ctx, store, root)
}
