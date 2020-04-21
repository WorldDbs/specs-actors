package nv4

import (
	"context"

	"github.com/filecoin-project/go-state-types/big"
	account0 "github.com/filecoin-project/specs-actors/actors/builtin/account"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	account2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/account"
)

type accountMigrator struct {
}

func (m accountMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ MigrationInfo) (*StateMigrationResult, error) {
	var inState account0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	outState := account2.State(inState) // Identical
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}
