package nv4

import (
	"context"

	"github.com/filecoin-project/go-state-types/big"
	cron0 "github.com/filecoin-project/specs-actors/actors/builtin/cron"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	cron2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/cron"
)

type cronMigrator struct {
}

func (m cronMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ MigrationInfo) (*StateMigrationResult, error) {
	var inState cron0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	outState := cron2.State{Entries: make([]cron2.Entry, len(inState.Entries))}
	for i, e := range inState.Entries {
		outState.Entries[i] = cron2.Entry(e) // Identical
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}
