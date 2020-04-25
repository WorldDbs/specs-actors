package nv4

import (
	"context"

	"github.com/filecoin-project/go-state-types/big"
	system0 "github.com/filecoin-project/specs-actors/actors/builtin/system"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	system2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/system"
)

type systemMigrator struct {
}

func (m systemMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ MigrationInfo) (*StateMigrationResult, error) {
	// No change
	var _ = system2.State(system0.State{})
	return &StateMigrationResult{
		NewHead:  head,
		Transfer: big.Zero(),
	}, nil
}
