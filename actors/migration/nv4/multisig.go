package nv4

import (
	"context"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	multisig0 "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	multisig2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
)

type multisigMigrator struct {
}

func (m multisigMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ MigrationInfo) (*StateMigrationResult, error) {
	var inState multisig0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	// Migrate pending txns map
	pendingRoot, err := m.migratePending(ctx, store, inState.PendingTxns)
	if err != nil {
		return nil, xerrors.Errorf("pending: %w", err)
	}

	// Verify signers are all ID addrs
	for _, signer := range inState.Signers {
		if signer.Protocol() != addr.ID {
			return nil, xerrors.Errorf("unexpected non-ID signer address %s", signer)
		}
	}

	outState := multisig2.State{
		Signers:               inState.Signers,
		NumApprovalsThreshold: inState.NumApprovalsThreshold,
		NextTxnID:             inState.NextTxnID,
		InitialBalance:        inState.InitialBalance,
		StartEpoch:            inState.StartEpoch,
		UnlockDuration:        inState.UnlockDuration,
		PendingTxns:           pendingRoot,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (m *multisigMigrator) migratePending(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type is identical.
	var _ = multisig2.Transaction(multisig0.Transaction{})

	return migrateHAMTRaw(ctx, store, root)
}
