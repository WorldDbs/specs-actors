package nv10

import (
	"context"

	multisig2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	builtin3 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	multisig3 "github.com/filecoin-project/specs-actors/v4/actors/builtin/multisig"
)

type multisigMigrator struct{}

func (m multisigMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState multisig2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	pendingTxnsOut, err := migrateHAMTRaw(ctx, store, inState.PendingTxns, builtin3.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	outState := multisig3.State{
		Signers:               inState.Signers,
		NumApprovalsThreshold: inState.NumApprovalsThreshold,
		NextTxnID:             inState.NextTxnID,
		InitialBalance:        inState.InitialBalance,
		StartEpoch:            inState.StartEpoch,
		UnlockDuration:        inState.UnlockDuration,
		PendingTxns:           pendingTxnsOut,
	}
	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m multisigMigrator) migratedCodeCID() cid.Cid {
	return builtin3.MultisigActorCodeID
}
