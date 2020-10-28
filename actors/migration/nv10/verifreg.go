package nv10

import (
	"context"

	verifreg2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	verifreg3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/verifreg"
)

type verifregMigrator struct{}

func (m verifregMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState verifreg2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	verifiersCIDOut, err := migrateHAMTRaw(ctx, store, inState.Verifiers, builtin3.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}
	verifiedClientsCIDOut, err := migrateHAMTRaw(ctx, store, inState.VerifiedClients, builtin3.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	outState := verifreg3.State{
		RootKey:         inState.RootKey,
		Verifiers:       verifiersCIDOut,
		VerifiedClients: verifiedClientsCIDOut,
	}

	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m verifregMigrator) migratedCodeCID() cid.Cid {
	return builtin3.VerifiedRegistryActorCodeID
}
