package nv10

import (
	"context"

	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	builtin3 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	init3 "github.com/filecoin-project/specs-actors/v4/actors/builtin/init"
)

type initMigrator struct{}

func (m initMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState init2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	addressMapOut, err := migrateHAMTRaw(ctx, store, inState.AddressMap, builtin3.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	outState := init3.State{
		AddressMap:  addressMapOut,
		NextID:      inState.NextID,
		NetworkName: inState.NetworkName,
	}
	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m initMigrator) migratedCodeCID() cid.Cid {
	return builtin3.InitActorCodeID
}
