package nv10

import (
	"context"

	paych2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/paych"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	paych3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/paych"
)

type paychMigrator struct{}

func (m paychMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState paych2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	laneStatesOut, err := migrateAMTRaw(ctx, store, inState.LaneStates, paych3.LaneStatesAmtBitwidth)
	if err != nil {
		return nil, err
	}

	outState := paych3.State{
		From:            inState.From,
		To:              inState.To,
		ToSend:          inState.ToSend,
		SettlingAt:      inState.SettlingAt,
		MinSettleHeight: inState.MinSettleHeight,
		LaneStates:      laneStatesOut,
	}
	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m paychMigrator) migratedCodeCID() cid.Cid {
	return builtin3.PaymentChannelActorCodeID
}
