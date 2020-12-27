package nv10

import (
	"context"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"

	builtin3 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	market3 "github.com/filecoin-project/specs-actors/v4/actors/builtin/market"
	adt3 "github.com/filecoin-project/specs-actors/v4/actors/util/adt"
)

type marketMigrator struct{}

func (m marketMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState market2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	pendingProposalsCidOut, err := m.MapPendingProposals(ctx, store, inState.PendingProposals)
	if err != nil {
		return nil, err
	}
	proposalsCidOut, err := migrateAMTRaw(ctx, store, inState.Proposals, market3.ProposalsAmtBitwidth)
	if err != nil {
		return nil, err
	}
	statesCidOut, err := migrateAMTRaw(ctx, store, inState.States, market3.StatesAmtBitwidth)
	if err != nil {
		return nil, err
	}
	escrowTableCidOut, err := migrateHAMTRaw(ctx, store, inState.EscrowTable, adt3.BalanceTableBitwidth)
	if err != nil {
		return nil, err
	}
	lockedTableCidOut, err := migrateHAMTRaw(ctx, store, inState.LockedTable, adt3.BalanceTableBitwidth)
	if err != nil {
		return nil, err
	}
	dobeCidOut, err := migrateHAMTHAMTRaw(ctx, store, inState.DealOpsByEpoch, builtin3.DefaultHamtBitwidth, builtin3.DefaultHamtBitwidth)
	if err != nil {
		return nil, err
	}

	outState := market3.State{
		Proposals:                     proposalsCidOut,
		States:                        statesCidOut,
		PendingProposals:              pendingProposalsCidOut,
		EscrowTable:                   escrowTableCidOut,
		LockedTable:                   lockedTableCidOut,
		NextID:                        inState.NextID,
		DealOpsByEpoch:                dobeCidOut,
		LastCron:                      inState.LastCron,
		TotalClientLockedCollateral:   inState.TotalClientLockedCollateral,
		TotalProviderLockedCollateral: inState.TotalProviderLockedCollateral,
		TotalClientStorageFee:         inState.TotalClientStorageFee,
	}

	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m marketMigrator) migratedCodeCID() cid.Cid {
	return builtin3.StorageMarketActorCodeID
}

func (a marketMigrator) MapPendingProposals(ctx context.Context, store cbor.IpldStore, pendingProposalsRoot cid.Cid) (cid.Cid, error) {
	oldPendingProposals, err := adt2.AsMap(adt2.WrapStore(ctx, store), pendingProposalsRoot)
	if err != nil {
		return cid.Undef, err
	}

	newPendingProposals, err := adt3.MakeEmptySet(adt3.WrapStore(ctx, store), builtin3.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	err = oldPendingProposals.ForEach(nil, func(key string) error {
		return newPendingProposals.Put(StringKey(key))
	})
	if err != nil {
		return cid.Undef, err
	}

	newPendingProposalsCid, err := newPendingProposals.Root()
	if err != nil {
		return cid.Undef, err
	}

	return newPendingProposalsCid, nil
}

// An adt.Map key that just preserves the underlying string.
type StringKey string

func (k StringKey) Key() string {
	return string(k)
}
