package nv4

import (
	"context"

	"github.com/filecoin-project/go-state-types/big"
	market0 "github.com/filecoin-project/specs-actors/actors/builtin/market"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
)

type marketMigrator struct {
}

func (m marketMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ MigrationInfo) (*StateMigrationResult, error) {
	var inState market0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	proposalsRoot, err := m.migrateProposals(ctx, store, inState.Proposals)
	if err != nil {
		return nil, xerrors.Errorf("proposals: %w", err)
	}

	statesRoot, err := m.migrateStates(ctx, store, inState.States)
	if err != nil {
		return nil, xerrors.Errorf("states: %w", err)
	}

	pendingRoot, err := m.migratePendingProposals(ctx, store, inState.PendingProposals)
	if err != nil {
		return nil, xerrors.Errorf("pending proposals: %w", err)
	}

	escrowRoot, err := m.migrateBalanceTable(ctx, store, inState.EscrowTable)
	if err != nil {
		return nil, xerrors.Errorf("escrow table: %w", err)
	}

	lockedRoot, err := m.migrateBalanceTable(ctx, store, inState.LockedTable)
	if err != nil {
		return nil, xerrors.Errorf("locked table: %w", err)
	}

	dealOpsRoot, err := m.migrateDealOps(ctx, store, inState.DealOpsByEpoch)
	if err != nil {
		return nil, xerrors.Errorf("deal ops by priorEpoch: %w", err)
	}

	outState := market2.State{
		Proposals:                     proposalsRoot,
		States:                        statesRoot,
		PendingProposals:              pendingRoot,
		EscrowTable:                   escrowRoot,
		LockedTable:                   lockedRoot,
		NextID:                        inState.NextID,
		DealOpsByEpoch:                dealOpsRoot,
		LastCron:                      inState.LastCron,
		TotalClientLockedCollateral:   inState.TotalClientLockedCollateral,
		TotalProviderLockedCollateral: inState.TotalProviderLockedCollateral,
		TotalClientStorageFee:         inState.TotalClientStorageFee,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (m *marketMigrator) migrateProposals(_ context.Context, _ cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT and both the key and value type unchanged between v0 and v2.
	// Verify that the value type is identical.
	var _ = market0.DealProposal(market2.DealProposal{})
	return root, nil
}

func (m *marketMigrator) migrateStates(_ context.Context, _ cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT and both the key and value type unchanged between v0 and v2.
	// Verify that the value type is identical.
	var _ = market0.DealState(market2.DealState{})
	return root, nil
}

func (m *marketMigrator) migratePendingProposals(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type is identical.
	var _ = market0.DealProposal(market2.DealProposal{})
	return migrateHAMTRaw(ctx, store, root)
}

func (m *marketMigrator) migrateBalanceTable(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type (abi.TokenAmount) is identical.
	return migrateHAMTRaw(ctx, store, root)
}

func (m *marketMigrator) migrateDealOps(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, at each level, but the final value type (abi.DealID) is identical.
	return migrateHAMTHAMTRaw(ctx, store, root)
}
