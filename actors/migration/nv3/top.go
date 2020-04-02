package nv3

import (
	"context"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/states"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

//
// State migration for network version 3.
// This is an in-place upgrade that makes functionality changes guarded by checks on the network version,
// and migrates some state.
//

// Loads and migrates the Filecoin state tree from a root, writing the new state tree blocks into the
// same store.
// Returns the new root of the state tree.
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, root cid.Cid, priorEpoch abi.ChainEpoch) (cid.Cid, error) {
	// Set up store and state tree.
	adtStore := adt.WrapStore(ctx, store)
	tree, err := states.LoadTree(adtStore, root)
	if err != nil {
		return cid.Undef, err
	}

	// Iterate all actors in old state root, set new actor states as we go.
	if err = tree.ForEach(func(addr address.Address, actorIn *states.Actor) error {
		migration, found := migrators[actorIn.Code]
		if !found {
			return nil // No migration for this actor type
		}

		headOut, err := migration.MigrateState(ctx, store, actorIn.Head, priorEpoch, addr, tree)
		if err != nil {
			return xerrors.Errorf("state migration error on %s actor at addr %s: %w", builtin.ActorNameByCode(actorIn.Code), addr, err)
		}

		// Write new state root with the migrated state.
		actorOut := states.Actor{
			Code:       actorIn.Code,
			Head:       headOut,
			CallSeqNum: actorIn.CallSeqNum,
			Balance:    actorIn.Balance,
		}

		return tree.SetActor(addr, &actorOut)
	}); err != nil {
		return cid.Undef, err
	}

	return tree.Flush()
}

// Checks that a state tree is internally consistent.
// This performs only very basic checks. Versions in v2 will be much more thorough.
func CheckStateTree(ctx context.Context, store cbor.IpldStore, root cid.Cid, priorEpoch abi.ChainEpoch, expectedBalanceTotal abi.TokenAmount) error {
	adtStore := adt.WrapStore(ctx, store)
	tree, err := states.LoadTree(adtStore, root)
	if err != nil {
		return err
	}

	balanceTotal := big.Zero()
	if err = tree.ForEach(func(addr address.Address, actor *states.Actor) error {
		balanceTotal = big.Add(balanceTotal, actor.Balance)
		return nil
	}); err != nil {
		return err
	}

	if !balanceTotal.Equals(expectedBalanceTotal) {
		return xerrors.Errorf("balance total %v doesn't match expected %v", balanceTotal, expectedBalanceTotal)
	}
	return nil
}
