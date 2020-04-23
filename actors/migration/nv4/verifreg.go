package nv4

import (
	"context"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg0 "github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	account2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/account"
	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	verifreg2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type verifregMigrator struct {
	actorsOut *states.Tree
}

func (m verifregMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ MigrationInfo) (*StateMigrationResult, error) {
	var inState verifreg0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	verifiersRoot, err := m.migrateCapTable(ctx, store, inState.Verifiers)
	if err != nil {
		return nil, xerrors.Errorf("verifiers cap table: %w", err)
	}

	clientsRoot, err := m.migrateCapTable(ctx, store, inState.VerifiedClients)
	if err != nil {
		return nil, xerrors.Errorf("clients cap table: %w", err)
	}

	if inState.RootKey.Protocol() != addr.ID {
		return nil, xerrors.Errorf("unexpected non-ID root key address %v", inState.RootKey)
	}

	outState := verifreg2.State{
		RootKey:         inState.RootKey,
		Verifiers:       verifiersRoot,
		VerifiedClients: clientsRoot,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (m *verifregMigrator) migrateCapTable(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type (big.Int) is identical.
	// The keys must be normalized to ID-addresses.
	inMap, err := adt0.AsMap(adt0.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outMap := adt2.MakeEmptyMap(adt2.WrapStore(ctx, store))

	outInit, found, err := m.actorsOut.GetActor(builtin2.InitActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, xerrors.Errorf("could not find init actor in input state")
	}
	var outInitState init2.State
	err = store.Get(ctx, outInit.Head, &outInitState)
	if err != nil {
		return cid.Undef, err
	}

	var inCap verifreg0.DataCap
	if err = inMap.ForEach(&inCap, func(key string) error {
		inAddr, err := addr.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		idInAddr, err := m.resolveAndMaybeCreateAccount(ctx, inAddr, adt2.WrapStore(ctx, store), &outInitState)
		if err != nil {
			return xerrors.Errorf("resolving address %s: %w", inAddr, err)
		}
		outAddr := idInAddr
		outCap := verifreg2.DataCap(inCap) // Identical
		return outMap.Put(abi.AddrKey(outAddr), &outCap)
	}); err != nil {
		return cid.Undef, err
	}

	// Flush the out init state up to the top of the state tree
	// store new state
	outInitHead, err := store.Put(ctx, &outInitState)
	if err != nil {
		return cid.Undef, err
	}
	// update init actor
	outInit.Head = outInitHead
	if err := m.actorsOut.SetActor(builtin2.InitActorAddr, outInit); err != nil {
		return cid.Undef, err
	}

	return outMap.Root()
}

// Resolve the given address.  If it does not yet exist and is signable, create
// an account actor and map an init address in the new init actor state.
func (m *verifregMigrator) resolveAndMaybeCreateAccount(ctx context.Context, inAddr addr.Address, store adt2.Store, outInitState *init2.State) (addr.Address, error) {
	// check if this already exists in init actor state
	idInAddr, found, err := outInitState.ResolveAddress(store, inAddr)
	if err != nil {
		return addr.Undef, xerrors.Errorf("init resolve: %w", err)
	}
	if found {
		return idInAddr, nil
	}
	// must create new id mapping
	if inAddr.Protocol() != addr.SECP256K1 && inAddr.Protocol() != addr.BLS {
		// Don't implicitly create an account actor for an address without an associated key.
		return addr.Undef, xerrors.Errorf("addr %v is not signable, cannot create account", inAddr)
	}

	idInAddr, err = outInitState.MapAddressToNewID(store, inAddr)
	if err != nil {
		return addr.Undef, xerrors.Errorf("init map: %w", err)
	}

	// and new account
	acctState := account2.State{Address: inAddr}
	acctCid, err := store.Put(ctx, &acctState)
	if err != nil {
		return addr.Undef, err
	}
	acctActor := states.Actor{
		Code:    builtin2.AccountActorCodeID,
		Balance: abi.NewTokenAmount(0),
		Head:    acctCid,
	}

	err = m.actorsOut.SetActor(idInAddr, &acctActor)
	return idInAddr, err
}
