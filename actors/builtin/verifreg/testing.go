package verifreg

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type StateSummary struct {
	Verifiers map[addr.Address]DataCap
	Clients   map[addr.Address]DataCap
}

// Checks internal invariants of verified registry state.
func CheckStateInvariants(st *State, store adt.Store) (*StateSummary, *builtin.MessageAccumulator) {
	acc := &builtin.MessageAccumulator{}
	acc.Require(st.RootKey.Protocol() == addr.ID, "root key %v should have ID protocol", st.RootKey)

	// Check verifiers
	allVerifiers := map[addr.Address]DataCap{}
	if verifiers, err := adt.AsMap(store, st.Verifiers); err != nil {
		acc.Addf("error loading verifiers: %v", err)
	} else {
		var vcap abi.StoragePower
		err = verifiers.ForEach(&vcap, func(key string) error {
			verifier, err := addr.NewFromBytes([]byte(key))
			if err != nil {
				return err
			}
			acc.Require(verifier.Protocol() == addr.ID, "verifier %v should have ID protocol", verifier)
			acc.Require(vcap.GreaterThanEqual(big.Zero()), "verifier %v cap %v is negative", verifier, vcap)
			allVerifiers[verifier] = vcap.Copy()
			return nil
		})
		acc.RequireNoError(err, "error iterating verifiers")
	}


	// Check clients
	allClients := map[addr.Address]DataCap{}
	if clients, err := adt.AsMap(store, st.VerifiedClients); err != nil {
		acc.Addf("error loading clients: %v", err)
	} else {
		var ccap abi.StoragePower
		err = clients.ForEach(&ccap, func(key string) error {
			client, err := addr.NewFromBytes([]byte(key))
			if err != nil {
				return err
			}
			acc.Require(client.Protocol() == addr.ID, "client %v should have ID protocol", client)
			acc.Require(ccap.GreaterThanEqual(big.Zero()), "client %v cap %v is negative", client, ccap)
			allClients[client] = ccap.Copy()
			return nil
		})
		acc.RequireNoError(err, "error iterating clients")
	}

	// Check verifiers and clients are disjoint.
	for v := range allVerifiers { //nolint:nomaprange
		_, found := allClients[v]
		acc.Require(!found, "verifier %v is also a client", v)
	}
	// No need to iterate all clients; any overlap must have been one of all verifiers.

	return &StateSummary{
		Verifiers: allVerifiers,
		Clients:   allClients,
	}, acc
}
