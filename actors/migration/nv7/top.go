package nv7

import (
	"context"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

// Config parameterizes a state tree migration
type Config struct{}

func DefaultConfig() Config {
	return Config{}
}

type MigrationInfo struct {
	address    address.Address // actor's address
	balance    abi.TokenAmount // actor's balance
	priorEpoch abi.ChainEpoch  // epoch of last state transition prior to migration
}

type StateMigrationResult struct {
	NewHead  cid.Cid
	Transfer abi.TokenAmount
}

type StateMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (result *StateMigrationResult, err error)
}

// Migrates the filecoin state tree starting from the global state tree and upgrading all actor state.
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, stateRootIn cid.Cid, priorEpoch abi.ChainEpoch, _ Config) (cid.Cid, error) {

	// Setup input and output state tree helpers
	adtStore := adt.WrapStore(ctx, store)
	actorsIn, err := states.LoadTree(adtStore, stateRootIn)
	if err != nil {
		return cid.Undef, err
	}

	// Migrate Power actor
	powerActorIn, found, err := actorsIn.GetActor(builtin.StoragePowerActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, xerrors.Errorf("could not find power actor in state")
	}
	powerResult, err := PowerMigrator{}.MigrateState(ctx, store, powerActorIn.Head, MigrationInfo{
		address:    builtin.StoragePowerActorAddr,
		balance:    powerActorIn.Balance,
		priorEpoch: priorEpoch,
	})
	if err != nil {
		return cid.Undef, err
	}
	powerActorOut := states.Actor{
		Code:       builtin.StoragePowerActorCodeID,
		Head:       powerResult.NewHead,
		CallSeqNum: powerActorIn.CallSeqNum,
		Balance:    big.Add(powerActorIn.Balance, powerResult.Transfer),
	}
	err = actorsIn.SetActor(builtin.StoragePowerActorAddr, &powerActorOut)
	if err != nil {
		return cid.Undef, err
	}

	return actorsIn.Flush()
}
