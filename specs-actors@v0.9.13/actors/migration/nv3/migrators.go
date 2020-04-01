package nv3

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/states"
)

type StateMigration interface {
	// Loads an actor's state from an input store at `head` and writes new state to an output store.
	// The migration is assumed to be running after the end of `priorEpoch` (the epoch can affect the migration logic).
	// The actor's address is `addr` and the state `tree` provides access to other actor states.
	// Returns the new state head CID.
	MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, priorEpoch abi.ChainEpoch, addr address.Address, tree *states.Tree) (newHead cid.Cid, err error)
}

var migrators = map[cid.Cid]StateMigration{ // nolint:varcheck,deadcode,unused
	builtin.RewardActorCodeID:       &rewardMigrator{},
	builtin.StorageMinerActorCodeID: &minerMigrator{},
	//builtin.AccountActorCodeID: nil,
	//builtin.CronActorCodeID: nil,
	//builtin.InitActorCodeID: nil,
	//builtin.MultisigActorCodeID:nil,
	//builtin.PaymentChannelActorCodeID: nil,
	//builtin.StorageMarketActorCodeID: nil,
	//builtin.StoragePowerActorCodeID: nil,
	//builtin.SystemActorCodeID: nil,
	//builtin.VerifiedRegistryActorCodeID: nil,
}
