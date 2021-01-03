package nv12

import (
	"context"

	miner3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/miner"
	builtin4 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	miner4 "github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

type minerMigrator struct{}

func (m minerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState miner3.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	outState := miner4.State{
		// No change
		Info:                      inState.Info,
		PreCommitDeposits:         inState.PreCommitDeposits,
		LockedFunds:               inState.LockedFunds,
		VestingFunds:              inState.VestingFunds,
		FeeDebt:                   inState.FeeDebt,
		InitialPledge:             inState.InitialPledge,
		PreCommittedSectors:       inState.PreCommittedSectors,
		PreCommittedSectorsExpiry: inState.PreCommittedSectorsExpiry,
		AllocatedSectors:          inState.AllocatedSectors,
		Sectors:                   inState.Sectors,
		ProvingPeriodStart:        inState.ProvingPeriodStart,
		CurrentDeadline:           inState.CurrentDeadline,
		Deadlines:                 inState.Deadlines,
		EarlyTerminations:         inState.EarlyTerminations,
		// Changed field
		DeadlineCronActive: true,
	}
	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m minerMigrator) migratedCodeCID() cid.Cid {
	return builtin4.StorageMinerActorCodeID
}
