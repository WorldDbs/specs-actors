package nv7

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	power "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type claimsSummary struct {
	committedRawBytes              abi.StoragePower
	committedQABytes               abi.StoragePower
	rawPower                       abi.StoragePower
	qaPower                        abi.StoragePower
	claimsWithSufficientPowerCount int64
}

type PowerMigrator struct{}

func (m PowerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (*StateMigrationResult, error) {
	var inState power.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	claimsSummary, err := m.ComputeClaimsStats(ctx, store, inState.Claims)
	if err != nil {
		return nil, err
	}

	outState := power.State{
		TotalRawBytePower:         claimsSummary.rawPower,
		TotalBytesCommitted:       claimsSummary.committedRawBytes,
		TotalQualityAdjPower:      claimsSummary.qaPower,
		TotalQABytesCommitted:     claimsSummary.committedQABytes,
		TotalPledgeCollateral:     inState.TotalPledgeCollateral,
		ThisEpochRawBytePower:     inState.ThisEpochRawBytePower,
		ThisEpochQualityAdjPower:  inState.ThisEpochQualityAdjPower,
		ThisEpochPledgeCollateral: inState.ThisEpochPledgeCollateral,
		ThisEpochQAPowerSmoothed:  inState.ThisEpochQAPowerSmoothed,
		MinerCount:                inState.MinerCount,
		MinerAboveMinPowerCount:   claimsSummary.claimsWithSufficientPowerCount,
		CronEventQueue:            inState.CronEventQueue,
		FirstCronEpoch:            inState.FirstCronEpoch,
		Claims:                    inState.Claims,
		ProofValidationBatch:      inState.ProofValidationBatch,
	}

	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (a PowerMigrator) ComputeClaimsStats(ctx context.Context, store cbor.IpldStore, claimsRoot cid.Cid) (*claimsSummary, error) {
	claims, err := adt.AsMap(adt.WrapStore(ctx, store), claimsRoot)
	if err != nil {
		return nil, err
	}

	summary := &claimsSummary{
		committedRawBytes:              abi.NewStoragePower(0),
		committedQABytes:               abi.NewStoragePower(0),
		rawPower:                       abi.NewStoragePower(0),
		qaPower:                        abi.NewStoragePower(0),
		claimsWithSufficientPowerCount: 0,
	}
	var claim power.Claim
	err = claims.ForEach(&claim, func(key string) error {
		summary.committedRawBytes = big.Add(summary.committedRawBytes, claim.RawBytePower)
		summary.committedQABytes = big.Add(summary.committedQABytes, claim.QualityAdjPower)

		minPower, err := builtin.ConsensusMinerMinPower(claim.SealProofType)
		if err != nil {
			return err
		}

		if claim.RawBytePower.GreaterThanEqual(minPower) {
			summary.claimsWithSufficientPowerCount += 1
			summary.rawPower = big.Add(summary.rawPower, claim.RawBytePower)
			summary.qaPower = big.Add(summary.qaPower, claim.QualityAdjPower)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return summary, nil
}
