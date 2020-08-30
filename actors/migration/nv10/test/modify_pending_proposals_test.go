package test_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/rt"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	vm2 "github.com/filecoin-project/specs-actors/v2/support/vm"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	exported3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/exported"
	market3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/market"
	miner3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v3/actors/migration/nv10"
	states3 "github.com/filecoin-project/specs-actors/v3/actors/states"
	tutil "github.com/filecoin-project/specs-actors/v3/support/testing"
	vm3 "github.com/filecoin-project/specs-actors/v3/support/vm"
)

func TestUpdatePendingDealsMigration(t *testing.T) {
	ctx := context.Background()
	log := nv10.TestLogger{TB: t}
	v := vm2.NewVMWithSingletons(ctx, t, ipld2.NewSyncBlockStoreInMemory())
	addrs := vm2.CreateAccounts(ctx, t, v, 10, big.Mul(big.NewInt(100_000), vm2.FIL), 93837778)
	worker := addrs[0]

	minerBalance := big.Mul(big.NewInt(10_000), vm2.FIL)

	// create miner
	params := power2.CreateMinerParams{
		Owner:         worker,
		Worker:        worker,
		SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret := vm2.ApplyOk(t, v, addrs[0], builtin2.StoragePowerActorAddr, minerBalance, builtin2.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power2.CreateMinerReturn)
	require.True(t, ok)

	// add market collateral for clients and miner
	for i := 0; i < 9; i++ {
		collateral := big.Mul(big.NewInt(30), vm2.FIL)
		vm2.ApplyOk(t, v, addrs[i+1], builtin2.StorageMarketActorAddr, collateral, builtin2.MethodsMarket.AddBalance, &addrs[i+1])
		collateral = big.Mul(big.NewInt(64), vm2.FIL)
		vm2.ApplyOk(t, v, worker, builtin2.StorageMarketActorAddr, collateral, builtin2.MethodsMarket.AddBalance, &minerAddrs.IDAddress)
	}

	// create 100 deals over 100 epochs to fill pending proposals structure
	dealStart := abi.ChainEpoch(252)
	var deals []*market2.PublishStorageDealsReturn
	for i := 0; i < 100; i++ {
		var err error
		v, err = v.WithEpoch(v.GetEpoch() + 1)
		require.NoError(t, err)
		deals = append(deals, publishDeal(t, v, worker, addrs[1+i%9], minerAddrs.IDAddress, fmt.Sprintf("deal%d", i),
			1<<26, false, dealStart, 210*builtin2.EpochsInDay))
	}

	// run migration
	nextRoot, err := nv10.MigrateStateTree(ctx, v.Store(), v.StateRoot(), v.GetEpoch(), nv10.Config{MaxWorkers: 1}, log, nv10.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported3.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v3, err := vm3.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// add 10 more deals after migration
	for i := 0; i < 10; i++ {
		var err error
		v3, err = v3.WithEpoch(v.GetEpoch() + 1)
		require.NoError(t, err)

		deals = append(deals, publishV3Deal(t, v3, worker, addrs[1+i%9], minerAddrs.IDAddress, fmt.Sprintf("deal1%d", i),
			1<<26, false, dealStart, 210*builtin3.EpochsInDay))
	}

	// add even deals to a sector we will commit (to let the odd deals expire)
	var dealIDs []abi.DealID
	for i := 0; i < len(deals); i += 2 {
		dealIDs = append(dealIDs, deals[i].IDs[0])
	}

	// precommit capacity upgrade sector with deals
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner3.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	preCommitParams := miner3.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       dealIDs,
		Expiration:    v.GetEpoch() + 220*builtin3.EpochsInDay,
	}
	vm3.ApplyOk(t, v3, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin3.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to min seal duration
	proveTime := v3.GetEpoch() + miner3.PreCommitChallengeDelay + 1
	v3, _ = vm3.AdvanceByDeadlineTillEpoch(t, v3, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v3, err = v3.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner3.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm3.ApplyOk(t, v3, worker, minerAddrs.RobustAddress, big.Zero(), builtin3.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	// This activates deals hitting the pending deals structure.
	vm3.ApplyOk(t, v3, builtin3.SystemActorAddr, builtin3.CronActorAddr, big.Zero(), builtin3.MethodsCron.EpochTick, nil)

	// Run cron after deal start to expire pending deals
	v3, err = v3.WithEpoch(dealStart + 1)
	require.NoError(t, err)
	vm3.ApplyOk(t, v3, builtin3.SystemActorAddr, builtin3.CronActorAddr, big.Zero(), builtin3.MethodsCron.EpochTick, nil)

	// Assert that no invariants are broken. CheckStateInvariants used to assert that Pending Proposal value cid
	// matched its key, so this also checks that the market invariants have been corrected.
	stateTree, err := v3.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v3.GetTotalActorBalance()
	require.NoError(t, err)
	msgs, err := states3.CheckStateInvariants(stateTree, totalBalance, v3.GetEpoch())
	require.NoError(t, err)

	assert.Equal(t, 0, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))

}

func publishDeal(t *testing.T, v *vm2.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market2.PublishStorageDealsReturn {
	deal := market2.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market2.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm2.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm2.FIL),
	}

	publishDealParams := market2.PublishStorageDealsParams{
		Deals: []market2.ClientDealProposal{{
			Proposal:        deal,
			ClientSignature: crypto.Signature{},
		}},
	}
	ret, code := v.ApplyMessage(provider, builtin2.StorageMarketActorAddr, big.Zero(), builtin2.MethodsMarket.PublishStorageDeals, &publishDealParams)
	require.Equal(t, exitcode.Ok, code)

	expectedPublishSubinvocations := []vm2.ExpectInvocation{
		{To: minerID, Method: builtin2.MethodsMiner.ControlAddresses, SubInvocations: []vm2.ExpectInvocation{}},
		{To: builtin2.RewardActorAddr, Method: builtin2.MethodsReward.ThisEpochReward, SubInvocations: []vm2.ExpectInvocation{}},
		{To: builtin2.StoragePowerActorAddr, Method: builtin2.MethodsPower.CurrentTotalPower, SubInvocations: []vm2.ExpectInvocation{}},
	}

	vm2.ExpectInvocation{
		To:             builtin2.StorageMarketActorAddr,
		Method:         builtin2.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return ret.(*market2.PublishStorageDealsReturn)
}

func publishV3Deal(t *testing.T, v *vm3.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market2.PublishStorageDealsReturn {
	deal := market3.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market2.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm2.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm2.FIL),
	}

	publishDealParams := market3.PublishStorageDealsParams{
		Deals: []market3.ClientDealProposal{{
			Proposal:        deal,
			ClientSignature: crypto.Signature{},
		}},
	}
	ret, code := v.ApplyMessage(provider, builtin3.StorageMarketActorAddr, big.Zero(), builtin3.MethodsMarket.PublishStorageDeals, &publishDealParams)
	require.Equal(t, exitcode.Ok, code)

	expectedPublishSubinvocations := []vm3.ExpectInvocation{
		{To: minerID, Method: builtin3.MethodsMiner.ControlAddresses, SubInvocations: []vm3.ExpectInvocation{}},
		{To: builtin3.RewardActorAddr, Method: builtin3.MethodsReward.ThisEpochReward, SubInvocations: []vm3.ExpectInvocation{}},
		{To: builtin3.StoragePowerActorAddr, Method: builtin3.MethodsPower.CurrentTotalPower, SubInvocations: []vm3.ExpectInvocation{}},
	}

	vm3.ExpectInvocation{
		To:             builtin3.StorageMarketActorAddr,
		Method:         builtin3.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return ret.(*market2.PublishStorageDealsReturn)
}
