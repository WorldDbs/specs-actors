package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v4/actors/builtin"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v4/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v4/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v4/support/testing"
	vm "github.com/filecoin-project/specs-actors/v4/support/vm"
)

func TestReplaceCommittedCapacitySectorWithDealLadenSector(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, verifier, unverifiedClient, verifiedClient := addrs[0], addrs[1], addrs[2], addrs[3]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	// create miner
	params := power.CreateMinerParams{
		Owner:                worker,
		Worker:               worker,
		WindowPoStProofType:  abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                 abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// Precommit, prove and PoSt empty sector (more fully tested in TestCommitPoStFlow)
	//

	// precommit sector
	preCommitParams := miner.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       nil,
		Expiration:    v.GetEpoch() + 200*builtin.EpochsInDay,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	submitParams := miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte("not really random"),
	}

	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	// check power table
	sectorPower := vm.PowerForMinerSector(t, v, minerAddrs.IDAddress, sectorNumber)
	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats := vm.GetNetworkStats(t, v)
	assert.Equal(t, sectorPower.Raw, minerPower.Raw)
	assert.Equal(t, sectorPower.QA, minerPower.Raw)
	assert.Equal(t, sectorPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, sectorPower.QA, networkStats.TotalQABytesCommitted)
	// miner does not meet consensus minimum so actual power is not added
	assert.Equal(t, big.Zero(), networkStats.TotalRawBytePower)
	assert.Equal(t, big.Zero(), networkStats.TotalQualityAdjPower)

	//
	// publish verified and unverified deals
	//

	// register verifier then verified client
	addVerifierParams := verifreg.AddVerifierParams{
		Address:   verifier,
		Allowance: abi.NewStoragePower(32 << 40),
	}
	vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)

	addClientParams := verifreg.AddVerifiedClientParams{
		Address:   verifiedClient,
		Allowance: abi.NewStoragePower(32 << 40),
	}
	vm.ApplyOk(t, v, verifier, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifiedClient, &addClientParams)

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	vm.ApplyOk(t, v, unverifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &unverifiedClient)
	vm.ApplyOk(t, v, verifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &verifiedClient)
	collateral = big.Mul(big.NewInt(64), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create 3 deals, some verified and some not
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	deals := publishDeal(t, v, worker, verifiedClient, minerAddrs.IDAddress, "deal1", 1<<30, true, dealStart, 181*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDeal(t, v, worker, verifiedClient, minerAddrs.IDAddress, "deal2", 1<<32, true, dealStart, 200*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDeal(t, v, worker, unverifiedClient, minerAddrs.IDAddress, "deal3", 1<<34, false, dealStart, 210*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	//
	// Precommit, Prove, Verify and PoSt committed capacity sector
	//

	// precommit capacity upgrade sector with deals
	upgradeSectorNumber := abi.SectorNumber(101)
	upgradeSealedCid := tutil.MakeCID("101", &miner.SealedCIDPrefix)
	preCommitParams = miner.PreCommitSectorParams{
		SealProof:              sealProof,
		SectorNumber:           upgradeSectorNumber,
		SealedCID:              upgradeSealedCid,
		SealRandEpoch:          v.GetEpoch() - 1,
		DealIDs:                dealIDs,
		Expiration:             v.GetEpoch() + 220*builtin.EpochsInDay,
		ReplaceCapacity:        true,
		ReplaceSectorDeadline:  dlInfo.Index,
		ReplaceSectorPartition: pIdx,
		ReplaceSectorNumber:    sectorNumber,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// assert successful precommit invocation
	none := []vm.ExpectInvocation{}
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.PreCommitSector,
		Params: vm.ExpectObject(&preCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: none},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: none},
			// addtion of deal ids prompts call to verify deals for activation
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.VerifyDealsForActivation, SubInvocations: none},
		},
	}.Matches(t, v.LastInvocation())

	t.Run("verified registry bytes are restored when verified deals are not proven", func(t *testing.T) {
		tv, err := v.WithEpoch(dealStart + market.DealUpdatesInterval)
		require.NoError(t, err)

		// run cron and check for deal expiry in market actor
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
						// pre-commit deposit is burnt
						{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick, SubInvocations: []vm.ExpectInvocation{
					// notify verified registry that used bytes are released
					{To: builtin.VerifiedRegistryActorAddr, Method: builtin.MethodsVerifiedRegistry.RestoreBytes},
					{To: builtin.VerifiedRegistryActorAddr, Method: builtin.MethodsVerifiedRegistry.RestoreBytes},
					// slash funds
					{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
				}},
			},
		}.Matches(t, tv.LastInvocation())
	})

	// advance time to min seal duration
	proveTime = v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams = miner.ProveCommitSectorParams{
		SectorNumber: upgradeSectorNumber,
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: vm.ExpectObject(&proveCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment, SubInvocations: none},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.SubmitPoRepForBulkVerify, SubInvocations: none},
		},
	}.Matches(t, v.LastInvocation())

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	vm.ExpectInvocation{
		To:     builtin.CronActorAddr,
		Method: builtin.MethodsCron.EpochTick,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
				{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
					// deals are now activated
					{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ActivateDeals},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
				}},
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
			}},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
		},
	}.Matches(t, v.LastInvocation())

	// miner still has power for old sector
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats = vm.GetNetworkStats(t, v)
	assert.Equal(t, sectorPower.Raw, minerPower.Raw)
	assert.Equal(t, sectorPower.QA, minerPower.Raw)
	assert.Equal(t, sectorPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, sectorPower.QA, networkStats.TotalQABytesCommitted)

	// Assert that old sector and new sector have the same deadline.
	// This is not generally true, but the current deadline assigment will always put these together when
	// no other sectors have been assigned in-between. The following tests assume this fact, and must be
	// modified if this no longer holds.
	oldDlIdx, _ := vm.SectorDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	newDlIdx, _ := vm.SectorDeadline(t, v, minerAddrs.IDAddress, upgradeSectorNumber)
	require.Equal(t, oldDlIdx, newDlIdx)

	t.Run("miner misses first PoSt of replacement sector", func(t *testing.T) {
		// advance to proving period end of new sector
		dlInfo, _, tv := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
		tv, err = tv.WithEpoch(dlInfo.Last())
		require.NoError(t, err)

		// run cron to penalize missing PoSt
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
						// power is removed for old sector and pledge is burnt
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick, SubInvocations: []vm.ExpectInvocation{}},
			},
		}.Matches(t, tv.LastInvocation())

		// miner's power is removed for old sector because it faulted and not added for the new sector.
		minerPower = vm.MinerPower(t, tv, minerAddrs.IDAddress)
		networkStats = vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), minerPower.Raw)
		assert.Equal(t, big.Zero(), minerPower.Raw)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.Equal(t, big.Zero(), networkStats.TotalQABytesCommitted)
	})

	// advance to proving period and submit post
	dlInfo, pIdx, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, upgradeSectorNumber)

	t.Run("miner skips replacing sector in first PoSt", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch()) // create vm copy
		require.NoError(t, err)

		submitParams = miner.SubmitWindowedPoStParams{
			Deadline: dlInfo.Index,
			Partitions: []miner.PoStPartition{{
				Index: pIdx,
				// skip cc upgrade
				Skipped: bitfield.NewFromSet([]uint64{uint64(upgradeSectorNumber)}),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: dlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		}
		vm.ApplyOk(t, tv, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

		vm.ExpectInvocation{
			To:     minerAddrs.IDAddress,
			Method: builtin.MethodsMiner.SubmitWindowedPoSt,
			Params: vm.ExpectObject(&submitParams),
		}.Matches(t, tv.LastInvocation())

		// old sector power remains (until its proving deadline)
		minerPower = vm.MinerPower(t, tv, minerAddrs.IDAddress)
		networkStats = vm.GetNetworkStats(t, tv)
		assert.Equal(t, sectorPower.Raw, minerPower.Raw)
		assert.Equal(t, sectorPower.QA, minerPower.QA)
		assert.Equal(t, sectorPower.Raw, networkStats.TotalBytesCommitted)
		assert.Equal(t, sectorPower.QA, networkStats.TotalQABytesCommitted)
	})

	t.Run("miner skips replaced sector in its last PoSt", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch()) // create vm copy
		require.NoError(t, err)

		submitParams = miner.SubmitWindowedPoStParams{
			Deadline: dlInfo.Index,
			Partitions: []miner.PoStPartition{{
				Index: pIdx,
				// skip cc upgrade
				Skipped: bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: dlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		}
		vm.ApplyOk(t, tv, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

		vm.ExpectInvocation{
			To:     minerAddrs.IDAddress,
			Method: builtin.MethodsMiner.SubmitWindowedPoSt,
			Params: vm.ExpectObject(&submitParams),
		}.Matches(t, tv.LastInvocation())

		// old sector power is immediately removed
		upgradeSectorPower := vm.PowerForMinerSector(t, v, minerAddrs.IDAddress, upgradeSectorNumber)
		minerPower = vm.MinerPower(t, tv, minerAddrs.IDAddress)
		networkStats = vm.GetNetworkStats(t, tv)
		assert.Equal(t, upgradeSectorPower.Raw, minerPower.Raw)
		assert.Equal(t, upgradeSectorPower.QA, minerPower.QA)
		assert.Equal(t, upgradeSectorPower.Raw, networkStats.TotalBytesCommitted)
		assert.Equal(t, upgradeSectorPower.QA, networkStats.TotalQABytesCommitted)
	})

	submitParams = miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte("not really random"),
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: vm.ExpectObject(&submitParams),
		SubInvocations: []vm.ExpectInvocation{
			// This call to the power actor indicates power has been added for the replaced sector
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower},
		},
	}.Matches(t, v.LastInvocation())

	// power is upgraded for new sector
	// Until the old sector is terminated at its proving period, miner gets combined power for new and old sectors
	upgradeSectorPower := vm.PowerForMinerSector(t, v, minerAddrs.IDAddress, upgradeSectorNumber)
	combinedPower := upgradeSectorPower.Add(sectorPower)
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats = vm.GetNetworkStats(t, v)
	assert.Equal(t, combinedPower.Raw, minerPower.Raw)
	assert.Equal(t, combinedPower.QA, minerPower.QA)
	assert.Equal(t, combinedPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, combinedPower.QA, networkStats.TotalQABytesCommitted)

	// proving period cron removes sector reducing the miner's power to that of the new sector
	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// power is removed
	// Until the old sector is terminated at its proving period, miner gets combined power for new and old sectors
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats = vm.GetNetworkStats(t, v)
	assert.Equal(t, upgradeSectorPower.Raw, minerPower.Raw)
	assert.Equal(t, upgradeSectorPower.QA, minerPower.QA)
	assert.Equal(t, upgradeSectorPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, upgradeSectorPower.QA, networkStats.TotalQABytesCommitted)
}
