package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v3/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v3/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v3/support/testing"
	vm "github.com/filecoin-project/specs-actors/v3/support/vm"
)

var fakeChainRandomness = []byte("not really random")

func TestCronCatchedCCExpirationsAtDeadlineBoundary(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, unverifiedClient := addrs[0], addrs[1]

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
	ret := vm.ApplyOk(t, v, worker, builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

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
	proveTime := v.GetEpoch() + miner.PreCommitChallengeDelay + 1
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
		ChainCommitRand:  fakeChainRandomness,
	}

	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	// add market collateral for client and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	vm.ApplyOk(t, v, unverifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &unverifiedClient)
	collateral = big.Mul(big.NewInt(64), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create a deal required by upgrade sector
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	deals := publishDeal(t, v, worker, unverifiedClient, minerAddrs.IDAddress, "deal1", 1<<30, false, dealStart, 181*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

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

	// Advance to beginning of the valid prove-commit window, then advance to proving deadline of original sector.
	// This should allow us to prove commit the upgrade on the last epoch of the original sector's proving period.
	proveTime = v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)
	dlInfo, _, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	// prove original sector so it won't be faulted
	submitParams.ChainCommitEpoch = dlInfo.Challenge
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	// one epoch before deadline close (i.e. Last) is where we might see a problem with cron scheduling of expirations
	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)

	// miner still has power for old sector
	sectorPower := vm.PowerForMinerSector(t, v, minerAddrs.IDAddress, sectorNumber)
	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	assert.Equal(t, sectorPower.Raw, minerPower.Raw)
	assert.Equal(t, sectorPower.QA, minerPower.QA)

	// Prove commit sector after max seal duration
	proveCommitParams = miner.ProveCommitSectorParams{
		SectorNumber: upgradeSectorNumber,
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	// Replaced sector should be terminated at end of deadline it was replace in, so it should be terminated
	// by this call. This requires the miner's proving period handling to be run after commit verification.
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// Loss of power indicates original sector has been terminated at correct time.
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	assert.Equal(t, big.Zero(), minerPower.Raw)
	assert.Equal(t, big.Zero(), minerPower.QA)
}
