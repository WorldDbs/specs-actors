package test_test

import (
	"context"
	"strings"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/migration/nv7"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
)

func TestMigrationPowerAccountingIssue(t *testing.T) {
	ctx := context.Background()
	v, err := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory()).WithNetworkVersion(network.Version6)
	require.NoError(t, err)

	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), vm.FIL), 93837778)
	worker := addrs[0]
	numSectors := uint64(400) // Enough power to meet consensus min

	minerBalance := big.Mul(big.NewInt(10_000), vm.FIL)
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1

	// create miner and add some power
	params := power.CreateMinerParams{
		Owner:         worker,
		Worker:        worker,
		SealProofType: sealProof,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// Precommit, prove and PoSt empty sector (more fully tested in TestCommitPoStFlow)
	//
	for i := uint64(0); i < numSectors; i++ {
		// precommit sector
		preCommitParams := miner.PreCommitSectorParams{
			SealProof:     sealProof,
			SectorNumber:  sectorNumber + abi.SectorNumber(i),
			SealedCID:     sealedCid,
			SealRandEpoch: v.GetEpoch() - 1,
			DealIDs:       nil,
			Expiration:    v.GetEpoch() + 200*builtin.EpochsInDay,
		}
		vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	}

	// advance time to seal duration
	proveTime := v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// miner should have no power yet
	assert.Equal(t, uint64(0), vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)

	// A miner is limited to 200 prove commits per epoch. Prove 200 sectors per epoch until they are all proven.
	for i := uint64(0); i < numSectors; i += 200 {
		for j := uint64(0); j < 200 && i+j < numSectors; j++ {
			proveCommitParams := miner.ProveCommitSectorParams{
				SectorNumber: sectorNumber + abi.SectorNumber(i+j),
			}
			vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
		}

		// In the same epoch, trigger cron to validate prove commits
		vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		v, err = v.WithEpoch(v.GetEpoch() + 1)
		require.NoError(t, err)
	}

	// In the same epoch, trigger cron to validate prove commits
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	v, err = v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)

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

	// miner should have power
	assert.Equal(t, numSectors*uint64(32<<30), vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// Trigger cron to keep reward accounting correct
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	//
	// Damage power actors stats
	// This simulates zeroing out a miner claim by updating stats and then adding a new claim without updating the stats
	//

	var powerState power.State
	err = v.GetState(builtin.StoragePowerActorAddr, &powerState)
	require.NoError(t, err)

	powerState.MinerAboveMinPowerCount = 0
	powerState.TotalQABytesCommitted = abi.NewStoragePower(0)
	powerState.TotalBytesCommitted = abi.NewStoragePower(0)
	powerState.TotalQualityAdjPower = abi.NewStoragePower(0)
	powerState.TotalRawBytePower = abi.NewStoragePower(0)

	err = v.SetActorState(ctx, builtin.StoragePowerActorAddr, &powerState)
	require.NoError(t, err)

	// assert that power total invariants are broken
	stateTree, err := v.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v.GetTotalActorBalance()
	require.NoError(t, err)
	msgs, err := states.CheckStateInvariants(stateTree, totalBalance, v.GetEpoch())
	require.NoError(t, err)

	assert.Equal(t, 5, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))
	for _, msg := range msgs.Messages() {
		assert.True(t, strings.Contains(msg, "t04 power:"))
	}

	//
	// Run nv7 migration
	//

	nextRoot, err := nv7.MigrateStateTree(ctx, v.Store(), v.StateRoot(), v.GetEpoch(), nv7.Config{})
	require.NoError(t, err)

	lookup := map[cid.Cid]runtime.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v, err = vm.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// Trigger cron to keep reward accounting correct
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	v, err = v.WithNetworkVersion(network.Version6)
	require.NoError(t, err)
	//
	// Confirm miner has power and invariants hold
	//

	// miner should have power
	assert.Equal(t, numSectors*uint64(32<<30), vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// assert that power total invariants are broken
	stateTree, err = v.GetStateTree()
	require.NoError(t, err)
	totalBalance, err = v.GetTotalActorBalance()
	require.NoError(t, err)
	msgs, err = states.CheckStateInvariants(stateTree, totalBalance, v.GetEpoch())
	require.NoError(t, err)

	assert.Equal(t, 0, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))
}
