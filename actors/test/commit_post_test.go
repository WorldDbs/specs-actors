package test_test

import (
	"context"
	"strings"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
)

func TestCommitPoStFlow(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	minerBalance := big.Mul(big.NewInt(10_000), vm.FIL)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	// create miner
	params := power.CreateMinerParams{
		Owner:         addrs[0],
		Worker:        addrs[0],
		SealProofType: sealProof,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// advance vm so we can have seal randomness epoch in the past
	v, err := v.WithEpoch(200)
	require.NoError(t, err)

	//
	// precommit sector
	//

	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	sectorSize, err := sealProof.SectorSize()
	require.NoError(t, err)

	preCommitParams := miner.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       nil,
		Expiration:    v.GetEpoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[sealProof] + 100,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// assert successful precommit invocation
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.PreCommitSector,
		Params: vm.ExpectObject(&preCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower}},
	}.Matches(t, v.Invocations()[0])

	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	// find information about precommited sector
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)

	precommit, found, err := minerState.GetPrecommittedSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, dlInfo := vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	//
	// overdue precommit
	//

	t.Run("missed prove commit results in precommit expiry", func(t *testing.T) {
		// advanced one more deadline so precommit is late
		tv, err := v.WithEpoch(dlInfo.Close)
		require.NoError(t, err)

		// run cron which should expire precommit
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},

						// The call to burnt funds indicates the overdue precommit has been penalized
						{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend, Value: vm.ExpectAttoFil(precommit.PreCommitDeposit)},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					//{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
			},
		}.Matches(t, tv.Invocations()[0])

		// precommit deposit has been reset
		balances := vm.GetMinerBalances(t, tv, minerAddrs.IDAddress)
		assert.Equal(t, big.Zero(), balances.InitialPledge)
		assert.Equal(t, big.Zero(), balances.PreCommitDeposit)

		// no power is added
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.Equal(t, big.Zero(), networkStats.TotalPledgeCollateral)
		assert.Equal(t, big.Zero(), networkStats.TotalRawBytePower)
		assert.Equal(t, big.Zero(), networkStats.TotalQualityAdjPower)
	})

	//
	// prove and verify
	//

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: vm.ExpectObject(&proveCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.SubmitPoRepForBulkVerify},
		},
	}.Matches(t, v.Invocations()[0])

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	vm.ExpectInvocation{
		To:     builtin.CronActorAddr,
		Method: builtin.MethodsCron.EpochTick,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
				// expect confirm sector proofs valid because we prove committed,
				// but not an on deferred cron event because this is not a deadline boundary
				{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
				}},
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
			}},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
		},
	}.Matches(t, v.Invocations()[1])

	// precommit deposit is released, ipr is added
	balances = vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))
	assert.Equal(t, big.Zero(), balances.PreCommitDeposit)

	// power is unproven so network stats are unchanged
	networkStats := vm.GetNetworkStats(t, v)
	assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
	assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))

	//
	// Submit PoSt
	//

	// advance to proving period
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)

	sector, found, err := minerState.GetSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)

	t.Run("submit PoSt succeeds", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch())
		require.NoError(t, err)

		// Submit PoSt
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
		vm.ApplyOk(t, tv, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

		sectorPower := miner.PowerForSector(sectorSize, sector)
		updatePowerParams := &power.UpdateClaimedPowerParams{
			RawByteDelta:         sectorPower.Raw,
			QualityAdjustedDelta: sectorPower.QA,
		}

		vm.ExpectInvocation{
			To:     minerAddrs.IDAddress,
			Method: builtin.MethodsMiner.SubmitWindowedPoSt,
			Params: vm.ExpectObject(&submitParams),
			SubInvocations: []vm.ExpectInvocation{
				// This call to the power actor indicates power has been added for the sector
				{
					To:     builtin.StoragePowerActorAddr,
					Method: builtin.MethodsPower.UpdateClaimedPower,
					Params: vm.ExpectObject(updatePowerParams),
				},
			},
		}.Matches(t, tv.Invocations()[0])

		// miner still has initial pledge
		balances = vm.GetMinerBalances(t, tv, minerAddrs.IDAddress)
		assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))

		// committed bytes are added (miner would have gained power if minimum requirement were met)
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.NewInt(int64(sectorSize)), networkStats.TotalBytesCommitted)
		assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))

		// Trigger cron to keep reward accounting correct
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		stateTree, err := tv.GetStateTree()
		require.NoError(t, err)
		totalBalance, err := tv.GetTotalActorBalance()
		require.NoError(t, err)
		acc, err := states.CheckStateInvariants(stateTree, totalBalance, tv.GetEpoch())
		require.NoError(t, err)
		assert.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))
	})

	t.Run("skip sector", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch())
		require.NoError(t, err)

		// Submit PoSt
		submitParams := miner.SubmitWindowedPoStParams{
			Deadline: dlInfo.Index,
			Partitions: []miner.PoStPartition{{
				Index:   pIdx,
				Skipped: bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: dlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		}
		// PoSt is rejected for skipping all sectors.
		_, code := tv.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
		assert.Equal(t, exitcode.ErrIllegalArgument, code)

		vm.ExpectInvocation{
			To:       minerAddrs.IDAddress,
			Method:   builtin.MethodsMiner.SubmitWindowedPoSt,
			Params:   vm.ExpectObject(&submitParams),
			Exitcode: exitcode.ErrIllegalArgument,
		}.Matches(t, tv.Invocations()[0])

		// miner still has initial pledge
		balances = vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
		assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))

		// network power is unchanged
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))
	})

	t.Run("missed first PoSt deadline", func(t *testing.T) {
		// move to proving period end
		tv, err := v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)

		// Run cron to detect missing PoSt
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
			},
		}.Matches(t, tv.Invocations()[0])

		// network power is unchanged
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))
	})
}
