package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v4/actors/builtin"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v4/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v4/support/testing"
	vm "github.com/filecoin-project/specs-actors/v4/support/vm"
)

func TestCreateMiner(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	params := power.CreateMinerParams{
		Owner:               addrs[0],
		Worker:              addrs[0],
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, big.NewInt(1e10), builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// all expectations implicitly expected to be Ok
	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     builtin.StoragePowerActorAddr,
		Method: builtin.MethodsPower.CreateMiner,
		Params: vm.ExpectObject(&params),
		Ret:    vm.ExpectObject(ret),
		SubInvocations: []vm.ExpectInvocation{{

			// Storage power requests init actor construct a miner
			To:     builtin.InitActorAddr,
			Method: builtin.MethodsInit.Exec,
			SubInvocations: []vm.ExpectInvocation{{

				// Miner constructor gets params from original call
				To:     minerAddrs.IDAddress,
				Method: builtin.MethodConstructor,
				Params: vm.ExpectObject(&miner.ConstructorParams{
					OwnerAddr:           params.Owner,
					WorkerAddr:          params.Worker,
					WindowPoStProofType: params.WindowPoStProofType,
					PeerId:              params.Peer,
				}),
				SubInvocations: []vm.ExpectInvocation{},
			}},
		}},
	}.Matches(t, v.Invocations()[0])
}

func TestOnEpochTickEnd(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	// create a miner
	minerBalance := big.Mul(big.NewInt(10_000), vm.FIL)
	params := power.CreateMinerParams{Owner: addrs[0], Worker: addrs[0],
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("pid")}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	ret, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// create precommit to schedule cron
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	preCommitParams := miner.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       nil,
		Expiration:    v.GetEpoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[sealProof] + 100,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// find epoch of miner's next cron task (precommit:1, enrollCron:2)
	cronParams := vm.ParamsForInvocation(t, v, 1, 2)
	cronConfig, ok := cronParams.(*power.EnrollCronEventParams)
	require.True(t, ok)

	// create new vm at epoch 1 less than epoch requested by miner
	v, err := v.WithEpoch(cronConfig.EventEpoch - 1)
	require.NoError(t, err)

	// run cron and expect a call to miner and a call to update reward actor parameters
	vm.ApplyOk(t, v, builtin.CronActorAddr, builtin.StoragePowerActorAddr, big.Zero(), builtin.MethodsPower.OnEpochTickEnd, abi.Empty)

	// expect miner call to be missing
	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     builtin.StoragePowerActorAddr,
		Method: builtin.MethodsPower.OnEpochTickEnd,
		SubInvocations: []vm.ExpectInvocation{{
			// expect call to reward to update kpi
			To:     builtin.RewardActorAddr,
			Method: builtin.MethodsReward.UpdateNetworkKPI,
			From:   builtin.StoragePowerActorAddr,
		}},
	}.Matches(t, v.Invocations()[0])

	// create new vm at cron epoch with existing state
	v, err = v.WithEpoch(cronConfig.EventEpoch)
	require.NoError(t, err)

	// run cron and expect a call to miner and a call to update reward actor parameters
	vm.ApplyOk(t, v, builtin.CronActorAddr, builtin.StoragePowerActorAddr, big.Zero(), builtin.MethodsPower.OnEpochTickEnd, abi.Empty)

	// expect call to miner
	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     builtin.StoragePowerActorAddr,
		Method: builtin.MethodsPower.OnEpochTickEnd,
		SubInvocations: []vm.ExpectInvocation{{

			// expect call back to miner that was set up in create miner
			To:     minerAddrs.IDAddress,
			Method: builtin.MethodsMiner.OnDeferredCronEvent,
			From:   builtin.StoragePowerActorAddr,
			Value:  vm.ExpectAttoFil(big.Zero()),
			Params: vm.ExpectBytes(cronConfig.Payload),
		}, {

			// expect call to reward to update kpi
			To:     builtin.RewardActorAddr,
			Method: builtin.MethodsReward.UpdateNetworkKPI,
			From:   builtin.StoragePowerActorAddr,
		}},
	}.Matches(t, v.Invocations()[0])
}
