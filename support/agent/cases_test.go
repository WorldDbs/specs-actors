package agent_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/rt"
	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	states2 "github.com/filecoin-project/specs-actors/v2/actors/states"
	vm_test2 "github.com/filecoin-project/specs-actors/v2/support/vm"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v3/actors/migration/nv10"
	"github.com/filecoin-project/specs-actors/v3/actors/states"
	"github.com/filecoin-project/specs-actors/v3/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v3/support/agent"
	"github.com/filecoin-project/specs-actors/v3/support/ipld"
	vm_test "github.com/filecoin-project/specs-actors/v3/support/vm"
)

func TestCreate20Miners(t *testing.T) {
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1000), big.NewInt(1e18))
	minerCount := 20

	rnd := rand.New(rand.NewSource(42))

	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})
	accounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		accounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.5,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  initialBalance,
			MinMarketBalance: big.Zero(),
			MaxMarketBalance: big.Zero(),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	// give it twice the number of ticks to account for variation in the rate
	for i := 0; i < 2*minerCount; i++ {
		require.NoError(t, sim.Tick())
	}

	// add 1 agent for miner generator
	assert.Equal(t, minerCount+1, len(sim.Agents))

	for _, a := range sim.Agents {
		miner, ok := a.(*agent.MinerAgent)
		if ok {
			actor, found, err := sim.GetVM().GetActor(miner.IDAddress)
			require.NoError(t, err)
			require.True(t, found)

			// demonstrate actor is created and has correct balance
			assert.Equal(t, initialBalance, actor.Balance)
		}
	}
}

// This test covers all the simulation functionality.
// 500 epochs is long enough for most, not all, important processes to take place, but runs fast enough
// to keep in CI.
func Test500Epochs(t *testing.T) {
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e8), big.NewInt(1e18))
	cumulativeStats := make(vm_test.StatsByCall)
	minerCount := 10
	clientCount := 9

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{
		Seed:             rnd.Int63(),
		CheckpointEpochs: 1000,
	})

	// create miners
	workerAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.0,
			FaultRate:        0.0001,
			RecoveryRate:     0.0001,
			UpgradeSectors:   true,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), clientCount, initialBalance, rnd.Int63())
	dealAgents := agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .05,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	var pwrSt power.State
	for i := 0; i < 500; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			// compute number of deals
			deals := 0
			for _, da := range dealAgents {
				deals += da.DealCount
			}

			stateTree, err := getV3VM(t, sim).GetStateTree()
			require.NoError(t, err)

			totalBalance, err := getV3VM(t, sim).GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  msgs: %d  deals: %d  gets: %d  puts: %d  write bytes: %d  read bytes: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				sim.MessageCount, deals, getV3VM(t, sim).StoreReads(), getV3VM(t, sim).StoreWrites(),
				getV3VM(t, sim).StoreReadBytes(), getV3VM(t, sim).StoreWriteBytes())
		}

		cumulativeStats.MergeAllStats(sim.GetCallStats())
	}
}

func TestCommitPowerAndCheckInvariants(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e9), big.NewInt(1e18))
	minerCount := 1

	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})
	accounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		accounts,
		agent.MinerAgentConfig{
			PrecommitRate:    0.1,
			FaultRate:        0.00001,
			RecoveryRate:     0.0001,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  initialBalance,
			MinMarketBalance: big.Zero(),
			MaxMarketBalance: big.Zero(),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	var pwrSt power.State
	for i := 0; i < 100_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := getV3VM(t, sim).GetStateTree()
			require.NoError(t, err)

			totalBalance, err := getV3VM(t, sim).GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  cnsMnrs: %d avgWins: %.3f  msgs: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				pwrSt.MinerAboveMinPowerCount, float64(sim.WinCount)/float64(epoch), sim.MessageCount)
		}
	}
}

func TestMigration(t *testing.T) {
	t.Skip("slow")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	initialBalance := big.Mul(big.NewInt(1e8), big.NewInt(1e18))
	minerCount := 10
	clientCount := 9

	blkStore := newBlockStore()
	v := vm_test2.NewVMWithSingletons(ctx, t, blkStore)
	v2VMFactory := func(ctx context.Context, impl vm_test2.ActorImplLookup, store adt.Store, stateRoot cid.Cid, epoch abi.ChainEpoch) (agent.SimVM, error) {
		return vm_test2.NewVMAtEpoch(ctx, impl, store, stateRoot, epoch)
	}
	v2MinerFactory := func(ctx context.Context, root cid.Cid) (agent.SimMinerState, error) {
		return &agent.MinerStateV2{
			Ctx:  ctx,
			Root: root,
		}, nil
	}

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSimWithVM(ctx, t, v, v2VMFactory, agent.ComputePowerTableV2, blkStore, newBlockStore, v2MinerFactory, agent.SimConfig{
		Seed:             rnd.Int63(),
		CheckpointEpochs: 1000,
	}, agent.CreateMinerParamsV2)

	// create miners
	workerAccounts := vm_test2.CreateAccounts(ctx, t, v, minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.0,
			FaultRate:        0.00001,
			RecoveryRate:     0.0001,
			UpgradeSectors:   true,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test2.CreateAccounts(ctx, t, v, clientCount, initialBalance, rnd.Int63())
	agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .05,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	// Run v2 for 5000 epochs
	var pwrSt power2.State
	for i := 0; i < 5000; i++ {
		require.NoError(t, sim.Tick())
		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := states2.LoadTree(sim.GetVM().Store(), sim.GetVM().StateRoot())
			require.NoError(t, err)

			totalBalance, err := sim.GetVM().GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states2.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  cnsMnrs: %d avgWins: %.3f  msgs: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				pwrSt.MinerAboveMinPowerCount, float64(sim.WinCount)/float64(epoch), sim.MessageCount)
		}
	}

	// Migrate
	v2 := sim.GetVM()
	log := nv10.TestLogger{TB: t}
	priorEpoch := v2.GetEpoch() - 1 // on tick sim internally creates new vm with epoch set to the next one
	nextRoot, err := nv10.MigrateStateTree(ctx, v2.Store(), v2.StateRoot(), priorEpoch, nv10.Config{MaxWorkers: 1}, log, nv10.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v3, err := vm_test.NewVMAtEpoch(ctx, lookup, v2.Store(), nextRoot, priorEpoch+1)
	require.NoError(t, err)

	stateTree, err := v3.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v3.GetTotalActorBalance()
	require.NoError(t, err)
	msgs, err := states.CheckStateInvariants(stateTree, totalBalance, priorEpoch)
	require.NoError(t, err)
	assert.Zero(t, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))

	v3VMFactory := func(ctx context.Context, impl vm_test2.ActorImplLookup, store adt.Store, stateRoot cid.Cid, epoch abi.ChainEpoch) (agent.SimVM, error) {
		return vm_test.NewVMAtEpoch(ctx, vm_test.ActorImplLookup(impl), store, stateRoot, epoch)
	}
	v3MinerFactory := func(ctx context.Context, root cid.Cid) (agent.SimMinerState, error) {
		return &agent.MinerStateV3{
			Ctx:  ctx,
			Root: root,
		}, nil
	}
	sim.SwapVM(v3, agent.VMFactoryFunc(v3VMFactory), v3MinerFactory, agent.ComputePowerTableV3, agent.CreateMinerParamsV3)

	// Run v3 for 5000 epochs
	for i := 0; i < 5000; i++ {
		require.NoError(t, sim.Tick())
		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := states.LoadTree(sim.GetVM().Store(), sim.GetVM().StateRoot())
			require.NoError(t, err)

			totalBalance, err := sim.GetVM().GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  cnsMnrs: %d avgWins: %.3f  msgs: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				pwrSt.MinerAboveMinPowerCount, float64(sim.WinCount)/float64(epoch), sim.MessageCount)
		}
	}

}

func TestCommitAndCheckReadWriteStats(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e8), big.NewInt(1e18))
	cumulativeStats := make(vm_test.StatsByCall)
	minerCount := 10
	clientCount := 9

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{
		Seed:             rnd.Int63(),
		CheckpointEpochs: 1000,
	})

	// create miners
	workerAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.0,
			FaultRate:        0.00001,
			RecoveryRate:     0.0001,
			UpgradeSectors:   true,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), clientCount, initialBalance, rnd.Int63())
	dealAgents := agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .05,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	var pwrSt power.State
	for i := 0; i < 20_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			// compute number of deals
			deals := 0
			for _, da := range dealAgents {
				deals += da.DealCount
			}

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  msgs: %d  deals: %d  gets: %d  puts: %d  write bytes: %d  read bytes: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				sim.MessageCount, deals, getV3VM(t, sim).StoreReads(), getV3VM(t, sim).StoreWrites(),
				getV3VM(t, sim).StoreReadBytes(), getV3VM(t, sim).StoreWriteBytes())
		}

		cumulativeStats.MergeAllStats(sim.GetCallStats())

		if sim.GetVM().GetEpoch()%1000 == 0 {
			for method, stats := range cumulativeStats {
				printCallStats(method, stats, "")
			}
			cumulativeStats = make(vm_test.StatsByCall)
		}
	}
}

func TestCreateDeals(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e9), big.NewInt(1e18))
	minerCount := 3
	clientCount := 9

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})

	// create miners
	workerAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    0.1,
			FaultRate:        0.0001,
			RecoveryRate:     0.0001,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), clientCount, initialBalance, rnd.Int63())
	dealAgents := agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .01,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	var pwrSt power.State
	for i := 0; i < 100_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := getV3VM(t, sim).GetStateTree()
			require.NoError(t, err)

			totalBalance, err := getV3VM(t, sim).GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			// compute number of deals
			deals := 0
			for _, da := range dealAgents {
				deals += da.DealCount
			}

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  cnsMnrs: %d avgWins: %.3f  msgs: %d  deals: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				pwrSt.MinerAboveMinPowerCount, float64(sim.WinCount)/float64(epoch), sim.MessageCount, deals)
		}
	}
}

func TestCCUpgrades(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1e10), big.NewInt(1e18))
	minerCount := 10
	clientCount := 9

	// set up sim
	rnd := rand.New(rand.NewSource(42))
	sim := agent.NewSim(ctx, t, newBlockStore, agent.SimConfig{Seed: rnd.Int63()})

	// create miners
	workerAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), minerCount, initialBalance, rnd.Int63())
	sim.AddAgent(agent.NewMinerGenerator(
		workerAccounts,
		agent.MinerAgentConfig{
			PrecommitRate:    2.0,
			FaultRate:        0.00001,
			RecoveryRate:     0.0001,
			UpgradeSectors:   true,
			ProofType:        abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			StartingBalance:  big.Div(initialBalance, big.NewInt(2)),
			MinMarketBalance: big.NewInt(1e18),
			MaxMarketBalance: big.NewInt(2e18),
		},
		1.0, // create miner probability of 1 means a new miner is created every tick
		rnd.Int63(),
	))

	clientAccounts := vm_test.CreateAccounts(ctx, t, getV3VM(t, sim), clientCount, initialBalance, rnd.Int63())
	agent.AddDealClientsForAccounts(sim, clientAccounts, rnd.Int63(), agent.DealClientConfig{
		DealRate:         .01,
		MinPieceSize:     1 << 29,
		MaxPieceSize:     32 << 30,
		MinStoragePrice:  big.Zero(),
		MaxStoragePrice:  abi.NewTokenAmount(200_000_000),
		MinMarketBalance: big.NewInt(1e18),
		MaxMarketBalance: big.NewInt(2e18),
	})

	var pwrSt power.State
	for i := 0; i < 100_000; i++ {
		require.NoError(t, sim.Tick())

		epoch := sim.GetVM().GetEpoch()
		if epoch%100 == 0 {
			stateTree, err := getV3VM(t, sim).GetStateTree()
			require.NoError(t, err)

			totalBalance, err := getV3VM(t, sim).GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))

			// assume each sector is 32Gb
			sectorCount := big.Div(pwrSt.TotalBytesCommitted, big.NewInt(32<<30))

			// compute stats
			deals := 0
			upgrades := uint64(0)
			for _, a := range sim.Agents {
				switch agnt := a.(type) {
				case *agent.MinerAgent:
					upgrades += agnt.UpgradedSectors
				case *agent.DealClientAgent:
					deals += agnt.DealCount
				}
			}

			// compute upgrades

			fmt.Printf("Power at %d: raw: %v  cmtRaw: %v  cmtSecs: %d  msgs: %d  deals: %d  upgrades: %d\n",
				epoch, pwrSt.TotalRawBytePower, pwrSt.TotalBytesCommitted, sectorCount.Uint64(),
				sim.MessageCount, deals, upgrades)
		}
	}
}

func newBlockStore() cbor.IpldBlockstore {
	return ipld.NewBlockStoreInMemory()
}

func printCallStats(method vm_test.MethodKey, stats *vm_test.CallStats, indent string) { // nolint:unused
	fmt.Printf("%s%v:%d: calls: %d  gets: %d  puts: %d  read: %d  written: %d  avg gets: %.2f, avg puts: %.2f\n",
		indent, builtin.ActorNameByCode(method.Code), method.Method, stats.Calls, stats.Reads, stats.Writes,
		stats.ReadBytes, stats.WriteBytes, float32(stats.Reads)/float32(stats.Calls),
		float32(stats.Writes)/float32(stats.Calls))

	if stats.SubStats == nil {
		return
	}

	for m, s := range stats.SubStats {
		printCallStats(m, s, indent+"  ")
	}
}

func getV3VM(t *testing.T, sim *agent.Sim) *vm_test.VM {
	vm, ok := sim.GetVM().(*vm_test.VM)
	require.True(t, ok)
	return vm
}
