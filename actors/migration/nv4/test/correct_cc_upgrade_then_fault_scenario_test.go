package test_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/v2/actors/migration/nv4"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	"github.com/filecoin-project/specs-actors/v2/actors/states"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	vm0 "github.com/filecoin-project/specs-actors/support/vm"

	exported2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/exported"
	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	reward2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	vm2 "github.com/filecoin-project/specs-actors/v2/support/vm"
)

func TestMigrationCorrectsCCThenFaultIssue(t *testing.T) {
	ctx := context.Background()
	v := vm0.NewVMWithSingletons(ctx, t)
	v = vmWithActorBalance(t, v, builtin.RewardActorAddr, reward2.StorageMiningAllocationCheck)
	addrs := vm0.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(100_000), vm0.FIL), 93837778)
	worker, unverifiedClient := addrs[0], addrs[1]

	// 2349 sectors is the exact number of sectors we need to fill a deadline, forcing the next sector into
	// the next deadline.
	numSectors := uint64(2349)

	minerBalance := big.Mul(big.NewInt(10_000), vm0.FIL)
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner0.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1 // Old seal proof type to match v0 expectations.

	// create miner
	params := power.CreateMinerParams{
		Owner:         worker,
		Worker:        worker,
		SealProofType: sealProof,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret := vm0.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// Precommit, prove and PoSt empty sector (more fully tested in TestCommitPoStFlow)
	//

	for i := uint64(0); i < numSectors; i++ {
		// precommit sector
		preCommitParams := miner0.SectorPreCommitInfo{
			SealProof:     sealProof,
			SectorNumber:  sectorNumber + abi.SectorNumber(i),
			SealedCID:     sealedCid,
			SealRandEpoch: v.GetEpoch() - 1,
			DealIDs:       nil,
			Expiration:    v.GetEpoch() + 200*builtin.EpochsInDay,
		}
		vm0.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	}

	// advance time to seal duration
	proveTime := v.GetEpoch() + miner0.PreCommitChallengeDelay + 1
	v, _ = vm0.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// miner should have no power yet
	assert.Equal(t, uint64(0), vm0.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// Prove commit sector after max seal duration
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)

	// A miner is limited to 200 prove commits per epoch. Prove 200 sectors per epoch until they are all proven.
	for i := uint64(0); i < numSectors; i += 200 {
		for j := uint64(0); j < 200 && i+j < numSectors; j++ {
			proveCommitParams := miner0.ProveCommitSectorParams{
				SectorNumber: sectorNumber + abi.SectorNumber(i+j),
			}
			vm0.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
		}

		// In the same epoch, trigger cron to validate prove commits
		vm0.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		v, err = v.WithEpoch(v.GetEpoch() + 1)
		require.NoError(t, err)
	}

	// all sectors should have power
	assert.Equal(t, numSectors*32<<30, vm0.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm0.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	submitParams := miner0.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner0.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte("not really random"),
	}

	vm0.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	//
	// publish verified and unverified deals
	//

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(3), vm0.FIL)
	vm0.ApplyOk(t, v, unverifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &unverifiedClient)
	collateral = big.Mul(big.NewInt(64), vm0.FIL)
	vm0.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create 3 deals, some verified and some not
	dealStart := v.GetEpoch() + 2*builtin.EpochsInDay
	deals := publishDeal(t, v, worker, unverifiedClient, minerAddrs.IDAddress, "deal1", 1<<34, false, dealStart, 210*builtin.EpochsInDay)
	dealIDs := deals.IDs

	//
	// Precommit, Prove, Verify and PoSt committed capacity sector
	//

	// precommit capacity upgrade sector with deals
	upgradeSectorNumber := abi.SectorNumber(3000)
	upgradeSealedCid := tutil.MakeCID("101", &miner0.SealedCIDPrefix)
	preCommitParams := miner0.SectorPreCommitInfo{
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
	vm0.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to min seal duration
	proveTime = v.GetEpoch() + miner0.PreCommitChallengeDelay + 1
	v, _ = vm0.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner0.ProveCommitSectorParams{
		SectorNumber: upgradeSectorNumber,
	}
	vm0.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	vm0.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// original and upgrade sector should have power, so total power is for num sectors + 1
	assert.Equal(t, (numSectors+1)*32<<30, vm0.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// advance into next deadline
	v, err = v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)

	// now declare original sector as a fault
	vm0.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.DeclareFaults, &miner0.DeclareFaultsParams{
		Faults: []miner0.FaultDeclaration{{
			Deadline:  dlInfo.Index,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
		}},
	})

	// original sector should lose power bringing us back down to num sectors
	assert.Equal(t, numSectors*32<<30, vm0.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// advance to original sector's period and submit post
	oDlInfo, oPIdx, v := vm0.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	vm0.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner0.SubmitWindowedPoStParams{
		Deadline: oDlInfo.Index,
		Partitions: []miner0.PoStPartition{{
			Index:   oPIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: oDlInfo.Challenge,
		ChainCommitRand:  []byte("not really random"),
	})

	v, err = v.WithEpoch(oDlInfo.Last())
	require.NoError(t, err)
	vm0.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// original sector's expires dropping its power again. This is a symptom of bad behavior.
	assert.Equal(t, (numSectors-1)*32<<30, vm0.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// advance to upgrade period and submit post
	dlInfo, pIdx, v = vm0.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, upgradeSectorNumber)

	vm0.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner0.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner0.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte("not really random"),
	})

	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)
	vm0.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance 14 proving periods submitting PoSts along the way
	for i := 0; i < 14; i++ {
		// advance to original sector's period and submit post
		oDlInfo, oPIdx, v = vm0.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber+1)

		vm0.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner0.SubmitWindowedPoStParams{
			Deadline: oDlInfo.Index,
			Partitions: []miner0.PoStPartition{{
				Index:   oPIdx,
				Skipped: bitfield.New(),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: oDlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		})

		v, err = v.WithEpoch(oDlInfo.Last())
		require.NoError(t, err)
		vm0.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		// once miner is borked, our logic to advance through deadlines stops working, so exit now.
		power := vm0.MinerPower(t, v, minerAddrs.IDAddress)
		if power.IsZero() {
			break
		}

		// advance to upgrade period and submit post
		dlInfo, pIdx, v = vm0.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, upgradeSectorNumber)
		require.True(t, dlInfo.Last() > oDlInfo.Last())

		vm0.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner0.SubmitWindowedPoStParams{
			Deadline: dlInfo.Index,
			Partitions: []miner0.PoStPartition{{
				Index:   pIdx,
				Skipped: bitfield.New(),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: dlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		})

		v, err = v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)
		vm0.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	}

	// advance a few proving periods and deadlines before running the migration
	v, err = v.WithEpoch(v.GetEpoch() + 5*miner0.WPoStProvingPeriod + 4*miner0.WPoStChallengeWindow)
	require.NoError(t, err)

	//
	// assert values that are WRONG due to cc upgrade then fault problem and show we can fix them
	//

	// miner has lost all power
	assert.Equal(t, miner0.NewPowerPairZero(), vm0.MinerPower(t, v, minerAddrs.IDAddress))

	// miner's proving period and deadline are stale
	var st miner0.State
	err = v.GetState(minerAddrs.IDAddress, &st)
	require.NoError(t, err)
	assert.True(t, st.ProvingPeriodStart+miner0.WPoStProvingPeriod < v.GetEpoch())

	//
	// migrate miner
	//

	nextRoot, err := nv4.MigrateStateTree(ctx, v.Store(), v.StateRoot(), v.GetEpoch(), nv4.Config{MaxWorkers: 1}) // v.Store() is not threadsafe
	require.NoError(t, err)

	lookup := map[cid.Cid]runtime.VMActor{}
	for _, ba := range exported2.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v2, err := vm2.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// miner state invariants hold
	var st1 miner2.State
	err = v2.GetState(minerAddrs.IDAddress, &st1)
	require.NoError(t, err)

	act, found, err := v2.GetActor(minerAddrs.IDAddress)
	require.NoError(t, err)
	require.True(t, found)

	_, msgs := miner2.CheckStateInvariants(&st1, v2.Store(), act.Balance)
	assert.True(t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))

	//
	// Test correction
	//

	// Raw power is num sectors * sector size (the upgrade exactly replaces the original)
	assert.Equal(t, numSectors*32<<30, vm2.MinerPower(t, v2, minerAddrs.IDAddress).Raw.Uint64())

	// run a few more proving periods to show miner is back in shape
	for i := 0; i < 5; i++ {
		// advance to original sector's period and submit post
		oDlInfo, oPIdx, v2 = vm2.AdvanceTillProvingDeadline(t, v2, minerAddrs.IDAddress, sectorNumber+1)

		vm2.ApplyOk(t, v2, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner2.SubmitWindowedPoStParams{
			Deadline: oDlInfo.Index,
			Partitions: []miner0.PoStPartition{{
				Index:   oPIdx,
				Skipped: bitfield.New(),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: oDlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		})

		v2, err = v2.WithEpoch(oDlInfo.Last())
		require.NoError(t, err)
		vm2.ApplyOk(t, v2, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		// advance to upgrade period and submit post
		dlInfo, pIdx, v2 = vm2.AdvanceTillProvingDeadline(t, v2, minerAddrs.IDAddress, upgradeSectorNumber)
		require.True(t, dlInfo.Last() > oDlInfo.Last())

		vm2.ApplyOk(t, v2, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner2.SubmitWindowedPoStParams{
			Deadline: dlInfo.Index,
			Partitions: []miner2.PoStPartition{{
				Index:   pIdx,
				Skipped: bitfield.New(),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: dlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		})

		v, err = v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)
		vm2.ApplyOk(t, v2, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	}

	// miner still has power
	assert.Equal(t, numSectors*32<<30, vm2.MinerPower(t, v2, minerAddrs.IDAddress).Raw.Uint64())

	// miner state invariants hold
	var st2 miner2.State
	err = v2.GetState(minerAddrs.IDAddress, &st2)
	require.NoError(t, err)

	stateTree, err := v2.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v2.GetTotalActorBalance()
	require.NoError(t, err)
	msgs, err = states.CheckStateInvariants(stateTree, totalBalance, v2.GetEpoch())
	require.NoError(t, err)

	// The v2 migration will not correctly update power totals. Expect 5 invariant violations all related to power
	assert.Equal(t, 5, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))
	for _, msg := range msgs.Messages() {
		assert.True(t, strings.Contains(msg, "t04 power:"))
	}
}

func publishDeal(t *testing.T, v *vm0.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market.PublishStorageDealsReturn {
	deal := market.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm0.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm0.FIL),
	}

	publishDealParams := market.PublishStorageDealsParams{
		Deals: []market.ClientDealProposal{{
			Proposal:        deal,
			ClientSignature: crypto.Signature{},
		}},
	}
	ret, code := v.ApplyMessage(provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams)
	require.Equal(t, exitcode.Ok, code)

	expectedPublishSubinvocations := []vm0.ExpectInvocation{
		{To: minerID, Method: builtin.MethodsMiner.ControlAddresses, SubInvocations: []vm0.ExpectInvocation{}},
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: []vm0.ExpectInvocation{}},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: []vm0.ExpectInvocation{}},
	}

	if verifiedDeal {
		expectedPublishSubinvocations = append(expectedPublishSubinvocations, vm0.ExpectInvocation{
			To:             builtin.VerifiedRegistryActorAddr,
			Method:         builtin.MethodsVerifiedRegistry.UseBytes,
			SubInvocations: []vm0.ExpectInvocation{},
		})
	}

	vm0.ExpectInvocation{
		To:             builtin.StorageMarketActorAddr,
		Method:         builtin.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return ret.(*market.PublishStorageDealsReturn)
}

func vmWithActorBalance(t *testing.T, v *vm0.VM, addr addr.Address, newBalance abi.TokenAmount) *vm0.VM {
	rewardActor, found, err := v.GetActor(builtin.RewardActorAddr)
	require.NoError(t, err)
	require.True(t, found)

	// update reward balance
	rewardActor.Balance = newBalance

	// load actors map
	oldRoot := v.StateRoot()
	actors, err := states.LoadTree(v.Store(), oldRoot)
	require.NoError(t, err)

	// rewrite reward actor and get new root
	require.NoError(t, actors.SetActor(builtin.RewardActorAddr, rewardActor))
	newRoot, err := actors.Flush()
	require.NoError(t, err)

	// get new vm with this root
	newV, err := v.WithRoot(newRoot)
	require.NoError(t, err)
	return newV
}
