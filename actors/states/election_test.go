package states_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestMinerEligibleForElection(t *testing.T) {
	ctx := context.Background()
	store := ipld.NewADTStore(ctx)
	proofType := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	pwr := abi.NewStoragePower(1)

	owner := tutil.NewIDAddr(t, 100)
	maddr := tutil.NewIDAddr(t, 101)

	t.Run("miner eligible", func(t *testing.T) {
		mstate := constructMinerState(ctx, t, store, owner)
		pstate := constructPowerStateWithMiner(t, store, maddr, pwr, proofType)
		assert.Equal(t, big.Zero(), mstate.InitialPledge) // Not directly relevant.

		currEpoch := abi.ChainEpoch(100000)
		eligible, err := states.MinerEligibleForElection(store, mstate, pstate, maddr, currEpoch)
		require.NoError(t, err)
		assert.True(t, eligible)
	})

	t.Run("zero claim", func(t *testing.T) {
		mstate := constructMinerState(ctx, t, store, owner)
		pstate := constructPowerStateWithMiner(t, store, maddr, big.Zero(), proofType)

		currEpoch := abi.ChainEpoch(100000)
		eligible, err := states.MinerEligibleForElection(store, mstate, pstate, maddr, currEpoch)
		require.NoError(t, err)
		assert.False(t, eligible)
	})

	t.Run("active consensus fault", func(t *testing.T) {
		mstate := constructMinerState(ctx, t, store, owner)
		pstate := constructPowerStateWithMiner(t, store, maddr, pwr, proofType)

		info, err := mstate.GetInfo(store)
		require.NoError(t, err)
		info.ConsensusFaultElapsed = abi.ChainEpoch(55)
		err = mstate.SaveInfo(store, info)
		require.NoError(t, err)

		currEpoch := abi.ChainEpoch(33) // 33 less than 55 so consensus fault still active
		eligible, err := states.MinerEligibleForElection(store, mstate, pstate, maddr, currEpoch)
		require.NoError(t, err)
		assert.False(t, eligible)
	})

	t.Run("fee debt", func(t *testing.T) {
		mstate := constructMinerState(ctx, t, store, owner)
		pstate := constructPowerStateWithMiner(t, store, maddr, pwr, proofType)
		mstate.FeeDebt = abi.NewTokenAmount(1000)

		currEpoch := abi.ChainEpoch(100000)
		eligible, err := states.MinerEligibleForElection(store, mstate, pstate, maddr, currEpoch)
		require.NoError(t, err)
		assert.False(t, eligible)
	})
}

func TestMinerEligibleAtLookback(t *testing.T) {
	ctx := context.Background()
	store := ipld.NewADTStore(ctx)
	proofType := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	maddr := tutil.NewIDAddr(t, 101)

	t.Run("power does not meet minimum", func(t *testing.T) {
		// get minimums
		pow32GiBMin, err := builtin.ConsensusMinerMinPower(proofType)
		require.NoError(t, err)
		pow64GiBMin, err := builtin.ConsensusMinerMinPower(abi.RegisteredSealProof_StackedDrg64GiBV1_1)
		require.NoError(t, err)

		for _, tc := range []struct {
			consensusMiners int64
			minerProof      abi.RegisteredSealProof
			power           abi.StoragePower
			eligible        bool
		}{{
			// below consensus minimum miners, power only needs to be positive to be eligible
			consensusMiners: 0,
			minerProof:      proofType,
			power:           big.Zero(),
			eligible:        false,
		}, {
			consensusMiners: 0,
			minerProof:      proofType,
			power:           big.NewInt(1),
			eligible:        true,
		}, {
			// with enough miners above minimum, power must be at or above consensus min
			consensusMiners: power.ConsensusMinerMinMiners,
			minerProof:      proofType,
			power:           big.Sub(pow32GiBMin, big.NewInt(1)),
			eligible:        false,
		}, {
			consensusMiners: power.ConsensusMinerMinMiners,
			minerProof:      proofType,
			power:           pow32GiBMin,
			eligible:        true,
		}, {
			// bigger sector size requires higher minimum
			consensusMiners: power.ConsensusMinerMinMiners,
			minerProof:      abi.RegisteredSealProof_StackedDrg64GiBV1_1,
			power:           pow32GiBMin,
			eligible:        false,
		}, {
			// bigger sector size requires higher minimum
			consensusMiners: power.ConsensusMinerMinMiners,
			minerProof:      abi.RegisteredSealProof_StackedDrg64GiBV1_1,
			power:           pow64GiBMin,
			eligible:        true,
		}} {
			pstate := constructPowerStateWithMiner(t, store, maddr, tc.power, tc.minerProof)
			pstate.MinerAboveMinPowerCount = tc.consensusMiners
			eligible, err := states.MinerPoStLookbackEligibleForElection(store, pstate, maddr)
			require.NoError(t, err)
			assert.Equal(t, tc.eligible, eligible)
		}
	})
}

func constructMinerState(ctx context.Context, t *testing.T, store adt.Store, owner address.Address) *miner.State {
	proofType := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	ssize, err := proofType.SectorSize()
	require.NoError(t, err)
	psize, err := builtin.SealProofWindowPoStPartitionSectors(proofType)
	require.NoError(t, err)

	info := miner.MinerInfo{
		Owner:                      owner,
		Worker:                     owner,
		ControlAddresses:           []address.Address{},
		PendingWorkerKey:           nil,
		PeerId:                     nil,
		Multiaddrs:                 [][]byte{},
		SealProofType:              proofType,
		SectorSize:                 ssize,
		WindowPoStPartitionSectors: psize,
		ConsensusFaultElapsed:      0,
	}
	infoCid, err := store.Put(ctx, &info)
	require.NoError(t, err)

	periodStart := abi.ChainEpoch(0)

	emptyMap, err := adt.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	emptyArray, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	emptyBitfield := bitfield.NewFromSet(nil)
	emptyBitfieldCid, err := store.Put(ctx, emptyBitfield)
	require.NoError(t, err)

	emptyDeadline := miner.ConstructDeadline(emptyArray)
	emptyDeadlineCid, err := store.Put(ctx, emptyDeadline)
	require.NoError(t, err)

	emptyDeadlines := miner.ConstructDeadlines(emptyDeadlineCid)
	emptyVestingFunds := miner.ConstructVestingFunds()
	emptyDeadlinesCid, err := store.Put(ctx, emptyDeadlines)
	require.NoError(t, err)

	emptyVestingFundsCid, err := store.Put(ctx, emptyVestingFunds)
	require.NoError(t, err)

	st, err := miner.ConstructState(infoCid, periodStart, 0, emptyBitfieldCid, emptyArray, emptyMap, emptyDeadlinesCid, emptyVestingFundsCid)
	require.NoError(t, err)

	return st
}

func constructPowerStateWithMiner(t *testing.T, store adt.Store, maddr address.Address, pwr abi.StoragePower, proof abi.RegisteredSealProof) *power.State {
	emptyMap, err := adt.MakeEmptyMap(store).Root()
	require.NoError(t, err)
	emptyMMap, err := adt.MakeEmptyMultimap(store).Root()
	require.NoError(t, err)
	pSt := power.ConstructState(emptyMap, emptyMMap)

	claims, err := adt.AsMap(store, pSt.Claims)
	require.NoError(t, err)

	claim := &power.Claim{SealProofType: proof, RawBytePower: pwr, QualityAdjPower: pwr}

	err = claims.Put(abi.AddrKey(maddr), claim)
	require.NoError(t, err)

	pSt.MinerCount += 1

	pSt.Claims, err = claims.Root()
	require.NoError(t, err)
	return pSt
}
