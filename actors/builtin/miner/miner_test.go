// nolint:unused // 20200716 until tests are restored from miner state refactor
package miner_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	bitfield "github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	cid "github.com/ipfs/go-cid"
	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

var testPid abi.PeerID
var testMultiaddrs []abi.Multiaddrs

// A balance for use in tests where the miner's low balance is not interesting.
var bigBalance = big.Mul(big.NewInt(1_000_000), big.NewInt(1e18))

// A reward amount for use in tests where the vesting amount wants to be large enough to cover penalties.
var bigRewards = big.Mul(big.NewInt(1_000), big.NewInt(1e18))

// an expriration 1 greater than min expiration
const defaultSectorExpiration = 190

func init() {
	testPid = abi.PeerID("peerID")

	testMultiaddrs = []abi.Multiaddrs{
		[]byte("foobar"),
		[]byte("imafilminer"),
	}

	// permit 2KiB sectors in tests
	miner.PreCommitSealProofTypesV8[abi.RegisteredSealProof_StackedDrg2KiBV1_1] = struct{}{}
}

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, miner.Actor{})
}

func TestConstruction(t *testing.T) {
	actor := miner.Actor{}
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)

	controlAddrs := []addr.Address{tutil.NewIDAddr(t, 999), tutil.NewIDAddr(t, 998)}

	receiver := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithActorType(controlAddrs[0], builtin.AccountActorCodeID).
		WithActorType(controlAddrs[1], builtin.AccountActorCodeID).
		WithHasher(blake2b.Sum256).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		params := miner.ConstructorParams{
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			ControlAddrs:  controlAddrs,
			SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			PeerId:        testPid,
			Multiaddrs:    testMultiaddrs,
		}

		provingPeriodStart := abi.ChainEpoch(-2222) // This is just set from running the code.
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		// Fetch worker pubkey.
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
		// Register proving period cron.
		dlIdx := (rt.Epoch() - provingPeriodStart) / miner.WPoStChallengeWindow
		firstDeadlineClose := provingPeriodStart + (1+dlIdx)*miner.WPoStChallengeWindow
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			makeDeadlineCronEventParams(t, firstDeadlineClose-1), big.Zero(), nil, exitcode.Ok)
		ret := rt.Call(actor.Constructor, &params)

		assert.Nil(t, ret)
		rt.Verify()

		var st miner.State
		rt.GetState(&st)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		assert.Equal(t, params.OwnerAddr, info.Owner)
		assert.Equal(t, params.WorkerAddr, info.Worker)
		assert.Equal(t, params.ControlAddrs, info.ControlAddresses)
		assert.Equal(t, params.PeerId, info.PeerId)
		assert.Equal(t, params.Multiaddrs, info.Multiaddrs)
		assert.Equal(t, abi.RegisteredSealProof_StackedDrg32GiBV1_1, info.SealProofType)
		assert.Equal(t, abi.SectorSize(1<<35), info.SectorSize)
		assert.Equal(t, uint64(2349), info.WindowPoStPartitionSectors)

		assert.Equal(t, big.Zero(), st.PreCommitDeposits)
		assert.Equal(t, big.Zero(), st.LockedFunds)
		assert.True(t, st.PreCommittedSectors.Defined())

		assert.True(t, st.Sectors.Defined())
		assert.Equal(t, provingPeriodStart, st.ProvingPeriodStart)
		assert.Equal(t, uint64(dlIdx), st.CurrentDeadline)

		var deadlines miner.Deadlines
		rt.StoreGet(st.Deadlines, &deadlines)
		for i := uint64(0); i < miner.WPoStPeriodDeadlines; i++ {
			var deadline miner.Deadline
			rt.StoreGet(deadlines.Due[i], &deadline)
			assert.True(t, deadline.Partitions.Defined())
			assert.True(t, deadline.ExpirationsEpochs.Defined())
			assertEmptyBitfield(t, deadline.PostSubmissions)
			assertEmptyBitfield(t, deadline.EarlyTerminations)
			assert.Equal(t, uint64(0), deadline.LiveSectors)
		}

		assertEmptyBitfield(t, st.EarlyTerminations)

		_, msgs := miner.CheckStateInvariants(&st, rt.AdtStore(), rt.Balance())
		assert.True(t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))
	})

	t.Run("control addresses are resolved during construction", func(t *testing.T) {
		control1 := tutil.NewBLSAddr(t, 501)
		control1Id := tutil.NewIDAddr(t, 555)

		control2 := tutil.NewBLSAddr(t, 502)
		control2Id := tutil.NewIDAddr(t, 655)

		rt := builder.WithActorType(control1Id, builtin.AccountActorCodeID).
			WithActorType(control2Id, builtin.AccountActorCodeID).Build(t)
		rt.AddIDAddress(control1, control1Id)
		rt.AddIDAddress(control2, control2Id)

		params := miner.ConstructorParams{
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			ControlAddrs:  []addr.Address{control1, control2},
			SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
		}

		provingPeriodStart := abi.ChainEpoch(-2222) // This is just set from running the code.
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
		dlIdx := (rt.Epoch() - provingPeriodStart) / miner.WPoStChallengeWindow
		firstDeadlineClose := provingPeriodStart + (1+dlIdx)*miner.WPoStChallengeWindow
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			makeDeadlineCronEventParams(t, firstDeadlineClose-1), big.Zero(), nil, exitcode.Ok)
		ret := rt.Call(actor.Constructor, &params)

		assert.Nil(t, ret)
		rt.Verify()

		var st miner.State
		rt.GetState(&st)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		assert.Equal(t, control1Id, info.ControlAddresses[0])
		assert.Equal(t, control2Id, info.ControlAddresses[1])
	})

	t.Run("fails if control address is not an account actor", func(t *testing.T) {
		control1 := tutil.NewIDAddr(t, 501)
		rt := builder.Build(t)
		rt.SetAddressActorType(control1, builtin.PaymentChannelActorCodeID)

		params := miner.ConstructorParams{
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			ControlAddrs:  []addr.Address{control1},
			SealProofType: abi.RegisteredSealProof_StackedDrg2KiBV1_1,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()
	})

	t.Run("test construct with invalid peer ID", func(t *testing.T) {
		rt := builder.Build(t)
		pid := [miner.MaxPeerIDLength + 1]byte{1, 2, 3, 4}
		params := miner.ConstructorParams{
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			PeerId:        pid[:],
			Multiaddrs:    testMultiaddrs,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "peer ID size", func() {
			rt.Call(actor.Constructor, &params)
		})
	})

	t.Run("fails if control addresses exceeds maximum length", func(t *testing.T) {
		rt := builder.Build(t)

		controlAddrs := make([]addr.Address, 0, miner.MaxControlAddresses+1)
		for i := 0; i <= miner.MaxControlAddresses; i++ {
			controlAddrs = append(controlAddrs, tutil.NewIDAddr(t, uint64(i)))
		}

		params := miner.ConstructorParams{
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			PeerId:        testPid,
			ControlAddrs:  controlAddrs,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "control addresses length", func() {
			rt.Call(actor.Constructor, &params)
		})
	})

	t.Run("test construct with large multiaddr", func(t *testing.T) {
		rt := builder.Build(t)
		maddrs := make([]abi.Multiaddrs, 100)
		for i := range maddrs {
			maddrs[i] = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		}
		params := miner.ConstructorParams{
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			PeerId:        testPid,
			Multiaddrs:    maddrs,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "multiaddr size", func() {
			rt.Call(actor.Constructor, &params)
		})
	})

	t.Run("test construct with empty multiaddr", func(t *testing.T) {
		rt := builder.Build(t)
		maddrs := []abi.Multiaddrs{
			{},
			{1},
		}
		params := miner.ConstructorParams{
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			PeerId:        testPid,
			Multiaddrs:    maddrs,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid empty multiaddr", func() {
			rt.Call(actor.Constructor, &params)
		})
	})

	t.Run("checks seal proof version", func(t *testing.T) {
		actor := newHarness(t, 0)
		builder := builderForHarness(actor)
		{
			// Before version 7, only V1 accepted
			rt := builder.Build(t)
			rt.SetNetworkVersion(network.Version6)
			actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.constructAndVerify(rt)
			})
			rt.Reset()
			actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
			actor.constructAndVerify(rt)
		}
		{
			// Version 7 accepts either
			rt := builder.Build(t)
			rt.SetNetworkVersion(network.Version7)
			actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
			actor.constructAndVerify(rt)

			rt = builder.Build(t)
			rt.SetNetworkVersion(network.Version7)
			actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
			actor.constructAndVerify(rt)
		}
		{
			// From version 8, only V1_1 accepted
			rt := builder.Build(t)
			rt.SetNetworkVersion(network.Version8)
			actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.constructAndVerify(rt)
			})
			rt.Reset()
			actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
			actor.constructAndVerify(rt)
		}
	})
}

// Test operations related to peer info (peer ID/multiaddrs)
func TestPeerInfo(t *testing.T) {
	h := newHarness(t, 0)
	builder := builderForHarness(h)

	t.Run("can set peer id", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		h.setPeerID(rt, abi.PeerID("new peer id"))
		h.checkState(rt)
	})

	t.Run("can clear peer id", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		h.setPeerID(rt, nil)
		h.checkState(rt)
	})

	t.Run("can't set large peer id", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		largePid := [miner.MaxPeerIDLength + 1]byte{1, 2, 3}
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "peer ID size", func() {
			h.setPeerID(rt, largePid[:])
		})
		h.checkState(rt)
	})

	t.Run("can set multiaddrs", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		h.setMultiaddrs(rt, abi.Multiaddrs("imanewminer"))
		h.checkState(rt)
	})

	t.Run("can set multiple multiaddrs", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		h.setMultiaddrs(rt, abi.Multiaddrs("imanewminer"), abi.Multiaddrs("ihavemany"))
		h.checkState(rt)
	})

	t.Run("can set clear the multiaddr", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		h.setMultiaddrs(rt)
		h.checkState(rt)
	})

	t.Run("can't set empty multiaddrs", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid empty multiaddr", func() {
			h.setMultiaddrs(rt, nil)
		})
		h.checkState(rt)
	})

	t.Run("can't set large multiaddrs", func(t *testing.T) {
		rt := builder.Build(t)
		h.constructAndVerify(rt)

		maddrs := make([]abi.Multiaddrs, 100)
		for i := range maddrs {
			maddrs[i] = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		}

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "multiaddr size", func() {
			h.setMultiaddrs(rt, maddrs...)
		})
		h.checkState(rt)
	})
}

// Tests for fetching and manipulating miner addresses.
func TestControlAddresses(t *testing.T) {
	actor := newHarness(t, 0)
	builder := builderForHarness(actor)

	t.Run("get addresses", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		o, w, control := actor.controlAddresses(rt)
		assert.Equal(t, actor.owner, o)
		assert.Equal(t, actor.worker, w)
		assert.NotEmpty(t, control)
		assert.Equal(t, actor.controlAddrs, control)
		actor.checkState(rt)
	})

}

// Test for sector precommitment and proving.
func TestCommitments(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	t.Run("valid precommit then provecommit", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		dlInfo := actor.deadline(rt)

		// Make a good commitment for the proof to target.
		// Use the max sector number to make sure everything works.
		sectorNo := abi.SectorNumber(abi.MaxSectorNumber)
		proveCommitEpoch := precommitEpoch + miner.PreCommitChallengeDelay + 1
		expiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // something on deadline boundary but > 180 days
		// Fill the sector with verified deals
		sectorWeight := big.Mul(big.NewInt(int64(actor.sectorSize)), big.NewInt(int64(expiration-proveCommitEpoch)))
		dealWeight := big.Zero()
		verifiedDealWeight := sectorWeight

		// Pre-commit with a deal in order to exercise non-zero deal weights.
		precommitParams := actor.makePreCommit(sectorNo, precommitEpoch-1, expiration, []abi.DealID{1})
		precommit := actor.preCommitSector(rt, precommitParams, preCommitConf{
			dealWeight:         dealWeight,
			verifiedDealWeight: verifiedDealWeight,
		})

		// assert precommit exists and meets expectations
		onChainPrecommit := actor.getPreCommit(rt, sectorNo)

		// deal weights must be set in precommit onchain info
		assert.Equal(t, dealWeight, onChainPrecommit.DealWeight)
		assert.Equal(t, verifiedDealWeight, onChainPrecommit.VerifiedDealWeight)

		pwrEstimate := miner.QAPowerForWeight(actor.sectorSize, precommit.Info.Expiration-precommitEpoch, onChainPrecommit.DealWeight, onChainPrecommit.VerifiedDealWeight)
		expectedDeposit := miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwrEstimate)
		assert.Equal(t, expectedDeposit, onChainPrecommit.PreCommitDeposit)

		// expect total precommit deposit to equal our new deposit
		st := getState(rt)
		assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

		// run prove commit logic
		rt.SetEpoch(proveCommitEpoch)
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))
		actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})

		// expect precommit to have been removed
		st = getState(rt)
		_, found, err := st.GetPrecommittedSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		require.False(t, found)

		// expect deposit to have been transferred to initial pledges
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)

		// The sector is exactly full with verified deals, so expect fully verified power.
		expectedPower := big.Mul(big.NewInt(int64(actor.sectorSize)), big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier))
		qaPower := miner.QAPowerForWeight(actor.sectorSize, precommit.Info.Expiration-rt.Epoch(), onChainPrecommit.DealWeight, onChainPrecommit.VerifiedDealWeight)
		assert.Equal(t, expectedPower, qaPower)
		expectedInitialPledge := miner.InitialPledgeForPower(qaPower, actor.baselinePower, actor.epochRewardSmooth,
			actor.epochQAPowerSmooth, rt.TotalFilCircSupply())
		assert.Equal(t, expectedInitialPledge, st.InitialPledge)

		// expect new onchain sector
		sector := actor.getSector(rt, sectorNo)
		sectorPower := miner.NewPowerPair(big.NewIntUnsigned(uint64(actor.sectorSize)), qaPower)

		// expect deal weights to be transferred to on chain info
		assert.Equal(t, onChainPrecommit.DealWeight, sector.DealWeight)
		assert.Equal(t, onChainPrecommit.VerifiedDealWeight, sector.VerifiedDealWeight)

		// expect activation epoch to be current epoch
		assert.Equal(t, rt.Epoch(), sector.Activation)

		// expect initial plege of sector to be set
		assert.Equal(t, expectedInitialPledge, sector.InitialPledge)

		// expect locked initial pledge of sector to be the same as pledge requirement
		assert.Equal(t, expectedInitialPledge, st.InitialPledge)

		// expect sector to be assigned a deadline/partition
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, pIdx)
		assert.Equal(t, uint64(1), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.PostSubmissions)
		assertEmptyBitfield(t, deadline.EarlyTerminations)

		quant := st.QuantSpecForDeadline(dlIdx)
		quantizedExpiration := quant.QuantizeUp(precommit.Info.Expiration)

		dQueue := actor.collectDeadlineExpirations(rt, deadline)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			quantizedExpiration: {pIdx},
		}, dQueue)

		assertBitfieldEquals(t, partition.Sectors, uint64(sectorNo))
		assertEmptyBitfield(t, partition.Faults)
		assertEmptyBitfield(t, partition.Recoveries)
		assertEmptyBitfield(t, partition.Terminated)
		assert.Equal(t, sectorPower, partition.LivePower)
		assert.Equal(t, miner.NewPowerPairZero(), partition.FaultyPower)
		assert.Equal(t, miner.NewPowerPairZero(), partition.RecoveringPower)

		pQueue := actor.collectPartitionExpirations(rt, partition)
		entry, ok := pQueue[quantizedExpiration]
		require.True(t, ok)
		assertBitfieldEquals(t, entry.OnTimeSectors, uint64(sectorNo))
		assertEmptyBitfield(t, entry.EarlySectors)
		assert.Equal(t, expectedInitialPledge, entry.OnTimePledge)
		assert.Equal(t, sectorPower, entry.ActivePower)
		assert.Equal(t, miner.NewPowerPairZero(), entry.FaultyPower)
	})

	for _, test := range []struct {
		name                string
		version             network.Version
		expectedPledgeDelta abi.TokenAmount
		sealProofType       abi.RegisteredSealProof
	}{{
		name:                "precommit vests funds in version 6",
		version:             network.Version6,
		expectedPledgeDelta: abi.NewTokenAmount(-1000),
		sealProofType:       abi.RegisteredSealProof_StackedDrg32GiBV1,
	}, {
		name:                "precommit stops vesting funds in version 7",
		version:             network.Version7,
		expectedPledgeDelta: abi.NewTokenAmount(0),
		sealProofType:       abi.RegisteredSealProof_StackedDrg32GiBV1_1,
	}, {
		name:                "precommit does not vest funds in version 8",
		version:             network.Version8,
		expectedPledgeDelta: abi.NewTokenAmount(0),
		sealProofType:       abi.RegisteredSealProof_StackedDrg32GiBV1_1,
	}} {
		t.Run(test.name, func(t *testing.T) {
			actor := newHarness(t, periodOffset)
			actor.setProofType(test.sealProofType)
			rt := builderForHarness(actor).
				WithBalance(bigBalance, big.Zero()).
				WithNetworkVersion(test.version).
				Build(t)
			precommitEpoch := periodOffset + 1
			rt.SetEpoch(precommitEpoch)
			actor.constructAndVerify(rt)
			dlInfo := actor.deadline(rt)

			// Make a good commitment for the proof to target.
			sectorNo := abi.SectorNumber(100)
			expiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // something on deadline boundary but > 180 days

			// add 1000 tokens that vest immediately
			st := getState(rt)
			_, err := st.AddLockedFunds(rt.AdtStore(), rt.Epoch(), abi.NewTokenAmount(1000), &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   1,
				StepDuration: 1,
				Quantization: 1,
			})
			require.NoError(t, err)
			rt.ReplaceState(st)

			rt.SetEpoch(rt.Epoch() + 2)

			// Pre-commit with a deal in order to exercise non-zero deal weights.
			precommitParams := actor.makePreCommit(sectorNo, precommitEpoch-1, expiration, []abi.DealID{1})
			actor.preCommitSector(rt, precommitParams, preCommitConf{
				pledgeDelta: &test.expectedPledgeDelta,
			})
		})
	}

	t.Run("insufficient funds for pre-commit", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		insufficientBalance := abi.NewTokenAmount(10) // 10 AttoFIL
		rt := builderForHarness(actor).
			WithBalance(insufficientBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{})
		})
		actor.checkState(rt)
	})

	t.Run("deal space exceeds sector space", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "deals too large to fit in sector", func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, []abi.DealID{1}), preCommitConf{
				dealSpace: actor.sectorSize + 1,
			})
		})
		actor.checkState(rt)
	})

	t.Run("precommit pays back fee debt", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		st := getState(rt)
		st.FeeDebt = abi.NewTokenAmount(9999)
		rt.ReplaceState(st)

		actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{})
		st = getState(rt)
		assert.Equal(t, big.Zero(), st.FeeDebt)
		actor.checkState(rt)
	})

	t.Run("invalid pre-commit rejected", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1

		oldSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)[0]

		// Good commitment.
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
		actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{})

		// Duplicate pre-commit sector ID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already been allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{})
		})
		rt.Reset()

		// Sector ID already committed
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already been allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(oldSector.SectorNumber, challengeEpoch, expiration, nil), preCommitConf{})
		})
		rt.Reset()

		// Bad sealed CID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "sealed CID had wrong prefix", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealedCID = tutil.MakeCID("Random Data", nil)
			actor.preCommitSector(rt, pc, preCommitConf{})
		})
		rt.Reset()

		// Bad seal proof type
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "unsupported seal proof type", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg8MiBV1_1
			actor.preCommitSector(rt, pc, preCommitConf{})
		})
		rt.Reset()

		// Expires at current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after activation", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch(), nil), preCommitConf{})
		})
		rt.Reset()

		// Expires before current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after activation", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch()-1, nil), preCommitConf{})
		})
		rt.Reset()

		// Expires too early
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration-20*builtin.EpochsInDay, nil), preCommitConf{})
		})
		rt.Reset()

		// Expires before min duration + max seal duration
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			expiration := rt.Epoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[actor.sealProofType] - 1
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{})
		})
		rt.Reset()

		// Errors when expiry too far in the future
		rt.SetEpoch(precommitEpoch)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid expiration", func() {
			expiration := deadline.PeriodEnd() + miner.WPoStProvingPeriod*(miner.MaxSectorExpirationExtension/miner.WPoStProvingPeriod+1)
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{})
		})
		rt.Reset()

		// Sector ID out of range
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "out of range", func() {
			actor.preCommitSector(rt, actor.makePreCommit(abi.MaxSectorNumber+1, challengeEpoch, expiration, nil), preCommitConf{})
		})
		rt.Reset()

		// Seal randomness challenge too far in past
		tooOldChallengeEpoch := precommitEpoch - miner.ChainFinality - miner.MaxProveCommitDuration[actor.sealProofType] - 1
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too old", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, tooOldChallengeEpoch, expiration, nil), preCommitConf{})
		})
		rt.Reset()

		// Try to precommit while in fee debt with insufficient balance
		st := getState(rt)
		st.FeeDebt = big.Add(rt.Balance(), abi.NewTokenAmount(1e18))
		rt.ReplaceState(st)
		rt.ExpectAbortContainsMessage(exitcode.ErrInsufficientFunds, "unlocked balance can not repay fee debt", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{})
		})
		// reset state back to normal
		st.FeeDebt = big.Zero()
		rt.ReplaceState(st)
		rt.Reset()

		// Try to precommit with an active consensus fault
		st = getState(rt)

		actor.reportConsensusFault(rt, addr.TestAddress, rt.Epoch()-1)
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "precommit not allowed during active consensus fault", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{})
		})
		// reset state back to normal
		rt.ReplaceState(st)
		rt.Reset()
		actor.checkState(rt)
	})

	t.Run("prove commit just after period start permits PoSt", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		// Epoch 101 should be at the beginning of the miner's proving period so there will be time to commit
		// and PoSt a sector.
		rt.SetEpoch(101)
		actor.constructAndVerify(rt)

		// Commit a sector the very next epoch
		rt.SetEpoch(102)
		sector := actor.commitAndProveSector(rt, abi.MaxSectorNumber, defaultSectorExpiration, nil)

		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, sector)
		actor.checkState(rt)
	})

	t.Run("invalid proof rejected", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)

		// Make a good commitment for the proof to target.
		sectorNo := abi.SectorNumber(100)
		params := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, []abi.DealID{1})
		precommit := actor.preCommitSector(rt, params, preCommitConf{})

		// Sector pre-commitment missing.
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo+1), proveCommitConf{})
		})
		rt.Reset()

		// Too late.
		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[precommit.Info.SealProof] + 1)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()

		// Too early.
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay - 1)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()

		// Set the right epoch for all following tests
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)

		// Invalid deals (market ActivateDeals aborts)
		verifyDealsExit := map[abi.SectorNumber]exitcode.ExitCode{
			precommit.Info.SectorNumber: exitcode.ErrIllegalArgument,
		}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{
				verifyDealsExit: verifyDealsExit,
			})
		})
		rt.Reset()

		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))

		// Too big at version 4
		proveCommit := makeProveCommit(sectorNo)
		proveCommit.Proof = make([]byte, 1920)
		rt.SetNetworkVersion(network.Version4)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, proveCommit, proveCommitConf{})
		})
		rt.Reset()

		// Good proof at version 5
		rt.SetNetworkVersion(network.Version5)
		actor.proveCommitSectorAndConfirm(rt, precommit, proveCommit, proveCommitConf{})
		st := getState(rt)

		// Verify new sectors
		// TODO minerstate
		//newSectors, err := st.NewSectors.All(miner.SectorsMax)
		//require.NoError(t, err)
		//assert.Equal(t, []uint64{uint64(sectorNo)}, newSectors)
		// Verify pledge lock-up
		assert.True(t, st.InitialPledge.GreaterThan(big.Zero()))
		rt.Reset()

		// Duplicate proof (sector no-longer pre-committed)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()
		actor.checkState(rt)
	})

	for _, test := range []struct {
		name               string
		version            network.Version
		vestingPledgeDelta abi.TokenAmount
		sealProofType      abi.RegisteredSealProof
	}{{
		name:               "verify proof vests funds in network version 6",
		version:            network.Version6,
		vestingPledgeDelta: abi.NewTokenAmount(-1000),
		sealProofType:      abi.RegisteredSealProof_StackedDrg32GiBV1,
	}, {
		name:               "verify proof does not vest starting version 7",
		version:            network.Version7,
		vestingPledgeDelta: abi.NewTokenAmount(0),
		sealProofType:      abi.RegisteredSealProof_StackedDrg32GiBV1_1,
	}, {
		name:               "verify proof still does not vest at version 7",
		version:            network.Version8,
		vestingPledgeDelta: abi.NewTokenAmount(0),
		sealProofType:      abi.RegisteredSealProof_StackedDrg32GiBV1_1,
	}} {
		t.Run(test.name, func(t *testing.T) {
			actor := newHarness(t, periodOffset)
			actor.setProofType(test.sealProofType)
			rt := builderForHarness(actor).
				WithNetworkVersion(test.version).
				WithBalance(bigBalance, big.Zero()).
				Build(t)
			precommitEpoch := periodOffset + 1
			rt.SetEpoch(precommitEpoch)
			actor.constructAndVerify(rt)
			deadline := actor.deadline(rt)

			// Make a good commitment for the proof to target.
			sectorNo := abi.SectorNumber(100)
			params := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, []abi.DealID{1})
			precommit := actor.preCommitSector(rt, params, preCommitConf{})

			// add 1000 tokens that vest immediately
			st := getState(rt)
			_, err := st.AddLockedFunds(rt.AdtStore(), rt.Epoch(), abi.NewTokenAmount(1000), &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   1,
				StepDuration: 1,
				Quantization: 1,
			})
			require.NoError(t, err)
			rt.ReplaceState(st)

			// Set the right epoch for all following tests
			rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
			rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))

			// Too big at version 4
			proveCommit := makeProveCommit(sectorNo)
			proveCommit.Proof = make([]byte, 1920)
			actor.proveCommitSectorAndConfirm(rt, precommit, proveCommit, proveCommitConf{
				vestingPledgeDelta: &test.vestingPledgeDelta,
			})
		})

	}

	t.Run("sector with non-positive lifetime is skipped in confirmation", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)

		sectorNo := abi.SectorNumber(100)
		params := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, nil)
		precommit := actor.preCommitSector(rt, params, preCommitConf{})

		// precommit at correct epoch
		rt.SetEpoch(rt.Epoch() + miner.PreCommitChallengeDelay + 1)
		actor.proveCommitSector(rt, precommit, makeProveCommit(sectorNo))

		// confirm at sector expiration (this probably can't happen)
		rt.SetEpoch(precommit.Info.Expiration)
		// sector skipped but no failure occurs
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")

		// it still skips if sector lifetime is negative
		rt.ClearLogs()
		rt.SetEpoch(precommit.Info.Expiration + 1)
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")

		// it fails up to the miniumum expiration
		rt.ClearLogs()
		rt.SetEpoch(precommit.Info.Expiration - miner.MinSectorExpiration + 1)
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")
		actor.checkState(rt)
	})

	t.Run("fails with too many deals", func(t *testing.T) {
		setup := func(proof abi.RegisteredSealProof) (*mock.Runtime, *actorHarness, *dline.Info) {
			actor := newHarness(t, periodOffset)
			actor.setProofType(proof)
			rt := builderForHarness(actor).
				WithBalance(bigBalance, big.Zero()).
				Build(t)
			rt.SetEpoch(periodOffset + 1)
			actor.constructAndVerify(rt)
			deadline := actor.deadline(rt)
			return rt, actor, deadline
		}

		makeDealIDs := func(n int) []abi.DealID {
			ids := make([]abi.DealID, n)
			for i := range ids {
				ids[i] = abi.DealID(i)
			}
			return ids
		}

		// Make a good commitment for the proof to target.
		sectorNo := abi.SectorNumber(100)

		dealLimits := map[abi.RegisteredSealProof]int{
			abi.RegisteredSealProof_StackedDrg2KiBV1_1:  256,
			abi.RegisteredSealProof_StackedDrg32GiBV1_1: 256,
			abi.RegisteredSealProof_StackedDrg64GiBV1_1: 512,
		}

		for proof, limit := range dealLimits {
			// attempt to pre-commmit a sector with too many sectors
			rt, actor, deadline := setup(proof)
			expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
			precommit := actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit+1))
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too many deals for sector", func() {
				actor.preCommitSector(rt, precommit, preCommitConf{})
			})

			// sector at or below limit succeeds
			rt, actor, _ = setup(proof)
			precommit = actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit))
			actor.preCommitSector(rt, precommit, preCommitConf{})
			actor.checkState(rt)
		}
	})

	t.Run("precommit checks seal proof version", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		// Create miner before version 7
		rt.SetNetworkVersion(network.Version6)
		actor.constructAndVerify(rt)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		// Before version 7, V1 ok, V1_1 rejected
		{
			pc := actor.makePreCommit(101, challengeEpoch, expiration, nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, pc, preCommitConf{})
			})
			rt.Reset()
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1
			actor.preCommitSector(rt, pc, preCommitConf{})
		}
		{
			// At version 7, both are accepted
			rt.SetNetworkVersion(network.Version7)
			pc := actor.makePreCommit(102, challengeEpoch, expiration, nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1
			actor.preCommitSector(rt, pc, preCommitConf{})
			pc3 := actor.makePreCommit(103, challengeEpoch, expiration, nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1
			actor.preCommitSector(rt, pc3, preCommitConf{})
		}
		{
			// After version 7, only V1_1 accepted
			rt.SetNetworkVersion(network.Version8)
			pc := actor.makePreCommit(104, challengeEpoch, expiration, nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, pc, preCommitConf{})
			})
			rt.Reset()
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1
			actor.preCommitSector(rt, pc, preCommitConf{})
		}

		actor.checkState(rt)
	})
}

// Test sector lifecycle when a sector is upgraded
func TestCCUpgrade(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	t.Run("valid committed capacity upgrade", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// Move the current epoch forward so that the first deadline is a stable candidate for both sectors
		rt.SetEpoch(periodOffset + miner.WPoStChallengeWindow)

		// Commit a sector to upgrade
		// Use the max sector number to make sure everything works.
		oldSector := actor.commitAndProveSector(rt, abi.MaxSectorNumber, defaultSectorExpiration, nil)

		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, oldSector)

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// Reduce the epoch reward so that a new sector's initial pledge would otherwise be lesser.
		// It has to be reduced quite a lot to overcome the new sector having more power due to verified deal weight.
		actor.epochRewardSmooth = smoothing.TestingConstantEstimate(big.Div(actor.epochRewardSmooth.Estimate(), big.NewInt(20)))

		challengeEpoch := rt.Epoch() - 1
		upgradeParams := actor.makePreCommit(200, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams.ReplaceCapacity = true
		upgradeParams.ReplaceSectorDeadline = dlIdx
		upgradeParams.ReplaceSectorPartition = partIdx
		upgradeParams.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade := actor.preCommitSector(rt, upgradeParams, preCommitConf{})

		// Check new pre-commit in state
		assert.True(t, upgrade.Info.ReplaceCapacity)
		assert.Equal(t, upgradeParams.ReplaceSectorNumber, upgrade.Info.ReplaceSectorNumber)

		// Old sector is unchanged
		oldSectorAgain := actor.getSector(rt, oldSector.SectorNumber)
		assert.Equal(t, oldSector, oldSectorAgain)

		// Deposit and pledge as expected
		st = getState(rt)
		assert.Equal(t, st.PreCommitDeposits, upgrade.PreCommitDeposit)
		assert.Equal(t, st.InitialPledge, oldSector.InitialPledge)

		// Prove new sector
		rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
		newSector := actor.proveCommitSectorAndConfirm(rt, upgrade, makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})

		// Both sectors' deposits are returned, and pledge is committed
		st = getState(rt)
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)
		assert.Equal(t, st.InitialPledge, big.Add(oldSector.InitialPledge, newSector.InitialPledge))
		// new sector pledge is max of computed pledge and pledge from old sector
		assert.Equal(t, oldSector.InitialPledge, newSector.InitialPledge)

		// Both sectors are present (in the same deadline/partition).
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, partIdx)
		assert.Equal(t, uint64(2), deadline.TotalSectors)
		assert.Equal(t, uint64(2), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.EarlyTerminations)

		assertBitfieldEquals(t, partition.Sectors, uint64(newSector.SectorNumber), uint64(oldSector.SectorNumber))
		assertEmptyBitfield(t, partition.Faults)
		assertEmptyBitfield(t, partition.Recoveries)
		assertEmptyBitfield(t, partition.Terminated)

		// The old sector's expiration has changed to the end of this proving deadline.
		// The new one expires when the old one used to.
		// The partition is registered with an expiry at both epochs.
		dQueue := actor.collectDeadlineExpirations(rt, deadline)
		dlInfo := miner.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rt.Epoch())
		quantizedExpiration := miner.QuantSpecForDeadline(dlInfo).QuantizeUp(oldSector.Expiration)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			dlInfo.NextNotElapsed().Last(): {uint64(0)},
			quantizedExpiration:            {uint64(0)},
		}, dQueue)

		pQueue := actor.collectPartitionExpirations(rt, partition)
		assertBitfieldEquals(t, pQueue[dlInfo.NextNotElapsed().Last()].OnTimeSectors, uint64(oldSector.SectorNumber))
		assertBitfieldEquals(t, pQueue[quantizedExpiration].OnTimeSectors, uint64(newSector.SectorNumber))

		// Roll forward to the beginning of the next iteration of this deadline
		advanceToEpochWithCron(rt, actor, dlInfo.NextNotElapsed().Open)

		// Fail to submit PoSt. This means that both sectors will be detected faulty.
		// Expect the old sector to be marked as terminated.
		bothSectors := []*miner.SectorOnChainInfo{oldSector, newSector}
		lostPower := actor.powerPairForSectors(bothSectors[:1]).Neg() // new sector not active yet.
		faultExpiration := miner.QuantSpecForDeadline(dlInfo).QuantizeUp(dlInfo.NextNotElapsed().Last() + miner.FaultMaxAge)

		advanceDeadline(rt, actor, &cronConfig{
			detectedFaultsPowerDelta:  &lostPower,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
		})

		// The old sector is marked as terminated
		st = getState(rt)
		deadline, partition = actor.getDeadlineAndPartition(rt, dlIdx, partIdx)
		assert.Equal(t, uint64(2), deadline.TotalSectors)
		assert.Equal(t, uint64(1), deadline.LiveSectors)
		assertBitfieldEquals(t, partition.Sectors, uint64(newSector.SectorNumber), uint64(oldSector.SectorNumber))
		assertBitfieldEquals(t, partition.Terminated, uint64(oldSector.SectorNumber))
		assertBitfieldEquals(t, partition.Faults, uint64(newSector.SectorNumber))
		newSectorPower := miner.PowerForSector(actor.sectorSize, newSector)
		assert.True(t, newSectorPower.Equals(partition.LivePower))
		assert.True(t, newSectorPower.Equals(partition.FaultyPower))

		// we expect the partition expiration to be scheduled twice, once early
		// and once on-time (inside the partition, the sector is scheduled only once).
		dQueue = actor.collectDeadlineExpirations(rt, deadline)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			miner.QuantSpecForDeadline(dlInfo).QuantizeUp(newSector.Expiration): {uint64(0)},
			faultExpiration: {uint64(0)},
		}, dQueue)

		// Old sector gone from pledge requirement and deposit
		assert.Equal(t, st.InitialPledge, newSector.InitialPledge)
		actor.checkState(rt)
	})

	t.Run("invalid committed capacity upgrade rejected", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// Commit sectors to target upgrade. The first has no deals, the second has a deal.
		oldSectors := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, [][]abi.DealID{nil, {10}})

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSectors[0].SectorNumber)
		require.NoError(t, err)

		challengeEpoch := rt.Epoch() - 1
		upgradeParams := actor.makePreCommit(200, challengeEpoch, oldSectors[0].Expiration, []abi.DealID{20})
		upgradeParams.ReplaceCapacity = true
		upgradeParams.ReplaceSectorDeadline = dlIdx
		upgradeParams.ReplaceSectorPartition = partIdx
		upgradeParams.ReplaceSectorNumber = oldSectors[0].SectorNumber

		{ // Must have deals
			params := *upgradeParams
			params.DealIDs = nil
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, &params, preCommitConf{})
			})
			rt.Reset()
		}
		{ // Old sector cannot have deals
			params := *upgradeParams
			params.ReplaceSectorNumber = oldSectors[1].SectorNumber
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, &params, preCommitConf{})
			})
			rt.Reset()
		}
		{ // Target sector must exist
			params := *upgradeParams
			params.ReplaceSectorNumber = 999
			rt.ExpectAbort(exitcode.ErrNotFound, func() {
				actor.preCommitSector(rt, &params, preCommitConf{})
			})
			rt.Reset()
		}
		{ // Target partition must exist
			params := *upgradeParams
			params.ReplaceSectorPartition = 999
			rt.ExpectAbortContainsMessage(exitcode.ErrNotFound, "no partition 999", func() {
				actor.preCommitSector(rt, &params, preCommitConf{})
			})
			rt.Reset()
		}
		{ // Expiration must not be sooner than target
			params := *upgradeParams
			params.Expiration = params.Expiration - miner.WPoStProvingPeriod
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, &params, preCommitConf{})
			})
			rt.Reset()
		}
		{ // Target must not be faulty
			params := *upgradeParams
			st := getState(rt)
			prevState := *st
			quant := st.QuantSpecForDeadline(dlIdx)
			deadlines, err := st.LoadDeadlines(rt.AdtStore())
			require.NoError(t, err)
			deadline, err := deadlines.LoadDeadline(rt.AdtStore(), dlIdx)
			require.NoError(t, err)
			partitions, err := deadline.PartitionsArray(rt.AdtStore())
			require.NoError(t, err)
			var partition miner.Partition
			found, err := partitions.Get(partIdx, &partition)
			require.True(t, found)
			require.NoError(t, err)
			sectorArr, err := miner.LoadSectors(rt.AdtStore(), st.Sectors)
			require.NoError(t, err)
			newFaults, _, _, err := partition.DeclareFaults(rt.AdtStore(), sectorArr, bf(uint64(oldSectors[0].SectorNumber)), 100000,
				actor.sectorSize, quant)
			require.NoError(t, err)
			assertBitfieldEquals(t, newFaults, uint64(oldSectors[0].SectorNumber))
			require.NoError(t, partitions.Set(partIdx, &partition))
			deadline.Partitions, err = partitions.Root()
			require.NoError(t, err)
			deadlines.Due[dlIdx] = rt.StorePut(deadline)
			require.NoError(t, st.SaveDeadlines(rt.AdtStore(), deadlines))
			// Phew!

			rt.ReplaceState(st)
			rt.ExpectAbort(exitcode.ErrForbidden, func() {
				actor.preCommitSector(rt, &params, preCommitConf{})
			})
			rt.ReplaceState(&prevState)
			rt.Reset()
		}

		{ // Target must not be terminated
			params := *upgradeParams
			st := getState(rt)
			prevState := *st
			quant := st.QuantSpecForDeadline(dlIdx)
			deadlines, err := st.LoadDeadlines(rt.AdtStore())
			require.NoError(t, err)
			deadline, err := deadlines.LoadDeadline(rt.AdtStore(), dlIdx)
			require.NoError(t, err)
			partitions, err := deadline.PartitionsArray(rt.AdtStore())
			require.NoError(t, err)
			var partition miner.Partition
			found, err := partitions.Get(partIdx, &partition)
			require.True(t, found)
			require.NoError(t, err)
			sectorArr, err := miner.LoadSectors(rt.AdtStore(), st.Sectors)
			require.NoError(t, err)
			result, err := partition.TerminateSectors(rt.AdtStore(), sectorArr, rt.Epoch(), bf(uint64(oldSectors[0].SectorNumber)),
				actor.sectorSize, quant)
			require.NoError(t, err)
			assertBitfieldEquals(t, result.OnTimeSectors, uint64(oldSectors[0].SectorNumber))
			require.NoError(t, partitions.Set(partIdx, &partition))
			deadline.Partitions, err = partitions.Root()
			require.NoError(t, err)
			deadlines.Due[dlIdx] = rt.StorePut(deadline)
			require.NoError(t, st.SaveDeadlines(rt.AdtStore(), deadlines))
			// Phew!

			rt.ReplaceState(st)
			rt.ExpectAbort(exitcode.ErrNotFound, func() {
				actor.preCommitSector(rt, &params, preCommitConf{})
			})
			rt.ReplaceState(&prevState)
			rt.Reset()
		}

		// Demonstrate that the params are otherwise ok
		actor.preCommitSector(rt, upgradeParams, preCommitConf{})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("upgrade sector before it is proven", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// Move the current epoch forward so that the first deadline is a stable candidate for both sectors
		rt.SetEpoch(periodOffset + miner.WPoStChallengeWindow)

		// Commit a sector to upgrade
		// Use the max sector number to make sure everything works.
		oldSector := actor.commitAndProveSector(rt, abi.MaxSectorNumber, defaultSectorExpiration, nil)

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// Reduce the epoch reward so that a new sector's initial pledge would otherwise be lesser.
		// It has to be reduced quite a lot to overcome the new sector having more power due to verified deal weight.
		actor.epochRewardSmooth = smoothing.TestingConstantEstimate(big.Div(actor.epochRewardSmooth.Estimate(), big.NewInt(20)))

		challengeEpoch := rt.Epoch() - 1
		upgradeParams := actor.makePreCommit(200, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams.ReplaceCapacity = true
		upgradeParams.ReplaceSectorDeadline = dlIdx
		upgradeParams.ReplaceSectorPartition = partIdx
		upgradeParams.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade := actor.preCommitSector(rt, upgradeParams, preCommitConf{})

		// Check new pre-commit in state
		assert.True(t, upgrade.Info.ReplaceCapacity)
		assert.Equal(t, upgradeParams.ReplaceSectorNumber, upgrade.Info.ReplaceSectorNumber)

		// Old sector is unchanged
		oldSectorAgain := actor.getSector(rt, oldSector.SectorNumber)
		assert.Equal(t, oldSector, oldSectorAgain)

		// Deposit and pledge as expected
		st = getState(rt)
		assert.Equal(t, st.PreCommitDeposits, upgrade.PreCommitDeposit)
		assert.Equal(t, st.InitialPledge, oldSector.InitialPledge)

		// Prove new sector
		rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
		newSector := actor.proveCommitSectorAndConfirm(rt, upgrade, makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})

		// Both sectors' deposits are returned, and pledge is committed
		st = getState(rt)
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)
		assert.Equal(t, st.InitialPledge, big.Add(oldSector.InitialPledge, newSector.InitialPledge))
		// new sector pledge is max of computed pledge and pledge from old sector
		assert.Equal(t, oldSector.InitialPledge, newSector.InitialPledge)

		// Both sectors are present (in the same deadline/partition).
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, partIdx)
		assert.Equal(t, uint64(2), deadline.TotalSectors)
		assert.Equal(t, uint64(2), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.EarlyTerminations)

		assertBitfieldEquals(t, partition.Sectors, uint64(newSector.SectorNumber), uint64(oldSector.SectorNumber))
		assertEmptyBitfield(t, partition.Faults)
		assertEmptyBitfield(t, partition.Recoveries)
		assertEmptyBitfield(t, partition.Terminated)

		// The old sector's expiration has changed to the end of this proving deadline.
		// The new one expires when the old one used to.
		// The partition is registered with an expiry at both epochs.
		dQueue := actor.collectDeadlineExpirations(rt, deadline)
		dlInfo := miner.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rt.Epoch())
		quantizedExpiration := miner.QuantSpecForDeadline(dlInfo).QuantizeUp(oldSector.Expiration)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			dlInfo.NextNotElapsed().Last(): {uint64(0)},
			quantizedExpiration:            {uint64(0)},
		}, dQueue)

		pQueue := actor.collectPartitionExpirations(rt, partition)
		assertBitfieldEquals(t, pQueue[dlInfo.NextNotElapsed().Last()].OnTimeSectors, uint64(oldSector.SectorNumber))
		assertBitfieldEquals(t, pQueue[quantizedExpiration].OnTimeSectors, uint64(newSector.SectorNumber))

		// advance to sector proving deadline
		dlInfo = actor.deadline(rt)
		for dlIdx != dlInfo.Index {
			advanceDeadline(rt, actor, &cronConfig{})
			dlInfo = actor.deadline(rt)
		}

		// PoSt both sectors. They both gain power and no penalties are incurred.
		rt.SetEpoch(dlInfo.Last())
		oldPower := miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), miner.QAPowerForSector(actor.sectorSize, oldSector))
		newPower := miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), miner.QAPowerForSector(actor.sectorSize, newSector))
		expectedPower := oldPower.Add(newPower)
		partitions := []miner.PoStPartition{
			{Index: partIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{newSector, oldSector}, &poStConfig{
			expectedPowerDelta: expectedPower,
		})

		// replaced sector expires at cron, removing its power and pledge.
		expectedPowerDelta := oldPower.Neg()
		actor.onDeadlineCron(rt, &cronConfig{
			expiredSectorsPowerDelta:  &expectedPowerDelta,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)
	})

	t.Run("declare fault for replaced cc upgrade sector doesn't double subtract power", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		oldSector, newSector := actor.commitProveAndUpgradeSector(rt, 100, 200, defaultSectorExpiration, []abi.DealID{1})

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// now declare old sector faulty
		actor.declareFaults(rt, oldSector)

		pIdx := uint64(0)
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, partIdx)
		dQueue := actor.collectDeadlineExpirations(rt, deadline)
		dlInfo := miner.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rt.Epoch())
		expectedReplacedExpiration := miner.QuantSpecForDeadline(dlInfo).QuantizeUp(rt.Epoch() + miner.FaultMaxAge)
		quantizedExpiration := miner.QuantSpecForDeadline(dlInfo).QuantizeUp(oldSector.Expiration)

		// deadling marks expirations for partition at expiration epoch
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			dlInfo.NextNotElapsed().Last(): {pIdx},
			expectedReplacedExpiration:     {pIdx},
			quantizedExpiration:            {pIdx},
		}, dQueue)

		// but partitions expiration set at that epoch is empty
		queue, err := miner.LoadExpirationQueue(rt.AdtStore(), partition.ExpirationsEpochs, miner.QuantSpecForDeadline(dlInfo))
		require.NoError(t, err)
		var es miner.ExpirationSet
		expirationSetNotEmpty, err := queue.Get(uint64(expectedReplacedExpiration), &es)
		require.NoError(t, err)
		assert.False(t, expirationSetNotEmpty)

		// advance to sector proving deadline
		dlInfo = actor.deadline(rt)
		for dlIdx != dlInfo.Index {
			advanceDeadline(rt, actor, &cronConfig{})
			dlInfo = actor.deadline(rt)
		}

		// submit post for new sector. Power is added for new sector and no penalties are paid yet
		rt.SetEpoch(dlInfo.Last())
		newPower := miner.QAPowerForSector(actor.sectorSize, newSector)
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		// Old sector is faulty, so expect new sector twice in PoSt.
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{newSector, newSector}, &poStConfig{
			expectedPowerDelta: miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), newPower),
		})

		// At proving period cron expect to pay declared fee for old sector
		// and to have its pledge requirement deducted indicating it has expired.
		// Importantly, power is NOT removed, because it was taken when fault was declared.
		oldPower := miner.QAPowerForSector(actor.sectorSize, oldSector)
		expectedFee := miner.PledgePenaltyForContinuedFault(actor.epochRewardSmooth, actor.epochQAPowerSmooth, oldPower)
		expectedPowerDelta := miner.NewPowerPairZero()
		actor.applyRewards(rt, bigRewards, big.Zero())
		actor.onDeadlineCron(rt, &cronConfig{
			continuedFaultsPenalty:    expectedFee,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
			expiredSectorsPowerDelta:  &expectedPowerDelta,
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)
	})

	t.Run("skip replaced sector in its last PoSt", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		actor.applyRewards(rt, bigRewards, big.Zero())

		oldSector, newSector := actor.commitProveAndUpgradeSector(rt, 100, 200, defaultSectorExpiration, []abi.DealID{1})

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// advance to sector proving deadline
		dlInfo := actor.deadline(rt)
		for dlIdx != dlInfo.Index {
			advanceDeadline(rt, actor, &cronConfig{})
			dlInfo = actor.deadline(rt)
		}

		// skip old sector when submitting post. Expect newSector to fill place of old sector in proof.
		// Miner should gain power for new sector, lose power for old sector (with no penalty).
		rt.SetEpoch(dlInfo.Last())

		oldQAPower := miner.QAPowerForSector(actor.sectorSize, oldSector)
		newQAPower := miner.QAPowerForSector(actor.sectorSize, newSector)
		expectedPowerDelta := miner.NewPowerPair(big.Zero(), big.Sub(newQAPower, oldQAPower))

		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bf(uint64(oldSector.SectorNumber))},
		}
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{newSector, newSector}, &poStConfig{
			expectedPowerDelta: expectedPowerDelta,
		})

		// At proving period cron expect to pay continued fee for old (now faulty) sector
		// and to have its pledge requirement deducted indicating it has expired.
		// Importantly, power is NOT removed, because it was taken when sector was skipped in Windowe PoSt.
		faultFee := miner.PledgePenaltyForContinuedFault(actor.epochRewardSmooth, actor.epochQAPowerSmooth, oldQAPower)

		actor.onDeadlineCron(rt, &cronConfig{
			continuedFaultsPenalty:    faultFee,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)
	})

	t.Run("skip PoSt altogether on replaced sector expiry", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		oldSector, _ := actor.commitProveAndUpgradeSector(rt, 100, 200, defaultSectorExpiration, []abi.DealID{1})

		st := getState(rt)
		dlIdx, _, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// advance to sector proving deadline
		dlInfo := actor.deadline(rt)
		for dlIdx != dlInfo.Index {
			advanceDeadline(rt, actor, &cronConfig{})
			dlInfo = actor.deadline(rt)
		}
		rt.SetEpoch(dlInfo.Last())

		// do not PoSt
		// expect old sector to lose power (new sector hasn't added any yet)
		// both sectors are detected faulty for the first time, with no penalty.
		oldQAPower := miner.QAPowerForSector(actor.sectorSize, oldSector)
		expectedPowerDelta := miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), oldQAPower).Neg()

		// At cron, expect both sectors to be treated as undeclared faults.
		// The replaced sector will expire anyway, so its pledge will be removed.
		actor.onDeadlineCron(rt, &cronConfig{
			expiredSectorsPowerDelta:  &expectedPowerDelta,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)
	})

	t.Run("terminate replaced sector early", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// create sector and upgrade it
		oldSector, newSector := actor.commitProveAndUpgradeSector(rt, 100, 200, defaultSectorExpiration, []abi.DealID{1})

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// now terminate replaced sector
		sectorPower := miner.QAPowerForSector(actor.sectorSize, oldSector)
		expectedFee := miner.PledgePenaltyForTermination(oldSector.ExpectedDayReward, rt.Epoch()-oldSector.Activation,
			oldSector.ExpectedStoragePledge, actor.epochQAPowerSmooth, sectorPower, actor.epochRewardSmooth,
			oldSector.ReplacedDayReward, oldSector.ReplacedSectorAge)
		actor.applyRewards(rt, bigRewards, big.Zero())
		powerDelta, pledgeDelta := actor.terminateSectors(rt, bf(uint64(oldSector.SectorNumber)), expectedFee)

		// power and pledge should have been removed
		assert.Equal(t, miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), sectorPower).Neg(), powerDelta)
		assert.Equal(t, big.Sum(oldSector.InitialPledge.Neg(), expectedFee.Neg()), pledgeDelta)

		// advance to sector proving deadline
		dlInfo := actor.deadline(rt)
		for dlIdx != dlInfo.Index {
			advanceDeadline(rt, actor, &cronConfig{})
			dlInfo = actor.deadline(rt)
		}

		// oldSector is no longer active, so expect newSector twice in validation; once for its proof and once
		// to replace oldSector. Power is added for new sector and no penalties are paid.
		rt.SetEpoch(dlInfo.Last())
		newPower := miner.QAPowerForSector(actor.sectorSize, newSector)
		partitions := []miner.PoStPartition{
			{Index: partIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{newSector, newSector}, &poStConfig{
			expectedPowerDelta: miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), newPower),
		})

		// Nothing interesting happens at cron.
		// Importantly, power and pledge are NOT removed. This happened when sector was terminated
		actor.onDeadlineCron(rt, &cronConfig{
			expectedEnrollment: rt.Epoch() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)
	})

	t.Run("extend a replaced sector's expiration", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// create sector and upgrade it
		oldSector, newSector := actor.commitProveAndUpgradeSector(rt, 100, 200, defaultSectorExpiration, []abi.DealID{1})

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     partIdx,
				Sectors:       bf(uint64(oldSector.SectorNumber)),
				NewExpiration: rt.Epoch() + 250*builtin.EpochsInDay,
			}},
		}
		actor.extendSectors(rt, params)

		// advance to sector proving deadline
		dlInfo := actor.deadline(rt)
		for dlIdx != dlInfo.Index {
			advanceDeadline(rt, actor, &cronConfig{})
			dlInfo = actor.deadline(rt)
		}

		// both sectors are now active and not set to expire
		rt.SetEpoch(dlInfo.Last())
		newPower := miner.QAPowerForSector(actor.sectorSize, newSector)
		partitions := []miner.PoStPartition{
			{Index: partIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{oldSector, newSector}, &poStConfig{
			expectedPowerDelta: miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), newPower),
		})

		// Nothing interesting happens at cron because both sectors are active
		actor.onDeadlineCron(rt, &cronConfig{
			expectedEnrollment: rt.Epoch() + miner.WPoStChallengeWindow,
		})

		actor.checkState(rt)
	})

	t.Run("fault and recover a replaced sector", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// create sector and upgrade it
		oldSector, newSector := actor.commitProveAndUpgradeSector(rt, 100, 200, defaultSectorExpiration, []abi.DealID{1})

		// declare replaced sector faulty
		powerDelta := actor.declareFaults(rt, oldSector)

		// power for old sector should have been removed
		oldQAPower := miner.QAPowerForSector(actor.sectorSize, oldSector)
		oldSectorPower := miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), oldQAPower)
		assert.Equal(t, oldSectorPower.Neg(), powerDelta)

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// recover replaced sector
		actor.declareRecoveries(rt, dlIdx, partIdx, bf(uint64(oldSector.SectorNumber)), big.Zero())

		// advance to sector proving deadline
		dlInfo := actor.deadline(rt)
		for dlIdx != dlInfo.Index {
			advanceDeadline(rt, actor, &cronConfig{})
			dlInfo = actor.deadline(rt)
		}

		// both sectors now need to be proven.
		// Upon success, new sector will gain power and replaced sector will briefly regain power.
		rt.SetEpoch(dlInfo.Last())
		newQAPower := miner.QAPowerForSector(actor.sectorSize, newSector)
		newSectorPower := miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), newQAPower)
		expectedPowerDelta := oldSectorPower.Add(newSectorPower)

		partitions := []miner.PoStPartition{
			{Index: partIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{oldSector, newSector}, &poStConfig{
			expectedPowerDelta: expectedPowerDelta,
		})

		// At cron replaced sector's power is removed because it expires, and its initial pledge is removed
		expectedPowerDelta = oldSectorPower.Neg()
		actor.onDeadlineCron(rt, &cronConfig{
			expiredSectorsPowerDelta:  &expectedPowerDelta,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)
	})

	t.Run("try to upgrade committed capacity sector twice", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// Move the current epoch forward so that the first deadline is a stable candidate for both sectors
		rt.SetEpoch(periodOffset + miner.WPoStChallengeWindow)

		// Commit a sector to upgrade
		// Use the max sector number to make sure everything works.
		oldSector := actor.commitAndProveSector(rt, abi.MaxSectorNumber, defaultSectorExpiration, nil)

		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, oldSector)

		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// Reduce the epoch reward so that a new sector's initial pledge would otherwise be lesser.
		actor.epochRewardSmooth = smoothing.TestingConstantEstimate(big.Div(actor.epochRewardSmooth.Estimate(), big.NewInt(20)))

		challengeEpoch := rt.Epoch() - 1

		// Upgrade 1
		upgradeParams1 := actor.makePreCommit(200, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams1.ReplaceCapacity = true
		upgradeParams1.ReplaceSectorDeadline = dlIdx
		upgradeParams1.ReplaceSectorPartition = partIdx
		upgradeParams1.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade1 := actor.preCommitSector(rt, upgradeParams1, preCommitConf{})

		// Check new pre-commit in state
		assert.True(t, upgrade1.Info.ReplaceCapacity)
		assert.Equal(t, upgradeParams1.ReplaceSectorNumber, upgrade1.Info.ReplaceSectorNumber)

		// Upgrade 2
		upgradeParams2 := actor.makePreCommit(201, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams2.ReplaceCapacity = true
		upgradeParams2.ReplaceSectorDeadline = dlIdx
		upgradeParams2.ReplaceSectorPartition = partIdx
		upgradeParams2.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade2 := actor.preCommitSector(rt, upgradeParams2, preCommitConf{})

		// Check new pre-commit in state
		assert.True(t, upgrade2.Info.ReplaceCapacity)
		assert.Equal(t, upgradeParams2.ReplaceSectorNumber, upgrade2.Info.ReplaceSectorNumber)

		// Old sector is unchanged
		oldSectorAgain := actor.getSector(rt, oldSector.SectorNumber)
		assert.Equal(t, oldSector, oldSectorAgain)

		// Deposit and pledge as expected
		st = getState(rt)
		assert.Equal(t, st.PreCommitDeposits, big.Add(upgrade1.PreCommitDeposit, upgrade2.PreCommitDeposit))
		assert.Equal(t, st.InitialPledge, oldSector.InitialPledge)

		// Prove new sectors
		rt.SetEpoch(upgrade1.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
		actor.proveCommitSector(rt, upgrade1, makeProveCommit(upgrade1.Info.SectorNumber))
		actor.proveCommitSector(rt, upgrade2, makeProveCommit(upgrade2.Info.SectorNumber))

		// confirm both.
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, upgrade1, upgrade2)

		newSector1 := actor.getSector(rt, upgrade1.Info.SectorNumber)
		newSector2 := actor.getSector(rt, upgrade2.Info.SectorNumber)

		// All three sectors pre-commit deposits are released, and have pledge committed.
		st = getState(rt)
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)
		assert.Equal(t, st.InitialPledge, big.Sum(
			oldSector.InitialPledge, newSector1.InitialPledge, newSector2.InitialPledge,
		))
		// Both new sectors' pledge are at least the old sector's pledge
		assert.Equal(t, oldSector.InitialPledge, newSector1.InitialPledge)
		assert.Equal(t, oldSector.InitialPledge, newSector2.InitialPledge)

		// All three sectors are present (in the same deadline/partition).
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, partIdx)
		assert.Equal(t, uint64(3), deadline.TotalSectors)
		assert.Equal(t, uint64(3), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.EarlyTerminations)

		assertBitfieldEquals(t, partition.Sectors,
			uint64(newSector1.SectorNumber),
			uint64(newSector2.SectorNumber),
			uint64(oldSector.SectorNumber))
		assertEmptyBitfield(t, partition.Faults)
		assertEmptyBitfield(t, partition.Recoveries)
		assertEmptyBitfield(t, partition.Terminated)

		// The old sector's expiration has changed to the end of this proving deadline.
		// The new one expires when the old one used to.
		// The partition is registered with an expiry at both epochs.
		dQueue := actor.collectDeadlineExpirations(rt, deadline)
		dlInfo := miner.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rt.Epoch())
		quantizedExpiration := miner.QuantSpecForDeadline(dlInfo).QuantizeUp(oldSector.Expiration)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			dlInfo.NextNotElapsed().Last(): {uint64(0)},
			quantizedExpiration:            {uint64(0)},
		}, dQueue)

		pQueue := actor.collectPartitionExpirations(rt, partition)
		assertBitfieldEquals(t, pQueue[dlInfo.NextNotElapsed().Last()].OnTimeSectors, uint64(oldSector.SectorNumber))
		assertBitfieldEquals(t, pQueue[quantizedExpiration].OnTimeSectors,
			uint64(newSector1.SectorNumber), uint64(newSector2.SectorNumber),
		)

		// Roll forward to the beginning of the next iteration of this deadline
		advanceToEpochWithCron(rt, actor, dlInfo.NextNotElapsed().Open)

		// Fail to submit PoSt. This means that both sectors will be detected faulty (no penalty).
		// Expect the old sector to be marked as terminated.
		allSectors := []*miner.SectorOnChainInfo{oldSector, newSector1, newSector2}
		lostPower := actor.powerPairForSectors(allSectors[:1]).Neg() // new sectors not active yet.
		faultExpiration := miner.QuantSpecForDeadline(dlInfo).QuantizeUp(dlInfo.NextNotElapsed().Last() + miner.FaultMaxAge)

		advanceDeadline(rt, actor, &cronConfig{
			detectedFaultsPowerDelta:  &lostPower,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
		})

		// The old sector is marked as terminated
		st = getState(rt)
		deadline, partition = actor.getDeadlineAndPartition(rt, dlIdx, partIdx)
		assert.Equal(t, uint64(3), deadline.TotalSectors)
		assert.Equal(t, uint64(2), deadline.LiveSectors)
		assertBitfieldEquals(t, partition.Sectors,
			uint64(newSector1.SectorNumber),
			uint64(newSector2.SectorNumber),
			uint64(oldSector.SectorNumber),
		)
		assertBitfieldEquals(t, partition.Terminated, uint64(oldSector.SectorNumber))
		assertBitfieldEquals(t, partition.Faults,
			uint64(newSector1.SectorNumber),
			uint64(newSector2.SectorNumber),
		)
		newPower := miner.PowerForSectors(actor.sectorSize, allSectors[1:])
		assert.True(t, newPower.Equals(partition.LivePower))
		assert.True(t, newPower.Equals(partition.FaultyPower))

		// we expect the expiration to be scheduled twice, once early
		// and once on-time.
		dQueue = actor.collectDeadlineExpirations(rt, deadline)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			miner.QuantSpecForDeadline(dlInfo).QuantizeUp(newSector1.Expiration): {uint64(0)},
			faultExpiration: {uint64(0)},
		}, dQueue)

		// Old sector gone from pledge
		assert.Equal(t, st.InitialPledge, big.Add(newSector1.InitialPledge, newSector2.InitialPledge))
		actor.checkState(rt)
	})

	t.Run("upgrade seal proof type", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		rt.SetNetworkVersion(network.Version7) // Version 7 allows both seal proof types
		actor.constructAndVerify(rt)

		// Commit and prove first sector with V1
		rt.SetEpoch(periodOffset + miner.WPoStChallengeWindow)
		oldSector := actor.commitAndProveSector(rt, 101, defaultSectorExpiration, nil)
		assert.Equal(t, abi.RegisteredSealProof_StackedDrg32GiBV1, oldSector.SealProof)
		advanceAndSubmitPoSts(rt, actor, oldSector)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// Commit and prove upgrade sector with V1_1
		challengeEpoch := rt.Epoch() - 1
		upgradeParams := actor.makePreCommit(102, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1
		upgradeParams.ReplaceCapacity = true
		upgradeParams.ReplaceSectorDeadline = dlIdx
		upgradeParams.ReplaceSectorPartition = pIdx
		upgradeParams.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade := actor.preCommitSector(rt, upgradeParams, preCommitConf{})

		// Prove new sector
		rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
		newSector := actor.proveCommitSectorAndConfirm(rt, upgrade, makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})
		assert.Equal(t, abi.RegisteredSealProof_StackedDrg32GiBV1_1, newSector.SealProof)

		// Both sectors are present (in the same deadline/partition).
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, pIdx)
		assert.Equal(t, uint64(2), deadline.TotalSectors)
		assert.Equal(t, uint64(2), deadline.LiveSectors)
		assertBitfieldEquals(t, partition.Sectors, uint64(newSector.SectorNumber), uint64(oldSector.SectorNumber))

		// Roll forward to the beginning of the next iteration of this deadline
		dlInfo := miner.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rt.Epoch())
		advanceToEpochWithCron(rt, actor, dlInfo.NextNotElapsed().Open)
		dlInfo = actor.deadline(rt)

		// Submit post for both sectors, new sector gains power.
		// Note that this PoSt is over two sectors with different seal proof types (but compatible Window PoSt parameters).
		newPower := miner.PowerForSector(actor.sectorSize, newSector)
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{oldSector, newSector}, &poStConfig{
			expectedPowerDelta: newPower,
		})

		// At deadline cron, old sector expires.
		expectedPowerDelta := miner.PowerForSector(actor.sectorSize, oldSector).Neg()
		actor.onDeadlineCron(rt, &cronConfig{
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
			expiredSectorsPowerDelta:  &expectedPowerDelta,
			expectedEnrollment:        dlInfo.Last() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)
	})
}

func TestWindowPost(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
	precommitEpoch := abi.ChainEpoch(1)
	builder := builderForHarness(actor).
		WithEpoch(precommitEpoch).
		WithBalance(bigBalance, big.Zero())

	t.Run("test proof", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)[0]
		pwr := miner.PowerForSector(actor.sectorSize, sector)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(store, sector.SectorNumber)
		require.NoError(t, err)

		// Skip over deadlines until the beginning of the one with the new sector
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Submit PoSt
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{sector}, &poStConfig{
			expectedPowerDelta: pwr,
		})

		// Verify proof recorded
		deadline := actor.getDeadline(rt, dlIdx)
		assertBitfieldEquals(t, deadline.PostSubmissions, pIdx)

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
		actor.checkState(rt)
	})

	t.Run("test duplicate proof rejected", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)[0]
		pwr := miner.PowerForSector(actor.sectorSize, sector)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(store, sector.SectorNumber)
		require.NoError(t, err)

		// Skip over deadlines until the beginning of the one with the new sector
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Submit PoSt
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{sector}, &poStConfig{
			expectedPowerDelta: pwr,
		})

		// Verify proof recorded
		deadline := actor.getDeadline(rt, dlIdx)
		assertBitfieldEquals(t, deadline.PostSubmissions, pIdx)

		// Submit a duplicate proof for the same partition. This will be rejected because after ignoring the
		// already-proven partition, there are no sectors remaining.
		// The skipped fault declared here has no effect.
		commitRand := abi.Randomness("chaincommitment")
		params := miner.SubmitWindowedPoStParams{
			Deadline: dlIdx,
			Partitions: []miner.PoStPartition{{
				Index:   pIdx,
				Skipped: bf(uint64(sector.SectorNumber)),
			}},
			Proofs:           makePoStProofs(actor.postProofType),
			ChainCommitEpoch: dlinfo.Challenge,
			ChainCommitRand:  commitRand,
		}
		expectQueryNetworkInfo(rt, actor)
		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)

		{
			// Before version 7, the rejection is a side-effect of there being no active sectors after
			// the duplicate is ignored.
			rt.SetNetworkVersion(network.Version6)
			rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)
			rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_PoStChainCommit, dlinfo.Challenge, nil, commitRand)
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "no active sectors", func() {
				rt.Call(actor.a.SubmitWindowedPoSt, &params)
			})
			rt.Reset()
		}

		{
			// From version 7, a duplicate is explicitly rejected.
			rt.SetNetworkVersion(network.Version7)
			rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)
			rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_PoStChainCommit, dlinfo.Challenge, nil, commitRand)
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "partition already proven", func() {
				rt.Call(actor.a.SubmitWindowedPoSt, &params)
			})
			rt.Reset()
		}

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
		actor.checkState(rt)
	})

	t.Run("test duplicate proof rejected with many partitions", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		// Commit more sectors than fit in one partition in every eligible deadline, overflowing to a second partition.
		sectorsToCommit := ((miner.WPoStPeriodDeadlines - 2) * actor.partitionSize) + 1
		sectors := actor.commitAndProveSectors(rt, int(sectorsToCommit), defaultSectorExpiration, nil)
		lastSector := sectors[len(sectors)-1]

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(store, lastSector.SectorNumber)
		require.NoError(t, err)
		require.Equal(t, uint64(2), dlIdx) // Deadlines 0 and 1 are empty (no PoSt needed) because excluded by proximity
		require.Equal(t, uint64(1), pIdx)  // Overflowed from partition 0 to partition 1

		// Skip over deadlines until the beginning of the one with two partitions
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		{
			// Submit PoSt for partition 0 on its own.
			partitions := []miner.PoStPartition{
				{Index: 0, Skipped: bitfield.New()},
			}
			sectorsToProve := sectors[:actor.partitionSize]
			pwr := miner.PowerForSectors(actor.sectorSize, sectorsToProve)
			actor.submitWindowPoSt(rt, dlinfo, partitions, sectorsToProve, &poStConfig{
				expectedPowerDelta: pwr,
			})
			// Verify proof recorded
			deadline := actor.getDeadline(rt, dlIdx)
			assertBitfieldEquals(t, deadline.PostSubmissions, 0)
		}
		{
			// Attempt PoSt for both partitions, thus duplicating proof for partition 0, so rejected
			partitions := []miner.PoStPartition{
				{Index: 0, Skipped: bitfield.New()},
				{Index: 1, Skipped: bitfield.New()},
			}
			sectorsToProve := append(sectors[:actor.partitionSize], lastSector)
			pwr := miner.PowerForSectors(actor.sectorSize, sectorsToProve)

			// Before network version 6, the miner would silently drop the sector infos for the partition already
			// proven. This means that the sectors provided for verification would not match the sectors from
			// which the proof was constructed by the miner worker, so will be rejected.
			rt.SetNetworkVersion(network.Version6)
			sectorsSubmitted := []*miner.SectorOnChainInfo{lastSector} // Doesn't match partitions
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid PoSt", func() {
				actor.submitWindowPoSt(rt, dlinfo, partitions, sectorsSubmitted, &poStConfig{
					expectedPowerDelta: miner.NewPowerPairZero(),
					verificationError:  fmt.Errorf("wrong sectors"),
				})
			})
			rt.Reset()

			// From network version 7, the miner outright rejects attempts to prove a partition twice.
			rt.SetNetworkVersion(network.Version7)
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "partition already proven", func() {
				actor.submitWindowPoSt(rt, dlinfo, partitions, sectorsToProve, &poStConfig{
					expectedPowerDelta: pwr,
				})
			})
			rt.Reset()
		}
		{
			// Submit PoSt for partition 1 on its own is ok.
			partitions := []miner.PoStPartition{
				{Index: 1, Skipped: bitfield.New()},
			}
			sectorsToProve := []*miner.SectorOnChainInfo{lastSector}
			pwr := miner.PowerForSectors(actor.sectorSize, sectorsToProve)
			actor.submitWindowPoSt(rt, dlinfo, partitions, sectorsToProve, &poStConfig{
				expectedPowerDelta: pwr,
			})
			// Verify both proofs now recorded
			deadline := actor.getDeadline(rt, dlIdx)
			assertBitfieldEquals(t, deadline.PostSubmissions, 0, 1)
		}

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
		actor.checkState(rt)
	})

	t.Run("successful recoveries recover power", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		pwr := miner.PowerForSectors(actor.sectorSize, infos)

		actor.applyRewards(rt, bigRewards, big.Zero())
		initialLocked := actor.getLockedFunds(rt)

		// Submit first PoSt to ensure we are sufficiently early to add a fault
		// advance to next proving period
		advanceAndSubmitPoSts(rt, actor, infos[0])

		// advance deadline and declare fault
		advanceDeadline(rt, actor, &cronConfig{})
		actor.declareFaults(rt, infos...)

		// advance a deadline and declare recovery
		advanceDeadline(rt, actor, &cronConfig{})

		// declare recovery
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(infos[0].SectorNumber)), big.Zero())

		// advance to epoch when submitPoSt is due
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Now submit PoSt
		// Power should return for recovered sector.
		cfg := &poStConfig{
			expectedPowerDelta: miner.NewPowerPair(pwr.Raw, pwr.QA),
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)

		// faulty power has been removed, partition no longer has faults or recoveries
		deadline, partition := actor.findSector(rt, infos[0].SectorNumber)
		assert.Equal(t, miner.NewPowerPairZero(), deadline.FaultyPower)
		assert.Equal(t, miner.NewPowerPairZero(), partition.FaultyPower)
		assertBitfieldEmpty(t, partition.Faults)
		assertBitfieldEmpty(t, partition.Recoveries)

		// Next deadline cron does not charge for the fault
		advanceDeadline(rt, actor, &cronConfig{})

		assert.Equal(t, initialLocked, actor.getLockedFunds(rt))
		actor.checkState(rt)
	})

	t.Run("skipped faults adjust power", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil)

		actor.applyRewards(rt, bigRewards, big.Zero())

		// advance to epoch when submitPoSt is due
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		dlIdx2, pIdx2, err := st.FindSector(rt.AdtStore(), infos[1].SectorNumber)
		require.NoError(t, err)
		assert.Equal(t, dlIdx, dlIdx2) // this test will need to change when these are not equal

		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Now submit PoSt with a skipped fault for first sector
		// First sector's power should not be activated.
		powerActive := miner.PowerForSectors(actor.sectorSize, infos[1:])
		cfg := &poStConfig{
			expectedPowerDelta: powerActive,
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bf(uint64(infos[0].SectorNumber))},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)

		// expect continued fault fee to be charged during cron
		faultFee := actor.continuedFaultPenalty(infos[:1])
		dlinfo = advanceDeadline(rt, actor, &cronConfig{continuedFaultsPenalty: faultFee})

		// advance to next proving period, expect no fees
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Attempt to skip second sector
		pwrDelta := miner.PowerForSectors(actor.sectorSize, infos[1:2])

		cfg = &poStConfig{
			expectedPowerDelta: pwrDelta.Neg(),
		}
		partitions = []miner.PoStPartition{
			{Index: pIdx2, Skipped: bf(uint64(infos[1].SectorNumber))},
		}
		// Now all sectors are faulty so there's nothing to prove.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "no active sectors", func() {
			actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)
		})
		rt.Reset()

		// The second sector is detected faulty but pays nothing yet.
		// Expect ongoing fault penalty for only the first, continuing-faulty sector.
		pwrDelta = miner.PowerForSectors(actor.sectorSize, infos[1:2]).Neg()
		faultFee = actor.continuedFaultPenalty(infos[:1])
		advanceDeadline(rt, actor, &cronConfig{
			detectedFaultsPowerDelta: &pwrDelta,
			continuedFaultsPenalty:   faultFee,
		})
		actor.checkState(rt)
	})

	t.Run("skipping all sectors in a partition rejected", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil)

		actor.applyRewards(rt, bigRewards, big.Zero())

		// advance to epoch when submitPoSt is due
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		dlIdx2, pIdx2, err := st.FindSector(rt.AdtStore(), infos[1].SectorNumber)
		require.NoError(t, err)
		assert.Equal(t, dlIdx, dlIdx2) // this test will need to change when these are not equal
		assert.Equal(t, pIdx, pIdx2)   // this test will need to change when these are not equal

		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// PoSt with all sectors skipped fails to validate, leaving power un-activated.
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bf(uint64(infos[0].SectorNumber), uint64(infos[1].SectorNumber))},
		}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.submitWindowPoSt(rt, dlinfo, partitions, infos, nil)
		})
		rt.Reset()

		// These sectors are detected faulty and pay no penalty this time.
		advanceDeadline(rt, actor, &cronConfig{continuedFaultsPenalty: big.Zero()})
		actor.checkState(rt)
	})

	t.Run("skipped recoveries are penalized and do not recover power", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil)

		actor.applyRewards(rt, bigRewards, big.Zero())

		// Submit first PoSt to ensure we are sufficiently early to add a fault
		// advance to next proving period
		advanceAndSubmitPoSts(rt, actor, infos...)

		// advance deadline and declare fault on the first sector
		advanceDeadline(rt, actor, &cronConfig{})
		actor.declareFaults(rt, infos[0])

		// advance a deadline and declare recovery
		advanceDeadline(rt, actor, &cronConfig{})

		// declare recovery
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(infos[0].SectorNumber)), big.Zero())

		// advance to epoch when submitPoSt is due
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Now submit PoSt and skip recovered sector.
		// No power should be returned
		cfg := &poStConfig{
			expectedPowerDelta: miner.NewPowerPairZero(),
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bf(uint64(infos[0].SectorNumber))},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)

		// sector will be charged ongoing fee at proving period cron
		ongoingFee := actor.continuedFaultPenalty(infos[:1])
		advanceDeadline(rt, actor, &cronConfig{continuedFaultsPenalty: ongoingFee})
		actor.checkState(rt)
	})

	t.Run("skipping a fault from the wrong partition is an error", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// create enough sectors that one will be in a different partition
		n := 95
		infos := actor.commitAndProveSectors(rt, n, defaultSectorExpiration, nil)

		// add lots of funds so we can pay penalties without going into debt
		st := getState(rt)
		dlIdx0, pIdx0, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		dlIdx1, pIdx1, err := st.FindSector(rt.AdtStore(), infos[n-1].SectorNumber)
		require.NoError(t, err)

		// if these assertions no longer hold, the test must be changed
		require.LessOrEqual(t, dlIdx0, dlIdx1)
		require.NotEqual(t, pIdx0, pIdx1)

		// advance to deadline when sector is due
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx0 {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Now submit PoSt for partition 1 and skip sector from other partition
		cfg := &poStConfig{
			expectedPowerDelta: miner.NewPowerPairZero(),
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx0, Skipped: bf(uint64(infos[n-1].SectorNumber))},
		}
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "skipped faults contains sectors outside partition", func() {
			actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)
		})
		actor.checkState(rt)
	})
}

func TestProveCommit(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("prove commit aborts if pledge requirement not met", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		// Set the circulating supply high and expected reward low in order to coerce
		// pledge requirements (BR + share of money supply, but capped at 1FIL)
		// to exceed pre-commit deposit (BR only).
		rt.SetCirculatingSupply(big.Mul(big.NewInt(100_000_000), big.NewInt(1e18)))
		actor.epochRewardSmooth = smoothing.TestingConstantEstimate(big.NewInt(1e15))

		// prove one sector to establish collateral and locked funds
		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// preecommit another sector so we may prove it
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		params := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, nil)
		precommit := actor.preCommitSector(rt, params, preCommitConf{})

		// Confirm the unlocked PCD will not cover the new IP
		assert.True(t, sectors[0].InitialPledge.GreaterThan(precommit.PreCommitDeposit))

		// Set balance to exactly cover locked funds.
		st := getState(rt)
		rt.SetBalance(big.Sum(st.PreCommitDeposits, st.InitialPledge, st.LockedFunds))
		info := actor.getInfo(rt)

		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[info.SealProofType] - 1)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(actor.nextSectorNo), proveCommitConf{})
		})
		rt.Reset()

		// succeeds with enough free balance (enough to cover 2x IP)
		rt.SetBalance(big.Sum(st.PreCommitDeposits, st.InitialPledge, st.InitialPledge, st.LockedFunds))
		actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(actor.nextSectorNo), proveCommitConf{})
		actor.checkState(rt)
	})

	t.Run("drop invalid prove commit while processing valid one", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// make two precommits
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		paramsA := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, []abi.DealID{1})
		preCommitA := actor.preCommitSector(rt, paramsA, preCommitConf{})
		sectorNoA := actor.nextSectorNo
		actor.nextSectorNo++
		paramsB := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, []abi.DealID{2})
		preCommitB := actor.preCommitSector(rt, paramsB, preCommitConf{})
		sectorNoB := actor.nextSectorNo

		// handle both prove commits in the same epoch
		info := actor.getInfo(rt)
		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[info.SealProofType] - 1)

		actor.proveCommitSector(rt, preCommitA, makeProveCommit(sectorNoA))
		actor.proveCommitSector(rt, preCommitB, makeProveCommit(sectorNoB))

		conf := proveCommitConf{
			verifyDealsExit: map[abi.SectorNumber]exitcode.ExitCode{
				sectorNoA: exitcode.ErrIllegalArgument,
			},
		}
		actor.confirmSectorProofsValid(rt, conf, preCommitA, preCommitB)
		actor.checkState(rt)
	})
}

func TestDeadlineCron(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("empty periods", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)
		assert.Equal(t, periodOffset-miner.WPoStProvingPeriod, st.ProvingPeriodStart)

		// crons before proving period do nothing
		secondCronEpoch := periodOffset + miner.WPoStProvingPeriod - 1
		dlinfo := actor.deadline(rt)
		for dlinfo.Close < secondCronEpoch {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// The proving period start isn't changed, because the period hadn't started yet.
		st = getState(rt)
		assert.Equal(t, periodOffset, st.ProvingPeriodStart)

		// next cron moves proving period forward and enrolls for next cron
		rt.SetEpoch(dlinfo.Last())
		actor.onDeadlineCron(rt, &cronConfig{
			expectedEnrollment: rt.Epoch() + miner.WPoStChallengeWindow,
		})
		st = getState(rt)
		assert.Equal(t, periodOffset+miner.WPoStProvingPeriod, st.ProvingPeriodStart)
		actor.checkState(rt)
	})

	t.Run("sector expires", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, sectors...)
		activePower := miner.PowerForSectors(actor.sectorSize, sectors)

		st := getState(rt)
		initialPledge := st.InitialPledge
		expiration := sectors[0].Expiration

		// setup state to simulate moving forward all the way to expiry
		dlIdx, _, err := st.FindSector(rt.AdtStore(), sectors[0].SectorNumber)
		require.NoError(t, err)
		expirationPeriod := (expiration/miner.WPoStProvingPeriod + 1) * miner.WPoStProvingPeriod
		st.ProvingPeriodStart = expirationPeriod
		st.CurrentDeadline = dlIdx
		rt.ReplaceState(st)

		// Advance to expiration epoch and expect expiration during cron
		rt.SetEpoch(expiration)
		powerDelta := activePower.Neg()
		// because we skip forward in state the sector is detected faulty, no penalty
		advanceDeadline(rt, actor, &cronConfig{
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
			expiredSectorsPowerDelta:  &powerDelta,
			expiredSectorsPledgeDelta: initialPledge.Neg(),
		})
		actor.checkState(rt)
	})

	t.Run("sector expires and repays fee debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, sectors...)
		activePower := miner.PowerForSectors(actor.sectorSize, sectors)

		st := getState(rt)
		initialPledge := st.InitialPledge
		expiration := sectors[0].Expiration

		// setup state to simulate moving forward all the way to expiry
		dlIdx, _, err := st.FindSector(rt.AdtStore(), sectors[0].SectorNumber)
		require.NoError(t, err)
		expirationPeriod := (expiration/miner.WPoStProvingPeriod + 1) * miner.WPoStProvingPeriod
		st.ProvingPeriodStart = expirationPeriod
		st.CurrentDeadline = dlIdx
		rt.ReplaceState(st)

		// Advance to expiration epoch and expect expiration during cron
		rt.SetEpoch(expiration)
		powerDelta := activePower.Neg()

		// introduce lots of fee debt
		st = getState(rt)
		feeDebt := big.Mul(big.NewInt(400), big.NewInt(1e18))
		st.FeeDebt = feeDebt
		rt.ReplaceState(st)
		// Miner balance = IP, debt repayment covered by unlocked funds
		rt.SetBalance(st.InitialPledge)

		// because we skip forward in state and don't check post, there's no penalty.
		// this is the first time the sector is detected faulty
		advanceDeadline(rt, actor, &cronConfig{
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
			expiredSectorsPowerDelta:  &powerDelta,
			expiredSectorsPledgeDelta: initialPledge.Neg(),
			repaidFeeDebt:             initialPledge, // We repay unlocked IP as fees
		})
		actor.checkState(rt)
	})

	t.Run("detects and penalizes faults", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		activeSectors := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil)
		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, activeSectors...)
		activePower := miner.PowerForSectors(actor.sectorSize, activeSectors)

		unprovenSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		unprovenPower := miner.PowerForSectors(actor.sectorSize, unprovenSectors)

		totalPower := unprovenPower.Add(activePower)
		allSectors := append(activeSectors, unprovenSectors...)

		// add lots of funds so penalties come from vesting funds
		actor.applyRewards(rt, bigRewards, big.Zero())

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), activeSectors[0].SectorNumber)
		require.NoError(t, err)

		// advance to next deadline where we expect the first sectors to appear
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Skip to end of the deadline, cron detects sectors as faulty
		activePowerDelta := activePower.Neg()
		advanceDeadline(rt, actor, &cronConfig{
			detectedFaultsPowerDelta: &activePowerDelta,
		})

		// expect faulty power to be added to state
		deadline := actor.getDeadline(rt, dlIdx)
		assert.True(t, totalPower.Equals(deadline.FaultyPower))

		// advance 3 deadlines
		advanceDeadline(rt, actor, &cronConfig{})
		advanceDeadline(rt, actor, &cronConfig{})
		dlinfo = advanceDeadline(rt, actor, &cronConfig{})

		actor.declareRecoveries(rt, dlIdx, pIdx, sectorInfoAsBitfield(allSectors[1:]), big.Zero())

		// Skip to end of proving period for sectors, cron detects all sectors as faulty
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Un-recovered faults (incl failed recovery) are charged as ongoing faults
		ongoingPwr := miner.PowerForSectors(actor.sectorSize, allSectors)
		ongoingPenalty := miner.PledgePenaltyForContinuedFault(actor.epochRewardSmooth, actor.epochQAPowerSmooth, ongoingPwr.QA)

		advanceDeadline(rt, actor, &cronConfig{
			continuedFaultsPenalty: ongoingPenalty,
		})

		// recorded faulty power is unchanged
		deadline = actor.getDeadline(rt, dlIdx)
		assert.True(t, totalPower.Equals(deadline.FaultyPower))
		checkDeadlineInvariants(t, rt.AdtStore(), deadline, st.QuantSpecForDeadline(dlIdx), actor.sectorSize, allSectors)
		actor.checkState(rt)
	})

	t.Run("test cron run late", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// add lots of funds so we can pay penalties without going into debt
		actor.applyRewards(rt, bigRewards, big.Zero())

		// create enough sectors that one will be in a different partition
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, allSectors...)

		st := getState(rt)
		dlIdx, _, err := st.FindSector(rt.AdtStore(), allSectors[0].SectorNumber)
		require.NoError(t, err)

		// advance to deadline prior to first
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// advance clock well past the end of next period (into next deadline period) without calling cron
		rt.SetEpoch(dlinfo.Last() + miner.WPoStChallengeWindow + 5)

		// run cron and expect all sectors to be detected as faults (no penalty)
		pwr := miner.PowerForSectors(actor.sectorSize, allSectors)

		// power for sectors is removed
		powerDeltaClaim := miner.NewPowerPair(pwr.Raw.Neg(), pwr.QA.Neg())

		// expect next cron to be one deadline period after expected cron for this deadline
		nextCron := dlinfo.Last() + +miner.WPoStChallengeWindow

		actor.onDeadlineCron(rt, &cronConfig{
			expectedEnrollment:       nextCron,
			detectedFaultsPowerDelta: &powerDeltaClaim,
		})
		actor.checkState(rt)
	})
}

func TestDeclareFaults(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("declare fault pays fee at window post", func(t *testing.T) {
		// Get sector into proving state
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		pwr := miner.PowerForSectors(actor.sectorSize, allSectors)

		// add lots of funds so penalties come from vesting funds
		actor.applyRewards(rt, bigRewards, big.Zero())

		// find deadline for sector
		st := getState(rt)
		dlIdx, _, err := st.FindSector(rt.AdtStore(), allSectors[0].SectorNumber)
		require.NoError(t, err)

		// advance to first proving period and submit so we'll have time to declare the fault next cycle
		advanceAndSubmitPoSts(rt, actor, allSectors...)

		// Declare the sector as faulted
		actor.declareFaults(rt, allSectors...)

		// faults are recorded in state
		dl := actor.getDeadline(rt, dlIdx)
		assert.True(t, pwr.Equals(dl.FaultyPower))

		// Skip to end of proving period.
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// faults are charged at ongoing rate and no additional power is removed
		ongoingPwr := miner.PowerForSectors(actor.sectorSize, allSectors)
		ongoingPenalty := miner.PledgePenaltyForContinuedFault(actor.epochRewardSmooth, actor.epochQAPowerSmooth, ongoingPwr.QA)

		advanceDeadline(rt, actor, &cronConfig{
			continuedFaultsPenalty: ongoingPenalty,
		})
		actor.checkState(rt)
	})
}

func TestDeclareRecoveries(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("recovery happy path", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		oneSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// advance to first proving period and submit so we'll have time to declare the fault next cycle
		advanceAndSubmitPoSts(rt, actor, oneSector...)

		// Declare the sector as faulted
		actor.declareFaults(rt, oneSector...)

		// Declare recoveries updates state
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oneSector[0].SectorNumber)
		require.NoError(t, err)
		actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(oneSector[0].SectorNumber)), big.Zero())

		dl := actor.getDeadline(rt, dlIdx)
		p, err := dl.LoadPartition(rt.AdtStore(), pIdx)
		require.NoError(t, err)
		assert.Equal(t, p.Faults, p.Recoveries)
		actor.checkState(rt)
	})

	t.Run("recovery must pay back fee debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		oneSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		// advance to first proving period and submit so we'll have time to declare the fault next cycle
		advanceAndSubmitPoSts(rt, actor, oneSector...)

		// Fault will take miner into fee debt
		st := getState(rt)
		rt.SetBalance(big.Sum(st.PreCommitDeposits, st.InitialPledge, st.LockedFunds))

		actor.declareFaults(rt, oneSector...)

		st = getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oneSector[0].SectorNumber)
		require.NoError(t, err)

		// Skip to end of proving period.
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Can't pay during this deadline so miner goes into fee debt
		ongoingPwr := miner.PowerForSectors(actor.sectorSize, oneSector)
		ff := miner.PledgePenaltyForContinuedFault(actor.epochRewardSmooth, actor.epochQAPowerSmooth, ongoingPwr.QA)
		advanceDeadline(rt, actor, &cronConfig{
			continuedFaultsPenalty: big.Zero(), // fee is instead added to debt
		})

		st = getState(rt)
		assert.Equal(t, ff, st.FeeDebt)

		// Recovery fails when it can't pay back fee debt
		rt.ExpectAbortContainsMessage(exitcode.ErrInsufficientFunds, "unlocked balance can not repay fee debt", func() {
			actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(oneSector[0].SectorNumber)), big.Zero())
		})

		// Recovery pays back fee debt and IP requirements and succeeds
		funds := big.Add(ff, st.InitialPledge)
		rt.SetBalance(funds) // this is how we send funds along with recovery message using mock rt
		actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(oneSector[0].SectorNumber)), ff)

		dl := actor.getDeadline(rt, dlIdx)
		p, err := dl.LoadPartition(rt.AdtStore(), pIdx)
		require.NoError(t, err)
		assert.Equal(t, p.Faults, p.Recoveries)
		st = getState(rt)
		assert.Equal(t, big.Zero(), st.FeeDebt)
		actor.checkState(rt)
	})

	t.Run("recovery fails during active consensus fault", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		oneSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// consensus fault
		actor.reportConsensusFault(rt, addr.TestAddress, rt.Epoch()-1)

		// advance to first proving period and submit so we'll have time to declare the fault next cycle
		advanceAndSubmitPoSts(rt, actor, oneSector...)

		// Declare the sector as faulted
		actor.declareFaults(rt, oneSector...)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oneSector[0].SectorNumber)
		require.NoError(t, err)
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "recovery not allowed during active consensus fault", func() {
			actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(oneSector[0].SectorNumber)), big.Zero())
		})
		actor.checkState(rt)
	})
}

func TestExtendSectorExpiration(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	precommitEpoch := abi.ChainEpoch(1)
	builder := builderForHarness(actor).
		WithEpoch(precommitEpoch).
		WithBalance(bigBalance, big.Zero())

	commitSector := func(t *testing.T, rt *mock.Runtime) *miner.SectorOnChainInfo {
		actor.constructAndVerify(rt)
		sectorInfo := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		return sectorInfo[0]
	}

	t.Run("rejects negative extension", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)

		// attempt to shorten epoch
		newExpiration := sector.Expiration - abi.ChainEpoch(miner.WPoStProvingPeriod)

		// find deadline and partition
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sector.SectorNumber)
		require.NoError(t, err)

		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     pIdx,
				Sectors:       bf(uint64(sector.SectorNumber)),
				NewExpiration: newExpiration,
			}},
		}

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, fmt.Sprintf("cannot reduce sector %d's expiration", sector.SectorNumber), func() {
			actor.extendSectors(rt, params)
		})
		actor.checkState(rt)
	})

	t.Run("rejects extension too far in future", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)

		// extend by even proving period after max
		rt.SetEpoch(sector.Expiration)
		extension := miner.WPoStProvingPeriod * (miner.MaxSectorExpirationExtension/miner.WPoStProvingPeriod + 1)
		newExpiration := rt.Epoch() + extension

		// find deadline and partition
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sector.SectorNumber)
		require.NoError(t, err)

		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     pIdx,
				Sectors:       bf(uint64(sector.SectorNumber)),
				NewExpiration: newExpiration,
			}},
		}

		expectedMessage := fmt.Sprintf("cannot be more than %d past current epoch", miner.MaxSectorExpirationExtension)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, expectedMessage, func() {
			actor.extendSectors(rt, params)
		})
		actor.checkState(rt)
	})

	t.Run("rejects extension past max for seal proof", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)
		// and prove it once to activate it.
		advanceAndSubmitPoSts(rt, actor, sector)

		maxLifetime, err := builtin.SealProofSectorMaximumLifetime(sector.SealProof)
		require.NoError(t, err)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sector.SectorNumber)
		require.NoError(t, err)

		// extend sector until just below threshold
		rt.SetEpoch(sector.Expiration)
		extension := abi.ChainEpoch(miner.MinSectorExpiration)
		expiration := sector.Expiration + extension
		for ; expiration-sector.Activation < maxLifetime; expiration += extension {
			params := &miner.ExtendSectorExpirationParams{
				Extensions: []miner.ExpirationExtension{{
					Deadline:      dlIdx,
					Partition:     pIdx,
					Sectors:       bf(uint64(sector.SectorNumber)),
					NewExpiration: expiration,
				}},
			}

			actor.extendSectors(rt, params)
			sector.Expiration = expiration
			rt.SetEpoch(expiration)
		}

		// next extension fails because it extends sector past max lifetime
		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     pIdx,
				Sectors:       bf(uint64(sector.SectorNumber)),
				NewExpiration: expiration,
			}},
		}

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "total sector lifetime", func() {
			actor.extendSectors(rt, params)
		})

		actor.checkState(rt)
	})

	t.Run("updates expiration with valid params", func(t *testing.T) {
		rt := builder.Build(t)
		oldSector := commitSector(t, rt)
		advanceAndSubmitPoSts(rt, actor, oldSector)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		extension := 42 * miner.WPoStProvingPeriod
		newExpiration := oldSector.Expiration + extension
		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     pIdx,
				Sectors:       bf(uint64(oldSector.SectorNumber)),
				NewExpiration: newExpiration,
			}},
		}

		actor.extendSectors(rt, params)

		// assert sector expiration is set to the new value
		newSector := actor.getSector(rt, oldSector.SectorNumber)
		assert.Equal(t, newExpiration, newSector.Expiration)

		quant := st.QuantSpecForDeadline(dlIdx)

		// assert that new expiration exists
		_, partition := actor.getDeadlineAndPartition(rt, dlIdx, pIdx)
		expirationSet, err := partition.PopExpiredSectors(rt.AdtStore(), newExpiration-1, quant)
		require.NoError(t, err)
		empty, err := expirationSet.IsEmpty()
		require.NoError(t, err)
		assert.True(t, empty)

		expirationSet, err = partition.PopExpiredSectors(rt.AdtStore(), quant.QuantizeUp(newExpiration), quant)
		require.NoError(t, err)
		empty, err = expirationSet.IsEmpty()
		require.NoError(t, err)
		assert.False(t, empty)

		actor.checkState(rt)
	})

	t.Run("fails to update deadline expiration queue until nv=7", func(t *testing.T) {
		rt := builder.Build(t)
		rt.SetNetworkVersion(network.Version6)
		// set actor to use proof type valid for nv=6
		proofTypeDefault := actor.sealProofType
		actor.sealProofType = abi.RegisteredSealProof_StackedDrg32GiBV1
		defer func() {
			actor.sealProofType = proofTypeDefault
		}()
		oldSector := commitSector(t, rt)
		advanceAndSubmitPoSts(rt, actor, oldSector)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		extension := 42 * miner.WPoStProvingPeriod
		newExpiration := oldSector.Expiration + extension
		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     pIdx,
				Sectors:       bf(uint64(oldSector.SectorNumber)),
				NewExpiration: newExpiration,
			}},
		}

		actor.extendSectors(rt, params)

		// ExtendSectorExpiration fails to update the deadline expiration queue.
		st = getState(rt)
		_, msgs := miner.CheckStateInvariants(st, rt.AdtStore(), rt.Balance())
		assert.Equal(t, 2, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))
		for _, msg := range msgs.Messages() {
			assert.True(t, strings.Contains(msg, "at epoch 668439"))
		}
	})

	t.Run("updates many sectors", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		const sectorCount = 4000

		// commit a bunch of sectors to ensure that we get multiple partitions.
		sectorInfos := actor.commitAndProveSectors(rt, sectorCount, defaultSectorExpiration, nil)
		advanceAndSubmitPoSts(rt, actor, sectorInfos...)

		newExpiration := sectorInfos[0].Expiration + 42*miner.WPoStProvingPeriod

		var params miner.ExtendSectorExpirationParams

		// extend all odd-numbered sectors.
		{
			st := getState(rt)
			deadlines, err := st.LoadDeadlines(rt.AdtStore())
			require.NoError(t, err)
			require.NoError(t, deadlines.ForEach(rt.AdtStore(), func(dlIdx uint64, dl *miner.Deadline) error {
				partitions, err := dl.PartitionsArray(rt.AdtStore())
				require.NoError(t, err)
				var partition miner.Partition
				require.NoError(t, partitions.ForEach(&partition, func(partIdx int64) error {
					oldSectorNos, err := partition.Sectors.All(miner.AddressedSectorsMax)
					require.NoError(t, err)

					// filter out even-numbered sectors.
					newSectorNos := make([]uint64, 0, len(oldSectorNos)/2)
					for _, sno := range oldSectorNos {
						if sno%2 == 0 {
							continue
						}
						newSectorNos = append(newSectorNos, sno)
					}
					params.Extensions = append(params.Extensions, miner.ExpirationExtension{
						Deadline:      dlIdx,
						Partition:     uint64(partIdx),
						Sectors:       bf(newSectorNos...),
						NewExpiration: newExpiration,
					})
					return nil
				}))
				return nil
			}))
		}

		// Make sure we're touching at least two sectors.
		require.GreaterOrEqual(t, len(params.Extensions), 2,
			"test error: this test should touch more than one partition",
		)

		actor.extendSectors(rt, &params)

		{
			st := getState(rt)
			deadlines, err := st.LoadDeadlines(rt.AdtStore())
			require.NoError(t, err)

			// Half the sectors should expire on-time.
			var onTimeTotal uint64
			require.NoError(t, deadlines.ForEach(rt.AdtStore(), func(dlIdx uint64, dl *miner.Deadline) error {
				expirationSet, err := dl.PopExpiredSectors(rt.AdtStore(), newExpiration-1, st.QuantSpecForDeadline(dlIdx))
				require.NoError(t, err)

				count, err := expirationSet.Count()
				require.NoError(t, err)
				onTimeTotal += count
				return nil
			}))
			assert.EqualValues(t, sectorCount/2, onTimeTotal)

			// Half the sectors should expire late.
			var extendedTotal uint64
			require.NoError(t, deadlines.ForEach(rt.AdtStore(), func(dlIdx uint64, dl *miner.Deadline) error {
				expirationSet, err := dl.PopExpiredSectors(rt.AdtStore(), newExpiration-1, st.QuantSpecForDeadline(dlIdx))
				require.NoError(t, err)

				count, err := expirationSet.Count()
				require.NoError(t, err)
				extendedTotal += count
				return nil
			}))
			assert.EqualValues(t, sectorCount/2, extendedTotal)
		}

		actor.checkState(rt)
	})

	t.Run("supports extensions off deadline boundary", func(t *testing.T) {
		rt := builder.Build(t)
		oldSector := commitSector(t, rt)
		advanceAndSubmitPoSts(rt, actor, oldSector)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		extension := 42*miner.WPoStProvingPeriod + miner.WPoStProvingPeriod/3
		newExpiration := oldSector.Expiration + extension
		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     pIdx,
				Sectors:       bf(uint64(oldSector.SectorNumber)),
				NewExpiration: newExpiration,
			}},
		}

		actor.extendSectors(rt, params)

		// assert sector expiration is set to the new value
		st = getState(rt)
		newSector := actor.getSector(rt, oldSector.SectorNumber)
		assert.Equal(t, newExpiration, newSector.Expiration)

		// advance clock to expiration
		rt.SetEpoch(newSector.Expiration)
		st.ProvingPeriodStart += miner.WPoStProvingPeriod * ((rt.Epoch()-st.ProvingPeriodStart)/miner.WPoStProvingPeriod + 1)
		rt.ReplaceState(st)

		// confirm it is not in sector's deadline
		dlinfo := actor.deadline(rt)
		assert.NotEqual(t, dlIdx, dlinfo.Index)

		// advance to deadline and submit one last PoSt
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{newSector}, nil)

		// advance one more time. No missed PoSt fees are charged. Total Power and pledge are lowered.
		pwr := miner.PowerForSectors(actor.sectorSize, []*miner.SectorOnChainInfo{newSector}).Neg()
		advanceDeadline(rt, actor, &cronConfig{
			expiredSectorsPowerDelta:  &pwr,
			expiredSectorsPledgeDelta: newSector.InitialPledge.Neg(),
		})
		actor.checkState(rt)
	})

	t.Run("prevents extending old seal proof types", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		rt.SetNetworkVersion(network.Version7)
		actor.constructAndVerify(rt)

		// Commit and prove first sector with V1
		rt.SetEpoch(periodOffset + miner.WPoStChallengeWindow)
		sector := actor.commitAndProveSector(rt, 101, defaultSectorExpiration, nil)
		assert.Equal(t, abi.RegisteredSealProof_StackedDrg32GiBV1, sector.SealProof)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sector.SectorNumber)
		require.NoError(t, err)

		newExpiration := sector.Expiration + abi.ChainEpoch(miner.MinSectorExpiration)
		params := &miner.ExtendSectorExpirationParams{
			Extensions: []miner.ExpirationExtension{{
				Deadline:      dlIdx,
				Partition:     pIdx,
				Sectors:       bf(uint64(sector.SectorNumber)),
				NewExpiration: newExpiration,
			}},
		}

		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "unsupported seal type", func() {
			actor.extendSectors(rt, params)
		})
		actor.checkState(rt)
	})
}

func TestTerminateSectors(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(big.Mul(big.NewInt(1e18), big.NewInt(200000)), big.Zero())

	t.Run("removes sector with correct accounting", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		rt.SetEpoch(abi.ChainEpoch(1))
		sectorInfo := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		sector := sectorInfo[0]
		advanceAndSubmitPoSts(rt, actor, sector)

		// A miner will pay the minimum of termination fee and locked funds. Add some locked funds to ensure
		// correct fee calculation is used.
		actor.applyRewards(rt, bigRewards, big.Zero())
		st := getState(rt)
		initialLockedFunds := st.LockedFunds

		sectorSize, err := sector.SealProof.SectorSize()
		require.NoError(t, err)
		sectorPower := miner.QAPowerForSector(sectorSize, sector)
		dayReward := miner.ExpectedRewardForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, builtin.EpochsInDay)
		twentyDayReward := miner.ExpectedRewardForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, miner.InitialPledgeProjectionPeriod)
		sectorAge := rt.Epoch() - sector.Activation
		expectedFee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, actor.epochQAPowerSmooth, sectorPower, actor.epochRewardSmooth, big.Zero(), 0)

		sectors := bf(uint64(sector.SectorNumber))
		actor.terminateSectors(rt, sectors, expectedFee)

		{
			st := getState(rt)

			// expect sector to be marked as terminated and the early termination queue to be empty (having been fully processed)
			_, partition := actor.findSector(rt, sector.SectorNumber)
			terminated, err := partition.Terminated.IsSet(uint64(sector.SectorNumber))
			require.NoError(t, err)
			assert.True(t, terminated)
			result, _, err := partition.PopEarlyTerminations(rt.AdtStore(), 1000)
			require.NoError(t, err)
			assert.True(t, result.IsEmpty())

			// expect fee to have been unlocked and burnt
			assert.Equal(t, big.Sub(initialLockedFunds, expectedFee), st.LockedFunds)

			// expect pledge requirement to have been decremented
			assert.Equal(t, big.Zero(), st.InitialPledge)
		}
		actor.checkState(rt)
	})

	t.Run("charges correct fee for young termination of committed capacity upgrade", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// Move the current epoch forward so that the first deadline is a stable candidate for both sectors
		rt.SetEpoch(periodOffset + miner.WPoStChallengeWindow)

		// Commit a sector to upgrade
		oldSector := actor.commitAndProveSector(rt, 1, defaultSectorExpiration, nil)
		advanceAndSubmitPoSts(rt, actor, oldSector) // activate power
		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// advance clock so upgrade happens later
		rt.SetEpoch(rt.Epoch() + 10_000)

		challengeEpoch := rt.Epoch() - 1
		upgradeParams := actor.makePreCommit(200, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams.ReplaceCapacity = true
		upgradeParams.ReplaceSectorDeadline = dlIdx
		upgradeParams.ReplaceSectorPartition = partIdx
		upgradeParams.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade := actor.preCommitSector(rt, upgradeParams, preCommitConf{})

		// Prove new sector
		rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
		newSector := actor.proveCommitSectorAndConfirm(rt, upgrade, makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})

		// Expect replace parameters have been set
		assert.Equal(t, oldSector.ExpectedDayReward, newSector.ReplacedDayReward)
		assert.Equal(t, rt.Epoch()-oldSector.Activation, newSector.ReplacedSectorAge)

		// post new sector to activate power
		advanceAndSubmitPoSts(rt, actor, oldSector, newSector)

		// advance clock a little and terminate new sector
		rt.SetEpoch(rt.Epoch() + 5_000)

		// Add some locked funds to ensure full termination fee appears as pledge change.
		actor.applyRewards(rt, bigRewards, big.Zero())

		sectorPower := miner.QAPowerForSector(actor.sectorSize, newSector)
		twentyDayReward := miner.ExpectedRewardForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, miner.InitialPledgeProjectionPeriod)
		newSectorAge := rt.Epoch() - newSector.Activation
		oldSectorAge := newSector.Activation - oldSector.Activation
		expectedFee := miner.PledgePenaltyForTermination(newSector.ExpectedDayReward, newSectorAge, twentyDayReward,
			actor.epochQAPowerSmooth, sectorPower, actor.epochRewardSmooth, oldSector.ExpectedDayReward, oldSectorAge)

		sectors := bf(uint64(newSector.SectorNumber))
		actor.terminateSectors(rt, sectors, expectedFee)
		actor.checkState(rt)
	})
}

func TestWithdrawBalance(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	onePercentBalance := big.Div(bigBalance, big.NewInt(100))

	t.Run("happy path withdraws funds", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// withdraw 1% of balance
		actor.withdrawFunds(rt, onePercentBalance, onePercentBalance, big.Zero())
		actor.checkState(rt)
	})

	t.Run("fails if miner can't repay fee debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		st := getState(rt)
		st.FeeDebt = big.Add(rt.Balance(), abi.NewTokenAmount(1e18))
		rt.ReplaceState(st)
		rt.ExpectAbortContainsMessage(exitcode.ErrInsufficientFunds, "unlocked balance can not repay fee debt", func() {
			actor.withdrawFunds(rt, onePercentBalance, onePercentBalance, big.Zero())
		})
		actor.checkState(rt)
	})

	t.Run("withdraw only what we can after fee debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		st := getState(rt)
		feeDebt := big.Sub(bigBalance, onePercentBalance)
		st.FeeDebt = feeDebt
		rt.ReplaceState(st)

		requested := rt.Balance()
		expectedWithdraw := big.Sub(requested, feeDebt)
		actor.withdrawFunds(rt, requested, expectedWithdraw, feeDebt)
		actor.checkState(rt)
	})
}

func TestRepayDebts(t *testing.T) {
	actor := newHarness(t, abi.ChainEpoch(100))
	builder := builderForHarness(actor).
		WithBalance(big.Zero(), big.Zero())

	t.Run("repay with no avaialable funds does nothing", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// introduce fee debt
		st := getState(rt)
		feeDebt := big.Mul(big.NewInt(4), big.NewInt(1e18))
		st.FeeDebt = feeDebt
		rt.ReplaceState(st)

		actor.repayDebt(rt, big.Zero(), big.Zero(), big.Zero())

		st = getState(rt)
		assert.Equal(t, feeDebt, st.FeeDebt)
		actor.checkState(rt)
	})

	t.Run("pay debt entirely from balance", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// introduce fee debt
		st := getState(rt)
		feeDebt := big.Mul(big.NewInt(4), big.NewInt(1e18))
		st.FeeDebt = feeDebt
		rt.ReplaceState(st)

		debtToRepay := big.Mul(big.NewInt(2), feeDebt)
		actor.repayDebt(rt, debtToRepay, big.Zero(), feeDebt)

		st = getState(rt)
		assert.Equal(t, big.Zero(), st.FeeDebt)
		actor.checkState(rt)
	})

	t.Run("partially repay debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// introduce fee debt
		st := getState(rt)
		feeDebt := big.Mul(big.NewInt(4), big.NewInt(1e18))
		st.FeeDebt = feeDebt
		rt.ReplaceState(st)

		debtToRepay := big.Mul(big.NewInt(3), big.Div(feeDebt, big.NewInt(4)))
		actor.repayDebt(rt, debtToRepay, big.Zero(), debtToRepay)

		st = getState(rt)
		assert.Equal(t, big.Div(feeDebt, big.NewInt(4)), st.FeeDebt)
		actor.checkState(rt)
	})

	t.Run("pay debt partially from vested funds", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rewardAmount := big.Mul(big.NewInt(4), big.NewInt(1e18))
		amountLocked, _ := miner.LockedRewardFromReward(rewardAmount, rt.NetworkVersion())
		rt.SetBalance(amountLocked)
		actor.applyRewards(rt, rewardAmount, big.Zero())
		require.Equal(t, amountLocked, actor.getLockedFunds(rt))

		// introduce fee debt
		st := getState(rt)
		feeDebt := big.Mul(big.NewInt(4), big.NewInt(1e18))
		st.FeeDebt = feeDebt
		rt.ReplaceState(st)

		// send 1 FIL and repay all debt from vesting funds and balance
		actor.repayDebt(rt,
			big.NewInt(1e18), // send 1 FIL
			amountLocked,     // 3 FIL comes from vesting funds
			big.NewInt(1e18)) // 1 FIL sent from balance

		st = getState(rt)
		assert.Equal(t, big.Zero(), st.FeeDebt)
		actor.checkState(rt)
	})
}

func TestChangePeerID(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("successfully change peer id", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		newPID := tutil.MakePID("test-change-peer-id")
		actor.changePeerID(rt, newPID)
		actor.checkState(rt)
	})
}

func TestCompactPartitions(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	assertSectorExists := func(store adt.Store, st *miner.State, sectorNum abi.SectorNumber, expectPart uint64, expectDeadline uint64) {
		_, found, err := st.GetSector(store, sectorNum)
		require.NoError(t, err)
		require.True(t, found)

		deadline, pid, err := st.FindSector(store, sectorNum)
		require.NoError(t, err)
		require.EqualValues(t, expectPart, pid)
		require.EqualValues(t, expectDeadline, deadline)
	}

	assertSectorNotFound := func(store adt.Store, st *miner.State, sectorNum abi.SectorNumber) {
		_, found, err := st.GetSector(store, sectorNum)
		require.NoError(t, err)
		require.False(t, found)

		_, _, err = st.FindSector(store, sectorNum)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not due at any deadline")
	}

	t.Run("compacting a partition with both live and dead sectors removes the dead sectors but retains the live sectors", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(200)
		// create 4 sectors in partition 0
		info := actor.commitAndProveSectors(rt, 4, defaultSectorExpiration, [][]abi.DealID{{10}, {20}, {30}, {40}})

		advanceAndSubmitPoSts(rt, actor, info...) // prove and activate power.

		sector1 := info[0].SectorNumber
		sector2 := info[1].SectorNumber
		sector3 := info[2].SectorNumber
		sector4 := info[3].SectorNumber

		// terminate sector1
		rt.SetEpoch(rt.Epoch() + 100)
		actor.applyRewards(rt, bigRewards, big.Zero())
		tsector := info[0]
		sectorSize, err := tsector.SealProof.SectorSize()
		require.NoError(t, err)
		sectorPower := miner.QAPowerForSector(sectorSize, tsector)
		dayReward := miner.ExpectedRewardForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, builtin.EpochsInDay)
		twentyDayReward := miner.ExpectedRewardForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, miner.InitialPledgeProjectionPeriod)
		sectorAge := rt.Epoch() - tsector.Activation
		expectedFee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, actor.epochQAPowerSmooth,
			sectorPower, actor.epochRewardSmooth, big.Zero(), 0)

		sectors := bitfield.NewFromSet([]uint64{uint64(sector1)})
		actor.terminateSectors(rt, sectors, expectedFee)

		// compacting partition will remove sector1 but retain sector 2, 3 and 4.
		partId := uint64(0)
		deadlineId := uint64(0)
		partitions := bitfield.NewFromSet([]uint64{partId})
		actor.compactPartitions(rt, deadlineId, partitions)

		st := getState(rt)
		assertSectorExists(rt.AdtStore(), st, sector2, partId, deadlineId)
		assertSectorExists(rt.AdtStore(), st, sector3, partId, deadlineId)
		assertSectorExists(rt.AdtStore(), st, sector4, partId, deadlineId)

		assertSectorNotFound(rt.AdtStore(), st, sector1)
		actor.checkState(rt)
	})

	t.Run("fail to compact partitions with faults", func(T *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(200)
		// create 2 sectors in partition 0
		info := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, [][]abi.DealID{{10}, {20}})
		advanceAndSubmitPoSts(rt, actor, info...) // prove and activate power.

		// fault sector1
		actor.declareFaults(rt, info[0])

		partId := uint64(0)
		deadlineId := uint64(0)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "failed to remove partitions from deadline 0: while removing partitions: cannot remove partition 0: has faults", func() {
			partitions := bitfield.NewFromSet([]uint64{partId})
			actor.compactPartitions(rt, deadlineId, partitions)
		})
		actor.checkState(rt)
	})

	t.Run("fails to compact partitions with unproven sectors", func(T *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(200)
		// create 2 sectors in partition 0
		actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, [][]abi.DealID{{10}, {20}})

		partId := uint64(0)
		deadlineId := uint64(0)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "failed to remove partitions from deadline 0: while removing partitions: cannot remove partition 0: has unproven sectors", func() {
			partitions := bitfield.NewFromSet([]uint64{partId})
			actor.compactPartitions(rt, deadlineId, partitions)
		})
		actor.checkState(rt)
	})

	t.Run("fails if deadline is equal to WPoStPeriodDeadlines", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.compactPartitions(rt, miner.WPoStPeriodDeadlines, bitfield.New())
		})
		actor.checkState(rt)
	})

	t.Run("fails if deadline is not mutable", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		epoch := abi.ChainEpoch(200)
		rt.SetEpoch(epoch)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.compactPartitions(rt, 1, bitfield.New())
		})
		actor.checkState(rt)
	})

	t.Run("fails if partition count is above limit", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// partition limit is 4 for the default construction
		bf := bitfield.NewFromSet([]uint64{1, 2, 3, 4, 5})

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.compactPartitions(rt, 1, bf)
		})
		actor.checkState(rt)
	})
}

func TestCheckSectorProven(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)

	t.Run("successfully check sector is proven", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, [][]abi.DealID{{10}})

		actor.checkSectorProven(rt, sectors[0].SectorNumber)
		actor.checkState(rt)
	})

	t.Run("fails is sector is not found", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.checkSectorProven(rt, abi.SectorNumber(1))
		})
		actor.checkState(rt)
	})
}

func TestChangeMultiAddrs(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)

	t.Run("successfully change multiaddrs", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		builder := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero())
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		addr1 := abi.Multiaddrs([]byte("addr1"))
		addr2 := abi.Multiaddrs([]byte("addr2"))

		actor.changeMultiAddrs(rt, []abi.Multiaddrs{addr1, addr2})
		actor.checkState(rt)
	})

	t.Run("clear multiaddrs by passing in empty slice", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		builder := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero())
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.changeMultiAddrs(rt, nil)
		actor.checkState(rt)
	})
}

func TestChangeWorkerAddress(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)

	setupFunc := func() (*mock.Runtime, *actorHarness) {
		actor := newHarness(t, periodOffset)
		builder := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero())
		rt := builder.Build(t)

		return rt, actor
	}

	t.Run("successfully change ONLY the worker address", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		originalControlAddrs := actor.controlAddrs

		newWorker := tutil.NewIDAddr(t, 999)

		// set epoch to something close to next deadline so first cron will be before effective date
		currentEpoch := abi.ChainEpoch(2970)
		rt.SetEpoch(currentEpoch)

		effectiveEpoch := currentEpoch + miner.WorkerKeyChangeDelay
		actor.changeWorkerAddress(rt, newWorker, effectiveEpoch, originalControlAddrs)

		// assert change has been made in state
		info := actor.getInfo(rt)
		assert.Equal(t, info.PendingWorkerKey.NewWorker, newWorker)
		assert.Equal(t, info.PendingWorkerKey.EffectiveAt, effectiveEpoch)

		// no change if current epoch is less than effective epoch
		actor.advancePastDeadlineEndWithCron(rt)

		info = actor.getInfo(rt)
		require.NotNil(t, info.PendingWorkerKey)
		require.EqualValues(t, actor.worker, info.Worker)

		// move to deadline containing effectiveEpoch
		advanceToEpochWithCron(rt, actor, effectiveEpoch)

		// run cron to enact worker change
		actor.advancePastDeadlineEndWithCron(rt)

		// assert address has changed
		info = actor.getInfo(rt)
		assert.Equal(t, newWorker, info.Worker)

		// assert control addresses are unchanged
		require.NotEmpty(t, info.ControlAddresses)
		require.Equal(t, originalControlAddrs, info.ControlAddresses)
		actor.checkState(rt)
	})

	t.Run("change cannot be overridden", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		originalControlAddrs := actor.controlAddrs

		newWorker1 := tutil.NewIDAddr(t, 999)
		newWorker2 := tutil.NewIDAddr(t, 1023)

		// set epoch to something close to next deadline so first cron will be before effective date
		currentEpoch := abi.ChainEpoch(2970)
		rt.SetEpoch(currentEpoch)

		effectiveEpoch := currentEpoch + miner.WorkerKeyChangeDelay
		actor.changeWorkerAddress(rt, newWorker1, effectiveEpoch, originalControlAddrs)

		// no change if current epoch is less than effective epoch
		actor.advancePastDeadlineEndWithCron(rt)

		// attempt to change address again
		actor.changeWorkerAddress(rt, newWorker2, rt.Epoch()+miner.WorkerKeyChangeDelay, originalControlAddrs)

		// assert change has not been modified
		info := actor.getInfo(rt)
		assert.Equal(t, info.PendingWorkerKey.NewWorker, newWorker1)
		assert.Equal(t, info.PendingWorkerKey.EffectiveAt, effectiveEpoch)

		advanceToEpochWithCron(rt, actor, effectiveEpoch)
		actor.advancePastDeadlineEndWithCron(rt)

		// assert original change is effected
		info = actor.getInfo(rt)
		assert.Equal(t, newWorker1, info.Worker)
		actor.checkState(rt)
	})

	t.Run("successfully resolve AND change ONLY control addresses", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)

		c1Id := tutil.NewIDAddr(t, 555)

		c2Id := tutil.NewIDAddr(t, 556)
		c2NonId := tutil.NewBLSAddr(t, 999)
		rt.AddIDAddress(c2NonId, c2Id)

		rt.SetAddressActorType(c1Id, builtin.AccountActorCodeID)
		rt.SetAddressActorType(c2Id, builtin.AccountActorCodeID)

		actor.changeWorkerAddress(rt, actor.worker, abi.ChainEpoch(-1), []addr.Address{c1Id, c2NonId})

		// assert there is no worker change request and worker key is unchanged
		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.Equal(t, actor.worker, info.Worker)
		require.Nil(t, info.PendingWorkerKey)
		actor.checkState(rt)
	})

	t.Run("successfully change both worker AND control addresses", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)

		newWorker := tutil.NewIDAddr(t, 999)

		c1 := tutil.NewIDAddr(t, 5001)
		c2 := tutil.NewIDAddr(t, 5002)
		rt.SetAddressActorType(c1, builtin.AccountActorCodeID)
		rt.SetAddressActorType(c2, builtin.AccountActorCodeID)

		currentEpoch := abi.ChainEpoch(5)
		rt.SetEpoch(currentEpoch)
		effectiveEpoch := currentEpoch + miner.WorkerKeyChangeDelay
		actor.changeWorkerAddress(rt, newWorker, effectiveEpoch, []addr.Address{c1, c2})

		// set current epoch the run deadline cron
		rt.SetEpoch(effectiveEpoch)
		advanceToEpochWithCron(rt, actor, effectiveEpoch)
		actor.advancePastDeadlineEndWithCron(rt)

		// assert both worker and control addresses have changed
		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.NotEmpty(t, info.ControlAddresses)
		require.Equal(t, []addr.Address{c1, c2}, info.ControlAddresses)
		require.Equal(t, newWorker, info.Worker)
		actor.checkState(rt)
	})

	t.Run("successfully clear all control addresses", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)

		actor.changeWorkerAddress(rt, actor.worker, abi.ChainEpoch(-1), nil)

		// assert control addresses are cleared
		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.Empty(t, info.ControlAddresses)
		actor.checkState(rt)
	})

	t.Run("fails if control addresses length exceeds maximum limit", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)

		controlAddrs := make([]addr.Address, 0, miner.MaxControlAddresses+1)
		for i := 0; i <= miner.MaxControlAddresses; i++ {
			controlAddrs = append(controlAddrs, tutil.NewIDAddr(t, uint64(i)))
		}

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "control addresses length", func() {
			actor.changeWorkerAddress(rt, actor.worker, abi.ChainEpoch(-1), controlAddrs)
		})
		actor.checkState(rt)
	})

	t.Run("fails if unable to resolve control address", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		c1 := tutil.NewBLSAddr(t, 501)

		rt.SetCaller(actor.owner, builtin.AccountActorCodeID)
		param := &miner.ChangeWorkerAddressParams{NewControlAddrs: []addr.Address{c1}}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.a.ChangeWorkerAddress, param)
		})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("fails if unable to resolve worker address", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		newWorker := tutil.NewBLSAddr(t, 999)
		rt.SetAddressActorType(newWorker, builtin.AccountActorCodeID)

		rt.SetCaller(actor.owner, builtin.AccountActorCodeID)
		param := &miner.ChangeWorkerAddressParams{NewWorker: newWorker}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.a.ChangeWorkerAddress, param)
		})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("fails if worker public key is not BLS", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		newWorker := tutil.NewIDAddr(t, 999)
		rt.SetAddressActorType(newWorker, builtin.AccountActorCodeID)
		key := tutil.NewIDAddr(t, 505)

		rt.ExpectSend(newWorker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &key, exitcode.Ok)

		rt.SetCaller(actor.owner, builtin.AccountActorCodeID)
		param := &miner.ChangeWorkerAddressParams{NewWorker: newWorker}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.a.ChangeWorkerAddress, param)
		})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("fails if new worker address does not have a code", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		newWorker := tutil.NewIDAddr(t, 5001)

		rt.SetCaller(actor.owner, builtin.AccountActorCodeID)
		param := &miner.ChangeWorkerAddressParams{NewWorker: newWorker}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.a.ChangeWorkerAddress, param)
		})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("fails if new worker is not an account actor", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		newWorker := tutil.NewIDAddr(t, 999)
		rt.SetAddressActorType(newWorker, builtin.StorageMinerActorCodeID)

		rt.SetCaller(actor.owner, builtin.AccountActorCodeID)
		param := &miner.ChangeWorkerAddressParams{NewWorker: newWorker}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.a.ChangeWorkerAddress, param)
		})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("fails when caller is not the owner", func(t *testing.T) {
		rt, actor := setupFunc()
		actor.constructAndVerify(rt)
		newWorker := tutil.NewIDAddr(t, 999)
		rt.SetAddressActorType(newWorker, builtin.AccountActorCodeID)

		rt.ExpectValidateCallerAddr(actor.owner)
		rt.ExpectSend(newWorker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &actor.key, exitcode.Ok)

		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		param := &miner.ChangeWorkerAddressParams{NewWorker: newWorker}
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			rt.Call(actor.a.ChangeWorkerAddress, param)
		})
		rt.Verify()
		actor.checkState(rt)
	})
}

func TestConfirmUpdateWorkerKey(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	newWorker := tutil.NewIDAddr(t, 999)
	currentEpoch := abi.ChainEpoch(5)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("successfully changes the worker address", func(t *testing.T) {
		rt := builder.Build(t)
		rt.SetEpoch(currentEpoch)
		actor.constructAndVerify(rt)

		effectiveEpoch := currentEpoch + miner.WorkerKeyChangeDelay
		actor.changeWorkerAddress(rt, newWorker, effectiveEpoch, actor.controlAddrs)

		// confirm at effective epoch
		rt.SetEpoch(effectiveEpoch)
		actor.confirmUpdateWorkerKey(rt)

		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.Equal(t, info.Worker, newWorker)
		require.Nil(t, info.PendingWorkerKey)
		actor.checkState(rt)
	})

	t.Run("does nothing before the effective date", func(t *testing.T) {
		rt := builder.Build(t)
		rt.SetEpoch(currentEpoch)
		actor.constructAndVerify(rt)

		effectiveEpoch := currentEpoch + miner.WorkerKeyChangeDelay
		actor.changeWorkerAddress(rt, newWorker, effectiveEpoch, actor.controlAddrs)

		// confirm right before the effective epoch
		rt.SetEpoch(effectiveEpoch - 1)
		actor.confirmUpdateWorkerKey(rt)

		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.Equal(t, actor.worker, info.Worker)
		require.NotNil(t, info.PendingWorkerKey)
		actor.checkState(rt)
	})

	t.Run("does nothing when no update is set", func(t *testing.T) {
		rt := builder.Build(t)
		rt.SetEpoch(currentEpoch)
		actor.constructAndVerify(rt)

		actor.confirmUpdateWorkerKey(rt)

		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.Equal(t, actor.worker, info.Worker)
		require.Nil(t, info.PendingWorkerKey)
		actor.checkState(rt)
	})
}

func TestChangeOwnerAddress(t *testing.T) {
	actor := newHarness(t, 0)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())
	newAddr := tutil.NewIDAddr(t, 1001)
	otherAddr := tutil.NewIDAddr(t, 1002)

	t.Run("successful change", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetCaller(actor.owner, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, newAddr)

		info := actor.getInfo(rt)
		assert.Equal(t, actor.owner, info.Owner)
		assert.Equal(t, newAddr, *info.PendingOwnerAddress)

		rt.SetCaller(newAddr, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, newAddr)

		info = actor.getInfo(rt)
		assert.Equal(t, newAddr, info.Owner)
		assert.Nil(t, info.PendingOwnerAddress)
	})

	t.Run("proposed must be valid", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		nominees := []addr.Address{
			addr.Undef,
			tutil.NewSECP256K1Addr(t, "asd"),
			tutil.NewBLSAddr(t, 1234),
			tutil.NewActorAddr(t, "asd"),
		}
		rt.SetCaller(actor.owner, builtin.MultisigActorCodeID)
		for _, a := range nominees {
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.changeOwnerAddress(rt, a)
			})
		}
	})

	t.Run("withdraw proposal", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetCaller(actor.owner, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, newAddr)

		// Revert it
		actor.changeOwnerAddress(rt, actor.owner)

		info := actor.getInfo(rt)
		assert.Equal(t, actor.owner, info.Owner)
		assert.Nil(t, info.PendingOwnerAddress)

		// New address cannot confirm.
		rt.SetCaller(newAddr, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.changeOwnerAddress(rt, newAddr)
		})
	})

	t.Run("only owner can propose", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.changeOwnerAddress(rt, newAddr)
		})
		rt.SetCaller(otherAddr, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.changeOwnerAddress(rt, newAddr)
		})
	})

	t.Run("only owner can change proposal", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// Make a proposal
		rt.SetCaller(actor.owner, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, newAddr)

		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.changeOwnerAddress(rt, otherAddr)
		})
		rt.SetCaller(otherAddr, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.changeOwnerAddress(rt, otherAddr)
		})

		// Owner can change it
		rt.SetCaller(actor.owner, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, otherAddr)
		info := actor.getInfo(rt)
		assert.Equal(t, actor.owner, info.Owner)
		assert.Equal(t, otherAddr, *info.PendingOwnerAddress)

	})

	t.Run("only nominee can confirm", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// Make a proposal
		rt.SetCaller(actor.owner, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, newAddr)

		// Owner re-proposing same address doesn't confirm it.
		actor.changeOwnerAddress(rt, newAddr)
		info := actor.getInfo(rt)
		assert.Equal(t, actor.owner, info.Owner)
		assert.Equal(t, newAddr, *info.PendingOwnerAddress) // Still staged

		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.changeOwnerAddress(rt, otherAddr)
		})
		rt.SetCaller(otherAddr, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			actor.changeOwnerAddress(rt, otherAddr)
		})

		// New addr can confirm itself
		rt.SetCaller(newAddr, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, newAddr)
		info = actor.getInfo(rt)
		assert.Equal(t, newAddr, info.Owner)
		assert.Nil(t, info.PendingOwnerAddress)
	})

	t.Run("nominee must confirm self explicitly", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// Make a proposal
		rt.SetCaller(actor.owner, builtin.MultisigActorCodeID)
		actor.changeOwnerAddress(rt, newAddr)

		rt.SetCaller(newAddr, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.changeOwnerAddress(rt, actor.owner) // Not own address
		})
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.changeOwnerAddress(rt, otherAddr) // Not own address
		})
	})
}

func TestReportConsensusFault(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("Report consensus fault pays reward and charges fee", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)
		dealIDs := [][]abi.DealID{{1, 2}, {3, 4}}
		sectorInfo := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, dealIDs)
		_ = sectorInfo

		actor.reportConsensusFault(rt, addr.TestAddress, rt.Epoch()-1)
		actor.checkState(rt)
	})

	t.Run("Report consensus fault updates consensus fault reported field", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)
		dealIDs := [][]abi.DealID{{1, 2}, {3, 4}}

		startInfo := actor.getInfo(rt)
		assert.Equal(t, abi.ChainEpoch(-1), startInfo.ConsensusFaultElapsed)
		sectorInfo := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, dealIDs)
		_ = sectorInfo

		reportEpoch := abi.ChainEpoch(333)
		rt.SetEpoch(reportEpoch)

		actor.reportConsensusFault(rt, addr.TestAddress, rt.Epoch()-1)
		endInfo := actor.getInfo(rt)
		assert.Equal(t, reportEpoch+miner.ConsensusFaultIneligibilityDuration, endInfo.ConsensusFaultElapsed)
		actor.checkState(rt)
	})

	t.Run("Double report of consensus fault fails", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)
		dealIDs := [][]abi.DealID{{1, 2}, {3, 4}}

		startInfo := actor.getInfo(rt)
		assert.Equal(t, abi.ChainEpoch(-1), startInfo.ConsensusFaultElapsed)
		sectorInfo := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, dealIDs)
		_ = sectorInfo

		reportEpoch := abi.ChainEpoch(333)
		rt.SetEpoch(reportEpoch)

		fault1 := rt.Epoch() - 1
		actor.reportConsensusFault(rt, addr.TestAddress, fault1)
		endInfo := actor.getInfo(rt)
		assert.Equal(t, reportEpoch+miner.ConsensusFaultIneligibilityDuration, endInfo.ConsensusFaultElapsed)

		// same fault can't be reported twice
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "too old", func() {
			actor.reportConsensusFault(rt, addr.TestAddress, fault1)
		})
		rt.Reset()

		// new consensus faults are forbidden until original has elapsed
		rt.SetEpoch(endInfo.ConsensusFaultElapsed)
		fault2 := endInfo.ConsensusFaultElapsed - 1
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "too old", func() {
			actor.reportConsensusFault(rt, addr.TestAddress, fault2)
		})
		rt.Reset()

		// a new consensus fault can be reported for blocks once original has expired
		rt.SetEpoch(endInfo.ConsensusFaultElapsed + 1)
		fault3 := endInfo.ConsensusFaultElapsed
		actor.reportConsensusFault(rt, addr.TestAddress, fault3)
		endInfo = actor.getInfo(rt)
		assert.Equal(t, rt.Epoch()+miner.ConsensusFaultIneligibilityDuration, endInfo.ConsensusFaultElapsed)

		// old fault still cannot be reported after fault interval has elapsed
		fault4 := fault1 + 1
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "too old", func() {
			actor.reportConsensusFault(rt, addr.TestAddress, fault4)
		})
		actor.checkState(rt)
	})
}

func TestApplyRewards(t *testing.T) {
	periodOffset := abi.ChainEpoch(1808)
	actor := newHarness(t, periodOffset)

	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("funds are locked", func(t *testing.T) {
		actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
		{
			rt := builder.Build(t)
			rt.SetNetworkVersion(network.Version5)
			actor.constructAndVerify(rt)

			rwd := abi.NewTokenAmount(1_000_000)
			actor.applyRewards(rt, rwd, big.Zero())

			assert.Equal(t, rwd, actor.getLockedFunds(rt))
		}
		{
			rt := builder.Build(t)
			rt.SetNetworkVersion(network.Version6)
			actor.constructAndVerify(rt)

			rwd := abi.NewTokenAmount(1_000_000)
			actor.applyRewards(rt, rwd, big.Zero())

			expected := abi.NewTokenAmount(750_000)
			assert.Equal(t, expected, actor.getLockedFunds(rt))
		}
	})

	actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
	t.Run("funds vest", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)

		vestingFunds, err := st.LoadVestingFunds(adt.AsStore(rt))
		require.NoError(t, err)

		// Nothing vesting to start
		assert.Empty(t, vestingFunds.Funds)
		assert.Equal(t, big.Zero(), st.LockedFunds)

		// Lock some funds with AddLockedFund
		amt := abi.NewTokenAmount(600_000)
		actor.applyRewards(rt, amt, big.Zero())
		st = getState(rt)
		vestingFunds, err = st.LoadVestingFunds(adt.AsStore(rt))
		require.NoError(t, err)

		require.Len(t, vestingFunds.Funds, 180)

		// Vested FIL pays out on epochs with expected offset
		quantSpec := miner.NewQuantSpec(miner.RewardVestingSpec.Quantization, periodOffset)

		currEpoch := rt.Epoch()
		for i := range vestingFunds.Funds {
			step := miner.RewardVestingSpec.InitialDelay + abi.ChainEpoch(i+1)*miner.RewardVestingSpec.StepDuration
			expectedEpoch := quantSpec.QuantizeUp(currEpoch + step)
			vf := vestingFunds.Funds[i]
			assert.Equal(t, expectedEpoch, vf.Epoch)
		}

		expectedOffset := periodOffset % miner.RewardVestingSpec.Quantization
		for i := range vestingFunds.Funds {
			vf := vestingFunds.Funds[i]
			require.EqualValues(t, expectedOffset, int64(vf.Epoch)%int64(miner.RewardVestingSpec.Quantization))
		}

		lockedAmt, _ := miner.LockedRewardFromReward(amt, rt.NetworkVersion())
		assert.Equal(t, lockedAmt, st.LockedFunds)
		actor.checkState(rt)
	})

	t.Run("penalty is burnt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rwd := abi.NewTokenAmount(600_000)
		penalty := abi.NewTokenAmount(300_000)
		rt.SetBalance(big.Add(rt.Balance(), rwd))
		actor.applyRewards(rt, rwd, penalty)

		expectedLockAmt, _ := miner.LockedRewardFromReward(rwd, rt.NetworkVersion())
		expectedLockAmt = big.Sub(expectedLockAmt, penalty)
		assert.Equal(t, expectedLockAmt, actor.getLockedFunds(rt))

		actor.checkState(rt)
	})

	t.Run("penalty is partially burnt and stored as fee debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)
		assert.Equal(t, big.Zero(), st.FeeDebt)

		amt := rt.Balance()
		penalty := big.Mul(big.NewInt(3), amt)
		reward := amt

		// manually update actor balance to include the added funds on reward message
		newBalance := big.Add(reward, amt)
		rt.SetBalance(newBalance)

		rt.SetCaller(builtin.RewardActorAddr, builtin.RewardActorCodeID)
		rt.ExpectValidateCallerAddr(builtin.RewardActorAddr)

		// pledge change is new reward - reward taken for fee debt
		// zero here since all reward goes to debt
		// so do not expect pledge update

		// burn initial balance + reward = 2*amt
		expectBurnt := big.Mul(big.NewInt(2), amt)
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectBurnt, nil, exitcode.Ok)

		rt.Call(actor.a.ApplyRewards, &builtin.ApplyRewardParams{Reward: reward, Penalty: penalty})
		rt.Verify()

		st = getState(rt)
		// fee debt =  penalty - reward - initial balance = 3*amt - 2*amt = amt
		assert.Equal(t, amt, st.FeeDebt)
		actor.checkState(rt)
	})

	// The system should not reach this state since fee debt removes mining eligibility
	// But if invariants are violated this should work.
	t.Run("rewards pay back fee debt ", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)

		assert.Equal(t, big.Zero(), st.LockedFunds)

		amt := rt.Balance()
		availableBefore, err := st.GetAvailableBalance(amt)
		require.NoError(t, err)
		assert.True(t, availableBefore.GreaterThan(big.Zero()))
		initFeeDebt := big.Mul(big.NewInt(2), amt) // FeeDebt twice total balance
		st.FeeDebt = initFeeDebt
		availableAfter, err := st.GetAvailableBalance(amt)
		require.NoError(t, err)
		assert.True(t, availableAfter.LessThan(big.Zero()))

		rt.ReplaceState(st)

		reward := big.Mul(big.NewInt(3), amt)
		penalty := big.Zero()
		// manually update actor balance to include the added funds from outside
		newBalance := big.Add(amt, reward)
		rt.SetBalance(newBalance)

		// pledge change is new reward - reward taken for fee debt
		// 3*LockedRewardFactor*amt - 2*amt = remainingLocked
		lockedReward, _ := miner.LockedRewardFromReward(reward, rt.NetworkVersion())
		remainingLocked := big.Sub(lockedReward, st.FeeDebt) // note that this would be clamped at 0 if difference above is < 0
		pledgeDelta := remainingLocked
		rt.SetCaller(builtin.RewardActorAddr, builtin.RewardActorCodeID)
		rt.ExpectValidateCallerAddr(builtin.RewardActorAddr)
		// expect pledge update
		rt.ExpectSend(
			builtin.StoragePowerActorAddr,
			builtin.MethodsPower.UpdatePledgeTotal,
			&pledgeDelta,
			abi.NewTokenAmount(0),
			nil,
			exitcode.Ok,
		)

		expectBurnt := st.FeeDebt
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectBurnt, nil, exitcode.Ok)

		rt.Call(actor.a.ApplyRewards, &builtin.ApplyRewardParams{Reward: reward, Penalty: penalty})
		rt.Verify()

		// Set balance to deduct fee
		finalBalance := big.Sub(newBalance, expectBurnt)

		st = getState(rt)
		// balance funds used to pay off fee debt
		// available balance should be 2
		availableBalance, err := st.GetAvailableBalance(finalBalance)
		require.NoError(t, err)
		assert.Equal(t, big.Sum(availableBefore, reward, initFeeDebt.Neg(), remainingLocked.Neg()), availableBalance)
		assert.True(t, st.IsDebtFree())
		// remaining funds locked in vesting table
		assert.Equal(t, remainingLocked, st.LockedFunds)
		actor.checkState(rt)
	})
}

func TestCompactSectorNumbers(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("compact sector numbers then pre-commit", func(t *testing.T) {
		// Create a sector.
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		targetSno := allSectors[0].SectorNumber
		actor.compactSectorNumbers(rt, bf(uint64(targetSno), uint64(targetSno)+1))

		precommitEpoch := rt.Epoch()
		deadline := actor.deadline(rt)
		expiration := deadline.PeriodEnd() + abi.ChainEpoch(defaultSectorExpiration)*miner.WPoStProvingPeriod

		// Allocating masked sector number should fail.
		{
			precommit := actor.makePreCommit(targetSno+1, precommitEpoch-1, expiration, nil)
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, precommit, preCommitConf{})
			})
		}

		{
			precommit := actor.makePreCommit(targetSno+2, precommitEpoch-1, expiration, nil)
			actor.preCommitSector(rt, precommit, preCommitConf{})
		}
		actor.checkState(rt)
	})

	t.Run("owner can also compact sectors", func(t *testing.T) {
		// Create a sector.
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		targetSno := allSectors[0].SectorNumber
		rt.SetCaller(actor.owner, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)

		rt.Call(actor.a.CompactSectorNumbers, &miner.CompactSectorNumbersParams{
			MaskSectorNumbers: bf(uint64(targetSno), uint64(targetSno)+1),
		})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("one of the control addresses can also compact sectors", func(t *testing.T) {
		// Create a sector.
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		targetSno := allSectors[0].SectorNumber
		rt.SetCaller(actor.controlAddrs[0], builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)

		rt.Call(actor.a.CompactSectorNumbers, &miner.CompactSectorNumbersParams{
			MaskSectorNumbers: bf(uint64(targetSno), uint64(targetSno)+1),
		})
		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("fail if caller is not among caller worker or control addresses", func(t *testing.T) {
		// Create a sector.
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		targetSno := allSectors[0].SectorNumber
		rAddr := tutil.NewIDAddr(t, 1005)
		rt.SetCaller(rAddr, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)

		rt.ExpectAbort(exitcode.SysErrForbidden, func() {
			rt.Call(actor.a.CompactSectorNumbers, &miner.CompactSectorNumbersParams{
				MaskSectorNumbers: bf(uint64(targetSno), uint64(targetSno)+1),
			})
		})

		rt.Verify()
		actor.checkState(rt)
	})

	t.Run("compacting no sector numbers aborts", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			// compact nothing
			actor.compactSectorNumbers(rt, bf())
		})
		actor.checkState(rt)
	})
}

type actorHarness struct {
	a miner.Actor
	t testing.TB

	receiver addr.Address // The miner actor's own address
	owner    addr.Address
	worker   addr.Address
	key      addr.Address

	controlAddrs []addr.Address

	sealProofType abi.RegisteredSealProof
	postProofType abi.RegisteredPoStProof
	sectorSize    abi.SectorSize
	partitionSize uint64
	periodOffset  abi.ChainEpoch
	nextSectorNo  abi.SectorNumber

	networkPledge   abi.TokenAmount
	networkRawPower abi.StoragePower
	networkQAPower  abi.StoragePower
	baselinePower   abi.StoragePower

	epochRewardSmooth  smoothing.FilterEstimate
	epochQAPowerSmooth smoothing.FilterEstimate
}

func newHarness(t testing.TB, provingPeriodOffset abi.ChainEpoch) *actorHarness {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)

	controlAddrs := []addr.Address{tutil.NewIDAddr(t, 999), tutil.NewIDAddr(t, 998), tutil.NewIDAddr(t, 997)}

	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	rwd := big.Mul(big.NewIntUnsigned(10), big.NewIntUnsigned(1e18))
	pwr := abi.NewStoragePower(1 << 50)
	h := &actorHarness{
		t:        t,
		receiver: receiver,
		owner:    owner,
		worker:   worker,
		key:      workerKey,

		controlAddrs: controlAddrs,

		sealProofType: 0, // Initialized in setProofType
		sectorSize:    0, // Initialized in setProofType
		partitionSize: 0, // Initialized in setProofType
		periodOffset:  provingPeriodOffset,
		nextSectorNo:  100,

		networkPledge:   big.Mul(rwd, big.NewIntUnsigned(1000)),
		networkRawPower: pwr,
		networkQAPower:  pwr,
		baselinePower:   pwr,

		epochRewardSmooth:  smoothing.TestingConstantEstimate(rwd),
		epochQAPowerSmooth: smoothing.TestingConstantEstimate(pwr),
	}
	h.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
	return h
}

func (h *actorHarness) setProofType(proof abi.RegisteredSealProof) {
	var err error
	h.sealProofType = proof
	h.postProofType, err = proof.RegisteredWindowPoStProof()
	require.NoError(h.t, err)
	h.sectorSize, err = proof.SectorSize()
	require.NoError(h.t, err)
	h.partitionSize, err = builtin.SealProofWindowPoStPartitionSectors(proof)
	require.NoError(h.t, err)
}

func (h *actorHarness) constructAndVerify(rt *mock.Runtime) {
	params := miner.ConstructorParams{
		OwnerAddr:     h.owner,
		WorkerAddr:    h.worker,
		ControlAddrs:  h.controlAddrs,
		SealProofType: h.sealProofType,
		PeerId:        testPid,
	}

	rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
	// Fetch worker pubkey.
	rt.ExpectSend(h.worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &h.key, exitcode.Ok)
	// Register proving period cron.
	deadlineEnd := h.periodOffset + ((rt.Epoch()-h.periodOffset)/miner.WPoStChallengeWindow)*miner.WPoStChallengeWindow
	if rt.Epoch() >= h.periodOffset {
		deadlineEnd += miner.WPoStChallengeWindow
	}
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		makeDeadlineCronEventParams(h.t, deadlineEnd-1), big.Zero(), nil, exitcode.Ok)
	rt.SetCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
	ret := rt.Call(h.a.Constructor, &params)
	assert.Nil(h.t, ret)
	rt.Verify()
}

//
// State access helpers
//

func (h *actorHarness) deadline(rt *mock.Runtime) *dline.Info {
	st := getState(rt)
	return st.DeadlineInfo(rt.Epoch())
}

func (h *actorHarness) getPreCommit(rt *mock.Runtime, sno abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	st := getState(rt)
	pc, found, err := st.GetPrecommittedSector(rt.AdtStore(), sno)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return pc
}

func (h *actorHarness) getSector(rt *mock.Runtime, sno abi.SectorNumber) *miner.SectorOnChainInfo {
	st := getState(rt)
	sector, found, err := st.GetSector(rt.AdtStore(), sno)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return sector
}

func (h *actorHarness) getInfo(rt *mock.Runtime) *miner.MinerInfo {
	var st miner.State
	rt.GetState(&st)
	info, err := st.GetInfo(rt.AdtStore())
	require.NoError(h.t, err)
	return info
}

func (h *actorHarness) getDeadlines(rt *mock.Runtime) *miner.Deadlines {
	st := getState(rt)
	deadlines, err := st.LoadDeadlines(rt.AdtStore())
	require.NoError(h.t, err)
	return deadlines
}

func (h *actorHarness) getDeadline(rt *mock.Runtime, idx uint64) *miner.Deadline {
	dls := h.getDeadlines(rt)
	deadline, err := dls.LoadDeadline(rt.AdtStore(), idx)
	require.NoError(h.t, err)
	return deadline
}

func (h *actorHarness) getPartition(rt *mock.Runtime, deadline *miner.Deadline, idx uint64) *miner.Partition {
	partition, err := deadline.LoadPartition(rt.AdtStore(), idx)
	require.NoError(h.t, err)
	return partition
}

func (h *actorHarness) getDeadlineAndPartition(rt *mock.Runtime, dlIdx, pIdx uint64) (*miner.Deadline, *miner.Partition) {
	deadline := h.getDeadline(rt, dlIdx)
	partition := h.getPartition(rt, deadline, pIdx)
	return deadline, partition
}

func (h *actorHarness) findSector(rt *mock.Runtime, sno abi.SectorNumber) (*miner.Deadline, *miner.Partition) {
	var st miner.State
	rt.GetState(&st)
	deadlines, err := st.LoadDeadlines(rt.AdtStore())
	require.NoError(h.t, err)
	dlIdx, pIdx, err := miner.FindSector(rt.AdtStore(), deadlines, sno)
	require.NoError(h.t, err)

	deadline, err := deadlines.LoadDeadline(rt.AdtStore(), dlIdx)
	require.NoError(h.t, err)
	partition, err := deadline.LoadPartition(rt.AdtStore(), pIdx)
	require.NoError(h.t, err)
	return deadline, partition
}

// Collects all sector infos into a map.
func (h *actorHarness) collectSectors(rt *mock.Runtime) map[abi.SectorNumber]*miner.SectorOnChainInfo {
	sectors := map[abi.SectorNumber]*miner.SectorOnChainInfo{}
	st := getState(rt)
	_ = st.ForEachSector(rt.AdtStore(), func(info *miner.SectorOnChainInfo) {
		sector := *info
		sectors[info.SectorNumber] = &sector
	})
	return sectors
}

func (h *actorHarness) collectDeadlineExpirations(rt *mock.Runtime, deadline *miner.Deadline) map[abi.ChainEpoch][]uint64 {
	queue, err := miner.LoadBitfieldQueue(rt.AdtStore(), deadline.ExpirationsEpochs, miner.NoQuantization)
	require.NoError(h.t, err)
	expirations := map[abi.ChainEpoch][]uint64{}
	_ = queue.ForEach(func(epoch abi.ChainEpoch, bf bitfield.BitField) error {
		expanded, err := bf.All(miner.AddressedSectorsMax)
		require.NoError(h.t, err)
		expirations[epoch] = expanded
		return nil
	})
	return expirations
}

func (h *actorHarness) collectPartitionExpirations(rt *mock.Runtime, partition *miner.Partition) map[abi.ChainEpoch]*miner.ExpirationSet {
	queue, err := miner.LoadExpirationQueue(rt.AdtStore(), partition.ExpirationsEpochs, miner.NoQuantization)
	require.NoError(h.t, err)
	expirations := map[abi.ChainEpoch]*miner.ExpirationSet{}
	var es miner.ExpirationSet
	_ = queue.ForEach(&es, func(i int64) error {
		cpy := es
		expirations[abi.ChainEpoch(i)] = &cpy
		return nil
	})
	return expirations
}

func (h *actorHarness) getLockedFunds(rt *mock.Runtime) abi.TokenAmount {
	st := getState(rt)
	return st.LockedFunds
}

func (h *actorHarness) checkState(rt *mock.Runtime) {
	st := getState(rt)
	_, msgs := miner.CheckStateInvariants(st, rt.AdtStore(), rt.Balance())
	assert.True(h.t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))
}

//
// Actor method calls
//

func (h *actorHarness) changeWorkerAddress(rt *mock.Runtime, newWorker addr.Address, effectiveEpoch abi.ChainEpoch, newControlAddrs []addr.Address) {
	rt.SetAddressActorType(newWorker, builtin.AccountActorCodeID)

	param := &miner.ChangeWorkerAddressParams{}
	param.NewControlAddrs = newControlAddrs
	param.NewWorker = newWorker
	rt.ExpectSend(newWorker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &h.key, exitcode.Ok)

	rt.ExpectValidateCallerAddr(h.owner)
	rt.SetCaller(h.owner, builtin.AccountActorCodeID)
	rt.Call(h.a.ChangeWorkerAddress, param)
	rt.Verify()

	st := getState(rt)
	info, err := st.GetInfo(adt.AsStore(rt))
	require.NoError(h.t, err)

	var controlAddrs []addr.Address
	for _, ca := range newControlAddrs {
		resolved, found := rt.GetIdAddr(ca)
		require.True(h.t, found)
		controlAddrs = append(controlAddrs, resolved)
	}
	require.EqualValues(h.t, controlAddrs, info.ControlAddresses)

}

func (h *actorHarness) confirmUpdateWorkerKey(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(h.owner)
	rt.SetCaller(h.owner, builtin.AccountActorCodeID)
	rt.Call(h.a.ConfirmUpdateWorkerKey, nil)
	rt.Verify()
}

func (h *actorHarness) changeOwnerAddress(rt *mock.Runtime, newAddr addr.Address) {
	if rt.Caller() == h.owner {
		rt.ExpectValidateCallerAddr(h.owner)
	} else {
		info := h.getInfo(rt)
		if info.PendingOwnerAddress != nil {
			rt.ExpectValidateCallerAddr(*info.PendingOwnerAddress)
		} else {
			rt.ExpectValidateCallerAddr(h.owner)
		}
	}
	rt.Call(h.a.ChangeOwnerAddress, &newAddr)
	rt.Verify()
}

func (h *actorHarness) checkSectorProven(rt *mock.Runtime, sectorNum abi.SectorNumber) {
	param := &miner.CheckSectorProvenParams{SectorNumber: sectorNum}

	rt.ExpectValidateCallerAny()

	rt.Call(h.a.CheckSectorProven, param)
	rt.Verify()
}

func (h *actorHarness) changeMultiAddrs(rt *mock.Runtime, newAddrs []abi.Multiaddrs) {
	param := &miner.ChangeMultiaddrsParams{NewMultiaddrs: newAddrs}
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)

	rt.Call(h.a.ChangeMultiaddrs, param)
	rt.Verify()

	// assert addrs has changed
	st := getState(rt)
	info, err := st.GetInfo(adt.AsStore(rt))
	require.NoError(h.t, err)
	require.EqualValues(h.t, newAddrs, info.Multiaddrs)
}

func (h *actorHarness) changePeerID(rt *mock.Runtime, newPID abi.PeerID) {
	param := &miner.ChangePeerIDParams{NewID: newPID}
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)

	rt.Call(h.a.ChangePeerID, param)
	rt.Verify()
	st := getState(rt)
	info, err := st.GetInfo(adt.AsStore(rt))
	require.NoError(h.t, err)
	require.EqualValues(h.t, newPID, info.PeerId)
}

func (h *actorHarness) controlAddresses(rt *mock.Runtime) (owner, worker addr.Address, control []addr.Address) {
	rt.ExpectValidateCallerAny()
	ret := rt.Call(h.a.ControlAddresses, nil).(*miner.GetControlAddressesReturn)
	require.NotNil(h.t, ret)
	rt.Verify()
	return ret.Owner, ret.Worker, ret.ControlAddrs
}

// Options for preCommitSector behaviour.
// Default zero values should let everything be ok.
type preCommitConf struct {
	dealWeight         abi.DealWeight
	verifiedDealWeight abi.DealWeight
	dealSpace          abi.SectorSize
	pledgeDelta        *abi.TokenAmount
}

func (h *actorHarness) preCommitSector(rt *mock.Runtime, params *miner.PreCommitSectorParams, conf preCommitConf) *miner.SectorPreCommitOnChainInfo {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	{
		expectQueryNetworkInfo(rt, h)
	}
	if len(params.DealIDs) > 0 {
		// If there are any deal IDs, allocate half the weight to non-verified and half to verified.
		vdParams := market.VerifyDealsForActivationParams{
			DealIDs:      params.DealIDs,
			SectorStart:  rt.Epoch(),
			SectorExpiry: params.Expiration,
		}

		vdReturn := market.VerifyDealsForActivationReturn{
			DealWeight:         conf.dealWeight,
			VerifiedDealWeight: conf.verifiedDealWeight,
			DealSpace:          uint64(conf.dealSpace),
		}
		if vdReturn.DealWeight.Nil() {
			vdReturn.DealWeight = big.Zero()
		}
		if vdReturn.VerifiedDealWeight.Nil() {
			vdReturn.VerifiedDealWeight = big.Zero()
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.VerifyDealsForActivation, &vdParams, big.Zero(), &vdReturn, exitcode.Ok)
	}
	st := getState(rt)

	if conf.pledgeDelta != nil {
		if !conf.pledgeDelta.IsZero() {
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, conf.pledgeDelta, big.Zero(), nil, exitcode.Ok)
		}
	} else if rt.NetworkVersion() < network.Version7 {
		pledgeDelta := immediatelyVestingFunds(rt, st).Neg()
		if !pledgeDelta.IsZero() {
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &pledgeDelta, big.Zero(), nil, exitcode.Ok)
		}
	}

	if st.FeeDebt.GreaterThan(big.Zero()) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, st.FeeDebt, nil, exitcode.Ok)
	}

	rt.Call(h.a.PreCommitSector, params)
	rt.Verify()
	return h.getPreCommit(rt, params.SectorNumber)
}

// Options for proveCommitSector behaviour.
// Default zero values should let everything be ok.
type proveCommitConf struct {
	verifyDealsExit    map[abi.SectorNumber]exitcode.ExitCode
	vestingPledgeDelta *abi.TokenAmount
}

func (h *actorHarness) proveCommitSector(rt *mock.Runtime, precommit *miner.SectorPreCommitOnChainInfo, params *miner.ProveCommitSectorParams) {
	commd := cbg.CborCid(tutil.MakeCID("commd", &market.PieceCIDPrefix))
	sealRand := abi.SealRandomness([]byte{1, 2, 3, 4})
	sealIntRand := abi.InteractiveSealRandomness([]byte{5, 6, 7, 8})
	interactiveEpoch := precommit.PreCommitEpoch + miner.PreCommitChallengeDelay

	// Prepare for and receive call to ProveCommitSector
	{
		cdcParams := market.ComputeDataCommitmentParams{
			DealIDs:    precommit.Info.DealIDs,
			SectorType: precommit.Info.SealProof,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ComputeDataCommitment, &cdcParams, big.Zero(), &commd, exitcode.Ok)
	}
	{
		var buf bytes.Buffer
		receiver := rt.Receiver()
		err := receiver.MarshalCBOR(&buf)
		require.NoError(h.t, err)
		rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_SealRandomness, precommit.Info.SealRandEpoch, buf.Bytes(), abi.Randomness(sealRand))
		rt.ExpectGetRandomnessBeacon(crypto.DomainSeparationTag_InteractiveSealChallengeSeed, interactiveEpoch, buf.Bytes(), abi.Randomness(sealIntRand))
	}
	{
		actorId, err := addr.IDFromAddress(h.receiver)
		require.NoError(h.t, err)
		seal := proof.SealVerifyInfo{
			SectorID: abi.SectorID{
				Miner:  abi.ActorID(actorId),
				Number: precommit.Info.SectorNumber,
			},
			SealedCID:             precommit.Info.SealedCID,
			SealProof:             precommit.Info.SealProof,
			Proof:                 params.Proof,
			DealIDs:               precommit.Info.DealIDs,
			Randomness:            sealRand,
			InteractiveRandomness: sealIntRand,
			UnsealedCID:           cid.Cid(commd),
		}
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.SubmitPoRepForBulkVerify, &seal, abi.NewTokenAmount(0), nil, exitcode.Ok)
	}
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAny()
	rt.Call(h.a.ProveCommitSector, params)
	rt.Verify()
}

func (h *actorHarness) confirmSectorProofsValid(rt *mock.Runtime, conf proveCommitConf, precommits ...*miner.SectorPreCommitOnChainInfo) {
	// expect calls to get network stats
	expectQueryNetworkInfo(rt, h)

	// Prepare for and receive call to ConfirmSectorProofsValid.
	var validPrecommits []*miner.SectorPreCommitOnChainInfo
	var allSectorNumbers []abi.SectorNumber
	for _, precommit := range precommits {
		allSectorNumbers = append(allSectorNumbers, precommit.Info.SectorNumber)
		validPrecommits = append(validPrecommits, precommit)
		if len(precommit.Info.DealIDs) > 0 {
			vdParams := market.ActivateDealsParams{
				DealIDs:      precommit.Info.DealIDs,
				SectorExpiry: precommit.Info.Expiration,
			}
			exit, found := conf.verifyDealsExit[precommit.Info.SectorNumber]
			if found {
				validPrecommits = validPrecommits[:len(validPrecommits)-1] // pop
			} else {
				exit = exitcode.Ok
			}
			rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ActivateDeals, &vdParams, big.Zero(), nil, exit)
		}
	}

	// expected pledge is the sum of initial pledges
	if len(validPrecommits) > 0 {
		expectPledge := big.Zero()

		expectQAPower := big.Zero()
		expectRawPower := big.Zero()
		for _, precommit := range validPrecommits {
			precommitOnChain := h.getPreCommit(rt, precommit.Info.SectorNumber)

			duration := precommit.Info.Expiration - rt.Epoch()
			if duration >= miner.MinSectorExpiration {
				qaPowerDelta := miner.QAPowerForWeight(h.sectorSize, duration, precommitOnChain.DealWeight, precommitOnChain.VerifiedDealWeight)
				expectQAPower = big.Add(expectQAPower, qaPowerDelta)
				expectRawPower = big.Add(expectRawPower, big.NewIntUnsigned(uint64(h.sectorSize)))
				pledge := miner.InitialPledgeForPower(qaPowerDelta, h.baselinePower, h.epochRewardSmooth,
					h.epochQAPowerSmooth, rt.TotalFilCircSupply())

				// if cc upgrade, pledge is max of new and replaced pledges
				if precommitOnChain.Info.ReplaceCapacity {
					replaced := h.getSector(rt, precommitOnChain.Info.ReplaceSectorNumber)
					pledge = big.Max(pledge, replaced.InitialPledge)
				}

				expectPledge = big.Add(expectPledge, pledge)
			}
		}

		if conf.vestingPledgeDelta != nil {
			expectPledge = big.Add(expectPledge, *conf.vestingPledgeDelta)
		}

		if !expectPledge.IsZero() {
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &expectPledge, big.Zero(), nil, exitcode.Ok)
		}
	}

	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	rt.Call(h.a.ConfirmSectorProofsValid, &builtin.ConfirmSectorProofsParams{Sectors: allSectorNumbers})
	rt.Verify()
}

func (h *actorHarness) proveCommitSectorAndConfirm(rt *mock.Runtime, precommit *miner.SectorPreCommitOnChainInfo,
	params *miner.ProveCommitSectorParams, conf proveCommitConf) *miner.SectorOnChainInfo {
	h.proveCommitSector(rt, precommit, params)
	h.confirmSectorProofsValid(rt, conf, precommit)

	newSector := h.getSector(rt, params.SectorNumber)
	return newSector
}

// Pre-commits and then proves a number of sectors.
// The sectors will expire at the end of lifetimePeriods proving periods after now.
// The runtime epoch will be moved forward to the epoch of commitment proofs.
func (h *actorHarness) commitAndProveSectors(rt *mock.Runtime, n int, lifetimePeriods uint64, dealIDs [][]abi.DealID) []*miner.SectorOnChainInfo {
	precommitEpoch := rt.Epoch()
	deadline := h.deadline(rt)
	expiration := deadline.PeriodEnd() + abi.ChainEpoch(lifetimePeriods)*miner.WPoStProvingPeriod

	// Precommit
	precommits := make([]*miner.SectorPreCommitOnChainInfo, n)
	for i := 0; i < n; i++ {
		sectorNo := h.nextSectorNo
		var sectorDealIDs []abi.DealID
		if dealIDs != nil {
			sectorDealIDs = dealIDs[i]
		}
		params := h.makePreCommit(sectorNo, precommitEpoch-1, expiration, sectorDealIDs)
		precommit := h.preCommitSector(rt, params, preCommitConf{})
		precommits[i] = precommit
		h.nextSectorNo++
	}

	advanceToEpochWithCron(rt, h, precommitEpoch+miner.PreCommitChallengeDelay+1)

	info := []*miner.SectorOnChainInfo{}
	for _, pc := range precommits {
		sector := h.proveCommitSectorAndConfirm(rt, pc, makeProveCommit(pc.Info.SectorNumber), proveCommitConf{})
		info = append(info, sector)
	}
	rt.Reset()
	return info
}

func (h *actorHarness) compactSectorNumbers(rt *mock.Runtime, bf bitfield.BitField) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	rt.Call(h.a.CompactSectorNumbers, &miner.CompactSectorNumbersParams{
		MaskSectorNumbers: bf,
	})
	rt.Verify()
}

func (h *actorHarness) commitAndProveSector(rt *mock.Runtime, sectorNo abi.SectorNumber, lifetimePeriods uint64, dealIDs []abi.DealID) *miner.SectorOnChainInfo {
	precommitEpoch := rt.Epoch()
	deadline := h.deadline(rt)
	expiration := deadline.PeriodEnd() + abi.ChainEpoch(lifetimePeriods)*miner.WPoStProvingPeriod

	// Precommit
	preCommitParams := h.makePreCommit(sectorNo, precommitEpoch-1, expiration, dealIDs)
	precommit := h.preCommitSector(rt, preCommitParams, preCommitConf{})

	advanceToEpochWithCron(rt, h, precommitEpoch+miner.PreCommitChallengeDelay+1)

	sectorInfo := h.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(preCommitParams.SectorNumber), proveCommitConf{})
	rt.Reset()
	return sectorInfo
}

func (h *actorHarness) commitProveAndUpgradeSector(rt *mock.Runtime, sectorNo, upgradeSectorNo abi.SectorNumber,
	lifetimePeriods uint64, dealIDs []abi.DealID,
) (oldSector *miner.SectorOnChainInfo, newSector *miner.SectorOnChainInfo) {
	// Move the current epoch forward so that the first deadline is a stable candidate for both sectors
	rt.SetEpoch(h.periodOffset + miner.WPoStChallengeWindow)

	// Commit a sector to upgrade
	// Use the max sector number to make sure everything works.
	oldSector = h.commitAndProveSector(rt, sectorNo, lifetimePeriods, nil)

	// advance cron to activate power.
	advanceAndSubmitPoSts(rt, h, oldSector)

	st := getState(rt)
	dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
	require.NoError(h.t, err)

	// Reduce the epoch reward so that a new sector's initial pledge would otherwise be lesser.
	// It has to be reduced quite a lot to overcome the new sector having more power due to verified deal weight.
	h.epochRewardSmooth = smoothing.TestingConstantEstimate(big.Div(h.epochRewardSmooth.Estimate(), big.NewInt(20)))

	challengeEpoch := rt.Epoch() - 1
	upgradeParams := h.makePreCommit(upgradeSectorNo, challengeEpoch, oldSector.Expiration, dealIDs)
	upgradeParams.ReplaceCapacity = true
	upgradeParams.ReplaceSectorDeadline = dlIdx
	upgradeParams.ReplaceSectorPartition = partIdx
	upgradeParams.ReplaceSectorNumber = oldSector.SectorNumber
	upgrade := h.preCommitSector(rt, upgradeParams, preCommitConf{
		dealWeight:         big.Zero(),
		verifiedDealWeight: big.NewInt(int64(h.sectorSize)),
		dealSpace:          h.sectorSize,
	})

	// Prove new sector
	rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
	newSector = h.proveCommitSectorAndConfirm(rt, upgrade, makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})

	return oldSector, newSector
}

// Deprecated
func (h *actorHarness) advancePastProvingPeriodWithCron(rt *mock.Runtime) {
	st := getState(rt)
	deadline := st.DeadlineInfo(rt.Epoch())
	rt.SetEpoch(deadline.PeriodEnd())
	nextCron := deadline.NextPeriodStart() + miner.WPoStProvingPeriod - 1
	h.onDeadlineCron(rt, &cronConfig{
		expectedEnrollment: nextCron,
	})
	rt.SetEpoch(deadline.NextPeriodStart())
}

func (h *actorHarness) advancePastDeadlineEndWithCron(rt *mock.Runtime) {
	deadline := h.deadline(rt)
	rt.SetEpoch(deadline.PeriodEnd())
	nextCron := deadline.Last() + miner.WPoStChallengeWindow
	h.onDeadlineCron(rt, &cronConfig{
		expectedEnrollment: nextCron,
	})
	rt.SetEpoch(deadline.NextPeriodStart())
}

type poStConfig struct {
	expectedPowerDelta miner.PowerPair
	verificationError  error
}

func (h *actorHarness) submitWindowPoSt(rt *mock.Runtime, deadline *dline.Info, partitions []miner.PoStPartition, infos []*miner.SectorOnChainInfo, poStCfg *poStConfig) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	commitRand := abi.Randomness("chaincommitment")
	rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_PoStChainCommit, deadline.Challenge, nil, commitRand)

	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	proofs := makePoStProofs(h.postProofType)
	challengeRand := abi.SealRandomness([]byte{10, 11, 12, 13})

	// only sectors that are not skipped and not existing non-recovered faults will be verified
	allIgnored := bf()
	dln := h.getDeadline(rt, deadline.Index)
	for _, p := range partitions {
		partition := h.getPartition(rt, dln, p.Index)
		expectedFaults, err := bitfield.SubtractBitField(partition.Faults, partition.Recoveries)
		require.NoError(h.t, err)
		allIgnored, err = bitfield.MultiMerge(allIgnored, expectedFaults, p.Skipped)
		require.NoError(h.t, err)
	}

	// find the first non-faulty, non-skipped sector in poSt to replace all faulty sectors.
	var goodInfo *miner.SectorOnChainInfo
	for _, ci := range infos {
		contains, err := allIgnored.IsSet(uint64(ci.SectorNumber))
		require.NoError(h.t, err)
		if !contains {
			goodInfo = ci
			break
		}
	}

	// goodInfo == nil indicates all the sectors have been skipped and should PoSt verification should not occur
	if goodInfo != nil {
		var buf bytes.Buffer
		receiver := rt.Receiver()
		err := receiver.MarshalCBOR(&buf)
		require.NoError(h.t, err)

		rt.ExpectGetRandomnessBeacon(crypto.DomainSeparationTag_WindowedPoStChallengeSeed, deadline.Challenge, buf.Bytes(), abi.Randomness(challengeRand))

		actorId, err := addr.IDFromAddress(h.receiver)
		require.NoError(h.t, err)

		// if not all sectors are skipped
		proofInfos := make([]proof.SectorInfo, len(infos))
		for i, ci := range infos {
			si := ci
			contains, err := allIgnored.IsSet(uint64(ci.SectorNumber))
			require.NoError(h.t, err)
			if contains {
				si = goodInfo
			}
			proofInfos[i] = proof.SectorInfo{
				SealProof:    si.SealProof,
				SectorNumber: si.SectorNumber,
				SealedCID:    si.SealedCID,
			}
		}

		vi := proof.WindowPoStVerifyInfo{
			Randomness:        abi.PoStRandomness(challengeRand),
			Proofs:            proofs,
			ChallengedSectors: proofInfos,
			Prover:            abi.ActorID(actorId),
		}
		var verifResult error
		if poStCfg != nil {
			verifResult = poStCfg.verificationError
		}
		rt.ExpectVerifyPoSt(vi, verifResult)
	}
	if poStCfg != nil {
		// expect power update
		if !poStCfg.expectedPowerDelta.IsZero() {
			claim := &power.UpdateClaimedPowerParams{
				RawByteDelta:         poStCfg.expectedPowerDelta.Raw,
				QualityAdjustedDelta: poStCfg.expectedPowerDelta.QA,
			}
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, claim, abi.NewTokenAmount(0),
				nil, exitcode.Ok)
		}
	}

	params := miner.SubmitWindowedPoStParams{
		Deadline:         deadline.Index,
		Partitions:       partitions,
		Proofs:           proofs,
		ChainCommitEpoch: deadline.Challenge,
		ChainCommitRand:  commitRand,
	}

	rt.Call(h.a.SubmitWindowedPoSt, &params)
	rt.Verify()
}

func (h *actorHarness) declareFaults(rt *mock.Runtime, faultSectorInfos ...*miner.SectorOnChainInfo) miner.PowerPair {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	ss, err := faultSectorInfos[0].SealProof.SectorSize()
	require.NoError(h.t, err)
	expectedRawDelta, expectedQADelta := powerForSectors(ss, faultSectorInfos)
	expectedRawDelta = expectedRawDelta.Neg()
	expectedQADelta = expectedQADelta.Neg()

	// expect power update
	claim := &power.UpdateClaimedPowerParams{
		RawByteDelta:         expectedRawDelta,
		QualityAdjustedDelta: expectedQADelta,
	}
	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdateClaimedPower,
		claim,
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)

	// Calculate params from faulted sector infos
	st := getState(rt)
	params := makeFaultParamsFromFaultingSectors(h.t, st, rt.AdtStore(), faultSectorInfos)
	rt.Call(h.a.DeclareFaults, params)
	rt.Verify()

	return miner.NewPowerPair(claim.RawByteDelta, claim.QualityAdjustedDelta)
}

func (h *actorHarness) declareRecoveries(rt *mock.Runtime, deadlineIdx uint64, partitionIdx uint64, recoverySectors bitfield.BitField, expectedDebtRepaid abi.TokenAmount) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	if expectedDebtRepaid.GreaterThan(big.Zero()) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectedDebtRepaid, nil, exitcode.Ok)
	}

	// Calculate params from faulted sector infos
	params := &miner.DeclareFaultsRecoveredParams{Recoveries: []miner.RecoveryDeclaration{{
		Deadline:  deadlineIdx,
		Partition: partitionIdx,
		Sectors:   recoverySectors,
	}}}

	rt.Call(h.a.DeclareFaultsRecovered, params)
	rt.Verify()
}

func (h *actorHarness) extendSectors(rt *mock.Runtime, params *miner.ExtendSectorExpirationParams) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	qaDelta := big.Zero()
	for _, extension := range params.Extensions {
		err := extension.Sectors.ForEach(func(sno uint64) error {
			sector := h.getSector(rt, abi.SectorNumber(sno))
			newSector := *sector
			newSector.Expiration = extension.NewExpiration
			qaDelta = big.Sum(qaDelta,
				miner.QAPowerForSector(h.sectorSize, &newSector),
				miner.QAPowerForSector(h.sectorSize, sector).Neg(),
			)
			return nil
		})
		require.NoError(h.t, err)
	}
	if !qaDelta.IsZero() {
		rt.ExpectSend(builtin.StoragePowerActorAddr,
			builtin.MethodsPower.UpdateClaimedPower,
			&power.UpdateClaimedPowerParams{
				RawByteDelta:         big.Zero(),
				QualityAdjustedDelta: qaDelta,
			},
			abi.NewTokenAmount(0),
			nil,
			exitcode.Ok,
		)
	}
	rt.Call(h.a.ExtendSectorExpiration, params)
	rt.Verify()
}

func (h *actorHarness) terminateSectors(rt *mock.Runtime, sectors bitfield.BitField, expectedFee abi.TokenAmount) (miner.PowerPair, abi.TokenAmount) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	dealIDs := []abi.DealID{}
	sectorInfos := []*miner.SectorOnChainInfo{}
	err := sectors.ForEach(func(secNum uint64) error {
		sector := h.getSector(rt, abi.SectorNumber(secNum))
		dealIDs = append(dealIDs, sector.DealIDs...)

		sectorInfos = append(sectorInfos, sector)
		return nil
	})
	require.NoError(h.t, err)

	expectQueryNetworkInfo(rt, h)

	pledgeDelta := big.Zero()
	var sectorPower miner.PowerPair
	if big.Zero().LessThan(expectedFee) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectedFee, nil, exitcode.Ok)
		pledgeDelta = big.Sum(pledgeDelta, expectedFee.Neg())
	}
	// notify change to initial pledge
	if len(sectorInfos) > 0 {
		for _, sector := range sectorInfos {
			pledgeDelta = big.Add(pledgeDelta, sector.InitialPledge.Neg())
		}
	}
	if !pledgeDelta.Equals(big.Zero()) {
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &pledgeDelta, big.Zero(), nil, exitcode.Ok)
	}
	if len(dealIDs) > 0 {
		size := len(dealIDs)
		if size > cbg.MaxLength {
			size = cbg.MaxLength
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.OnMinerSectorsTerminate, &market.OnMinerSectorsTerminateParams{
			Epoch:   rt.Epoch(),
			DealIDs: dealIDs[:size],
		}, abi.NewTokenAmount(0), nil, exitcode.Ok)
		dealIDs = dealIDs[size:]
	}
	{
		sectorPower = miner.PowerForSectors(h.sectorSize, sectorInfos)
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, &power.UpdateClaimedPowerParams{
			RawByteDelta:         sectorPower.Raw.Neg(),
			QualityAdjustedDelta: sectorPower.QA.Neg(),
		}, abi.NewTokenAmount(0), nil, exitcode.Ok)
	}

	// create declarations
	st := getState(rt)
	deadlines, err := st.LoadDeadlines(rt.AdtStore())
	require.NoError(h.t, err)

	declarations := []miner.TerminationDeclaration{}
	err = sectors.ForEach(func(id uint64) error {
		dlIdx, pIdx, err := miner.FindSector(rt.AdtStore(), deadlines, abi.SectorNumber(id))
		require.NoError(h.t, err)

		declarations = append(declarations, miner.TerminationDeclaration{
			Deadline:  dlIdx,
			Partition: pIdx,
			Sectors:   bf(id),
		})
		return nil
	})
	require.NoError(h.t, err)

	params := &miner.TerminateSectorsParams{Terminations: declarations}
	rt.Call(h.a.TerminateSectors, params)
	rt.Verify()

	return sectorPower.Neg(), pledgeDelta
}

func (h *actorHarness) reportConsensusFault(rt *mock.Runtime, from addr.Address, faultEpoch abi.ChainEpoch) {
	rt.SetCaller(from, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
	params := &miner.ReportConsensusFaultParams{
		BlockHeader1:     nil,
		BlockHeader2:     nil,
		BlockHeaderExtra: nil,
	}

	rt.ExpectVerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra, &runtime.ConsensusFault{
		Target: h.receiver,
		Epoch:  faultEpoch,
		Type:   runtime.ConsensusFaultDoubleForkMining,
	}, nil)

	currentReward := reward.ThisEpochRewardReturn{
		ThisEpochBaselinePower:  h.baselinePower,
		ThisEpochRewardSmoothed: h.epochRewardSmooth,
	}
	rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.ThisEpochReward, nil, big.Zero(), &currentReward, exitcode.Ok)

	penaltyTotal := miner.ConsensusFaultPenalty(h.epochRewardSmooth.Estimate())
	// slash reward
	rwd := miner.RewardForConsensusSlashReport(1, penaltyTotal)
	rt.ExpectSend(from, builtin.MethodSend, nil, rwd, nil, exitcode.Ok)

	// pay fault fee
	toBurn := big.Sub(penaltyTotal, rwd)
	rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, toBurn, nil, exitcode.Ok)

	rt.Call(h.a.ReportConsensusFault, params)
	rt.Verify()
}

func (h *actorHarness) applyRewards(rt *mock.Runtime, amt, penalty abi.TokenAmount) {
	// This harness function does not handle the state where apply rewards is
	// on a miner with existing fee debt.  This state is not protocol reachable
	// because currently fee debt prevents election participation.
	//
	// We further assume the miner can pay the penalty.  If the miner
	// goes into debt we can't rely on the harness call
	// TODO unify those cases
	lockAmt, _ := miner.LockedRewardFromReward(amt, rt.NetworkVersion())
	pledgeDelta := big.Sub(lockAmt, penalty)

	rt.SetCaller(builtin.RewardActorAddr, builtin.RewardActorCodeID)
	rt.ExpectValidateCallerAddr(builtin.RewardActorAddr)
	// expect pledge update
	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdatePledgeTotal,
		&pledgeDelta,
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)

	if penalty.GreaterThan(big.Zero()) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penalty, nil, exitcode.Ok)
	}

	rt.Call(h.a.ApplyRewards, &builtin.ApplyRewardParams{Reward: amt, Penalty: penalty})
	rt.Verify()
}

type cronConfig struct {
	expectedEnrollment        abi.ChainEpoch
	detectedFaultsPowerDelta  *miner.PowerPair
	expiredSectorsPowerDelta  *miner.PowerPair
	expiredSectorsPledgeDelta abi.TokenAmount
	continuedFaultsPenalty    abi.TokenAmount // Expected amount burnt to pay continued fault penalties.
	repaidFeeDebt             abi.TokenAmount // Expected amount burnt to repay fee debt.
	penaltyFromUnlocked       abi.TokenAmount // Expected reduction in unlocked balance from penalties exceeding vesting funds.
}

func (h *actorHarness) onDeadlineCron(rt *mock.Runtime, config *cronConfig) {
	var st miner.State
	rt.GetState(&st)
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)

	// Preamble
	rwd := reward.ThisEpochRewardReturn{
		ThisEpochBaselinePower:  h.baselinePower,
		ThisEpochRewardSmoothed: h.epochRewardSmooth,
	}
	rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.ThisEpochReward, nil, big.Zero(), &rwd, exitcode.Ok)
	networkPower := big.NewIntUnsigned(1 << 50)
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.CurrentTotalPower, nil, big.Zero(),
		&power.CurrentTotalPowerReturn{
			RawBytePower:            networkPower,
			QualityAdjPower:         networkPower,
			PledgeCollateral:        h.networkPledge,
			QualityAdjPowerSmoothed: h.epochQAPowerSmooth,
		},
		exitcode.Ok)

	powerDelta := miner.NewPowerPairZero()
	if config.detectedFaultsPowerDelta != nil {
		powerDelta = powerDelta.Add(*config.detectedFaultsPowerDelta)
	}
	if config.expiredSectorsPowerDelta != nil {
		powerDelta = powerDelta.Add(*config.expiredSectorsPowerDelta)
	}

	if !powerDelta.IsZero() {
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, &power.UpdateClaimedPowerParams{
			RawByteDelta:         powerDelta.Raw,
			QualityAdjustedDelta: powerDelta.QA,
		},
			abi.NewTokenAmount(0), nil, exitcode.Ok)
	}

	penaltyTotal := big.Zero()
	pledgeDelta := big.Zero()
	if !config.continuedFaultsPenalty.NilOrZero() {
		penaltyTotal = big.Add(penaltyTotal, config.continuedFaultsPenalty)
	}
	if !config.repaidFeeDebt.NilOrZero() {
		penaltyTotal = big.Add(penaltyTotal, config.repaidFeeDebt)
	}
	if !penaltyTotal.IsZero() {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penaltyTotal, nil, exitcode.Ok)
		penaltyFromVesting := penaltyTotal
		// Outstanding fee debt is only repaid from unlocked balance, not vesting funds.
		if !config.repaidFeeDebt.NilOrZero() {
			penaltyFromVesting = big.Sub(penaltyFromVesting, config.repaidFeeDebt)
		}
		// New penalties are paid first from vesting funds but, if exhausted, overflow to unlocked balance.
		if !config.penaltyFromUnlocked.NilOrZero() {
			penaltyFromVesting = big.Sub(penaltyFromVesting, config.penaltyFromUnlocked)
		}
		pledgeDelta = big.Sub(pledgeDelta, penaltyFromVesting)
	}

	if !config.expiredSectorsPledgeDelta.NilOrZero() {
		pledgeDelta = big.Add(pledgeDelta, config.expiredSectorsPledgeDelta)
	}

	pledgeDelta = big.Sub(pledgeDelta, immediatelyVestingFunds(rt, &st))

	if !pledgeDelta.IsZero() {
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &pledgeDelta, big.Zero(), nil, exitcode.Ok)
	}

	// Re-enrollment for next period.
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		makeDeadlineCronEventParams(h.t, config.expectedEnrollment), big.Zero(), nil, exitcode.Ok)

	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.Call(h.a.OnDeferredCronEvent, &miner.CronEventPayload{
		EventType: miner.CronEventProvingDeadline,
	})
	rt.Verify()
}

func (h *actorHarness) withdrawFunds(rt *mock.Runtime, amountRequested, amountWithdrawn, expectedDebtRepaid abi.TokenAmount) {
	rt.SetCaller(h.owner, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.owner)

	rt.ExpectSend(h.owner, builtin.MethodSend, nil, amountWithdrawn, nil, exitcode.Ok)
	if expectedDebtRepaid.GreaterThan(big.Zero()) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectedDebtRepaid, nil, exitcode.Ok)
	}
	rt.Call(h.a.WithdrawBalance, &miner.WithdrawBalanceParams{
		AmountRequested: amountRequested,
	})

	rt.Verify()
}

func (h *actorHarness) repayDebt(rt *mock.Runtime, value, expectedRepayedFromVest, expectedRepaidFromBalance abi.TokenAmount) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	rt.SetBalance(big.Sum(rt.Balance(), value))
	rt.SetReceived(value)
	if expectedRepayedFromVest.GreaterThan(big.Zero()) {
		pledgeDelta := expectedRepayedFromVest.Neg()
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &pledgeDelta, big.Zero(), nil, exitcode.Ok)
	}

	totalRepaid := big.Sum(expectedRepayedFromVest, expectedRepaidFromBalance)
	if totalRepaid.GreaterThan((big.Zero())) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, totalRepaid, nil, exitcode.Ok)
	}
	rt.Call(h.a.RepayDebt, nil)

	rt.Verify()
}

func (h *actorHarness) compactPartitions(rt *mock.Runtime, deadline uint64, partitions bitfield.BitField) {
	param := miner.CompactPartitionsParams{Deadline: deadline, Partitions: partitions}

	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)

	rt.Call(h.a.CompactPartitions, &param)
	rt.Verify()
}

func (h *actorHarness) continuedFaultPenalty(sectors []*miner.SectorOnChainInfo) abi.TokenAmount {
	_, qa := powerForSectors(h.sectorSize, sectors)
	return miner.PledgePenaltyForContinuedFault(h.epochRewardSmooth, h.epochQAPowerSmooth, qa)
}

func (h *actorHarness) powerPairForSectors(sectors []*miner.SectorOnChainInfo) miner.PowerPair {
	rawPower, qaPower := powerForSectors(h.sectorSize, sectors)
	return miner.NewPowerPair(rawPower, qaPower)
}

func (h *actorHarness) makePreCommit(sectorNo abi.SectorNumber, challenge, expiration abi.ChainEpoch, dealIDs []abi.DealID) *miner.PreCommitSectorParams {
	return &miner.PreCommitSectorParams{
		SealProof:     h.sealProofType,
		SectorNumber:  sectorNo,
		SealedCID:     tutil.MakeCID("commr", &miner.SealedCIDPrefix),
		SealRandEpoch: challenge,
		DealIDs:       dealIDs,
		Expiration:    expiration,
	}
}

func (h *actorHarness) setPeerID(rt *mock.Runtime, newID abi.PeerID) {
	params := miner.ChangePeerIDParams{NewID: newID}

	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	ret := rt.Call(h.a.ChangePeerID, &params)
	assert.Nil(h.t, ret)
	rt.Verify()

	var st miner.State
	rt.GetState(&st)
	info, err := st.GetInfo(adt.AsStore(rt))
	require.NoError(h.t, err)

	assert.Equal(h.t, newID, info.PeerId)
}

func (h *actorHarness) setMultiaddrs(rt *mock.Runtime, newMultiaddrs ...abi.Multiaddrs) {
	params := miner.ChangeMultiaddrsParams{NewMultiaddrs: newMultiaddrs}

	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	ret := rt.Call(h.a.ChangeMultiaddrs, &params)
	assert.Nil(h.t, ret)
	rt.Verify()

	var st miner.State
	rt.GetState(&st)
	info, err := st.GetInfo(adt.AsStore(rt))
	require.NoError(h.t, err)

	assert.Equal(h.t, newMultiaddrs, info.Multiaddrs)
}

//
// Higher-level orchestration
//

// Completes a deadline by moving the epoch forward to the penultimate one, calling the deadline cron handler,
// and then advancing to the first epoch in the new deadline.
func advanceDeadline(rt *mock.Runtime, h *actorHarness, config *cronConfig) *dline.Info {
	deadline := h.deadline(rt)
	rt.SetEpoch(deadline.Last())
	config.expectedEnrollment = deadline.Last() + miner.WPoStChallengeWindow
	h.onDeadlineCron(rt, config)
	rt.SetEpoch(deadline.NextOpen())
	return h.deadline(rt)
}

func advanceToEpochWithCron(rt *mock.Runtime, h *actorHarness, e abi.ChainEpoch) {
	deadline := h.deadline(rt)
	for e > deadline.Last() {
		advanceDeadline(rt, h, &cronConfig{})
		deadline = h.deadline(rt)
	}
	rt.SetEpoch(e)
}

func advanceAndSubmitPoSts(rt *mock.Runtime, h *actorHarness, sectors ...*miner.SectorOnChainInfo) {
	st := getState(rt)

	sectorArr, err := miner.LoadSectors(rt.AdtStore(), st.Sectors)
	require.NoError(h.t, err)

	deadlines := map[uint64][]*miner.SectorOnChainInfo{}
	for _, sector := range sectors {
		dlIdx, _, err := st.FindSector(rt.AdtStore(), sector.SectorNumber)
		require.NoError(h.t, err)
		deadlines[dlIdx] = append(deadlines[dlIdx], sector)
	}

	dlinfo := h.deadline(rt)
	for len(deadlines) > 0 {
		dlSectors, ok := deadlines[dlinfo.Index]
		if ok {
			sectorNos := bitfield.New()
			for _, sector := range dlSectors {
				sectorNos.Set(uint64(sector.SectorNumber))
			}

			dlArr, err := st.LoadDeadlines(rt.AdtStore())
			require.NoError(h.t, err)
			dl, err := dlArr.LoadDeadline(rt.AdtStore(), dlinfo.Index)
			require.NoError(h.t, err)
			parts, err := dl.PartitionsArray(rt.AdtStore())
			require.NoError(h.t, err)

			var partition miner.Partition
			partitions := []miner.PoStPartition{}
			powerDelta := miner.NewPowerPairZero()
			require.NoError(h.t, parts.ForEach(&partition, func(partIdx int64) error {
				live, err := partition.LiveSectors()
				require.NoError(h.t, err)
				toProve, err := bitfield.IntersectBitField(live, sectorNos)
				require.NoError(h.t, err)
				noProven, err := toProve.IsEmpty()
				require.NoError(h.t, err)
				if noProven {
					// not proving anything in this partition.
					return nil
				}

				toSkip, err := bitfield.SubtractBitField(live, toProve)
				require.NoError(h.t, err)

				notRecovering, err := bitfield.SubtractBitField(partition.Faults, partition.Recoveries)
				require.NoError(h.t, err)

				// Don't double-count skips.
				toSkip, err = bitfield.SubtractBitField(toSkip, notRecovering)
				require.NoError(h.t, err)

				skippedProven, err := bitfield.SubtractBitField(toSkip, partition.Unproven)
				require.NoError(h.t, err)

				skippedProvenSectorInfos, err := sectorArr.Load(skippedProven)
				require.NoError(h.t, err)
				newFaultyPower := h.powerPairForSectors(skippedProvenSectorInfos)

				newProven, err := bitfield.SubtractBitField(partition.Unproven, toSkip)
				require.NoError(h.t, err)

				newProvenInfos, err := sectorArr.Load(newProven)
				require.NoError(h.t, err)
				newProvenPower := h.powerPairForSectors(newProvenInfos)

				powerDelta = powerDelta.Sub(newFaultyPower)
				powerDelta = powerDelta.Add(newProvenPower)

				partitions = append(partitions, miner.PoStPartition{Index: uint64(partIdx), Skipped: toSkip})
				return nil
			}))

			h.submitWindowPoSt(rt, dlinfo, partitions, dlSectors, &poStConfig{
				expectedPowerDelta: powerDelta,
			})
			delete(deadlines, dlinfo.Index)
		}

		advanceDeadline(rt, h, &cronConfig{})
		dlinfo = h.deadline(rt)
	}
}

func immediatelyVestingFunds(rt *mock.Runtime, st *miner.State) big.Int {
	// Account just the very next vesting funds entry.
	var vesting miner.VestingFunds
	rt.StoreGet(st.VestingFunds, &vesting)
	sum := big.Zero()
	for _, v := range vesting.Funds {
		if v.Epoch <= rt.Epoch() {
			sum = big.Add(sum, v.Amount)
		} else {
			break
		}
	}
	return sum
}

//
// Construction helpers, etc
//

func builderForHarness(actor *actorHarness) *mock.RuntimeBuilder {
	rb := mock.NewBuilder(context.Background(), actor.receiver).
		WithActorType(actor.owner, builtin.AccountActorCodeID).
		WithActorType(actor.worker, builtin.AccountActorCodeID).
		WithHasher(fixedHasher(uint64(actor.periodOffset)))

	for _, ca := range actor.controlAddrs {
		rb = rb.WithActorType(ca, builtin.AccountActorCodeID)
	}

	return rb
}

func getState(rt *mock.Runtime) *miner.State {
	var st miner.State
	rt.GetState(&st)
	return &st
}

func makeDeadlineCronEventParams(t testing.TB, epoch abi.ChainEpoch) *power.EnrollCronEventParams {
	eventPayload := miner.CronEventPayload{EventType: miner.CronEventProvingDeadline}
	buf := bytes.Buffer{}
	err := eventPayload.MarshalCBOR(&buf)
	require.NoError(t, err)
	return &power.EnrollCronEventParams{
		EventEpoch: epoch,
		Payload:    buf.Bytes(),
	}
}

func makeProveCommit(sectorNo abi.SectorNumber) *miner.ProveCommitSectorParams {
	return &miner.ProveCommitSectorParams{
		SectorNumber: sectorNo,
		Proof:        []byte("proof"),
	}
}

func makePoStProofs(registeredPoStProof abi.RegisteredPoStProof) []proof.PoStProof {
	proofs := make([]proof.PoStProof, 1) // Number of proofs doesn't depend on partition count
	for i := range proofs {
		proofs[i].PoStProof = registeredPoStProof
		proofs[i].ProofBytes = []byte(fmt.Sprintf("proof%d", i))
	}
	return proofs
}

func makeFaultParamsFromFaultingSectors(t testing.TB, st *miner.State, store adt.Store, faultSectorInfos []*miner.SectorOnChainInfo) *miner.DeclareFaultsParams {
	deadlines, err := st.LoadDeadlines(store)
	require.NoError(t, err)

	declarationMap := map[string]*miner.FaultDeclaration{}
	for _, sector := range faultSectorInfos {
		dlIdx, pIdx, err := miner.FindSector(store, deadlines, sector.SectorNumber)
		require.NoError(t, err)

		key := fmt.Sprintf("%d:%d", dlIdx, pIdx)
		declaration, ok := declarationMap[key]
		if !ok {
			declaration = &miner.FaultDeclaration{
				Deadline:  dlIdx,
				Partition: pIdx,
				Sectors:   bf(),
			}
			declarationMap[key] = declaration
		}
		declaration.Sectors.Set(uint64(sector.SectorNumber))
	}
	require.NoError(t, err)

	var declarations []miner.FaultDeclaration
	for _, declaration := range declarationMap {
		declarations = append(declarations, *declaration)
	}

	return &miner.DeclareFaultsParams{Faults: declarations}
}

func sectorInfoAsBitfield(infos []*miner.SectorOnChainInfo) bitfield.BitField {
	bf := bitfield.New()
	for _, info := range infos {
		bf.Set(uint64(info.SectorNumber))
	}
	return bf
}

func powerForSectors(sectorSize abi.SectorSize, sectors []*miner.SectorOnChainInfo) (rawBytePower, qaPower big.Int) {
	rawBytePower = big.Mul(big.NewIntUnsigned(uint64(sectorSize)), big.NewIntUnsigned(uint64(len(sectors))))
	qaPower = big.Zero()
	for _, s := range sectors {
		qaPower = big.Add(qaPower, miner.QAPowerForSector(sectorSize, s))
	}
	return rawBytePower, qaPower
}

func assertEmptyBitfield(t *testing.T, b bitfield.BitField) {
	empty, err := b.IsEmpty()
	require.NoError(t, err)
	assert.True(t, empty)
}

// Returns a fake hashing function that always arranges the first 8 bytes of the digest to be the binary
// encoding of a target uint64.
func fixedHasher(target uint64) func([]byte) [32]byte {
	return func(_ []byte) [32]byte {
		var buf bytes.Buffer
		err := binary.Write(&buf, binary.BigEndian, target)
		if err != nil {
			panic(err)
		}
		var digest [32]byte
		copy(digest[:], buf.Bytes())
		return digest
	}
}

func expectQueryNetworkInfo(rt *mock.Runtime, h *actorHarness) {
	currentPower := power.CurrentTotalPowerReturn{
		RawBytePower:            h.networkRawPower,
		QualityAdjPower:         h.networkQAPower,
		PledgeCollateral:        h.networkPledge,
		QualityAdjPowerSmoothed: h.epochQAPowerSmooth,
	}
	currentReward := reward.ThisEpochRewardReturn{
		ThisEpochBaselinePower:  h.baselinePower,
		ThisEpochRewardSmoothed: h.epochRewardSmooth,
	}

	rt.ExpectSend(
		builtin.RewardActorAddr,
		builtin.MethodsReward.ThisEpochReward,
		nil,
		big.Zero(),
		&currentReward,
		exitcode.Ok,
	)

	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.CurrentTotalPower,
		nil,
		big.Zero(),
		&currentPower,
		exitcode.Ok,
	)
}
