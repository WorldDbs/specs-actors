// nolint:unused // 20200716 until tests are restored from miner state refactor
package miner_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/actors/util/smoothing"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

var testPid abi.PeerID
var testMultiaddrs []abi.Multiaddrs

// A balance for use in tests where the miner's low balance is not interesting.
var bigBalance = big.Mul(big.NewInt(1000000), big.NewInt(1e18))

// an expriration 1 greater than min expiration
const defaultSectorExpiration = 190

func init() {
	testPid = abi.PeerID("peerID")

	testMultiaddrs = []abi.Multiaddrs{
		{1},
		{2},
	}

	// permit 2KiB sectors in tests
	miner.SupportedProofTypes[abi.RegisteredSealProof_StackedDrg2KiBV1] = struct{}{}
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
			SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1,
			PeerId:        testPid,
			Multiaddrs:    testMultiaddrs,
		}

		provingPeriodStart := abi.ChainEpoch(658) // This is just set from running the code.
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		// Fetch worker pubkey.
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
		// Register proving period cron.
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			makeDeadlineCronEventParams(t, provingPeriodStart-1), big.Zero(), nil, exitcode.Ok)
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
		assert.Equal(t, abi.RegisteredSealProof_StackedDrg32GiBV1, info.SealProofType)
		assert.Equal(t, abi.SectorSize(1<<35), info.SectorSize)
		assert.Equal(t, uint64(2349), info.WindowPoStPartitionSectors)

		assert.Equal(t, big.Zero(), st.PreCommitDeposits)
		assert.Equal(t, big.Zero(), st.LockedFunds)
		assert.True(t, st.PreCommittedSectors.Defined())

		assert.True(t, st.Sectors.Defined())
		assert.Equal(t, provingPeriodStart, st.ProvingPeriodStart)
		assert.Equal(t, uint64(0), st.CurrentDeadline)

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
			OwnerAddr:    owner,
			WorkerAddr:   worker,
			ControlAddrs: []addr.Address{control1, control2},
		}

		provingPeriodStart := abi.ChainEpoch(658) // This is just set from running the code.
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			makeDeadlineCronEventParams(t, provingPeriodStart-1), big.Zero(), nil, exitcode.Ok)
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
			OwnerAddr:    owner,
			WorkerAddr:   worker,
			ControlAddrs: []addr.Address{control1},
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.Constructor, &params)
		})
		rt.Verify()
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
		expiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // something on deadline boundary but > 180 days
		precommit := actor.makePreCommit(sectorNo, precommitEpoch-1, expiration, nil)
		actor.preCommitSector(rt, precommit)

		// assert precommit exists and meets expectations
		onChainPrecommit := actor.getPreCommit(rt, sectorNo)

		// expect precommit deposit to be initial pledge calculated at precommit time
		sectorSize, err := precommit.SealProof.SectorSize()
		require.NoError(t, err)

		// deal weights mocked by actor harness for market actor must be set in precommit onchain info
		assert.Equal(t, big.NewInt(int64(sectorSize/2)), onChainPrecommit.DealWeight)
		assert.Equal(t, big.NewInt(int64(sectorSize/2)), onChainPrecommit.VerifiedDealWeight)

		qaPower := miner.QAPowerForWeight(sectorSize, precommit.Expiration-precommitEpoch, onChainPrecommit.DealWeight, onChainPrecommit.VerifiedDealWeight)
		expectedDeposit := miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, qaPower)
		assert.Equal(t, expectedDeposit, onChainPrecommit.PreCommitDeposit)

		// expect total precommit deposit to equal our new deposit
		st := getState(rt)
		assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

		// run prove commit logic
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))
		actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})

		// expect precommit to have been removed
		st = getState(rt)
		_, found, err := st.GetPrecommittedSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		require.False(t, found)

		// expect deposit to have been transferred to initial pledges
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)

		qaPower = miner.QAPowerForWeight(sectorSize, precommit.Expiration-rt.Epoch(), onChainPrecommit.DealWeight,
			onChainPrecommit.VerifiedDealWeight)
		expectedInitialPledge := miner.InitialPledgeForPower(qaPower, actor.baselinePower, actor.networkPledge, actor.epochRewardSmooth,
			actor.epochQAPowerSmooth, rt.TotalFilCircSupply())
		assert.Equal(t, expectedInitialPledge, st.InitialPledgeRequirement)

		// expect new onchain sector
		sector := actor.getSector(rt, sectorNo)
		sectorPower := miner.PowerForSector(sectorSize, sector)

		// expect deal weights to be transfered to on chain info
		assert.Equal(t, onChainPrecommit.DealWeight, sector.DealWeight)
		assert.Equal(t, onChainPrecommit.VerifiedDealWeight, sector.VerifiedDealWeight)

		// expect activation epoch to be current epoch
		assert.Equal(t, rt.Epoch(), sector.Activation)

		// expect initial plege of sector to be set
		assert.Equal(t, expectedInitialPledge, sector.InitialPledge)

		// expect locked initial pledge of sector to be the same as pledge requirement
		assert.Equal(t, expectedInitialPledge, st.InitialPledgeRequirement)

		// expect sector to be assigned a deadline/partition
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, pIdx)
		assert.Equal(t, uint64(1), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.PostSubmissions)
		assertEmptyBitfield(t, deadline.EarlyTerminations)

		quant := st.QuantSpecForDeadline(dlIdx)
		quantizedExpiration := quant.QuantizeUp(precommit.Expiration)

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
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil))
		})
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
		actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil))

		// Duplicate pre-commit sector ID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already been allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil))
		})
		rt.Reset()

		// Sector ID already committed
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already been allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(oldSector.SectorNumber, challengeEpoch, expiration, nil))
		})
		rt.Reset()

		// Bad sealed CID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "sealed CID had wrong prefix", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealedCID = tutil.MakeCID("Random Data", nil)
			actor.preCommitSector(rt, pc)
		})
		rt.Reset()

		// Bad seal proof type
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "unsupported seal proof type", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg8MiBV1
			actor.preCommitSector(rt, pc)
		})
		rt.Reset()

		// Expires at current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after now", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch(), nil))
		})
		rt.Reset()

		// Expires before current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after now", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch()-1, nil))
		})
		rt.Reset()

		// Expires too early
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration-20*builtin.EpochsInDay, nil))
		})
		rt.Reset()

		// Expires before min duration + max seal duration
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			expiration := rt.Epoch() + miner.MinSectorExpiration + miner.MaxSealDuration[actor.sealProofType] - 1
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil))
		})
		rt.Reset()

		// Errors when expiry too far in the future
		rt.SetEpoch(precommitEpoch)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid expiration", func() {
			expiration := deadline.PeriodEnd() + miner.WPoStProvingPeriod*(miner.MaxSectorExpirationExtension/miner.WPoStProvingPeriod+1)
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil))
		})
		rt.Reset()

		// Sector ID out of range
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "out of range", func() {
			actor.preCommitSector(rt, actor.makePreCommit(abi.MaxSectorNumber+1, challengeEpoch, expiration, nil))
		})
		rt.Reset()

		// Seal randomness challenge too far in past
		tooOldChallengeEpoch := precommitEpoch - miner.ChainFinality - miner.MaxSealDuration[actor.sealProofType] - 1
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too old", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, tooOldChallengeEpoch, expiration, nil))
		})
		rt.Reset()
	})

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
		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// Reduce the epoch reward so that a new sector's initial pledge would otherwise be lesser.
		actor.epochReward = big.Div(actor.epochReward, big.NewInt(2))
		actor.epochRewardSmooth = smoothing.TestingConstantEstimate(actor.epochReward)

		challengeEpoch := rt.Epoch() - 1
		upgradeParams := actor.makePreCommit(200, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams.ReplaceCapacity = true
		upgradeParams.ReplaceSectorDeadline = dlIdx
		upgradeParams.ReplaceSectorPartition = partIdx
		upgradeParams.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade := actor.preCommitSector(rt, upgradeParams)

		// Check new pre-commit in state
		assert.True(t, upgrade.Info.ReplaceCapacity)
		assert.Equal(t, upgradeParams.ReplaceSectorNumber, upgrade.Info.ReplaceSectorNumber)
		// Require new sector's pledge to be at least that of the old sector.
		assert.True(t, upgrade.PreCommitDeposit.GreaterThanEqual(oldSector.InitialPledge))

		// Old sector is unchanged
		oldSectorAgain := actor.getSector(rt, oldSector.SectorNumber)
		assert.Equal(t, oldSector, oldSectorAgain)

		// Deposit and pledge as expected
		st = getState(rt)
		assert.Equal(t, st.PreCommitDeposits, upgrade.PreCommitDeposit)
		assert.Equal(t, st.InitialPledgeRequirement, oldSector.InitialPledge)

		// Prove new sector
		rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
		newSector := actor.proveCommitSectorAndConfirm(rt, &upgrade.Info, upgrade.PreCommitEpoch,
			makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})

		// Both sectors have pledge
		st = getState(rt)
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)
		assert.Equal(t, st.InitialPledgeRequirement, big.Add(oldSector.InitialPledge, newSector.InitialPledge))

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
		lostPower := actor.powerPairForSectors(bothSectors).Neg()
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

		// we expect the expiration to be scheduled twice, once early
		// and once on-time.
		dQueue = actor.collectDeadlineExpirations(rt, deadline)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			miner.QuantSpecForDeadline(dlInfo).QuantizeUp(newSector.Expiration): {uint64(0)},
			faultExpiration: {uint64(0)},
		}, dQueue)

		// Old sector gone from pledge requirement and deposit
		assert.Equal(t, st.InitialPledgeRequirement, newSector.InitialPledge)
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
				actor.preCommitSector(rt, &params)
			})
			rt.Reset()
		}
		{ // Old sector cannot have deals
			params := *upgradeParams
			params.ReplaceSectorNumber = oldSectors[1].SectorNumber
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, &params)
			})
			rt.Reset()
		}
		{ // Target sector must exist
			params := *upgradeParams
			params.ReplaceSectorNumber = 999
			rt.ExpectAbort(exitcode.ErrNotFound, func() {
				actor.preCommitSector(rt, &params)
			})
			rt.Reset()
		}
		{ // Expiration must not be sooner than target
			params := *upgradeParams
			params.Expiration = params.Expiration - miner.WPoStProvingPeriod
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, &params)
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
			newFaults, _, err := partition.DeclareFaults(rt.AdtStore(), sectorArr, bf(uint64(oldSectors[0].SectorNumber)), 100000,
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
				actor.preCommitSector(rt, &params)
			})
			rt.ReplaceState(&prevState)
			rt.Reset()
		}

		// Demonstrate that the params are otherwise ok
		actor.preCommitSector(rt, upgradeParams)
		rt.Verify()
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
		precommit := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, nil)
		actor.preCommitSector(rt, precommit)

		// Sector pre-commitment missing.
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(sectorNo+1), proveCommitConf{})
		})
		rt.Reset()

		// Too late.
		rt.SetEpoch(precommitEpoch + miner.MaxSealDuration[precommit.SealProof] + 1)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()

		// Too early.
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay - 1)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()

		// Set the right epoch for all following tests
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)

		// Invalid deals (market ActivateDeals aborts)
		verifyDealsExit := make(map[abi.SectorNumber]exitcode.ExitCode)
		verifyDealsExit[precommit.SectorNumber] = exitcode.ErrIllegalArgument
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
				verifyDealsExit: verifyDealsExit,
			})
		})
		rt.Reset()

		// Good proof
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))
		actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})
		st := getState(rt)
		// Verify new sectors
		// TODO minerstate
		//newSectors, err := st.NewSectors.All(miner.SectorsMax)
		//require.NoError(t, err)
		//assert.Equal(t, []uint64{uint64(sectorNo)}, newSectors)
		// Verify pledge lock-up
		assert.True(t, st.InitialPledgeRequirement.GreaterThan(big.Zero()))
		rt.Reset()

		// Duplicate proof (sector no-longer pre-committed)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()
	})

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
		precommit := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, nil)
		actor.preCommitSector(rt, precommit)

		// precommit at correct epoch
		rt.SetEpoch(rt.Epoch() + miner.PreCommitChallengeDelay + 1)
		actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo))

		// confirm at sector expiration (this probably can't happen)
		rt.SetEpoch(precommit.Expiration)
		// sector skipped but no failure occurs
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")

		// it still skips if sector lifetime is negative
		rt.ClearLogs()
		rt.SetEpoch(precommit.Expiration + 1)
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")

		// it fails up to the miniumum expiration
		rt.ClearLogs()
		rt.SetEpoch(precommit.Expiration - miner.MinSectorExpiration + 1)
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")
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
			abi.RegisteredSealProof_StackedDrg2KiBV1:  256,
			abi.RegisteredSealProof_StackedDrg32GiBV1: 256,
			abi.RegisteredSealProof_StackedDrg64GiBV1: 512,
		}

		for proof, limit := range dealLimits {
			// attempt to pre-commmit a sector with too many sectors
			rt, actor, deadline := setup(proof)
			expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
			precommit := actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit+1))
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too many deals for sector", func() {
				actor.preCommitSector(rt, precommit)
			})

			// sector at or below limit succeeds
			rt, actor, _ = setup(proof)
			precommit = actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit))
			actor.preCommitSector(rt, precommit)
		}

	})
}

func TestWindowPost(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1)
	precommitEpoch := abi.ChainEpoch(1)
	builder := builderForHarness(actor).
		WithEpoch(precommitEpoch).
		WithBalance(bigBalance, big.Zero())

	t.Run("test proof", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)[0]

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
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{sector}, nil)

		// Verify proof recorded
		deadline := actor.getDeadline(rt, dlIdx)
		assertBitfieldEquals(t, deadline.PostSubmissions, pIdx)

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
	})

	t.Run("test duplicate proof ignored", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)[0]

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
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{sector}, nil)

		// Submit a duplicate proof for the same partition, which should be ignored.
		// The skipped fault declared here has no effect.
		commitEpoch := rt.Epoch() - 1
		commitRand := abi.Randomness("chaincommitment")
		params := miner.SubmitWindowedPoStParams{
			Deadline: dlIdx,
			Partitions: []miner.PoStPartition{{
				Index:   pIdx,
				Skipped: bf(uint64(sector.SectorNumber)),
			}},
			Proofs:           makePoStProofs(actor.postProofType),
			ChainCommitEpoch: commitEpoch,
			ChainCommitRand:  commitRand,
		}
		expectQueryNetworkInfo(rt, actor)
		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)
		rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_PoStChainCommit, commitEpoch, nil, commitRand)
		rt.Call(actor.a.SubmitWindowedPoSt, &params)
		rt.Verify()

		// Verify proof recorded
		deadline := actor.getDeadline(rt, dlIdx)
		assertBitfieldEquals(t, deadline.PostSubmissions, pIdx)

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
	})

	t.Run("successful recoveries recover power", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		pwr := miner.PowerForSectors(actor.sectorSize, infos)

		// add lots of funds so we can pay penalties without going into debt
		initialLocked := big.Mul(big.NewInt(200), big.NewInt(1e18))
		actor.addLockedFunds(rt, initialLocked)

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
		actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(infos[0].SectorNumber)))

		// advance to epoch when submitPoSt is due
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Now submit PoSt
		// Power should return for recovered sector.
		cfg := &poStConfig{
			expectedRawPowerDelta: pwr.Raw,
			expectedQAPowerDelta:  pwr.QA,
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

		expectedBalance := initialLocked
		assert.Equal(t, expectedBalance, actor.getLockedFunds(rt))
	})

	t.Run("skipped faults are penalized and adjust power", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil)

		// add lots of funds so we can pay penalties without going into debt
		actor.addLockedFunds(rt, big.Mul(big.NewInt(200), big.NewInt(1e18)))

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
		pwr := miner.PowerForSectors(actor.sectorSize, infos[:1])
		cfg := &poStConfig{
			expectedRawPowerDelta: pwr.Raw.Neg(),
			expectedQAPowerDelta:  pwr.QA.Neg(),
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bf(uint64(infos[0].SectorNumber))},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)

		// expect declared fee to be charged during cron
		declaredFee := actor.declaredFaultPenalty(infos[:1], rt.NetworkVersion())
		dlinfo = advanceDeadline(rt, actor, &cronConfig{ongoingFaultsPenalty: declaredFee})

		// advance to next proving period, expect no fees
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// skip second fault
		pwr = miner.PowerForSectors(actor.sectorSize, infos[1:])

		cfg = &poStConfig{
			expectedRawPowerDelta: pwr.Raw.Neg(),
			expectedQAPowerDelta:  pwr.QA.Neg(),
		}
		partitions = []miner.PoStPartition{
			{Index: pIdx2, Skipped: bf(uint64(infos[1].SectorNumber))},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)

		// expect ongoing fault from both sectors
		advanceDeadline(rt, actor, &cronConfig{ongoingFaultsPenalty: actor.declaredFaultPenalty(infos, rt.NetworkVersion())})
	})

	t.Run("skipped all sectors in a deadline may be skipped", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil)

		// add lots of funds so we can pay penalties without going into debt
		actor.addLockedFunds(rt, big.Mul(big.NewInt(200), big.NewInt(1e18)))

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

		// Now submit PoSt with all faults skipped
		declaredFee := actor.declaredFaultPenalty(infos, rt.NetworkVersion())

		pwr := miner.PowerForSectors(actor.sectorSize, infos)
		cfg := &poStConfig{
			expectedRawPowerDelta: pwr.Raw.Neg(),
			expectedQAPowerDelta:  pwr.QA.Neg(),
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bf(uint64(infos[0].SectorNumber), uint64(infos[1].SectorNumber))},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)

		// expect declared fee to be charged during cron
		advanceDeadline(rt, actor, &cronConfig{ongoingFaultsPenalty: declaredFee})
	})

	t.Run("skipped recoveries are penalized and do not recover power", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// add lots of funds so we can pay penalties without going into debt
		initialLocked := big.Mul(big.NewInt(200), big.NewInt(1e18))
		actor.addLockedFunds(rt, initialLocked)

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
		actor.declareRecoveries(rt, dlIdx, pIdx, bf(uint64(infos[0].SectorNumber)))

		// advance to epoch when submitPoSt is due
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Now submit PoSt and skip recovered sector
		// No power should be returned
		cfg := &poStConfig{
			expectedRawPowerDelta: big.Zero(),
			expectedQAPowerDelta:  big.Zero(),
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bf(uint64(infos[0].SectorNumber))},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)

		// sector will be charged ongoing fee at proving period cron
		ongoingFee := actor.declaredFaultPenalty(infos, rt.NetworkVersion())
		advanceDeadline(rt, actor, &cronConfig{ongoingFaultsPenalty: ongoingFee})

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
			expectedRawPowerDelta: big.Zero(),
			expectedQAPowerDelta:  big.Zero(),
		}
		partitions := []miner.PoStPartition{
			{Index: pIdx0, Skipped: bf(uint64(infos[n-1].SectorNumber))},
		}
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "skipped faults contains sectors outside partition", func() {
			actor.submitWindowPoSt(rt, dlinfo, partitions, infos, cfg)
		})
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

		// prove one sector to establish collateral and locked funds
		actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// preecommit another sector so we may prove it
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		precommit := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, nil)
		actor.preCommitSector(rt, precommit)

		// alter balance to simulate dipping into it for fees

		st := getState(rt)
		bal := rt.Balance()
		rt.SetBalance(big.Add(st.PreCommitDeposits, st.LockedFunds))
		info := actor.getInfo(rt)

		rt.SetEpoch(precommitEpoch + miner.MaxSealDuration[info.SealProofType] - 1)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(actor.nextSectorNo), proveCommitConf{})
		})
		rt.Reset()

		// succeeds when pledge deposits satisfy initial pledge requirement
		rt.SetBalance(bal)
		actor.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(actor.nextSectorNo), proveCommitConf{})
	})

	t.Run("drop invalid prove commit while processing valid one", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// make two precommits
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		precommitA := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, nil)
		actor.preCommitSector(rt, precommitA)
		sectorNoA := actor.nextSectorNo
		actor.nextSectorNo++
		precommitB := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, nil)
		actor.preCommitSector(rt, precommitB)
		sectorNoB := actor.nextSectorNo

		// handle both prove commits in the same epoch
		info := actor.getInfo(rt)
		rt.SetEpoch(precommitEpoch + miner.MaxSealDuration[info.SealProofType] - 1)

		actor.proveCommitSector(rt, precommitA, precommitEpoch, makeProveCommit(sectorNoA))
		actor.proveCommitSector(rt, precommitB, precommitEpoch, makeProveCommit(sectorNoB))

		conf := proveCommitConf{
			verifyDealsExit: map[abi.SectorNumber]exitcode.ExitCode{
				sectorNoA: exitcode.ErrIllegalArgument,
			},
		}
		actor.confirmSectorProofsValid(rt, conf, precommitA, precommitB)
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
		assert.Equal(t, periodOffset, st.ProvingPeriodStart)

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
	})

	t.Run("detects and penalizes faults", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		allSectors := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil)
		pwr := miner.PowerForSectors(actor.sectorSize, allSectors)

		// add lots of funds so penalties come from vesting funds
		initialLocked := big.Mul(big.NewInt(200), big.NewInt(1e18))
		actor.addLockedFunds(rt, initialLocked)

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), allSectors[0].SectorNumber)
		require.NoError(t, err)

		// advance to next deadline where we expect the first sectors to appear
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// Skip to end of the deadline, cron detects and penalizes sectors as faulty
		pwrDelta := pwr.Neg()
		advanceDeadline(rt, actor, &cronConfig{
			detectedFaultsPowerDelta: &pwrDelta,
		})

		// expect faulty power to be added to state
		deadline := actor.getDeadline(rt, dlIdx)
		assert.True(t, pwr.Equals(deadline.FaultyPower))

		// advance 3 deadlines
		advanceDeadline(rt, actor, &cronConfig{})
		advanceDeadline(rt, actor, &cronConfig{})
		dlinfo = advanceDeadline(rt, actor, &cronConfig{})

		actor.declareRecoveries(rt, dlIdx, pIdx, sectorInfoAsBitfield(allSectors[1:]))

		// Skip to end of proving period for sectors, cron detects all sectors as faulty
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		// All ongoing faults are charged
		ongoingPwr := miner.PowerForSectors(actor.sectorSize, allSectors)
		ongoingPenalty := miner.PledgePenaltyForDeclaredFault(actor.epochRewardSmooth, actor.epochQAPowerSmooth, ongoingPwr.QA, rt.NetworkVersion())

		advanceDeadline(rt, actor, &cronConfig{
			ongoingFaultsPenalty:  ongoingPenalty,
		})

		// recorded faulty power is unchanged
		deadline = actor.getDeadline(rt, dlIdx)
		assert.True(t, pwr.Equals(deadline.FaultyPower))
		checkDeadlineInvariants(t, rt.AdtStore(), deadline, st.QuantSpecForDeadline(dlIdx), actor.sectorSize, uint64(4), allSectors)
	})

	t.Run("test cron run late", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// add lots of funds so we can pay penalties without going into debt
		initialLocked := big.Mul(big.NewInt(200), big.NewInt(1e18))
		actor.addLockedFunds(rt, initialLocked)

		// create enough sectors that one will be in a different partition
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// add lots of funds so we can pay penalties without going into debt
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

		// run cron and expect all sectors to be penalized as undetected faults
		pwr := miner.PowerForSectors(actor.sectorSize, allSectors)

		// power for sectors is removed
		powerDeltaClaim := miner.NewPowerPair(pwr.Raw.Neg(), pwr.QA.Neg())

		// expect next cron to be one deadline period after expected cron for this deadline
		nextCron := dlinfo.Last() + +miner.WPoStChallengeWindow

		actor.onDeadlineCron(rt, &cronConfig{
			expectedEnrollment:       nextCron,
			detectedFaultsPowerDelta: &powerDeltaClaim,
		})
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
		initialLocked := big.Mul(big.NewInt(200), big.NewInt(1e18))
		actor.addLockedFunds(rt, initialLocked)

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
		ongoingPenalty := miner.PledgePenaltyForDeclaredFault(actor.epochRewardSmooth, actor.epochQAPowerSmooth, ongoingPwr.QA, rt.NetworkVersion())

		advanceDeadline(rt, actor, &cronConfig{
			ongoingFaultsPenalty: ongoingPenalty,
		})
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

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "cannot reduce sector expiration", func() {
			actor.extendSectors(rt, params)
		})
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
	})

	t.Run("rejects extension past max for seal proof", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)
		rt.SetEpoch(sector.Expiration)

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
	})

	t.Run("updates expiration with valid params", func(t *testing.T) {
		rt := builder.Build(t)
		oldSector := commitSector(t, rt)

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
	})

	t.Run("updates many sectors", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		const sectorCount = 4000

		// commit a bunch of sectors to ensure that we get multiple partitions.
		sectorInfos := actor.commitAndProveSectors(rt, sectorCount, defaultSectorExpiration, nil)

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
					oldSectorNos, err := partition.Sectors.All(miner.SectorsMax)
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
	})

	t.Run("supports extensions off deadline boundary", func(t *testing.T) {
		rt := builder.Build(t)
		oldSector := commitSector(t, rt)

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
	})
}

func TestTerminateSectors(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(big.Mul(big.NewInt(1e18), big.NewInt(200000)), big.Zero())

	commitSector := func(t *testing.T, rt *mock.Runtime) *miner.SectorOnChainInfo {
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)
		sectorInfo := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)
		return sectorInfo[0]
	}

	t.Run("removes sector with correct accounting", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)
		rt.SetEpoch(rt.Epoch() + 100)

		// A miner will pay the minimum of termination fee and locked funds. Add some locked funds to ensure
		// correct fee calculation is used.
		actor.addLockedFunds(rt, big.Mul(big.NewInt(1e18), big.NewInt(20000)))
		st := getState(rt)
		initialLockedFunds := st.LockedFunds

		sectorSize, err := sector.SealProof.SectorSize()
		require.NoError(t, err)
		sectorPower := miner.QAPowerForSector(sectorSize, sector)
		dayReward := miner.ExpectedRewardForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, builtin.EpochsInDay)
		twentyDayReward := miner.ExpectedRewardForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, miner.InitialPledgeProjectionPeriod)
		sectorAge := rt.Epoch() - sector.Activation
		expectedFee := miner.PledgePenaltyForTermination(dayReward, twentyDayReward, sectorAge, actor.epochRewardSmooth, actor.epochQAPowerSmooth, sectorPower, rt.NetworkVersion())

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
			assert.Equal(t, big.Zero(), st.InitialPledgeRequirement)
		}
	})
}

func TestWithdrawBalance(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("happy path withdraws funds", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// withdraw 1% of balance
		actor.withdrawFunds(rt, big.Mul(big.NewInt(10), big.NewInt(1e18)))
	})

	t.Run("fails if miner is currently undercollateralized", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// prove one sector to establish collateral and locked funds
		actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil)

		// alter initial pledge requirement to simulate undercollateralization
		st := getState(rt)
		st.InitialPledgeRequirement = big.Mul(big.NewInt(1e7), st.InitialPledgeRequirement)
		rt.ReplaceState(st)

		// withdraw 1% of balance
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.withdrawFunds(rt, big.Mul(big.NewInt(10), big.NewInt(1e18)))
		})
	})
}

func TestChangePeerID(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)

	t.Run("successfully change peer id", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		builder := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero())
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		newPID := tutil.MakePID("test-change-peer-id")
		actor.changePeerID(rt, newPID)
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

		currentEpoch := abi.ChainEpoch(5)
		rt.SetEpoch(currentEpoch)

		effectiveEpoch := currentEpoch + miner.WorkerKeyChangeDelay
		actor.changeWorkerAddress(rt, newWorker, effectiveEpoch, originalControlAddrs)

		// no change if current epoch is less than effective epoch
		rt.SetEpoch(effectiveEpoch - 1)
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
		rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
		rt.Call(actor.a.OnDeferredCronEvent, &miner.CronEventPayload{
			EventType: miner.CronEventWorkerKeyChange,
		})
		rt.Verify()
		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.NotNil(t, info.PendingWorkerKey)
		require.EqualValues(t, actor.worker, info.Worker)

		// set current epoch to effective epoch and ask to change the address
		actor.cronWorkerAddrChange(rt, effectiveEpoch, newWorker)

		// assert control addresses are unchanged
		st = getState(rt)
		info, err = st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.NotEmpty(t, info.ControlAddresses)
		require.Equal(t, originalControlAddrs, info.ControlAddresses)
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

		// set current epoch to effective epoch and ask to change the address
		actor.cronWorkerAddrChange(rt, effectiveEpoch, newWorker)

		// assert both worker and control addresses have changed
		st := getState(rt)
		info, err := st.GetInfo(adt.AsStore(rt))
		require.NoError(t, err)
		require.NotEmpty(t, info.ControlAddresses)
		require.Equal(t, []addr.Address{c1, c2}, info.ControlAddresses)
		require.Equal(t, newWorker, info.Worker)
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
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(actor.a.ChangeWorkerAddress, param)
		})
		rt.Verify()
	})
}

func TestReportConsensusFault(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("Report consensus fault terminates deals when multiple sectors have multiple deals", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)
		dealIDs := [][]abi.DealID{{1, 2}, {3, 4}}
		sectorInfo := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, dealIDs)
		_ = sectorInfo

		params := &miner.ReportConsensusFaultParams{
			BlockHeader1:     nil,
			BlockHeader2:     nil,
			BlockHeaderExtra: nil,
		}

		// miner should send a single call to terminate the deals for all its sectors
		allDeals := []abi.DealID{}
		for _, ids := range dealIDs {
			allDeals = append(allDeals, ids...)
		}
		actor.reportConsensusFault(rt, addr.TestAddress, params, allDeals)
	})

	t.Run("miner batches termination requests when number of deals exceeds limit", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)

		numSectors := 40
		dealsPerSector := 256
		dealIDs := make([][]abi.DealID, numSectors)
		for i := 0; i < numSectors; i++ {
			dealIDs[i] = make([]abi.DealID, dealsPerSector)
			for j := 0; j < dealsPerSector; j++ {
				dealIDs[i][j] = abi.DealID(dealsPerSector*i + j)
			}
		}
		sectorInfo := actor.commitAndProveSectors(rt, numSectors, defaultSectorExpiration, dealIDs)
		_ = sectorInfo

		params := &miner.ReportConsensusFaultParams{
			BlockHeader1:     nil,
			BlockHeader2:     nil,
			BlockHeaderExtra: nil,
		}

		// report consensus fault will assert deal termination is split into multiple requests
		allDeals := []abi.DealID{}
		for _, ids := range dealIDs {
			allDeals = append(allDeals, ids...)
		}
		actor.reportConsensusFault(rt, addr.TestAddress, params, allDeals)
	})

}

func TestAddLockedFund(t *testing.T) {
	periodOffset := abi.ChainEpoch(1808)
	actor := newHarness(t, periodOffset)

	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

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
		actor.addLockedFunds(rt, amt)
		st = getState(rt)
		vestingFunds, err = st.LoadVestingFunds(adt.AsStore(rt))
		require.NoError(t, err)

		require.Len(t, vestingFunds.Funds, 180)

		// Vested FIL pays out on epochs with expected offset
		expectedOffset := periodOffset % miner.RewardVestingSpecV0.Quantization

		for i := range vestingFunds.Funds {
			vf := vestingFunds.Funds[i]
			require.EqualValues(t, expectedOffset, int64(vf.Epoch)%int64(miner.RewardVestingSpecV0.Quantization))
		}

		assert.Equal(t, amt, st.LockedFunds)
	})

	t.Run("funds vest when under collateralized", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)

		assert.Equal(t, big.Zero(), st.LockedFunds)

		balance := rt.Balance()
		st.InitialPledgeRequirement = big.Mul(big.NewInt(2), balance) // ip req twice total balance
		availableBefore := st.GetAvailableBalance(balance)
		assert.True(t, availableBefore.LessThan(big.Zero()))
		rt.ReplaceState(st)

		amt := abi.NewTokenAmount(600_000)
		actor.addLockedFunds(rt, amt)
		// manually update actor balance to include the added funds from outside
		newBalance := big.Add(balance, amt)
		rt.SetBalance(newBalance)

		st = getState(rt)
		// no funds used to pay off ip debt
		assert.Equal(t, availableBefore, st.GetAvailableBalance(newBalance))
		assert.False(t, st.MeetsInitialPledgeCondition(newBalance))
		// all funds locked in vesting table
		assert.Equal(t, amt, st.LockedFunds)
	})

	t.Run("unvested funds will recollateralize a miner", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)

		balance := rt.Balance()
		st.InitialPledgeRequirement = balance
		underCollateralizedBalance := big.Div(balance, big.NewInt(2)) // ip req twice total balance
		assert.False(t, st.MeetsInitialPledgeCondition(underCollateralizedBalance))

		st.InitialPledgeRequirement = balance
		assert.True(t, st.MeetsInitialPledgeCondition(balance))
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
				actor.preCommitSector(rt, precommit)
			})
		}

		{
			precommit := actor.makePreCommit(targetSno+2, precommitEpoch-1, expiration, nil)
			actor.preCommitSector(rt, precommit)
		}
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

		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(actor.a.CompactSectorNumbers, &miner.CompactSectorNumbersParams{
				MaskSectorNumbers: bf(uint64(targetSno), uint64(targetSno)+1),
			})
		})

		rt.Verify()
	})

	t.Run("compacting no sector numbers aborts", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			// compact nothing
			actor.compactSectorNumbers(rt, bf())
		})
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

	epochReward     abi.TokenAmount
	networkPledge   abi.TokenAmount
	networkRawPower abi.StoragePower
	networkQAPower  abi.StoragePower
	baselinePower   abi.StoragePower

	epochRewardSmooth  *smoothing.FilterEstimate
	epochQAPowerSmooth *smoothing.FilterEstimate
}

func newHarness(t testing.TB, provingPeriodOffset abi.ChainEpoch) *actorHarness {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)

	controlAddrs := []addr.Address{tutil.NewIDAddr(t, 999), tutil.NewIDAddr(t, 998), tutil.NewIDAddr(t, 997)}

	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	rwd := big.Mul(big.NewIntUnsigned(100), big.NewIntUnsigned(1e18))
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

		epochReward:     rwd,
		networkPledge:   big.Mul(rwd, big.NewIntUnsigned(1000)),
		networkRawPower: pwr,
		networkQAPower:  pwr,
		baselinePower:   pwr,

		epochRewardSmooth:  smoothing.TestingConstantEstimate(rwd),
		epochQAPowerSmooth: smoothing.TestingConstantEstimate(pwr),
	}
	h.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
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
	nextProvingPeriodEnd := h.periodOffset - 1
	for nextProvingPeriodEnd < rt.Epoch() {
		nextProvingPeriodEnd += miner.WPoStProvingPeriod
	}
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		makeDeadlineCronEventParams(h.t, nextProvingPeriodEnd), big.Zero(), nil, exitcode.Ok)
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
		expanded, err := bf.All(miner.SectorsMax)
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

//
// Actor method calls
//

func (h *actorHarness) changeWorkerAddress(rt *mock.Runtime, newWorker addr.Address, effectiveEpoch abi.ChainEpoch, newControlAddrs []addr.Address) {
	rt.SetAddressActorType(newWorker, builtin.AccountActorCodeID)

	param := &miner.ChangeWorkerAddressParams{}
	param.NewControlAddrs = newControlAddrs
	param.NewWorker = newWorker
	rt.ExpectSend(newWorker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &h.key, exitcode.Ok)

	if newWorker != h.worker {
		cronPayload := miner.CronEventPayload{
			EventType: miner.CronEventWorkerKeyChange,
		}
		payload := new(bytes.Buffer)
		err := cronPayload.MarshalCBOR(payload)
		require.NoError(h.t, err)
		cronEvt := &power.EnrollCronEventParams{
			EventEpoch: effectiveEpoch,
			Payload:    payload.Bytes(),
		}

		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			cronEvt, big.Zero(), nil, exitcode.Ok)
	}

	rt.ExpectValidateCallerAddr(h.owner)
	rt.SetCaller(h.owner, builtin.AccountActorCodeID)
	rt.Call(h.a.ChangeWorkerAddress, param)
	rt.Verify()

	st := getState(rt)
	info, err := st.GetInfo(adt.AsStore(rt))
	require.NoError(h.t, err)

	if newWorker != h.worker {
		require.EqualValues(h.t, effectiveEpoch, info.PendingWorkerKey.EffectiveAt)
		require.EqualValues(h.t, newWorker, info.PendingWorkerKey.NewWorker)
	}

	var controlAddrs []addr.Address
	for _, ca := range newControlAddrs {
		resolved, found := rt.GetIdAddr(ca)
		require.True(h.t, found)
		controlAddrs = append(controlAddrs, resolved)
	}
	require.EqualValues(h.t, controlAddrs, info.ControlAddresses)

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

func (h *actorHarness) cronWorkerAddrChange(rt *mock.Runtime, effectiveEpoch abi.ChainEpoch, newWorker addr.Address) {
	rt.SetEpoch(effectiveEpoch)
	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	rt.Call(h.a.OnDeferredCronEvent, &miner.CronEventPayload{
		EventType: miner.CronEventWorkerKeyChange,
	})
	rt.Verify()

	st := getState(rt)
	info, err := st.GetInfo(adt.AsStore(rt))
	require.NoError(h.t, err)
	require.Nil(h.t, info.PendingWorkerKey)
	require.EqualValues(h.t, newWorker, info.Worker)
}

func (h *actorHarness) controlAddresses(rt *mock.Runtime) (owner, worker addr.Address, control []addr.Address) {
	rt.ExpectValidateCallerAny()
	ret := rt.Call(h.a.ControlAddresses, nil).(*miner.GetControlAddressesReturn)
	require.NotNil(h.t, ret)
	rt.Verify()
	return ret.Owner, ret.Worker, ret.ControlAddrs
}

func (h *actorHarness) preCommitSector(rt *mock.Runtime, params *miner.SectorPreCommitInfo) *miner.SectorPreCommitOnChainInfo {

	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	{
		expectQueryNetworkInfo(rt, h)
	}
	{
		sectorSize, err := params.SealProof.SectorSize()
		require.NoError(h.t, err)

		vdParams := market.VerifyDealsForActivationParams{
			DealIDs:      params.DealIDs,
			SectorStart:  rt.Epoch(),
			SectorExpiry: params.Expiration,
		}

		vdReturn := market.VerifyDealsForActivationReturn{
			DealWeight:         big.NewInt(int64(sectorSize / 2)),
			VerifiedDealWeight: big.NewInt(int64(sectorSize / 2)),
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.VerifyDealsForActivation, &vdParams, big.Zero(), &vdReturn, exitcode.Ok)
	}

	rt.Call(h.a.PreCommitSector, params)
	rt.Verify()
	return h.getPreCommit(rt, params.SectorNumber)
}

// Options for proveCommitSector behaviour.
// Default zero values should let everything be ok.
type proveCommitConf struct {
	verifyDealsExit map[abi.SectorNumber]exitcode.ExitCode
}

func (h *actorHarness) proveCommitSector(rt *mock.Runtime, precommit *miner.SectorPreCommitInfo, precommitEpoch abi.ChainEpoch,
	params *miner.ProveCommitSectorParams) {
	commd := cbg.CborCid(tutil.MakeCID("commd", &market.PieceCIDPrefix))
	sealRand := abi.SealRandomness([]byte{1, 2, 3, 4})
	sealIntRand := abi.InteractiveSealRandomness([]byte{5, 6, 7, 8})
	interactiveEpoch := precommitEpoch + miner.PreCommitChallengeDelay

	// Prepare for and receive call to ProveCommitSector
	{
		cdcParams := market.ComputeDataCommitmentParams{
			DealIDs:    precommit.DealIDs,
			SectorType: precommit.SealProof,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ComputeDataCommitment, &cdcParams, big.Zero(), &commd, exitcode.Ok)
	}
	{
		var buf bytes.Buffer
		receiver := rt.Receiver()
		err := receiver.MarshalCBOR(&buf)
		require.NoError(h.t, err)
		rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_SealRandomness, precommit.SealRandEpoch, buf.Bytes(), abi.Randomness(sealRand))
		rt.ExpectGetRandomnessBeacon(crypto.DomainSeparationTag_InteractiveSealChallengeSeed, interactiveEpoch, buf.Bytes(), abi.Randomness(sealIntRand))
	}
	{
		actorId, err := addr.IDFromAddress(h.receiver)
		require.NoError(h.t, err)
		seal := proof.SealVerifyInfo{
			SectorID: abi.SectorID{
				Miner:  abi.ActorID(actorId),
				Number: precommit.SectorNumber,
			},
			SealedCID:             precommit.SealedCID,
			SealProof:             precommit.SealProof,
			Proof:                 params.Proof,
			DealIDs:               precommit.DealIDs,
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

func (h *actorHarness) confirmSectorProofsValid(rt *mock.Runtime, conf proveCommitConf, precommits ...*miner.SectorPreCommitInfo) {
	// expect calls to get network stats
	expectQueryNetworkInfo(rt, h)

	// Prepare for and receive call to ConfirmSectorProofsValid.
	var validPrecommits []*miner.SectorPreCommitInfo
	var allSectorNumbers []abi.SectorNumber
	for _, precommit := range precommits {
		allSectorNumbers = append(allSectorNumbers, precommit.SectorNumber)

		vdParams := market.ActivateDealsParams{
			DealIDs:      precommit.DealIDs,
			SectorExpiry: precommit.Expiration,
		}
		exit, found := conf.verifyDealsExit[precommit.SectorNumber]
		if !found {
			exit = exitcode.Ok
			validPrecommits = append(validPrecommits, precommit)
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ActivateDeals, &vdParams, big.Zero(), nil, exit)
	}

	// expected pledge is the sum of initial pledges
	if len(validPrecommits) > 0 {
		expectPledge := big.Zero()

		expectQAPower := big.Zero()
		expectRawPower := big.Zero()
		for _, precommit := range validPrecommits {
			precommitOnChain := h.getPreCommit(rt, precommit.SectorNumber)

			duration := precommit.Expiration - rt.Epoch()
			if duration >= miner.MinSectorExpiration {
				qaPowerDelta := miner.QAPowerForWeight(h.sectorSize, duration, precommitOnChain.DealWeight, precommitOnChain.VerifiedDealWeight)
				expectQAPower = big.Add(expectQAPower, qaPowerDelta)
				expectRawPower = big.Add(expectRawPower, big.NewIntUnsigned(uint64(h.sectorSize)))
				pledge := miner.InitialPledgeForPower(qaPowerDelta, h.baselinePower, h.networkPledge,
					h.epochRewardSmooth, h.epochQAPowerSmooth, rt.TotalFilCircSupply())
				expectPledge = big.Add(expectPledge, pledge)
			}
		}

		if !expectRawPower.IsZero() || !expectQAPower.IsZero() {
			pcParams := power.UpdateClaimedPowerParams{
				RawByteDelta:         expectRawPower,
				QualityAdjustedDelta: expectQAPower,
			}
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, &pcParams, big.Zero(), nil, exitcode.Ok)
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

func (h *actorHarness) proveCommitSectorAndConfirm(rt *mock.Runtime, precommit *miner.SectorPreCommitInfo, precommitEpoch abi.ChainEpoch,
	params *miner.ProveCommitSectorParams, conf proveCommitConf) *miner.SectorOnChainInfo {
	h.proveCommitSector(rt, precommit, precommitEpoch, params)
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
	precommits := make([]*miner.SectorPreCommitInfo, n)
	for i := 0; i < n; i++ {
		sectorNo := h.nextSectorNo
		var sectorDealIDs []abi.DealID
		if dealIDs != nil {
			sectorDealIDs = dealIDs[i]
		}
		precommit := h.makePreCommit(sectorNo, precommitEpoch-1, expiration, sectorDealIDs)
		h.preCommitSector(rt, precommit)
		precommits[i] = precommit
		h.nextSectorNo++
	}

	advanceToEpochWithCron(rt, h, precommitEpoch+miner.PreCommitChallengeDelay+1)

	info := []*miner.SectorOnChainInfo{}
	for _, pc := range precommits {
		sector := h.proveCommitSectorAndConfirm(rt, pc, precommitEpoch, makeProveCommit(pc.SectorNumber), proveCommitConf{})
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
	precommit := h.makePreCommit(sectorNo, precommitEpoch-1, expiration, dealIDs)
	h.preCommitSector(rt, precommit)

	advanceToEpochWithCron(rt, h, precommitEpoch+miner.PreCommitChallengeDelay+1)

	sectorInfo := h.proveCommitSectorAndConfirm(rt, precommit, precommitEpoch, makeProveCommit(precommit.SectorNumber), proveCommitConf{})
	rt.Reset()
	return sectorInfo
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

type poStConfig struct {
	expectedRawPowerDelta abi.StoragePower
	expectedQAPowerDelta  abi.StoragePower
}

func (h *actorHarness) submitWindowPoSt(rt *mock.Runtime, deadline *dline.Info, partitions []miner.PoStPartition, infos []*miner.SectorOnChainInfo, poStCfg *poStConfig) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	commitRand := abi.Randomness("chaincommitment")
	commitEpoch := rt.Epoch() - 4
	rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_PoStChainCommit, commitEpoch, nil, commitRand)

	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	expectQueryNetworkInfo(rt, h)

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
		rt.ExpectVerifyPoSt(vi, nil)
	}
	if poStCfg != nil {
		// expect power update
		if !poStCfg.expectedRawPowerDelta.IsZero() || !poStCfg.expectedQAPowerDelta.IsZero() {
			claim := &power.UpdateClaimedPowerParams{
				RawByteDelta:         poStCfg.expectedRawPowerDelta,
				QualityAdjustedDelta: poStCfg.expectedQAPowerDelta,
			}
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, claim, abi.NewTokenAmount(0),
				nil, exitcode.Ok)
		}
	}

	params := miner.SubmitWindowedPoStParams{
		Deadline:         deadline.Index,
		Partitions:       partitions,
		Proofs:           proofs,
		ChainCommitEpoch: commitEpoch,
		ChainCommitRand:  commitRand,
	}

	rt.Call(h.a.SubmitWindowedPoSt, &params)
	rt.Verify()
}

func (h *actorHarness) declareFaults(rt *mock.Runtime, faultSectorInfos ...*miner.SectorOnChainInfo) {
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
}

func (h *actorHarness) declareRecoveries(rt *mock.Runtime, deadlineIdx uint64, partitionIdx uint64, recoverySectors bitfield.BitField) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

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

func (h *actorHarness) terminateSectors(rt *mock.Runtime, sectors bitfield.BitField, expectedFee abi.TokenAmount) {
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

	{
		expectQueryNetworkInfo(rt, h)
	}

	pledgeDelta := big.Zero()
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
	{
		sectorPower := miner.PowerForSectors(h.sectorSize, sectorInfos)
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
}

func (h *actorHarness) reportConsensusFault(rt *mock.Runtime, from addr.Address, params *miner.ReportConsensusFaultParams, dealIDs []abi.DealID) {
	rt.SetCaller(from, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

	rt.ExpectVerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra, &runtime.ConsensusFault{
		Target: h.receiver,
		Epoch:  rt.Epoch() - 1,
		Type:   runtime.ConsensusFaultDoubleForkMining,
	}, nil)

	// slash reward
	rwd := miner.RewardForConsensusSlashReport(1, rt.Balance())
	rt.ExpectSend(from, builtin.MethodSend, nil, rwd, nil, exitcode.Ok)

	// power termination
	lockedFunds := getState(rt).LockedFunds
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.OnConsensusFault, &lockedFunds, abi.NewTokenAmount(0), nil, exitcode.Ok)

	// expect sends to be batched into a limited number of deals
	for len(dealIDs) > 0 {
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

	// expect actor to be deleted
	rt.ExpectDeleteActor(builtin.BurntFundsActorAddr)

	rt.Call(h.a.ReportConsensusFault, params)
	rt.Verify()
}

func (h *actorHarness) addLockedFunds(rt *mock.Runtime, amt abi.TokenAmount) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker, builtin.RewardActorAddr)...)
	// expect pledge update
	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdatePledgeTotal,
		&amt,
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)

	rt.Call(h.a.AddLockedFund, &amt)
	rt.Verify()
}

type cronConfig struct {
	expectedEnrollment        abi.ChainEpoch
	vestingPledgeDelta        abi.TokenAmount // nolint:structcheck,unused
	detectedFaultsPowerDelta  *miner.PowerPair
	expiredSectorsPowerDelta  *miner.PowerPair
	expiredSectorsPledgeDelta abi.TokenAmount
	ongoingFaultsPenalty      abi.TokenAmount
}

func (h *actorHarness) onDeadlineCron(rt *mock.Runtime, config *cronConfig) {
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)

	// Preamble
	rwd := reward.ThisEpochRewardReturn{
		ThisEpochReward:         h.epochReward,
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
	if !config.ongoingFaultsPenalty.Nil() && !config.ongoingFaultsPenalty.IsZero() {
		penaltyTotal = big.Add(penaltyTotal, config.ongoingFaultsPenalty)
	}
	if !penaltyTotal.IsZero() {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penaltyTotal, nil, exitcode.Ok)
		pledgeDelta = big.Sub(pledgeDelta, penaltyTotal)
	}

	if !config.expiredSectorsPledgeDelta.Nil() && !config.expiredSectorsPledgeDelta.IsZero() {
		pledgeDelta = big.Add(pledgeDelta, config.expiredSectorsPledgeDelta)
	}
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

func (h *actorHarness) withdrawFunds(rt *mock.Runtime, amount abi.TokenAmount) {
	rt.SetCaller(h.owner, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.owner)

	rt.ExpectSend(h.owner, builtin.MethodSend, nil, amount, nil, exitcode.Ok)

	rt.Call(h.a.WithdrawBalance, &miner.WithdrawBalanceParams{
		AmountRequested: amount,
	})
	rt.Verify()
}

func (h *actorHarness) declaredFaultPenalty(sectors []*miner.SectorOnChainInfo, nv network.Version) abi.TokenAmount {
	_, qa := powerForSectors(h.sectorSize, sectors)
	return miner.PledgePenaltyForDeclaredFault(h.epochRewardSmooth, h.epochQAPowerSmooth, qa, nv)
}

func (h *actorHarness) undeclaredFaultPenalty(sectors []*miner.SectorOnChainInfo, nv network.Version) abi.TokenAmount {
	_, qa := powerForSectors(h.sectorSize, sectors)
	return miner.PledgePenaltyForUndeclaredFault(h.epochRewardSmooth, h.epochQAPowerSmooth, qa, nv)
}

func (h *actorHarness) powerPairForSectors(sectors []*miner.SectorOnChainInfo) miner.PowerPair {
	rawPower, qaPower := powerForSectors(h.sectorSize, sectors)
	return miner.NewPowerPair(rawPower, qaPower)
}

func (h *actorHarness) makePreCommit(sectorNo abi.SectorNumber, challenge, expiration abi.ChainEpoch, dealIDs []abi.DealID) *miner.SectorPreCommitInfo {
	return &miner.SectorPreCommitInfo{
		SealProof:     h.sealProofType,
		SectorNumber:  sectorNo,
		SealedCID:     tutil.MakeCID("commr", &miner.SealedCIDPrefix),
		SealRandEpoch: challenge,
		DealIDs:       dealIDs,
		Expiration:    expiration,
	}
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
			partitions := []miner.PoStPartition{}
			for _, sector := range dlSectors {
				_, pIdx, err := st.FindSector(rt.AdtStore(), sector.SectorNumber)
				require.NoError(h.t, err)
				partitions = append(partitions, miner.PoStPartition{Index: pIdx, Skipped: bitfield.New()})
			}
			h.submitWindowPoSt(rt, dlinfo, partitions, dlSectors, nil)
			delete(deadlines, dlinfo.Index)
		}

		advanceDeadline(rt, h, &cronConfig{})
		dlinfo = h.deadline(rt)
	}
}

//
// Construction helpers, etc
//

func builderForHarness(actor *actorHarness) *mock.RuntimeBuilder {
	rb := mock.NewBuilder(context.Background(), actor.receiver).
		WithNetworkVersion(network.Version3).
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

	declarationMap := map[miner.PartitionKey]*miner.FaultDeclaration{}
	for _, sector := range faultSectorInfos {
		dlIdx, pIdx, err := miner.FindSector(store, deadlines, sector.SectorNumber)
		require.NoError(t, err)

		declaration, ok := declarationMap[miner.PartitionKey{dlIdx, pIdx}]
		if !ok {
			declaration = &miner.FaultDeclaration{
				Deadline:  dlIdx,
				Partition: pIdx,
				Sectors:   bf(),
			}
			declarationMap[miner.PartitionKey{dlIdx, pIdx}] = declaration
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
		ThisEpochReward:         h.epochReward,
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
