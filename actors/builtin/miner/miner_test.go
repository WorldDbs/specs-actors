package miner_test

import (
	"bytes"
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
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	cid "github.com/ipfs/go-cid"
	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v5/actors/runtime"
	"github.com/filecoin-project/specs-actors/v5/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v5/actors/util/smoothing"
	"github.com/filecoin-project/specs-actors/v5/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v5/support/testing"
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
	builder := mock.NewBuilder(receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithActorType(controlAddrs[0], builtin.AccountActorCodeID).
		WithActorType(controlAddrs[1], builtin.AccountActorCodeID).
		WithHasher(blake2b.Sum256).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		params := miner.ConstructorParams{
			OwnerAddr:           owner,
			WorkerAddr:          worker,
			ControlAddrs:        controlAddrs,
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			PeerId:              testPid,
			Multiaddrs:          testMultiaddrs,
		}

		provingPeriodStart := abi.ChainEpoch(-2222) // This is just set from running the code.
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		// Fetch worker pubkey.
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
		// Register proving period cron.
		dlIdx := (rt.Epoch() - provingPeriodStart) / miner.WPoStChallengeWindow
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
		assert.Equal(t, abi.RegisteredPoStProof_StackedDrgWindow32GiBV1, info.WindowPoStProofType)
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
			assertEmptyBitfield(t, deadline.PartitionsPoSted)
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
			OwnerAddr:           owner,
			WorkerAddr:          worker,
			ControlAddrs:        []addr.Address{control1, control2},
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
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
			OwnerAddr:           owner,
			WorkerAddr:          worker,
			ControlAddrs:        []addr.Address{control1},
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
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
			OwnerAddr:           owner,
			WorkerAddr:          worker,
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			PeerId:              pid[:],
			Multiaddrs:          testMultiaddrs,
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
			OwnerAddr:           owner,
			WorkerAddr:          worker,
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			PeerId:              testPid,
			ControlAddrs:        controlAddrs,
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
			OwnerAddr:           owner,
			WorkerAddr:          worker,
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			PeerId:              testPid,
			Multiaddrs:          maddrs,
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
			OwnerAddr:           owner,
			WorkerAddr:          worker,
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			PeerId:              testPid,
			Multiaddrs:          maddrs,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid empty multiaddr", func() {
			rt.Call(actor.Constructor, &params)
		})
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
		upgrade := actor.preCommitSector(rt, upgradeParams, preCommitConf{}, false)

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
		oldSectors := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, [][]abi.DealID{nil, {10}}, true)

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
				actor.preCommitSector(rt, &params, preCommitConf{}, false)
			})
			rt.Reset()
		}
		{ // Old sector cannot have deals
			params := *upgradeParams
			params.ReplaceSectorNumber = oldSectors[1].SectorNumber
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, &params, preCommitConf{}, false)
			})
			rt.Reset()
		}
		{ // Target sector must exist
			params := *upgradeParams
			params.ReplaceSectorNumber = 999
			rt.ExpectAbort(exitcode.ErrNotFound, func() {
				actor.preCommitSector(rt, &params, preCommitConf{}, false)
			})
			rt.Reset()
		}
		{ // Target partition must exist
			params := *upgradeParams
			params.ReplaceSectorPartition = 999
			rt.ExpectAbortContainsMessage(exitcode.ErrNotFound, "no partition 999", func() {
				actor.preCommitSector(rt, &params, preCommitConf{}, false)
			})
			rt.Reset()
		}
		{ // Expiration must not be sooner than target
			params := *upgradeParams
			params.Expiration = params.Expiration - miner.WPoStProvingPeriod
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, &params, preCommitConf{}, false)
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
			newFaults, _, _, err := partition.RecordFaults(rt.AdtStore(), sectorArr, bf(uint64(oldSectors[0].SectorNumber)), 100000,
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
				actor.preCommitSector(rt, &params, preCommitConf{}, false)
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
				actor.preCommitSector(rt, &params, preCommitConf{}, false)
			})
			rt.ReplaceState(&prevState)
			rt.Reset()
		}

		// Demonstrate that the params are otherwise ok
		actor.preCommitSector(rt, upgradeParams, preCommitConf{}, false)
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
		upgrade := actor.preCommitSector(rt, upgradeParams, preCommitConf{}, false)

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

		advanceToEpochWithCron(rt, actor, rt.Epoch())
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
		queue, err := miner.LoadExpirationQueue(rt.AdtStore(), partition.ExpirationsEpochs, miner.QuantSpecForDeadline(dlInfo), miner.PartitionExpirationAmtBitwidth)
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

		advanceToEpochWithCron(rt, actor, rt.Epoch())

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
		upgrade1 := actor.preCommitSector(rt, upgradeParams1, preCommitConf{}, false)

		// Check new pre-commit in state
		assert.True(t, upgrade1.Info.ReplaceCapacity)
		assert.Equal(t, upgradeParams1.ReplaceSectorNumber, upgrade1.Info.ReplaceSectorNumber)

		// Upgrade 2
		upgradeParams2 := actor.makePreCommit(201, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams2.ReplaceCapacity = true
		upgradeParams2.ReplaceSectorDeadline = dlIdx
		upgradeParams2.ReplaceSectorPartition = partIdx
		upgradeParams2.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade2 := actor.preCommitSector(rt, upgradeParams2, preCommitConf{}, false)

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
}

func TestWindowPost(t *testing.T) {
	// Remove this nasty static/global access when policy is encapsulated in a structure.
	// See https://github.com/filecoin-project/specs-actors/issues/353.
	miner.WindowPoStProofTypes[abi.RegisteredPoStProof_StackedDrgWindow2KiBV1] = struct{}{}
	defer func() {
		delete(miner.WindowPoStProofTypes, abi.RegisteredPoStProof_StackedDrgWindow2KiBV1)
	}()

	periodOffset := abi.ChainEpoch(100)
	precommitEpoch := abi.ChainEpoch(1)

	t.Run("basic PoSt and dispute", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		builder := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero())

		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)[0]
		pwr := miner.PowerForSector(actor.sectorSize, sector)

		// Skip to the right deadline.
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(store, sector.SectorNumber)
		require.NoError(t, err)
		dlinfo := advanceToDeadline(rt, actor, dlIdx)

		// Submit PoSt
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{sector}, &poStConfig{
			expectedPowerDelta: pwr,
		})

		// Verify proof recorded
		deadline := actor.getDeadline(rt, dlIdx)
		assertBitfieldEquals(t, deadline.PartitionsPoSted, pIdx)

		postsCid := deadline.OptimisticPoStSubmissions

		posts, err := adt.AsArray(store, postsCid, miner.DeadlineOptimisticPoStSubmissionsAmtBitwidth)
		require.NoError(t, err)
		require.EqualValues(t, posts.Length(), 1)
		var post miner.WindowedPoSt
		found, err := posts.Get(0, &post)
		require.NoError(t, err)
		require.True(t, found)
		assertBitfieldEquals(t, post.Partitions, pIdx)

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
		actor.checkState(rt)

		deadline = actor.getDeadline(rt, dlIdx)

		// Proofs should exist in snapshot.
		require.Equal(t, deadline.OptimisticPoStSubmissionsSnapshot, postsCid)

		// Try a failed dispute.
		var result *poStDisputeResult
		actor.disputeWindowPoSt(rt, dlinfo, 0, []*miner.SectorOnChainInfo{sector}, result)

		// Now a successful dispute.
		expectedFee := miner.PledgePenaltyForInvalidWindowPoSt(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwr.QA)
		result = &poStDisputeResult{
			expectedPowerDelta:  pwr.Neg(),
			expectedPenalty:     expectedFee,
			expectedReward:      miner.BaseRewardForDisputedWindowPoSt,
			expectedPledgeDelta: big.Zero(),
		}
		actor.disputeWindowPoSt(rt, dlinfo, 0, []*miner.SectorOnChainInfo{sector}, result)
	})

	t.Run("invalid submissions", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)[0]
		pwr := miner.PowerForSector(actor.sectorSize, sector)

		// Skip to the due deadline.
		dlIdx, pIdx, err := getState(rt).FindSector(store, sector.SectorNumber)
		require.NoError(t, err)
		dlInfo := advanceToDeadline(rt, actor, dlIdx)

		// Invalid deadline.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid deadline", func() {
			partitions := []miner.PoStPartition{{Index: pIdx, Skipped: bf()}}
			params := miner.SubmitWindowedPoStParams{
				Deadline:         miner.WPoStPeriodDeadlines,
				Partitions:       partitions,
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// No partitions.
		// This is a weird message because we don't check this precondition explicitly.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "expected proof to be smaller", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Too many partitions.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too many partitions", func() {
			partitions := make([]miner.PoStPartition, 11)
			for i, p := range partitions {
				p.Index = pIdx + uint64(i)
			}
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       partitions,
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Invalid partition index.
		rt.ExpectAbortContainsMessage(exitcode.ErrNotFound, "no such partition", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx + 1, Skipped: bf()}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Skip sectors that don't exist.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "skipped faults contains sectors outside partition", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf(123)}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Empty proofs array.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "expected exactly one proof", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           []proof.PoStProof{},
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Invalid proof type
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "proof type 6 not allowed", func() {
			proofs := makePoStProofs(abi.RegisteredPoStProof_StackedDrgWindow8MiBV1)
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           proofs,
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Unexpected proof type
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "expected proof of type", func() {
			proofs := makePoStProofs(abi.RegisteredPoStProof_StackedDrgWindow64GiBV1)
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           proofs,
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Proof too large
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "expected proof to be smaller", func() {
			proofs := makePoStProofs(actor.windowPostProofType)
			proofs[0].ProofBytes = make([]byte, 192+1)
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           proofs,
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Invalid randomness type
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "bytes of randomness", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("123456789012345678901234567890123"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Deadline not open.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid deadline 2 at epoch", func() {
			rt.SetEpoch(rt.Epoch() + miner.WPoStChallengeWindow)
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge + miner.WPoStProvingPeriod/2,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.SetEpoch(dlInfo.CurrentEpoch)
		rt.Reset()

		// Chain commit epoch too old.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "expected chain commit epoch", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge - 1,
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Chain commit epoch too new.
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be less than the current epoch", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: rt.Epoch(),
				ChainCommitRand:  abi.Randomness("chaincommitment"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Mismatched randomness
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "randomness mismatched", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("boo"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params,
				&poStConfig{chainRandomness: abi.Randomness("far")})
		})
		rt.Reset()

		// Skip all the sectors
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "no active sectors", func() {
			params := miner.SubmitWindowedPoStParams{
				Deadline:         dlInfo.Index,
				Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf(uint64(sector.SectorNumber))}},
				Proofs:           makePoStProofs(actor.windowPostProofType),
				ChainCommitEpoch: dlInfo.Challenge,
				ChainCommitRand:  abi.Randomness("boo"),
			}
			actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, nil)
		})
		rt.Reset()

		// Demonstrate the good params are good.
		params := miner.SubmitWindowedPoStParams{
			Deadline:         dlIdx,
			Partitions:       []miner.PoStPartition{{Index: pIdx, Skipped: bf()}},
			Proofs:           makePoStProofs(actor.windowPostProofType),
			ChainCommitEpoch: dlInfo.Challenge,
			ChainCommitRand:  abi.Randomness("chaincommitment"),
		}
		actor.submitWindowPoStRaw(rt, dlInfo, []*miner.SectorOnChainInfo{sector}, &params, &poStConfig{
			expectedPowerDelta: pwr,
		})
		rt.Verify()
	})

	t.Run("test duplicate proof rejected", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)[0]
		pwr := miner.PowerForSector(actor.sectorSize, sector)

		// Skip to the due deadline.
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(store, sector.SectorNumber)
		require.NoError(t, err)
		dlinfo := advanceToDeadline(rt, actor, dlIdx)

		// Submit PoSt
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{sector}, &poStConfig{
			expectedPowerDelta: pwr,
		})

		// Verify proof recorded
		deadline := actor.getDeadline(rt, dlIdx)
		assertBitfieldEquals(t, deadline.PartitionsPoSted, pIdx)

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
			Proofs:           makePoStProofs(actor.windowPostProofType),
			ChainCommitEpoch: dlinfo.Challenge,
			ChainCommitRand:  commitRand,
		}
		expectQueryNetworkInfo(rt, actor)
		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)

		// From version 7, a duplicate is explicitly rejected.
		rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)
		rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_PoStChainCommit, dlinfo.Challenge, nil, commitRand)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "partition already proven", func() {
			rt.Call(actor.a.SubmitWindowedPoSt, &params)
		})
		rt.Reset()

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
		actor.checkState(rt)
	})

	t.Run("test duplicate proof rejected with many partitions", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		// Commit more sectors than fit in one partition in every eligible deadline, overflowing to a second partition.
		sectorsToCommit := ((miner.WPoStPeriodDeadlines - 2) * actor.partitionSize) + 1
		sectors := actor.commitAndProveSectors(rt, int(sectorsToCommit), defaultSectorExpiration, nil, true)
		lastSector := sectors[len(sectors)-1]

		// Skip to the due deadline.
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(store, lastSector.SectorNumber)
		require.NoError(t, err)
		require.Equal(t, uint64(2), dlIdx) // Deadlines 0 and 1 are empty (no PoSt needed) because excluded by proximity
		require.Equal(t, uint64(1), pIdx)  // Overflowed from partition 0 to partition 1
		dlinfo := advanceToDeadline(rt, actor, dlIdx)

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
			assertBitfieldEquals(t, deadline.PartitionsPoSted, 0)
		}
		{
			// Attempt PoSt for both partitions, thus duplicating proof for partition 0, so rejected
			partitions := []miner.PoStPartition{
				{Index: 0, Skipped: bitfield.New()},
				{Index: 1, Skipped: bitfield.New()},
			}
			sectorsToProve := append(sectors[:actor.partitionSize], lastSector)
			pwr := miner.PowerForSectors(actor.sectorSize, sectorsToProve)

			// From network version 7, the miner outright rejects attempts to prove a partition twice.
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
			assertBitfieldEquals(t, deadline.PartitionsPoSted, 0, 1)
		}

		// Advance to end-of-deadline cron to verify no penalties.
		advanceDeadline(rt, actor, &cronConfig{})
		actor.checkState(rt)
	})

	t.Run("successful recoveries recover power", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
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

		// We restored power, so we should not have recorded a post.
		deadline = actor.getDeadline(rt, dlIdx)
		assertBitfieldEquals(t, deadline.PartitionsPoSted, pIdx)
		postsCid := deadline.OptimisticPoStSubmissions
		posts, err := adt.AsArray(rt.AdtStore(), postsCid,
			miner.DeadlineOptimisticPoStSubmissionsAmtBitwidth)
		require.NoError(t, err)
		require.EqualValues(t, posts.Length(), 0)

		// Next deadline cron does not charge for the fault
		advanceDeadline(rt, actor, &cronConfig{})

		assert.Equal(t, initialLocked, actor.getLockedFunds(rt))
		actor.checkState(rt)
	})

	t.Run("skipped faults adjust power", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil, true)

		actor.applyRewards(rt, bigRewards, big.Zero())

		// Skip to the due deadline.
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		dlIdx2, pIdx2, err := st.FindSector(rt.AdtStore(), infos[1].SectorNumber)
		require.NoError(t, err)
		assert.Equal(t, dlIdx, dlIdx2) // this test will need to change when these are not equal
		dlinfo := advanceToDeadline(rt, actor, dlIdx)

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
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil, true)

		actor.applyRewards(rt, bigRewards, big.Zero())

		// Skip to the due deadline.
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		dlIdx2, pIdx2, err := st.FindSector(rt.AdtStore(), infos[1].SectorNumber)
		require.NoError(t, err)
		assert.Equal(t, dlIdx, dlIdx2) // this test will need to change when these are not equal
		assert.Equal(t, pIdx, pIdx2)   // this test will need to change when these are not equal
		dlinfo := advanceToDeadline(rt, actor, dlIdx)

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
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		infos := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil, true)

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

		// Skip to the due deadline.
		dlinfo := advanceToDeadline(rt, actor, dlIdx)

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
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// create enough sectors that one will be in a different partition
		n := 95
		infos := actor.commitAndProveSectors(rt, n, defaultSectorExpiration, nil, true)

		// Skip to the due deadline.
		st := getState(rt)
		dlIdx0, pIdx0, err := st.FindSector(rt.AdtStore(), infos[0].SectorNumber)
		require.NoError(t, err)
		dlIdx1, pIdx1, err := st.FindSector(rt.AdtStore(), infos[n-1].SectorNumber)
		require.NoError(t, err)
		dlinfo := advanceToDeadline(rt, actor, dlIdx0)

		// if these assertions no longer hold, the test must be changed
		require.LessOrEqual(t, dlIdx0, dlIdx1)
		require.NotEqual(t, pIdx0, pIdx1)

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

	t.Run("cannot dispute posts when the challenge window is open", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)[0]
		pwr := miner.PowerForSector(actor.sectorSize, sector)

		// Skip to the due deadline.
		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(store, sector.SectorNumber)
		require.NoError(t, err)
		dlinfo := advanceToDeadline(rt, actor, dlIdx)

		// Submit PoSt
		partitions := []miner.PoStPartition{
			{Index: pIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlinfo, partitions, []*miner.SectorOnChainInfo{sector}, &poStConfig{
			expectedPowerDelta: pwr,
		})

		// Dispute it.
		params := miner.DisputeWindowedPoStParams{
			Deadline:  dlinfo.Index,
			PoStIndex: 0,
		}

		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

		expectQueryNetworkInfo(rt, actor)

		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "can only dispute window posts during the dispute window", func() {
			rt.Call(actor.a.DisputeWindowedPoSt, &params)
		})
		rt.Verify()
	})
	t.Run("can dispute up till window end, but not after", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		sector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)[0]

		st := getState(rt)
		dlIdx, _, err := st.FindSector(store, sector.SectorNumber)
		require.NoError(t, err)

		nextDl := miner.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rt.Epoch()).
			NextNotElapsed()

		advanceAndSubmitPoSts(rt, actor, sector)

		windowEnd := nextDl.Close + miner.WPoStDisputeWindow

		// first, try to dispute right before the window end.
		// We expect this to fail "normally" (fail to disprove).
		rt.SetEpoch(windowEnd - 1)
		actor.disputeWindowPoSt(rt, nextDl, 0, []*miner.SectorOnChainInfo{sector}, nil)

		// Now set the epoch at the window end. We expect a different error.
		rt.SetEpoch(windowEnd)

		// Now try to dispute.
		params := miner.DisputeWindowedPoStParams{
			Deadline:  dlIdx,
			PoStIndex: 0,
		}

		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

		currentReward := reward.ThisEpochRewardReturn{
			ThisEpochBaselinePower:  actor.baselinePower,
			ThisEpochRewardSmoothed: actor.epochRewardSmooth,
		}
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.ThisEpochReward, nil, big.Zero(), &currentReward, exitcode.Ok)

		networkPower := big.NewIntUnsigned(1 << 50)
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.CurrentTotalPower, nil, big.Zero(),
			&power.CurrentTotalPowerReturn{
				RawBytePower:            networkPower,
				QualityAdjPower:         networkPower,
				PledgeCollateral:        actor.networkPledge,
				QualityAdjPowerSmoothed: actor.epochQAPowerSmooth,
			},
			exitcode.Ok)

		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "can only dispute window posts during the dispute window", func() {
			rt.Call(actor.a.DisputeWindowedPoSt, &params)
		})
		rt.Verify()
	})

	t.Run("can't dispute up with an invalid deadline", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		params := miner.DisputeWindowedPoStParams{
			Deadline:  50,
			PoStIndex: 0,
		}

		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid deadline", func() {
			rt.Call(actor.a.DisputeWindowedPoSt, &params)
		})
		rt.Verify()
	})

	t.Run("can dispute test after proving period changes", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg2KiBV1_1)
		rt := builderForHarness(actor).
			WithEpoch(precommitEpoch).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		periodStart := actor.deadline(rt).NextPeriodStart()

		// go to the next deadline 0
		rt.SetEpoch(periodStart)

		// fill one partition in each mutable deadline.
		numSectors := int(actor.partitionSize * (miner.WPoStPeriodDeadlines - 2))

		// creates a partition in every deadline except 0 and 47
		sectors := actor.commitAndProveSectors(rt, numSectors, defaultSectorExpiration, nil, true)
		actor.t.Log("here")

		// prove every sector once to activate power. This
		// simplifies the test a bit.
		advanceAndSubmitPoSts(rt, actor, sectors...)

		// Make sure we're in the correct deadline. We should
		// finish at deadline 2 because precommit takes some
		// time.
		dlinfo := actor.deadline(rt)
		require.True(t, dlinfo.Index < 46,
			"we need to be before the target deadline for this test to make sense")

		// Now challenge find the sectors in the last partition.
		_, partition := actor.getDeadlineAndPartition(rt, 46, 0)
		var targetSectors []*miner.SectorOnChainInfo
		err := partition.Sectors.ForEach(func(i uint64) error {
			for _, sector := range sectors {
				if uint64(sector.SectorNumber) == i {
					targetSectors = append(targetSectors, sector)
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.NotEmpty(t, targetSectors)

		pwr := miner.PowerForSectors(actor.sectorSize, targetSectors)

		// And challenge the last partition.
		var result *poStDisputeResult
		expectedFee := miner.PledgePenaltyForInvalidWindowPoSt(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwr.QA)
		result = &poStDisputeResult{
			expectedPowerDelta:  pwr.Neg(),
			expectedPenalty:     expectedFee,
			expectedReward:      miner.BaseRewardForDisputedWindowPoSt,
			expectedPledgeDelta: big.Zero(),
		}

		targetDlInfo := miner.NewDeadlineInfo(periodStart, 46, rt.Epoch())
		actor.disputeWindowPoSt(rt, targetDlInfo, 0, targetSectors, result)
	})
}

func TestDeadlineCron(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("cron on inactive state", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)
		assert.Equal(t, periodOffset-miner.WPoStProvingPeriod, st.ProvingPeriodStart)
		assert.False(t, st.ContinueDeadlineCron())

		// cron does nothing and does not enroll another cron
		deadline := actor.deadline(rt)
		rt.SetEpoch(deadline.Last())
		actor.onDeadlineCron(rt, &cronConfig{noEnrollment: true})

		actor.checkState(rt)
	})

	t.Run("sector expires", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, sectors...)
		activePower := miner.PowerForSectors(actor.sectorSize, sectors)

		st := getState(rt)
		initialPledge := st.InitialPledge
		expirationRaw := sectors[0].Expiration
		assert.True(t, st.DeadlineCronActive)

		// setup state to simulate moving forward all the way to expiry

		dlIdx, _, err := st.FindSector(rt.AdtStore(), sectors[0].SectorNumber)
		require.NoError(t, err)
		expQuantSpec := st.QuantSpecForDeadline(dlIdx)
		expiration := expQuantSpec.QuantizeUp(expirationRaw)
		remainingEpochs := expiration - st.ProvingPeriodStart
		remainingPeriods := remainingEpochs/miner.WPoStProvingPeriod + 1
		st.ProvingPeriodStart += remainingPeriods * miner.WPoStProvingPeriod
		st.CurrentDeadline = dlIdx
		rt.ReplaceState(st)

		// Advance to expiration epoch and expect expiration during cron
		rt.SetEpoch(expiration)
		powerDelta := activePower.Neg()
		// because we skip forward in state the sector is detected faulty, no penalty
		advanceDeadline(rt, actor, &cronConfig{
			noEnrollment:              true, // the last power has expired so we expect cron to go inactive
			expiredSectorsPowerDelta:  &powerDelta,
			expiredSectorsPledgeDelta: initialPledge.Neg(),
		})
		st = getState(rt)
		assert.False(t, st.DeadlineCronActive)
		actor.checkState(rt)
	})

	t.Run("sector expires and repays fee debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, sectors...)
		activePower := miner.PowerForSectors(actor.sectorSize, sectors)

		st := getState(rt)
		initialPledge := st.InitialPledge
		expirationRaw := sectors[0].Expiration
		assert.True(t, st.DeadlineCronActive)

		// setup state to simulate moving forward all the way to expiry
		dlIdx, _, err := st.FindSector(rt.AdtStore(), sectors[0].SectorNumber)
		require.NoError(t, err)
		expQuantSpec := st.QuantSpecForDeadline(dlIdx)
		expiration := expQuantSpec.QuantizeUp(expirationRaw)
		remainingEpochs := expiration - st.ProvingPeriodStart
		remainingPeriods := remainingEpochs/miner.WPoStProvingPeriod + 1
		st.ProvingPeriodStart += remainingPeriods * miner.WPoStProvingPeriod
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
			noEnrollment:              true,
			expiredSectorsPowerDelta:  &powerDelta,
			expiredSectorsPledgeDelta: initialPledge.Neg(),
			repaidFeeDebt:             initialPledge, // We repay unlocked IP as fees
		})
		st = getState(rt)
		assert.False(t, st.DeadlineCronActive)
		actor.checkState(rt)
	})

	t.Run("detects and penalizes faults", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		activeSectors := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, nil, true)
		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, activeSectors...)
		activePower := miner.PowerForSectors(actor.sectorSize, activeSectors)

		unprovenSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, false)
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

	t.Run("test cron run trigger faults", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// add lots of funds so we can pay penalties without going into debt
		actor.applyRewards(rt, bigRewards, big.Zero())

		// create enough sectors that one will be in a different partition
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

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

		rt.SetEpoch(dlinfo.Last())

		// run cron and expect all sectors to be detected as faults (no penalty)
		pwr := miner.PowerForSectors(actor.sectorSize, allSectors)

		// power for sectors is removed
		powerDeltaClaim := miner.NewPowerPair(pwr.Raw.Neg(), pwr.QA.Neg())

		// expect next cron to be one deadline period after expected cron for this deadline
		nextCron := dlinfo.Last() + miner.WPoStChallengeWindow

		actor.onDeadlineCron(rt, &cronConfig{
			expectedEnrollment:       nextCron,
			detectedFaultsPowerDelta: &powerDeltaClaim,
		})
		actor.checkState(rt)
	})
}

// cronControl is a convenience harness on top of the actor harness giving the caller access to common
// sequences of miner actor actions useful for checking that cron starts, runs, stops and continues
type cronControl struct {
	rt           *mock.Runtime
	actor        *actorHarness
	preCommitNum int
}

func newCronControl(rt *mock.Runtime, actor *actorHarness) *cronControl {
	return &cronControl{
		rt:           rt,
		actor:        actor,
		preCommitNum: 0,
	}
}

// Start cron by precommitting at preCommitEpoch, return clean up epoch.
// Verifies that cron is not started, precommit is run and cron is enrolled.
// Returns epoch at which precommit is scheduled for clean up and removed from state by cron.
func (h *cronControl) preCommitToStartCron(t *testing.T, preCommitEpoch abi.ChainEpoch) abi.ChainEpoch {
	h.rt.SetEpoch(preCommitEpoch)
	st := getState(h.rt)
	h.requireCronInactive(t)

	dlinfo := miner.NewDeadlineInfoFromOffsetAndEpoch(st.ProvingPeriodStart, preCommitEpoch) // actor.deadline might be out of date
	sectorNo := abi.SectorNumber(h.preCommitNum)
	h.preCommitNum++
	expiration := dlinfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // something on deadline boundary but > 180 days
	precommitParams := h.actor.makePreCommit(sectorNo, preCommitEpoch-1, expiration, nil)
	h.actor.preCommitSector(h.rt, precommitParams, preCommitConf{}, true)

	// PCD != 0 so cron must be active
	h.requireCronActive(t)

	cleanUpEpoch := preCommitEpoch + miner.MaxProveCommitDuration[h.actor.sealProofType] + miner.ExpiredPreCommitCleanUpDelay
	return cleanUpEpoch
}

// Stop cron by advancing to the preCommit clean up epoch.
// Assumes no proved sectors, no vesting funds.
// Verifies cron runs until clean up, PCD burnt and cron discontinued during last deadline
// Return open of first deadline after expiration.
func (h *cronControl) expirePreCommitStopCron(t *testing.T, startEpoch, cleanUpEpoch abi.ChainEpoch) abi.ChainEpoch {
	h.requireCronActive(t)
	st := getState(h.rt)
	dlinfo := miner.NewDeadlineInfoFromOffsetAndEpoch(st.ProvingPeriodStart, startEpoch) // actor.deadline might be out of date

	for dlinfo.Open <= cleanUpEpoch { // PCDs are quantized to be burnt on the *next* new deadline after the one they are cleaned up in
		// asserts cron is rescheduled
		dlinfo = advanceDeadline(h.rt, h.actor, &cronConfig{})
	}
	// We expect PCD burnt and cron not rescheduled here.
	h.rt.SetEpoch(dlinfo.Last())
	h.actor.onDeadlineCron(h.rt, &cronConfig{
		noEnrollment:            true,
		expiredPrecommitPenalty: st.PreCommitDeposits,
	})
	h.rt.SetEpoch(dlinfo.NextOpen())

	h.requireCronInactive(t)
	return h.rt.Epoch()
}

func (h *cronControl) requireCronInactive(t *testing.T) {
	st := getState(h.rt)
	assert.False(t, st.DeadlineCronActive)     // No cron running now
	assert.False(t, st.ContinueDeadlineCron()) // No reason to cron now, state inactive
}

func (h *cronControl) requireCronActive(t *testing.T) {
	st := getState(h.rt)
	require.True(t, st.DeadlineCronActive)
	require.True(t, st.ContinueDeadlineCron())
}

func (h *cronControl) preCommitStartCronExpireStopCron(t *testing.T, startEpoch abi.ChainEpoch) abi.ChainEpoch {
	cleanUpEpoch := h.preCommitToStartCron(t, startEpoch)
	return h.expirePreCommitStopCron(t, startEpoch, cleanUpEpoch)
}

func TestDeadlineCronDefersStopsRestarts(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("cron enrolls on precommit, prove commits and continues enrolling", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		cronCtrl := newCronControl(rt, actor)
		longExpiration := uint64(500)

		cronCtrl.requireCronInactive(t)
		sectors := actor.commitAndProveSectors(rt, 1, longExpiration, nil, true)
		cronCtrl.requireCronActive(t)

		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, sectors...)
		// advance 499 days of deadline (1 before expiration occurrs)
		// this asserts that cron continues to enroll within advanceAndSubmitPoSt
		for i := 0; i < 499; i++ {
			advanceAndSubmitPoSts(rt, actor, sectors...)
		}
		actor.checkState(rt)
		st := getState(rt)
		assert.True(t, st.DeadlineCronActive)
	})

	t.Run("cron enrolls on precommit, expires on pcd expiration, re-enrolls on new precommit immediately", func(t *testing.T) {
		rt := builder.Build(t)
		epoch := periodOffset + 1
		rt.SetEpoch(epoch)
		actor.constructAndVerify(rt)
		cronCtrl := newCronControl(rt, actor)

		epoch = cronCtrl.preCommitStartCronExpireStopCron(t, epoch)
		cronCtrl.preCommitToStartCron(t, epoch)
	})

	t.Run("cron enrolls on precommit, expires on pcd expiration, re-enrolls on new precommit after falling out of date", func(t *testing.T) {
		rt := builder.Build(t)
		epoch := periodOffset + 1
		rt.SetEpoch(epoch)
		actor.constructAndVerify(rt)
		cronCtrl := newCronControl(rt, actor)

		epoch = cronCtrl.preCommitStartCronExpireStopCron(t, epoch)
		// Advance some epochs to fall several pp out of date, then precommit again reenrolling cron
		epoch = epoch + 200*miner.WPoStProvingPeriod
		epoch = cronCtrl.preCommitStartCronExpireStopCron(t, epoch)
		// Stay within the same deadline but advance an epoch
		epoch = epoch + 1
		cronCtrl.preCommitToStartCron(t, epoch)
	})

	t.Run("enroll, pcd expire, re-enroll x 3", func(t *testing.T) {
		rt := builder.Build(t)
		epoch := periodOffset + 1
		rt.SetEpoch(epoch)
		actor.constructAndVerify(rt)
		cronCtrl := newCronControl(rt, actor)
		for i := 0; i < 3; i++ {
			epoch = cronCtrl.preCommitStartCronExpireStopCron(t, epoch) + 42
		}
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
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
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
		oneSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

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
		oneSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
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
		oneSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

		// consensus fault
		actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
			Target: actor.receiver,
			Epoch:  rt.Epoch() - 1,
			Type:   runtime.ConsensusFaultDoubleForkMining,
		})

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
		sectorInfo := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
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

	t.Run("updates many sectors", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		const sectorCount = 4000

		// commit a bunch of sectors to ensure that we get multiple partitions.
		sectorInfos := actor.commitAndProveSectors(rt, sectorCount, defaultSectorExpiration, nil, true)
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
			noEnrollment:              true,
			expiredSectorsPowerDelta:  &pwr,
			expiredSectorsPledgeDelta: newSector.InitialPledge.Neg(),
		})
		st = getState(rt)
		assert.False(t, st.DeadlineCronActive)
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
		sectorInfo := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
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
		daysBeforeUpgrade := 4
		// push expiration so we don't hit minimum lifetime limits when upgrading with the same expiration
		oldExpiration := defaultSectorExpiration + daysBeforeUpgrade
		oldSector := actor.commitAndProveSector(rt, 1, uint64(oldExpiration), nil)
		advanceAndSubmitPoSts(rt, actor, oldSector) // activate power
		st := getState(rt)
		dlIdx, partIdx, err := st.FindSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)

		// advance clock so upgrade happens later
		for i := 0; i < daysBeforeUpgrade; i++ { // 4 * 2880 = 11,520
			advanceAndSubmitPoSts(rt, actor, oldSector)
		}

		challengeEpoch := rt.Epoch() - 1
		upgradeParams := actor.makePreCommit(200, challengeEpoch, oldSector.Expiration, []abi.DealID{1})
		upgradeParams.ReplaceCapacity = true
		upgradeParams.ReplaceSectorDeadline = dlIdx
		upgradeParams.ReplaceSectorPartition = partIdx
		upgradeParams.ReplaceSectorNumber = oldSector.SectorNumber
		upgrade := actor.preCommitSector(rt, upgradeParams, preCommitConf{}, false)

		// Prove new sector
		rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
		newSector := actor.proveCommitSectorAndConfirm(rt, upgrade, makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})

		// Expect replace parameters have been set
		assert.Equal(t, oldSector.ExpectedDayReward, newSector.ReplacedDayReward)
		assert.Equal(t, rt.Epoch()-oldSector.Activation, newSector.ReplacedSectorAge)

		// advance to deadline of new and old sectors
		dlInfo := actor.currentDeadline(rt)
		for dlInfo.Index != dlIdx {
			dlInfo = advanceDeadline(rt, actor, &cronConfig{})
		}
		/**/
		// PoSt both sectors. They both gain power and no penalties are incurred.
		rt.SetEpoch(dlInfo.Last())
		oldPower := miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), miner.QAPowerForSector(actor.sectorSize, oldSector))
		newPower := miner.NewPowerPair(big.NewInt(int64(actor.sectorSize)), miner.QAPowerForSector(actor.sectorSize, newSector))
		partitions := []miner.PoStPartition{
			{Index: partIdx, Skipped: bitfield.New()},
		}
		actor.submitWindowPoSt(rt, dlInfo, partitions, []*miner.SectorOnChainInfo{newSector, oldSector}, &poStConfig{
			expectedPowerDelta: newPower,
		})

		// replaced sector expires at cron, removing its power and pledge.
		expectedPowerDelta := oldPower.Neg()
		actor.onDeadlineCron(rt, &cronConfig{
			expiredSectorsPowerDelta:  &expectedPowerDelta,
			expiredSectorsPledgeDelta: oldSector.InitialPledge.Neg(),
			expectedEnrollment:        rt.Epoch() + miner.WPoStChallengeWindow,
		})
		actor.checkState(rt)

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

	t.Run("cannot terminate a sector when the challenge window is open", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		rt.SetEpoch(abi.ChainEpoch(1))
		sectorInfo := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)
		sector := sectorInfo[0]

		st := getState(rt)
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sector.SectorNumber)
		require.NoError(t, err)

		// advance into the deadline, but not past it.
		dlinfo := actor.deadline(rt)
		for dlinfo.Index != dlIdx {
			dlinfo = advanceDeadline(rt, actor, &cronConfig{})
		}

		params := &miner.TerminateSectorsParams{Terminations: []miner.TerminationDeclaration{{
			Deadline:  dlIdx,
			Partition: pIdx,
			Sectors:   bf(uint64(sector.SectorNumber)),
		}}}
		rt.SetCaller(actor.worker, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(append(actor.controlAddrs, actor.owner, actor.worker)...)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "cannot terminate sectors in immutable deadline", func() {
			rt.Call(actor.a.TerminateSectors, params)
		})

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
		amountLocked, _ := miner.LockedRewardFromReward(rewardAmount)
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
		info := actor.commitAndProveSectors(rt, 4, defaultSectorExpiration, [][]abi.DealID{{10}, {20}, {30}, {40}}, true)

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

		// Wait WPoStProofChallengePeriod epochs so we can compact the sector.
		advanceToEpochWithCron(rt, actor, rt.Epoch()+miner.WPoStDisputeWindow)

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
		info := actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, [][]abi.DealID{{10}, {20}}, true)
		advanceAndSubmitPoSts(rt, actor, info...) // prove and activate power.

		// fault sector1
		actor.declareFaults(rt, info[0])

		// Wait WPoStProofChallengePeriod epochs so we can compact the sector.
		advanceToEpochWithCron(rt, actor, rt.Epoch()+miner.WPoStDisputeWindow)

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

		// Wait until deadline 0 (the one to which we'll assign the
		// sector) has elapsed. That'll let us commit, prove, then wait
		// finality epochs.
		st := getState(rt)
		deadlineEpoch := miner.NewDeadlineInfo(st.ProvingPeriodStart, 0, rt.Epoch()).NextNotElapsed().NextOpen()
		rt.SetEpoch(deadlineEpoch)

		// create 2 sectors in partition 0
		actor.commitAndProveSectors(rt, 2, defaultSectorExpiration, [][]abi.DealID{{10}, {20}}, true)

		// Wait WPoStProofChallengePeriod epochs so we can compact the sector.
		advanceToEpochWithCron(rt, actor, rt.Epoch()+miner.WPoStDisputeWindow)

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

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid deadline 48", func() {
			actor.compactPartitions(rt, miner.WPoStPeriodDeadlines, bitfield.New())
		})
		actor.checkState(rt)
	})

	t.Run("fails if deadline is open for challenging", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(periodOffset)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.compactPartitions(rt, 0, bitfield.New())
		})
		actor.checkState(rt)
	})

	t.Run("fails if deadline is next up to be challenged", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(periodOffset)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.compactPartitions(rt, 1, bitfield.New())
		})
		actor.checkState(rt)
	})

	t.Run("the deadline after the next deadline should still be open for compaction", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(periodOffset)
		actor.compactPartitions(rt, 3, bitfield.New())
		actor.checkState(rt)
	})

	t.Run("deadlines challenged last proving period should still be in the dispute window", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(periodOffset)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.compactPartitions(rt, 47, bitfield.New())
		})
		actor.checkState(rt)
	})

	disputeEnd := periodOffset + miner.WPoStChallengeWindow + miner.WPoStDisputeWindow - 1
	t.Run("compaction should be forbidden during the dispute window", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(disputeEnd)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.compactPartitions(rt, 0, bitfield.New())
		})
		actor.checkState(rt)
	})

	t.Run("compaction should be allowed following the dispute window", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(disputeEnd + 1)
		actor.compactPartitions(rt, 0, bitfield.New())
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

		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, [][]abi.DealID{{10}}, true)

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
		st := getState(rt)
		deadline := st.DeadlineInfo(rt.Epoch())
		rt.SetEpoch(deadline.PeriodEnd())

		info = actor.getInfo(rt)
		require.NotNil(t, info.PendingWorkerKey)
		require.EqualValues(t, actor.worker, info.Worker)

		// move to deadline containing effectiveEpoch
		rt.SetEpoch(effectiveEpoch)

		// enact worker change
		actor.confirmUpdateWorkerKey(rt)

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
		st := getState(rt)
		deadline := st.DeadlineInfo(rt.Epoch())
		rt.SetEpoch(deadline.PeriodEnd())

		// attempt to change address again
		actor.changeWorkerAddress(rt, newWorker2, rt.Epoch()+miner.WorkerKeyChangeDelay, originalControlAddrs)

		// assert change has not been modified
		info := actor.getInfo(rt)
		assert.Equal(t, info.PendingWorkerKey.NewWorker, newWorker1)
		assert.Equal(t, info.PendingWorkerKey.EffectiveAt, effectiveEpoch)

		rt.SetEpoch(effectiveEpoch)
		actor.confirmUpdateWorkerKey(rt)

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

		// set current epoch and update worker key
		rt.SetEpoch(effectiveEpoch)
		actor.confirmUpdateWorkerKey(rt)

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

	t.Run("invalid report rejected", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		rt.SetEpoch(abi.ChainEpoch(1))

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.reportConsensusFault(rt, addr.TestAddress, nil)
		})
		actor.checkState(rt)
	})

	t.Run("mis-targeted report rejected", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		rt.SetEpoch(abi.ChainEpoch(1))

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
				Target: tutil.NewIDAddr(t, 1234), // Not receiver
				Epoch:  rt.Epoch() - 1,
				Type:   runtime.ConsensusFaultDoubleForkMining,
			})
		})
		actor.checkState(rt)
	})

	t.Run("Report consensus fault pays reward and charges fee", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)

		actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
			Target: actor.receiver,
			Epoch:  rt.Epoch() - 1,
			Type:   runtime.ConsensusFaultDoubleForkMining,
		})
		actor.checkState(rt)
	})

	t.Run("Report consensus fault updates consensus fault reported field", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)

		startInfo := actor.getInfo(rt)
		assert.Equal(t, abi.ChainEpoch(-1), startInfo.ConsensusFaultElapsed)

		reportEpoch := abi.ChainEpoch(333)
		rt.SetEpoch(reportEpoch)

		actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
			Target: actor.receiver,
			Epoch:  rt.Epoch() - 1,
			Type:   runtime.ConsensusFaultDoubleForkMining,
		})
		endInfo := actor.getInfo(rt)
		assert.Equal(t, reportEpoch+miner.ConsensusFaultIneligibilityDuration, endInfo.ConsensusFaultElapsed)
		actor.checkState(rt)
	})

	t.Run("Double report of consensus fault fails", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)

		startInfo := actor.getInfo(rt)
		assert.Equal(t, abi.ChainEpoch(-1), startInfo.ConsensusFaultElapsed)

		reportEpoch := abi.ChainEpoch(333)
		rt.SetEpoch(reportEpoch)

		fault1 := rt.Epoch() - 1
		actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
			Target: actor.receiver,
			Epoch:  fault1,
			Type:   runtime.ConsensusFaultDoubleForkMining,
		})
		endInfo := actor.getInfo(rt)
		assert.Equal(t, reportEpoch+miner.ConsensusFaultIneligibilityDuration, endInfo.ConsensusFaultElapsed)

		// same fault can't be reported twice
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "too old", func() {
			actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
				Target: actor.receiver,
				Epoch:  fault1,
				Type:   runtime.ConsensusFaultDoubleForkMining,
			})
		})
		rt.Reset()

		// new consensus faults are forbidden until original has elapsed
		rt.SetEpoch(endInfo.ConsensusFaultElapsed)
		fault2 := endInfo.ConsensusFaultElapsed - 1
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "too old", func() {
			actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
				Target: actor.receiver,
				Epoch:  fault2,
				Type:   runtime.ConsensusFaultDoubleForkMining,
			})
		})
		rt.Reset()

		// a new consensus fault can be reported for blocks once original has expired
		rt.SetEpoch(endInfo.ConsensusFaultElapsed + 1)
		fault3 := endInfo.ConsensusFaultElapsed
		actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
			Target: actor.receiver,
			Epoch:  fault3,
			Type:   runtime.ConsensusFaultDoubleForkMining,
		})
		endInfo = actor.getInfo(rt)
		assert.Equal(t, rt.Epoch()+miner.ConsensusFaultIneligibilityDuration, endInfo.ConsensusFaultElapsed)

		// old fault still cannot be reported after fault interval has elapsed
		fault4 := fault1 + 1
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "too old", func() {
			actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
				Target: actor.receiver,
				Epoch:  fault4,
				Type:   runtime.ConsensusFaultDoubleForkMining,
			})
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
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rwd := abi.NewTokenAmount(1_000_000)
		actor.applyRewards(rt, rwd, big.Zero())

		expected := abi.NewTokenAmount(750_000)
		assert.Equal(t, expected, actor.getLockedFunds(rt))
	})

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

		st = getState(rt)
		lockedAmt, _ := miner.LockedRewardFromReward(amt)
		assert.Equal(t, lockedAmt, st.LockedFunds)
		// technically applying rewards without first activating cron is an impossible state but convenient for testing
		_, msgs := miner.CheckStateInvariants(st, rt.AdtStore(), rt.Balance())
		assert.Equal(t, 1, len(msgs.Messages()))
		assert.Contains(t, msgs.Messages()[0], "DeadlineCronActive == false")
	})

	t.Run("penalty is burnt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rwd := abi.NewTokenAmount(600_000)
		penalty := abi.NewTokenAmount(300_000)
		rt.SetBalance(big.Add(rt.Balance(), rwd))
		actor.applyRewards(rt, rwd, penalty)

		expectedLockAmt, _ := miner.LockedRewardFromReward(rwd)
		expectedLockAmt = big.Sub(expectedLockAmt, penalty)
		assert.Equal(t, expectedLockAmt, actor.getLockedFunds(rt))

		// technically applying rewards without first activating cron is an impossible state but convenient for testing
		st := getState(rt)
		_, msgs := miner.CheckStateInvariants(st, rt.AdtStore(), rt.Balance())
		assert.Equal(t, 1, len(msgs.Messages()))
		assert.Contains(t, msgs.Messages()[0], "DeadlineCronActive == false")
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
		// technically applying rewards without first activating cron is an impossible state but convenient for testing
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
		lockedReward, _ := miner.LockedRewardFromReward(reward)
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
		// technically applying rewards without first activating cron is an impossible state but convenient for testing
		_, msgs := miner.CheckStateInvariants(st, rt.AdtStore(), rt.Balance())
		assert.Equal(t, 1, len(msgs.Messages()))
		assert.Contains(t, msgs.Messages()[0], "DeadlineCronActive == false")
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
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

		targetSno := allSectors[0].SectorNumber
		actor.compactSectorNumbers(rt, bf(uint64(targetSno), uint64(targetSno)+1))

		precommitEpoch := rt.Epoch()
		deadline := actor.deadline(rt)
		expiration := deadline.PeriodEnd() + abi.ChainEpoch(defaultSectorExpiration)*miner.WPoStProvingPeriod

		// Allocating masked sector number should fail.
		{
			precommit := actor.makePreCommit(targetSno+1, precommitEpoch-1, expiration, nil)
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, precommit, preCommitConf{}, false)
			})
		}

		{
			precommit := actor.makePreCommit(targetSno+2, precommitEpoch-1, expiration, nil)
			actor.preCommitSector(rt, precommit, preCommitConf{}, false)
		}
		actor.checkState(rt)
	})

	t.Run("owner can also compact sectors", func(t *testing.T) {
		// Create a sector.
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

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
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

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
		allSectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

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

	t.Run("sector number range limits", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// Limits ok
		actor.compactSectorNumbers(rt, bf(0, abi.MaxSectorNumber))

		// Out of range fails
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.compactSectorNumbers(rt, bf(abi.MaxSectorNumber+1))
		})
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

	sealProofType       abi.RegisteredSealProof
	windowPostProofType abi.RegisteredPoStProof
	sectorSize          abi.SectorSize
	partitionSize       uint64
	periodOffset        abi.ChainEpoch
	nextSectorNo        abi.SectorNumber

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

		// Proof types and metadata initialized in setProofType
		periodOffset: provingPeriodOffset,
		nextSectorNo: 0,

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
	h.windowPostProofType, err = proof.RegisteredWindowPoStProof()
	require.NoError(h.t, err)
	h.sectorSize, err = proof.SectorSize()
	require.NoError(h.t, err)
	h.partitionSize, err = builtin.PoStProofWindowPoStPartitionSectors(h.windowPostProofType)
	require.NoError(h.t, err)
}

func (h *actorHarness) constructAndVerify(rt *mock.Runtime) {
	params := miner.ConstructorParams{
		OwnerAddr:           h.owner,
		WorkerAddr:          h.worker,
		ControlAddrs:        h.controlAddrs,
		WindowPoStProofType: h.windowPostProofType,
		PeerId:              testPid,
	}

	rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
	// Fetch worker pubkey.
	rt.ExpectSend(h.worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &h.key, exitcode.Ok)
	// Register proving period cron.
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
	return st.RecordedDeadlineInfo(rt.Epoch())
}

func (h *actorHarness) currentDeadline(rt *mock.Runtime) *dline.Info {
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

func (h *actorHarness) getPartitionSnapshot(rt *mock.Runtime, deadline *miner.Deadline, idx uint64) *miner.Partition {
	partition, err := deadline.LoadPartitionSnapshot(rt.AdtStore(), idx)
	require.NoError(h.t, err)
	return partition
}

func (h *actorHarness) getSubmittedProof(rt *mock.Runtime, deadline *miner.Deadline, idx uint64) *miner.WindowedPoSt {
	proofs, err := adt.AsArray(rt.AdtStore(), deadline.OptimisticPoStSubmissionsSnapshot, miner.DeadlineOptimisticPoStSubmissionsAmtBitwidth)
	require.NoError(h.t, err)
	var post miner.WindowedPoSt
	found, err := proofs.Get(idx, &post)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return &post
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
// nolint:unused
func (h *actorHarness) collectSectors(rt *mock.Runtime) map[abi.SectorNumber]*miner.SectorOnChainInfo {
	sectors := map[abi.SectorNumber]*miner.SectorOnChainInfo{}
	st := getState(rt)
	_ = st.ForEachSector(rt.AdtStore(), func(info *miner.SectorOnChainInfo) {
		sector := *info
		sectors[info.SectorNumber] = &sector
	})
	return sectors
}

func (h *actorHarness) collectPrecommitExpirations(rt *mock.Runtime, st *miner.State) map[abi.ChainEpoch][]uint64 {
	queue, err := miner.LoadBitfieldQueue(rt.AdtStore(), st.PreCommittedSectorsCleanUp, miner.NoQuantization, miner.PrecommitCleanUpAmtBitwidth)
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

func (h *actorHarness) collectDeadlineExpirations(rt *mock.Runtime, deadline *miner.Deadline) map[abi.ChainEpoch][]uint64 {
	queue, err := miner.LoadBitfieldQueue(rt.AdtStore(), deadline.ExpirationsEpochs, miner.NoQuantization, miner.DeadlineExpirationAmtBitwidth)
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
	queue, err := miner.LoadExpirationQueue(rt.AdtStore(), partition.ExpirationsEpochs, miner.NoQuantization, miner.PartitionExpirationAmtBitwidth)
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
}

func (h *actorHarness) preCommitSector(rt *mock.Runtime, params *miner.PreCommitSectorParams, conf preCommitConf, first bool) *miner.SectorPreCommitOnChainInfo {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	{
		expectQueryNetworkInfo(rt, h)
	}
	if len(params.DealIDs) > 0 {
		vdParams := market.VerifyDealsForActivationParams{
			Sectors: []market.SectorDeals{{
				SectorExpiry: params.Expiration,
				DealIDs:      params.DealIDs,
			}},
		}

		if conf.dealWeight.Nil() {
			conf.dealWeight = big.Zero()
		}
		if conf.verifiedDealWeight.Nil() {
			conf.verifiedDealWeight = big.Zero()
		}
		vdReturn := market.VerifyDealsForActivationReturn{
			Sectors: []market.SectorWeights{{
				DealSpace:          uint64(conf.dealSpace),
				DealWeight:         conf.dealWeight,
				VerifiedDealWeight: conf.verifiedDealWeight,
			}},
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.VerifyDealsForActivation, &vdParams, big.Zero(), &vdReturn, exitcode.Ok)
	} else {
		// Ensure the deal IDs and configured deal weight returns are consistent.
		require.Equal(h.t, abi.SectorSize(0), conf.dealSpace, "no deals but positive deal space configured")
		require.True(h.t, conf.dealWeight.NilOrZero(), "no deals but positive deal weight configured")
		require.True(h.t, conf.verifiedDealWeight.NilOrZero(), "no deals but positive deal weight configured")
	}
	st := getState(rt)
	if st.FeeDebt.GreaterThan(big.Zero()) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, st.FeeDebt, nil, exitcode.Ok)
	}

	if first {
		dlInfo := miner.NewDeadlineInfoFromOffsetAndEpoch(st.ProvingPeriodStart, rt.Epoch())
		cronParams := makeDeadlineCronEventParams(h.t, dlInfo.Last())
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent, cronParams, big.Zero(), nil, exitcode.Ok)
	}

	rt.Call(h.a.PreCommitSector, params)
	rt.Verify()
	return h.getPreCommit(rt, params.SectorNumber)
}

type preCommitBatchConf struct {
	// Weights to be returned from the market actor for sectors 0..len(sectorWeights).
	// Any remaining sectors are taken to have zero deal weight.
	sectorWeights []market.SectorWeights
	// Set if this is the first commitment by this miner, hence should expect scheduling end-of-deadline cron.
	firstForMiner bool
}

func (h *actorHarness) preCommitSectorBatch(rt *mock.Runtime, params *miner.PreCommitSectorBatchParams, conf preCommitBatchConf) []*miner.SectorPreCommitOnChainInfo {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)
	{
		expectQueryNetworkInfo(rt, h)
	}
	sectorDeals := make([]market.SectorDeals, len(params.Sectors))
	sectorWeights := make([]market.SectorWeights, len(params.Sectors))
	anyDeals := false
	for i, sector := range params.Sectors {
		sectorDeals[i] = market.SectorDeals{
			SectorExpiry: sector.Expiration,
			DealIDs:      sector.DealIDs,
		}

		if len(conf.sectorWeights) > i {
			sectorWeights[i] = conf.sectorWeights[i]
		} else {
			sectorWeights[i] = market.SectorWeights{
				DealSpace:          0,
				DealWeight:         big.Zero(),
				VerifiedDealWeight: big.Zero(),
			}
		}
		// Sanity check on expectations
		sectorHasDeals := len(sector.DealIDs) > 0
		dealTotalWeight := big.Add(sectorWeights[i].DealWeight, sectorWeights[i].VerifiedDealWeight)
		require.True(h.t, sectorHasDeals == !dealTotalWeight.Equals(big.Zero()), "sector deals inconsistent with configured weight")
		require.True(h.t, sectorHasDeals == (sectorWeights[i].DealSpace != 0), "sector deals inconsistent with configured space")
		anyDeals = anyDeals || sectorHasDeals
	}
	if anyDeals {
		vdParams := market.VerifyDealsForActivationParams{
			Sectors: sectorDeals,
		}
		vdReturn := market.VerifyDealsForActivationReturn{
			Sectors: sectorWeights,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.VerifyDealsForActivation, &vdParams, big.Zero(), &vdReturn, exitcode.Ok)
	}
	st := getState(rt)
	if st.FeeDebt.GreaterThan(big.Zero()) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, st.FeeDebt, nil, exitcode.Ok)
	}

	if conf.firstForMiner {
		dlInfo := miner.NewDeadlineInfoFromOffsetAndEpoch(st.ProvingPeriodStart, rt.Epoch())
		cronParams := makeDeadlineCronEventParams(h.t, dlInfo.Last())
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent, cronParams, big.Zero(), nil, exitcode.Ok)
	}

	rt.Call(h.a.PreCommitSectorBatch, params)
	rt.Verify()
	precommits := make([]*miner.SectorPreCommitOnChainInfo, len(params.Sectors))
	for i, sector := range params.Sectors {
		precommits[i] = h.getPreCommit(rt, sector.SectorNumber)
	}
	return precommits
}

// Options for proveCommitSector behaviour.
// Default zero values should let everything be ok.
type proveCommitConf struct {
	verifyDealsExit map[abi.SectorNumber]exitcode.ExitCode
}

func (h *actorHarness) proveCommitSector(rt *mock.Runtime, precommit *miner.SectorPreCommitOnChainInfo, params *miner.ProveCommitSectorParams) {
	commd := cbg.CborCid(tutil.MakeCID("commd", &market.PieceCIDPrefix))
	sealRand := abi.SealRandomness([]byte{1, 2, 3, 4})
	sealIntRand := abi.InteractiveSealRandomness([]byte{5, 6, 7, 8})
	interactiveEpoch := precommit.PreCommitEpoch + miner.PreCommitChallengeDelay

	// Prepare for and receive call to ProveCommitSector
	{
		inputs := []*market.SectorDataSpec{
			{
				DealIDs:    precommit.Info.DealIDs,
				SectorType: precommit.Info.SealProof,
			},
		}
		cdcParams := market.ComputeDataCommitmentParams{Inputs: inputs}
		cdcRet := market.ComputeDataCommitmentReturn{
			CommDs: []cbg.CborCid{commd},
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ComputeDataCommitment, &cdcParams, big.Zero(), &cdcRet, exitcode.Ok)
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

func (h *actorHarness) proveCommitAggregateSector(rt *mock.Runtime, conf proveCommitConf, precommits []*miner.SectorPreCommitOnChainInfo, params *miner.ProveCommitAggregateParams) {
	// Receive call to ComputeDataCommittments
	commDs := make([]cbg.CborCid, len(precommits))
	{
		cdcInputs := make([]*market.SectorDataSpec, len(precommits))
		for i, precommit := range precommits {
			cdcInputs[i] = &market.SectorDataSpec{
				DealIDs:    precommit.Info.DealIDs,
				SectorType: precommit.Info.SealProof,
			}
			commD := cbg.CborCid(tutil.MakeCID(fmt.Sprintf("commd-%d", i), &market.PieceCIDPrefix))
			commDs[i] = commD
		}
		cdcParams := market.ComputeDataCommitmentParams{Inputs: cdcInputs}
		cdcRet := market.ComputeDataCommitmentReturn{
			CommDs: commDs,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ComputeDataCommitment, &cdcParams, big.Zero(), &cdcRet, exitcode.Ok)
	}
	// Expect randomness queries for provided precommits
	var sealRands []abi.SealRandomness
	var sealIntRands []abi.InteractiveSealRandomness
	{

		for _, precommit := range precommits {
			sealRand := abi.SealRandomness([]byte{1, 2, 3, 4})
			sealRands = append(sealRands, sealRand)
			sealIntRand := abi.InteractiveSealRandomness([]byte{5, 6, 7, 8})
			sealIntRands = append(sealIntRands, sealIntRand)
			interactiveEpoch := precommit.PreCommitEpoch + miner.PreCommitChallengeDelay
			var buf bytes.Buffer
			receiver := rt.Receiver()
			err := receiver.MarshalCBOR(&buf)
			require.NoError(h.t, err)
			rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_SealRandomness, precommit.Info.SealRandEpoch, buf.Bytes(), abi.Randomness(sealRand))
			rt.ExpectGetRandomnessBeacon(crypto.DomainSeparationTag_InteractiveSealChallengeSeed, interactiveEpoch, buf.Bytes(), abi.Randomness(sealIntRand))
		}
	}
	// Verify syscall
	{
		svis := make([]proof.AggregateSealVerifyInfo, len(precommits))
		for i, precommit := range precommits {
			svis[i] = proof.AggregateSealVerifyInfo{
				Number:                precommit.Info.SectorNumber,
				InteractiveRandomness: sealIntRands[i],
				Randomness:            sealRands[i],
				SealedCID:             precommit.Info.SealedCID,
				UnsealedCID:           cid.Cid(commDs[i]),
			}
		}
		actorId, err := addr.IDFromAddress(h.receiver)
		require.NoError(h.t, err)
		rt.ExpectAggregateVerifySeals(proof.AggregateSealVerifyProofAndInfos{
			Infos:          svis,
			Proof:          params.AggregateProof,
			Miner:          abi.ActorID(actorId),
			SealProof:      h.sealProofType,
			AggregateProof: abi.RegisteredAggregationProof_SnarkPackV1,
		}, nil)
	}

	// confirmSectorProofsValid
	{
		h.confirmSectorProofsValidInternal(rt, conf, precommits...)
	}

	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAny()
	rt.Call(h.a.ProveCommitAggregate, params)
	rt.Verify()
}

func (h *actorHarness) confirmSectorProofsValidInternal(rt *mock.Runtime, conf proveCommitConf, precommits ...*miner.SectorPreCommitOnChainInfo) {
	// expect calls to get network stats
	expectQueryNetworkInfo(rt, h)

	// Prepare for and receive call to ConfirmSectorProofsValid.
	var validPrecommits []*miner.SectorPreCommitOnChainInfo
	for _, precommit := range precommits {
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

		if !expectPledge.IsZero() {
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &expectPledge, big.Zero(), nil, exitcode.Ok)
		}
	}
}

func (h *actorHarness) confirmSectorProofsValid(rt *mock.Runtime, conf proveCommitConf, precommits ...*miner.SectorPreCommitOnChainInfo) {
	h.confirmSectorProofsValidInternal(rt, conf, precommits...)
	var allSectorNumbers []abi.SectorNumber
	for _, precommit := range precommits {
		allSectorNumbers = append(allSectorNumbers, precommit.Info.SectorNumber)
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
func (h *actorHarness) commitAndProveSectors(rt *mock.Runtime, n int, lifetimePeriods uint64, dealIDs [][]abi.DealID, first bool) []*miner.SectorOnChainInfo {
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
		precommit := h.preCommitSector(rt, params, preCommitConf{}, first && i == 0)
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
	precommit := h.preCommitSector(rt, preCommitParams, preCommitConf{}, true)

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
	}, false)

	// Prove new sector
	rt.SetEpoch(upgrade.PreCommitEpoch + miner.PreCommitChallengeDelay + 1)
	newSector = h.proveCommitSectorAndConfirm(rt, upgrade, makeProveCommit(upgrade.Info.SectorNumber), proveCommitConf{})

	return oldSector, newSector
}

// Deprecated
// nolint:unused
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

// nolint:unused
func (h *actorHarness) advancePastDeadlineEndWithCron(rt *mock.Runtime) {
	deadline := h.deadline(rt)
	rt.SetEpoch(deadline.PeriodEnd())
	nextCron := deadline.Last() + miner.WPoStChallengeWindow
	h.onDeadlineCron(rt, &cronConfig{
		expectedEnrollment: nextCron,
	})
	rt.SetEpoch(deadline.NextPeriodStart())
}

type poStDisputeResult struct {
	expectedPowerDelta  miner.PowerPair
	expectedPledgeDelta abi.TokenAmount
	expectedPenalty     abi.TokenAmount
	expectedReward      abi.TokenAmount
}

func (h *actorHarness) disputeWindowPoSt(rt *mock.Runtime, deadline *dline.Info, proofIndex uint64, infos []*miner.SectorOnChainInfo, expectSuccess *poStDisputeResult) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

	expectQueryNetworkInfo(rt, h)
	challengeRand := abi.SealRandomness([]byte{10, 11, 12, 13})

	// only sectors that are not skipped and not existing non-recovered faults will be verified
	allIgnored := bf()
	dln := h.getDeadline(rt, deadline.Index)

	post := h.getSubmittedProof(rt, dln, proofIndex)

	var err error
	err = post.Partitions.ForEach(func(idx uint64) error {
		partition := h.getPartitionSnapshot(rt, dln, idx)
		allIgnored, err = bitfield.MergeBitFields(allIgnored, partition.Faults)
		require.NoError(h.t, err)
		noRecoveries, err := partition.Recoveries.IsEmpty()
		require.NoError(h.t, err)
		require.True(h.t, noRecoveries)
		return nil
	})
	require.NoError(h.t, err)

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
	require.NotNil(h.t, goodInfo, "stored proof should prove at least one sector")

	var buf bytes.Buffer
	receiver := rt.Receiver()
	err = receiver.MarshalCBOR(&buf)
	require.NoError(h.t, err)

	rt.ExpectGetRandomnessBeacon(crypto.DomainSeparationTag_WindowedPoStChallengeSeed, deadline.Challenge, buf.Bytes(), abi.Randomness(challengeRand))

	actorId, err := addr.IDFromAddress(h.receiver)
	require.NoError(h.t, err)

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
		Proofs:            post.Proofs,
		ChallengedSectors: proofInfos,
		Prover:            abi.ActorID(actorId),
	}
	var verifResult error
	if expectSuccess != nil {
		// if we succeed at challenging, proof verification needs to fail.
		verifResult = fmt.Errorf("invalid post")
	}
	rt.ExpectVerifyPoSt(vi, verifResult)

	if expectSuccess != nil {
		// expect power update
		if !expectSuccess.expectedPowerDelta.IsZero() {
			claim := &power.UpdateClaimedPowerParams{
				RawByteDelta:         expectSuccess.expectedPowerDelta.Raw,
				QualityAdjustedDelta: expectSuccess.expectedPowerDelta.QA,
			}
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, claim, abi.NewTokenAmount(0),
				nil, exitcode.Ok)
		}
		// expect reward
		if !expectSuccess.expectedReward.IsZero() {
			rt.ExpectSend(h.worker, builtin.MethodSend, nil, expectSuccess.expectedReward, nil, exitcode.Ok)
		}
		// expect penalty
		if !expectSuccess.expectedPenalty.IsZero() {
			rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectSuccess.expectedPenalty, nil, exitcode.Ok)
		}
		// expect pledge update
		if !expectSuccess.expectedPledgeDelta.IsZero() {
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal,
				&expectSuccess.expectedPledgeDelta, abi.NewTokenAmount(0), nil, exitcode.Ok)
		}
	}

	params := miner.DisputeWindowedPoStParams{
		Deadline:  deadline.Index,
		PoStIndex: proofIndex,
	}
	if expectSuccess == nil {
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "failed to dispute valid post", func() {
			rt.Call(h.a.DisputeWindowedPoSt, &params)
		})
	} else {
		rt.Call(h.a.DisputeWindowedPoSt, &params)
	}
	rt.Verify()
}

type poStConfig struct {
	chainRandomness    abi.Randomness
	expectedPowerDelta miner.PowerPair
	verificationError  error
}

func (h *actorHarness) submitWindowPoSt(rt *mock.Runtime, deadline *dline.Info, partitions []miner.PoStPartition, infos []*miner.SectorOnChainInfo, poStCfg *poStConfig) {
	params := miner.SubmitWindowedPoStParams{
		Deadline:         deadline.Index,
		Partitions:       partitions,
		Proofs:           makePoStProofs(h.windowPostProofType),
		ChainCommitEpoch: deadline.Challenge,
		ChainCommitRand:  abi.Randomness("chaincommitment"),
	}
	h.submitWindowPoStRaw(rt, deadline, infos, &params, poStCfg)
}

func (h *actorHarness) submitWindowPoStRaw(rt *mock.Runtime, deadline *dline.Info,
	infos []*miner.SectorOnChainInfo, params *miner.SubmitWindowedPoStParams, poStCfg *poStConfig) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	chainCommitRand := params.ChainCommitRand
	if poStCfg != nil && len(poStCfg.chainRandomness) > 0 {
		chainCommitRand = poStCfg.chainRandomness
	}
	rt.ExpectGetRandomnessTickets(crypto.DomainSeparationTag_PoStChainCommit, params.ChainCommitEpoch, nil, chainCommitRand)

	rt.ExpectValidateCallerAddr(append(h.controlAddrs, h.owner, h.worker)...)

	challengeRand := abi.SealRandomness([]byte{10, 11, 12, 13})

	// only sectors that are not skipped and not existing non-recovered faults will be verified
	allIgnored := bf()
	allRecovered := bf()
	dln := h.getDeadline(rt, deadline.Index)
	for _, p := range params.Partitions {
		if partition, err := dln.LoadPartition(rt.AdtStore(), p.Index); err == nil {
			expectedFaults, err := bitfield.SubtractBitField(partition.Faults, partition.Recoveries)
			require.NoError(h.t, err)
			allIgnored, err = bitfield.MultiMerge(allIgnored, expectedFaults, p.Skipped)
			require.NoError(h.t, err)
			recovered, err := bitfield.SubtractBitField(partition.Recoveries, p.Skipped)
			require.NoError(h.t, err)
			allRecovered, err = bitfield.MergeBitFields(allRecovered, recovered)
			require.NoError(h.t, err)
		}
	}
	optimistic, err := allRecovered.IsEmpty()
	require.NoError(h.t, err)

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
	if !optimistic && goodInfo != nil {
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
			Proofs:            params.Proofs,
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
		if !poStCfg.expectedPowerDelta.Raw.NilOrZero() || !poStCfg.expectedPowerDelta.QA.NilOrZero() {
			claim := &power.UpdateClaimedPowerParams{
				RawByteDelta:         poStCfg.expectedPowerDelta.Raw,
				QualityAdjustedDelta: poStCfg.expectedPowerDelta.QA,
			}
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, claim, abi.NewTokenAmount(0),
				nil, exitcode.Ok)
		}
	}

	rt.Call(h.a.SubmitWindowedPoSt, params)
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

func (h *actorHarness) reportConsensusFault(rt *mock.Runtime, from addr.Address, fault *runtime.ConsensusFault) {
	rt.SetCaller(from, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
	params := &miner.ReportConsensusFaultParams{
		BlockHeader1:     nil,
		BlockHeader2:     nil,
		BlockHeaderExtra: nil,
	}

	if fault != nil {
		rt.ExpectVerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra, fault, nil)
	} else {
		rt.ExpectVerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra, nil, fmt.Errorf("no fault"))
	}

	currentReward := reward.ThisEpochRewardReturn{
		ThisEpochBaselinePower:  h.baselinePower,
		ThisEpochRewardSmoothed: h.epochRewardSmooth,
	}
	rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.ThisEpochReward, nil, big.Zero(), &currentReward, exitcode.Ok)

	thisEpochReward := h.epochRewardSmooth.Estimate()
	penaltyTotal := miner.ConsensusFaultPenalty(thisEpochReward)
	rewardTotal := miner.RewardForConsensusSlashReport(thisEpochReward)
	rt.ExpectSend(from, builtin.MethodSend, nil, rewardTotal, nil, exitcode.Ok)

	// pay fault fee
	toBurn := big.Sub(penaltyTotal, rewardTotal)
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
	lockAmt, _ := miner.LockedRewardFromReward(amt)
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
	noEnrollment              bool // true if expect not to continue enrollment false otherwise
	expectedEnrollment        abi.ChainEpoch
	detectedFaultsPowerDelta  *miner.PowerPair
	expiredSectorsPowerDelta  *miner.PowerPair
	expiredSectorsPledgeDelta abi.TokenAmount
	continuedFaultsPenalty    abi.TokenAmount // Expected amount burnt to pay continued fault penalties.
	expiredPrecommitPenalty   abi.TokenAmount // Expected amount burnt to pay for expired precommits
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
	if !config.expiredPrecommitPenalty.NilOrZero() {
		penaltyTotal = big.Add(penaltyTotal, config.expiredPrecommitPenalty)
	}
	if !penaltyTotal.IsZero() {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penaltyTotal, nil, exitcode.Ok)
		penaltyFromVesting := penaltyTotal
		// Outstanding fee debt is only repaid from unlocked balance, not vesting funds.
		if !config.repaidFeeDebt.NilOrZero() {
			penaltyFromVesting = big.Sub(penaltyFromVesting, config.repaidFeeDebt)
		}
		// Precommit deposit burns are repaid from PCD account
		if !config.expiredPrecommitPenalty.NilOrZero() {
			penaltyFromVesting = big.Sub(penaltyFromVesting, config.expiredPrecommitPenalty)
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
	if !config.noEnrollment {
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			makeDeadlineCronEventParams(h.t, config.expectedEnrollment), big.Zero(), nil, exitcode.Ok)
	}

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

func (h *actorHarness) makePreCommit(sectorNo abi.SectorNumber, challenge, expiration abi.ChainEpoch, dealIDs []abi.DealID) *miner0.SectorPreCommitInfo {
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

// Completes a deadline by moving the epoch forward to the penultimate one,
// if cron is active calling the deadline cron handler,
// and then advancing to the first epoch in the new deadline.
// If cron is run asserts that the deadline schedules a new cron on the next deadline
func advanceDeadline(rt *mock.Runtime, h *actorHarness, config *cronConfig) *dline.Info {
	st := getState(rt)
	deadline := miner.NewDeadlineInfoFromOffsetAndEpoch(st.ProvingPeriodStart, rt.Epoch())

	if st.DeadlineCronActive {
		rt.SetEpoch(deadline.Last())

		config.expectedEnrollment = deadline.Last() + miner.WPoStChallengeWindow
		h.onDeadlineCron(rt, config)
	}
	rt.SetEpoch(deadline.NextOpen())
	st = getState(rt)

	return st.DeadlineInfo(rt.Epoch())
}

// Advances one deadline at a time until the opening  of the next instance of a deadline index.
func advanceToDeadline(rt *mock.Runtime, h *actorHarness, dlIdx uint64) *dline.Info {
	dlinfo := h.deadline(rt)
	for dlinfo.Index != dlIdx {
		dlinfo = advanceDeadline(rt, h, &cronConfig{})
	}
	return dlinfo
}

func advanceToEpochWithCron(rt *mock.Runtime, h *actorHarness, e abi.ChainEpoch) {
	deadline := h.deadline(rt)
	for e > deadline.Last() {
		advanceDeadline(rt, h, &cronConfig{})
		deadline = h.deadline(rt)
	}
	rt.SetEpoch(e)
}

// Advance between 0 and 48 deadlines submitting window posts where necessary to keep
// sectors proven.  If sectors is empty this is a noop. If sectors is a singleton this
// will advance to that sector's proving deadline running deadline crons up to and
// including this deadline. If sectors includes a sector assigned to the furthest
// away deadline this will process a whole proving period.
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

	dlinfo := h.currentDeadline(rt)
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
		dlinfo = h.currentDeadline(rt)
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

func builderForHarness(actor *actorHarness) mock.RuntimeBuilder {
	rb := mock.NewBuilder(actor.receiver).
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

	params := &power.EnrollCronEventParams{
		EventEpoch: epoch,
		Payload:    buf.Bytes(),
	}
	return params
}

func makeProveCommit(sectorNo abi.SectorNumber) *miner.ProveCommitSectorParams {
	return &miner.ProveCommitSectorParams{
		SectorNumber: sectorNo,
		Proof:        make([]byte, 192),
	}
}

func makeProveCommitAggregate(sectorNos bitfield.BitField) *miner.ProveCommitAggregateParams {
	return &miner.ProveCommitAggregateParams{
		SectorNumbers:  sectorNos,
		AggregateProof: make([]byte, 1024),
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
