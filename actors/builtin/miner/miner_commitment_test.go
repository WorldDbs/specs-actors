package miner_test

import (
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v5/actors/runtime"
	"github.com/filecoin-project/specs-actors/v5/actors/util/smoothing"
	"github.com/filecoin-project/specs-actors/v5/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v5/support/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test for sector precommitment and proving.
func TestCommitments(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)

	// Simple pre-commits
	for _, test := range []struct {
		name             string
		sectorNo         abi.SectorNumber
		dealSize         uint64
		verifiedDealSize uint64
		dealIds          []abi.DealID
	}{{
		name:             "no deals",
		sectorNo:         0,
		dealSize:         0,
		verifiedDealSize: 0,
		dealIds:          nil,
	}, {
		name:             "max sector number",
		sectorNo:         abi.MaxSectorNumber,
		dealSize:         0,
		verifiedDealSize: 0,
		dealIds:          nil,
	}, {
		name:             "unverified deal",
		sectorNo:         100,
		dealSize:         32 << 30,
		verifiedDealSize: 0,
		dealIds:          []abi.DealID{1},
	}, {
		name:             "verified deal",
		sectorNo:         100,
		dealSize:         0,
		verifiedDealSize: 32 << 30,
		dealIds:          []abi.DealID{1},
	}, {
		name:             "two deals",
		sectorNo:         100,
		dealSize:         16 << 30,
		verifiedDealSize: 16 << 30,
		dealIds:          []abi.DealID{1, 2},
	},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor := newHarness(t, periodOffset)
			rt := builderForHarness(actor).
				WithBalance(bigBalance, big.Zero()).
				Build(t)
			precommitEpoch := periodOffset + 1
			rt.SetEpoch(precommitEpoch)
			actor.constructAndVerify(rt)
			dlInfo := actor.deadline(rt)

			expiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // on deadline boundary but > 180 days
			proveCommitEpoch := precommitEpoch + miner.PreCommitChallengeDelay + 1
			dealLifespan := expiration - proveCommitEpoch
			dealSpace := test.dealSize + test.verifiedDealSize
			dealWeight := big.Mul(big.NewIntUnsigned(test.dealSize), big.NewInt(int64(dealLifespan)))
			verifiedDealWeight := big.Mul(big.NewIntUnsigned(test.verifiedDealSize), big.NewInt(int64(dealLifespan)))

			precommitParams := actor.makePreCommit(test.sectorNo, precommitEpoch-1, expiration, test.dealIds)
			precommit := actor.preCommitSector(rt, precommitParams, preCommitConf{
				dealWeight:         dealWeight,
				verifiedDealWeight: verifiedDealWeight,
				dealSpace:          abi.SectorSize(dealSpace),
			}, true)

			// Check precommit expectations.
			assert.Equal(t, precommitEpoch, precommit.PreCommitEpoch)
			assert.Equal(t, dealWeight, precommit.DealWeight)
			assert.Equal(t, verifiedDealWeight, precommit.VerifiedDealWeight)

			assert.Equal(t, test.sectorNo, precommit.Info.SectorNumber)
			assert.Equal(t, precommitParams.SealProof, precommit.Info.SealProof)
			assert.Equal(t, precommitParams.SealedCID, precommit.Info.SealedCID)
			assert.Equal(t, precommitParams.SealRandEpoch, precommit.Info.SealRandEpoch)
			assert.Equal(t, precommitParams.DealIDs, precommit.Info.DealIDs)
			assert.Equal(t, precommitParams.Expiration, precommit.Info.Expiration)

			pwrEstimate := miner.QAPowerForWeight(actor.sectorSize, precommit.Info.Expiration-precommitEpoch, dealWeight, verifiedDealWeight)
			expectedDeposit := miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwrEstimate)
			assert.Equal(t, expectedDeposit, precommit.PreCommitDeposit)

			st := getState(rt)
			assert.True(t, expectedDeposit.GreaterThan(big.Zero()))
			assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

			expirations := actor.collectPrecommitExpirations(rt, st)
			expectedPrecommitExpiration := st.QuantSpecEveryDeadline().QuantizeUp(precommitEpoch + miner.MaxProveCommitDuration[actor.sealProofType] + miner.ExpiredPreCommitCleanUpDelay)
			assert.Equal(t, map[abi.ChainEpoch][]uint64{expectedPrecommitExpiration: {uint64(test.sectorNo)}}, expirations)
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
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, true)
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
			}, true)
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

		actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, true)
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

		oldSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)[0]
		st := getState(rt)
		assert.True(t, st.DeadlineCronActive)
		// Good commitment.
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
		actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, false)
		// Duplicate pre-commit sector ID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Sector ID already committed
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(oldSector.SectorNumber, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Bad sealed CID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "sealed CID had wrong prefix", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealedCID = tutil.MakeCID("Random Data", nil)
			actor.preCommitSector(rt, pc, preCommitConf{}, false)
		})
		rt.Reset()

		// Bad seal proof type
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "unsupported seal proof type", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg8MiBV1_1
			actor.preCommitSector(rt, pc, preCommitConf{}, false)
		})
		rt.Reset()

		// Expires at current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after activation", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch(), nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Expires before current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after activation", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch()-1, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Expires too early
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration-20*builtin.EpochsInDay, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Expires before min duration + max seal duration
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			expiration := rt.Epoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[actor.sealProofType] - 1
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Errors when expiry too far in the future
		rt.SetEpoch(precommitEpoch)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid expiration", func() {
			expiration := deadline.PeriodEnd() + miner.WPoStProvingPeriod*(miner.MaxSectorExpirationExtension/miner.WPoStProvingPeriod+1)
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Sector ID out of range
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "out of range", func() {
			actor.preCommitSector(rt, actor.makePreCommit(abi.MaxSectorNumber+1, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Seal randomness challenge too far in past
		tooOldChallengeEpoch := precommitEpoch - miner.ChainFinality - miner.MaxProveCommitDuration[actor.sealProofType] - 1
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too old", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, tooOldChallengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Deals too large for sector
		dealWeight := big.Mul(big.NewIntUnsigned(32<<30), big.NewInt(int64(expiration-rt.Epoch())))
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "deals too large", func() {
			actor.preCommitSector(rt, actor.makePreCommit(0, challengeEpoch, expiration, []abi.DealID{1}), preCommitConf{
				dealWeight:         dealWeight,
				verifiedDealWeight: big.Zero(),
				dealSpace:          32<<30 + 1,
			}, false)
		})
		rt.Reset()

		// Try to precommit while in fee debt with insufficient balance
		st = getState(rt)
		st.FeeDebt = big.Add(rt.Balance(), abi.NewTokenAmount(1e18))
		rt.ReplaceState(st)
		rt.ExpectAbortContainsMessage(exitcode.ErrInsufficientFunds, "unlocked balance can not repay fee debt", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		// reset state back to normal
		st.FeeDebt = big.Zero()
		rt.ReplaceState(st)
		rt.Reset()

		// Try to precommit with an active consensus fault
		st = getState(rt)
		actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
			Target: actor.receiver,
			Epoch:  rt.Epoch() - 1,
			Type:   runtime.ConsensusFaultDoubleForkMining,
		})
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "active consensus fault", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		// reset state back to normal
		rt.ReplaceState(st)
		rt.Reset()
		actor.checkState(rt)
	})

	t.Run("fails with too many deals", func(t *testing.T) {
		// Remove this nasty static/global access when policy is encapsulated in a structure.
		// See https://github.com/filecoin-project/specs-actors/issues/353.
		miner.WindowPoStProofTypes[abi.RegisteredPoStProof_StackedDrgWindow2KiBV1] = struct{}{}
		defer func() {
			delete(miner.WindowPoStProofTypes, abi.RegisteredPoStProof_StackedDrgWindow2KiBV1)
		}()

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

		sectorNo := abi.SectorNumber(100)
		dealLimits := map[abi.RegisteredSealProof]int{
			abi.RegisteredSealProof_StackedDrg2KiBV1_1:  256,
			abi.RegisteredSealProof_StackedDrg32GiBV1_1: 256,
			abi.RegisteredSealProof_StackedDrg64GiBV1_1: 512,
		}

		for proof, limit := range dealLimits {
			// attempt to pre-commmit a sector with too many deals
			rt, actor, deadline := setup(proof)
			expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
			precommit := actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit+1))
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too many deals for sector", func() {
				actor.preCommitSector(rt, precommit, preCommitConf{}, true)
			})

			// sector at or below limit succeeds
			rt, actor, _ = setup(proof)
			precommit = actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit))
			actor.preCommitSector(rt, precommit, preCommitConf{}, true)
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
		{
			// After version 7, only V1_1 accepted
			rt.SetNetworkVersion(network.Version8)
			pc := actor.makePreCommit(104, challengeEpoch, expiration, nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, pc, preCommitConf{}, true)
			})
			rt.Reset()
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1
			actor.preCommitSector(rt, pc, preCommitConf{}, true)
		}

		actor.checkState(rt)
	})

	t.Run("precommit does not vest funds", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
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
		// The below call expects no pledge delta.
		actor.preCommitSector(rt, precommitParams, preCommitConf{}, true)
	})
}

func TestPreCommitBatch(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	type dealSpec struct {
		size         uint64
		verifiedSize uint64
		IDs          []abi.DealID
	}

	// Simple batches
	for _, test := range []struct {
		name           string
		batchSize      int
		balanceSurplus abi.TokenAmount
		deals          []dealSpec
		exit           exitcode.ExitCode
		error          string
	}{{
		name:           "one sector",
		batchSize:      1,
		balanceSurplus: big.Zero(),
	}, {
		name:           "max sectors",
		batchSize:      32,
		balanceSurplus: big.Zero(),
	}, {
		name:           "one deal",
		batchSize:      3,
		balanceSurplus: big.Zero(),
		deals: []dealSpec{{
			size:         32 << 30,
			verifiedSize: 0,
			IDs:          []abi.DealID{1},
		}},
	}, {
		name:           "many deals",
		batchSize:      3,
		balanceSurplus: big.Zero(),
		deals: []dealSpec{{
			size:         32 << 30,
			verifiedSize: 0,
			IDs:          []abi.DealID{1},
		}, {
			size:         0,
			verifiedSize: 32 << 30,
			IDs:          []abi.DealID{1},
		}, {
			size:         16 << 30,
			verifiedSize: 16 << 30,
			IDs:          []abi.DealID{1, 2},
		}},
	}, {
		name:           "empty batch",
		batchSize:      0,
		balanceSurplus: big.Zero(),
		exit:           exitcode.ErrIllegalArgument,
		error:          "batch empty",
	}, {
		name:           "too many sectors",
		batchSize:      33,
		balanceSurplus: big.Zero(),
		exit:           exitcode.ErrIllegalArgument,
		error:          "batch of 33 too large",
	}, {
		name:           "insufficient balance",
		batchSize:      10,
		balanceSurplus: abi.NewTokenAmount(1).Neg(),
		exit:           exitcode.ErrInsufficientFunds,
		error:          "insufficient funds",
	}} {
		t.Run(test.name, func(t *testing.T) {
			actor := newHarness(t, periodOffset)
			rt := builderForHarness(actor).
				Build(t)
			precommitEpoch := periodOffset + 1
			rt.SetEpoch(precommitEpoch)
			actor.constructAndVerify(rt)
			dlInfo := actor.deadline(rt)

			batchSize := test.batchSize
			sectorNos := make([]abi.SectorNumber, batchSize)
			sectorNoAsUints := make([]uint64, batchSize)
			for i := 0; i < batchSize; i++ {
				sectorNos[i] = abi.SectorNumber(100 + i)
				sectorNoAsUints[i] = uint64(100 + i)
			}
			sectorExpiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // on deadline boundary but > 180 days
			proveCommitEpoch := precommitEpoch + miner.PreCommitChallengeDelay + 1
			dealLifespan := sectorExpiration - proveCommitEpoch

			sectors := make([]miner0.SectorPreCommitInfo, batchSize)
			conf := preCommitBatchConf{
				sectorWeights: make([]market.SectorWeights, batchSize),
				firstForMiner: true,
			}
			deposits := make([]big.Int, batchSize)
			for i := 0; i < batchSize; i++ {
				deals := dealSpec{}
				if len(test.deals) > i {
					deals = test.deals[i]
				}
				sectors[i] = *actor.makePreCommit(sectorNos[i], precommitEpoch-1, sectorExpiration, deals.IDs)

				dealSpace := deals.size + deals.verifiedSize
				dealWeight := big.Mul(big.NewIntUnsigned(deals.size), big.NewInt(int64(dealLifespan)))
				verifiedDealWeight := big.Mul(big.NewIntUnsigned(deals.verifiedSize), big.NewInt(int64(dealLifespan)))
				conf.sectorWeights[i] = market.SectorWeights{
					DealSpace:          dealSpace,
					DealWeight:         dealWeight,
					VerifiedDealWeight: verifiedDealWeight,
				}
				pwrEstimate := miner.QAPowerForWeight(actor.sectorSize, sectors[i].Expiration-precommitEpoch, dealWeight, verifiedDealWeight)
				deposits[i] = miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwrEstimate)
			}
			totalDeposit := big.Sum(deposits...)
			rt.SetBalance(big.Add(totalDeposit, test.balanceSurplus))

			if test.exit != exitcode.Ok {
				rt.ExpectAbortContainsMessage(test.exit, test.error, func() {
					actor.preCommitSectorBatch(rt, &miner.PreCommitSectorBatchParams{Sectors: sectors}, conf)

					// State untouched.
					st := getState(rt)
					assert.True(t, st.PreCommitDeposits.IsZero())
					expirations := actor.collectPrecommitExpirations(rt, st)
					assert.Equal(t, map[abi.ChainEpoch][]uint64{}, expirations)
				})
				return
			}
			precommits := actor.preCommitSectorBatch(rt, &miner.PreCommitSectorBatchParams{Sectors: sectors}, conf)

			// Check precommits
			st := getState(rt)
			for i := 0; i < batchSize; i++ {
				assert.Equal(t, precommitEpoch, precommits[i].PreCommitEpoch)
				assert.Equal(t, conf.sectorWeights[i].DealWeight, precommits[i].DealWeight)
				assert.Equal(t, conf.sectorWeights[i].VerifiedDealWeight, precommits[i].VerifiedDealWeight)

				assert.Equal(t, sectorNos[i], precommits[i].Info.SectorNumber)
				assert.Equal(t, sectors[i].SealProof, precommits[i].Info.SealProof)
				assert.Equal(t, sectors[i].SealedCID, precommits[i].Info.SealedCID)
				assert.Equal(t, sectors[i].SealRandEpoch, precommits[i].Info.SealRandEpoch)
				assert.Equal(t, sectors[i].DealIDs, precommits[i].Info.DealIDs)
				assert.Equal(t, sectors[i].Expiration, precommits[i].Info.Expiration)

				pwrEstimate := miner.QAPowerForWeight(actor.sectorSize, precommits[i].Info.Expiration-precommitEpoch,
					conf.sectorWeights[i].DealWeight, conf.sectorWeights[i].VerifiedDealWeight)
				expectedDeposit := miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwrEstimate)
				assert.Equal(t, expectedDeposit, precommits[i].PreCommitDeposit)
			}

			assert.True(t, totalDeposit.GreaterThan(big.Zero()))
			assert.Equal(t, totalDeposit, st.PreCommitDeposits)

			expirations := actor.collectPrecommitExpirations(rt, st)
			expectedPrecommitExpiration := st.QuantSpecEveryDeadline().QuantizeUp(precommitEpoch + miner.MaxProveCommitDuration[actor.sealProofType] + miner.ExpiredPreCommitCleanUpDelay)
			assert.Equal(t, map[abi.ChainEpoch][]uint64{expectedPrecommitExpiration: sectorNoAsUints}, expirations)
		})
	}

	t.Run("one bad apple ruins batch", func(t *testing.T) {
		// This test does not enumerate all the individual conditions that could cause a single precommit
		// to be rejected. Those are covered in the PreCommitSector tests, and we know that that
		// method is implemented in terms of a batch of one.
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		dlInfo := actor.deadline(rt)

		sectorExpiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
		sectors := []miner0.SectorPreCommitInfo{
			*actor.makePreCommit(100, precommitEpoch-1, sectorExpiration, nil),
			*actor.makePreCommit(101, precommitEpoch-1, sectorExpiration, nil),
			*actor.makePreCommit(102, precommitEpoch-1, rt.Epoch(), nil), // Expires too soon
		}

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "sector expiration", func() {
			actor.preCommitSectorBatch(rt, &miner.PreCommitSectorBatchParams{Sectors: sectors}, preCommitBatchConf{firstForMiner: true})
		})
	})

	t.Run("duplicate sector rejects batch", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		dlInfo := actor.deadline(rt)

		sectorExpiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
		sectors := []miner0.SectorPreCommitInfo{
			*actor.makePreCommit(100, precommitEpoch-1, sectorExpiration, nil),
			*actor.makePreCommit(101, precommitEpoch-1, sectorExpiration, nil),
			*actor.makePreCommit(100, precommitEpoch-1, sectorExpiration, nil),
		}
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "duplicate sector number 100", func() {
			actor.preCommitSectorBatch(rt, &miner.PreCommitSectorBatchParams{Sectors: sectors}, preCommitBatchConf{firstForMiner: true})
		})
	})
}

func TestProveCommit(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)

	t.Run("prove single sector", func(t *testing.T) {
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
		}, true)

		// Check precommit
		// deal weights must be set in precommit onchain info
		assert.Equal(t, dealWeight, precommit.DealWeight)
		assert.Equal(t, verifiedDealWeight, precommit.VerifiedDealWeight)

		pwrEstimate := miner.QAPowerForWeight(actor.sectorSize, precommit.Info.Expiration-precommitEpoch, precommit.DealWeight, precommit.VerifiedDealWeight)
		expectedDeposit := miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwrEstimate)
		assert.Equal(t, expectedDeposit, precommit.PreCommitDeposit)

		// expect total precommit deposit to equal our new deposit
		st := getState(rt)
		assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

		// run prove commit logic
		rt.SetEpoch(proveCommitEpoch)
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))
		sector := actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})

		assert.Equal(t, precommit.Info.SealProof, sector.SealProof)
		assert.Equal(t, precommit.Info.SealedCID, sector.SealedCID)
		assert.Equal(t, precommit.Info.DealIDs, sector.DealIDs)
		assert.Equal(t, rt.Epoch(), sector.Activation)
		assert.Equal(t, precommit.Info.Expiration, sector.Expiration)
		assert.Equal(t, precommit.DealWeight, sector.DealWeight)
		assert.Equal(t, precommit.VerifiedDealWeight, sector.VerifiedDealWeight)

		// expect precommit to have been removed
		st = getState(rt)
		_, found, err := st.GetPrecommittedSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		require.False(t, found)

		// expect deposit to have been transferred to initial pledges
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)

		// The sector is exactly full with verified deals, so expect fully verified power.
		expectedPower := big.Mul(big.NewInt(int64(actor.sectorSize)), big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier))
		qaPower := miner.QAPowerForWeight(actor.sectorSize, precommit.Info.Expiration-rt.Epoch(), precommit.DealWeight, precommit.VerifiedDealWeight)
		assert.Equal(t, expectedPower, qaPower)
		sectorPower := miner.NewPowerPair(big.NewIntUnsigned(uint64(actor.sectorSize)), qaPower)

		// expect deal weights to be transferred to on chain info
		assert.Equal(t, precommit.DealWeight, sector.DealWeight)
		assert.Equal(t, precommit.VerifiedDealWeight, sector.VerifiedDealWeight)

		// expect initial plege of sector to be set, and be total pledge requirement
		expectedInitialPledge := miner.InitialPledgeForPower(qaPower, actor.baselinePower, actor.epochRewardSmooth, actor.epochQAPowerSmooth, rt.TotalFilCircSupply())
		assert.Equal(t, expectedInitialPledge, sector.InitialPledge)
		assert.Equal(t, expectedInitialPledge, st.InitialPledge)

		// expect sector to be assigned a deadline/partition
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, pIdx)
		assert.Equal(t, uint64(1), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.PartitionsPoSted)
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

	t.Run("prove sectors from batch pre-commit", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		dlInfo := actor.deadline(rt)

		sectorExpiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		sectors := []miner0.SectorPreCommitInfo{
			*actor.makePreCommit(100, precommitEpoch-1, sectorExpiration, nil),
			*actor.makePreCommit(101, precommitEpoch-1, sectorExpiration, []abi.DealID{1}),    // 1 * 32GiB verified deal
			*actor.makePreCommit(102, precommitEpoch-1, sectorExpiration, []abi.DealID{2, 3}), // 2 * 16GiB verified deals
		}

		dealSpace := uint64(32 << 30)
		dealWeight := big.Zero()
		proveCommitEpoch := precommitEpoch + miner.PreCommitChallengeDelay + 1
		dealLifespan := sectorExpiration - proveCommitEpoch
		verifiedDealWeight := big.Mul(big.NewIntUnsigned(dealSpace), big.NewInt(int64(dealLifespan)))

		// Power estimates made a pre-commit time
		noDealPowerEstimate := miner.QAPowerForWeight(actor.sectorSize, sectorExpiration-precommitEpoch, big.Zero(), big.Zero())
		fullDealPowerEstimate := miner.QAPowerForWeight(actor.sectorSize, sectorExpiration-precommitEpoch, dealWeight, verifiedDealWeight)

		deposits := []big.Int{
			miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, noDealPowerEstimate),
			miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, fullDealPowerEstimate),
			miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, fullDealPowerEstimate),
		}
		conf := preCommitBatchConf{
			sectorWeights: []market.SectorWeights{
				{DealSpace: 0, DealWeight: big.Zero(), VerifiedDealWeight: big.Zero()},
				{DealSpace: dealSpace, DealWeight: dealWeight, VerifiedDealWeight: verifiedDealWeight},
				{DealSpace: dealSpace, DealWeight: dealWeight, VerifiedDealWeight: verifiedDealWeight},
			},
			firstForMiner: true,
		}

		precommits := actor.preCommitSectorBatch(rt, &miner.PreCommitSectorBatchParams{Sectors: sectors}, conf)

		rt.SetEpoch(proveCommitEpoch)
		noDealPower := miner.QAPowerForWeight(actor.sectorSize, sectorExpiration-proveCommitEpoch, big.Zero(), big.Zero())
		noDealPledge := miner.InitialPledgeForPower(noDealPower, actor.baselinePower, actor.epochRewardSmooth, actor.epochQAPowerSmooth, rt.TotalFilCircSupply())
		fullDealPower := miner.QAPowerForWeight(actor.sectorSize, sectorExpiration-proveCommitEpoch, dealWeight, verifiedDealWeight)
		assert.Equal(t, big.Mul(big.NewInt(int64(actor.sectorSize)), big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier)), fullDealPower)
		fullDealPledge := miner.InitialPledgeForPower(fullDealPower, actor.baselinePower, actor.epochRewardSmooth, actor.epochQAPowerSmooth, rt.TotalFilCircSupply())

		// Prove just the first sector, with no deals
		{
			precommit := precommits[0]
			sector := actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(precommit.Info.SectorNumber), proveCommitConf{})
			assert.Equal(t, rt.Epoch(), sector.Activation)
			st := getState(rt)
			expectedDeposit := big.Sum(deposits[1:]...) // First sector deposit released
			assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

			// Expect power/pledge for a sector with no deals
			assert.Equal(t, noDealPledge, sector.InitialPledge)
			assert.Equal(t, noDealPledge, st.InitialPledge)
		}
		// Prove the next, with one deal
		{
			precommit := precommits[1]
			sector := actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(precommit.Info.SectorNumber), proveCommitConf{})
			assert.Equal(t, rt.Epoch(), sector.Activation)
			st := getState(rt)
			expectedDeposit := big.Sum(deposits[2:]...) // First and second sector deposits released
			assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

			// Expect power/pledge for the two sectors (only this one having any deal weight)
			assert.Equal(t, fullDealPledge, sector.InitialPledge)
			assert.Equal(t, big.Add(noDealPledge, fullDealPledge), st.InitialPledge)
		}
		// Prove the last
		{
			precommit := precommits[2]
			sector := actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(precommit.Info.SectorNumber), proveCommitConf{})
			assert.Equal(t, rt.Epoch(), sector.Activation)
			st := getState(rt)
			assert.Equal(t, big.Zero(), st.PreCommitDeposits)

			// Expect power/pledge for the three sectors
			assert.Equal(t, fullDealPledge, sector.InitialPledge)
			assert.Equal(t, big.Sum(noDealPledge, fullDealPledge, fullDealPledge), st.InitialPledge)
		}
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
		precommit := actor.preCommitSector(rt, params, preCommitConf{}, true)

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

		proveCommit := makeProveCommit(sectorNo)
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

	t.Run("prove commit aborts if pledge requirement not met", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)
		// Set the circulating supply high and expected reward low in order to coerce
		// pledge requirements (BR + share of money supply, but capped at 1FIL)
		// to exceed pre-commit deposit (BR only).
		rt.SetCirculatingSupply(big.Mul(big.NewInt(100_000_000), big.NewInt(1e18)))
		actor.epochRewardSmooth = smoothing.TestingConstantEstimate(big.NewInt(1e15))

		// prove one sector to establish collateral and locked funds
		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

		// preecommit another sector so we may prove it
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		params := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, nil)
		precommit := actor.preCommitSector(rt, params, preCommitConf{}, false)

		// Confirm the unlocked PCD will not cover the new IP
		assert.True(t, sectors[0].InitialPledge.GreaterThan(precommit.PreCommitDeposit))

		// Set balance to exactly cover locked funds.
		st := getState(rt)
		rt.SetBalance(big.Sum(st.PreCommitDeposits, st.InitialPledge, st.LockedFunds))

		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[actor.sealProofType] - 1)
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
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		actor.constructAndVerify(rt)

		// make two precommits
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		paramsA := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, []abi.DealID{1})
		preCommitA := actor.preCommitSector(rt, paramsA, preCommitConf{}, true)
		sectorNoA := actor.nextSectorNo
		actor.nextSectorNo++
		paramsB := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, []abi.DealID{2})
		preCommitB := actor.preCommitSector(rt, paramsB, preCommitConf{}, false)
		sectorNoB := actor.nextSectorNo

		// handle both prove commits in the same epoch
		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[actor.sealProofType] - 1)

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
		precommit := actor.preCommitSector(rt, params, preCommitConf{}, true)

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

	t.Run("verify proof does not vest funds", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
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
		precommit := actor.preCommitSector(rt, params, preCommitConf{}, true)

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

		proveCommit := makeProveCommit(sectorNo)
		proveCommit.Proof = make([]byte, 192)
		// The below call expects exactly the pledge delta for the proven sector, zero for any other vesting.
		actor.proveCommitSectorAndConfirm(rt, precommit, proveCommit, proveCommitConf{})
	})
}

func TestAggregateProveCommit(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	t.Run("valid precommits then aggregate provecommit", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		dlInfo := actor.deadline(rt)

		// Make a good commitment for the proof to target.

		proveCommitEpoch := precommitEpoch + miner.PreCommitChallengeDelay + 1
		expiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // something on deadline boundary but > 180 days
		// Fill the sector with verified deals
		sectorWeight := big.Mul(big.NewInt(int64(actor.sectorSize)), big.NewInt(int64(expiration-proveCommitEpoch)))
		dealWeight := big.Zero()
		verifiedDealWeight := sectorWeight

		var precommits []*miner.SectorPreCommitOnChainInfo
		sectorNosBf := bitfield.New()
		for i := 0; i < 10; i++ {
			sectorNo := abi.SectorNumber(i)
			sectorNosBf.Set(uint64(i))
			precommitParams := actor.makePreCommit(sectorNo, precommitEpoch-1, expiration, []abi.DealID{1})
			precommit := actor.preCommitSector(rt, precommitParams, preCommitConf{
				dealWeight:         dealWeight,
				verifiedDealWeight: verifiedDealWeight,
			}, i == 0)
			precommits = append(precommits, precommit)
		}
		sectorNosBf, err := sectorNosBf.Copy() //flush map to run to match partition state
		require.NoError(t, err)

		// run prove commit logic
		rt.SetEpoch(proveCommitEpoch)
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))

		actor.proveCommitAggregateSector(rt, proveCommitConf{}, precommits, makeProveCommitAggregate(sectorNosBf))

		// expect precommits to have been removed
		st := getState(rt)
		require.NoError(t, sectorNosBf.ForEach(func(sectorNo uint64) error {
			_, found, err := st.GetPrecommittedSector(rt.AdtStore(), abi.SectorNumber(sectorNo))
			require.False(t, found)
			return err
		}))

		// expect deposit to have been transferred to initial pledges
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)

		// The sector is exactly full with verified deals, so expect fully verified power.
		expectedPower := big.Mul(big.NewInt(int64(actor.sectorSize)), big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier))
		qaPower := miner.QAPowerForWeight(actor.sectorSize, expiration-rt.Epoch(), dealWeight, verifiedDealWeight)
		assert.Equal(t, expectedPower, qaPower)
		expectedInitialPledge := miner.InitialPledgeForPower(qaPower, actor.baselinePower, actor.epochRewardSmooth,
			actor.epochQAPowerSmooth, rt.TotalFilCircSupply())
		tenSectorsInitialPledge := big.Mul(big.NewInt(10), expectedInitialPledge)
		assert.Equal(t, tenSectorsInitialPledge, st.InitialPledge)

		// expect new onchain sector
		require.NoError(t, sectorNosBf.ForEach(func(sectorNo uint64) error {
			sector := actor.getSector(rt, abi.SectorNumber(sectorNo))
			// expect deal weights to be transferred to on chain info
			assert.Equal(t, dealWeight, sector.DealWeight)
			assert.Equal(t, verifiedDealWeight, sector.VerifiedDealWeight)

			// expect activation epoch to be current epoch
			assert.Equal(t, rt.Epoch(), sector.Activation)

			// expect initial plege of sector to be set
			assert.Equal(t, expectedInitialPledge, sector.InitialPledge)

			// expect sector to be assigned a deadline/partition
			dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), abi.SectorNumber(sectorNo))
			// first ten sectors should be assigned to deadline 0 partition 0
			assert.Equal(t, uint64(0), dlIdx)
			assert.Equal(t, uint64(0), pIdx)
			require.NoError(t, err)

			return nil

		}))

		sectorPower := miner.NewPowerPair(big.NewIntUnsigned(uint64(actor.sectorSize)), qaPower)
		tenSectorsPower := miner.NewPowerPair(big.Mul(big.NewInt(10), sectorPower.Raw), big.Mul(big.NewInt(10), sectorPower.QA))

		dlIdx := uint64(0)
		pIdx := uint64(0)
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, pIdx)
		assert.Equal(t, uint64(10), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.PartitionsPoSted)
		assertEmptyBitfield(t, deadline.EarlyTerminations)

		quant := st.QuantSpecForDeadline(dlIdx)
		quantizedExpiration := quant.QuantizeUp(expiration)

		dQueue := actor.collectDeadlineExpirations(rt, deadline)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			quantizedExpiration: {pIdx},
		}, dQueue)

		assert.Equal(t, partition.Sectors, sectorNosBf)
		assertEmptyBitfield(t, partition.Faults)
		assertEmptyBitfield(t, partition.Recoveries)
		assertEmptyBitfield(t, partition.Terminated)
		assert.Equal(t, tenSectorsPower, partition.LivePower)
		assert.Equal(t, miner.NewPowerPairZero(), partition.FaultyPower)
		assert.Equal(t, miner.NewPowerPairZero(), partition.RecoveringPower)

		pQueue := actor.collectPartitionExpirations(rt, partition)
		entry, ok := pQueue[quantizedExpiration]
		require.True(t, ok)
		assert.Equal(t, entry.OnTimeSectors, sectorNosBf)
		assertEmptyBitfield(t, entry.EarlySectors)
		assert.Equal(t, tenSectorsInitialPledge, entry.OnTimePledge)
		assert.Equal(t, tenSectorsPower, entry.ActivePower)
		assert.Equal(t, miner.NewPowerPairZero(), entry.FaultyPower)

		// expect 10x locked initial pledge of sector to be the same as pledge requirement
		assert.Equal(t, tenSectorsInitialPledge, st.InitialPledge)

	})
}
