package miner_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v5/support/ipld"
	tutils "github.com/filecoin-project/specs-actors/v5/support/testing"
)

func TestPrecommittedSectorsStore(t *testing.T) {
	t.Run("Put, get and delete", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		pc1 := newPreCommitOnChain(1, tutils.MakeCID("1", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), 1)
		require.NoError(t, harness.s.PutPrecommittedSectors(harness.store, pc1))
		assert.Equal(t, pc1, harness.getPreCommit(1))

		pc2 := newPreCommitOnChain(2, tutils.MakeCID("2", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), 1)
		require.NoError(t, harness.s.PutPrecommittedSectors(harness.store, pc2))
		assert.Equal(t, pc2, harness.getPreCommit(2))

		pc3 := newPreCommitOnChain(3, tutils.MakeCID("2", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), 1)
		pc4 := newPreCommitOnChain(4, tutils.MakeCID("2", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), 1)
		require.NoError(t, harness.s.PutPrecommittedSectors(harness.store, pc3, pc4))
		assert.Equal(t, pc3, harness.getPreCommit(3))
		assert.Equal(t, pc4, harness.getPreCommit(4))

		harness.deletePreCommit(1)
		assert.False(t, harness.hasPreCommit(1))
		assert.True(t, harness.hasPreCommit(2))
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		err := harness.s.DeletePrecommittedSectors(harness.store, 1)
		assert.Error(t, err)
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		assert.False(t, harness.hasPreCommit(1))
	})

	t.Run("Duplicate put rejected", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		pc1 := newPreCommitOnChain(1, tutils.MakeCID("1", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), 1)
		// In sequence
		assert.NoError(t, harness.s.PutPrecommittedSectors(harness.store, pc1))
		assert.Error(t, harness.s.PutPrecommittedSectors(harness.store, pc1))

		// In batch
		pc2 := newPreCommitOnChain(2, tutils.MakeCID("2", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), 1)
		assert.Error(t, harness.s.PutPrecommittedSectors(harness.store, pc2, pc2))
	})
}

func TestSectorsStore(t *testing.T) {
	t.Run("Put get and delete", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo1 := newSectorOnChainInfo(sectorNo, tutils.MakeCID("1", &miner.SealedCIDPrefix), big.NewInt(1), abi.ChainEpoch(1))
		sectorInfo2 := newSectorOnChainInfo(sectorNo, tutils.MakeCID("2", &miner.SealedCIDPrefix), big.NewInt(2), abi.ChainEpoch(2))

		harness.putSector(sectorInfo1)
		assert.True(t, harness.hasSectorNo(sectorNo))
		out := harness.getSector(sectorNo)
		assert.Equal(t, sectorInfo1, out)

		harness.putSector(sectorInfo2)
		out = harness.getSector(sectorNo)
		assert.Equal(t, sectorInfo2, out)

		harness.deleteSectors(uint64(sectorNo))
		assert.False(t, harness.hasSectorNo(sectorNo))
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		bf := bitfield.New()
		bf.Set(uint64(sectorNo))

		assert.Error(t, harness.s.DeleteSectors(harness.store, bf))
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		assert.False(t, harness.hasSectorNo(sectorNo))
	})

	t.Run("Iterate and Delete multiple sector", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

		// put all the sectors in the store
		for _, s := range sectorNos {
			i := int64(0)
			harness.putSector(newSectorOnChainInfo(abi.SectorNumber(s), tutils.MakeCID(fmt.Sprintf("%d", i), &miner.SealedCIDPrefix), big.NewInt(i), abi.ChainEpoch(i)))
			i++
		}

		sectorNoIdx := 0
		err := harness.s.ForEachSector(harness.store, func(si *miner.SectorOnChainInfo) {
			require.Equal(t, abi.SectorNumber(sectorNos[sectorNoIdx]), si.SectorNumber)
			sectorNoIdx++
		})
		assert.NoError(t, err)

		// ensure we iterated over the expected number of sectors
		assert.Equal(t, len(sectorNos), sectorNoIdx)

		harness.deleteSectors(sectorNos...)
		for _, s := range sectorNos {
			assert.False(t, harness.hasSectorNo(abi.SectorNumber(s)))
		}
	})
}

// TODO minerstate: move to partition
//func TestRecoveriesBitfield(t *testing.T) {
//	t.Run("Add new recoveries happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		// set of sectors, the larger numbers here are not significant
//		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
//		harness.addRecoveries(sectorNos...)
//		assert.Equal(t, uint64(len(sectorNos)), harness.getRecoveriesCount())
//	})
//
//	t.Run("Add new recoveries excludes duplicates", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
//		harness.addRecoveries(sectorNos...)
//		assert.Equal(t, uint64(5), harness.getRecoveriesCount())
//	})
//
//	t.Run("Remove recoveries happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 2, 3, 4, 5}
//		harness.addRecoveries(sectorNos...)
//		assert.Equal(t, uint64(len(sectorNos)), harness.getRecoveriesCount())
//
//		harness.removeRecoveries(1, 3, 5)
//		assert.Equal(t, uint64(2), harness.getRecoveriesCount())
//
//		recoveries, err := harness.s.Recoveries.All(uint64(len(sectorNos)))
//		assert.NoError(t, err)
//		assert.Equal(t, []uint64{2, 4}, recoveries)
//	})
//}

//func TestPostSubmissionsBitfield(t *testing.T) {
//	t.Run("Add new submission happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		// set of sectors, the larger numbers here are not significant
//		partitionNos := []uint64{10, 20, 30, 40}
//		harness.addPoStSubmissions(partitionNos...)
//		assert.Equal(t, uint64(len(partitionNos)), harness.getPoStSubmissionsCount())
//	})
//
//	t.Run("Add new submission excludes duplicates", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
//		harness.addPoStSubmissions(sectorNos...)
//		assert.Equal(t, uint64(5), harness.getPoStSubmissionsCount())
//	})
//
//	t.Run("Clear submission happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 2, 3, 4, 5}
//		harness.addPoStSubmissions(sectorNos...)
//		assert.Equal(t, uint64(len(sectorNos)), harness.getPoStSubmissionsCount())
//
//		harness.clearPoStSubmissions()
//		assert.Equal(t, uint64(0), harness.getPoStSubmissionsCount())
//	})
//}

func TestVesting_AddLockedFunds_Table(t *testing.T) {
	vestStartDelay := abi.ChainEpoch(10)
	vestSum := int64(100)

	testcase := []struct {
		desc        string
		vspec       *miner.VestSpec
		periodStart abi.ChainEpoch
		vepocs      []int64
	}{
		{
			desc: "vest funds in a single epoch",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   1,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 100, 0},
		},
		{
			desc: "vest funds with period=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   2,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 50, 50, 0},
		},
		{
			desc: "vest funds with period=2 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   2,
				StepDuration: 1,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 100, 0},
		},
		{desc: "vest funds with period=3",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   3,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 33, 33, 34, 0},
		},
		{
			desc: "vest funds with period=3 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   3,
				StepDuration: 1,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 66, 0, 34, 0},
		},
		{desc: "vest funds with period=2 step=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   2,
				StepDuration: 2,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 100, 0},
		},
		{
			desc: "vest funds with period=5 step=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 2,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 40, 0, 40, 0, 20, 0},
		},
		{
			desc: "vest funds with delay=1 period=5 step=2",
			vspec: &miner.VestSpec{
				InitialDelay: 1,
				VestPeriod:   5,
				StepDuration: 2,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 40, 0, 40, 0, 20, 0},
		},
		{
			desc: "vest funds with period=5 step=2 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 2,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 40, 0, 40, 0, 20, 0},
		},
		{
			desc: "vest funds with period=5 step=3 quantization=1",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 3,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 60, 0, 0, 40, 0},
		},
		{
			desc: "vest funds with period=5 step=3 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 3,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 0, 0, 80, 0, 20, 0},
		},
		{
			desc: "(step greater than period) vest funds with period=5 step=6 quantization=1",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 6,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 0, 0, 0, 100, 0},
		},
		{
			desc: "vest funds with delay=5 period=5 step=1 quantization=1",
			vspec: &miner.VestSpec{
				InitialDelay: 5,
				VestPeriod:   5,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 0, 0, 0, 20, 20, 20, 20, 20, 0},
		},
		{
			desc: "vest funds with offset 0",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   10,
				StepDuration: 2,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 20, 0, 20, 0, 20, 0, 20, 0, 20},
		},
		{
			desc: "vest funds with offset 1",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   10,
				StepDuration: 2,
				Quantization: 2,
			},
			periodStart: abi.ChainEpoch(1),
			// start epoch is at 11 instead of 10 so vepocs are shifted by one from above case
			vepocs: []int64{0, 0, 0, 20, 0, 20, 0, 20, 0, 20, 0, 20},
		},
		{
			desc: "vest funds with proving period start > quantization unit",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   10,
				StepDuration: 2,
				Quantization: 2,
			},
			// 55 % 2 = 1 so expect same vepocs with offset 1 as in previous case
			periodStart: abi.ChainEpoch(55),
			vepocs:      []int64{0, 0, 0, 20, 0, 20, 0, 20, 0, 20, 0, 20},
		},
		{
			desc: "vest funds with step much smaller than quantization",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   10,
				StepDuration: 1,
				Quantization: 5,
			},
			vepocs: []int64{0, 0, 0, 0, 0, 0, 50, 0, 0, 0, 0, 50},
		},
	}
	for _, tc := range testcase {
		t.Run(tc.desc, func(t *testing.T) {
			harness := constructStateHarness(t, tc.periodStart)
			vestStart := tc.periodStart + vestStartDelay

			harness.addLockedFunds(vestStart, abi.NewTokenAmount(vestSum), tc.vspec)
			assert.Equal(t, abi.NewTokenAmount(vestSum), harness.s.LockedFunds)

			var totalVested int64
			for e, v := range tc.vepocs {
				assert.Equal(t, abi.NewTokenAmount(v), harness.unlockVestedFunds(vestStart+abi.ChainEpoch(e)))
				totalVested += v
				assert.Equal(t, vestSum-totalVested, harness.s.LockedFunds.Int64())
			}

			assert.Equal(t, abi.NewTokenAmount(vestSum), abi.NewTokenAmount(totalVested))
			assert.True(t, harness.vestingFundsStoreEmpty())
			assert.Zero(t, harness.s.LockedFunds.Int64())
		})
	}
}

func TestVestingFunds_AddLockedFunds(t *testing.T) {
	t.Run("LockedFunds increases with sequential calls", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   1,
			StepDuration: 1,
			Quantization: 1,
		}

		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)

		harness.addLockedFunds(vestStart, vestSum, vspec)
		assert.Equal(t, vestSum, harness.s.LockedFunds)

		harness.addLockedFunds(vestStart, vestSum, vspec)
		assert.Equal(t, big.Mul(vestSum, big.NewInt(2)), harness.s.LockedFunds)
	})

	t.Run("Vests when quantize, step duration, and vesting period are coprime", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   27,
			StepDuration: 5,
			Quantization: 7,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		assert.Equal(t, vestSum, harness.s.LockedFunds)

		totalVested := abi.NewTokenAmount(0)
		for e := vestStart; e <= 43; e++ {
			amountVested := harness.unlockVestedFunds(e)
			switch e {
			case 22:
				assert.Equal(t, abi.NewTokenAmount(40), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			case 29:
				assert.Equal(t, abi.NewTokenAmount(26), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			case 36:
				assert.Equal(t, abi.NewTokenAmount(26), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			case 43:
				assert.Equal(t, abi.NewTokenAmount(8), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			default:
				assert.Equal(t, abi.NewTokenAmount(0), amountVested)
			}
		}
		assert.Equal(t, vestSum, totalVested)
		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})
}

func TestVestingFunds_UnvestedFunds(t *testing.T) {
	t.Run("Unlock unvested funds leaving bucket with non-zero tokens", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   5,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(100)
		vestSum := abi.NewTokenAmount(100)

		harness.addLockedFunds(vestStart, vestSum, vspec)

		amountUnlocked := harness.unlockUnvestedFunds(vestStart, big.NewInt(39))
		assert.Equal(t, big.NewInt(39), amountUnlocked)

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart))
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+1))

		// expected to be zero due to unlocking of UNvested funds
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+2))
		// expected to be non-zero due to unlocking of UNvested funds
		assert.Equal(t, abi.NewTokenAmount(1), harness.unlockVestedFunds(vestStart+3))

		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+4))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+5))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+6))

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+7))

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Unlock unvested funds leaving bucket with zero tokens", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   5,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(100)
		vestSum := abi.NewTokenAmount(100)

		harness.addLockedFunds(vestStart, vestSum, vspec)

		amountUnlocked := harness.unlockUnvestedFunds(vestStart, big.NewInt(40))
		assert.Equal(t, big.NewInt(40), amountUnlocked)

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart))
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+1))

		// expected to be zero due to unlocking of UNvested funds
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+2))
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+3))

		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+4))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+5))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+6))

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+7))

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Unlock all unvested funds", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   5,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		unvestedFunds := harness.unlockUnvestedFunds(vestStart, vestSum)
		assert.Equal(t, vestSum, unvestedFunds)

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Unlock unvested funds value greater than LockedFunds", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   1,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		unvestedFunds := harness.unlockUnvestedFunds(vestStart, abi.NewTokenAmount(200))
		assert.Equal(t, vestSum, unvestedFunds)

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())

	})

	t.Run("Unlock unvested funds when there are vested funds in the table", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   50,
			StepDuration: 1,
			Quantization: 1,
		}

		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)

		// will lock funds from epochs 11 to 60
		harness.addLockedFunds(vestStart, vestSum, vspec)

		// unlock funds from epochs 30 to 60
		newEpoch := abi.ChainEpoch(30)
		target := abi.NewTokenAmount(60)
		remaining := big.Sub(vestSum, target)
		unvestedFunds := harness.unlockUnvestedFunds(newEpoch, target)
		assert.Equal(t, target, unvestedFunds)

		assert.EqualValues(t, remaining, harness.s.LockedFunds)

		// vesting funds should have all epochs from 11 to 29
		funds, err := harness.s.LoadVestingFunds(harness.store)
		assert.NoError(t, err)
		epoch := 11
		for _, vf := range funds.Funds {
			assert.EqualValues(t, epoch, vf.Epoch)
			epoch = epoch + 1
			if epoch == 30 {
				break
			}
		}
	})
}

func TestAddPreCommitExpiry(t *testing.T) {
	t.Run("simple pre-commit expiry and cleanup", func(t *testing.T) {
		harness := constructStateHarness(t, 0)
		err := harness.s.AddPreCommitCleanUps(harness.store, map[abi.ChainEpoch][]uint64{100: {1}})
		require.NoError(t, err)

		quant := harness.s.QuantSpecEveryDeadline()
		ExpectBQ().
			Add(quant.QuantizeUp(100), 1).
			Equals(t, harness.loadPreCommitCleanUps())

		err = harness.s.AddPreCommitCleanUps(harness.store, map[abi.ChainEpoch][]uint64{100: {2}})
		require.NoError(t, err)
		ExpectBQ().
			Add(quant.QuantizeUp(100), 1, 2).
			Equals(t, harness.loadPreCommitCleanUps())

		err = harness.s.AddPreCommitCleanUps(harness.store, map[abi.ChainEpoch][]uint64{200: {3}})
		require.NoError(t, err)
		ExpectBQ().
			Add(quant.QuantizeUp(100), 1, 2).
			Add(quant.QuantizeUp(200), 3).
			Equals(t, harness.loadPreCommitCleanUps())
	})

	t.Run("batch pre-commit expiry", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		err := harness.s.AddPreCommitCleanUps(harness.store, map[abi.ChainEpoch][]uint64{
			100: {1},
			200: {2, 3},
			300: {},
		})
		require.NoError(t, err)

		quant := harness.s.QuantSpecEveryDeadline()
		ExpectBQ().
			Add(quant.QuantizeUp(100), 1).
			Add(quant.QuantizeUp(200), 2, 3).
			Equals(t, harness.loadPreCommitCleanUps())

		err = harness.s.AddPreCommitCleanUps(harness.store, map[abi.ChainEpoch][]uint64{
			100: {1}, // Redundant
			200: {4},
			300: {5, 6},
		})
		require.NoError(t, err)
		ExpectBQ().
			Add(quant.QuantizeUp(100), 1).
			Add(quant.QuantizeUp(200), 2, 3, 4).
			Add(quant.QuantizeUp(300), 5, 6).
			Equals(t, harness.loadPreCommitCleanUps())
	})
}

func TestSectorAssignment(t *testing.T) {
	partitionSectors, err := builtin.SealProofWindowPoStPartitionSectors(abi.RegisteredSealProof_StackedDrg32GiBV1_1)
	require.NoError(t, err)
	sectorSize, err := abi.RegisteredSealProof_StackedDrg32GiBV1_1.SectorSize()
	require.NoError(t, err)

	openDeadlines := miner.WPoStPeriodDeadlines - 2

	partitionsPerDeadline := uint64(3)
	noSectors := int(partitionSectors * openDeadlines * partitionsPerDeadline)
	sectorInfos := make([]*miner.SectorOnChainInfo, noSectors)
	for i := range sectorInfos {
		sectorInfos[i] = newSectorOnChainInfo(
			abi.SectorNumber(i), tutils.MakeCID(fmt.Sprintf("%d", i), &miner.SealedCIDPrefix), big.NewInt(1), abi.ChainEpoch(0),
		)
	}

	dlState := expectedDeadlineState{
		sectorSize:    sectorSize,
		partitionSize: partitionSectors,
		sectors:       sectorInfos,
	}

	t.Run("assign sectors to deadlines", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		err := harness.s.AssignSectorsToDeadlines(harness.store, 0, sectorInfos,
			partitionSectors, sectorSize)
		require.NoError(t, err)

		sectorArr := sectorsArr(t, harness.store, sectorInfos)

		dls, err := harness.s.LoadDeadlines(harness.store)
		require.NoError(t, err)
		require.NoError(t, dls.ForEach(harness.store, func(dlIdx uint64, dl *miner.Deadline) error {
			quantSpec := harness.s.QuantSpecForDeadline(dlIdx)
			// deadlines 0 & 1 are closed for assignment right now.
			if dlIdx < 2 {
				dlState.withQuantSpec(quantSpec).
					assert(t, harness.store, dl)
				return nil
			}

			var partitions []bitfield.BitField
			var postPartitions []miner.PoStPartition
			for i := uint64(0); i < uint64(partitionsPerDeadline); i++ {
				start := ((i * openDeadlines) + (dlIdx - 2)) * partitionSectors
				partBf := seq(t, start, partitionSectors)
				partitions = append(partitions, partBf)
				postPartitions = append(postPartitions, miner.PoStPartition{
					Index:   i,
					Skipped: bf(),
				})
			}
			allSectorBf, err := bitfield.MultiMerge(partitions...)
			require.NoError(t, err)
			allSectorNos, err := allSectorBf.All(uint64(noSectors))
			require.NoError(t, err)

			dlState.withQuantSpec(quantSpec).
				withUnproven(allSectorNos...).
				withPartitions(partitions...).
				assert(t, harness.store, dl)

			// Now make sure proving activates power.

			result, err := dl.RecordProvenSectors(harness.store, sectorArr, sectorSize, quantSpec, 0, postPartitions)
			require.NoError(t, err)

			expectedPowerDelta := miner.PowerForSectors(sectorSize, selectSectors(t, sectorInfos, allSectorBf))

			assertBitfieldsEqual(t, allSectorBf, result.Sectors)
			assertBitfieldEmpty(t, result.IgnoredSectors)
			assert.True(t, result.NewFaultyPower.Equals(miner.NewPowerPairZero()))
			assert.True(t, result.PowerDelta.Equals(expectedPowerDelta))
			assert.True(t, result.RecoveredPower.Equals(miner.NewPowerPairZero()))
			assert.True(t, result.RetractedRecoveryPower.Equals(miner.NewPowerPairZero()))
			return nil
		}))

		// Now prove and activate/check power.
	})
}

func TestSectorNumberAllocation(t *testing.T) {
	allocate := func(h *stateHarness, numbers ...uint64) error {
		return h.s.AllocateSectorNumbers(h.store, bitfield.NewFromSet(numbers), miner.DenyCollisions)
	}
	mask := func(h *stateHarness, ns bitfield.BitField) error {
		return h.s.AllocateSectorNumbers(h.store, ns, miner.AllowCollisions)
	}
	expect := func(h *stateHarness, expected bitfield.BitField) {
		var b bitfield.BitField
		err := h.store.Get(context.Background(), h.s.AllocatedSectors, &b)
		assert.NoError(t, err)
		assertBitfieldsEqual(t, expected, b)
	}

	t.Run("batch allocation", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		assert.NoError(t, allocate(harness, 1, 2, 3))
		assert.NoError(t, allocate(harness, 4, 5, 6))
		expect(harness, bf(1, 2, 3, 4, 5, 6))
	})

	t.Run("repeat allocation rejected", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		assert.NoError(t, allocate(harness, 1))
		assert.Error(t, allocate(harness, 1))
		expect(harness, bf(1))
	})

	t.Run("overlapping batch rejected", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		assert.NoError(t, allocate(harness, 1, 2, 3))
		assert.Error(t, allocate(harness, 3, 4, 5))
		expect(harness, bf(1, 2, 3))
	})

	t.Run("batch masking", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		assert.NoError(t, allocate(harness, 1))

		assert.NoError(t, mask(harness, bf(0, 1, 2, 3)))
		expect(harness, bf(0, 1, 2, 3))

		assert.Error(t, allocate(harness, 0))
		assert.Error(t, allocate(harness, 3))
		assert.NoError(t, allocate(harness, 4))
		expect(harness, bf(0, 1, 2, 3, 4))
	})

	t.Run("range limits", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		assert.NoError(t, allocate(harness, 0))
		assert.NoError(t, allocate(harness, abi.MaxSectorNumber))
		expect(harness, bf(0, abi.MaxSectorNumber))
	})

	t.Run("mask range limits", func(t *testing.T) {
		harness := constructStateHarness(t, 0)

		assert.NoError(t, mask(harness, bf(0)))
		assert.NoError(t, mask(harness, bf(abi.MaxSectorNumber)))
		expect(harness, bf(0, abi.MaxSectorNumber))
	})

	t.Run("compaction with mask", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		// Allocate widely-spaced numbers to consume the run-length encoded bytes quickly,
		// until the limit is reached.
		limitReached := false
		for i := uint64(0); i < math.MaxUint64; i++ {
			no := (i + 1) << 50
			err := allocate(harness, no)
			if err != nil {
				// We failed, yay!
				limitReached = true
				code := exitcode.Unwrap(err, exitcode.Ok)
				assert.Equal(t, code, exitcode.ErrIllegalArgument)

				// mask half the sector ranges.
				toMask := seq(t, 0, uint64(no)/2)
				require.NoError(t, mask(harness, toMask))

				// try again
				require.NoError(t, allocate(harness, no))
				break
			}
		}
		assert.True(t, limitReached)
	})
}

func TestRepayDebtInPriorityOrder(t *testing.T) {
	harness := constructStateHarness(t, abi.ChainEpoch(0))

	currentBalance := abi.NewTokenAmount(300)
	fee := abi.NewTokenAmount(1000)
	err := harness.s.ApplyPenalty(fee)
	require.NoError(t, err)

	assert.Equal(t, harness.s.FeeDebt, fee)
	penaltyFromVesting, penaltyFromBalance, err := harness.s.RepayPartialDebtInPriorityOrder(harness.store, abi.ChainEpoch(0), currentBalance)
	require.NoError(t, err)

	assert.Equal(t, penaltyFromVesting, big.Zero())
	assert.Equal(t, penaltyFromBalance, currentBalance)

	expectedDebt := big.Sub(currentBalance, fee).Neg()
	assert.Equal(t, expectedDebt, harness.s.FeeDebt)

	currentBalance = abi.NewTokenAmount(0)
	fee = abi.NewTokenAmount(2050)
	err = harness.s.ApplyPenalty(fee)
	require.NoError(t, err)

	_, _, err = harness.s.RepayPartialDebtInPriorityOrder(harness.store, abi.ChainEpoch(33), currentBalance)
	require.NoError(t, err)

	expectedDebt = big.Add(expectedDebt, fee)
	assert.Equal(t, expectedDebt, harness.s.FeeDebt)
}

type stateHarness struct {
	t testing.TB

	s     *miner.State
	store adt.Store
}

//
// Vesting Store
//

func (h *stateHarness) addLockedFunds(epoch abi.ChainEpoch, sum abi.TokenAmount, spec *miner.VestSpec) {
	_, err := h.s.AddLockedFunds(h.store, epoch, sum, spec)
	require.NoError(h.t, err)
}

func (h *stateHarness) unlockUnvestedFunds(epoch abi.ChainEpoch, target abi.TokenAmount) abi.TokenAmount {
	amount, err := h.s.UnlockUnvestedFunds(h.store, epoch, target)
	require.NoError(h.t, err)
	return amount
}

func (h *stateHarness) unlockVestedFunds(epoch abi.ChainEpoch) abi.TokenAmount {
	amount, err := h.s.UnlockVestedFunds(h.store, epoch)
	require.NoError(h.t, err)

	return amount
}

func (h *stateHarness) vestingFundsStoreEmpty() bool {
	funds, err := h.s.LoadVestingFunds(h.store)
	require.NoError(h.t, err)

	return len(funds.Funds) == 0
}

//
// Sector Store Assertion Operations
//

func (h *stateHarness) hasSectorNo(sectorNo abi.SectorNumber) bool {
	found, err := h.s.HasSectorNo(h.store, sectorNo)
	require.NoError(h.t, err)
	return found
}

func (h *stateHarness) putSector(sector *miner.SectorOnChainInfo) {
	err := h.s.PutSectors(h.store, sector)
	require.NoError(h.t, err)
}

func (h *stateHarness) getSector(sectorNo abi.SectorNumber) *miner.SectorOnChainInfo {
	sectors, found, err := h.s.GetSector(h.store, sectorNo)
	require.NoError(h.t, err)
	assert.True(h.t, found)
	assert.NotNil(h.t, sectors)
	return sectors
}

// makes a bit field from the passed sector numbers
func (h *stateHarness) deleteSectors(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.DeleteSectors(h.store, bf)
	require.NoError(h.t, err)
}

//
// Precommit Store Operations
//

func (h *stateHarness) getPreCommit(sectorNo abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	out, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
	assert.True(h.t, found)
	return out
}

func (h *stateHarness) hasPreCommit(sectorNo abi.SectorNumber) bool {
	_, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
	return found
}

func (h *stateHarness) deletePreCommit(sectorNo abi.SectorNumber) {
	err := h.s.DeletePrecommittedSectors(h.store, sectorNo)
	require.NoError(h.t, err)
}

func (h *stateHarness) loadPreCommitCleanUps() miner.BitfieldQueue {
	queue, err := miner.LoadBitfieldQueue(h.store, h.s.PreCommittedSectorsCleanUp, h.s.QuantSpecEveryDeadline(), miner.PrecommitCleanUpAmtBitwidth)
	require.NoError(h.t, err)
	return queue
}

func constructStateHarness(t *testing.T, periodBoundary abi.ChainEpoch) *stateHarness {
	// store init
	store := ipld.NewADTStore(context.Background())
	// state field init
	owner := tutils.NewBLSAddr(t, 1)
	worker := tutils.NewBLSAddr(t, 2)

	testWindowPoStProofType := abi.RegisteredPoStProof_StackedDrgWindow2KiBV1

	sectorSize, err := testWindowPoStProofType.SectorSize()
	require.NoError(t, err)

	partitionSectors, err := builtin.PoStProofWindowPoStPartitionSectors(testWindowPoStProofType)
	require.NoError(t, err)

	info := miner.MinerInfo{
		Owner:                      owner,
		Worker:                     worker,
		PendingWorkerKey:           nil,
		PeerId:                     abi.PeerID("peer"),
		Multiaddrs:                 testMultiaddrs,
		WindowPoStProofType:        testWindowPoStProofType,
		SectorSize:                 sectorSize,
		WindowPoStPartitionSectors: partitionSectors,
	}
	infoCid, err := store.Put(context.Background(), &info)
	require.NoError(t, err)

	state, err := miner.ConstructState(store, infoCid, periodBoundary, 0)
	require.NoError(t, err)

	return &stateHarness{
		t: t,

		s:     state,
		store: store,
	}
}

//
// Type Construction Methods
//

// returns a unique SectorPreCommitOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func newPreCommitOnChain(sectorNo abi.SectorNumber, sealed cid.Cid, deposit abi.TokenAmount, epoch abi.ChainEpoch) *miner.SectorPreCommitOnChainInfo {
	info := newSectorPreCommitInfo(sectorNo, sealed)
	return &miner.SectorPreCommitOnChainInfo{
		Info:               *info,
		PreCommitDeposit:   deposit,
		PreCommitEpoch:     epoch,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
	}
}

// returns a unique SectorOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func newSectorOnChainInfo(sectorNo abi.SectorNumber, sealed cid.Cid, weight big.Int, activation abi.ChainEpoch) *miner.SectorOnChainInfo {
	return &miner.SectorOnChainInfo{
		SectorNumber:          sectorNo,
		SealProof:             abi.RegisteredSealProof_StackedDrg32GiBV1_1,
		SealedCID:             sealed,
		DealIDs:               nil,
		Activation:            activation,
		Expiration:            abi.ChainEpoch(1),
		DealWeight:            weight,
		VerifiedDealWeight:    weight,
		InitialPledge:         abi.NewTokenAmount(0),
		ExpectedDayReward:     abi.NewTokenAmount(0),
		ExpectedStoragePledge: abi.NewTokenAmount(0),
		ReplacedSectorAge:     abi.ChainEpoch(0),
		ReplacedDayReward:     big.Zero(),
	}
}

// returns a unique SectorPreCommitInfo with each invocation with SectorNumber set to `sectorNo`.
func newSectorPreCommitInfo(sectorNo abi.SectorNumber, sealed cid.Cid) *miner.SectorPreCommitInfo {
	return &miner.SectorPreCommitInfo{
		SealProof:     abi.RegisteredSealProof_StackedDrg32GiBV1_1,
		SectorNumber:  sectorNo,
		SealedCID:     sealed,
		SealRandEpoch: abi.ChainEpoch(1),
		DealIDs:       nil,
		Expiration:    abi.ChainEpoch(1),
	}
}
