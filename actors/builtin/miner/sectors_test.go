package miner_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func sectorsArr(t *testing.T, store adt.Store, sectors []*miner.SectorOnChainInfo) miner.Sectors {
	sectorArr := miner.Sectors{adt.MakeEmptyArray(store)}
	require.NoError(t, sectorArr.Store(sectors...))
	return sectorArr
}

func TestSectors(t *testing.T) {
	makeSector := func(t *testing.T, i uint64) *miner.SectorOnChainInfo {
		return &miner.SectorOnChainInfo{
			SectorNumber:          abi.SectorNumber(i),
			SealProof:             abi.RegisteredSealProof_StackedDrg32GiBV1_1,
			SealedCID:             tutil.MakeCID(fmt.Sprintf("commR-%d", i), &miner.SealedCIDPrefix),
			DealWeight:            big.Zero(),
			VerifiedDealWeight:    big.Zero(),
			InitialPledge:         big.Zero(),
			ExpectedDayReward:     big.Zero(),
			ExpectedStoragePledge: big.Zero(),
			ReplacedDayReward:     big.Zero(),
		}
	}
	setupSectors := func(t *testing.T) miner.Sectors {
		return sectorsArr(t, ipld.NewADTStore(context.Background()), []*miner.SectorOnChainInfo{
			makeSector(t, 0), makeSector(t, 1), makeSector(t, 5),
		})
	}

	t.Run("loads sectors", func(t *testing.T) {
		arr := setupSectors(t)
		sectors, err := arr.Load(bf(0, 5))
		require.NoError(t, err)
		require.Len(t, sectors, 2)
		require.Equal(t, makeSector(t, 0), sectors[0])
		require.Equal(t, makeSector(t, 5), sectors[1])

		_, err = arr.Load(bf(0, 3))
		require.Error(t, err)
	})

	t.Run("stores sectors", func(t *testing.T) {
		arr := setupSectors(t)
		s0 := makeSector(t, 0)
		s1 := makeSector(t, 1)
		s1.Activation = 1
		s3 := makeSector(t, 3)
		s5 := makeSector(t, 5)

		require.NoError(t, arr.Store(s3, s1))
		sectors, err := arr.Load(bf(0, 1, 3, 5))
		require.NoError(t, err)
		require.Len(t, sectors, 4)
		require.Equal(t, s0, sectors[0])
		require.Equal(t, s1, sectors[1])
		require.Equal(t, s3, sectors[2])
		require.Equal(t, s5, sectors[3])
	})

	t.Run("loads and stores no sectors", func(t *testing.T) {
		arr := setupSectors(t)

		sectors, err := arr.Load(bf())
		require.NoError(t, err)
		require.Empty(t, sectors)

		require.NoError(t, arr.Store())
	})

	t.Run("gets sectors", func(t *testing.T) {
		arr := setupSectors(t)
		s0, found, err := arr.Get(0)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, makeSector(t, 0), s0)

		_, found, err = arr.Get(3)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("must get", func(t *testing.T) {
		arr := setupSectors(t)
		s0, err := arr.MustGet(0)
		require.NoError(t, err)
		require.Equal(t, makeSector(t, 0), s0)

		_, err = arr.MustGet(3)
		require.Error(t, err)
	})

	t.Run("loads for proof with replacement", func(t *testing.T) {
		arr := setupSectors(t)
		s1 := makeSector(t, 1)
		infos, err := arr.LoadForProof(bf(0, 1), bf(0))
		require.NoError(t, err)
		require.Equal(t, []*miner.SectorOnChainInfo{s1, s1}, infos)
	})

	t.Run("loads for proof without replacement", func(t *testing.T) {
		arr := setupSectors(t)
		s0 := makeSector(t, 0)
		s1 := makeSector(t, 1)
		infos, err := arr.LoadForProof(bf(0, 1), bf())
		require.NoError(t, err)
		require.Equal(t, []*miner.SectorOnChainInfo{s0, s1}, infos)
	})

	t.Run("empty proof", func(t *testing.T) {
		arr := setupSectors(t)
		infos, err := arr.LoadForProof(bf(), bf())
		require.NoError(t, err)
		require.Empty(t, infos)
	})

	t.Run("no non-faulty sectors", func(t *testing.T) {
		arr := setupSectors(t)
		infos, err := arr.LoadForProof(bf(1), bf(1))
		require.NoError(t, err)
		require.Empty(t, infos)
	})
}
