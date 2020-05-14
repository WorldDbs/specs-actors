package miner

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpirations(t *testing.T) {
	quant := QuantSpec{unit: 10, offset: 3}
	sectors := []*SectorOnChainInfo{
		testSector(7, 1, 0, 0, 0),
		testSector(8, 2, 0, 0, 0),
		testSector(14, 3, 0, 0, 0),
		testSector(13, 4, 0, 0, 0),
	}
	result := groupNewSectorsByDeclaredExpiration(2048, sectors, quant)
	expected := []*sectorEpochSet{{
		epoch:   13,
		sectors: []uint64{1, 2, 4},
		power:   NewPowerPair(big.NewIntUnsigned(2048*3), big.NewIntUnsigned(2048*3)),
		pledge:  big.Zero(),
	}, {
		epoch:   23,
		sectors: []uint64{3},
		power:   NewPowerPair(big.NewIntUnsigned(2048), big.NewIntUnsigned(2048)),
		pledge:  big.Zero(),
	}}
	require.Equal(t, len(expected), len(result))
	for i, ex := range expected {
		assertSectorSet(t, ex, &result[i])
	}
}

func TestExpirationsEmpty(t *testing.T) {
	sectors := []*SectorOnChainInfo{}
	result := groupNewSectorsByDeclaredExpiration(2048, sectors, NoQuantization)
	expected := []sectorEpochSet{}
	require.Equal(t, expected, result)
}

func assertSectorSet(t *testing.T, expected, actual *sectorEpochSet) {
	assert.Equal(t, expected.epoch, actual.epoch)
	assert.Equal(t, expected.sectors, actual.sectors)
	assert.True(t, expected.power.Equals(actual.power), "expected %v, actual %v", expected.power, actual.power)
	assert.True(t, expected.pledge.Equals(actual.pledge), "expected %v, actual %v", expected.pledge, actual.pledge)
}

func testSector(expiration, number, weight, vweight, pledge int64) *SectorOnChainInfo {
	return &SectorOnChainInfo{
		Expiration:         abi.ChainEpoch(expiration),
		SectorNumber:       abi.SectorNumber(number),
		DealWeight:         big.NewInt(weight),
		VerifiedDealWeight: big.NewInt(vweight),
		InitialPledge:      abi.NewTokenAmount(pledge),
	}
}
