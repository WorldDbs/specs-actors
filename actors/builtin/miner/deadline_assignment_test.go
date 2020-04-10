package miner

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadlineAssignment(t *testing.T) {
	const partitionSize = 4

	type deadline struct {
		liveSectors, deadSectors uint64
		expectSectors            []uint64
	}

	type testCase struct {
		sectors   uint64
		deadlines [WPoStPeriodDeadlines]*deadline
	}

	testCases := []testCase{{
		// Even assignment and striping.
		sectors: 10,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				expectSectors: []uint64{
					0, 1, 2, 3,
					8, 9,
				},
			},
			1: {
				expectSectors: []uint64{
					4, 5, 6, 7,
				},
			},
		},
	}, {
		// Fill non-full first
		sectors: 5,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				expectSectors: []uint64{3, 4},
			},
			1: {}, // expect nothing.
			3: {
				liveSectors:   1,
				expectSectors: []uint64{0, 1, 2},
			},
		},
	}, {
		// Assign to deadline with least number of live partitions.
		sectors: 1,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				// 2 live partitions. +1 would add another.
				liveSectors: 8,
			},
			1: {
				// 2 live partitions. +1 wouldn't add another.
				// 1 dead partition.
				liveSectors:   7,
				deadSectors:   5,
				expectSectors: []uint64{0},
			},
		},
	}, {
		// Avoid increasing max partitions. Both deadlines have the same
		// number of partitions post-compaction, but deadline 1 has
		// fewer pre-compaction.
		sectors: 1,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				// one live, one dead.
				liveSectors: 4,
				deadSectors: 4,
			},
			1: {
				// 1 live partitions. +1 would add another.
				liveSectors:   4,
				expectSectors: []uint64{0},
			},
		},
	}, {
		// With multiple open partitions, assign to most full first.
		sectors: 1,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				liveSectors: 1,
			},
			1: {
				liveSectors:   2,
				expectSectors: []uint64{0},
			},
		},
	}, {
		// dead sectors also count
		sectors: 1,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				liveSectors: 1,
			},
			1: {
				deadSectors:   2,
				expectSectors: []uint64{0},
			},
		},
	}, {
		// dead sectors really do count.
		sectors: 1,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				deadSectors: 1,
			},
			1: {
				deadSectors:   2,
				expectSectors: []uint64{0},
			},
		},
	}, {
		// when partitions are equally full, assign based on live sectors.
		sectors: 1,
		deadlines: [WPoStPeriodDeadlines]*deadline{
			0: {
				liveSectors: 1,
				deadSectors: 1,
			},
			1: {
				deadSectors:   2,
				expectSectors: []uint64{0},
			},
		},
	}}

	for _, tc := range testCases {
		var deadlines [WPoStPeriodDeadlines]*Deadline
		for i := range deadlines {
			dl := tc.deadlines[i]
			if dl == nil {
				// blackout
				continue
			}
			deadlines[i] = &Deadline{
				LiveSectors:  dl.liveSectors,
				TotalSectors: dl.liveSectors + dl.deadSectors,
			}
		}
		sectors := make([]*SectorOnChainInfo, tc.sectors)
		for i := range sectors {
			sectors[i] = &SectorOnChainInfo{SectorNumber: abi.SectorNumber(i)}
		}
		assignment := assignDeadlines(partitionSize, &deadlines, sectors)
		for i, sectors := range assignment {
			dl := tc.deadlines[i]
			// blackout?
			if dl == nil {
				assert.Empty(t, sectors, "expected no sectors to have been assigned to blacked out deadline")
				continue
			}
			require.Equal(t, len(dl.expectSectors), len(sectors), "for deadline %d", i)

			for i, expectedSectorNo := range dl.expectSectors {
				assert.Equal(t, uint64(sectors[i].SectorNumber), expectedSectorNo)
			}
		}
	}
}
