package miner

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadlineAssignment(t *testing.T) {
	const partitionSize = 4
	const maxPartitions = 100

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
		assignment, err := assignDeadlines(maxPartitions, partitionSize, &deadlines, sectors)
		require.NoError(t, err)
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

func TestMaxPartitionsPerDeadline(t *testing.T) {
	const maxPartitions = 5
	const partitionSize = 5

	t.Run("fails if all deadlines hit their max partitions limit before assigning all sectors to deadlines", func(T *testing.T) {
		// one deadline can take 5 * 5 = 25 sectors
		// so 48 deadlines can take 48 * 25 = 1200 sectors.
		// Hence, we should fail if we try to assign 1201 sectors.

		var deadlines [WPoStPeriodDeadlines]*Deadline
		for i := range deadlines {
			deadlines[i] = &Deadline{
				LiveSectors:  0,
				TotalSectors: 0,
			}
		}

		sectors := make([]*SectorOnChainInfo, 1201)
		for i := range sectors {
			sectors[i] = &SectorOnChainInfo{SectorNumber: abi.SectorNumber(i)}
		}

		_, err := assignDeadlines(maxPartitions, partitionSize, &deadlines, sectors)
		require.Error(t, err)
	})

	t.Run("succeeds if all all deadlines hit their max partitions limit but assignment is complete", func(t *testing.T) {
		// one deadline can take 5 * 5 = 25 sectors
		// so 48 deadlines that can take 48 * 25 = 1200 sectors.
		var deadlines [WPoStPeriodDeadlines]*Deadline
		for i := range deadlines {
			deadlines[i] = &Deadline{
				LiveSectors:  0,
				TotalSectors: 0,
			}
		}

		sectors := make([]*SectorOnChainInfo, 1200)
		for i := range sectors {
			sectors[i] = &SectorOnChainInfo{SectorNumber: abi.SectorNumber(i)}
		}

		deadlineToSectors, err := assignDeadlines(maxPartitions, partitionSize, &deadlines, sectors)
		require.NoError(t, err)

		for _, sectors := range deadlineToSectors {
			require.Len(t, sectors, 25) // there should be (1200/48) = 25 sectors per deadline
		}
	})

	t.Run("fails if some deadlines have sectors beforehand and all deadlines hit their max partition limit", func(t *testing.T) {
		var deadlines [WPoStPeriodDeadlines]*Deadline
		for i := range deadlines {
			deadlines[i] = &Deadline{
				LiveSectors:  1,
				TotalSectors: 2,
			}
		}

		// can only take 1200 - (2 * 48) = 1104 sectors

		sectors := make([]*SectorOnChainInfo, 1105)
		for i := range sectors {
			sectors[i] = &SectorOnChainInfo{SectorNumber: abi.SectorNumber(i)}
		}

		_, err := assignDeadlines(maxPartitions, partitionSize, &deadlines, sectors)
		require.Error(t, err)
	})
}
