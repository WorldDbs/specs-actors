package miner

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
)

func TestCompactionWindow(t *testing.T) {
	periodStart := abi.ChainEpoch(1024)
	dlInfo := NewDeadlineInfo(periodStart, 0, 0)
	assert.True(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Open-WPoStChallengeWindow-1),
		"compaction is possible up till the blackout period")
	assert.False(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Open-WPoStChallengeWindow),
		"compaction is not possible during the prior window")

	assert.False(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Open+10),
		"compaction is not possible during the window")

	assert.False(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Close),
		"compaction is not possible immediately after the window")

	assert.False(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Last()+WPoStDisputeWindow),
		"compaction is not possible before the proof challenge period has passed")

	assert.True(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Close+WPoStDisputeWindow),
		"compaction is possible after the proof challenge period has passed")

	assert.True(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Open+WPoStProvingPeriod-WPoStChallengeWindow-1),
		"compaction remains possible until the next blackout")
	assert.False(t, deadlineAvailableForCompaction(periodStart, 0, dlInfo.Open+WPoStProvingPeriod-WPoStChallengeWindow),
		"compaction is not possible during the next blackout")
}

func TestChallengeWindow(t *testing.T) {
	periodStart := abi.ChainEpoch(1024)
	dlInfo := NewDeadlineInfo(periodStart, 0, 0)
	assert.False(t, deadlineAvailableForOptimisticPoStDispute(periodStart, 0, dlInfo.Open),
		"proof challenge is not possible while the window is open")
	assert.True(t, deadlineAvailableForOptimisticPoStDispute(periodStart, 0, dlInfo.Close),
		"proof challenge is possible after the window is closes")
	assert.True(t, deadlineAvailableForOptimisticPoStDispute(periodStart, 0, dlInfo.Close+WPoStDisputeWindow-1),
		"proof challenge is possible until the proof challenge period has passed")
	assert.False(t, deadlineAvailableForOptimisticPoStDispute(periodStart, 0, dlInfo.Close+WPoStDisputeWindow),
		"proof challenge is not possible after the proof challenge period has passed")
}
