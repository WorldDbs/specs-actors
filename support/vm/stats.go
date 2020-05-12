package vm_test

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
)

type StatsSource interface {
	WriteCount() uint64
	ReadCount() uint64
	WriteSize() uint64
	ReadSize() uint64
}

type StatsByCall map[MethodKey]*CallStats

type MethodKey struct {
	Code   cid.Cid
	Method abi.MethodNum
}

func (sbc StatsByCall) MergeStats(code cid.Cid, methodNum abi.MethodNum, newStats *CallStats) {
	key := MethodKey{code, methodNum}
	stats, ok := sbc[key]
	if !ok {
		sbc[key] = newStats
	} else {
		stats.MergeStats(newStats)
	}
}

func (sbc StatsByCall) MergeAllStats(other StatsByCall) {
	for key, stats := range other { // nolint:nomaprange
		sbc.MergeStats(key.Code, key.Method, stats)
	}
}

type CallStats struct {
	Reads       uint64
	Writes      uint64
	ReadBytes   uint64
	WriteBytes  uint64
	Calls       uint64
	statsSource StatsSource
	SubStats    StatsByCall

	startReads      uint64
	startWrites     uint64
	startReadBytes  uint64
	startWriteBytes uint64
}

func NewCallStats(statsSource StatsSource) *CallStats {
	var startReads, startWrites, startReadBytes, startWriteBytes uint64
	if statsSource != nil {
		startReads = statsSource.ReadCount()
		startWrites = statsSource.WriteCount()
		startReadBytes = statsSource.ReadSize()
		startWriteBytes = statsSource.WriteSize()
	}
	return &CallStats{
		Reads:           0,
		Writes:          0,
		ReadBytes:       0,
		WriteBytes:      0,
		Calls:           0,
		statsSource:     statsSource,
		SubStats:        nil,
		startReads:      startReads,
		startWrites:     startWrites,
		startReadBytes:  startReadBytes,
		startWriteBytes: startWriteBytes,
	}
}

func (s *CallStats) Capture() {
	if s.statsSource == nil {
		return
	}

	s.Calls++
	s.Writes = s.statsSource.WriteCount() - s.startWrites
	s.Reads = s.statsSource.ReadCount() - s.startReads
	s.WriteBytes = s.statsSource.WriteSize() - s.startWriteBytes
	s.ReadBytes = s.statsSource.ReadSize() - s.startReadBytes
}

// assume stats have same method type and that other will be discarded after this call
func (s *CallStats) MergeStats(other *CallStats) {
	s.Calls += other.Calls
	s.Reads += other.Reads
	s.Writes += other.Writes
	s.WriteBytes += other.WriteBytes
	s.ReadBytes += other.WriteBytes

	if other.SubStats == nil {
		return
	}

	if s.SubStats == nil {
		s.SubStats = other.SubStats
		return
	}

	for method, stats := range other.SubStats { // nolint:nomaprange
		sub, ok := s.SubStats[method]
		if !ok {
			s.SubStats[method] = stats
		} else {
			sub.MergeStats(stats)
		}
	}
}

func (s *CallStats) MergeSubStat(code cid.Cid, methodNum abi.MethodNum, newStats *CallStats) {
	if s.SubStats == nil {
		s.SubStats = make(StatsByCall)
	}
	s.SubStats.MergeStats(code, methodNum, newStats)
}
