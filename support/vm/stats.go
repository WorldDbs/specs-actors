package vm_test

import (
	vm2 "github.com/filecoin-project/specs-actors/v2/support/vm"
)

// type StatsSource interface {
// 	WriteCount() uint64
// 	ReadCount() uint64
// 	WriteSize() uint64
// 	ReadSize() uint64
// }
type StatsSource = vm2.StatsSource

// type StatsByCall map[MethodKey]*CallStats
type StatsByCall = vm2.StatsByCall

// type MethodKey struct {
// 	Code   cid.Cid
// 	Method abi.MethodNum
// }
type MethodKey = vm2.MethodKey

// type CallStats struct {
// 	Reads       uint64
// 	Writes      uint64
// 	ReadBytes   uint64
// 	WriteBytes  uint64
// 	Calls       uint64
// 	statsSource StatsSource
// 	SubStats    StatsByCall

// 	startReads      uint64
// 	startWrites     uint64
// 	startReadBytes  uint64
// 	startWriteBytes uint64
// }
type CallStats = vm2.CallStats
