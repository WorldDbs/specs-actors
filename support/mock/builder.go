package mock

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/ipfs/go-cid"
	"github.com/minio/blake2b-simd"
)

// Build for fluent initialization of a mock runtime.
type RuntimeBuilder struct {
	options []func(rt *Runtime)
}

// Initializes a new builder with a receiving actor address.
func NewBuilder(receiver addr.Address) RuntimeBuilder {
	var b RuntimeBuilder
	b.add(func(rt *Runtime) {
		rt.receiver = receiver
	})
	return b
}

// Builds a new runtime object with the configured values.
func (b RuntimeBuilder) Build(t testing.TB) *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	m := &Runtime{
		ctx:               ctx,
		epoch:             0,
		networkVersion:    network.VersionMax,
		caller:            addr.Address{},
		callerType:        cid.Undef,
		miner:             addr.Address{},
		idAddresses:       make(map[addr.Address]addr.Address),
		circulatingSupply: abi.NewTokenAmount(0),

		state:    cid.Undef,
		store:    make(map[cid.Cid][]byte),
		hashfunc: blake2b.Sum256,

		balance:       abi.NewTokenAmount(0),
		valueReceived: abi.NewTokenAmount(0),

		actorCodeCIDs: make(map[addr.Address]cid.Cid),
		newActorAddr:  addr.Undef,

		t:                        t,
		expectValidateCallerAny:  false,
		expectValidateCallerAddr: nil,
		expectValidateCallerType: nil,
		expectCreateActor:        nil,

		expectSends:      make([]*expectedMessage, 0),
		expectVerifySigs: make([]*expectVerifySig, 0),
	}
	for _, opt := range b.options {
		opt(m)
	}
	return m
}

func (b *RuntimeBuilder) add(cb func(*Runtime)) {
	b.options = append(b.options, cb)
}

func (b RuntimeBuilder) WithEpoch(epoch abi.ChainEpoch) RuntimeBuilder {
	b.add(func(rt *Runtime) {
		rt.epoch = epoch
	})
	return b
}

func (b RuntimeBuilder) WithCaller(address addr.Address, code cid.Cid) RuntimeBuilder {
	b.add(func(rt *Runtime) {
		rt.caller = address
		rt.callerType = code
	})
	return b
}

func (b RuntimeBuilder) WithMiner(miner addr.Address) RuntimeBuilder {
	b.add(func(rt *Runtime) {
		rt.miner = miner
	})
	return b
}

func (b RuntimeBuilder) WithBalance(balance, received abi.TokenAmount) RuntimeBuilder {
	b.add(func(rt *Runtime) {
		rt.balance = balance
		rt.valueReceived = received
	})
	return b
}

func (b RuntimeBuilder) WithNetworkVersion(version network.Version) RuntimeBuilder {
	b.add(func(rt *Runtime) {
		rt.networkVersion = version
	})
	return b
}

func (b RuntimeBuilder) WithActorType(addr addr.Address, code cid.Cid) RuntimeBuilder {
	b.add(func(rt *Runtime) {
		rt.actorCodeCIDs[addr] = code
	})
	return b
}

func (b RuntimeBuilder) WithHasher(f func(data []byte) [32]byte) RuntimeBuilder {
	b.add(func(rt *Runtime) {
		rt.hashfunc = f
	})
	return b
}
