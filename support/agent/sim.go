package agent

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/rt"
	cid "github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	vm2 "github.com/filecoin-project/specs-actors/v2/support/vm"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v5/actors/states"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v5/support/ipld"
	"github.com/filecoin-project/specs-actors/v5/support/vm"
)

// Sim is a simulation framework to exercise actor code in a network-like environment.
// It's goal is to simulate realistic call sequences and interactions to perform invariant analysis
// and test performance assumptions prior to shipping actor code out to implementations.
// The model is that the simulation will "Tick" once per epoch. Within this tick:
// * It will first compute winning tickets from previous state for miners to simulate block mining.
// * It will create any agents it is configured to create and generate messages to create their associated actors.
// * It will call tick on all it agents. This call will return messages that will get added to the simulated "tipset".
// * Messages will be shuffled to simulate network entropy.
// * Messages will be applied and an new VM will be created from the resulting state tree for the next tick.
type Sim struct {
	Config        SimConfig
	Agents        []Agent
	DealProviders []DealProvider
	WinCount      uint64
	MessageCount  uint64

	v                 SimVM
	vmFactory         VMFactoryFunc
	minerStateFactory func(context.Context, cid.Cid) (SimMinerState, error)
	rnd               *rand.Rand
	statsByMethod     map[vm.MethodKey]*vm.CallStats
	blkStore          ipldcbor.IpldBlockstore
	blkStoreFactory   func() ipldcbor.IpldBlockstore
	ctx               context.Context
	t                 testing.TB
}

type VMFactoryFunc func(context.Context, vm2.ActorImplLookup, adt.Store, cid.Cid, abi.ChainEpoch) (SimVM, error)

func NewSim(ctx context.Context, t testing.TB, blockstoreFactory func() ipldcbor.IpldBlockstore, config SimConfig) *Sim {
	blkStore := blockstoreFactory()
	metrics := ipld.NewMetricsBlockStore(blkStore)
	v := vm.NewVMWithSingletons(ctx, t, metrics)
	vmFactory := func(ctx context.Context, impl vm2.ActorImplLookup, store adt.Store, stateRoot cid.Cid, epoch abi.ChainEpoch) (SimVM, error) {
		return vm.NewVMAtEpoch(ctx, vm.ActorImplLookup(impl), store, stateRoot, epoch)
	}
	v.SetStatsSource(metrics)
	minerStateFactory := func(ctx context.Context, root cid.Cid) (SimMinerState, error) {
		return &MinerStateV5{
			Ctx:  ctx,
			Root: root,
		}, nil
	}
	return &Sim{
		Config:            config,
		Agents:            []Agent{},
		DealProviders:     []DealProvider{},
		v:                 v,
		vmFactory:         vmFactory,
		minerStateFactory: minerStateFactory,
		rnd:               rand.New(rand.NewSource(config.Seed)),
		blkStore:          blkStore,
		blkStoreFactory:   blockstoreFactory,
		ctx:               ctx,
		t:                 t,
	}
}

func NewSimWithVM(ctx context.Context, t testing.TB, v SimVM, vmFactory VMFactoryFunc,
	blkStore ipldcbor.IpldBlockstore, blockstoreFactory func() ipldcbor.IpldBlockstore,
	minerStateFactory func(context.Context, cid.Cid) (SimMinerState, error), config SimConfig,
) *Sim {
	metrics := ipld.NewMetricsBlockStore(blkStore)
	v.SetStatsSource(metrics)

	return &Sim{
		Config:            config,
		Agents:            []Agent{},
		DealProviders:     []DealProvider{},
		v:                 v,
		vmFactory:         vmFactory,
		minerStateFactory: minerStateFactory,
		rnd:               rand.New(rand.NewSource(config.Seed)),
		blkStore:          blkStore,
		blkStoreFactory:   blockstoreFactory,
		ctx:               ctx,
		t:                 t,
	}
}

func (s *Sim) SwapVM(v SimVM, vmFactory VMFactoryFunc, minerStateFactory func(context.Context, cid.Cid) (SimMinerState, error),
) {
	s.v = v
	s.vmFactory = vmFactory
	s.minerStateFactory = minerStateFactory
}

//////////////////////////////////////////
//
//  Sim execution
//
//////////////////////////////////////////

func (s *Sim) Tick() error {
	var err error
	var blockMessages []message
	// compute power table before state transition to create block rewards at the end
	powerTable, err := s.computePowerTable(s.v, s.Agents)
	if err != nil {
		return err
	}

	if err := computeCircSupply(s.v); err != nil {
		return err
	}

	// add all agent messages
	for _, agent := range s.Agents {
		msgs, err := agent.Tick(s)
		if err != nil {
			return err
		}

		blockMessages = append(blockMessages, msgs...)
	}

	// shuffle messages
	s.rnd.Shuffle(len(blockMessages), func(i, j int) {
		blockMessages[i], blockMessages[j] = blockMessages[j], blockMessages[i]
	})

	// run messages
	for _, msg := range blockMessages {
		result := s.v.ApplyMessage(msg.From, msg.To, msg.Value, msg.Method, msg.Params)

		// for now, assume everything should work
		if result.Code != exitcode.Ok {
			return errors.Errorf("exitcode %d: message failed: %v\n%s\n", result.Code, msg, strings.Join(s.v.GetLogs(), "\n"))
		}

		if msg.ReturnHandler != nil {
			if err := msg.ReturnHandler(s, msg, result.Ret); err != nil {
				return err
			}
		}
	}
	s.MessageCount += uint64(len(blockMessages))
	// Apply block rewards
	// Note that this differs from the specification in that it applies all reward messages at the end, whereas
	// a real implementation would apply a reward messages at the end of each block in the tipset (thereby
	// interleaving them with the rest of the messages).
	for _, miner := range powerTable.minerPower {
		if powerTable.totalQAPower.GreaterThan(big.Zero()) {
			wins := WinCount(miner.qaPower, powerTable.totalQAPower, s.rnd.Float64())
			s.WinCount += wins
			err := s.rewardMiner(miner.addr, wins)
			if err != nil {
				return err
			}
		}
	}

	// run cron
	result := s.v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	if result.Code != exitcode.Ok {
		return errors.Errorf("exitcode %d: cron message failed:\n%s\n", result.Code, strings.Join(s.v.GetLogs(), "\n"))
	}

	// store last stats
	s.statsByMethod = s.v.GetCallStats()

	// dump logs if we have them
	if len(s.v.GetLogs()) > 0 {
		fmt.Printf("%s\n", strings.Join(s.v.GetLogs(), "\n"))
	}

	// create next vm
	nextEpoch := s.v.GetEpoch() + 1
	if s.Config.CheckpointEpochs > 0 && uint64(nextEpoch)%s.Config.CheckpointEpochs == 0 {
		nextStore := s.blkStoreFactory()
		blks, size, err := BlockstoreCopy(s.blkStore, nextStore, s.v.StateRoot())
		if err != nil {
			return err
		}
		fmt.Printf("CHECKPOINT: state blocks: %d, state data size %d\n", blks, size)

		s.blkStore = nextStore
		metrics := ipld.NewMetricsBlockStore(nextStore)
		s.v, err = s.vmFactory(s.ctx, s.v.GetActorImpls(), adt.WrapBlockStore(s.ctx, metrics), s.v.StateRoot(), nextEpoch)
		if err != nil {
			return err
		}
		s.v.SetStatsSource(metrics)

	} else {
		statsSource := s.v.GetStatsSource()
		s.v, err = s.vmFactory(s.ctx, s.v.GetActorImpls(), s.v.Store(), s.v.StateRoot(), nextEpoch)
		if err != nil {
			return err
		}
		s.v.SetStatsSource(statsSource)
	}

	return err
}

//////////////////////////////////////////////////
//
//  SimState Methods and other accessors
//
//////////////////////////////////////////////////

func (s *Sim) GetEpoch() abi.ChainEpoch {
	return s.v.GetEpoch()
}

func (s *Sim) GetState(addr address.Address, out cbor.Unmarshaler) error {
	return s.v.GetState(addr, out)
}

func (s *Sim) Store() adt.Store {
	return s.v.Store()
}

func (s *Sim) MinerState(addr address.Address) (SimMinerState, error) {
	act, found, err := s.v.GetActor(addr)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, xerrors.Errorf("miner %s not found", addr)
	}
	return s.minerStateFactory(s.ctx, act.Head)
}

func (s *Sim) AddAgent(a Agent) {
	s.Agents = append(s.Agents, a)
}

func (s *Sim) AddDealProvider(d DealProvider) {
	s.DealProviders = append(s.DealProviders, d)
}

func (s *Sim) GetVM() SimVM {
	return s.v
}

func (s *Sim) GetCallStats() map[vm.MethodKey]*vm.CallStats {
	return s.statsByMethod
}

func (s *Sim) ChooseDealProvider() DealProvider {
	if len(s.DealProviders) == 0 {
		return nil
	}
	return s.DealProviders[s.rnd.Int63n(int64(len(s.DealProviders)))]
}

func (s *Sim) NetworkCirculatingSupply() abi.TokenAmount {
	return s.v.GetCirculatingSupply()
}

func (s *Sim) CreateMinerParams(worker, owner address.Address, sealProof abi.RegisteredSealProof) (*power.CreateMinerParams, error) {
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	if err != nil {
		return nil, err
	}

	return &power.CreateMinerParams{
		Owner:               owner,
		Worker:              worker,
		WindowPoStProofType: wPoStProof,
	}, nil

}

//////////////////////////////////////////////////
//
//  Misc Methods
//
//////////////////////////////////////////////////

func (s *Sim) rewardMiner(addr address.Address, wins uint64) error {
	if wins < 1 {
		return nil
	}

	rewardParams := reward.AwardBlockRewardParams{
		Miner:     addr,
		Penalty:   big.Zero(),
		GasReward: big.Zero(),
		WinCount:  int64(wins),
	}
	result := s.v.ApplyMessage(builtin.SystemActorAddr, builtin.RewardActorAddr, big.Zero(), builtin.MethodsReward.AwardBlockReward, &rewardParams)
	if result.Code != exitcode.Ok {
		return errors.Errorf("exitcode %d: reward message failed:\n%s\n", result.Code, strings.Join(s.v.GetLogs(), "\n"))
	}
	return nil
}

func (s *Sim) computePowerTable(v SimVM, agents []Agent) (PowerTable, error) {
	pt := PowerTable{}

	var rwst reward.State
	if err := v.GetState(builtin.RewardActorAddr, &rwst); err != nil {
		return PowerTable{}, err
	}
	pt.blockReward = rwst.ThisEpochReward

	var st power.State
	if err := v.GetState(builtin.StoragePowerActorAddr, &st); err != nil {
		return PowerTable{}, err
	}
	pt.totalQAPower = st.TotalQualityAdjPower

	for _, agent := range agents {
		if miner, ok := agent.(*MinerAgent); ok {
			if claim, found, err := st.GetClaim(v.Store(), miner.IDAddress); err != nil {
				return pt, err
			} else if found {
				if sufficient, err := st.MinerNominalPowerMeetsConsensusMinimum(v.Store(), miner.IDAddress); err != nil {
					return pt, err
				} else if sufficient {
					pt.minerPower = append(pt.minerPower, minerPowerTable{miner.IDAddress, claim.QualityAdjPower})
				}
			}
		}
	}
	return pt, nil
}

func computeCircSupply(v SimVM) error {
	// disbursed + reward.State.TotalStoragePowerReward - burnt.Balance - power.State.TotalPledgeCollateral
	var rewardSt reward.State
	if err := v.GetState(builtin.RewardActorAddr, &rewardSt); err != nil {
		return err
	}

	var powerSt power.State
	if err := v.GetState(builtin.StoragePowerActorAddr, &powerSt); err != nil {
		return err
	}

	burnt, found, err := v.GetActor(builtin.BurntFundsActorAddr)
	if err != nil {
		return err
	} else if !found {
		return errors.Errorf("burnt actor not found at %v", builtin.BurntFundsActorAddr)
	}

	v.SetCirculatingSupply(big.Sum(DisbursedAmount, rewardSt.TotalStoragePowerReward,
		powerSt.TotalPledgeCollateral.Neg(), burnt.Balance.Neg()))
	return nil
}

//////////////////////////////////////////////
//
//  Internal Types
//
//////////////////////////////////////////////

type SimState interface {
	GetEpoch() abi.ChainEpoch
	GetState(addr address.Address, out cbor.Unmarshaler) error
	Store() adt.Store
	AddAgent(a Agent)
	AddDealProvider(d DealProvider)
	NetworkCirculatingSupply() abi.TokenAmount
	MinerState(addr address.Address) (SimMinerState, error)
	CreateMinerParams(worker, owner address.Address, sealProof abi.RegisteredSealProof) (*power.CreateMinerParams, error)

	// randomly select an agent capable of making deals.
	// Returns nil if no providers exist.
	ChooseDealProvider() DealProvider
}

type Agent interface {
	Tick(v SimState) ([]message, error)
}

type DealProvider interface {
	Address() address.Address
	DealRange(currentEpoch abi.ChainEpoch) (start abi.ChainEpoch, end abi.ChainEpoch)
	CreateDeal(proposal market.ClientDealProposal)
	AvailableCollateral() abi.TokenAmount
}

type SimConfig struct {
	AccountCount           int
	AccountInitialBalance  abi.TokenAmount
	Seed                   int64
	CreateMinerProbability float32
	CheckpointEpochs       uint64
}

type returnHandler func(v SimState, msg message, ret cbor.Marshaler) error

type message struct {
	From          address.Address
	To            address.Address
	Value         abi.TokenAmount
	Method        abi.MethodNum
	Params        interface{}
	ReturnHandler returnHandler
}

type minerPowerTable struct {
	addr    address.Address
	qaPower abi.StoragePower
}

type PowerTable struct {
	blockReward  abi.TokenAmount
	totalQAPower abi.StoragePower
	minerPower   []minerPowerTable
}

// VM interface allowing a simulation to operate over multiple VM versions
type SimVM interface {
	ApplyMessage(from, to address.Address, value abi.TokenAmount, method abi.MethodNum, params interface{}) vm.MessageResult
	GetCirculatingSupply() abi.TokenAmount
	GetLogs() []string
	GetState(addr address.Address, out cbor.Unmarshaler) error
	SetStatsSource(stats vm2.StatsSource)
	GetCallStats() map[vm2.MethodKey]*vm2.CallStats
	GetEpoch() abi.ChainEpoch
	Store() adt2.Store
	GetActor(addr address.Address) (*states.Actor, bool, error)
	SetCirculatingSupply(supply big.Int)
	GetActorImpls() map[cid.Cid]rt.VMActor
	StateRoot() cid.Cid
	GetStatsSource() vm2.StatsSource
	GetTotalActorBalance() (abi.TokenAmount, error)
}

var _ SimVM = (*vm.VM)(nil)

type SimMinerState interface {
	HasSectorNo(adt.Store, abi.SectorNumber) (bool, error)
	FindSector(adt.Store, abi.SectorNumber) (uint64, uint64, error)
	ProvingPeriodStart(adt.Store) (abi.ChainEpoch, error)
	LoadSectorInfo(adt.Store, uint64) (SimSectorInfo, error)
	DeadlineInfo(adt.Store, abi.ChainEpoch) (*dline.Info, error)
	FeeDebt(adt.Store) (abi.TokenAmount, error)
	LoadDeadlineState(adt.Store, uint64) (SimDeadlineState, error)
}

type SimSectorInfo interface {
	Expiration() abi.ChainEpoch
}

type SimDeadlineState interface {
	LoadPartition(adt.Store, uint64) (SimPartitionState, error)
}

type SimPartitionState interface {
	Terminated() bitfield.BitField
}
