package agent

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/pkg/errors"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
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

	v               *vm.VM
	rnd             *rand.Rand
	statsByMethod   map[vm.MethodKey]*vm.CallStats
	blkStore        ipldcbor.IpldBlockstore
	blkStoreFactory func() ipldcbor.IpldBlockstore
	ctx             context.Context
	t               testing.TB
}

func NewSim(ctx context.Context, t testing.TB, blockstoreFactory func() ipldcbor.IpldBlockstore, config SimConfig) *Sim {
	blkStore := blockstoreFactory()
	metrics := ipld.NewMetricsBlockStore(blkStore)
	v := vm.NewVMWithSingletons(ctx, t, metrics)
	v.SetStatsSource(metrics)

	return &Sim{
		Config:          config,
		Agents:          []Agent{},
		DealProviders:   []DealProvider{},
		v:               v,
		rnd:             rand.New(rand.NewSource(config.Seed)),
		blkStore:        blkStore,
		blkStoreFactory: blockstoreFactory,
		ctx:             ctx,
		t:               t,
	}
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
	powerTable, err := computePowerTable(s.v, s.Agents)
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
		ret, code := s.v.ApplyMessage(msg.From, msg.To, msg.Value, msg.Method, msg.Params)

		// for now, assume everything should work
		if code != exitcode.Ok {
			return errors.Errorf("exitcode %d: message failed: %v\n%s\n", code, msg, strings.Join(s.v.GetLogs(), "\n"))
		}

		if msg.ReturnHandler != nil {
			if err := msg.ReturnHandler(s, msg, ret); err != nil {
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
	_, code := s.v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	if code != exitcode.Ok {
		return errors.Errorf("exitcode %d: cron message failed:\n%s\n", code, strings.Join(s.v.GetLogs(), "\n"))
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
		nextStore := ipld.NewBlockStoreInMemory()
		blks, size, err := BlockstoreCopy(s.blkStore, nextStore, s.v.StateRoot())
		if err != nil {
			return err
		}
		fmt.Printf("CHECKPOINT: state blocks: %d, state data size %d\n", blks, size)

		s.blkStore = nextStore
		metrics := ipld.NewMetricsBlockStore(nextStore)
		s.v, err = vm.NewVMAtEpoch(s.ctx, s.v.ActorImpls, adt.WrapStore(s.ctx, ipldcbor.NewCborStore(metrics)), s.v.StateRoot(), nextEpoch)
		if err != nil {
			return err
		}
		s.v.SetStatsSource(metrics)

	} else {
		s.v, err = s.v.WithEpoch(nextEpoch)
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

func (s *Sim) AddAgent(a Agent) {
	s.Agents = append(s.Agents, a)
}

func (s *Sim) AddDealProvider(d DealProvider) {
	s.DealProviders = append(s.DealProviders, d)
}

func (s *Sim) GetVM() *vm.VM {
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
	_, code := s.v.ApplyMessage(builtin.SystemActorAddr, builtin.RewardActorAddr, big.Zero(), builtin.MethodsReward.AwardBlockReward, &rewardParams)
	if code != exitcode.Ok {
		return errors.Errorf("exitcode %d: reward message failed:\n%s\n", code, strings.Join(s.v.GetLogs(), "\n"))
	}
	return nil
}

func computePowerTable(v *vm.VM, agents []Agent) (powerTable, error) {
	pt := powerTable{}

	var rwst reward.State
	if err := v.GetState(builtin.RewardActorAddr, &rwst); err != nil {
		return powerTable{}, err
	}
	pt.blockReward = rwst.ThisEpochReward

	var st power.State
	if err := v.GetState(builtin.StoragePowerActorAddr, &st); err != nil {
		return powerTable{}, err
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

func computeCircSupply(v *vm.VM) error {
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

type powerTable struct {
	blockReward  abi.TokenAmount
	totalQAPower abi.StoragePower
	minerPower   []minerPowerTable
}
