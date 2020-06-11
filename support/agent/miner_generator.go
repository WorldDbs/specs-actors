package agent

import (
	"math/rand"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/pkg/errors"

	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/power"
	power3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/power"
)

// MinerGenerator adds miner agents to the simulation at a configured rate.
// When triggered to add a new miner, it:
// * Selects the next owner address from the accounts it has been given.
// * Sends a createMiner message from that account
// * Handles the response by creating a MinerAgent with MinerAgentConfig and registering it in the sim.
type MinerGenerator struct {
	config            MinerAgentConfig // eventually this should become a set of probabilities to support miner differentiation
	createMinerEvents *RateIterator
	minersCreated     int
	accounts          []address.Address
	rnd               *rand.Rand
}

func NewMinerGenerator(accounts []address.Address, config MinerAgentConfig, createMinerRate float64, rndSeed int64) *MinerGenerator {
	rnd := rand.New(rand.NewSource(rndSeed))
	return &MinerGenerator{
		config:            config,
		createMinerEvents: NewRateIterator(createMinerRate, rnd.Int63()),
		accounts:          accounts,
		rnd:               rnd,
	}
}

func (mg *MinerGenerator) Tick(s SimState) ([]message, error) {
	var msgs []message
	if mg.minersCreated >= len(mg.accounts) {
		return msgs, nil
	}

	err := mg.createMinerEvents.Tick(func() error {
		if mg.minersCreated < len(mg.accounts) {
			addr := mg.accounts[mg.minersCreated]
			mg.minersCreated++
			msg, err := mg.createMiner(addr, mg.config, s)
			if err != nil {
				return err
			}
			msgs = append(msgs, msg)
		}
		return nil
	})
	return msgs, err
}

func (mg *MinerGenerator) createMiner(owner address.Address, cfg MinerAgentConfig, s SimState) (message, error) {
	params, err := s.CreateMinerParams(owner, owner, cfg.ProofType)
	if err != nil {
		return message{}, err
	}
	return message{
		From:   owner,
		To:     builtin.StoragePowerActorAddr,
		Value:  mg.config.StartingBalance, // miner gets all account funds
		Method: builtin.MethodsPower.CreateMiner,
		Params: params,
		ReturnHandler: func(s SimState, msg message, ret cbor.Marshaler) error {
			createMinerRet, ok := ret.(*power.CreateMinerReturn)
			if !ok {
				return errors.Errorf("create miner return has wrong type: %v", ret)
			}

			var worker, owner address.Address
			params, okV3 := msg.Params.(*power3.CreateMinerParams)
			if !okV3 {
				params, okV2 := msg.Params.(*power2.CreateMinerParams)
				if !okV2 {
					return errors.Errorf("create miner params has wrong type: %v", msg.Params)
				}
				worker = params.Worker
				owner = params.Owner
			} else {
				worker = params.Worker
				owner = params.Owner
			}

			// register agent as both a miner and deal provider
			minerAgent := NewMinerAgent(owner, worker, createMinerRet.IDAddress, createMinerRet.RobustAddress, mg.rnd.Int63(), cfg)
			s.AddAgent(minerAgent)
			s.AddDealProvider(minerAgent)
			return nil
		},
	}, nil
}
