package agent

import (
	"crypto/sha256"
	"math/bits"
	"math/rand"
	"strconv"

	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/reward"
	"github.com/ipfs/go-cid"
)

type DealClientConfig struct {
	DealRate         float64         // deals made per epoch
	MinPieceSize     uint64          // minimum piece size (actual piece size be rounded up to power of 2)
	MaxPieceSize     uint64          // maximum piece size
	MinStoragePrice  abi.TokenAmount // minimum price per epoch a client will pay for storage (may be zero)
	MaxStoragePrice  abi.TokenAmount // maximum price per epoch a client will pay for storage
	MinMarketBalance abi.TokenAmount // balance below which client will top up funds in market actor
	MaxMarketBalance abi.TokenAmount // balance to which client will top up funds in market actor
}

type DealClientAgent struct {
	DealCount int

	account    address.Address
	config     DealClientConfig
	dealEvents *RateIterator
	rnd        *rand.Rand

	// tracks funds expected to be locked for client deal payment
	expectedMarketBalance abi.TokenAmount
}

func AddDealClientsForAccounts(s SimState, accounts []address.Address, seed int64, config DealClientConfig) []*DealClientAgent {
	rnd := rand.New(rand.NewSource(seed))
	var agents []*DealClientAgent
	for _, account := range accounts {
		agent := NewDealClientAgent(account, rnd.Int63(), config)
		agents = append(agents, agent)
		s.AddAgent(agent)
	}
	return agents
}

func NewDealClientAgent(account address.Address, seed int64, config DealClientConfig) *DealClientAgent {
	rnd := rand.New(rand.NewSource(seed))
	return &DealClientAgent{
		account:               account,
		config:                config,
		rnd:                   rnd,
		expectedMarketBalance: big.Zero(),
		dealEvents:            NewRateIterator(config.DealRate, rnd.Int63()),
	}
}

func (dca *DealClientAgent) Tick(s SimState) ([]message, error) {
	// aggregate all deals into one message
	if err := dca.dealEvents.Tick(func() error {
		provider := s.ChooseDealProvider()

		// provider will be nil if called before any miners are added to system
		if provider == nil {
			return nil
		}

		if err := dca.createDeal(s, provider); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// add message to update market balance if necessary
	messages := dca.updateMarketBalance()

	return messages, nil
}

func (dca *DealClientAgent) updateMarketBalance() []message {
	if dca.expectedMarketBalance.GreaterThanEqual(dca.config.MinMarketBalance) {
		return []message{}
	}

	balanceToAdd := big.Sub(dca.config.MaxMarketBalance, dca.expectedMarketBalance)

	return []message{{
		From:   dca.account,
		To:     builtin.StorageMarketActorAddr,
		Value:  balanceToAdd,
		Method: builtin.MethodsMarket.AddBalance,
		Params: &dca.account,
		ReturnHandler: func(v SimState, msg message, ret cbor.Marshaler) error {
			dca.expectedMarketBalance = dca.config.MaxMarketBalance
			return nil
		},
	}}
}

// Create a proposal
// Return false if provider if deal can't be performed because client or provider lacks funds
func (dca *DealClientAgent) createDeal(s SimState, provider DealProvider) error {
	pieceCid, err := dca.generatePieceCID()
	if err != nil {
		return err
	}

	pieceSize := dca.config.MinPieceSize +
		uint64(dca.rnd.Int63n(int64(dca.config.MaxPieceSize-dca.config.MinPieceSize)))

	// round to next power of two
	if bits.OnesCount64(pieceSize) > 1 {
		pieceSize = 1 << bits.Len64(pieceSize)
	}

	providerCollateral, err := calculateProviderCollateral(s, abi.PaddedPieceSize(pieceSize))
	if err != nil {
		return err
	}

	// if provider does not have enough collateral, just skip this deal
	if provider.AvailableCollateral().LessThan(providerCollateral) {
		return nil
	}

	// storage price is uniformly distributed between min an max
	price := big.Add(dca.config.MinStoragePrice,
		big.NewInt(dca.rnd.Int63n(int64(big.Sub(dca.config.MaxStoragePrice, dca.config.MinStoragePrice).Uint64()))))

	// deal start is earliest possible epoch, deal end is uniformly distributed between start and max.
	dealStart, maxDealEnd := provider.DealRange(s.GetEpoch())
	dealEnd := dealStart + abi.ChainEpoch(dca.rnd.Int63n(int64(maxDealEnd-dealStart)))
	if dealEnd-dealStart < market.DealMinDuration {
		dealEnd = dealStart + market.DealMinDuration
	}

	// lower expected balance in anticipation of market actor locking storage fee
	storageFee := big.Mul(big.NewInt(int64(dealEnd-dealStart)), price)

	// if this client does not have enough balance for storage fee, just skip this deal
	if dca.expectedMarketBalance.LessThan(storageFee) {
		return nil
	}

	dca.expectedMarketBalance = big.Sub(dca.expectedMarketBalance, storageFee)

	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            abi.PaddedPieceSize(pieceSize),
		VerifiedDeal:         false,
		Client:               dca.account,
		Provider:             provider.Address(),
		Label:                dca.account.String() + ":" + strconv.Itoa(dca.DealCount),
		StartEpoch:           dealStart,
		EndEpoch:             dealEnd,
		StoragePricePerEpoch: price,
		ProviderCollateral:   providerCollateral,
		ClientCollateral:     big.Zero(),
	}

	provider.CreateDeal(market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: crypto.Signature{Type: crypto.SigTypeBLS},
	})
	dca.DealCount++
	return nil
}

func (dca *DealClientAgent) generatePieceCID() (cid.Cid, error) {
	data := make([]byte, 10)
	if _, err := dca.rnd.Read(data); err != nil {
		return cid.Cid{}, err
	}

	sum := sha256.Sum256(data)
	hash, err := mh.Encode(sum[:], market.PieceCIDPrefix.MhType)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(market.PieceCIDPrefix.Codec, hash), nil
}

// Always choose the minimum collateral. This appears to be realistic, and there's is not an obvious way to model a
// more complex distribution.
func calculateProviderCollateral(s SimState, pieceSize abi.PaddedPieceSize) (abi.TokenAmount, error) {
	var powerSt power.State
	if err := s.GetState(builtin.StoragePowerActorAddr, &powerSt); err != nil {
		return big.Zero(), err
	}

	var rewardSt reward.State
	if err := s.GetState(builtin.RewardActorAddr, &rewardSt); err != nil {
		return big.Zero(), err
	}

	min, _ := market.DealProviderCollateralBounds(pieceSize, false, powerSt.TotalRawBytePower,
		powerSt.TotalQualityAdjPower, rewardSt.ThisEpochBaselinePower, s.NetworkCirculatingSupply())
	return min, nil
}
