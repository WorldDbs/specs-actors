package market

import (
	"bytes"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/ipfs/go-cid"
	xerrors "golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
)

const epochUndefined = abi.ChainEpoch(-1)

// BalanceLockingReason is the reason behind locking an amount.
type BalanceLockingReason int

const (
	ClientCollateral BalanceLockingReason = iota
	ClientStorageFee
	ProviderCollateral
)

// Bitwidth of AMTs determined empirically from mutation patterns and projections of mainnet data.
const ProposalsAmtBitwidth = 5
const StatesAmtBitwidth = 6

type State struct {
	// Proposals are deals that have been proposed and not yet cleaned up after expiry or termination.
	Proposals cid.Cid // AMT[DealID]DealProposal
	// States contains state for deals that have been activated and not yet cleaned up after expiry or termination.
	// After expiration, the state exists until the proposal is cleaned up too.
	// Invariant: keys(States) ⊆ keys(Proposals).
	States cid.Cid // AMT[DealID]DealState

	// PendingProposals tracks dealProposals that have not yet reached their deal start date.
	// We track them here to ensure that miners can't publish the same deal proposal twice
	PendingProposals cid.Cid // Set[DealCid]

	// Total amount held in escrow, indexed by actor address (including both locked and unlocked amounts).
	EscrowTable cid.Cid // BalanceTable

	// Amount locked, indexed by actor address.
	// Note: the amounts in this table do not affect the overall amount in escrow:
	// only the _portion_ of the total escrow amount that is locked.
	LockedTable cid.Cid // BalanceTable

	NextID abi.DealID

	// Metadata cached for efficient iteration over deals.
	DealOpsByEpoch cid.Cid // SetMultimap, HAMT[epoch]Set
	LastCron       abi.ChainEpoch

	// Total Client Collateral that is locked -> unlocked when deal is terminated
	TotalClientLockedCollateral abi.TokenAmount
	// Total Provider Collateral that is locked -> unlocked when deal is terminated
	TotalProviderLockedCollateral abi.TokenAmount
	// Total storage fee that is locked in escrow -> unlocked when payments are made
	TotalClientStorageFee abi.TokenAmount
}

func ConstructState(store adt.Store) (*State, error) {
	emptyProposalsArrayCid, err := adt.StoreEmptyArray(store, ProposalsAmtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty array: %w", err)
	}
	emptyStatesArrayCid, err := adt.StoreEmptyArray(store, StatesAmtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty states array: %w", err)
	}

	emptyPendingProposalsMapCid, err := adt.StoreEmptyMap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty map: %w", err)
	}
	emptyDealOpsHamtCid, err := StoreEmptySetMultimap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty multiset: %w", err)
	}
	emptyBalanceTableCid, err := adt.StoreEmptyMap(store, adt.BalanceTableBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty balance table: %w", err)
	}

	return &State{
		Proposals:        emptyProposalsArrayCid,
		States:           emptyStatesArrayCid,
		PendingProposals: emptyPendingProposalsMapCid,
		EscrowTable:      emptyBalanceTableCid,
		LockedTable:      emptyBalanceTableCid,
		NextID:           abi.DealID(0),
		DealOpsByEpoch:   emptyDealOpsHamtCid,
		LastCron:         abi.ChainEpoch(-1),

		TotalClientLockedCollateral:   abi.NewTokenAmount(0),
		TotalProviderLockedCollateral: abi.NewTokenAmount(0),
		TotalClientStorageFee:         abi.NewTokenAmount(0),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Deal state operations
////////////////////////////////////////////////////////////////////////////////

func (m *marketStateMutation) updatePendingDealState(rt Runtime, state *DealState, deal *DealProposal, epoch abi.ChainEpoch) (amountSlashed abi.TokenAmount, nextEpoch abi.ChainEpoch, removeDeal bool) {
	amountSlashed = abi.NewTokenAmount(0)

	everUpdated := state.LastUpdatedEpoch != epochUndefined
	everSlashed := state.SlashEpoch != epochUndefined

	builtin.RequireState(rt, !everUpdated || (state.LastUpdatedEpoch <= epoch), "deal updated at future epoch %d", state.LastUpdatedEpoch)

	// This would be the case that the first callback somehow triggers before it is scheduled to
	// This is expected not to be able to happen
	if deal.StartEpoch > epoch {
		return amountSlashed, epochUndefined, false
	}

	paymentEndEpoch := deal.EndEpoch
	if everSlashed {
		builtin.RequireState(rt, epoch >= state.SlashEpoch, "current epoch less than deal slash epoch %d", state.SlashEpoch)
		builtin.RequireState(rt, state.SlashEpoch <= deal.EndEpoch, "deal slash epoch %d after deal end %d", state.SlashEpoch, deal.EndEpoch)
		paymentEndEpoch = state.SlashEpoch
	} else if epoch < paymentEndEpoch {
		paymentEndEpoch = epoch
	}

	paymentStartEpoch := deal.StartEpoch
	if everUpdated && state.LastUpdatedEpoch > paymentStartEpoch {
		paymentStartEpoch = state.LastUpdatedEpoch
	}

	numEpochsElapsed := paymentEndEpoch - paymentStartEpoch

	{
		// Process deal payment for the elapsed epochs.
		totalPayment := big.Mul(big.NewInt(int64(numEpochsElapsed)), deal.StoragePricePerEpoch)

		// the transfer amount can be less than or equal to zero if a deal is slashed before or at the deal's start epoch.
		if totalPayment.GreaterThan(big.Zero()) {
			err := m.transferBalance(deal.Client, deal.Provider, totalPayment)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to transfer %v from %v to %v",
				totalPayment, deal.Client, deal.Provider)
		}
	}

	if everSlashed {
		// unlock client collateral and locked storage fee
		paymentRemaining, err := dealGetPaymentRemaining(deal, state.SlashEpoch)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to compute remaining payment")

		// unlock remaining storage fee
		err = m.unlockBalance(deal.Client, paymentRemaining, ClientStorageFee)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to unlock remaining client storage fee")

		// unlock client collateral
		err = m.unlockBalance(deal.Client, deal.ClientCollateral, ClientCollateral)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to unlock client collateral")

		// slash provider collateral
		amountSlashed = deal.ProviderCollateral
		err = m.slashBalance(deal.Provider, amountSlashed, ProviderCollateral)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "slashing balance")
		return amountSlashed, epochUndefined, true
	}

	if epoch >= deal.EndEpoch {
		m.processDealExpired(rt, deal, state)
		return amountSlashed, epochUndefined, true
	}

	// We're explicitly not inspecting the end epoch and may process a deal's expiration late, in order to prevent an outsider
	// from loading a cron tick by activating too many deals with the same end epoch.
	nextEpoch = epoch + DealUpdatesInterval

	return amountSlashed, nextEpoch, false
}

// Deal start deadline elapsed without appearing in a proven sector.
// Slash a portion of provider's collateral, and unlock remaining collaterals
// for both provider and client.
func (m *marketStateMutation) processDealInitTimedOut(rt Runtime, deal *DealProposal) abi.TokenAmount {
	if err := m.unlockBalance(deal.Client, deal.TotalStorageFee(), ClientStorageFee); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failure unlocking client storage fee: %s", err)
	}
	if err := m.unlockBalance(deal.Client, deal.ClientCollateral, ClientCollateral); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failure unlocking client collateral: %s", err)
	}

	amountSlashed := CollateralPenaltyForDealActivationMissed(deal.ProviderCollateral)
	amountRemaining := big.Sub(deal.ProviderBalanceRequirement(), amountSlashed)

	if err := m.slashBalance(deal.Provider, amountSlashed, ProviderCollateral); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to slash balance: %s", err)
	}

	if err := m.unlockBalance(deal.Provider, amountRemaining, ProviderCollateral); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to unlock deal provider balance: %s", err)
	}

	return amountSlashed
}

// Normal expiration. Unlock collaterals for both provider and client.
func (m *marketStateMutation) processDealExpired(rt Runtime, deal *DealProposal, state *DealState) {
	builtin.RequireState(rt, state.SectorStartEpoch != epochUndefined, "sector start epoch undefined")

	// Note: payment has already been completed at this point (_rtProcessDealPaymentEpochsElapsed)
	err := m.unlockBalance(deal.Provider, deal.ProviderCollateral, ProviderCollateral)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed unlocking deal provider balance")

	err = m.unlockBalance(deal.Client, deal.ClientCollateral, ClientCollateral)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed unlocking deal client balance")
}

func (m *marketStateMutation) generateStorageDealID() abi.DealID {
	ret := m.nextDealId
	m.nextDealId = m.nextDealId + abi.DealID(1)
	return ret
}

////////////////////////////////////////////////////////////////////////////////
// State utility functions
////////////////////////////////////////////////////////////////////////////////

func dealProposalIsInternallyValid(rt Runtime, proposal ClientDealProposal) error {
	// Note: we do not verify the provider signature here, since this is implicit in the
	// authenticity of the on-chain message publishing the deal.
	buf := bytes.Buffer{}
	err := proposal.Proposal.MarshalCBOR(&buf)
	if err != nil {
		return xerrors.Errorf("proposal signature verification failed to marshal proposal: %w", err)
	}
	err = rt.VerifySignature(proposal.ClientSignature, proposal.Proposal.Client, buf.Bytes())
	if err != nil {
		return xerrors.Errorf("signature proposal invalid: %w", err)
	}
	return nil
}

func dealGetPaymentRemaining(deal *DealProposal, slashEpoch abi.ChainEpoch) (abi.TokenAmount, error) {
	if slashEpoch > deal.EndEpoch {
		return big.Zero(), xerrors.Errorf("deal slash epoch %d after end epoch %d", slashEpoch, deal.EndEpoch)
	}

	// Payments are always for start -> end epoch irrespective of when the deal is slashed.
	if slashEpoch < deal.StartEpoch {
		slashEpoch = deal.StartEpoch
	}

	durationRemaining := deal.EndEpoch - slashEpoch
	if durationRemaining < 0 {
		return big.Zero(), xerrors.Errorf("deal remaining duration negative: %d", durationRemaining)
	}

	return big.Mul(big.NewInt(int64(durationRemaining)), deal.StoragePricePerEpoch), nil
}

// MarketStateMutationPermission is the mutation permission on a state field
type MarketStateMutationPermission int

const (
	// Invalid means NO permission
	Invalid MarketStateMutationPermission = iota
	// ReadOnlyPermission allows reading but not mutating the field
	ReadOnlyPermission
	// WritePermission allows mutating the field
	WritePermission
)

type marketStateMutation struct {
	st    *State
	store adt.Store

	proposalPermit MarketStateMutationPermission
	dealProposals  *DealArray

	statePermit MarketStateMutationPermission
	dealStates  *DealMetaArray

	escrowPermit MarketStateMutationPermission
	escrowTable  *adt.BalanceTable

	pendingPermit MarketStateMutationPermission
	pendingDeals  *adt.Set

	dpePermit    MarketStateMutationPermission
	dealsByEpoch *SetMultimap

	lockedPermit                  MarketStateMutationPermission
	lockedTable                   *adt.BalanceTable
	totalClientLockedCollateral   abi.TokenAmount
	totalProviderLockedCollateral abi.TokenAmount
	totalClientStorageFee         abi.TokenAmount

	nextDealId abi.DealID
}

func (s *State) mutator(store adt.Store) *marketStateMutation {
	return &marketStateMutation{st: s, store: store}
}

func (m *marketStateMutation) build() (*marketStateMutation, error) {
	if m.proposalPermit != Invalid {
		proposals, err := AsDealProposalArray(m.store, m.st.Proposals)
		if err != nil {
			return nil, xerrors.Errorf("failed to load deal proposals: %w", err)
		}
		m.dealProposals = proposals
	}

	if m.statePermit != Invalid {
		states, err := AsDealStateArray(m.store, m.st.States)
		if err != nil {
			return nil, xerrors.Errorf("failed to load deal state: %w", err)
		}
		m.dealStates = states
	}

	if m.lockedPermit != Invalid {
		lt, err := adt.AsBalanceTable(m.store, m.st.LockedTable)
		if err != nil {
			return nil, xerrors.Errorf("failed to load locked table: %w", err)
		}
		m.lockedTable = lt
		m.totalClientLockedCollateral = m.st.TotalClientLockedCollateral.Copy()
		m.totalClientStorageFee = m.st.TotalClientStorageFee.Copy()
		m.totalProviderLockedCollateral = m.st.TotalProviderLockedCollateral.Copy()
	}

	if m.escrowPermit != Invalid {
		et, err := adt.AsBalanceTable(m.store, m.st.EscrowTable)
		if err != nil {
			return nil, xerrors.Errorf("failed to load escrow table: %w", err)
		}
		m.escrowTable = et
	}

	if m.pendingPermit != Invalid {
		pending, err := adt.AsSet(m.store, m.st.PendingProposals, builtin.DefaultHamtBitwidth)
		if err != nil {
			return nil, xerrors.Errorf("failed to load pending proposals: %w", err)
		}
		m.pendingDeals = pending
	}

	if m.dpePermit != Invalid {
		dbe, err := AsSetMultimap(m.store, m.st.DealOpsByEpoch, builtin.DefaultHamtBitwidth, builtin.DefaultHamtBitwidth)
		if err != nil {
			return nil, xerrors.Errorf("failed to load deals by epoch: %w", err)
		}
		m.dealsByEpoch = dbe
	}

	m.nextDealId = m.st.NextID

	return m, nil
}

func (m *marketStateMutation) withDealProposals(permit MarketStateMutationPermission) *marketStateMutation {
	m.proposalPermit = permit
	return m
}

func (m *marketStateMutation) withDealStates(permit MarketStateMutationPermission) *marketStateMutation {
	m.statePermit = permit
	return m
}

func (m *marketStateMutation) withEscrowTable(permit MarketStateMutationPermission) *marketStateMutation {
	m.escrowPermit = permit
	return m
}

func (m *marketStateMutation) withLockedTable(permit MarketStateMutationPermission) *marketStateMutation {
	m.lockedPermit = permit
	return m
}

func (m *marketStateMutation) withPendingProposals(permit MarketStateMutationPermission) *marketStateMutation {
	m.pendingPermit = permit
	return m
}

func (m *marketStateMutation) withDealsByEpoch(permit MarketStateMutationPermission) *marketStateMutation {
	m.dpePermit = permit
	return m
}

func (m *marketStateMutation) commitState() error {
	var err error
	if m.proposalPermit == WritePermission {
		if m.st.Proposals, err = m.dealProposals.Root(); err != nil {
			return xerrors.Errorf("failed to flush deal dealProposals: %w", err)
		}
	}

	if m.statePermit == WritePermission {
		if m.st.States, err = m.dealStates.Root(); err != nil {
			return xerrors.Errorf("failed to flush deal states: %w", err)
		}
	}

	if m.lockedPermit == WritePermission {
		if m.st.LockedTable, err = m.lockedTable.Root(); err != nil {
			return xerrors.Errorf("failed to flush locked table: %w", err)
		}
		m.st.TotalClientLockedCollateral = m.totalClientLockedCollateral.Copy()
		m.st.TotalProviderLockedCollateral = m.totalProviderLockedCollateral.Copy()
		m.st.TotalClientStorageFee = m.totalClientStorageFee.Copy()
	}

	if m.escrowPermit == WritePermission {
		if m.st.EscrowTable, err = m.escrowTable.Root(); err != nil {
			return xerrors.Errorf("failed to flush escrow table: %w", err)
		}
	}

	if m.pendingPermit == WritePermission {
		if m.st.PendingProposals, err = m.pendingDeals.Root(); err != nil {
			return xerrors.Errorf("failed to flush pending deals: %w", err)
		}
	}

	if m.dpePermit == WritePermission {
		if m.st.DealOpsByEpoch, err = m.dealsByEpoch.Root(); err != nil {
			return xerrors.Errorf("failed to flush deals by epoch: %w", err)
		}
	}

	m.st.NextID = m.nextDealId
	return nil
}
