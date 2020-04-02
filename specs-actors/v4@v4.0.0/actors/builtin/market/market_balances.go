package market

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"golang.org/x/xerrors"
)

func (m *marketStateMutation) lockClientAndProviderBalances(proposal *DealProposal) error {
	if err := m.maybeLockBalance(proposal.Client, proposal.ClientBalanceRequirement()); err != nil {
		return xerrors.Errorf("failed to lock client funds: %w", err)
	}
	if err := m.maybeLockBalance(proposal.Provider, proposal.ProviderCollateral); err != nil {
		return xerrors.Errorf("failed to lock provider funds: %w", err)
	}

	m.totalClientLockedCollateral = big.Add(m.totalClientLockedCollateral, proposal.ClientCollateral)
	m.totalClientStorageFee = big.Add(m.totalClientStorageFee, proposal.TotalStorageFee())
	m.totalProviderLockedCollateral = big.Add(m.totalProviderLockedCollateral, proposal.ProviderCollateral)
	return nil
}

func (m *marketStateMutation) unlockBalance(addr addr.Address, amount abi.TokenAmount, lockReason BalanceLockingReason) error {
	if amount.LessThan(big.Zero()) {
		return xerrors.Errorf("unlock negative amount %v", amount)
	}

	err := m.lockedTable.MustSubtract(addr, amount)
	if err != nil {
		return xerrors.Errorf("subtracting from locked balance: %w", err)
	}

	switch lockReason {
	case ClientCollateral:
		m.totalClientLockedCollateral = big.Sub(m.totalClientLockedCollateral, amount)
	case ClientStorageFee:
		m.totalClientStorageFee = big.Sub(m.totalClientStorageFee, amount)
	case ProviderCollateral:
		m.totalProviderLockedCollateral = big.Sub(m.totalProviderLockedCollateral, amount)
	}

	return nil
}

// move funds from locked in client to available in provider
func (m *marketStateMutation) transferBalance(fromAddr addr.Address, toAddr addr.Address, amount abi.TokenAmount) error {
	if amount.LessThan(big.Zero()) {
		return xerrors.Errorf("transfer negative amount %v", amount)
	}
	if err := m.escrowTable.MustSubtract(fromAddr, amount); err != nil {
		return xerrors.Errorf("subtract from escrow: %w", err)
	}
	if err := m.unlockBalance(fromAddr, amount, ClientStorageFee); err != nil {
		return xerrors.Errorf("subtract from locked: %w", err)
	}
	if err := m.escrowTable.Add(toAddr, amount); err != nil {
		return xerrors.Errorf("add to escrow: %w", err)
	}
	return nil
}

func (m *marketStateMutation) slashBalance(addr addr.Address, amount abi.TokenAmount, reason BalanceLockingReason) error {
	if amount.LessThan(big.Zero()) {
		return xerrors.Errorf("negative amount to slash: %v", amount)
	}

	if err := m.escrowTable.MustSubtract(addr, amount); err != nil {
		return xerrors.Errorf("subtract from escrow: %v", err)
	}

	return m.unlockBalance(addr, amount, reason)
}

func (m *marketStateMutation) maybeLockBalance(addr addr.Address, amount abi.TokenAmount) error {
	if amount.LessThan(big.Zero()) {
		return xerrors.Errorf("cannot lock negative amount %v", amount)
	}

	prevLocked, err := m.lockedTable.Get(addr)
	if err != nil {
		return xerrors.Errorf("failed to get locked balance: %w", err)
	}

	escrowBalance, err := m.escrowTable.Get(addr)
	if err != nil {
		return xerrors.Errorf("failed to get escrow balance: %w", err)
	}

	if big.Add(prevLocked, amount).GreaterThan(escrowBalance) {
		return exitcode.ErrInsufficientFunds.Wrapf("insufficient balance for addr %s: escrow balance %s < locked %s + required %s",
			addr, escrowBalance, prevLocked, amount)
	}

	if err := m.lockedTable.Add(addr, amount); err != nil {
		return xerrors.Errorf("failed to add locked balance: %w", err)
	}
	return nil
}
