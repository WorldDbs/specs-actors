package multisig

import (
	"bytes"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	multisig0 "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	. "github.com/filecoin-project/specs-actors/v2/actors/util"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type TxnID = multisig0.TxnID

//type Transaction struct {
//	To     addr.Address
//	Value  abi.TokenAmount
//	Method abi.MethodNum
//	Params []byte
//
//	// This address at index 0 is the transaction proposer, order of this slice must be preserved.
//	Approved []addr.Address
//}
type Transaction = multisig0.Transaction

// Data for a BLAKE2B-256 to be attached to methods referencing proposals via TXIDs.
// Ensures the existence of a cryptographic reference to the original proposal. Useful
// for offline signers and for protection when reorgs change a multisig TXID.
//
// Requester - The requesting multisig wallet member.
// All other fields - From the "Transaction" struct.
//type ProposalHashData struct {
//	Requester addr.Address
//	To        addr.Address
//	Value     abi.TokenAmount
//	Method    abi.MethodNum
//	Params    []byte
//}
type ProposalHashData = multisig0.ProposalHashData

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.Propose,
		3:                         a.Approve,
		4:                         a.Cancel,
		5:                         a.AddSigner,
		6:                         a.RemoveSigner,
		7:                         a.SwapSigner,
		8:                         a.ChangeNumApprovalsThreshold,
		9:                         a.LockBalance,
	}
}

func (a Actor) Code() cid.Cid {
	return builtin.MultisigActorCodeID
}

func (a Actor) State() cbor.Er {
	return new(State)
}

var _ runtime.VMActor = Actor{}

// Changed since v0:
// - Added StartEpoch
type ConstructorParams struct {
	Signers               []addr.Address
	NumApprovalsThreshold uint64
	UnlockDuration        abi.ChainEpoch
	StartEpoch            abi.ChainEpoch
}

func (a Actor) Constructor(rt runtime.Runtime, params *ConstructorParams) *abi.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.InitActorAddr)

	if len(params.Signers) < 1 {
		rt.Abortf(exitcode.ErrIllegalArgument, "must have at least one signer")
	}

	if len(params.Signers) > SignersMax {
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot add more than %d signers", SignersMax)
	}

	// resolve signer addresses and do not allow duplicate signers
	resolvedSigners := make([]addr.Address, 0, len(params.Signers))
	deDupSigners := make(map[addr.Address]struct{}, len(params.Signers))
	for _, signer := range params.Signers {
		resolved, err := builtin.ResolveToIDAddr(rt, signer)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve addr %v to ID addr", signer)

		if _, ok := deDupSigners[resolved]; ok {
			rt.Abortf(exitcode.ErrIllegalArgument, "duplicate signer not allowed: %s", signer)
		}

		resolvedSigners = append(resolvedSigners, resolved)
		deDupSigners[resolved] = struct{}{}
	}

	if params.NumApprovalsThreshold > uint64(len(params.Signers)) {
		rt.Abortf(exitcode.ErrIllegalArgument, "must not require more approvals than signers")
	}

	if params.NumApprovalsThreshold < 1 {
		rt.Abortf(exitcode.ErrIllegalArgument, "must require at least one approval")
	}

	if params.UnlockDuration < 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "negative unlock duration disallowed")
	}

	pending, err := adt.MakeEmptyMap(adt.AsStore(rt)).Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to create empty map: %v", err)
	}

	var st State
	st.Signers = resolvedSigners
	st.NumApprovalsThreshold = params.NumApprovalsThreshold
	st.PendingTxns = pending
	st.InitialBalance = abi.NewTokenAmount(0)
	if params.UnlockDuration != 0 {
		st.SetLocked(params.StartEpoch, params.UnlockDuration, rt.ValueReceived())
	}

	rt.StateCreate(&st)
	return nil
}

//type ProposeParams struct {
//	To     addr.Address
//	Value  abi.TokenAmount
//	Method abi.MethodNum
//	Params []byte
//}
type ProposeParams = multisig0.ProposeParams

//type ProposeReturn struct {
//	// TxnID is the ID of the proposed transaction
//	TxnID TxnID
//	// Applied indicates if the transaction was applied as opposed to proposed but not applied due to lack of approvals
//	Applied bool
//	// Code is the exitcode of the transaction, if Applied is false this field should be ignored.
//	Code exitcode.ExitCode
//	// Ret is the return vale of the transaction, if Applied is false this field should be ignored.
//	Ret []byte
//}
type ProposeReturn = multisig0.ProposeReturn

func (a Actor) Propose(rt runtime.Runtime, params *ProposeParams) *ProposeReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	proposer := rt.Caller()

	if params.Value.Sign() < 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "proposed value must be non-negative, was %v", params.Value)
	}

	var txnID TxnID
	var st State
	var txn *Transaction
	rt.StateTransaction(&st, func() {
		if !isSigner(proposer, st.Signers) {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", proposer)
		}

		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		txnID = st.NextTxnID
		st.NextTxnID += 1
		txn = &Transaction{
			To:       params.To,
			Value:    params.Value,
			Method:   params.Method,
			Params:   params.Params,
			Approved: []addr.Address{},
		}

		if err := ptx.Put(txnID, txn); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to put transaction for propose: %v", err)
		}

		st.PendingTxns, err = ptx.Root()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
	})

	applied, ret, code := a.approveTransaction(rt, txnID, txn)

	// Note: this transaction ID may not be stable across chain re-orgs.
	// The proposal hash may be provided as a stability check when approving.
	return &ProposeReturn{
		TxnID:   txnID,
		Applied: applied,
		Code:    code,
		Ret:     ret,
	}
}

//type TxnIDParams struct {
//	ID TxnID
//	// Optional hash of proposal to ensure an operation can only apply to a
//	// specific proposal.
//	ProposalHash []byte
//}
type TxnIDParams = multisig0.TxnIDParams

//type ApproveReturn struct {
//	// Applied indicates if the transaction was applied as opposed to proposed but not applied due to lack of approvals
//	Applied bool
//	// Code is the exitcode of the transaction, if Applied is false this field should be ignored.
//	Code exitcode.ExitCode
//	// Ret is the return vale of the transaction, if Applied is false this field should be ignored.
//	Ret []byte
//}
type ApproveReturn = multisig0.ApproveReturn

func (a Actor) Approve(rt runtime.Runtime, params *TxnIDParams) *ApproveReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.Caller()

	var st State
	var txn *Transaction
	rt.StateTransaction(&st, func() {
		callerIsSigner := isSigner(callerAddr, st.Signers)
		if !callerIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", callerAddr)
		}

		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		txn = getTransaction(rt, ptx, params.ID, params.ProposalHash, true)
	})

	// if the transaction already has enough approvers, execute it without "processing" this approval.
	approved, ret, code := executeTransactionIfApproved(rt, st, params.ID, txn)
	if !approved {
		// if the transaction hasn't already been approved, let's "process" this approval
		// and see if we can execute the transaction
		approved, ret, code = a.approveTransaction(rt, params.ID, txn)
	}

	return &ApproveReturn{
		Applied: approved,
		Code:    code,
		Ret:     ret,
	}
}

func (a Actor) Cancel(rt runtime.Runtime, params *TxnIDParams) *abi.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.Caller()

	var st State
	rt.StateTransaction(&st, func() {
		callerIsSigner := isSigner(callerAddr, st.Signers)
		if !callerIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", callerAddr)
		}

		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending txns")

		txn, err := getPendingTransaction(ptx, params.ID)
		if err != nil {
			rt.Abortf(exitcode.ErrNotFound, "failed to get transaction for cancel: %v", err)
		}
		proposer := txn.Approved[0]
		if proposer != callerAddr {
			rt.Abortf(exitcode.ErrForbidden, "Cannot cancel another signers transaction")
		}

		// confirm the hashes match
		calculatedHash, err := ComputeProposalHash(&txn, rt.HashBlake2b)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to compute proposal hash for %v", params.ID)
		if params.ProposalHash != nil && !bytes.Equal(params.ProposalHash, calculatedHash[:]) {
			rt.Abortf(exitcode.ErrIllegalState, "hash does not match proposal params (ensure requester is an ID address)")
		}

		err = ptx.Delete(params.ID)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to delete pending transaction")

		st.PendingTxns, err = ptx.Root()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
	})
	return nil
}

//type AddSignerParams struct {
//	Signer   addr.Address
//	Increase bool
//}
type AddSignerParams = multisig0.AddSignerParams

func (a Actor) AddSigner(rt runtime.Runtime, params *AddSignerParams) *abi.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Receiver())
	resolvedNewSigner, err := builtin.ResolveToIDAddr(rt, params.Signer)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve address %v", params.Signer)

	var st State
	rt.StateTransaction(&st, func() {
		if len(st.Signers) >= SignersMax {
			rt.Abortf(exitcode.ErrForbidden, "cannot add more than %d signers", SignersMax)
		}

		isSigner := isSigner(resolvedNewSigner, st.Signers)
		if isSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is already a signer", resolvedNewSigner)
		}

		st.Signers = append(st.Signers, resolvedNewSigner)
		if params.Increase {
			st.NumApprovalsThreshold = st.NumApprovalsThreshold + 1
		}
	})
	return nil
}

//type RemoveSignerParams struct {
//	Signer   addr.Address
//	Decrease bool
//}
type RemoveSignerParams = multisig0.RemoveSignerParams

func (a Actor) RemoveSigner(rt runtime.Runtime, params *RemoveSignerParams) *abi.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Receiver())
	resolvedOldSigner, err := builtin.ResolveToIDAddr(rt, params.Signer)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve address %v", params.Signer)

	store := adt.AsStore(rt)
	var st State
	rt.StateTransaction(&st, func() {
		isSigner := isSigner(resolvedOldSigner, st.Signers)
		if !isSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", resolvedOldSigner)
		}

		if len(st.Signers) == 1 {
			rt.Abortf(exitcode.ErrForbidden, "cannot remove only signer")
		}

		newSigners := make([]addr.Address, 0, len(st.Signers))
		// signers have already been resolved
		for _, s := range st.Signers {
			if resolvedOldSigner != s {
				newSigners = append(newSigners, s)
			}
		}

		// if the number of signers is below the threshold after removing the given signer,
		// we should decrease the threshold by 1. This means that decrease should NOT be set to false
		// in such a scenario.
		if !params.Decrease && uint64(len(st.Signers)-1) < st.NumApprovalsThreshold {
			rt.Abortf(exitcode.ErrIllegalArgument, "can't reduce signers to %d below threshold %d with decrease=false", len(st.Signers)-1, st.NumApprovalsThreshold)
		}

		if params.Decrease {
			if st.NumApprovalsThreshold < 2 {
				rt.Abortf(exitcode.ErrIllegalArgument, "can't decrease approvals from %d to %d", st.NumApprovalsThreshold, st.NumApprovalsThreshold-1)
			}
			st.NumApprovalsThreshold = st.NumApprovalsThreshold - 1
		}

		err := st.PurgeApprovals(store, resolvedOldSigner)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to purge approvals of removed signer")

		st.Signers = newSigners
	})

	return nil
}

//type SwapSignerParams struct {
//	From addr.Address
//	To   addr.Address
//}
type SwapSignerParams = multisig0.SwapSignerParams

func (a Actor) SwapSigner(rt runtime.Runtime, params *SwapSignerParams) *abi.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Receiver())

	fromResolved, err := builtin.ResolveToIDAddr(rt, params.From)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve from address %v", params.From)

	toResolved, err := builtin.ResolveToIDAddr(rt, params.To)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to address %v", params.To)

	store := adt.AsStore(rt)
	var st State
	rt.StateTransaction(&st, func() {
		fromIsSigner := isSigner(fromResolved, st.Signers)
		if !fromIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "from addr %s is not a signer", fromResolved)
		}

		toIsSigner := isSigner(toResolved, st.Signers)
		if toIsSigner {
			rt.Abortf(exitcode.ErrIllegalArgument, "%s already a signer", toResolved)
		}

		newSigners := make([]addr.Address, 0, len(st.Signers))
		for _, s := range st.Signers {
			if s != fromResolved {
				newSigners = append(newSigners, s)
			}
		}
		newSigners = append(newSigners, toResolved)
		st.Signers = newSigners

		err := st.PurgeApprovals(store, fromResolved)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to purge approvals of removed signer")
	})

	return nil
}

//type ChangeNumApprovalsThresholdParams struct {
//	NewThreshold uint64
//}
type ChangeNumApprovalsThresholdParams = multisig0.ChangeNumApprovalsThresholdParams

func (a Actor) ChangeNumApprovalsThreshold(rt runtime.Runtime, params *ChangeNumApprovalsThresholdParams) *abi.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Receiver())

	var st State
	rt.StateTransaction(&st, func() {
		if params.NewThreshold == 0 || params.NewThreshold > uint64(len(st.Signers)) {
			rt.Abortf(exitcode.ErrIllegalArgument, "New threshold value not supported")
		}

		st.NumApprovalsThreshold = params.NewThreshold
	})
	return nil
}

//type LockBalanceParams struct {
//	StartEpoch abi.ChainEpoch
//	UnlockDuration abi.ChainEpoch
//	Amount abi.TokenAmount
//}
type LockBalanceParams = multisig0.LockBalanceParams

func (a Actor) LockBalance(rt runtime.Runtime, params *LockBalanceParams) *abi.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Receiver())

	if params.UnlockDuration <= 0 {
		// Note: Unlock duration of zero is workable, but rejected as ineffective, probably an error.
		rt.Abortf(exitcode.ErrIllegalArgument, "unlock duration must be positive")
	}

	nv := rt.NetworkVersion()
	if nv >= network.Version7 && params.Amount.LessThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "amount to lock must be positive")
	}

	var st State
	rt.StateTransaction(&st, func() {
		if st.UnlockDuration != 0 {
			rt.Abortf(exitcode.ErrForbidden, "modification of unlock disallowed")
		}
		st.SetLocked(params.StartEpoch, params.UnlockDuration, params.Amount)
	})
	return nil
}

func (a Actor) approveTransaction(rt runtime.Runtime, txnID TxnID, txn *Transaction) (bool, []byte, exitcode.ExitCode) {
	caller := rt.Caller()

	var st State
	// abort duplicate approval
	for _, previousApprover := range txn.Approved {
		if previousApprover == caller {
			rt.Abortf(exitcode.ErrForbidden, "%s already approved this message", previousApprover)
		}
	}

	// add the caller to the list of approvers
	rt.StateTransaction(&st, func() {
		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		// update approved on the transaction
		txn.Approved = append(txn.Approved, caller)
		err = ptx.Put(txnID, txn)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to put transaction %v for approval", txnID)

		st.PendingTxns, err = ptx.Root()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
	})

	return executeTransactionIfApproved(rt, st, txnID, txn)
}

func getTransaction(rt runtime.Runtime, ptx *adt.Map, txnID TxnID, proposalHash []byte, checkHash bool) *Transaction {
	var txn Transaction

	// get transaction from the state trie
	var err error
	txn, err = getPendingTransaction(ptx, txnID)
	if err != nil {
		rt.Abortf(exitcode.ErrNotFound, "failed to get transaction for approval: %v", err)
	}

	// confirm the hashes match
	if checkHash {
		calculatedHash, err := ComputeProposalHash(&txn, rt.HashBlake2b)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to compute proposal hash for %v", txnID)
		if proposalHash != nil && !bytes.Equal(proposalHash, calculatedHash[:]) {
			rt.Abortf(exitcode.ErrIllegalArgument, "hash does not match proposal params (ensure requester is an ID address)")
		}
	}

	return &txn
}

func executeTransactionIfApproved(rt runtime.Runtime, st State, txnID TxnID, txn *Transaction) (bool, []byte, exitcode.ExitCode) {
	var out builtin.CBORBytes
	var code exitcode.ExitCode
	applied := false
	nv := rt.NetworkVersion()

	thresholdMet := uint64(len(txn.Approved)) >= st.NumApprovalsThreshold
	if thresholdMet {
		if err := st.assertAvailable(rt.CurrentBalance(), txn.Value, rt.CurrEpoch()); err != nil {
			rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds unlocked: %v", err)
		}

		// A sufficient number of approvals have arrived and sufficient funds have been unlocked: relay the message and delete from pending queue.
		code = rt.Send(
			txn.To,
			txn.Method,
			builtin.CBORBytes(txn.Params),
			txn.Value,
			&out,
		)
		applied = true

		// This could be rearranged to happen inside the first state transaction, before the send().
		rt.StateTransaction(&st, func() {
			ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

			// Prior to version 6 we attempt to delete all transactions, even those
			// no longer in the pending txns map because they have been purged.
			shouldDelete := true
			// Starting at version 6 we first check if the transaction exists before
			// deleting. This allows 1 out of n multisig swaps and removes initiated
			// by the swapped/removed signer to go through without an illegal state error
			if nv >= network.Version6 {
				txnExists, err := ptx.Has(txnID)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check existance of transaction %v for cleanup", txnID)
				shouldDelete = txnExists
			}

			if shouldDelete {
				if err := ptx.Delete(txnID); err != nil {
					rt.Abortf(exitcode.ErrIllegalState, "failed to delete transaction for cleanup: %v", err)
				}
			}

			st.PendingTxns, err = ptx.Root()
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
		})
	}

	// Pass the return value through uninterpreted with the expectation that serializing into a CBORBytes never fails
	// since it just copies the bytes.

	return applied, out, code
}

func isSigner(address addr.Address, signers []addr.Address) bool {
	AssertMsg(address.Protocol() == addr.ID, "address %v passed to isSigner must be a resolved address", address)
	// signer addresses have already been resolved
	for _, signer := range signers {
		if signer == address {
			return true
		}
	}

	return false
}

// Computes a digest of a proposed transaction. This digest is used to confirm identity of the transaction
// associated with an ID, which might change under chain re-orgs.
func ComputeProposalHash(txn *Transaction, hash func([]byte) [32]byte) ([]byte, error) {
	hashData := ProposalHashData{
		Requester: txn.Approved[0],
		To:        txn.To,
		Value:     txn.Value,
		Method:    txn.Method,
		Params:    txn.Params,
	}

	data, err := hashData.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to construct multisig approval hash: %w", err)
	}

	hashResult := hash(data)
	return hashResult[:], nil
}
