package multisig

import (
	"bytes"
	"encoding/binary"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type StateSummary struct {
	PendingTxnCount       uint64
	NumApprovalsThreshold uint64
	SignerCount           int
}

// Checks internal invariants of multisig state.
func CheckStateInvariants(st *State, store adt.Store) (*StateSummary, *builtin.MessageAccumulator) {
	acc := &builtin.MessageAccumulator{}

	// assert invariants involving signers
	acc.Require(len(st.Signers) <= SignersMax, "multisig has too many signers: %d", len(st.Signers))
	acc.Require(uint64(len(st.Signers)) >= st.NumApprovalsThreshold,
		"multisig has insufficient signers to meet threshold (%d < %d)", len(st.Signers), st.NumApprovalsThreshold)

	if st.UnlockDuration == 0 { // See https://github.com/filecoin-project/specs-actors/issues/1185
		acc.Require(st.StartEpoch == 0, "non-zero start epoch %d with zero unlock duration", st.StartEpoch)
		acc.Require(st.InitialBalance.IsZero(), "non-zero locked balance %v with zero unlock duration", st.InitialBalance)
	}

	// create lookup to test transaction approvals are multisig signers.
	signers := make(map[address.Address]struct{})
	for _, a := range st.Signers {
		signers[a] = struct{}{}
	}

	// test pending transactions
	maxTxnID := TxnID(-1)
	numPending := uint64(0)
	if transactions, err := adt.AsMap(store, st.PendingTxns); err != nil {
		acc.Addf("error loading transactions: %v", err)
	} else {
		var txn Transaction
		err = transactions.ForEach(&txn, func(txnIDStr string) error {
			txnID, err := ParseTxnIDKey(txnIDStr)
			if err != nil {
				return err
			}
			if txnID > maxTxnID {
				maxTxnID = txnID
			}

			seenApprovals := make(map[address.Address]struct{})
			for _, approval := range txn.Approved {
				_, found := signers[approval]
				acc.Require(found, "approval %v for transaction %d is not in signers list", approval, txnID)

				_, seen := seenApprovals[approval]
				acc.Require(!seen, "duplicate approval %v for transaction %d", approval, txnID)

				seenApprovals[approval] = struct{}{}
			}

			numPending++
			return nil
		})
		acc.RequireNoError(err, "error iterating transactions")
	}

	acc.Require(st.NextTxnID > maxTxnID, "next transaction id %d is not greater than pending ids", st.NextTxnID)
	return &StateSummary{
		PendingTxnCount:       numPending,
		NumApprovalsThreshold: st.NumApprovalsThreshold,
		SignerCount:           len(st.Signers),
	}, acc
}

func ParseTxnIDKey(key string) (TxnID, error) {
	id, err := binary.ReadVarint(bytes.NewReader([]byte(key)))
	return TxnID(id), err
}
