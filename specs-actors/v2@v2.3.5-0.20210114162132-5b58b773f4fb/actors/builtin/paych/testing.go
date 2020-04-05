package paych

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type StateSummary struct {
	Redeemed abi.TokenAmount
}

// Checks internal invariants of paych state.
func CheckStateInvariants(st *State, store adt.Store, balance abi.TokenAmount) (*StateSummary, *builtin.MessageAccumulator) {
	acc := &builtin.MessageAccumulator{}
	paychSummary := &StateSummary{
		Redeemed: big.Zero(),
	}

	acc.Require(st.From.Protocol() == address.ID, "from address is not ID address %v", st.From)
	acc.Require(st.To.Protocol() == address.ID, "to address is not ID address %v", st.To)
	acc.Require(st.SettlingAt >= st.MinSettleHeight,
		"channel is setting at epoch %d before min settle height %d", st.SettlingAt, st.MinSettleHeight)

	if lanes, err := adt.AsArray(store, st.LaneStates); err != nil {
		acc.Addf("error loading lanes: %v", err)
	} else {
		var lane LaneState
		err = lanes.ForEach(&lane, func(i int64) error {
			acc.Require(lane.Redeemed.GreaterThan(big.Zero()), "land %d redeemed is not greater than zero %v", i, lane.Redeemed)
			paychSummary.Redeemed = big.Add(paychSummary.Redeemed, lane.Redeemed)
			return nil
		})
		acc.RequireNoError(err, "error iterating lanes")
	}

	acc.Require(balance.GreaterThanEqual(st.ToSend),
		"channel has insufficient funds to send (%v < %v)", balance, st.ToSend)

	return paychSummary, acc
}
