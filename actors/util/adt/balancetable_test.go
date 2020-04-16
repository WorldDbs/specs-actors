package adt_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestBalanceTable(t *testing.T) {
	buildBalanceTable := func() *adt.BalanceTable {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		store := adt.AsStore(rt)
		emptyMap := adt.MakeEmptyMap(store)

		bt, err := adt.AsBalanceTable(store, tutil.MustRoot(t, emptyMap))
		require.NoError(t, err)
		return bt
	}

	t.Run("Add adds or creates", func(t *testing.T) {
		addr := tutil.NewIDAddr(t, 100)
		bt := buildBalanceTable()

		prev, err := bt.Get(addr)
		assert.NoError(t, err)
		assert.Equal(t, big.Zero(), prev)

		err = bt.Add(addr, abi.NewTokenAmount(10))
		assert.NoError(t, err)
		amount, err := bt.Get(addr)
		assert.NoError(t, err)
		assert.Equal(t, abi.NewTokenAmount(10), amount)

		err = bt.Add(addr, abi.NewTokenAmount(20))
		assert.NoError(t, err)
		amount, err = bt.Get(addr)
		assert.NoError(t, err)
		assert.Equal(t, abi.NewTokenAmount(30), amount)

		// Add negative to subtract.
		err = bt.Add(addr, abi.NewTokenAmount(-30))
		assert.NoError(t, err)
		amount, err = bt.Get(addr)
		assert.NoError(t, err)
		assert.Equal(t, abi.NewTokenAmount(0), amount)
		// The zero entry is not stored.
		found, err := ((*adt.Map)(bt)).Get(abi.AddrKey(addr), nil)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("Must subtract fails if account balance is insufficient", func(t *testing.T) {
		addr := tutil.NewIDAddr(t, 100)
		bt := buildBalanceTable()

		// Ok to subtract zero from nothing
		require.NoError(t, bt.MustSubtract(addr, abi.NewTokenAmount(0)))

		// Fail to subtract something from nothing
		require.Error(t, bt.MustSubtract(addr, abi.NewTokenAmount(1)))

		require.NoError(t, bt.Add(addr, abi.NewTokenAmount(5)))

		// Fail to subtract more than available
		require.Error(t, bt.MustSubtract(addr, abi.NewTokenAmount(6)))
		bal, err := bt.Get(addr)
		require.NoError(t, err)
		require.Equal(t, abi.NewTokenAmount(5), bal)

		// Ok to subtract less than available
		require.NoError(t, bt.MustSubtract(addr, abi.NewTokenAmount(4)))
		bal, err = bt.Get(addr)
		require.NoError(t, err)
		require.Equal(t, abi.NewTokenAmount(1), bal)
		// ...and the rest
		require.NoError(t, bt.MustSubtract(addr, abi.NewTokenAmount(1)))
		bal, err = bt.Get(addr)
		require.NoError(t, err)
		require.Equal(t, abi.NewTokenAmount(0), bal)

		// The zero entry is not stored.
		found, err := ((*adt.Map)(bt)).Get(abi.AddrKey(addr), nil)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("Total returns total amount tracked", func(t *testing.T) {
		addr1 := tutil.NewIDAddr(t, 100)
		addr2 := tutil.NewIDAddr(t, 101)

		bt := buildBalanceTable()
		total, err := bt.Total()
		assert.NoError(t, err)
		assert.Equal(t, big.Zero(), total)

		testCases := []struct {
			amount int64
			addr   address.Address
			total  int64
		}{
			{10, addr1, 10},
			{20, addr1, 30},
			{40, addr2, 70},
			{50, addr2, 120},
		}

		for _, tc := range testCases {
			err = bt.Add(tc.addr, abi.NewTokenAmount(tc.amount))
			assert.NoError(t, err)

			total, err = bt.Total()
			assert.NoError(t, err)
			assert.Equal(t, abi.NewTokenAmount(tc.total), total)
		}
	})
}

func TestSubtractWithMinimum(t *testing.T) {
	buildBalanceTable := func() *adt.BalanceTable {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		store := adt.AsStore(rt)
		emptyMap := adt.MakeEmptyMap(store)

		bt, err := adt.AsBalanceTable(store, tutil.MustRoot(t, emptyMap))
		require.NoError(t, err)
		return bt
	}
	addr := tutil.NewIDAddr(t, 100)
	zeroAmt := abi.NewTokenAmount(0)

	t.Run("ok with zero balance", func(t *testing.T) {
		bt := buildBalanceTable()
		s, err := bt.SubtractWithMinimum(addr, zeroAmt, zeroAmt)
		require.NoError(t, err)
		require.EqualValues(t, zeroAmt, s)
	})

	t.Run("withdraw available when account does not have sufficient balance", func(t *testing.T) {
		bt := buildBalanceTable()
		require.NoError(t, bt.Add(addr, abi.NewTokenAmount(5)))

		s, err := bt.SubtractWithMinimum(addr, abi.NewTokenAmount(2), abi.NewTokenAmount(4))
		require.NoError(t, err)
		require.EqualValues(t, abi.NewTokenAmount(1), s)

		remaining, err := bt.Get(addr)
		require.NoError(t, err)
		require.EqualValues(t, abi.NewTokenAmount(4), remaining)
	})

	t.Run("account has sufficient balance", func(t *testing.T) {
		bt := buildBalanceTable()
		require.NoError(t, bt.Add(addr, abi.NewTokenAmount(5)))

		s, err := bt.SubtractWithMinimum(addr, abi.NewTokenAmount(3), abi.NewTokenAmount(2))
		require.NoError(t, err)
		require.EqualValues(t, abi.NewTokenAmount(3), s)

		remaining, err := bt.Get(addr)
		require.NoError(t, err)
		require.EqualValues(t, abi.NewTokenAmount(2), remaining)
	})
}
