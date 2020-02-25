package adt_test

import (
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v4/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v4/support/mock"
)

func TestArrayNotFound(t *testing.T) {
	rt := mock.NewBuilder(address.Undef).Build(t)
	store := adt.AsStore(rt)
	arr, err := adt.MakeEmptyArray(store, 3)
	require.NoError(t, err)

	found, err := arr.Get(7, nil)
	require.NoError(t, err)
	require.False(t, found)
}
