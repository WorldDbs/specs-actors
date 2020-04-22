package states_test

import (
	"context"
	"fmt"
	"testing"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
)

func BenchmarkStateTreeSet(b *testing.B) {
	store := ipld.NewADTStore(context.Background())
	st, err := states.NewTree(store)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		a, err := address.NewIDAddress(uint64(i))
		if err != nil {
			b.Fatal(err)
		}
		err = st.SetActor(a, &states.Actor{
			Code:       builtin.StorageMinerActorCodeID,
			Head:       builtin.AccountActorCodeID,
			CallSeqNum: uint64(i),
			Balance:    big.NewInt(1258812523),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStateTreeSetFlush(b *testing.B) {
	store := ipld.NewADTStore(context.Background())
	st, err := states.NewTree(store)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		a, err := address.NewIDAddress(uint64(i))
		if err != nil {
			b.Fatal(err)
		}

		err = st.SetActor(a, &states.Actor{
			Code:       builtin.StorageMinerActorCodeID,
			Head:       builtin.AccountActorCodeID,
			CallSeqNum: uint64(i),
			Balance:    big.NewInt(1258812523),
		})
		if err != nil {
			b.Fatal(err)
		}

		_, err = st.Flush()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStateTree10kGetActor(b *testing.B) {
	store := ipld.NewADTStore(context.Background())
	st, err := states.NewTree(store)
	require.NoError(b, err)

	for i := 0; i < 10000; i++ {
		a, err := address.NewIDAddress(uint64(i))
		if err != nil {
			b.Fatal(err)
		}

		err = st.SetActor(a, &states.Actor{
			Code:       builtin.StorageMinerActorCodeID,
			Head:       builtin.AccountActorCodeID,
			CallSeqNum: uint64(i),
			Balance:    big.NewIntUnsigned(1258812523 + uint64(i)),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	_, err = st.Flush()
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		a, err := address.NewIDAddress(uint64(i % 10000))
		if err != nil {
			b.Fatal(err)
		}

		_, found, err := st.GetActor(a)
		if !found {
			b.Fatal(err)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSetCache(t *testing.T) {
	store := ipld.NewADTStore(context.Background())
	st, err := states.NewTree(store)
	require.NoError(t, err)

	a, err := address.NewIDAddress(uint64(222))
	if err != nil {
		t.Fatal(err)
	}

	act := &states.Actor{
		Code:       builtin.StorageMinerActorCodeID,
		Head:       builtin.AccountActorCodeID,
		CallSeqNum: 0,
		Balance:    big.NewInt(0),
	}

	err = st.SetActor(a, act)
	require.NoError(t, err)

	act.CallSeqNum = 1

	outact, found, err := st.GetActor(a)
	require.True(t, found)
	require.NoError(t, err)

	if outact.CallSeqNum == 1 {
		t.Error("nonce should not have updated")
	}
}

func TestStateTreeConsistency(t *testing.T) {
	store := ipld.NewADTStore(context.Background())
	st, err := states.NewTree(store)
	require.NoError(t, err)

	var addrs []address.Address
	for i := 100; i < 150; i++ {
		a, err := address.NewIDAddress(uint64(i))
		require.NoError(t, err)
		addrs = append(addrs, a)
	}

	randomCid, err := cid.Decode("bafy2bzacecu7n7wbtogznrtuuvf73dsz7wasgyneqasksdblxupnyovmtwxxu")
	require.NoError(t, err)

	for i, a := range addrs {
		err := st.SetActor(a, &states.Actor{
			Code:       randomCid,
			Head:       randomCid,
			CallSeqNum: uint64(1000 - i),
			Balance:    big.NewInt(int64(10000 + i)),
		})
		require.NoError(t, err)
	}

	root, err := st.Flush()
	require.NoError(t, err)

	fmt.Println("root is: ", root)
	if root.String() != "bafy2bzaceb2bhqw75pqp44efoxvlnm73lnctq6djair56bfn5x3gw56epcxbi" {
		t.Fatal("MISMATCH!")
	}
}
