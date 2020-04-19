package states

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	states0 "github.com/filecoin-project/specs-actors/actors/states"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

// Value type of the top level of the state tree.
// Represents the on-chain state of a single actor.
// type Actor struct {
// 	Code       cid.Cid // CID representing the code associated with the actor
// 	Head       cid.Cid // CID of the head state object for the actor
// 	CallSeqNum uint64  // CallSeqNum for the next message to be received by the actor (non-zero for accounts only)
// 	Balance    big.Int // Token balance of the actor
// }
type Actor = states0.Actor

// A specialization of a map of ID-addresses to actor heads.
type Tree struct {
	Map   *adt.Map
	Store adt.Store
}

// Initializes a new, empty state tree backed by a store.
func NewTree(store adt.Store) (*Tree, error) {
	emptyMap := adt.MakeEmptyMap(store)
	return &Tree{
		Map:   emptyMap,
		Store: store,
	}, nil
}

// Loads a tree from a root CID and store.
func LoadTree(s adt.Store, r cid.Cid) (*Tree, error) {
	m, err := adt.AsMap(s, r)
	if err != nil {
		return nil, err
	}
	return &Tree{
		Map:   m,
		Store: s,
	}, nil
}

// Writes the tree root node to the store, and returns its CID.
func (t *Tree) Flush() (cid.Cid, error) {
	return t.Map.Root()
}

// Loads the state associated with an address.
func (t *Tree) GetActor(addr address.Address) (*Actor, bool, error) {
	if addr.Protocol() != address.ID {
		return nil, false, xerrors.Errorf("non-ID address %v invalid as actor key", addr)
	}
	var actor Actor
	found, err := t.Map.Get(abi.AddrKey(addr), &actor)
	return &actor, found, err
}

// Sets the state associated with an address, overwriting if it already present.
func (t *Tree) SetActor(addr address.Address, actor *Actor) error {
	if addr.Protocol() != address.ID {
		return xerrors.Errorf("non-ID address %v invalid as actor key", addr)
	}
	return t.Map.Put(abi.AddrKey(addr), actor)
}

// Traverses all entries in the tree.
func (t *Tree) ForEach(fn func(addr address.Address, actor *Actor) error) error {
	var val Actor
	return t.Map.ForEach(&val, func(key string) error {
		addr, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		return fn(addr, &val)
	})
}

// Traverses all keys in the tree, without decoding the actor states.
func (t *Tree) ForEachKey(fn func(addr address.Address) error) error {
	return t.Map.ForEach(nil, func(key string) error {
		addr, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		return fn(addr)
	})
}
