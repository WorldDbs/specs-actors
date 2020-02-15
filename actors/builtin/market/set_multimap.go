package market

import (
	"reflect"

	"github.com/filecoin-project/go-state-types/abi"
	cid "github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v4/actors/util/adt"
)

type SetMultimap struct {
	mp            *adt.Map
	store         adt.Store
	innerBitwidth int
}

// Interprets a store as a HAMT-based map of HAMT-based sets with root `r`.
// Both inner and outer HAMTs are interpreted with branching factor 2^bitwidth.
func AsSetMultimap(s adt.Store, r cid.Cid, outerBitwidth, innerBitwidth int) (*SetMultimap, error) {
	m, err := adt.AsMap(s, r, outerBitwidth)
	if err != nil {
		return nil, err
	}
	return &SetMultimap{mp: m, store: s, innerBitwidth: innerBitwidth}, nil
}

// Creates a new map backed by an empty HAMT and flushes it to the store.
// Both inner and outer HAMTs have branching factor 2^bitwidth.
func MakeEmptySetMultimap(s adt.Store, bitwidth int) (*SetMultimap, error) {
	m, err := adt.MakeEmptyMap(s, bitwidth)
	if err != nil {
		return nil, err
	}
	return &SetMultimap{mp: m, store: s, innerBitwidth: bitwidth}, nil
}

// Writes a new empty map to the store and returns its CID.
func StoreEmptySetMultimap(s adt.Store, bitwidth int) (cid.Cid, error){
	mm, err := MakeEmptySetMultimap(s, bitwidth)
	if err != nil {
		return cid.Undef, err
	}
	return mm.Root()
}

// Returns the root cid of the underlying HAMT.
func (mm *SetMultimap) Root() (cid.Cid, error) {
	return mm.mp.Root()
}

func (mm *SetMultimap) Put(epoch abi.ChainEpoch, v abi.DealID) error {
	// Load the hamt under key, or initialize a new empty one if not found.
	k := abi.UIntKey(uint64(epoch))
	set, found, err := mm.get(k)
	if err != nil {
		return err
	}
	if !found {
		set, err = adt.MakeEmptySet(mm.store, mm.innerBitwidth)
		if err != nil {
			return err
		}
	}

	// Add to the set.
	if err = set.Put(dealKey(v)); err != nil {
		return errors.Wrapf(err, "failed to add key to set %v", epoch)
	}

	src, err := set.Root()
	if err != nil {
		return xerrors.Errorf("failed to flush set root: %w", err)
	}
	// Store the new set root under key.
	newSetRoot := cbg.CborCid(src)
	err = mm.mp.Put(k, &newSetRoot)
	if err != nil {
		return errors.Wrapf(err, "failed to store set")
	}
	return nil
}

func (mm *SetMultimap) PutMany(epoch abi.ChainEpoch, vs []abi.DealID) error {
	// Load the hamt under key, or initialize a new empty one if not found.
	k := abi.UIntKey(uint64(epoch))
	set, found, err := mm.get(k)
	if err != nil {
		return err
	}
	if !found {
		set, err = adt.MakeEmptySet(mm.store, mm.innerBitwidth)
		if err != nil {
			return err
		}
	}

	// Add to the set.
	for _, v := range vs {
		if err = set.Put(dealKey(v)); err != nil {
			return errors.Wrapf(err, "failed to add key to set %v", epoch)
		}
	}

	src, err := set.Root()
	if err != nil {
		return xerrors.Errorf("failed to flush set root: %w", err)
	}
	// Store the new set root under key.
	newSetRoot := cbg.CborCid(src)
	err = mm.mp.Put(k, &newSetRoot)
	if err != nil {
		return errors.Wrapf(err, "failed to store set")
	}
	return nil
}

// Removes all values for a key.
func (mm *SetMultimap) RemoveAll(key abi.ChainEpoch) error {
	if _, err := mm.mp.TryDelete(abi.UIntKey(uint64(key))); err != nil {
		return xerrors.Errorf("failed to delete set key %v: %w", key, err)
	}
	return nil
}

// Iterates all entries for a key, iteration halts if the function returns an error.
func (mm *SetMultimap) ForEach(epoch abi.ChainEpoch, fn func(id abi.DealID) error) error {
	set, found, err := mm.get(abi.UIntKey(uint64(epoch)))
	if err != nil {
		return err
	}
	if found {
		return set.ForEach(func(k string) error {
			v, err := parseDealKey(k)
			if err != nil {
				return err
			}
			return fn(v)
		})
	}
	return nil
}

func (mm *SetMultimap) get(key abi.Keyer) (*adt.Set, bool, error) {
	var setRoot cbg.CborCid
	found, err := mm.mp.Get(key, &setRoot)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load set key %v", key)
	}
	var set *adt.Set
	if found {
		set, err = adt.AsSet(mm.store, cid.Cid(setRoot), mm.innerBitwidth)
		if err != nil {
			return nil, false, err
		}
	}
	return set, found, nil
}

func dealKey(e abi.DealID) abi.Keyer {
	return abi.UIntKey(uint64(e))
}

func parseDealKey(s string) (abi.DealID, error) {
	key, err := abi.ParseUIntKey(s)
	return abi.DealID(key), err
}

func init() {
	// Check that DealID is indeed an unsigned integer to confirm that dealKey is making the right interpretation.
	var e abi.DealID
	if reflect.TypeOf(e).Kind() != reflect.Uint64 {
		panic("incorrect sector number encoding")
	}
}
