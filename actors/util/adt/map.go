package adt

import (
	"bytes"
	"crypto/sha256"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/cbor"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

// DefaultHamtOptions specifies default options used to construct Filecoin HAMTs.
// Specific HAMT instances may specify additional options, especially the bitwidth.
var DefaultHamtOptions = []hamt.Option{
	hamt.UseHashFunction(func(input []byte) []byte {
		res := sha256.Sum256(input)
		return res[:]
	}),
}

// Map stores key-value pairs in a HAMT.
type Map struct {
	lastCid cid.Cid
	root    *hamt.Node
	store   Store
}

// AsMap interprets a store as a HAMT-based map with root `r`.
// The HAMT is interpreted with branching factor 2^bitwidth.
// We could drop this parameter if https://github.com/filecoin-project/go-hamt-ipld/issues/79 is implemented.
func AsMap(s Store, root cid.Cid, bitwidth int) (*Map, error) {
	options := append(DefaultHamtOptions, hamt.UseTreeBitWidth(bitwidth))
	nd, err := hamt.LoadNode(s.Context(), s, root, options...)
	if err != nil {
		return nil, xerrors.Errorf("failed to load hamt node: %w", err)
	}

	return &Map{
		lastCid: root,
		root:    nd,
		store:   s,
	}, nil
}

// Creates a new map backed by an empty HAMT.
func MakeEmptyMap(s Store, bitwidth int) (*Map, error) {
	options := append(DefaultHamtOptions, hamt.UseTreeBitWidth(bitwidth))
	nd, err := hamt.NewNode(s, options...)
	if err != nil {
		return nil, err
	}
	return &Map{
		lastCid: cid.Undef,
		root:    nd,
		store:   s,
	}, nil
}

// Creates and stores a new empty map, returning its CID.
func StoreEmptyMap(s Store, bitwidth int) (cid.Cid, error) {
	m, err := MakeEmptyMap(s, bitwidth)
	if err != nil {
		return cid.Undef, err
	}
	return m.Root()
}

// Returns the root cid of underlying HAMT.
func (m *Map) Root() (cid.Cid, error) {
	if err := m.root.Flush(m.store.Context()); err != nil {
		return cid.Undef, xerrors.Errorf("failed to flush map root: %w", err)
	}

	c, err := m.store.Put(m.store.Context(), m.root)
	if err != nil {
		return cid.Undef, xerrors.Errorf("writing map root object: %w", err)
	}
	m.lastCid = c

	return c, nil
}

// Put adds value `v` with key `k` to the hamt store.
func (m *Map) Put(k abi.Keyer, v cbor.Marshaler) error {
	if err := m.root.Set(m.store.Context(), k.Key(), v); err != nil {
		return xerrors.Errorf("failed to set key %v value %v in node %v: %w", k.Key(), v, m.lastCid, err)
	}
	return nil
}

// Get retrieves the value at `k` into `out`, if the `k` is present and `out` is non-nil.
// Returns whether the key was found.
func (m *Map) Get(k abi.Keyer, out cbor.Unmarshaler) (bool, error) {
	if found, err := m.root.Find(m.store.Context(), k.Key(), out); err != nil {
		return false, xerrors.Errorf("failed to get key %v in node %v: %w", m.lastCid, k.Key(), err)
	} else {
		return found, nil
	}
}

// Has checks for the existence of a key without deserializing its value.
func (m *Map) Has(k abi.Keyer) (bool, error) {
	if found, err := m.root.Find(m.store.Context(), k.Key(), nil); err != nil {
		return false, xerrors.Errorf("failed to check key %v in node %v: %w", m.lastCid, k.Key(), err)
	} else {
		return found, nil
	}
}

// Sets key key `k` to value `v` iff the key is not already present.
func (m *Map) PutIfAbsent(k abi.Keyer, v cbor.Marshaler) (bool, error) {
	if modified, err := m.root.SetIfAbsent(m.store.Context(), k.Key(), v); err != nil {
		return false, xerrors.Errorf("failed to set key %v value %v in node %v: %w", k.Key(), v, m.lastCid, err)
	} else {
		return modified, nil
	}
}

// Removes the value at `k` from the hamt store, if it exists.
// Returns whether the key was previously present.
func (m *Map) TryDelete(k abi.Keyer) (bool, error) {
	if found, err := m.root.Delete(m.store.Context(), k.Key()); err != nil {
		return false, xerrors.Errorf("failed to delete key %v in node %v: %v", k.Key(), m.root, err)
	} else {
		return found, nil
	}
}

// Removes the value at `k` from the hamt store, expecting it to exist.
func (m *Map) Delete(k abi.Keyer) error {
	if found, err := m.root.Delete(m.store.Context(), k.Key()); err != nil {
		return xerrors.Errorf("failed to delete key %v in node %v: %v", k.Key(), m.root, err)
	} else if !found {
		return xerrors.Errorf("no such key %v to delete in node %v", k.Key(), m.root)
	}
	return nil
}

// Iterates all entries in the map, deserializing each value in turn into `out` and then
// calling a function with the corresponding key.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (m *Map) ForEach(out cbor.Unmarshaler, fn func(key string) error) error {
	return m.root.ForEach(m.store.Context(), func(k string, val *cbg.Deferred) error {
		if out != nil {
			// Why doesn't hamt.ForEach() just return the value as bytes?
			err := out.UnmarshalCBOR(bytes.NewReader(val.Raw))
			if err != nil {
				return err
			}
		}
		return fn(k)
	})
}

// Collects all the keys from the map into a slice of strings.
func (m *Map) CollectKeys() (out []string, err error) {
	err = m.ForEach(nil, func(key string) error {
		out = append(out, key)
		return nil
	})
	return
}

// Retrieves the value for `k` into the 'out' unmarshaler (if non-nil), and removes the entry.
// Returns a boolean indicating whether the element was previously in the map.
func (m *Map) Pop(k abi.Keyer, out cbor.Unmarshaler) (bool, error) {
	key := k.Key()
	if found, err := m.root.Find(m.store.Context(), key, out); err != nil || !found {
		return found, err
	}

	if found, err := m.root.Delete(m.store.Context(), key); err != nil {
		return false, err
	} else if !found {
		return false, xerrors.Errorf("failed to find key %v to delete", k.Key())
	}
	return true, nil
}
