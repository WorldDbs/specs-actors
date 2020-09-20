package adt

import (
	"github.com/filecoin-project/go-state-types/abi"
	cid "github.com/ipfs/go-cid"
)

// Set interprets a Map as a set, storing keys (with empty values) in a HAMT.
type Set struct {
	m *Map
}

// AsSet interprets a store as a HAMT-based set with root `r`.
// The HAMT is interpreted with branching factor 2^bitwidth.
func AsSet(s Store, r cid.Cid, bitwidth int) (*Set, error) {
	m, err := AsMap(s, r, bitwidth)
	if err != nil {
		return nil, err
	}

	return &Set{
		m: m,
	}, nil
}

// NewSet creates a new HAMT with root `r` and store `s`.
// The HAMT has branching factor 2^bitwidth.
func MakeEmptySet(s Store, bitwidth int) (*Set, error) {
	m, err := MakeEmptyMap(s, bitwidth)
	if err != nil {
		return nil, err
	}
	return &Set{m}, nil
}

// Root return the root cid of HAMT.
func (h *Set) Root() (cid.Cid, error) {
	return h.m.Root()
}

// Put adds `k` to the set.
func (h *Set) Put(k abi.Keyer) error {
	return h.m.Put(k, nil)
}

// Has returns true iff `k` is in the set.
func (h *Set) Has(k abi.Keyer) (bool, error) {
	return h.m.Get(k, nil)
}

// Removes `k` from the set, if present.
// Returns whether the key was previously present.
func (h *Set) TryDelete(k abi.Keyer) (bool, error) {
	return h.m.TryDelete(k)
}

// Removes `k` from the set, expecting it to be present.
func (h *Set) Delete(k abi.Keyer) error {
	return h.m.Delete(k)
}

// ForEach iterates over all values in the set, calling the callback for each value.
// Returning error from the callback stops the iteration.
func (h *Set) ForEach(cb func(k string) error) error {
	return h.m.ForEach(nil, cb)
}

// Collects all the keys from the set into a slice of strings.
func (h *Set) CollectKeys() (out []string, err error) {
	return h.m.CollectKeys()
}
