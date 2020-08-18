package adt

import (
	"bytes"

	amt "github.com/filecoin-project/go-amt-ipld/v3"

	"github.com/filecoin-project/go-state-types/cbor"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

var DefaultAmtOptions = []amt.Option{}

// Array stores a sparse sequence of values in an AMT.
type Array struct {
	root  *amt.Root
	store Store
}

// AsArray interprets a store as an AMT-based array with root `r`.
func AsArray(s Store, r cid.Cid, bitwidth int) (*Array, error) {
	options := append(DefaultAmtOptions, amt.UseTreeBitWidth(uint(bitwidth)))
	root, err := amt.LoadAMT(s.Context(), s, r, options...)
	if err != nil {
		return nil, xerrors.Errorf("failed to root: %w", err)
	}

	return &Array{
		root:  root,
		store: s,
	}, nil
}

// Creates a new array backed by an empty AMT.
func MakeEmptyArray(s Store, bitwidth int) (*Array, error) {
	options := append(DefaultAmtOptions, amt.UseTreeBitWidth(uint(bitwidth)))
	root, err := amt.NewAMT(s, options...)
	if err != nil {
		return nil, err
	}
	return &Array{
		root:  root,
		store: s,
	}, nil
}

// Writes a new empty array to the store, returning its CID.
func StoreEmptyArray(s Store, bitwidth int) (cid.Cid, error) {
	arr, err := MakeEmptyArray(s, bitwidth)
	if err != nil {
		return cid.Undef, err
	}
	return arr.Root()
}

// Returns the root CID of the underlying AMT.
func (a *Array) Root() (cid.Cid, error) {
	return a.root.Flush(a.store.Context())
}

// Appends a value to the end of the array. Assumes continuous array.
// If the array isn't continuous use Set and a separate counter
func (a *Array) AppendContinuous(value cbor.Marshaler) error {
	if err := a.root.Set(a.store.Context(), a.root.Len(), value); err != nil {
		return xerrors.Errorf("append failed to set index %v value %v in root %v: %w", a.root.Len(), value, a.root, err)
	}
	return nil
}

func (a *Array) Set(i uint64, value cbor.Marshaler) error {
	if err := a.root.Set(a.store.Context(), i, value); err != nil {
		return xerrors.Errorf("failed to set index %v value %v in root %v: %w", i, value, a.root, err)
	}
	return nil
}

// Removes the value at index `i` from the AMT, if it exists.
// Returns whether the index was previously present.
func (a *Array) TryDelete(i uint64) (bool, error) {
	if found, err := a.root.Delete(a.store.Context(), i); err != nil {
		return false, xerrors.Errorf("array delete failed to delete index %v in root %v: %w", i, a.root, err)
	} else {
		return found, nil
	}
}

// Removes the value at index `i` from the AMT, expecting it to exist.
func (a *Array) Delete(i uint64) error {
	if found, err := a.root.Delete(a.store.Context(), i); err != nil {
		return xerrors.Errorf("failed to delete index %v in root %v: %w", i, a.root, err)
	} else if !found {
		return xerrors.Errorf("no such index %v in root %v to delete: %w", i, a.root, err)
	}
	return nil
}

func (a *Array) BatchDelete(ix []uint64, strict bool) error {
	if _, err := a.root.BatchDelete(a.store.Context(), ix, strict); err != nil {
		return xerrors.Errorf("failed to batch delete keys %v: %w", ix, err)
	}
	return nil
}

// Iterates all entries in the array, deserializing each value in turn into `out` and then calling a function.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (a *Array) ForEach(out cbor.Unmarshaler, fn func(i int64) error) error {
	return a.root.ForEach(a.store.Context(), func(k uint64, val *cbg.Deferred) error {
		if out != nil {
			if deferred, ok := out.(*cbg.Deferred); ok {
				// fast-path deferred -> deferred to avoid re-decoding.
				*deferred = *val
			} else if err := out.UnmarshalCBOR(bytes.NewReader(val.Raw)); err != nil {
				return err
			}
		}
		return fn(int64(k))
	})
}

func (a *Array) Length() uint64 {
	return a.root.Len()
}

// Get retrieves array element into the 'out' unmarshaler, returning a boolean
//  indicating whether the element was found in the array
func (a *Array) Get(k uint64, out cbor.Unmarshaler) (bool, error) {
	if found, err := a.root.Get(a.store.Context(), k, out); err != nil {
		return false, xerrors.Errorf("failed to get index %v in root %v: %w", k, a.root, err)
	} else {
		return found, nil
	}
}

// Retrieves an array value into the 'out' unmarshaler (if non-nil), and removes the entry.
// Returns a boolean indicating whether the element was previously in the array.
func (a *Array) Pop(k uint64, out cbor.Unmarshaler) (bool, error) {
	if found, err := a.root.Get(a.store.Context(), k, out); err != nil {
		return false, xerrors.Errorf("failed to get index %v in root %v: %w", k, a.root, err)
	} else if !found {
		return false, nil
	}

	if found, err := a.root.Delete(a.store.Context(), k); err != nil {
		return false, xerrors.Errorf("failed to delete index %v in root %v: %w", k, a.root, err)
	} else if !found {
		return false, xerrors.Errorf("can't find index %v to delete in root %v", k, a.root)
	}
	return true, nil
}
