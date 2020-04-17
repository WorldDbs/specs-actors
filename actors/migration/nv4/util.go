package nv4

import (
	"bytes"
	"context"

	hamt0 "github.com/filecoin-project/go-hamt-ipld"
	hamt2 "github.com/filecoin-project/go-hamt-ipld/v2"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"

	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

// An adt.Map key that just preserves the underlying string.
type StringKey string

func (k StringKey) Key() string {
	return string(k)
}

// Migrates a HAMT from v0 (ipfs) to v2 (filecoin-project) without re-encoding keys or values.
func migrateHAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	inRootNode, err := hamt0.LoadNode(ctx, store, root, adt0.HamtOptions...)
	if err != nil {
		return cid.Undef, err
	}

	outRootNode := hamt2.NewNode(store, adt2.HamtOptions...)

	if err = inRootNode.ForEach(ctx, func(k string, val interface{}) error {
		return outRootNode.SetRaw(ctx, k, val.(*cbg.Deferred).Raw)
	}); err != nil {
		return cid.Undef, err
	}

	err = outRootNode.Flush(ctx)
	if err != nil {
		return cid.Undef, err
	}
	return store.Put(ctx, outRootNode)
}

// Migrates a HAMT of HAMTS from v0 (ipfs) to v2 (filecoin-project) without re-encoding leaf keys or values.
func migrateHAMTHAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	inRootNode, err := hamt0.LoadNode(ctx, store, root, adt0.HamtOptions...)
	if err != nil {
		return cid.Undef, err
	}
	outRootNode := hamt2.NewNode(store, adt2.HamtOptions...)

	if err = inRootNode.ForEach(ctx, func(k string, val interface{}) error {
		var inInner cbg.CborCid
		if err := inInner.UnmarshalCBOR(bytes.NewReader(val.(*cbg.Deferred).Raw)); err != nil {
			return err
		}

		outInner, err := migrateHAMTRaw(ctx, store, cid.Cid(inInner))
		if err != nil {
			return err
		}
		c := cbg.CborCid(outInner)
		return outRootNode.Set(ctx, k, &c)
	}); err != nil {
		return cid.Undef, err
	}

	err = outRootNode.Flush(ctx)
	if err != nil {
		return cid.Undef, err
	}
	return store.Put(ctx, outRootNode)

}
