package nv10

import (
	"bytes"
	"context"
	"sync"
	"testing"

	amt2 "github.com/filecoin-project/go-amt-ipld/v2"
	amt3 "github.com/filecoin-project/go-amt-ipld/v3"
	hamt2 "github.com/filecoin-project/go-hamt-ipld/v2"
	hamt3 "github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/filecoin-project/go-state-types/rt"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

	adt3 "github.com/filecoin-project/specs-actors/v4/actors/util/adt"
)

// Migrates a HAMT from v2 to v3 without re-encoding keys or values.
func migrateHAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newBitwidth int) (cid.Cid, error) {
	inRootNode, err := hamt2.LoadNode(ctx, store, root, adt2.HamtOptions...)
	if err != nil {
		return cid.Undef, err
	}

	newOpts := append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(newBitwidth))
	outRootNode, err := hamt3.NewNode(store, newOpts...)
	if err != nil {
		return cid.Undef, err
	}

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

// Migrates an AMT from v2 to v3 without re-encoding values.
func migrateAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newBitwidth int) (cid.Cid, error) {
	inRootNode, err := amt2.LoadAMT(ctx, store, root)
	if err != nil {
		return cid.Undef, err
	}

	newOpts := append(adt3.DefaultAmtOptions, amt3.UseTreeBitWidth(uint(newBitwidth)))
	outRootNode, err := amt3.NewAMT(store, newOpts...)
	if err != nil {
		return cid.Undef, err
	}

	if err = inRootNode.ForEach(ctx, func(k uint64, d *cbg.Deferred) error {
		return outRootNode.Set(ctx, k, d)
	}); err != nil {
		return cid.Undef, err
	}

	return outRootNode.Flush(ctx)
}

// Migrates a HAMT of HAMTs from v2 to v3 without re-encoding leaf keys or values.
func migrateHAMTHAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newOuterBitwidth, newInnerBitwidth int) (cid.Cid, error) {
	inRootNodeOuter, err := hamt2.LoadNode(ctx, store, root)
	if err != nil {
		return cid.Undef, err
	}

	newOptsOuter := append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(newOuterBitwidth))
	outRootNodeOuter, err := hamt3.NewNode(store, newOptsOuter...)
	if err != nil {
		return cid.Undef, err
	}

	if err = inRootNodeOuter.ForEach(ctx, func(k string, val interface{}) error {
		var inInner cbg.CborCid
		if err := inInner.UnmarshalCBOR(bytes.NewReader(val.(*cbg.Deferred).Raw)); err != nil {
			return err
		}
		outInner, err := migrateHAMTRaw(ctx, store, cid.Cid(inInner), newInnerBitwidth)
		if err != nil {
			return err
		}
		c := cbg.CborCid(outInner)
		return outRootNodeOuter.Set(ctx, k, &c)
	}); err != nil {
		return cid.Undef, err
	}

	if err := outRootNodeOuter.Flush(ctx); err != nil {
		return cid.Undef, err
	}
	return store.Put(ctx, outRootNodeOuter)
}

// Migrates a HAMT of AMTs from v2 to v3 without re-encoding values.
func migrateHAMTAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newOuterBitwidth, newInnerBitwidth int) (cid.Cid, error) {
	inRootNodeOuter, err := hamt2.LoadNode(ctx, store, root)
	if err != nil {
		return cid.Undef, err
	}
	newOptsOuter := append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(newOuterBitwidth))
	outRootNodeOuter, err := hamt3.NewNode(store, newOptsOuter...)
	if err != nil {
		return cid.Undef, err
	}

	if err = inRootNodeOuter.ForEach(ctx, func(k string, val interface{}) error {
		var inInner cbg.CborCid
		if err := inInner.UnmarshalCBOR(bytes.NewReader(val.(*cbg.Deferred).Raw)); err != nil {
			return err
		}
		outInner, err := migrateAMTRaw(ctx, store, cid.Cid(inInner), newInnerBitwidth)
		if err != nil {
			return err
		}
		c := cbg.CborCid(outInner)
		return outRootNodeOuter.Set(ctx, k, &c)
	}); err != nil {
		return cid.Undef, err
	}

	if err := outRootNodeOuter.Flush(ctx); err != nil {
		return cid.Undef, err
	}
	return store.Put(ctx, outRootNodeOuter)
}

type MemMigrationCache struct {
	MigrationMap sync.Map
}

func NewMemMigrationCache() *MemMigrationCache {
	return new(MemMigrationCache)
}

func (m *MemMigrationCache) Write(key string, c cid.Cid) error {
	m.MigrationMap.Store(key, c)
	return nil
}

func (m *MemMigrationCache) Read(key string) (bool, cid.Cid, error) {
	val, found := m.MigrationMap.Load(key)
	if !found {
		return false, cid.Undef, nil
	}
	c, ok := val.(cid.Cid)
	if !ok {
		return false, cid.Undef, xerrors.Errorf("non cid value in cache")
	}

	return true, c, nil
}

func (m *MemMigrationCache) Load(key string, loadFunc func() (cid.Cid, error)) (cid.Cid, error) {
	found, c, err := m.Read(key)
	if err != nil {
		return cid.Undef, err
	}
	if found {
		return c, nil
	}
	c, err = loadFunc()
	if err != nil {
		return cid.Undef, err
	}
	m.MigrationMap.Store(key, c)
	return c, nil
}

func (m *MemMigrationCache) Clone() *MemMigrationCache {
	newCache := NewMemMigrationCache()
	newCache.Update(m)
	return newCache
}

func (m *MemMigrationCache) Update(other *MemMigrationCache) {
	other.MigrationMap.Range(func(key, value interface{}) bool {
		m.MigrationMap.Store(key, value)
		return true
	})
}

type TestLogger struct {
	TB testing.TB
}

func (t TestLogger) Log(_ rt.LogLevel, msg string, args ...interface{}) {
	t.TB.Logf(msg, args...)
}
