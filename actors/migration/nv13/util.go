package nv13

import (
	"sync"
	"testing"

	"github.com/filecoin-project/go-state-types/rt"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

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
