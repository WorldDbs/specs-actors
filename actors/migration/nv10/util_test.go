package nv10_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/v4/actors/migration/nv10"
	atesting "github.com/filecoin-project/specs-actors/v4/support/testing"
	"github.com/stretchr/testify/require"
)

func TestMemMigrationCache(t *testing.T) {
	cache := nv10.NewMemMigrationCache()
	cid1 := atesting.MakeCID("foo", nil)
	cid2 := atesting.MakeCID("bar", nil)
	require.NoError(t, cache.Write("first", cid1))

	// We see the key we wrote.
	found, result, err := cache.Read("first")
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, cid1, result)

	// Make sure we can't find random keys.
	found, _, err = cache.Read("other")
	require.False(t, found)
	require.NoError(t, err)

	newCache := cache.Clone()
	require.NoError(t, newCache.Write("second", cid2))

	// We see both keys.
	found, result, err = newCache.Read("first")
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, cid1, result)

	found, result, err = newCache.Read("second")
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, cid2, result)

	// But the original cache was not updated.
	found, _, err = cache.Read("second")
	require.False(t, found)
	require.NoError(t, err)

	// Now write back to the original cache.
	cache.Update(newCache)

	// And make sure that worked.
	found, result, err = cache.Read("second")
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, cid2, result)
}
