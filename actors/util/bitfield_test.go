package util_test

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v2/actors/util"
)

func TestBitFieldUnset(t *testing.T) {
	bf := bitfield.New()
	bf.Set(1)
	bf.Set(2)
	bf.Set(3)
	bf.Set(4)
	bf.Set(5)

	bf.Unset(3)

	m, err := bf.AllMap(100)
	assert.NoError(t, err)
	_, found := m[3]
	assert.False(t, found)

	cnt, err := bf.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), cnt)

	bf2 := roundtripMarshal(t, bf)

	cnt, err = bf2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), cnt)

	m, err = bf.AllMap(100)
	assert.NoError(t, err)
	_, found = m[3]
	assert.False(t, found)
}

func roundtripMarshal(t *testing.T, in bitfield.BitField) bitfield.BitField {
	buf := new(bytes.Buffer)
	err := in.MarshalCBOR(buf)
	assert.NoError(t, err)

	bf2 := bitfield.New()
	err = bf2.UnmarshalCBOR(buf)
	assert.NoError(t, err)
	return bf2
}

func TestBitFieldContains(t *testing.T) {
	a := bitfield.New()
	a.Set(2)
	a.Set(4)
	a.Set(5)

	b := bitfield.New()
	b.Set(3)
	b.Set(4)

	c := bitfield.New()
	c.Set(2)
	c.Set(5)

	assertContainsAny := func(a, b bitfield.BitField, expected bool) {
		t.Helper()
		actual, err := util.BitFieldContainsAny(a, b)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}

	assertContainsAll := func(a, b bitfield.BitField, expected bool) {
		t.Helper()
		actual, err := util.BitFieldContainsAll(a, b)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}

	assertContainsAny(a, b, true)
	assertContainsAny(b, a, true)
	assertContainsAny(a, c, true)
	assertContainsAny(c, a, true)
	assertContainsAny(b, c, false)
	assertContainsAny(c, b, false)

	assertContainsAll(a, b, false)
	assertContainsAll(b, a, false)
	assertContainsAll(a, c, true)
	assertContainsAll(c, a, false)
	assertContainsAll(b, c, false)
	assertContainsAll(c, b, false)
}
