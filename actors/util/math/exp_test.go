package math_test

import (
	"testing"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/util/math"
	"github.com/stretchr/testify/assert"
)

func TestExpBySquaring(t *testing.T) {
	one := big.Lsh(big.NewInt(1), math.Precision128)
	two := big.Lsh(big.NewInt(2), math.Precision128)
	three := big.Lsh(big.NewInt(3), math.Precision128)
	five := big.Lsh(big.NewInt(5), math.Precision128)
	seven := big.Lsh(big.NewInt(7), math.Precision128)

	assert.Equal(t, one, math.ExpBySquaring(three, int64(0)))
	assert.Equal(t, one, math.ExpBySquaring(one, 1))
	assert.Equal(t, three, math.ExpBySquaring(three, 1))

	assert.Equal(t,
		big.Lsh(big.NewInt(70368744177664), math.Precision128),
		math.ExpBySquaring(two, 46),
	)
	assert.Equal(t,
		big.Lsh(big.NewInt(3486784401), math.Precision128),
		math.ExpBySquaring(three, 20),
	)
	assert.Equal(t,
		big.Lsh(big.NewInt(1953125), math.Precision128),
		math.ExpBySquaring(five, 9),
	)
	assert.Equal(t,
		big.Lsh(big.NewInt(117649), math.Precision128),
		math.ExpBySquaring(seven, 6),
	)
}
