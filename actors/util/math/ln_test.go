package math_test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/util/math"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNaturalLog(t *testing.T) {
	lnInputs := math.Parse([]string{
		"340282366920938463463374607431768211456",                       // Q.128 format of 1
		"924990000000000000000000000000000000000",                       // Q.128 format of e (rounded up in 5th decimal place to handle truncation)
		"34028236692093846346337460743176821145600000000000000000000",   // Q.128 format of 100e18
		"6805647338418769269267492148635364229120000000000000000000000", // Q.128 format of 2e22
		"204169000000000000000000000000000000",                          // Q.128 format of 0.0006
		"34028236692093846346337460743",                                 // Q.128 format of 1e-10
	})

	expectedLnOutputs := math.Parse([]string{
		"0", // Q.128 format of 0 = ln(1)
		"340282366920938463463374607431768211456",   // Q.128 format of 1 = ln(e)
		"15670582109617661336106769654068947397831", // Q.128 format of 46.051... = ln(100e18)
		"17473506083804940763855390762239996622013", // Q.128 format of  51.35... = ln(2e22)
		"-2524410000000000000000000000000000000000", // Q.128 format of -7.41.. = ln(0.0006)
		"-7835291054808830668053384827034473698915", // Q.128 format of -23.02.. = ln(1e-10)
	})
	fmt.Printf("%v %v\n", lnInputs, expectedLnOutputs)
	require.Equal(t, len(lnInputs), len(expectedLnOutputs))
	for i := 0; i < len(lnInputs); i++ {
		z := big.NewFromGo(lnInputs[i])
		lnOfZ := math.Ln(z)
		expectedZ := big.NewFromGo(expectedLnOutputs[i])
		assert.Equal(t, big.Rsh(expectedZ, math.Precision128), big.Rsh(lnOfZ, math.Precision128), "failed ln of %v", z)
	}
}
