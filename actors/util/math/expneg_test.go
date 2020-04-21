package math_test

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/xorcare/golden"

	"github.com/filecoin-project/specs-actors/v2/actors/util/math"
)

var Res big.Word

func BenchmarkExpneg(b *testing.B) {
	x := new(big.Int).SetUint64(14)
	x = x.Lsh(x, math.Precision128-3) // set x to 1.75
	dec := new(big.Int)
	dec = dec.Div(x, big.NewInt(int64(b.N)))
	b.ResetTimer()
	b.ReportAllocs()
	var res big.Word

	for i := 0; i < b.N; i++ {
		r := math.ExpNeg(x)
		res += r.Bits()[0]
		x.Sub(x, dec)
	}
	Res += res
}

func TestExpFunction(t *testing.T) {
	const N = 256

	step := big.NewInt(5)
	step = step.Lsh(step, math.Precision128) // Q.128
	step = step.Div(step, big.NewInt(N-1))

	x := big.NewInt(0)
	b := &bytes.Buffer{}

	b.WriteString("x, y\n")
	for i := 0; i < N; i++ {
		y := math.ExpNeg(x)
		fmt.Fprintf(b, "%s,%s\n", x, y)
		x = x.Add(x, step)
	}

	golden.Assert(t, b.Bytes())
}
