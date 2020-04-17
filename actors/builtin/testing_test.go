package builtin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
)

func TestMessageAccumulator(t *testing.T) {
	t.Run("basics", func(t *testing.T) {
		acc := &builtin.MessageAccumulator{}
		assert.Equal(t, []string(nil), acc.Messages())
		assert.True(t, acc.Messages() == nil)
		assert.True(t, acc.IsEmpty())

		acc.Add("one")
		assert.False(t, acc.IsEmpty())
		acc.Addf("tw%s", "o")
		assert.False(t, acc.IsEmpty())
		assert.Equal(t, []string{"one", "two"}, acc.Messages())
	})

	t.Run("prefix", func(t *testing.T) {
		acc := &builtin.MessageAccumulator{}
		accA := acc.WithPrefix("A")

		accA.Add("aa")
		assert.Equal(t, []string{"Aaa"}, acc.Messages())
		assert.Equal(t, []string{"Aaa"}, accA.Messages())

		accAB := accA.WithPrefix("B")
		accAB.Add("bb")
		assert.Equal(t, []string{"Aaa", "ABbb"}, acc.Messages())
		assert.Equal(t, []string{"Aaa", "ABbb"}, accA.Messages())
		assert.Equal(t, []string{"Aaa", "ABbb"}, accA.Messages())

		func() {
			acc := acc.WithPrefix("C") // Shadow
			acc.Add("cc")
			assert.Equal(t, []string{"Aaa", "ABbb", "Ccc"}, acc.Messages())
		}()
		assert.Equal(t, []string{"Aaa", "ABbb", "Ccc"}, acc.Messages())
	})

	t.Run("merge", func(t *testing.T) {
		acc := &builtin.MessageAccumulator{}
		acc1 := &builtin.MessageAccumulator{}

		acc1.WithPrefix("A").Add("a1")
		acc1.WithPrefix("A").Add("a2")

		acc.AddAll(acc1)
		acc.WithPrefix("B").AddAll(acc1)

		assert.Equal(t, []string{"Aa1", "Aa2", "BAa1", "BAa2"}, acc.Messages())
	})
}
