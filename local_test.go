package local

import (
	"testing"
)

func assertNil(e any) func(*testing.T) {
	return func(t *testing.T) {
		if nil != e {
			t.Fatalf("Must be nil: %v\n", e)
		}
	}
}

func assertEqNew[T any](cmp func(a, b T) (same bool)) func(a, b T) func(*testing.T) {
	return func(a, b T) func(*testing.T) {
		return func(t *testing.T) {
			var same bool = cmp(a, b)
			var diff bool = !same
			if diff {
				t.Errorf("a != b\n")
				t.Errorf("a: %v\n", a)
				t.Fatalf("b: %v\n", b)
			}
		}
	}
}

func assertEq[T comparable](a, b T) func(*testing.T) {
	var f func(a, b T) func(*testing.T) = assertEqNew(
		func(a, b T) (same bool) {
			return a == b
		},
	)
	return f(a, b)
}

type localFilterSample struct {
	lbi int32
	ube int32
}

func TestLocal(t *testing.T) {
	t.Parallel()

	t.Run("LocalFilterNew", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var empty []int
			filter := LocalFilterNew[int, localFilterSample](nil)
			var filtered []int = filter(empty, localFilterSample{})
			t.Run("0 length", assertEq(len(filtered), 0))
		})

		t.Run("non-empty", func(t *testing.T) {
			t.Parallel()

			var sampleRows []int32 = []int32{
				634,
				3776,
				333,
				599,
			}
			filter := LocalFilterNew[int32, localFilterSample](
				func(val int32, flt localFilterSample) (keep bool) {
					var lbi int32 = flt.lbi
					var ube int32 = flt.ube
					return lbi <= val && val < ube
				},
			)
			var filtered []int32 = filter(sampleRows, localFilterSample{
				lbi: 599,
				ube: 3776,
			})
			t.Run("2 rows", assertEq(len(filtered), 2))
		})
	})
}
