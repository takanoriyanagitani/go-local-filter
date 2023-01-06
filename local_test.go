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
