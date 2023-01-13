package local

// LocalFilterNew creates a new closure which returns only required values.
//
// # Arguments
//   - f: The closure which checks if a value required or not.
func LocalFilterNew[V, F any](f func(V, F) (keep bool)) func(all []V, filter F) []V {
	return func(all []V, filter F) (filtered []V) {
		for _, val := range all {
			var keep bool = f(val, filter)
			if keep {
				filtered = append(filtered, val)
			}
		}
		return
	}
}

// LocalFilter is a closure which checks if a value must be required or not.
type LocalFilter[V, F any] func(value V, filter F) (keep bool)

// And creates a new local filter which uses two closures to check a value.
func (l LocalFilter[V, F]) And(other LocalFilter[V, F]) LocalFilter[V, F] {
	return func(val V, flt F) (keep bool) {
		var a bool = l(val, flt)
		var b bool = other(val, flt)
		return a && b
	}
}
