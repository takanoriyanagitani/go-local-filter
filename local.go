package local

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

type LocalFilter[V, F any] func(value V, filter F) (keep bool)

func (l LocalFilter[V, F]) And(other LocalFilter[V, F]) LocalFilter[V, F] {
	return func(val V, flt F) (keep bool) {
		var a bool = l(val, flt)
		var b bool = other(val, flt)
		return a && b
	}
}
