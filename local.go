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
