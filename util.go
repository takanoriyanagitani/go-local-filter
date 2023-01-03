package local

func callEither[T, U any](callA bool, a, b func(T) (U, error)) func(T) (U, error) {
	if callA {
		return a
	}
	return b
}
