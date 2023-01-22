package local

func callEither[T, U any](callA bool, a, b func(T) (U, error)) func(T) (U, error) {
	if callA {
		return a
	}
	return b
}

func errFuncNew[T, U any](noerr func(T) U) func(T) (U, error) {
	return func(t T) (U, error) { return noerr(t), nil }
}

func doEither(doFormer bool, former func() error, latter func() error) error {
	switch doFormer {
	case true:
		return former()
	default:
		return latter()
	}
}
