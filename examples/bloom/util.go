package main

func error1st(f []func() error) error {
	for _, item := range f {
		e := item()
		if nil != e {
			return e
		}
	}
	return nil
}
