package local

import (
	"context"
)

// Iter2UnpackedNew creates a new closure which gets unpacked items.
//
// # Arguments
//   - packed2unpacked: Gets an unpacked item from a packed item.
//   - hasNext: Checks if an iterator has a next item.
//   - getPacked: Gets a packed item from an iterator.
//   - getError: Gets an error if exists.
func Iter2UnpackedNew[I, P, U any](
	packed2unpacked func(packed *P) (unpacked U, e error),
	hasNext func(iter I) bool,
	getPacked func(iter I, buf *P) error,
	getError func(iter I) error,
) func(ctx context.Context, iter I) (unpacked []U, e error) {
	return func(ctx context.Context, iter I) (unpacked []U, e error) {
		var buf P
		for hasNext(iter) {
			e = getPacked(iter, &buf)
			if nil != e {
				return nil, e
			}

			unpackedItem, e := packed2unpacked(&buf)
			if nil != e {
				return nil, e
			}

			unpacked = append(unpacked, unpackedItem)
		}

		e = getError(iter)
		if nil != e {
			return nil, e
		}

		return
	}
}

// Iter2UnpackedWithFilterNew creates a new closure which gets required unpacked items.
//
// # Arguments
//   - packed2unpacked: Gets an unpacked item from a packed item.
//   - hasNext: Checks if an iterator has a next item.
//   - getPacked: Gets a packed item from an iterator.
//   - getError: Gets an error if exists.
//   - coarseFilter: Checks if a packed value is required or not.
//   - fineFilter: Checks if an unpacked value is required or not.
func Iter2UnpackedWithFilterNew[I, P, U, F any](
	packed2unpacked func(packed *P) (unpacked U, e error),
	hasNext func(iter I) bool,
	getPacked func(iter I, buf *P) error,
	getError func(iter I) error,
	coarseFilter func(packed *P, filter *F) (keep bool),
	fineFilter func(unpacked *U, filter *F) (keep bool),
) func(ctx context.Context, iter I, filter *F) (unpacked []U, e error) {
	return func(ctx context.Context, iter I, filter *F) (unpacked []U, e error) {
		var buf P
		for hasNext(iter) {
			e = getPacked(iter, &buf)
			if nil != e {
				return nil, e
			}

			var keepCoarse bool = coarseFilter(&buf, filter)
			if !keepCoarse {
				continue
			}

			unpackedItem, e := packed2unpacked(&buf)
			if nil != e {
				return nil, e
			}

			var keepFine bool = fineFilter(&unpackedItem, filter)
			if !keepFine {
				continue
			}

			unpacked = append(unpacked, unpackedItem)
		}

		e = getError(iter)
		if nil != e {
			return nil, e
		}

		return
	}
}

// IterConsumer may consume a value.
//
// # Return value
//   - stop: Must be true in order to stop the iteration.
//   - e:    Must not be nil to propagate the error.
type IterConsumer[T any] func(value *T) (stop bool, e error)

func (c IterConsumer[T]) ConsumeMany(values []T) (stop bool, e error) {
	for _, val := range values {
		var item T = val
		stop, e := c(&item)
		if nil != e {
			return false, e
		}
		if stop {
			return true, nil
		}
	}
	return
}

// IterConsumerFilterMany processes filtered items.
//
// # Arguments
//   - consumer: Processes a filtered item.
//   - items: Items to be filtered.
//   - filterFunc: Checks if an item must be used or not.
//   - filter: A filter used by filterFunc.
func IterConsumerFilterMany[T, F any](
	consumer IterConsumer[T],
	items []T,
	filterFunc func(item *T, filter *F) (keep bool),
	filter *F,
) (stop bool, e error) {
	for _, item := range items {
		var i T = item
		var keep bool = filterFunc(&i, filter)
		if !keep {
			continue
		}
		stop, e := consumer(&i)
		if nil != e {
			return stop, e
		}
		if stop {
			return stop, nil
		}
	}
	return
}

// IterConsumerNewPacked creates a new packed item consumer from an unpacked consumer.
func IterConsumerNewPacked[P, U any](
	unpack func(packed *P) (unpacked []U, e error),
	consumer IterConsumer[U],
) IterConsumer[P] {
	return func(packed *P) (stop bool, e error) {
		unpacked, e := unpack(packed)
		if nil != e {
			return
		}
		for _, unpackedItem := range unpacked {
			var item U = unpackedItem
			stop, err := consumer(&item)
			if nil != err {
				return true, err
			}
			if stop {
				return true, nil
			}
		}
		return
	}
}

// Iter2ConsumerNewFiltered creates a closure which consumes filtered values.
//
// # Arguments
//   - iterNext: Checks if an iterator has a next item or not.
//   - iterGet: Gets a next value.
//   - iterErr: Gets an error from an iterator.
//   - keep: Checks if a value must be consumed or not.
//   - consumer: Processes a value.
func Iter2ConsumerNewFiltered[I, T, F any](
	iterNext func(iter I) bool,
	iterGet func(iter I, value *T) error,
	iterErr func(iter I) error,
	keep func(filter *F, value *T) bool,
	consumer IterConsumer[T],
) func(ctx context.Context, iter I, buf *T, filter *F) error {
	return func(ctx context.Context, iter I, buf *T, filter *F) (e error) {
		for iterNext(iter) {
			e = iterGet(iter, buf)
			if nil != e {
				return
			}
			var ignore bool = !keep(filter, buf)
			if ignore {
				continue
			}
			stop, err := consumer(buf)
			if nil != err {
				return err
			}
			if stop {
				return nil
			}
		}
		return iterErr(iter)
	}
}

// Iter2ConsumerNewUnpacked creates a closure which consumes an unpacked items after filtering.
//
// # Arguments
//   - iterNext: Checks if an iterator has a next item or not.
//   - iterGet: Gets a packed item.
//   - iterErr: Gets an error from an iterator.
//   - unpack: Gets an unpacked item from a packed item.
//   - filterCoarse: Checks if a packed item must be used or not.
//   - filterFine: Checks if an unpacked item must be used or not.
//   - consumer: Processes an unpacked item.
func Iter2ConsumerNewUnpacked[I, P, F, U any](
	iterNext func(iter I) bool,
	iterGet func(iter I, packed *P) error,
	iterErr func(iter I) error,
	unpack func(packed *P) (unpacked U, e error),
	filterCoarse func(packed *P, filter *F) (keep bool),
	filterFine func(unpacked *U, filter *F) (keep bool),
	consumer IterConsumer[U],
) func(ctx context.Context, iter I, buf *P, filter *F) error {
	return func(ctx context.Context, iter I, buf *P, filter *F) (e error) {
		for iterNext(iter) {
			e = iterGet(iter, buf)
			if nil != e {
				return
			}

			var keepCoarse bool = filterCoarse(buf, filter)
			if !keepCoarse {
				continue
			}

			unpacked, err := unpack(buf)
			if nil != err {
				return err
			}

			var keepFine bool = filterFine(&unpacked, filter)
			if !keepFine {
				continue
			}

			stop, err := consumer(&unpacked)
			if nil != err {
				return err
			}

			if stop {
				return nil
			}
		}
		return iterErr(iter)
	}
}

type IterConsumerFiltered[T, F any] func(value *T, filter *F) (stop bool, e error)

func ConsumerUnpackedNew[P, U, F any](
	unpackedConsumer IterConsumerFiltered[U, F],
	packed2unpacked func(packed *P) (unpacked []U, e error),
	filterPacked func(packed *P, filter *F) (keep bool),
) IterConsumerFiltered[P, F] {
	return func(packed *P, filter *F) (stop bool, e error) {
		var keep bool = filterPacked(packed, filter)
		if !keep {
			return false, nil
		}
		unpacked, e := packed2unpacked(packed)
		if nil != e {
			return true, e
		}
		for _, unpackedItem := range unpacked {
			var item U = unpackedItem
			stop, e := unpackedConsumer(&item, filter)
			if nil != e {
				return stop, e
			}
			if stop {
				return true, nil
			}
		}
		return false, nil
	}
}

func ConsumerDecodedNew[E, D, F any](
	decodedConsumer IterConsumerFiltered[D, F],
	decoder func(encoded *E) (decoded D, e error),
	filterEncoded func(encoded *E, filter *F) (keep bool),
) IterConsumerFiltered[E, F] {
	return func(encoded *E, filter *F) (stop bool, e error) {
		var keep bool = filterEncoded(encoded, filter)
		if !keep {
			return false, nil
		}
		decoded, e := decoder(encoded)
		if nil != e {
			return true, e
		}
		return decodedConsumer(&decoded, filter)
	}
}

func IterConsumeManyFilteredNew[I, T, F any](
	iterNext func(iter I) (hasNext bool),
	iterGet func(iter I, value *T) error,
	iterErr func(iter I) error,
) func(iter I, filter *F, consumer IterConsumerFiltered[T, F], buf *T) (stop bool, e error) {
	return func(iter I, filter *F, cf IterConsumerFiltered[T, F], buf *T) (stop bool, e error) {
		for iterNext(iter) {
			e = iterGet(iter, buf)
			if nil != e {
				return true, e
			}
			stop, e = cf(buf, filter)
			if nil != e {
				return stop, e
			}
			if stop {
				return true, nil
			}
		}
		return false, iterErr(iter)
	}
}
