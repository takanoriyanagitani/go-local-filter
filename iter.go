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
