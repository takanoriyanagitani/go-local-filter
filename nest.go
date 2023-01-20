package local

import (
	"context"
)

type Unnest[P, U any] func(packed *P) (unnested []U, e error)

// Iter2ConsumerNewUnnested creates a closure which consumes unnested items after filtering.
//
// # Arguments
//   - iterNext: Checks if an iterator has a next item or not.
//   - iterGet: Gets a packed item.
//   - iterErr: Gets an error from an iterator.
//   - unnest: Gets unnested items from a packed value.
//   - filterPacked: Checks if a packed item is required or not.
//   - filterUnpacked: Checks if an unpacked item is required or not.
//   - consumeUnpacked: Processes an unpacked item.
func Iter2ConsumerNewUnnested[I, P, F, U any](
	iterNext func(iter I) bool,
	iterGet func(iter I, packed *P) error,
	iterErr func(iter I) error,
	unnest Unnest[P, U],
	filterPacked func(packed *P, filter *F) (keep bool),
	filterUnpacked func(unpacked *U, filter *F) (keep bool),
	consumeUnpacked IterConsumer[U],
) func(ctx context.Context, iter I, buf *P, filter *F) error {
	return func(ctx context.Context, iter I, buf *P, filter *F) error {
		for iterNext(iter) {
			e := iterGet(iter, buf)
			if nil != e {
				return e
			}

			var keepPacked bool = filterPacked(buf, filter)
			if !keepPacked {
				continue
			}

			unpackedItems, e := unnest(buf)
			if nil != e {
				return e
			}

			stop, e := IterConsumerFilterMany(
				consumeUnpacked,
				unpackedItems,
				filterUnpacked,
				filter,
			)
			if nil != e {
				return e
			}
			if stop {
				return nil
			}
		}
		return iterErr(iter)
	}
}
