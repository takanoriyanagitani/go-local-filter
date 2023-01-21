package local

import (
	"context"
)

func GetByKeyNewDecoded[G, K, E, D any](
	getEncodedByKey func(ctx context.Context, con G, key K, encoded *E) (got bool, e error),
	decoder Decode[*E, D],
	buf *E,
) func(ctx context.Context, con G, key K, decoded *D) (got bool, e error) {
	return func(ctx context.Context, con G, key K, decoded *D) (got bool, e error) {
		got, e = getEncodedByKey(ctx, con, key, buf)
		if nil != e {
			return false, e
		}
		if !got {
			return false, nil
		}
		dec, e := decoder(buf)
		if nil != e {
			return false, e
		}
		*decoded = dec
		return true, nil
	}
}

func GetByKeysNewUnnested[G, K, P, F, U any](
	getByKey func(ctx context.Context, con G, key K, packed *P) (got bool, e error),
	unnest Unnest[P, U],
	filterPacked func(packed *P, filter *F) (keep bool),
	filterUnpacked func(unpacked *U, filter *F) (keep bool),
	consumeUnpacked IterConsumer[U],
) func(ctx context.Context, keys []K, get G, buf *P, filter *F) error {
	return func(ctx context.Context, keys []K, con G, buf *P, filter *F) error {
		for _, key := range keys {
			got, e := getByKey(ctx, con, key, buf)
			if nil != e {
				return e
			}
			if !got {
				continue
			}

			var keepPacked bool = filterPacked(buf, filter)
			if !keepPacked {
				continue
			}

			unnested, e := unnest(buf)
			if nil != e {
				return e
			}

			stop, e := IterConsumerFilterMany(
				consumeUnpacked,
				unnested,
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
		return nil
	}
}
