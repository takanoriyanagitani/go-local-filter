package local

import (
	"context"
)

// GetByKeyNewDecoded creates a closure which gets a decoded item.
//
// # Arguments
//   - getEncodedByKey: Gets an encoded item.
//   - decoder: Gets a decoded item from an encoded item.
//   - buf: The buffer to save an encoded item.
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

// GetByKeysNewUnnested creates a closure which gets unnested items.
//
// # Arguments
//   - getByKey: Gets a packed item by a key.
//   - unnest: Gets unnested items from a packed item.
//   - filterPacked: Checks if a packed item must be used or not.
//   - filterUnpacked: Check if an unpacked item must be used or not.
//   - consumeUnpacked: Uses an unpacked item.
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

// GetKeys must return keys from a bucket using a filter.
//
// # Arguments
//   - ctx: A context.
//   - con: A data store which may contain keys.
//   - bucket: A bucket which may contain keys.
//   - filter: A filter to minimize keys to get.
type GetKeys[D, B, F, K any] func(
	ctx context.Context,
	con D,
	bucket *B,
	filter *F,
) (keys []K, e error)

// WithBucketFilter creates a new GetKeys which gets keys after checking a bucket.
//
// # Arguments
//   - checkBucket: Must return true if a bucket must be checked.
func (g GetKeys[D, B, F, K]) WithBucketFilter(
	checkBucket func(ctx context.Context, con D, bucket *B, filter *F) (checkMe bool),
) GetKeys[D, B, F, K] {
	return func(ctx context.Context, con D, bucket *B, filter *F) (keys []K, e error) {
		var checkMe bool = checkBucket(ctx, con, bucket, filter)
		if !checkMe {
			return nil, nil
		}
		return g(ctx, con, bucket, filter)
	}
}

// GetByKey must get a value(if exists).
//
// # Arguments
//   - ctx: A context.
//   - con: A data store connection.
//   - bucket: A bucket which may have an item to get.
//   - key: The key of the item to get.
//   - val: The buffer to save an item info.
//   - filter: The filter which may be used to get or skip getting an item.
type GetByKey[D, B, F, K, V any] func(
	ctx context.Context,
	con D,
	bucket *B,
	key K,
	val *V,
	filter *F,
) (got bool, e error)

// GetByKeyDecodedNew creates a new closure which gets a decoded value.
//
// # Arguments
//   - getEncodedByKey: Gets an encoded value.
//   - decoder: Gets a decoded value from an encoded value.
//   - buf: A buffer to save an encoded item.
//   - filterDecoded: Checks if a decoded item must be used or not.
func GetByKeyDecodedNew[G, B, F, K, E, D any](
	getEncodedByKey GetByKey[G, B, F, K, E],
	decoder func(encoded *E) (decoded D, e error),
	buf *E,
	filterDecoded func(decoded *D, filter *F) (keep bool),
) GetByKey[G, B, F, K, D] {
	return func(
		ctx context.Context,
		con G,
		bucket *B,
		key K,
		val *D,
		filter *F,
	) (got bool, e error) {
		got, e = getEncodedByKey(ctx, con, bucket, key, buf, filter)
		if nil != e {
			return got, e
		}
		decoded, e := decoder(buf)
		if nil != e {
			return false, e
		}
		var keep bool = filterDecoded(&decoded, filter)
		if !keep {
			return false, nil
		}
		*val = decoded
		return true, nil
	}
}

type Got2Consumer[D, K, F, B, V any] func(
	ctx context.Context,
	con D,
	bucket *B,
	filter *F,
	buf *V,
	consumer func(val *V, filter *F) (stop bool, e error),
) error

func GetByKeysNew[G, K, F, B, V any](
	getKeys GetKeys[G, B, F, K],
	getByKey GetByKey[G, B, F, K, V],
) Got2Consumer[G, K, F, B, V] {
	return func(
		ctx context.Context,
		con G,
		bucket *B,
		filter *F,
		buf *V,
		consumer func(val *V, filter *F) (stop bool, e error),
	) error {
		keys, e := getKeys(ctx, con, bucket, filter)
		if nil != e {
			return e
		}
		for _, key := range keys {
			got, e := getByKey(ctx, con, bucket, key, buf, filter)
			if nil != e {
				return e
			}
			if !got {
				continue
			}
			stop, e := consumer(buf, filter)
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

// GetWithPlanNew creates a closure which get items.
//
// # Arguments
//   - getByKeys: Gets items using keys(indirect scan).
//   - getDirect: Gets items(direct scan).
//   - plan:      Checks if the scan must be direct or not.
func GetWithPlanNew[G, K, F, B, V any](
	getByKeys Got2Consumer[G, K, F, B, V],
	getDirect Got2Consumer[G, K, F, B, V],
	plan func(filter *F) (directScan bool),
) func(
	ctx context.Context,
	con G,
	bucket *B,
	filter *F,
	buf *V,
	consumer func(val *V, filter *F) (stop bool, e error),
) error {
	return func(
		ctx context.Context,
		con G,
		bucket *B,
		filter *F,
		buf *V,
		consumer func(val *V, filter *F) (stop bool, e error),
	) error {
		var useDirectScan bool = plan(filter)
		return doEither(
			useDirectScan,
			func() error { return getDirect(ctx, con, bucket, filter, buf, consumer) },
			func() error { return getByKeys(ctx, con, bucket, filter, buf, consumer) },
		)
	}
}
