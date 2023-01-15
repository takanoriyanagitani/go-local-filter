package local

import (
	"context"
)

type Decode[E, D any] func(encoded E) (decoded D, e error)

func (d Decode[E, D]) NewAll(
	all func(context.Context, Bucket) (encoded []E, e error),
) func(context.Context, Bucket) (decoded []D, e error) {
	return func(ctx context.Context, bkt Bucket) (decoded []D, e error) {
		return composeErr(
			func(b Bucket) ([]E, error) { return all(ctx, b) },
			func(encodedItems []E) (decoded []D, e error) {
				for _, encodedItem := range encodedItems {
					decodedItem, e := d(encodedItem)
					if nil != e {
						return nil, e
					}
					decoded = append(decoded, decodedItem)
				}
				return
			},
		)(bkt)
	}
}

// RemoteFilterNewDecoded gets decoded items from encoded items.
//
// # Arguments
//   - decode: Gets a decoded item from an encoded item.
//   - remote: Gets encoded items.
func RemoteFilterNewDecoded[E, D, F any](
	decode Decode[E, D],
	remote func(ctx context.Context, b Bucket, filter F) (encoded []E, e error),
) func(ctx context.Context, b Bucket, filter F) (decoded []D, e error) {
	return func(ctx context.Context, b Bucket, filter F) (decoded []D, e error) {
		return composeErr(
			func(bkt Bucket) (encoded []E, e error) { return remote(ctx, bkt, filter) },
			func(encoded []E) (decoded []D, e error) {
				for _, encodedItem := range encoded {
					decodedItem, e := decode(encodedItem)
					if nil != e {
						return nil, e
					}
					decoded = append(decoded, decodedItem)
				}
				return
			},
		)(b)
	}
}
