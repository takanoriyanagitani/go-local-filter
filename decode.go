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
