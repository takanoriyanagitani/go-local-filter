package local

import (
	"context"
)

// Unpack must get unpacked items from a packed item.
type Unpack[P, U any] func(packed P) (unpacked []U, e error)

// NewAll creates a closure which gets unpacked items.
//
// # Arguments
//   - all: Gets packed items.
func (u Unpack[P, U]) NewAll(
	all func(context.Context, Bucket) (packed []P, e error),
) func(context.Context, Bucket) (unpacked []U, e error) {
	return func(ctx context.Context, b Bucket) (unpacked []U, e error) {
		return composeErr(
			func(bkt Bucket) ([]P, error) { return all(ctx, bkt) },
			func(packed []P) (unpacked []U, e error) {
				for _, packedItem := range packed {
					chunk, e := u(packedItem)
					if nil != e {
						return nil, e
					}
					unpacked = append(unpacked, chunk...)
				}
				return
			},
		)(b)
	}
}

// RemoteFilterNewUnpacked creates a closure which gets unpacked items.
//
// # Arguments
//   - unpack: Gets unpacked items from a packed item.
//   - remote: Gets packed items.
func RemoteFilterNewUnpacked[P, U, F any](
	unpack Unpack[P, U],
	remote func(ctx context.Context, b Bucket, filter F) (packed []P, e error),
) func(ctx context.Context, b Bucket, filter F) (unpacked []U, e error) {
	return func(ctx context.Context, b Bucket, filter F) (unpacked []U, e error) {
		return composeErr(
			func(bkt Bucket) (packed []P, e error) { return remote(ctx, bkt, filter) },
			func(packed []P) (unpacked []U, e error) {
				for _, packedItem := range packed {
					chunk, e := unpack(packedItem)
					if nil != e {
						return nil, e
					}
					unpacked = append(unpacked, chunk...)
				}
				return
			},
		)(b)
	}
}
