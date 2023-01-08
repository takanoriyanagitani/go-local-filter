package local

import (
	"context"
)

type Unpack[P, U any] func(packed P) (unpacked []U)

func (u Unpack[P, U]) NewAll(
	all func(context.Context, Bucket) (packed []P, e error),
) func(context.Context, Bucket) (unpacked []U, e error) {
	return func(ctx context.Context, b Bucket) (unpacked []U, e error) {
		return composeErr(
			func(bkt Bucket) ([]P, error) { return all(ctx, bkt) },
			func(packed []P) (unpacked []U, e error) {
				for _, packedItem := range packed {
					unpacked = append(unpacked, u(packedItem)...)
				}
				return
			},
		)(b)
	}
}

func RemoteFilterNewUnpacked[P, U, F any](
	unpack Unpack[P, U],
	remote func(ctx context.Context, b Bucket, filter F) (packed []P, e error),
) func(ctx context.Context, b Bucket, filter F) (unpacked []U, e error) {
	return func(ctx context.Context, b Bucket, filter F) (unpacked []U, e error) {
		return composeErr(
			func(bkt Bucket) (packed []P, e error) { return remote(ctx, bkt, filter) },
			func(packed []P) (unpacked []U, e error) {
				for _, packedItem := range packed {
					unpacked = append(unpacked, unpack(packedItem)...)
				}
				return
			},
		)(b)
	}
}
