package local

import (
	"context"
)

// FilterRemote gets filtered rows.
//
// If pushdown returns true, remote filter will be used
// and local filter will be ignored.
//
// If pushdown returns false, local filter will be used
// and remote filter will be ignored.
//
// # Arguments
//
//   - ctx: A context
//   - b: The bucket which may contain values.
//   - filter: A filter to filter values.
//   - all: Gets all values in a bucket.
//   - remote: Gets filtered values in a bucket.
//   - local: Gets a part of values.
//   - pushdown: Checks if a remote filter must be used or not.
func FilterRemote[V, F any](
	ctx context.Context,
	b Bucket,
	filter F,
	all func(context.Context, Bucket) ([]V, error),
	remote func(context.Context, Bucket, F) ([]V, error),
	local func([]V, F) []V,
	pushdown func(F) bool,
) (rows []V, e error) {
	var useRemoteFilter bool = pushdown(filter)
	return callEither(
		useRemoteFilter,
		func(bkt Bucket) ([]V, error) { return remote(ctx, bkt, filter) },
		func(bkt Bucket) ([]V, error) {
			values, e := all(ctx, bkt)
			return local(values, filter), e
		},
	)(b)
}

// FilterRemoteNew creates a new closure which gets filtered rows.
//
// # Arguments
//
//   - all: Gets all values in a bucket.
//   - remote: Gets filtered values in a bucket.
//   - local: Gets a part of values.
//   - pushdown: Checks if a remote filter must be used or not.
func FilterRemoteNew[V, F any](
	all func(context.Context, Bucket) ([]V, error),
	remote func(ctx context.Context, b Bucket, filter F) ([]V, error),
	local func(all []V, filter F) []V,
	pushdown func(filter F) bool,
) func(c context.Context, b Bucket, filter F) (rows []V, e error) {
	return func(ctx context.Context, b Bucket, filter F) (rows []V, e error) {
		return FilterRemote(
			ctx,
			b,
			filter,
			all,
			remote,
			local,
			pushdown,
		)
	}
}
