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

// PushDown must return true to filter items by a remote service.
type PushDown[F any] func(filter F) (useRemoteFilter bool)

// ScanEstimate can be used to estimate costs to scan.
type ScanEstimate struct {
	scans   float64
	latency float64
}

// ToCost estimates the cost to scan.
func (e ScanEstimate) ToCost() float64 { return e.scans * e.latency }

// ScanEstimateNew creates an estimate.
//
// # Arguments
//   - numberOfScans: Expected number of scans.
//   - latencyPerScan: Expected latency for each scan.
func ScanEstimateNew(numberOfScans, latencyPerScan float64) ScanEstimate {
	return ScanEstimate{
		scans:   numberOfScans,
		latency: latencyPerScan,
	}
}

// UseIxScanByCount checks if scans using indices must be used or not.
//
// # Arguments
//   - limit: Max number of scans using indices(exclusive).
func (s ScanEstimate) UseIxScanByCount(limit float64) (useIxScan bool) {
	return s.scans < limit
}

// PushdownNewByIxScanLimit creates a PushDown which uses a ScanEstimate.
//
// # Arguments
//   - limit: Max number of scans using indices(exclusive).
//   - filter2scan: Gets a ScanEstimate by a filter.
func PushdownNewByIxScanLimit[F any](
	limit float64,
	filter2scan func(filter F) ScanEstimate,
) PushDown[F] {
	return func(filter F) (useRemoteFilter bool) {
		var s ScanEstimate = filter2scan(filter)
		return s.UseIxScanByCount(limit)
	}
}

type ScanEstimates struct {
	ix ScanEstimate
	sq ScanEstimate
}

func ScanEstimatesNew(ixscan, sqscan ScanEstimate) ScanEstimates {
	return ScanEstimates{
		ix: ixscan,
		sq: sqscan,
	}
}

func (s ScanEstimates) toIxScanCost() float64 { return s.ix.ToCost() }
func (s ScanEstimates) toSqScanCost() float64 { return s.sq.ToCost() }

func (s ScanEstimates) UseIxScan() bool { return s.toIxScanCost() < s.toSqScanCost() }

func PushdownNewByCost[F any](
	filter2estimates func(filter F) ScanEstimates,
) PushDown[F] {
	return func(filter F) (useRemoteFilter bool) {
		var s ScanEstimates = filter2estimates(filter)
		return s.UseIxScan()
	}
}

func (p PushDown[F]) And(other PushDown[F]) PushDown[F] {
	return func(filter F) (useRemoteFilter bool) {
		var a bool = p(filter)
		var b bool = other(filter)
		return a && b
	}
}
