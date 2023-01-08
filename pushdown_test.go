package local

import (
	"context"
	"testing"
)

type filter struct {
	timestampLbi string
	timestampUbi string
}

type item struct {
	key string
	val string
}

type testFilterUnixtime struct {
	lbi float64
	ubi float64
}

func (t testFilterUnixtime) toDuration() float64 { return t.ubi - t.lbi }
func (t testFilterUnixtime) estimateScansByRate(maxRowsPerSecond float64) float64 {
	var duration float64 = t.toDuration()
	return duration * maxRowsPerSecond
}

func TestPushdown(t *testing.T) {
	t.Parallel()
	t.Run("FilterRemote", func(t *testing.T) {
		t.Parallel()

		t.Run("local-only-nop-filter", func(t *testing.T) {
			t.Parallel()

			var ctx context.Context = context.Background()
			var bkt Bucket = BucketNew("items_2023_01_16_cafef00ddeadbeafface864299792458")
			var flt filter = filter{
				timestampLbi: "01:21:25.0Z",
				timestampUbi: "01:23:04.8Z",
			}
			all := func(_ context.Context, b Bucket) ([]item, error) {
				return []item{
					{key: "01:20:26.0Z", val: `{}`},
					{key: "01:21:26.0Z", val: `{}`},
					{key: "01:22:26.0Z", val: `{}`},
					{key: "01:23:26.0Z", val: `{}`},
					{key: "01:24:26.0Z", val: `{}`},
				}, nil
			}
			local := func(items []item, f filter) []item {
				return items
			}
			pushdown := func(_ filter) bool { return false }

			filtered, e := FilterRemote(
				ctx,
				bkt,
				flt,
				all,
				nil,
				local,
				pushdown,
			)

			t.Run("No error", assertNil(e))
			t.Run("Length match", assertEq(len(filtered), 5))
		})

		t.Run("local-only-filter", func(t *testing.T) {
			t.Parallel()

			var ctx context.Context = context.Background()
			var bkt Bucket = BucketNew("items_2023_01_16_cafef00ddeadbeafface864299792458")
			var flt filter = filter{
				timestampLbi: "01:21:25.0Z",
				timestampUbi: "01:24:04.8Z",
			}
			all := func(_ context.Context, b Bucket) ([]item, error) {
				return []item{
					{key: "01:20:26.0Z", val: `{}`},
					{key: "01:21:26.0Z", val: `{}`},
					{key: "01:22:26.0Z", val: `{}`},
					{key: "01:23:26.0Z", val: `{}`},
					{key: "01:24:26.0Z", val: `{}`},
				}, nil
			}
			local := func(items []item, f filter) []item {
				var slow []item
				for _, i := range items {
					var key string = i.key
					if f.timestampLbi <= key && key <= f.timestampUbi {
						slow = append(slow, i)
					}
				}
				return slow
			}
			pushdown := func(_ filter) bool { return false }

			filtered, e := FilterRemote(
				ctx,
				bkt,
				flt,
				all,
				nil,
				local,
				pushdown,
			)

			t.Run("No error", assertNil(e))
			t.Run("Length match", assertEq(len(filtered), 3))
		})

		t.Run("remote-only-filter", func(t *testing.T) {
			t.Parallel()

			var ctx context.Context = context.Background()
			var bkt Bucket = BucketNew("items_2023_01_16_cafef00ddeadbeafface864299792458")
			var flt filter = filter{
				timestampLbi: "01:21:25.0Z",
				timestampUbi: "01:24:04.8Z",
			}
			pushdown := func(_ filter) bool { return true }
			rmt := func(_c context.Context, _b Bucket, _f filter) ([]item, error) {
				return []item{
					{key: "01:21:26.0Z", val: `{}`},
					{key: "01:22:26.0Z", val: `{}`},
					{key: "01:23:26.0Z", val: `{}`},
				}, nil
			}

			filtered, e := FilterRemote(
				ctx,
				bkt,
				flt,
				nil,
				rmt,
				nil,
				pushdown,
			)

			t.Run("No error", assertNil(e))
			t.Run("Length match", assertEq(len(filtered), 3))
		})

	})

	t.Run("FilterRemoteNew", func(t *testing.T) {
		t.Parallel()

		t.Run("remote-only-filter", func(t *testing.T) {
			t.Parallel()

			var ctx context.Context = context.Background()
			var bkt Bucket = BucketNew("items_2023_01_16_cafef00ddeadbeafface864299792458")
			var flt filter = filter{
				timestampLbi: "01:21:25.0Z",
				timestampUbi: "01:24:04.8Z",
			}
			pushdown := func(_ filter) bool { return true }
			rmt := func(_c context.Context, _b Bucket, _f filter) ([]item, error) {
				return []item{
					{key: "01:21:26.0Z", val: `{}`},
					{key: "01:22:26.0Z", val: `{}`},
					{key: "01:23:26.0Z", val: `{}`},
				}, nil
			}

			var fr func(context.Context, Bucket, filter) ([]item, error) = FilterRemoteNew(
				nil,
				rmt,
				nil,
				pushdown,
			)

			filtered, e := fr(ctx, bkt, flt)

			t.Run("No error", assertNil(e))
			t.Run("Length match", assertEq(len(filtered), 3))
		})

	})

	t.Run("ScanEstimate", func(t *testing.T) {
		t.Parallel()

		t.Run("ToCost", func(t *testing.T) {
			t.Parallel()

			var e ScanEstimate = ScanEstimateNew(10.0, 1)
			var costInUs float64 = e.ToCost()
			t.Run("10 us", assertEq(costInUs, 10.0))
		})

		t.Run("UseIxScanByLimit", func(t *testing.T) {
			t.Parallel()

			t.Run("ix scan", func(t *testing.T) {
				t.Parallel()

				var e ScanEstimate = ScanEstimateNew(9.0, 1.0)
				var useIxScan bool = e.UseIxScanByCount(10.0)
				t.Run("use ix scan", assertEq(useIxScan, true))
			})
		})
	})

	t.Run("ScanEstimates", func(t *testing.T) {
		t.Parallel()

		t.Run("UseIxScan", func(t *testing.T) {
			t.Parallel()

			t.Run("ix scan", func(t *testing.T) {
				t.Parallel()

				var rows float64 = 9.0
				var ix ScanEstimate = ScanEstimateNew(rows, 1.0)
				var sq ScanEstimate = ScanEstimateNew(rows, 10.0)
				var e ScanEstimates = ScanEstimatesNew(ix, sq)
				var useIxScan bool = e.UseIxScan()
				t.Run("use ix scan", assertEq(useIxScan, true))
			})
		})
	})

	t.Run("PushdownNewByIxScanLimit", func(t *testing.T) {
		t.Parallel()

		const ixScanLimit float64 = 10.0
		const maxRowsPerSecond float64 = 1.0

		var filter testFilterUnixtime = testFilterUnixtime{lbi: 0.0, ubi: 1.0}
		var pushdown PushDown[testFilterUnixtime] = PushdownNewByIxScanLimit(
			ixScanLimit,
			func(f testFilterUnixtime) ScanEstimate {
				var estimatedRows float64 = f.estimateScansByRate(maxRowsPerSecond)
				return ScanEstimateNew(estimatedRows, 1.0)
			},
		)
		var useRemoteFilter bool = pushdown(filter)
		t.Run("remote filter must be used", assertEq(useRemoteFilter, true))
	})

	t.Run("PushdownNewByCost", func(t *testing.T) {
		t.Parallel()

		const maxRowsPerSecond float64 = 1.0

		var filter testFilterUnixtime = testFilterUnixtime{lbi: 0.0, ubi: 1.0}
		var pushdown PushDown[testFilterUnixtime] = PushdownNewByCost(
			func(f testFilterUnixtime) ScanEstimates {
				var estimatedScans float64 = f.estimateScansByRate(maxRowsPerSecond)
				return ScanEstimatesNew(
					ScanEstimateNew(estimatedScans, 1.0),
					ScanEstimateNew(estimatedScans, 10.0),
				)
			},
		)

		var useIxScan bool = pushdown(filter)
		t.Run("use ix scan", assertEq(useIxScan, true))
	})
}
