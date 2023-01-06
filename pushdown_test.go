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

func TestPushdown(t *testing.T) {
	t.Run("FilterRemote", func(t *testing.T) {

		t.Run("local-only-nop-filter", func(t *testing.T) {
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

	})
}
