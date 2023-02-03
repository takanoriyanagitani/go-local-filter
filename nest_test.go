package local

import (
	"context"
	"testing"
)

type testNestPackedItem struct {
	val [8]uint64
	key uint8
}

func (p *testNestPackedItem) Unpack() (unpacked []testNestUnpackedItem) {
	for _, item := range p.val {
		var u uint64 = item
		var hi uint32 = uint32(u >> 32)
		var lo uint32 = uint32(u & 0xffffffff)

		var serial int32 = int32(hi)

		var lh uint16 = uint16(lo >> 16)
		var ll uint16 = uint16(lo & 0xffff)

		var status uint16 = lh
		var full uint16 = (uint16(p.key) << 8) | (ll >> 8)
		var valid uint8 = uint8(ll & 0xff)

		unpacked = append(unpacked, testNestUnpackedItem{
			serial,
			status,
			full,
			valid,
		})
	}
	return
}

type testNestUnpackedItem struct {
	serial int32
	status uint16
	full   uint16
	valid  uint8
}

type testNestFilter struct {
	key    uint8
	serial int32
}

func TestNest(t *testing.T) {
	t.Parallel()

	t.Run("Iter2ConsumerNewUnnested", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var unnest Unnest[testNestPackedItem, testNestUnpackedItem] = func(
				packed *testNestPackedItem,
			) (unnested []testNestUnpackedItem, e error) {
				return
			}

			var buf []testNestUnpackedItem
			var consumer IterConsumer[testNestUnpackedItem] = func(
				value *testNestUnpackedItem,
			) (stop bool, e error) {
				buf = append(buf, *value)
				return false, nil
			}

			var f func(
				ctx context.Context,
				dummyIter uint8,
				buf *testNestPackedItem,
				f *testNestFilter,
			) error = Iter2ConsumerNewUnnested(
				func(_iter uint8) (hasNext bool) { return false },
				func(_iter uint8, p *testNestPackedItem) error { return nil },
				func(_iter uint8) error { return nil },
				unnest,
				func(packed *testNestPackedItem, f *testNestFilter) (keep bool) { return true },
				func(unpacked *testNestUnpackedItem, f *testNestFilter) (keep bool) { return true },
				consumer,
			)

			var packedBuf testNestPackedItem
			var filter testNestFilter = testNestFilter{
				key:    0x42,
				serial: 0x01234567,
			}
			e := f(context.Background(), 0, &packedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(buf), 0))
		})

		t.Run("single packed item", func(t *testing.T) {
			t.Parallel()

			var unnest Unnest[testNestPackedItem, testNestUnpackedItem] = func(
				packed *testNestPackedItem,
			) (unnested []testNestUnpackedItem, e error) {
				return packed.Unpack(), nil
			}

			var buf []testNestUnpackedItem
			var consumer IterConsumer[testNestUnpackedItem] = func(
				value *testNestUnpackedItem,
			) (stop bool, e error) {
				buf = append(buf, *value)
				return false, nil
			}

			var dummyIter uint8 = 0

			var f func(
				ctx context.Context,
				dummyIter *uint8,
				buf *testNestPackedItem,
				f *testNestFilter,
			) error = Iter2ConsumerNewUnnested(
				func(iter *uint8) (hasNext bool) {
					hasNext = 0 == *iter
					*iter += 1
					return hasNext
				},
				func(iter *uint8, p *testNestPackedItem) error {
					p.key = *iter
					p.val = [8]uint64{
						0x0123456789abcdef,
						0x1123456789abcdef,
						0x2123456789abcdef,
						0x3123456789abcdef,
						0x4123456789abcdef,
						0x5123456789abcdef,
						0x6123456789abcdef,
						0x7123456789abcdef,
					}
					return nil
				},
				func(iter *uint8) error { return nil },
				unnest,
				func(packed *testNestPackedItem, f *testNestFilter) (keep bool) { return true },
				func(unpacked *testNestUnpackedItem, f *testNestFilter) (keep bool) { return true },
				consumer,
			)

			var packedBuf testNestPackedItem
			var filter testNestFilter = testNestFilter{
				key:    0x42,
				serial: 0x01234567,
			}
			e := f(context.Background(), &dummyIter, &packedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("8 unpacked items", assertEq(len(buf), 8))
		})

		t.Run("coarse filter", func(t *testing.T) {
			t.Parallel()

			var unnest Unnest[testNestPackedItem, testNestUnpackedItem] = func(
				packed *testNestPackedItem,
			) (unnested []testNestUnpackedItem, e error) {
				return packed.Unpack(), nil
			}

			var buf []testNestUnpackedItem
			var consumer IterConsumer[testNestUnpackedItem] = func(
				value *testNestUnpackedItem,
			) (stop bool, e error) {
				buf = append(buf, *value)
				return false, nil
			}

			var dummyIter uint8 = 0

			var f func(
				ctx context.Context,
				dummyIter *uint8,
				buf *testNestPackedItem,
				f *testNestFilter,
			) error = Iter2ConsumerNewUnnested(
				func(iter *uint8) (hasNext bool) {
					hasNext = *iter < 2
					*iter += 1
					return hasNext
				},
				func(iter *uint8, p *testNestPackedItem) error {
					p.key = 0x41 + *iter
					p.val = [8]uint64{
						0x0123456789abcdef,
						0x1123456789abcdef,
						0x2123456789abcdef,
						0x3123456789abcdef,
						0x4123456789abcdef,
						0x5123456789abcdef,
						0x6123456789abcdef,
						0x7123456789abcdef,
					}
					return nil
				},
				func(iter *uint8) error { return nil },
				unnest,
				func(packed *testNestPackedItem, f *testNestFilter) (keep bool) {
					return packed.key == f.key
				},
				func(unpacked *testNestUnpackedItem, f *testNestFilter) (keep bool) { return true },
				consumer,
			)

			var packedBuf testNestPackedItem
			var filter testNestFilter = testNestFilter{
				key:    0x42,
				serial: 0x01234567,
			}
			e := f(context.Background(), &dummyIter, &packedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("8 unpacked items", assertEq(len(buf), 8))
		})

		t.Run("fine filter", func(t *testing.T) {
			t.Parallel()

			var unnest Unnest[testNestPackedItem, testNestUnpackedItem] = func(
				packed *testNestPackedItem,
			) (unnested []testNestUnpackedItem, e error) {
				return packed.Unpack(), nil
			}

			var buf []testNestUnpackedItem
			var consumer IterConsumer[testNestUnpackedItem] = func(
				value *testNestUnpackedItem,
			) (stop bool, e error) {
				buf = append(buf, *value)
				return false, nil
			}

			var dummyIter uint8 = 0

			var f func(
				ctx context.Context,
				dummyIter *uint8,
				buf *testNestPackedItem,
				f *testNestFilter,
			) error = Iter2ConsumerNewUnnested(
				func(iter *uint8) (hasNext bool) {
					hasNext = *iter < 2
					*iter += 1
					return hasNext
				},
				func(iter *uint8, p *testNestPackedItem) error {
					p.key = 0x42
					p.val = [8]uint64{
						0x0123456789abcdef,
						0x1123456789abcdef,
						0x2123456789abcdef,
						0x3123456789abcdef,
						0x4123456789abcdef,
						0x5123456789abcdef,
						0x6123456789abcdef,
						0x7123456789abcdef,
					}
					return nil
				},
				func(iter *uint8) error { return nil },
				unnest,
				func(packed *testNestPackedItem, f *testNestFilter) (keep bool) {
					return packed.key == f.key
				},
				func(unpacked *testNestUnpackedItem, f *testNestFilter) (keep bool) {
					return unpacked.serial == f.serial
				},
				consumer,
			)

			var packedBuf testNestPackedItem
			var filter testNestFilter = testNestFilter{
				key:    0x42,
				serial: 0x01234567,
			}
			e := f(context.Background(), &dummyIter, &packedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("2 unpacked items", assertEq(len(buf), 2))
		})

		t.Run("stop", func(t *testing.T) {
			t.Parallel()

			var unnest Unnest[testNestPackedItem, testNestUnpackedItem] = func(
				packed *testNestPackedItem,
			) (unnested []testNestUnpackedItem, e error) {
				return packed.Unpack(), nil
			}

			var buf []testNestUnpackedItem
			var consumer IterConsumer[testNestUnpackedItem] = func(
				value *testNestUnpackedItem,
			) (stop bool, e error) {
				buf = append(buf, *value)
				return 8 < len(buf), nil
			}

			var dummyIter uint8 = 0

			var f func(
				ctx context.Context,
				dummyIter *uint8,
				buf *testNestPackedItem,
				f *testNestFilter,
			) error = Iter2ConsumerNewUnnested(
				func(iter *uint8) (hasNext bool) {
					hasNext = *iter < 2
					*iter += 1
					return hasNext
				},
				func(iter *uint8, p *testNestPackedItem) error {
					p.key = 0x42
					p.val = [8]uint64{
						0x0123456789abcdef,
						0x1123456789abcdef,
						0x2123456789abcdef,
						0x3123456789abcdef,
						0x4123456789abcdef,
						0x5123456789abcdef,
						0x6123456789abcdef,
						0x7123456789abcdef,
					}
					return nil
				},
				func(iter *uint8) error { return nil },
				unnest,
				func(packed *testNestPackedItem, f *testNestFilter) (keep bool) {
					return packed.key == f.key
				},
				func(unpacked *testNestUnpackedItem, f *testNestFilter) (keep bool) {
					return true
				},
				consumer,
			)

			var packedBuf testNestPackedItem
			var filter testNestFilter = testNestFilter{
				key:    0x42,
				serial: 0x01234567,
			}
			e := f(context.Background(), &dummyIter, &packedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("9 unpacked items", assertEq(len(buf), 9))
		})
	})
}
