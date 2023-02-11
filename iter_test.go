package local

import (
	"context"
	"encoding/binary"
	"testing"
)

type testIterPacked struct {
	key uint8
	val [21]uint8
}

func (p *testIterPacked) unpack() ([]testIterUnpacked, error) {
	var subBucket uint16 = (uint16(p.key) << 8) | uint16(p.val[0])
	i, _ := binary.Varint(p.val[1:9])
	var rowId int32 = int32(i >> 32)
	var bloom [2]uint64 = [2]uint64{
		binary.BigEndian.Uint64(p.val[0x05:0x0d]),
		binary.BigEndian.Uint64(p.val[0x0d:0x15]),
	}
	return []testIterUnpacked{
		{
			rowId,
			subBucket,
			bloom,
		},
	}, nil
}

type testIterDecoded struct {
	key    uint8
	subKey uint8
	rowId  int32
	dat    [16]byte
}

type testIterUnpacked struct {
	rowId     int32
	subBucket uint16
	bloom     [2]uint64
}

type testIterFilter struct {
	key    uint8
	bloom1 uint64
}

func TestIter(t *testing.T) {
	t.Parallel()

	t.Run("Iter2UnpackedNew", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			const dummyIter uint8 = 0
			hasNext := func(_iter uint8) bool { return false }
			getPacked := func(_iter uint8, buf *testIterPacked) error { return nil }
			getError := func(_iter uint8) error { return nil }
			packed2unpacked := func(_ *testIterPacked) (u testIterUnpacked, e error) { return }

			var iter2unpacked func(
				ctx context.Context,
				iter uint8,
			) (unpacked []testIterUnpacked, e error) = Iter2UnpackedNew(
				packed2unpacked,
				hasNext,
				getPacked,
				getError,
			)

			unpacked, e := iter2unpacked(context.Background(), dummyIter)

			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			hasNext := func(iter *uint8) (foundNext bool) {
				foundNext = 0 == *iter
				*iter += 1
				return
			}
			getPacked := func(_iter *uint8, buf *testIterPacked) error {
				buf.key = 0x37
				buf.val[0] = 0x76

				buf.val[1] = 0x01
				buf.val[2] = 0x23
				buf.val[3] = 0x45
				buf.val[4] = 0x67

				buf.val[0x5] = 0x01
				buf.val[0x6] = 0x23
				buf.val[0x7] = 0x45
				buf.val[0x8] = 0x67
				buf.val[0x9] = 0x89
				buf.val[0xa] = 0xab
				buf.val[0xb] = 0xcd
				buf.val[0xc] = 0xef

				buf.val[0x0d] = 0x01
				buf.val[0x0e] = 0x23
				buf.val[0x0f] = 0x45
				buf.val[0x10] = 0x67
				buf.val[0x11] = 0x89
				buf.val[0x12] = 0xab
				buf.val[0x13] = 0xcd
				buf.val[0x14] = 0xef

				return nil
			}
			getError := func(_iter *uint8) error { return nil }
			packed2unpacked := func(p *testIterPacked) (u testIterUnpacked, e error) {
				u.subBucket = (uint16(p.key) << 8) | uint16(p.val[0])
				u.rowId = int32(binary.BigEndian.Uint32(p.val[1:]))
				u.bloom[0] = binary.BigEndian.Uint64(p.val[0x5:])
				u.bloom[1] = binary.BigEndian.Uint64(p.val[0xd:])
				return
			}

			var iter2unpacked func(
				ctx context.Context,
				iter *uint8,
			) (unpacked []testIterUnpacked, e error) = Iter2UnpackedNew(
				packed2unpacked,
				hasNext,
				getPacked,
				getError,
			)

			unpacked, e := iter2unpacked(context.Background(), &dummyIter)

			t.Run("no error", assertNil(e))
			t.Run("single item", assertEq(len(unpacked), 1))

			var unpackedItem testIterUnpacked = unpacked[0]
			t.Run("sub bucket match", assertEq(unpackedItem.subBucket, 0x3776))
			t.Run("rowid match", assertEq(unpackedItem.rowId, 0x01234567))
			t.Run("bloom 0 match", assertEq(unpackedItem.bloom[0], 0x0123456789abcdef))
			t.Run("bloom 1 match", assertEq(unpackedItem.bloom[1], 0x0123456789abcdef))
		})
	})

	t.Run("Iter2UnpackedWithFilterNew", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			hasNext := func(iter *uint8) (foundNext bool) {
				return false
			}
			getPacked := func(_iter *uint8, buf *testIterPacked) error {
				buf.key = 0x37
				buf.val[0] = 0x76

				buf.val[1] = 0x01
				buf.val[2] = 0x23
				buf.val[3] = 0x45
				buf.val[4] = 0x67

				buf.val[0x5] = 0x01
				buf.val[0x6] = 0x23
				buf.val[0x7] = 0x45
				buf.val[0x8] = 0x67
				buf.val[0x9] = 0x89
				buf.val[0xa] = 0xab
				buf.val[0xb] = 0xcd
				buf.val[0xc] = 0xef

				buf.val[0x0d] = 0x01
				buf.val[0x0e] = 0x23
				buf.val[0x0f] = 0x45
				buf.val[0x10] = 0x67
				buf.val[0x11] = 0x89
				buf.val[0x12] = 0xab
				buf.val[0x13] = 0xcd
				buf.val[0x14] = 0xef

				return nil
			}
			getError := func(_iter *uint8) error { return nil }
			packed2unpacked := func(p *testIterPacked) (u testIterUnpacked, e error) {
				u.subBucket = (uint16(p.key) << 8) | uint16(p.val[0])
				u.rowId = int32(binary.BigEndian.Uint32(p.val[1:]))
				u.bloom[0] = binary.BigEndian.Uint64(p.val[0x5:])
				u.bloom[1] = binary.BigEndian.Uint64(p.val[0xd:])
				return
			}

			coarseFilter := func(p *testIterPacked, _f *testIterFilter) (keep bool) { return true }
			fineFilter := func(u *testIterUnpacked, _f *testIterFilter) (keep bool) { return true }

			var iter2unpacked func(
				ctx context.Context,
				iter *uint8,
				f *testIterFilter,
			) (unpacked []testIterUnpacked, e error) = Iter2UnpackedWithFilterNew(
				packed2unpacked,
				hasNext,
				getPacked,
				getError,
				coarseFilter,
				fineFilter,
			)

			unpacked, e := iter2unpacked(context.Background(), &dummyIter, nil)

			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 0))
		})

		t.Run("coarse filter", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			hasNext := func(iter *uint8) (foundNext bool) {
				foundNext = 0 == *iter
				*iter += 1
				return
			}
			getPacked := func(_iter *uint8, buf *testIterPacked) error {
				buf.key = 0x37
				buf.val[0] = 0x76

				buf.val[1] = 0x01
				buf.val[2] = 0x23
				buf.val[3] = 0x45
				buf.val[4] = 0x67

				buf.val[0x5] = 0x01
				buf.val[0x6] = 0x23
				buf.val[0x7] = 0x45
				buf.val[0x8] = 0x67
				buf.val[0x9] = 0x89
				buf.val[0xa] = 0xab
				buf.val[0xb] = 0xcd
				buf.val[0xc] = 0xef

				buf.val[0x0d] = 0x01
				buf.val[0x0e] = 0x23
				buf.val[0x0f] = 0x45
				buf.val[0x10] = 0x67
				buf.val[0x11] = 0x89
				buf.val[0x12] = 0xab
				buf.val[0x13] = 0xcd
				buf.val[0x14] = 0xef

				return nil
			}
			getError := func(_iter *uint8) error { return nil }
			packed2unpacked := func(p *testIterPacked) (u testIterUnpacked, e error) {
				u.subBucket = (uint16(p.key) << 8) | uint16(p.val[0])
				u.rowId = int32(binary.BigEndian.Uint32(p.val[1:]))
				u.bloom[0] = binary.BigEndian.Uint64(p.val[0x5:])
				u.bloom[1] = binary.BigEndian.Uint64(p.val[0xd:])
				return
			}

			coarseFilter := func(p *testIterPacked, _f *testIterFilter) (keep bool) { return false }
			fineFilter := func(u *testIterUnpacked, _f *testIterFilter) (keep bool) { return true }

			var iter2unpacked func(
				ctx context.Context,
				iter *uint8,
				f *testIterFilter,
			) (unpacked []testIterUnpacked, e error) = Iter2UnpackedWithFilterNew(
				packed2unpacked,
				hasNext,
				getPacked,
				getError,
				coarseFilter,
				fineFilter,
			)

			unpacked, e := iter2unpacked(context.Background(), &dummyIter, nil)

			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 0))
		})

		t.Run("fine filter", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			hasNext := func(iter *uint8) (foundNext bool) {
				foundNext = 0 == *iter
				*iter += 1
				return
			}
			getPacked := func(_iter *uint8, buf *testIterPacked) error {
				buf.key = 0x37
				buf.val[0] = 0x76

				buf.val[1] = 0x01
				buf.val[2] = 0x23
				buf.val[3] = 0x45
				buf.val[4] = 0x67

				buf.val[0x5] = 0x01
				buf.val[0x6] = 0x23
				buf.val[0x7] = 0x45
				buf.val[0x8] = 0x67
				buf.val[0x9] = 0x89
				buf.val[0xa] = 0xab
				buf.val[0xb] = 0xcd
				buf.val[0xc] = 0xef

				buf.val[0x0d] = 0x01
				buf.val[0x0e] = 0x23
				buf.val[0x0f] = 0x45
				buf.val[0x10] = 0x67
				buf.val[0x11] = 0x89
				buf.val[0x12] = 0xab
				buf.val[0x13] = 0xcd
				buf.val[0x14] = 0xef

				return nil
			}
			getError := func(_iter *uint8) error { return nil }
			packed2unpacked := func(p *testIterPacked) (u testIterUnpacked, e error) {
				u.subBucket = (uint16(p.key) << 8) | uint16(p.val[0])
				u.rowId = int32(binary.BigEndian.Uint32(p.val[1:]))
				u.bloom[0] = binary.BigEndian.Uint64(p.val[0x5:])
				u.bloom[1] = binary.BigEndian.Uint64(p.val[0xd:])
				return
			}

			coarseFilter := func(p *testIterPacked, _f *testIterFilter) (keep bool) { return true }
			fineFilter := func(u *testIterUnpacked, _f *testIterFilter) (keep bool) { return false }

			var iter2unpacked func(
				ctx context.Context,
				iter *uint8,
				f *testIterFilter,
			) (unpacked []testIterUnpacked, e error) = Iter2UnpackedWithFilterNew(
				packed2unpacked,
				hasNext,
				getPacked,
				getError,
				coarseFilter,
				fineFilter,
			)

			unpacked, e := iter2unpacked(context.Background(), &dummyIter, nil)

			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			hasNext := func(iter *uint8) (foundNext bool) {
				foundNext = 0 == *iter
				*iter += 1
				return
			}
			getPacked := func(_iter *uint8, buf *testIterPacked) error {
				buf.key = 0x37
				buf.val[0] = 0x76

				buf.val[1] = 0x01
				buf.val[2] = 0x23
				buf.val[3] = 0x45
				buf.val[4] = 0x67

				buf.val[0x5] = 0x01
				buf.val[0x6] = 0x23
				buf.val[0x7] = 0x45
				buf.val[0x8] = 0x67
				buf.val[0x9] = 0x89
				buf.val[0xa] = 0xab
				buf.val[0xb] = 0xcd
				buf.val[0xc] = 0xef

				buf.val[0x0d] = 0x01
				buf.val[0x0e] = 0x23
				buf.val[0x0f] = 0x45
				buf.val[0x10] = 0x67
				buf.val[0x11] = 0x89
				buf.val[0x12] = 0xab
				buf.val[0x13] = 0xcd
				buf.val[0x14] = 0xef

				return nil
			}
			getError := func(_iter *uint8) error { return nil }
			packed2unpacked := func(p *testIterPacked) (u testIterUnpacked, e error) {
				u.subBucket = (uint16(p.key) << 8) | uint16(p.val[0])
				u.rowId = int32(binary.BigEndian.Uint32(p.val[1:]))
				u.bloom[0] = binary.BigEndian.Uint64(p.val[0x5:])
				u.bloom[1] = binary.BigEndian.Uint64(p.val[0xd:])
				return
			}

			coarseFilter := func(p *testIterPacked, f *testIterFilter) (keep bool) {
				return p.key == f.key
			}
			fineFilter := func(u *testIterUnpacked, f *testIterFilter) (keep bool) {
				return (u.bloom[1] & f.bloom1) == f.bloom1
			}

			var iter2unpacked func(
				ctx context.Context,
				iter *uint8,
				f *testIterFilter,
			) (unpacked []testIterUnpacked, e error) = Iter2UnpackedWithFilterNew(
				packed2unpacked,
				hasNext,
				getPacked,
				getError,
				coarseFilter,
				fineFilter,
			)

			unpacked, e := iter2unpacked(context.Background(), &dummyIter, &testIterFilter{
				key:    0x37,
				bloom1: 0x00000000000000ef,
			})

			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 1))

			var unpackedItem testIterUnpacked = unpacked[0]
			t.Run("sub bucket match", assertEq(unpackedItem.subBucket, 0x3776))
			t.Run("rowid match", assertEq(unpackedItem.rowId, 0x01234567))
			t.Run("bloom 0 match", assertEq(unpackedItem.bloom[0], 0x0123456789abcdef))
			t.Run("bloom 1 match", assertEq(unpackedItem.bloom[1], 0x0123456789abcdef))
		})
	})

	t.Run("Iter2ConsumerNewFiltered", func(t *testing.T) {
		t.Parallel()

		t.Run("no item", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			iterNext := func(iter *uint8) bool {
				var hasNext bool = 0 == (*iter)
				*iter += 1
				return hasNext
			}
			iterGet := func(_iter *uint8, p *testIterPacked) error {
				p.key = 0x42
				return nil
			}
			iterErr := func(_iter *uint8) error { return nil }
			keep := func(f *testIterFilter, p *testIterPacked) bool { return f.key == p.key }

			var keys []uint8

			var consumer IterConsumer[testIterPacked] = func(
				_packed *testIterPacked,
			) (stop bool, e error) {
				keys = append(keys, _packed.key)
				return false, nil
			}

			var f func(
				ctx context.Context,
				iter *uint8,
				buf *testIterPacked,
				f *testIterFilter,
			) error = Iter2ConsumerNewFiltered(
				iterNext,
				iterGet,
				iterErr,
				keep,
				consumer,
			)

			var buf testIterPacked
			var filt *testIterFilter = &testIterFilter{
				key:    0x52,
				bloom1: 0x634,
			}

			var e error = f(context.Background(), &dummyIter, &buf, filt)
			t.Run("no error", assertNil(e))
			t.Run("no item", assertEq(len(keys), 0))
		})

		t.Run("stop", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			iterNext := func(iter *uint8) bool {
				var hasNext bool = 0 == (*iter)
				*iter += 1
				return hasNext
			}
			iterGet := func(_iter *uint8, p *testIterPacked) error {
				p.key = 0x42
				return nil
			}
			iterErr := func(_iter *uint8) error { return nil }
			keep := func(f *testIterFilter, p *testIterPacked) bool { return f.key == p.key }

			var keys []uint8

			var consumer IterConsumer[testIterPacked] = func(
				_packed *testIterPacked,
			) (stop bool, e error) {
				return true, nil
			}

			var f func(
				ctx context.Context,
				iter *uint8,
				buf *testIterPacked,
				f *testIterFilter,
			) error = Iter2ConsumerNewFiltered(
				iterNext,
				iterGet,
				iterErr,
				keep,
				consumer,
			)

			var buf testIterPacked
			var filt *testIterFilter = &testIterFilter{
				key:    0x42,
				bloom1: 0x634,
			}

			var e error = f(context.Background(), &dummyIter, &buf, filt)
			t.Run("no error", assertNil(e))
			t.Run("no item", assertEq(len(keys), 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			iterNext := func(iter *uint8) bool {
				var hasNext bool = 0 == (*iter)
				*iter += 1
				return hasNext
			}
			iterGet := func(_iter *uint8, p *testIterPacked) error {
				p.key = 0x42
				return nil
			}
			iterErr := func(_iter *uint8) error { return nil }
			keep := func(f *testIterFilter, p *testIterPacked) bool { return f.key == p.key }

			var keys []uint8

			var consumer IterConsumer[testIterPacked] = func(
				_packed *testIterPacked,
			) (stop bool, e error) {
				keys = append(keys, _packed.key)
				return false, nil
			}

			var f func(
				ctx context.Context,
				iter *uint8,
				buf *testIterPacked,
				f *testIterFilter,
			) error = Iter2ConsumerNewFiltered(
				iterNext,
				iterGet,
				iterErr,
				keep,
				consumer,
			)

			var buf testIterPacked
			var filt *testIterFilter = &testIterFilter{
				key:    0x42,
				bloom1: 0x634,
			}

			var e error = f(context.Background(), &dummyIter, &buf, filt)
			t.Run("no error", assertNil(e))
			t.Run("single item", assertEq(len(keys), 1))
			var key uint8 = keys[0]
			t.Run("key check", assertEq(key, 0x42))
		})
	})

	t.Run("Iter2ConsumerNewUnpacked", func(t *testing.T) {
		t.Parallel()

		t.Run("fine filter", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			iterNext := func(iter *uint8) bool {
				var hasNext bool = 0 == (*iter)
				*iter += 1
				return hasNext
			}
			iterGet := func(_iter *uint8, packed *testIterPacked) error {
				packed.key = 0x42
				packed.val[0] = 0x37
				packed.val[1] = 0x76
				return nil
			}
			iterErr := func(_iter *uint8) error { return nil }
			unpack := func(packed *testIterPacked) (testIterUnpacked, error) {
				return testIterUnpacked{
					rowId:     0x634,
					subBucket: uint16(packed.val[1]) | (uint16(packed.val[0]) << 8),
					bloom: [2]uint64{
						0x0599,
						0x0333,
					},
				}, nil
			}
			filterCoarse := func(packed *testIterPacked, f *testIterFilter) (keep bool) {
				return packed.key == f.key
			}
			filterFine := func(unpacked *testIterUnpacked, f *testIterFilter) (keep bool) {
				return unpacked.bloom[1] == f.bloom1
			}
			var wroteCnt uint8 = 0
			consumer := func(unpacked *testIterUnpacked) (stop bool, e error) {
				wroteCnt += 1
				return false, nil
			}

			var f func(
				ctx context.Context,
				iter *uint8,
				buf *testIterPacked,
				filter *testIterFilter,
			) (e error) = Iter2ConsumerNewUnpacked(
				iterNext, iterGet, iterErr, unpack, filterCoarse, filterFine, consumer,
			)

			var buf testIterPacked
			var flt *testIterFilter = &testIterFilter{
				key:    0x42,
				bloom1: 0x3776,
			}

			e := f(context.Background(), &dummyIter, &buf, flt)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(wroteCnt, 0))
		})

		t.Run("coarse filter", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			iterNext := func(iter *uint8) bool {
				var hasNext bool = 0 == (*iter)
				*iter += 1
				return hasNext
			}
			iterGet := func(_iter *uint8, packed *testIterPacked) error {
				packed.key = 0x43
				packed.val[0] = 0x37
				packed.val[1] = 0x76
				return nil
			}
			iterErr := func(_iter *uint8) error { return nil }
			unpack := func(packed *testIterPacked) (testIterUnpacked, error) {
				return testIterUnpacked{
					rowId:     0x634,
					subBucket: uint16(packed.val[1]) | (uint16(packed.val[0]) << 8),
					bloom: [2]uint64{
						0x0599,
						0x3776,
					},
				}, nil
			}
			filterCoarse := func(packed *testIterPacked, f *testIterFilter) (keep bool) {
				return packed.key == f.key
			}
			filterFine := func(unpacked *testIterUnpacked, f *testIterFilter) (keep bool) {
				return unpacked.bloom[1] == f.bloom1
			}
			var wroteCnt uint8 = 0
			consumer := func(unpacked *testIterUnpacked) (stop bool, e error) {
				wroteCnt += 1
				return false, nil
			}

			var f func(
				ctx context.Context,
				iter *uint8,
				buf *testIterPacked,
				filter *testIterFilter,
			) (e error) = Iter2ConsumerNewUnpacked(
				iterNext, iterGet, iterErr, unpack, filterCoarse, filterFine, consumer,
			)

			var buf testIterPacked
			var flt *testIterFilter = &testIterFilter{
				key:    0x42,
				bloom1: 0x3776,
			}

			e := f(context.Background(), &dummyIter, &buf, flt)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(wroteCnt, 0))
		})

		t.Run("stop", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			iterNext := func(iter *uint8) bool {
				var hasNext bool = 0 == (*iter)
				*iter += 1
				return hasNext
			}
			iterGet := func(_iter *uint8, packed *testIterPacked) error {
				packed.key = 0x42
				packed.val[0] = 0x37
				packed.val[1] = 0x76
				return nil
			}
			iterErr := func(_iter *uint8) error { return nil }
			unpack := func(packed *testIterPacked) (testIterUnpacked, error) {
				return testIterUnpacked{
					rowId:     0x634,
					subBucket: uint16(packed.val[1]) | (uint16(packed.val[0]) << 8),
					bloom: [2]uint64{
						0x0599,
						0x3776,
					},
				}, nil
			}
			filterCoarse := func(packed *testIterPacked, f *testIterFilter) (keep bool) {
				return packed.key == f.key
			}
			filterFine := func(unpacked *testIterUnpacked, f *testIterFilter) (keep bool) {
				return unpacked.bloom[1] == f.bloom1
			}
			var wroteCnt uint8 = 0
			consumer := func(unpacked *testIterUnpacked) (stop bool, e error) {
				return true, nil
			}

			var f func(
				ctx context.Context,
				iter *uint8,
				buf *testIterPacked,
				filter *testIterFilter,
			) (e error) = Iter2ConsumerNewUnpacked(
				iterNext, iterGet, iterErr, unpack, filterCoarse, filterFine, consumer,
			)

			var buf testIterPacked
			var flt *testIterFilter = &testIterFilter{
				key:    0x42,
				bloom1: 0x3776,
			}

			e := f(context.Background(), &dummyIter, &buf, flt)
			t.Run("no error", assertNil(e))
			t.Run("no write", assertEq(wroteCnt, 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var dummyIter uint8 = 0
			iterNext := func(iter *uint8) bool {
				var hasNext bool = 0 == (*iter)
				*iter += 1
				return hasNext
			}
			iterGet := func(_iter *uint8, packed *testIterPacked) error {
				packed.key = 0x42
				packed.val[0] = 0x37
				packed.val[1] = 0x76
				return nil
			}
			iterErr := func(_iter *uint8) error { return nil }
			unpack := func(packed *testIterPacked) (testIterUnpacked, error) {
				return testIterUnpacked{
					rowId:     0x634,
					subBucket: uint16(packed.val[1]) | (uint16(packed.val[0]) << 8),
					bloom: [2]uint64{
						0x0599,
						0x3776,
					},
				}, nil
			}
			filterCoarse := func(packed *testIterPacked, f *testIterFilter) (keep bool) {
				return packed.key == f.key
			}
			filterFine := func(unpacked *testIterUnpacked, f *testIterFilter) (keep bool) {
				return unpacked.bloom[1] == f.bloom1
			}
			var wroteCnt uint8 = 0
			consumer := func(unpacked *testIterUnpacked) (stop bool, e error) {
				wroteCnt += 1
				return false, nil
			}

			var f func(
				ctx context.Context,
				iter *uint8,
				buf *testIterPacked,
				filter *testIterFilter,
			) (e error) = Iter2ConsumerNewUnpacked(
				iterNext, iterGet, iterErr, unpack, filterCoarse, filterFine, consumer,
			)

			var buf testIterPacked
			var flt *testIterFilter = &testIterFilter{
				key:    0x42,
				bloom1: 0x3776,
			}

			e := f(context.Background(), &dummyIter, &buf, flt)
			t.Run("no error", assertNil(e))
			t.Run("single write", assertEq(wroteCnt, 1))
		})
	})

	t.Run("IterConsumerNewPacked", func(t *testing.T) {
		t.Parallel()

		t.Run("single packed item", func(t *testing.T) {
			t.Parallel()

			var unpackBuf [2]uint32
			unpack := func(packed *uint64) (unpacked []uint32, e error) {
				var hi uint64 = (*packed) >> 32
				var lo uint64 = (*packed) & 0xffff_ffff
				unpackBuf[0] = uint32(hi)
				unpackBuf[1] = uint32(lo)
				return unpackBuf[:], nil
			}

			var cnt int = 0
			var consumer IterConsumer[uint32] = func(_val *uint32) (stop bool, e error) {
				cnt += 1
				return false, nil
			}
			var packedConsumer IterConsumer[uint64] = IterConsumerNewPacked(
				unpack,
				consumer,
			)

			var i uint64 = 0x01234567_89abcdef
			stop, e := packedConsumer(&i)
			t.Run("no error", assertNil(e))
			t.Run("non stop", assertEq(stop, false))
			t.Run("two unpacked items", assertEq(cnt, 2))
		})

		t.Run("3 unpacked items", func(t *testing.T) {
			t.Parallel()

			var unpackBuf [2]uint32
			unpack := func(packed *uint64) (unpacked []uint32, e error) {
				var hi uint64 = (*packed) >> 32
				var lo uint64 = (*packed) & 0xffff_ffff
				unpackBuf[0] = uint32(hi)
				unpackBuf[1] = uint32(lo)
				return unpackBuf[:], nil
			}

			var cnt int = 0
			var lmt int = 3
			var consumer IterConsumer[uint32] = func(_val *uint32) (stop bool, e error) {
				cnt += 1
				return lmt <= cnt, nil
			}
			var packedConsumer IterConsumer[uint64] = IterConsumerNewPacked(
				unpack,
				consumer,
			)

			var i uint64 = 0x01234567_89abcdef
			stop, e := packedConsumer(&i)
			t.Run("no error", assertNil(e))
			t.Run("non stop", assertEq(stop, false))
			stop, e = packedConsumer(&i)
			t.Run("stop", assertEq(stop, true))
			t.Run("3 unpacked items", assertEq(cnt, 3))
		})
	})

	t.Run("IterConsumerFilterMany", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var buf []int32
			var consumer IterConsumer[int32] = func(val *int32) (stop bool, e error) {
				buf = append(buf, *val)
				return false, nil
			}
			var all []int32 = []int32{}
			var f testIterFilter = testIterFilter{
				key:    0x42,
				bloom1: 0x0123456789abcdef,
			}
			filter := func(item *int32, f *testIterFilter) (keep bool) {
				return true
			}
			stop, e := IterConsumerFilterMany(
				consumer,
				all,
				filter,
				&f,
			)
			t.Run("no error", assertNil(e))
			t.Run("non stop", assertEq(stop, false))
			t.Run("empty", assertEq(len(buf), 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var buf []int32
			var consumer IterConsumer[int32] = func(val *int32) (stop bool, e error) {
				buf = append(buf, *val)
				return false, nil
			}
			var all []int32 = []int32{
				0x01234567,
				0x42234567,
				0x21234567,
			}
			var f testIterFilter = testIterFilter{
				key:    0x42,
				bloom1: 0x0123456789abcdef,
			}
			filter := func(item *int32, f *testIterFilter) (keep bool) {
				var head uint8 = uint8(*item >> 24)
				return head == f.key
			}
			stop, e := IterConsumerFilterMany(
				consumer,
				all,
				filter,
				&f,
			)
			t.Run("no error", assertNil(e))
			t.Run("non stop", assertEq(stop, false))
			t.Run("single item", assertEq(len(buf), 1))
		})

		t.Run("3 items", func(t *testing.T) {
			t.Parallel()

			var buf []int32
			var consumer IterConsumer[int32] = func(val *int32) (stop bool, e error) {
				buf = append(buf, *val)
				return false, nil
			}
			var all []int32 = []int32{
				0x42000000,
				0x42111111,
				0x42222222,
			}
			var f testIterFilter = testIterFilter{
				key:    0x42,
				bloom1: 0x0123456789abcdef,
			}
			filter := func(item *int32, f *testIterFilter) (keep bool) {
				var head uint8 = uint8(*item >> 24)
				return head == f.key
			}
			stop, e := IterConsumerFilterMany(
				consumer,
				all,
				filter,
				&f,
			)
			t.Run("no error", assertNil(e))
			t.Run("non stop", assertEq(stop, false))
			t.Run("iii items", assertEq(len(buf), 3))
		})

		t.Run("stopper", func(t *testing.T) {
			t.Parallel()

			var buf []int32
			var consumer IterConsumer[int32] = func(val *int32) (stop bool, e error) {
				buf = append(buf, *val)
				return 1 < len(buf), nil
			}
			var all []int32 = []int32{
				0x42000000,
				0x42111111,
				0x42222222,
			}
			var f testIterFilter = testIterFilter{
				key:    0x42,
				bloom1: 0x0123456789abcdef,
			}
			filter := func(item *int32, f *testIterFilter) (keep bool) {
				var head uint8 = uint8(*item >> 24)
				return head == f.key
			}
			stop, e := IterConsumerFilterMany(
				consumer,
				all,
				filter,
				&f,
			)
			t.Run("no error", assertNil(e))
			t.Run("stop", assertEq(stop, true))
			t.Run("two items", assertEq(len(buf), 2))
		})
	})

	t.Run("IterConsumerFiltered", func(t *testing.T) {
		t.Parallel()

		t.Run("ConsumerUnpackedNew", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var unpackedItems []testIterUnpacked
				var unpackedConsumer IterConsumerFiltered[testIterUnpacked, testIterFilter] = func(
					unpacked *testIterUnpacked,
					f *testIterFilter,
				) (stop bool, e error) {
					unpackedItems = append(unpackedItems, *unpacked)
					return
				}

				packed2unpacked := func(p *testIterPacked) ([]testIterUnpacked, error) {
					return nil, nil
				}

				filterPacked := func(p *testIterPacked, f *testIterFilter) (keep bool) {
					return true
				}

				var packedConsumer IterConsumerFiltered[
					testIterPacked,
					testIterFilter,
				] = ConsumerUnpackedNew(
					unpackedConsumer,
					packed2unpacked,
					filterPacked,
				)

				var buf testIterPacked
				f := testIterFilter{}
				stop, e := packedConsumer(&buf, &f)
				t.Run("no error", assertNil(e))
				t.Run("non stop", assertEq(stop, false))
				t.Run("no items", assertEq(len(unpackedItems), 0))
			})

			t.Run("coarse filter", func(t *testing.T) {
				t.Parallel()

				var unpackedItems []testIterUnpacked
				var unpackedConsumer IterConsumerFiltered[testIterUnpacked, testIterFilter] = func(
					unpacked *testIterUnpacked,
					f *testIterFilter,
				) (stop bool, e error) {
					unpackedItems = append(unpackedItems, *unpacked)
					return
				}

				packed2unpacked := func(p *testIterPacked) ([]testIterUnpacked, error) {
					return nil, nil
				}

				filterPacked := func(p *testIterPacked, f *testIterFilter) (keep bool) {
					return false
				}

				var packedConsumer IterConsumerFiltered[
					testIterPacked,
					testIterFilter,
				] = ConsumerUnpackedNew(
					unpackedConsumer,
					packed2unpacked,
					filterPacked,
				)

				var buf testIterPacked
				f := testIterFilter{}
				stop, e := packedConsumer(&buf, &f)
				t.Run("no error", assertNil(e))
				t.Run("non stop", assertEq(stop, false))
				t.Run("no items", assertEq(len(unpackedItems), 0))
			})

			t.Run("single item", func(t *testing.T) {
				t.Parallel()

				var unpackedItems []testIterUnpacked
				var unpackedConsumer IterConsumerFiltered[testIterUnpacked, testIterFilter] = func(
					unpacked *testIterUnpacked,
					f *testIterFilter,
				) (stop bool, e error) {
					unpackedItems = append(unpackedItems, *unpacked)
					return
				}

				packed2unpacked := func(p *testIterPacked) ([]testIterUnpacked, error) {
					return p.unpack()
				}

				filterPacked := func(p *testIterPacked, f *testIterFilter) (keep bool) {
					return true
				}

				var packedConsumer IterConsumerFiltered[
					testIterPacked,
					testIterFilter,
				] = ConsumerUnpackedNew(
					unpackedConsumer,
					packed2unpacked,
					filterPacked,
				)

				var buf testIterPacked
				f := testIterFilter{}
				stop, e := packedConsumer(&buf, &f)
				t.Run("no error", assertNil(e))
				t.Run("non stop", assertEq(stop, false))
				t.Run("1 item", assertEq(len(unpackedItems), 1))
			})

			t.Run("too many items", func(t *testing.T) {
				t.Parallel()

				var unpackedItems []testIterUnpacked = []testIterUnpacked{
					{},
					{},
				}
				var unpackedConsumer IterConsumerFiltered[testIterUnpacked, testIterFilter] = func(
					unpacked *testIterUnpacked,
					f *testIterFilter,
				) (stop bool, e error) {
					unpackedItems = append(unpackedItems, *unpacked)
					return 2 < len(unpackedItems), nil
				}

				packed2unpacked := func(p *testIterPacked) ([]testIterUnpacked, error) {
					return p.unpack()
				}

				filterPacked := func(p *testIterPacked, f *testIterFilter) (keep bool) {
					return true
				}

				var packedConsumer IterConsumerFiltered[
					testIterPacked,
					testIterFilter,
				] = ConsumerUnpackedNew(
					unpackedConsumer,
					packed2unpacked,
					filterPacked,
				)

				var buf testIterPacked
				f := testIterFilter{}
				stop, e := packedConsumer(&buf, &f)
				t.Run("no error", assertNil(e))
				t.Run("stop", assertEq(stop, true))
				t.Run("3 items", assertEq(len(unpackedItems), 3))
			})
		})

		t.Run("IterConsumeManyFilteredNew", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var dummyIter uint8 = 0
				iterNext := func(_dummy *uint8) (hasNext bool) { return false }
				iterGet := func(_dummy *uint8, val *testIterPacked) error { return nil }
				iterErr := func(_dummy *uint8) error { return nil }

				var items []testIterPacked
				var consumer IterConsumerFiltered[testIterPacked, testIterFilter] = func(
					val *testIterPacked,
					f *testIterFilter,
				) (stop bool, e error) {
					items = append(items, *val)
					return
				}
				var f func(
					dummyIter *uint8,
					f *testIterFilter,
					consumer IterConsumerFiltered[testIterPacked, testIterFilter],
					buf *testIterPacked,
				) (stop bool, e error) = IterConsumeManyFilteredNew[
					*uint8,
					testIterPacked,
					testIterFilter,
				](
					iterNext,
					iterGet,
					iterErr,
				)

				var buf testIterPacked
				var filter testIterFilter = testIterFilter{}
				stop, e := f(&dummyIter, &filter, consumer, &buf)
				t.Run("no error", assertNil(e))
				t.Run("non stop", assertEq(stop, false))
				t.Run("no items", assertEq(len(items), 0))
			})

			t.Run("single item", func(t *testing.T) {
				t.Parallel()

				var dummyIter uint8 = 0
				iterNext := func(dummy *uint8) (hasNext bool) {
					hasNext = 0 == (*dummy)
					*dummy += 1
					return
				}
				iterGet := func(_dummy *uint8, val *testIterPacked) error { return nil }
				iterErr := func(_dummy *uint8) error { return nil }

				var items []testIterPacked
				var consumer IterConsumerFiltered[testIterPacked, testIterFilter] = func(
					val *testIterPacked,
					f *testIterFilter,
				) (stop bool, e error) {
					items = append(items, *val)
					return
				}
				var f func(
					dummyIter *uint8,
					f *testIterFilter,
					consumer IterConsumerFiltered[testIterPacked, testIterFilter],
					buf *testIterPacked,
				) (stop bool, e error) = IterConsumeManyFilteredNew[
					*uint8,
					testIterPacked,
					testIterFilter,
				](
					iterNext,
					iterGet,
					iterErr,
				)

				var buf testIterPacked
				var filter testIterFilter = testIterFilter{}
				stop, e := f(&dummyIter, &filter, consumer, &buf)
				t.Run("no error", assertNil(e))
				t.Run("non stop", assertEq(stop, false))
				t.Run("single item", assertEq(len(items), 1))
			})

			t.Run("too many items", func(t *testing.T) {
				t.Parallel()

				var dummyIter uint8 = 0
				iterNext := func(dummy *uint8) (hasNext bool) {
					hasNext = (*dummy) < 10
					*dummy += 1
					return
				}
				iterGet := func(_dummy *uint8, val *testIterPacked) error { return nil }
				iterErr := func(_dummy *uint8) error { return nil }

				var items []testIterPacked
				var consumer IterConsumerFiltered[testIterPacked, testIterFilter] = func(
					val *testIterPacked,
					f *testIterFilter,
				) (stop bool, e error) {
					items = append(items, *val)
					return 5 <= len(items), nil
				}
				var f func(
					dummyIter *uint8,
					f *testIterFilter,
					consumer IterConsumerFiltered[testIterPacked, testIterFilter],
					buf *testIterPacked,
				) (stop bool, e error) = IterConsumeManyFilteredNew[
					*uint8,
					testIterPacked,
					testIterFilter,
				](
					iterNext,
					iterGet,
					iterErr,
				)

				var buf testIterPacked
				var filter testIterFilter = testIterFilter{}
				stop, e := f(&dummyIter, &filter, consumer, &buf)
				t.Run("no error", assertNil(e))
				t.Run("stop", assertEq(stop, true))
				t.Run("single item", assertEq(len(items), 5))
			})
		})
	})

	t.Run("ConsumerDecodedNew", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var decodedItems []testIterDecoded
			var decodedConsumer IterConsumerFiltered[testIterDecoded, testIterFilter] = func(
				val *testIterDecoded,
				f *testIterFilter,
			) (stop bool, e error) {
				decodedItems = append(decodedItems, *val)
				return
			}
			decoder := func(encoded *testIterPacked) (d testIterDecoded, e error) { return }
			filterEncoded := func(e *testIterPacked, f *testIterFilter) (keep bool) { return }
			var encodedConsumer IterConsumerFiltered[
				testIterPacked,
				testIterFilter,
			] = ConsumerDecodedNew(
				decodedConsumer,
				decoder,
				filterEncoded,
			)

			var encoded testIterPacked = testIterPacked{}
			var f testIterFilter = testIterFilter{}
			stop, e := encodedConsumer(&encoded, &f)
			t.Run("no error", assertNil(e))
			t.Run("non stop", assertEq(stop, false))
			t.Run("no item", assertEq(len(decodedItems), 0))
		})

		t.Run("encoded filter ok", func(t *testing.T) {
			t.Parallel()

			var decodedItems []testIterDecoded
			var decodedConsumer IterConsumerFiltered[testIterDecoded, testIterFilter] = func(
				val *testIterDecoded,
				f *testIterFilter,
			) (stop bool, e error) {
				decodedItems = append(decodedItems, *val)
				return
			}
			decoder := func(encoded *testIterPacked) (d testIterDecoded, e error) { return }
			filterEncoded := func(e *testIterPacked, f *testIterFilter) (keep bool) { return true }
			var encodedConsumer IterConsumerFiltered[
				testIterPacked,
				testIterFilter,
			] = ConsumerDecodedNew(
				decodedConsumer,
				decoder,
				filterEncoded,
			)

			var encoded testIterPacked = testIterPacked{}
			var f testIterFilter = testIterFilter{}
			stop, e := encodedConsumer(&encoded, &f)
			t.Run("no error", assertNil(e))
			t.Run("non stop", assertEq(stop, false))
			t.Run("single item", assertEq(len(decodedItems), 1))
		})
	})
}
