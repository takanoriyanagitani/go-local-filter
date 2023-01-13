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
}
