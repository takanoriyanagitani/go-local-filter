package local

import (
	"context"
	"testing"
)

type testPackedRow struct {
	key uint8
	val uint64
}

func (p testPackedRow) unpack() (unpacked []testUnpackedRow) {
	var u [4]uint16 = [4]uint16{
		uint16(0xffff & (p.val >> 0x30)),
		uint16(0xffff & (p.val >> 0x20)),
		uint16(0xffff & (p.val >> 0x10)),
		uint16(0xffff & (p.val >> 0x00)),
	}
	var keyHi uint16 = uint16(p.key) << 8
	return []testUnpackedRow{
		{key: keyHi | (u[0] >> 8), flag: uint8(u[0] & 0xff)},
		{key: keyHi | (u[1] >> 8), flag: uint8(u[1] & 0xff)},
		{key: keyHi | (u[2] >> 8), flag: uint8(u[2] & 0xff)},
		{key: keyHi | (u[3] >> 8), flag: uint8(u[3] & 0xff)},
	}
}

type testUnpackedRow struct {
	key  uint16
	flag uint8
}

func TestUnpack(t *testing.T) {
	t.Parallel()

	t.Run("Unpack", func(t *testing.T) {
		t.Parallel()

		t.Run("NewAll", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				getPacked := func(_ context.Context, _b Bucket) ([]testPackedRow, error) {
					return nil, nil
				}
				var unpack Unpack[testPackedRow, testUnpackedRow] = func(
					packed testPackedRow,
				) (unpacked []testUnpackedRow, e error) {
					return
				}
				getUnpacked := unpack.NewAll(getPacked)
				unpacked, e := getUnpacked(context.Background(), BucketNew(""))
				t.Run("no error", assertNil(e))
				t.Run("0 length", assertEq(len(unpacked), 0))
			})

			t.Run("single", func(t *testing.T) {
				t.Parallel()

				getPacked := func(_ context.Context, _b Bucket) ([]testPackedRow, error) {
					return []testPackedRow{
						{key: 0x42, val: 0x0123456789abcdef},
					}, nil
				}
				var unpack Unpack[testPackedRow, testUnpackedRow] = func(
					packed testPackedRow,
				) (unpacked []testUnpackedRow, e error) {
					return packed.unpack(), nil
				}
				getUnpacked := unpack.NewAll(getPacked)
				unpacked, e := getUnpacked(context.Background(), BucketNew(""))
				t.Run("no error", assertNil(e))
				t.Run("4 items", assertEq(len(unpacked), 4))
			})

			t.Run("double", func(t *testing.T) {
				t.Parallel()

				getPacked := func(_ context.Context, _b Bucket) ([]testPackedRow, error) {
					return []testPackedRow{
						{key: 0x42, val: 0x0123456789abcdef},
						{key: 0xff, val: 0x0123456789abcdef},
					}, nil
				}
				var unpack Unpack[testPackedRow, testUnpackedRow] = func(
					packed testPackedRow,
				) (unpacked []testUnpackedRow, e error) {
					return packed.unpack(), nil
				}
				getUnpacked := unpack.NewAll(getPacked)
				unpacked, e := getUnpacked(context.Background(), BucketNew(""))
				t.Run("no error", assertNil(e))
				t.Run("8 items", assertEq(len(unpacked), 8))
			})
		})
	})

	t.Run("RemoteFilterNewUnpacked", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var unpack Unpack[uint64, uint8] = func(packed uint64) (unpacked []uint8, e error) {
				var u [8]uint8 = [8]uint8{
					uint8(packed >> 56),
					uint8(packed >> 48),
					uint8(packed >> 40),
					uint8(packed >> 32),
					uint8(packed >> 24),
					uint8(packed >> 16),
					uint8(packed >> 8),
					uint8(packed >> 0),
				}
				return u[:], nil
			}

			remote := func(_ context.Context, _b Bucket, filter uint64) (packed []uint64, e error) {
				return
			}

			var getUnpacked func(
				ctx context.Context,
				bucket Bucket,
				filter uint64,
			) (unpacked []uint8, e error) = RemoteFilterNewUnpacked(
				unpack,
				remote,
			)

			unpacked, e := getUnpacked(context.Background(), BucketNew(""), 0x42)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var unpack Unpack[uint64, uint8] = func(packed uint64) (unpacked []uint8, e error) {
				var u [8]uint8 = [8]uint8{
					uint8(packed >> 56),
					uint8(packed >> 48),
					uint8(packed >> 40),
					uint8(packed >> 32),
					uint8(packed >> 24),
					uint8(packed >> 16),
					uint8(packed >> 8),
					uint8(packed >> 0),
				}
				return u[:], nil
			}

			remote := func(_ context.Context, _b Bucket, filter uint64) (packed []uint64, e error) {
				return []uint64{
					0x0123456789abcdef,
				}, nil
			}

			var getUnpacked func(
				ctx context.Context,
				bucket Bucket,
				filter uint64,
			) (unpacked []uint8, e error) = RemoteFilterNewUnpacked(
				unpack,
				remote,
			)

			unpacked, e := getUnpacked(context.Background(), BucketNew(""), 0x0123456789abcdef)
			t.Run("no error", assertNil(e))
			t.Run("8 items", assertEq(len(unpacked), 8))

			t.Run("item 0", assertEq(unpacked[0], 0x01))
			t.Run("item 1", assertEq(unpacked[1], 0x23))
			t.Run("item 2", assertEq(unpacked[2], 0x45))
			t.Run("item 3", assertEq(unpacked[3], 0x67))
			t.Run("item 4", assertEq(unpacked[4], 0x89))
			t.Run("item 5", assertEq(unpacked[5], 0xab))
			t.Run("item 6", assertEq(unpacked[6], 0xcd))
			t.Run("item 7", assertEq(unpacked[7], 0xef))
		})
	})
}
