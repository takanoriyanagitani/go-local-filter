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
				) (unpacked []testUnpackedRow) {
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
				) (unpacked []testUnpackedRow) {
					return packed.unpack()
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
				) (unpacked []testUnpackedRow) {
					return packed.unpack()
				}
				getUnpacked := unpack.NewAll(getPacked)
				unpacked, e := getUnpacked(context.Background(), BucketNew(""))
				t.Run("no error", assertNil(e))
				t.Run("8 items", assertEq(len(unpacked), 8))
			})
		})
	})
}
