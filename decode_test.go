package local

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

var testErrorInvalidKey = errors.New("invalid key")
var testErrorInvalidVal = errors.New("invalid val")

type testEncodedRow struct {
	key []byte
	val []byte
}

func (e testEncodedRow) decodeKey() (uint8, error) {
	if 1 == len(e.key) {
		return e.key[0], nil
	}
	return 0, testErrorInvalidKey
}

func (e testEncodedRow) decodeVal() (uint64, error) {
	if 8 == len(e.val) {
		return binary.BigEndian.Uint64(e.val), nil
	}
	return 0, testErrorInvalidVal
}

func (e testEncodedRow) decode(key uint8) (testDecodedRow, error) {
	val, err := e.decodeVal()
	return testDecodedRow{key, val}, err
}

func (e testEncodedRow) Decode() (testDecodedRow, error) {
	return composeErr(
		func(_ testEncodedRow) (uint8, error) { return e.decodeKey() },
		e.decode,
	)(e)
}

type testDecodedRow struct {
	key uint8
	val uint64
}

func TestDecode(t *testing.T) {
	t.Parallel()

	t.Run("Decode", func(t *testing.T) {
		t.Parallel()

		t.Run("NewAll", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				getEncoded := func(_ context.Context, _b Bucket) ([]testEncodedRow, error) {
					return nil, nil
				}

				var decode Decode[testEncodedRow, testDecodedRow] = func(e testEncodedRow) (
					dec testDecodedRow,
					err error,
				) {
					return e.Decode()
				}

				var getDecoded func(
					ctx context.Context,
					bkt Bucket,
				) (decoded []testDecodedRow, err error) = decode.NewAll(getEncoded)

				decoded, e := getDecoded(context.Background(), BucketNew(""))

				t.Run("no error", assertNil(e))
				t.Run("0 length", assertEq(len(decoded), 0))
			})

			t.Run("single", func(t *testing.T) {
				t.Parallel()

				getEncoded := func(_ context.Context, _b Bucket) ([]testEncodedRow, error) {
					return []testEncodedRow{
						{
							key: []byte{0x42},
							val: []byte{
								0x01,
								0x23,
								0x45,
								0x67,
								0x89,
								0xab,
								0xcd,
								0xef,
							},
						},
					}, nil
				}

				var decode Decode[testEncodedRow, testDecodedRow] = func(e testEncodedRow) (
					dec testDecodedRow,
					err error,
				) {
					return e.Decode()
				}

				var getDecoded func(
					ctx context.Context,
					bkt Bucket,
				) (decoded []testDecodedRow, err error) = decode.NewAll(getEncoded)

				decoded, e := getDecoded(context.Background(), BucketNew(""))

				t.Run("no error", assertNil(e))
				t.Run("single item", assertEq(len(decoded), 1))
			})

			t.Run("invalid item", func(t *testing.T) {
				t.Parallel()

				getEncoded := func(_ context.Context, _b Bucket) ([]testEncodedRow, error) {
					return []testEncodedRow{
						{
							key: []byte{0x42},
							val: []byte{
								0x23,
								0x45,
								0x67,
								0x89,
								0xab,
								0xcd,
								0xef,
							},
						},
					}, nil
				}

				var decode Decode[testEncodedRow, testDecodedRow] = func(e testEncodedRow) (
					dec testDecodedRow,
					err error,
				) {
					return e.Decode()
				}

				var getDecoded func(
					ctx context.Context,
					bkt Bucket,
				) (decoded []testDecodedRow, err error) = decode.NewAll(getEncoded)

				_, e := getDecoded(context.Background(), BucketNew(""))

				t.Run("error", assertEq(nil != e, true))
			})
		})
	})

	t.Run("RemoteFilterNewDecoded", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var decode Decode[uint16, [2]uint8] = func(encoded uint16) ([2]uint8, error) {
				return [2]uint8{0, 0}, nil
			}

			const filter uint16 = 0x42

			var getDecoded func(
				ctx context.Context,
				bucket Bucket,
				filter uint16,
			) (decoded [][2]uint8, e error) = RemoteFilterNewDecoded(
				decode,
				func(_ context.Context, _b Bucket, _filter uint16) (encoded []uint16, e error) {
					return nil, nil
				},
			)

			decoded, e := getDecoded(context.Background(), BucketNew(""), 0x00)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(decoded), 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var decode Decode[uint16, [2]uint8] = func(encoded uint16) ([2]uint8, error) {
				return [2]uint8{
					uint8(encoded >> 8),
					uint8(encoded & 0xff),
				}, nil
			}

			const filter uint16 = 0x0042

			var getDecoded func(
				ctx context.Context,
				bucket Bucket,
				filter uint16,
			) (decoded [][2]uint8, e error) = RemoteFilterNewDecoded(
				decode,
				func(_ context.Context, _b Bucket, _filter uint16) (encoded []uint16, e error) {
					return []uint16{
						0x0042,
					}, nil
				},
			)

			decoded, e := getDecoded(context.Background(), BucketNew(""), 0x0042)
			t.Run("no error", assertNil(e))
			t.Run("single item", assertEq(len(decoded), 1))

			var item1 [2]uint8 = decoded[0]
			t.Run("ix 0", assertEq(item1[0], 0x00))
			t.Run("ix 1", assertEq(item1[1], 0x42))
		})
	})
}
