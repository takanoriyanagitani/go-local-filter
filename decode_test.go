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
}
