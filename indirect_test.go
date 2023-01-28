package local

import (
	"context"
	"encoding/binary"
	"testing"
)

type testIndirectKey struct {
	serial int32
}

type testIndirectEncoded struct {
	key [10]byte
	val [16]byte
}

func (e *testIndirectEncoded) decode() (decoded testIndirectDecoded, err error) {
	decoded.timestamp, _ = binary.Varint(e.key[0:8])
	decoded.coarse = binary.BigEndian.Uint16(e.key[8:10])
	decoded.val[0] = binary.BigEndian.Uint64(e.val[0x00:0x08])
	decoded.val[1] = binary.BigEndian.Uint64(e.val[0x08:0x10])
	return
}

type testIndirectDecoded struct {
	val       [2]uint64
	timestamp int64
	coarse    uint16
}

func (d *testIndirectDecoded) unnest() (unnested []testIndirectUnpacked, e error) {
	unnested = append(unnested, testIndirectUnpacked{
		timestamp: d.timestamp,
		code:      uint32(d.val[0] >> 32),
		id:        uint32(d.val[0] & 0xffffffff),
		coarse:    d.coarse,
	})
	unnested = append(unnested, testIndirectUnpacked{
		timestamp: d.timestamp,
		code:      uint32(d.val[1] >> 32),
		id:        uint32(d.val[1] & 0xffffffff),
		coarse:    d.coarse,
	})
	return
}

type testIndirectUnpacked struct {
	timestamp int64
	code      uint32
	id        uint32
	coarse    uint16
}

type testIndirectFilter struct {
	coarse uint16
	code   uint32
}

func testIndirectGetByKeyDecodedNew[G any](
	getEncoded func(
		ctx context.Context, con G, key testIndirectKey, encoded *testIndirectEncoded,
	) (got bool, e error),
	buf *testIndirectEncoded,
) func(c context.Context, con G, k testIndirectKey, d *testIndirectDecoded) (got bool, e error) {
	return GetByKeyNewDecoded(
		getEncoded,
		func(encoded *testIndirectEncoded) (decoded testIndirectDecoded, e error) {
			return encoded.decode()
		},
		buf,
	)
}

func testIndirectGetByKeysNew[G any](
	getByKey func(ctx context.Context, con G, key testIndirectKey, decoded *testIndirectDecoded) (
		got bool,
		e error,
	),
	filterPacked func(decoded *testIndirectDecoded, f *testIndirectFilter) (keep bool),
	filterUnpacked func(unpacked *testIndirectUnpacked, f *testIndirectFilter) (keep bool),
	consumeUnpacked IterConsumer[testIndirectUnpacked],
) func(
	ctx context.Context,
	keys []testIndirectKey,
	con G,
	buf *testIndirectDecoded,
	f *testIndirectFilter,
) error {
	return GetByKeysNewUnnested(
		getByKey,
		func(decoded *testIndirectDecoded) (unnested []testIndirectUnpacked, e error) {
			return decoded.unnest()
		},
		filterPacked,
		filterUnpacked,
		consumeUnpacked,
	)
}

func TestIndirect(t *testing.T) {
	t.Parallel()

	t.Run("GetByKeysNewUnnested", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			getByKey := func(
				_ context.Context,
				dummyCon uint8,
				k testIndirectKey,
				d *testIndirectDecoded,
			) (got bool, e error) {
				return false, nil
			}

			filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
				return true
			}

			filterUnpacked := func(d *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
				return true
			}

			var unpacked []testIndirectUnpacked

			var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
				stop bool,
				e error,
			) {
				unpacked = append(unpacked, *u)
				return
			}

			var f func(
				ctx context.Context,
				keys []testIndirectKey,
				dummyCon uint8,
				buf *testIndirectDecoded,
				f *testIndirectFilter,
			) error = testIndirectGetByKeysNew(
				getByKey,
				filterPacked,
				filterUnpacked,
				consumer,
			)

			e := f(context.Background(), nil, 0, nil, nil)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 0))
		})

		t.Run("single key", func(t *testing.T) {
			t.Parallel()

			getByKey := func(
				_ context.Context,
				dummyCon uint8,
				k testIndirectKey,
				d *testIndirectDecoded,
			) (got bool, e error) {
				d.val[0] = 0x0123456789abcdef
				d.val[1] = 0x1123456789abcdef
				d.timestamp = 0x2123456789abcdef
				d.coarse = 0x0123
				return true, nil
			}

			filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
				return true
			}

			filterUnpacked := func(d *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
				return true
			}

			var unpacked []testIndirectUnpacked

			var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
				stop bool,
				e error,
			) {
				unpacked = append(unpacked, *u)
				return
			}

			var f func(
				ctx context.Context,
				keys []testIndirectKey,
				dummyCon uint8,
				buf *testIndirectDecoded,
				f *testIndirectFilter,
			) error = testIndirectGetByKeysNew(
				getByKey,
				filterPacked,
				filterUnpacked,
				consumer,
			)

			var decodedBuf testIndirectDecoded
			var filter testIndirectFilter = testIndirectFilter{
				coarse: 0x0123,
				code:   0x1123,
			}

			e := f(context.Background(), []testIndirectKey{
				{serial: 0x01234567},
				{serial: 0x01234568},
			}, 0, &decodedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("4 items", assertEq(len(unpacked), 4))
		})

		t.Run("coarse filter", func(t *testing.T) {
			t.Parallel()

			getByKey := func(
				_ context.Context,
				dummyCon uint8,
				k testIndirectKey,
				d *testIndirectDecoded,
			) (got bool, e error) {
				d.val[0] = 0x0123456789abcde0
				d.val[1] = 0x0123456789abcde1
				d.timestamp = 0x2123456789abcdef
				d.coarse = uint16(k.serial >> 16)
				return true, nil
			}

			filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
				return d.coarse == f.coarse
			}

			filterUnpacked := func(u *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
				return u.code == f.code
			}

			var unpacked []testIndirectUnpacked

			var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
				stop bool,
				e error,
			) {
				unpacked = append(unpacked, *u)
				return
			}

			var f func(
				ctx context.Context,
				keys []testIndirectKey,
				dummyCon uint8,
				buf *testIndirectDecoded,
				f *testIndirectFilter,
			) error = testIndirectGetByKeysNew(
				getByKey,
				filterPacked,
				filterUnpacked,
				consumer,
			)

			var decodedBuf testIndirectDecoded
			var filter testIndirectFilter = testIndirectFilter{
				coarse: 0x0123,
				code:   0x01234567,
			}

			e := f(context.Background(), []testIndirectKey{
				{serial: 0x01234567},
				{serial: 0x11234568},
			}, 0, &decodedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("2 items", assertEq(len(unpacked), 2))
		})

		t.Run("fine filter", func(t *testing.T) {
			t.Parallel()

			getByKey := func(
				_ context.Context,
				dummyCon uint8,
				k testIndirectKey,
				d *testIndirectDecoded,
			) (got bool, e error) {
				d.val[0] = 0x0123456789abcde0
				d.val[1] = 0x1123456789abcde1
				d.timestamp = 0x2123456789abcdef
				d.coarse = uint16(k.serial >> 16)
				return true, nil
			}

			filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
				return d.coarse == f.coarse
			}

			filterUnpacked := func(u *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
				return u.code == f.code
			}

			var unpacked []testIndirectUnpacked

			var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
				stop bool,
				e error,
			) {
				unpacked = append(unpacked, *u)
				return
			}

			var f func(
				ctx context.Context,
				keys []testIndirectKey,
				dummyCon uint8,
				buf *testIndirectDecoded,
				f *testIndirectFilter,
			) error = testIndirectGetByKeysNew(
				getByKey,
				filterPacked,
				filterUnpacked,
				consumer,
			)

			var decodedBuf testIndirectDecoded
			var filter testIndirectFilter = testIndirectFilter{
				coarse: 0x0123,
				code:   0x01234567,
			}

			e := f(context.Background(), []testIndirectKey{
				{serial: 0x01234567},
				{serial: 0x11234568},
			}, 0, &decodedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("single item", assertEq(len(unpacked), 1))
		})

		t.Run("no val found", func(t *testing.T) {
			t.Parallel()

			getByKey := func(
				_ context.Context,
				dummyCon uint8,
				k testIndirectKey,
				d *testIndirectDecoded,
			) (got bool, e error) {
				d.val[0] = 0x0123456789abcde0
				d.val[1] = 0x1123456789abcde1
				d.timestamp = 0x2123456789abcdef
				d.coarse = uint16(k.serial >> 16)
				return false, nil
			}

			filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
				return d.coarse == f.coarse
			}

			filterUnpacked := func(u *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
				return u.code == f.code
			}

			var unpacked []testIndirectUnpacked

			var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
				stop bool,
				e error,
			) {
				unpacked = append(unpacked, *u)
				return
			}

			var f func(
				ctx context.Context,
				keys []testIndirectKey,
				dummyCon uint8,
				buf *testIndirectDecoded,
				f *testIndirectFilter,
			) error = testIndirectGetByKeysNew(
				getByKey,
				filterPacked,
				filterUnpacked,
				consumer,
			)

			var decodedBuf testIndirectDecoded
			var filter testIndirectFilter = testIndirectFilter{
				coarse: 0x0123,
				code:   0x01234567,
			}

			e := f(context.Background(), []testIndirectKey{
				{serial: 0x01234567},
				{serial: 0x11234568},
			}, 0, &decodedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(unpacked), 0))
		})

		t.Run("stop", func(t *testing.T) {
			t.Parallel()

			getByKey := func(
				_ context.Context,
				dummyCon uint8,
				k testIndirectKey,
				d *testIndirectDecoded,
			) (got bool, e error) {
				d.val[0] = 0x0123456789abcdef
				d.val[1] = 0x1123456789abcdef
				d.timestamp = 0x2123456789abcdef
				d.coarse = 0x0123
				return true, nil
			}

			filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
				return true
			}

			filterUnpacked := func(d *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
				return true
			}

			var unpacked []testIndirectUnpacked

			var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
				stop bool,
				e error,
			) {
				unpacked = append(unpacked, *u)
				stop = 2 < len(unpacked)
				return
			}

			var f func(
				ctx context.Context,
				keys []testIndirectKey,
				dummyCon uint8,
				buf *testIndirectDecoded,
				f *testIndirectFilter,
			) error = testIndirectGetByKeysNew(
				getByKey,
				filterPacked,
				filterUnpacked,
				consumer,
			)

			var decodedBuf testIndirectDecoded
			var filter testIndirectFilter = testIndirectFilter{
				coarse: 0x0123,
				code:   0x1123,
			}

			e := f(context.Background(), []testIndirectKey{
				{serial: 0x01234567},
				{serial: 0x01234568},
			}, 0, &decodedBuf, &filter)
			t.Run("no error", assertNil(e))
			t.Run("3 items", assertEq(len(unpacked), 3))
		})

		t.Run("use encoded", func(t *testing.T) {
			t.Parallel()

			t.Run("got", func(t *testing.T) {
				t.Parallel()

				getEncodedByKey := func(
					c context.Context,
					dummyCon uint8,
					k testIndirectKey,
					e *testIndirectEncoded,
				) (got bool, err error) {
					binary.BigEndian.PutUint64(e.key[0:8], 0x0123456789abcef)
					binary.BigEndian.PutUint16(e.key[8:10], 0x0123)
					binary.BigEndian.PutUint64(e.val[0x00:0x08], 0x0123456789abcdef)
					binary.BigEndian.PutUint64(e.val[0x08:0x10], 0x0123456789abcdef)
					return true, nil
				}
				var encodedBuf testIndirectEncoded
				var getByKey func(
					ctx context.Context,
					dummyCon uint8,
					k testIndirectKey,
					d *testIndirectDecoded,
				) (got bool, e error) = GetByKeyNewDecoded(
					getEncodedByKey,
					func(encoded *testIndirectEncoded) (decoded testIndirectDecoded, e error) {
						return encoded.decode()
					},
					&encodedBuf,
				)

				filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
					return true
				}

				filterUnpacked := func(d *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
					return true
				}

				var unpacked []testIndirectUnpacked

				var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
					stop bool,
					e error,
				) {
					unpacked = append(unpacked, *u)
					return
				}

				var f func(
					ctx context.Context,
					keys []testIndirectKey,
					dummyCon uint8,
					buf *testIndirectDecoded,
					f *testIndirectFilter,
				) error = testIndirectGetByKeysNew(
					getByKey,
					filterPacked,
					filterUnpacked,
					consumer,
				)

				var decodedBuf testIndirectDecoded
				var filter testIndirectFilter = testIndirectFilter{
					coarse: 0x0123,
					code:   0x1123,
				}

				e := f(context.Background(), []testIndirectKey{
					{serial: 0x01234567},
					{serial: 0x01234568},
				}, 0, &decodedBuf, &filter)
				t.Run("no error", assertNil(e))
				t.Run("4 items", assertEq(len(unpacked), 4))
			})

			t.Run("nothing got", func(t *testing.T) {
				t.Parallel()

				getEncodedByKey := func(
					c context.Context,
					dummyCon uint8,
					k testIndirectKey,
					e *testIndirectEncoded,
				) (got bool, err error) {
					binary.BigEndian.PutUint64(e.key[0:8], 0x0123456789abcef)
					binary.BigEndian.PutUint16(e.key[8:10], 0x0123)
					binary.BigEndian.PutUint64(e.val[0x00:0x08], 0x0123456789abcdef)
					binary.BigEndian.PutUint64(e.val[0x08:0x10], 0x0123456789abcdef)
					return false, nil
				}
				var encodedBuf testIndirectEncoded
				var getByKey func(
					ctx context.Context,
					dummyCon uint8,
					k testIndirectKey,
					d *testIndirectDecoded,
				) (got bool, e error) = GetByKeyNewDecoded(
					getEncodedByKey,
					func(encoded *testIndirectEncoded) (decoded testIndirectDecoded, e error) {
						return encoded.decode()
					},
					&encodedBuf,
				)

				filterPacked := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
					return true
				}

				filterUnpacked := func(d *testIndirectUnpacked, f *testIndirectFilter) (keep bool) {
					return true
				}

				var unpacked []testIndirectUnpacked

				var consumer IterConsumer[testIndirectUnpacked] = func(u *testIndirectUnpacked) (
					stop bool,
					e error,
				) {
					unpacked = append(unpacked, *u)
					return
				}

				var f func(
					ctx context.Context,
					keys []testIndirectKey,
					dummyCon uint8,
					buf *testIndirectDecoded,
					f *testIndirectFilter,
				) error = testIndirectGetByKeysNew(
					getByKey,
					filterPacked,
					filterUnpacked,
					consumer,
				)

				var decodedBuf testIndirectDecoded
				var filter testIndirectFilter = testIndirectFilter{
					coarse: 0x0123,
					code:   0x1123,
				}

				e := f(context.Background(), []testIndirectKey{
					{serial: 0x01234567},
					{serial: 0x01234568},
				}, 0, &decodedBuf, &filter)
				t.Run("no error", assertNil(e))
				t.Run("no items", assertEq(len(unpacked), 0))
			})

		})
	})

	t.Run("GetByKeyDecodedNew", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var getEncoded GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				dummy uint8,
				bucket *string,
				k testIndirectKey,
				encoded *testIndirectEncoded,
				f *testIndirectFilter,
			) (got bool, e error) {
				return
			}

			decoder := func(e *testIndirectEncoded) (d testIndirectDecoded, err error) {
				return e.decode()
			}

			var buf testIndirectEncoded
			filter := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) { return }

			var gbk GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectDecoded,
			] = GetByKeyDecodedNew(
				getEncoded,
				decoder,
				&buf,
				filter,
			)

			f := testIndirectFilter{}

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			var bufDecoded testIndirectDecoded
			got, e := gbk(
				context.Background(),
				0,
				&bucket,
				testIndirectKey{serial: 0x01234567},
				&bufDecoded,
				&f,
			)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(got, false))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var getEncoded GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				dummy uint8,
				bucket *string,
				k testIndirectKey,
				encoded *testIndirectEncoded,
				f *testIndirectFilter,
			) (got bool, e error) {
				copy(encoded.key[:], []byte("0123456789"))
				copy(encoded.val[:], []byte("0123456789abcdef"))
				return true, nil
			}

			decoder := func(e *testIndirectEncoded) (d testIndirectDecoded, err error) {
				return e.decode()
			}

			var buf testIndirectEncoded
			filter := func(d *testIndirectDecoded, f *testIndirectFilter) (keep bool) {
				return true
			}

			var gbk GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectDecoded,
			] = GetByKeyDecodedNew(
				getEncoded,
				decoder,
				&buf,
				filter,
			)

			f := testIndirectFilter{}

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			var bufDecoded testIndirectDecoded
			got, e := gbk(
				context.Background(),
				0,
				&bucket,
				testIndirectKey{serial: 0x01234567},
				&bufDecoded,
				&f,
			)
			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(got, true))
		})
	})

	t.Run("GetByKeysNew", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var getKeys GetKeys[uint8, string, testIndirectFilter, testIndirectKey] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
			) (keys []testIndirectKey, e error) {
				return
			}

			var getByKey GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				key testIndirectKey,
				val *testIndirectEncoded,
				filter *testIndirectFilter,
			) (got bool, e error) {
				return
			}

			var getByKeys Got2Consumer[
				uint8,
				testIndirectKey,
				testIndirectFilter,
				string,
				testIndirectEncoded,
			] = GetByKeysNew(
				getKeys,
				getByKey,
			)

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			f := testIndirectFilter{}
			var buf testIndirectEncoded
			var items []testIndirectEncoded
			consumer := func(val *testIndirectEncoded, f *testIndirectFilter) (stop bool, e error) {
				items = append(items, *val)
				return
			}
			e := getByKeys(
				context.Background(),
				0,
				&bucket,
				&f,
				&buf,
				consumer,
			)

			t.Run("no error", assertNil(e))
			t.Run("no items", assertEq(len(items), 0))
		})

		t.Run("single item", func(t *testing.T) {
			t.Parallel()

			var getKeys GetKeys[uint8, string, testIndirectFilter, testIndirectKey] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
			) (keys []testIndirectKey, e error) {
				keys = append(keys, testIndirectKey{serial: 0x01234567})
				return
			}

			var getByKey GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				key testIndirectKey,
				val *testIndirectEncoded,
				filter *testIndirectFilter,
			) (got bool, e error) {
				return true, nil
			}

			var getByKeys Got2Consumer[
				uint8,
				testIndirectKey,
				testIndirectFilter,
				string,
				testIndirectEncoded,
			] = GetByKeysNew(
				getKeys,
				getByKey,
			)

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			f := testIndirectFilter{}
			var buf testIndirectEncoded
			var items []testIndirectEncoded
			consumer := func(val *testIndirectEncoded, f *testIndirectFilter) (stop bool, e error) {
				items = append(items, *val)
				return
			}
			e := getByKeys(
				context.Background(),
				0,
				&bucket,
				&f,
				&buf,
				consumer,
			)

			t.Run("no error", assertNil(e))
			t.Run("single item", assertEq(len(items), 1))
		})

		t.Run("stale key", func(t *testing.T) {
			t.Parallel()

			var getKeys GetKeys[uint8, string, testIndirectFilter, testIndirectKey] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
			) (keys []testIndirectKey, e error) {
				keys = append(keys, testIndirectKey{serial: 0x01234567})
				return
			}

			var getByKey GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				key testIndirectKey,
				val *testIndirectEncoded,
				filter *testIndirectFilter,
			) (got bool, e error) {
				return false, nil
			}

			var getByKeys Got2Consumer[
				uint8,
				testIndirectKey,
				testIndirectFilter,
				string,
				testIndirectEncoded,
			] = GetByKeysNew(
				getKeys,
				getByKey,
			)

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			f := testIndirectFilter{}
			var buf testIndirectEncoded
			var items []testIndirectEncoded
			consumer := func(val *testIndirectEncoded, f *testIndirectFilter) (stop bool, e error) {
				items = append(items, *val)
				return
			}
			e := getByKeys(
				context.Background(),
				0,
				&bucket,
				&f,
				&buf,
				consumer,
			)

			t.Run("no error", assertNil(e))
			t.Run("no item", assertEq(len(items), 0))
		})

		t.Run("too many items", func(t *testing.T) {
			t.Parallel()

			var getKeys GetKeys[uint8, string, testIndirectFilter, testIndirectKey] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
			) (keys []testIndirectKey, e error) {
				keys = append(keys, testIndirectKey{serial: 0x01234567})
				keys = append(keys, testIndirectKey{serial: 0x01234568})
				keys = append(keys, testIndirectKey{serial: 0x01234569})
				return
			}

			var getByKey GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				key testIndirectKey,
				val *testIndirectEncoded,
				filter *testIndirectFilter,
			) (got bool, e error) {
				return true, nil
			}

			var getByKeys Got2Consumer[
				uint8,
				testIndirectKey,
				testIndirectFilter,
				string,
				testIndirectEncoded,
			] = GetByKeysNew(
				getKeys,
				getByKey,
			)

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			f := testIndirectFilter{}
			var buf testIndirectEncoded
			var items []testIndirectEncoded
			consumer := func(val *testIndirectEncoded, f *testIndirectFilter) (stop bool, e error) {
				items = append(items, *val)
				return 1 < len(items), nil
			}
			e := getByKeys(
				context.Background(),
				0,
				&bucket,
				&f,
				&buf,
				consumer,
			)

			t.Run("no error", assertNil(e))
			t.Run("2 items", assertEq(len(items), 2))
		})
	})

	t.Run("GetWithPlanNew", func(t *testing.T) {
		t.Parallel()

		t.Run("indirect", func(t *testing.T) {
			t.Parallel()

			var getKeys GetKeys[uint8, string, testIndirectFilter, testIndirectKey] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
			) (keys []testIndirectKey, e error) {
				keys = append(keys, testIndirectKey{serial: 0x01234567})
				return
			}

			var getByKey GetByKey[
				uint8,
				string,
				testIndirectFilter,
				testIndirectKey,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				key testIndirectKey,
				val *testIndirectEncoded,
				filter *testIndirectFilter,
			) (got bool, e error) {
				return true, nil
			}

			var getByKeys Got2Consumer[
				uint8,
				testIndirectKey,
				testIndirectFilter,
				string,
				testIndirectEncoded,
			] = GetByKeysNew(
				getKeys,
				getByKey,
			)

			plan := func(_ *testIndirectFilter) (directScan bool) { return false }

			var getWithPlan func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
				buf *testIndirectEncoded,
				consumer func(val *testIndirectEncoded, f *testIndirectFilter) (stop bool, e error),
			) error = GetWithPlanNew(
				getByKeys,
				nil,
				plan,
			)

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			f := testIndirectFilter{}
			var buf testIndirectEncoded
			var items []testIndirectEncoded
			consumer := func(
				val *testIndirectEncoded,
				f *testIndirectFilter,
			) (stop bool, e error) {
				items = append(items, *val)
				return
			}
			e := getWithPlan(
				context.Background(),
				0,
				&bucket,
				&f,
				&buf,
				consumer,
			)

			t.Run("no error", assertNil(e))
			t.Run("single item", assertEq(len(items), 1))
		})

		t.Run("direct", func(t *testing.T) {
			t.Parallel()

			var getDirect Got2Consumer[
				uint8,
				testIndirectKey,
				testIndirectFilter,
				string,
				testIndirectEncoded,
			] = func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
				buf *testIndirectEncoded,
				consumer func(val *testIndirectEncoded, f *testIndirectFilter) (stop bool, e error),
			) error {
				_, _ = consumer(buf, f)
				return nil
			}

			plan := func(_ *testIndirectFilter) (directScan bool) { return true }

			var getWithPlan func(
				ctx context.Context,
				_dummy uint8,
				bucket *string,
				f *testIndirectFilter,
				buf *testIndirectEncoded,
				consumer func(v *testIndirectEncoded, f *testIndirectFilter) (stop bool, e error),
			) error = GetWithPlanNew(
				nil,
				getDirect,
				plan,
			)

			var bucket string = "items_2023_01_22_cafef00ddeadbeafface864299792458"
			f := testIndirectFilter{}
			var buf testIndirectEncoded
			var items []testIndirectEncoded
			consumer := func(
				val *testIndirectEncoded,
				f *testIndirectFilter,
			) (stop bool, e error) {
				items = append(items, *val)
				return
			}
			e := getWithPlan(
				context.Background(),
				0,
				&bucket,
				&f,
				&buf,
				consumer,
			)

			t.Run("no error", assertNil(e))
			t.Run("single item", assertEq(len(items), 1))
		})
	})

	t.Run("GetKeys", func(t *testing.T) {
		t.Parallel()

		t.Run("WithBucketFilter", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var dummyCon uint8 = 0
				var dummyBucket string = "items_2023_01_28_cafef00ddeadbeafface864299792458"
				var dummyFilter uint64 = 0x0123456789abcdef

				var getKeys GetKeys[uint8, string, uint64, int32] = func(
					_ctx context.Context,
					_con uint8,
					_bkt *string,
					_flt *uint64,
				) (keys []int32, e error) {
					return
				}

				bucketFilter := func(
					_ctx context.Context,
					_con uint8,
					bucket *string,
					filter *uint64,
				) (checkMe bool) {
					return false
				}

				var withBucketFilter GetKeys[
					uint8, string, uint64, int32,
				] = getKeys.WithBucketFilter(
					bucketFilter,
				)

				keys, e := withBucketFilter(
					context.Background(),
					dummyCon,
					&dummyBucket,
					&dummyFilter,
				)
				t.Run("no error", assertNil(e))
				t.Run("no items", assertEq(len(keys), 0))
			})

			t.Run("try get keys from a bucket", func(t *testing.T) {
				t.Parallel()

				var dummyCon uint8 = 0
				var dummyBucket string = "items_2023_01_28_cafef00ddeadbeafface864299792458"
				var dummyFilter uint64 = 0x0123456789abcdef

				var getKeys GetKeys[uint8, string, uint64, int32] = func(
					_ctx context.Context,
					_con uint8,
					_bkt *string,
					_flt *uint64,
				) (keys []int32, e error) {
					return []int32{0x42}, nil
				}

				bucketFilter := func(
					_ctx context.Context,
					_con uint8,
					bucket *string,
					filter *uint64,
				) (checkMe bool) {
					return true
				}

				var withBucketFilter GetKeys[
					uint8, string, uint64, int32,
				] = getKeys.WithBucketFilter(
					bucketFilter,
				)

				keys, e := withBucketFilter(
					context.Background(),
					dummyCon,
					&dummyBucket,
					&dummyFilter,
				)
				t.Run("no error", assertNil(e))
				t.Run("single item", assertEq(len(keys), 1))
			})
		})
	})
}
