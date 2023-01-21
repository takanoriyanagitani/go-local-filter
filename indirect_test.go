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
}
