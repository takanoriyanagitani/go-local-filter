package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"sort"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/takanoriyanagitani/go-local-filter/v2"
)

type unpackSampleRaw struct {
	key []byte
	val []byte
}

type unpackSampleFlatKey struct {
	full   uint16
	serial int32
	bloom  [16]byte
}

type unpackSampleRow struct {
	flat       unpackSampleFlatKey
	primaryKey int64
}

func (u *unpackSampleRow) sort(s []unpackSampleRow) {
	sort.SliceStable(
		s,
		func(j, i int) bool {
			var ir = s[i]
			var jr = s[j]
			return ir.flat.serial < jr.flat.serial
		},
	)
}

type unpackSampleFlatKeys struct{ keys []unpackSampleFlatKey }

func (k *unpackSampleFlatKeys) Clear() { k.keys = k.keys[:0] }

func (k *unpackSampleFlatKeys) SortStable() {
	sort.SliceStable(
		k.keys,
		func(i, j int) bool {
			var fi = k.keys[i].full
			var fj = k.keys[j].full
			return fi < fj
		},
	)
}

func (k *unpackSampleFlatKeys) SortStableDesc() {
	sort.SliceStable(
		k.keys,
		func(j, i int) bool {
			var fi = k.keys[i].full
			var fj = k.keys[j].full
			return fi < fj
		},
	)
}

func (k *unpackSampleFlatKeys) FromRaw(raw *unpackSampleRaw) error {
	if len(raw.key) < 1 {
		return fmt.Errorf("No key found. len: %v", len(raw.key))
	}
	k.Clear()
	var high uint8 = raw.key[0]
	hasNext := func(i int, l int) bool {
		var lbi int = i * 21
		var ube int = lbi + 21
		return ube <= l
	}
	var l int = len(raw.val)
	var bloom [16]byte
	for i := 0; hasNext(i, l); i += 1 {
		var lbi int = i * 21
		var ube int = lbi + 21
		var chunk []byte = raw.val[lbi:ube]
		var low uint8 = chunk[0]
		var full uint16 = (uint16(high) << 8) | uint16(low)
		u6 := binary.BigEndian.Uint64(chunk[1:9])
		var serial int32 = (int32(u6>>32) | (int32(full) << 16)) & 0x7fffffff
		copy(bloom[:], chunk[5:21])
		k.keys = append(k.keys, unpackSampleFlatKey{
			full,
			serial,
			bloom,
		})
	}
	return nil
}

func unpackSampleIterGetNew[I any](
	raw func(iter I, val *unpackSampleRaw) error,
	rawBuf *unpackSampleRaw,
) func(iter I, val *unpackSampleFlatKeys) error {
	return func(iter I, val *unpackSampleFlatKeys) error {
		e := raw(iter, rawBuf)
		if nil != e {
			return e
		}
		return val.FromRaw(rawBuf)
	}
}

func unpackSampleFlatKeyConsumerNew[I, F any](
	iterNext func(iter I) (hasNext bool),
	iterGet func(iter I, val *unpackSampleRaw) error,
	iterErr func(iter I) error,
	rawBuf *unpackSampleRaw,
) func(
	iter I,
	filter *F,
	consumer local.IterConsumerFiltered[unpackSampleFlatKeys, F],
	buf *unpackSampleFlatKeys,
) (stop bool, e error) {
	return local.IterConsumeManyFilteredNew[I, unpackSampleFlatKeys, F](
		iterNext,
		unpackSampleIterGetNew(iterGet, rawBuf),
		iterErr,
	)
}

func unpackSample() {
	const pgxConnStr string = ""
	db, e := sql.Open("pgx", pgxConnStr)
	if nil != e {
		log.Fatalf("Unable to open: %v\n", e)
	}
	defer db.Close()

	e = db.PingContext(context.Background())
	if nil != e {
		log.Fatalf("Unable to connect: %v\n", e)
	}

	var rawBuf unpackSampleRaw = unpackSampleRaw{
		key: make([]byte, 1),
		val: make([]byte, 0, 21*480),
	}

	var allKeys []unpackSampleFlatKey = make([]unpackSampleFlatKey, 0, 256*512)

	var limitReq int = 3
	var overRatio int = 2
	var limit int = limitReq * overRatio

	var keysConsumer local.IterConsumerFiltered[unpackSampleFlatKeys, int64] = func(
		val *unpackSampleFlatKeys,
		_filter *int64,
	) (stop bool, e error) {
		val.SortStable()
		allKeys = append(allKeys, val.keys...)
		var few bool = len(allKeys) < limit
		return !few, nil
	}

	rows, e := db.QueryContext(
		context.Background(),
		`
			WITH integers AS (
				SELECT GENERATE_SERIES(0, 191) AS i
			)
			SELECT
				(191-i)::SMALLINT AS key16,
				(
					TO_CHAR((191-i)::SMALLINT, 'fm0000') || '0123456789abcdefg'
					||
					TO_CHAR((191-i)::SMALLINT, 'fm0000') || 'hijklmnopqrstuvwx'
				)::BYTEA AS packed
			FROM integers
		`,
	)
	if nil != e {
		panic(e)
	}
	defer rows.Close()

	var flatKeys2consumer func(
		iter *sql.Rows,
		_filter *int64,
		consumer local.IterConsumerFiltered[unpackSampleFlatKeys, int64],
		buf *unpackSampleFlatKeys,
	) (stop bool, e error) = unpackSampleFlatKeyConsumerNew[*sql.Rows, int64](
		func(iter *sql.Rows) (hasNext bool) { return iter.Next() },
		func(iter *sql.Rows, val *unpackSampleRaw) error {
			var raw int16 = 0
			e := iter.Scan(
				&raw,
				&val.val,
			)
			val.key[0] = uint8(raw)
			return e
		},
		func(iter *sql.Rows) error { return iter.Err() },
		&rawBuf,
	)

	var keysBuf unpackSampleFlatKeys = unpackSampleFlatKeys{
		keys: make([]unpackSampleFlatKey, 0, 192*480),
	}
	_, e = flatKeys2consumer(
		rows,
		nil,
		keysConsumer,
		&keysBuf,
	)
	if nil != e {
		panic(e)
	}

	dummyRows := make(map[int32]unpackSampleRow)
	for i, key := range allKeys {
		dummyRows[key.serial] = unpackSampleRow{
			flat:       key,
			primaryKey: int64(len(allKeys) - i),
		}
	}

	var rawKeysBuf []int32 = make([]int32, 0, 16384)
	var dummyGetKeys local.GetKeys[uint8, string, int64, int32] = func(
		_ctx context.Context,
		_con uint8,
		_bkt *string,
		_flt *int64,
	) (keys []int32, e error) {
		rawKeysBuf = rawKeysBuf[:0]
		for _, key := range allKeys {
			rawKeysBuf = append(rawKeysBuf, key.serial)
		}
		return rawKeysBuf, nil
	}

	var dummyGetByKey local.GetByKey[
		uint8,
		string,
		int64,
		int32,
		unpackSampleRow,
	] = func(
		_ctx context.Context,
		_con uint8,
		_bkt *string,
		key int32,
		val *unpackSampleRow,
		_flt *int64,
	) (got bool, e error) {
		row, got := dummyRows[key]
		if got {
			*val = row
		}
		return got, nil
	}

	var val2consumer local.Got2Consumer[
		uint8, int32, int64, string, unpackSampleRow,
	] = local.GetByKeysNew(
		dummyGetKeys,
		dummyGetByKey,
	)

	var consumerBuf unpackSampleRow
	var finalRows []unpackSampleRow = make([]unpackSampleRow, 0, 16384)
	e = val2consumer(
		context.Background(),
		0x42,
		nil,
		nil,
		&consumerBuf,
		func(
			val *unpackSampleRow,
			_flt *int64,
		) (stop bool, e error) {
			finalRows = append(finalRows, *val)
			return
		},
	)

	finalRows[0].sort(finalRows)

	for _, k := range finalRows {
		fmt.Printf("final: %v\n", k)
	}
	finalRows = finalRows[:limitReq]

	fmt.Printf("all keys len: %v\n", len(allKeys))

	for _, k := range allKeys {
		fmt.Printf("all: %v\n", k)
	}

	for _, k := range finalRows {
		fmt.Printf("final: %v\n", k)
	}
}
