package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/takanoriyanagitani/go-local-filter/v2"
)

type indirectKey struct{ serial int32 }

type indirectEncoded struct {
	key []byte
	val []byte
}

type indirectDecBuilder struct{ buf strings.Builder }

func (b *indirectDecBuilder) getKeyAsString(e *indirectEncoded) string {
	b.buf.Reset()
	_, _ = b.buf.Write(e.key)
	return b.buf.String()
}

func (b *indirectDecBuilder) getKeyAsInteger(e *indirectEncoded) (key uint32, err error) {
	var keyString string = b.getKeyAsString(e)
	u6, err := strconv.ParseUint(keyString, 10, 32)
	return uint32(u6), err
}

func (b *indirectDecBuilder) Decode(e *indirectEncoded) (d indirectDecoded, err error) {
	d.key, err = b.getKeyAsInteger(e)
	d.val = e.val
	return
}

func (b *indirectDecBuilder) IntoDecoder() local.Decode[*indirectEncoded, indirectDecoded] {
	return b.Decode
}

func indirectNewDecoder() local.Decode[*indirectEncoded, indirectDecoded] {
	var b indirectDecBuilder
	return b.IntoDecoder()
}

type indirectDecoded struct {
	key uint32
	val []byte
}

func (d *indirectDecoded) unpack() (u []indirectUnpacked, e error) {
	e = json.Unmarshal(d.val, &u)
	return
}

var indirectUnnest local.Unnest[indirectDecoded, indirectUnpacked] = func(
	d *indirectDecoded,
) (unnested []indirectUnpacked, e error) {
	return d.unpack()
}

type indirectUnpacked struct {
	Key    uint32 `json:"key"`
	Second uint8  `json:"second"`
	SeqNo  int32  `json:"seqno"`
	Bloom  uint64 `json:"bloom"`
}

type indirectFilter struct {
	key    uint32
	second uint8
}

const indirectGetByKeyQuery string = `
	SELECT
		key::BYTEA,
		val::BYTEA
	FROM kvs_2023_01_08_cafef00ddeadbeafface864299792458
	WHERE rowid=$1
	LIMIT 1
`

const indirectGetSerialLimitedQuery string = `
	SELECT rowid
	FROM kvs_2023_01_08_cafef00ddeadbeafface864299792458
	ORDER BY rowid DESC
	LIMIT 16384
`

func indirectGetSerialLimited(d *sql.DB) (integers []indirectKey, e error) {
	rows, e := d.Query(indirectGetSerialLimitedQuery)
	if nil != e {
		return nil, e
	}
	defer rows.Close()
	for rows.Next() {
		var i int32
		e := rows.Scan(&i)
		if nil != e {
			return nil, e
		}
		integers = append(integers, indirectKey{serial: i})
	}
	return integers, rows.Err()
}

func indirectGetEncodedByKey(
	ctx context.Context,
	con *sql.DB,
	k indirectKey,
	buf *indirectEncoded,
) (got bool, e error) {
	var row *sql.Row = con.QueryRowContext(
		ctx,
		indirectGetByKeyQuery,
		k.serial,
	)
	e = row.Scan(&buf.key, &buf.val)

	switch e {
	case sql.ErrNoRows:
		log.Printf("warning: invalid key: %v\n", k)
		return false, nil
	case nil:
		return true, nil
	default:
		log.Fatalf("Unexpected err: %v\n", e)
		return false, e
	}
}

func indirectGetByKeyNewDecoded(buf *indirectEncoded) func(
	ctx context.Context,
	con *sql.DB,
	key indirectKey,
	dec *indirectDecoded,
) (got bool, e error) {
	return local.GetByKeyNewDecoded(
		indirectGetEncodedByKey,
		indirectNewDecoder(),
		buf,
	)
}

func indirectFilterPacked(d *indirectDecoded, f *indirectFilter) (keep bool)    { return true }
func indirectFilterUnpacked(d *indirectUnpacked, f *indirectFilter) (keep bool) { return true }

func indirectSample() {
	const pgxConnStr string = ""
	db, e := sql.Open("pgx", pgxConnStr)
	if nil != e {
		log.Fatalf("Unable to open: %v\n", e)
	}
	defer db.Close()

	var unpackedItems []indirectUnpacked

	var consumeUnpacked local.IterConsumer[indirectUnpacked] = func(
		unpacked *indirectUnpacked,
	) (stop bool, e error) {
		unpackedItems = append(unpackedItems, *unpacked)
		return
	}

	var bufEncoded indirectEncoded

	var f func(
		ctx context.Context,
		keys []indirectKey,
		con *sql.DB,
		buf *indirectDecoded,
		f *indirectFilter,
	) error = local.GetByKeysNewUnnested(
		indirectGetByKeyNewDecoded(&bufEncoded),
		indirectUnnest,
		indirectFilterPacked,
		indirectFilterUnpacked,
		consumeUnpacked,
	)

	filter := indirectFilter{}

	keys, e := indirectGetSerialLimited(db)
	if nil != e {
		log.Fatalf("Unable to get keys: %v\n", e)
	}
	log.Printf("key len: %v\n", len(keys))

	var decodedBuf indirectDecoded
	var scanStart time.Time = time.Now()
	e = f(
		context.Background(),
		keys,
		db,
		&decodedBuf,
		&filter,
	)
	var scanDuration time.Duration = time.Since(scanStart)
	log.Printf("duration ms: %v\n", scanDuration.Milliseconds())

	if nil != e {
		log.Fatalf("Unable to get unpacked rows: %v\n", e)
	}

	log.Printf("unpacked len: %v\n", len(unpackedItems))
}
