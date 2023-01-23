package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"text/template"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/takanoriyanagitani/go-local-filter/v2"
)

type testPlanBucket struct{ name string }

type testPlanFilter struct {
	key   int32
	bloom int64
}

func (f testPlanFilter) WithBloom(bloom int64) testPlanFilter {
	var key int32 = f.key
	return testPlanFilter{
		key,
		bloom,
	}
}

func (f testPlanFilter) WithBloomString(bloom string) testPlanFilter {
	i, e := strconv.ParseInt(bloom, 10, 64)
	if nil == e {
		return f.WithBloom(i)
	}
	return f
}

type testPlanKeyRaw struct{ key []byte }
type testPlanKey struct{ key int32 }

func (raw *testPlanKeyRaw) toKey(buf *strings.Builder) (key testPlanKey, e error) {
	buf.Reset()
	_, _ = buf.Write(raw.key)
	i, e := strconv.ParseInt(buf.String(), 10, 32)
	key.key = int32(i)
	return key, e
}

var testPlanTmpl *template.Template = template.Must(template.New("plan").Parse(`
	{{define "GetKeysAllLimited"}}
		SELECT rowid::INTEGER
		FROM {{ .bucketName }}
		ORDER BY key
		LIMIT 16383
	{{end}}

	{{define "GetByKey"}}
		SELECT
			key::BYTEA,
			val::BYTEA
		FROM {{ .bucketName }}
		WHERE rowid=$1
		LIMIT 1
	{{end}}

	{{define "GetAllDirectLimited"}}
		SELECT
			key::BYTEA,
			val::BYTEA
		FROM {{ .bucketName }}
		LIMIT 16383
	{{end}}
`))

var testPlanGetKeys local.GetKeys[*sql.DB, testPlanBucket, testPlanFilter, testPlanKey] = func(
	ctx context.Context,
	con *sql.DB,
	b *testPlanBucket,
	f *testPlanFilter,
) (keys []testPlanKey, e error) {
	var queryBuf strings.Builder
	e = testPlanTmpl.ExecuteTemplate(&queryBuf, "GetKeysAllLimited", map[string]interface{}{
		"bucketName": b.name,
	})
	if nil != e {
		return nil, e
	}

	rows, e := con.QueryContext(ctx, queryBuf.String())
	if nil != e {
		return nil, e
	}
	defer rows.Close()

	var consumeKeys func(
		ctx context.Context,
		iter *sql.Rows,
		buf *testPlanKey,
		f *testPlanFilter,
	) error = local.Iter2ConsumerNewFiltered(
		func(iter *sql.Rows) (hasNext bool) { return rows.Next() },
		func(iter *sql.Rows, buf *testPlanKey) error { return iter.Scan(&buf.key) },
		func(iter *sql.Rows) error { return iter.Err() },
		func(_ *testPlanFilter, _ *testPlanKey) (keep bool) { return true },
		func(k *testPlanKey) (stop bool, e error) {
			keys = append(keys, *k)
			return false, nil
		},
	)

	var keyBuf testPlanKey
	e = consumeKeys(ctx, rows, &keyBuf, f)
	if nil != e {
		return nil, e
	}
	return keys, nil
}

type testPlanEncoded struct {
	key []byte
	val []byte
}

type testPlanDecodedBuilder struct {
	buf1 []testPlanItem
	buf2 strings.Builder
}

func (b *testPlanDecodedBuilder) fromEncoded(raw *testPlanEncoded) (d testPlanDecoded, e error) {
	b.buf2.Reset()
	_, _ = b.buf2.Write(raw.key)
	u, e := strconv.ParseUint(b.buf2.String(), 10, 32)
	if nil != e {
		return d, e
	}
	d.key = uint32(u)
	e = json.Unmarshal(raw.val, &b.buf1)
	d.val = b.buf1
	return d, e
}

type testPlanDecoded struct {
	key uint32
	val []testPlanItem
}

type testPlanItem struct {
	Second uint8  `json:"second"`
	SeqNo  int32  `json:"seqno"`
	Bloom  uint64 `json:"bloom"`
}

type testPlanFlatBuilder struct{ buf []testPlanFlat }

func (b *testPlanFlatBuilder) fromDecoded(d *testPlanDecoded) []testPlanFlat {
	b.buf = b.buf[:0]
	for _, item := range d.val {
		var flat testPlanFlat = testPlanFlat{
			key:    d.key,
			second: item.Second,
			seqno:  item.SeqNo,
			bloom:  item.Bloom,
		}
		b.buf = append(b.buf, flat)
	}
	return b.buf
}

func testPlanConsumerUnpackedNew(
	unpackedConsumer local.IterConsumerFiltered[testPlanFlat, testPlanFilter],
	filterPacked func(p *testPlanDecoded, f *testPlanFilter) (keep bool),
	builder *testPlanFlatBuilder,
) local.IterConsumerFiltered[testPlanDecoded, testPlanFilter] {
	return local.ConsumerUnpackedNew(
		unpackedConsumer,
		func(packed *testPlanDecoded) (unpacked []testPlanFlat, e error) {
			return builder.fromDecoded(packed), nil
		},
		filterPacked,
	)
}

type testPlanFlat struct {
	key    uint32
	second uint8
	seqno  int32
	bloom  uint64
}

var testPlanGetByKeyEncoded local.GetByKey[
	*sql.DB,
	testPlanBucket,
	testPlanFilter,
	testPlanKey,
	testPlanEncoded,
] = func(
	ctx context.Context,
	con *sql.DB,
	b *testPlanBucket,
	k testPlanKey,
	v *testPlanEncoded,
	f *testPlanFilter,
) (got bool, e error) {
	var queryBuf strings.Builder
	e = testPlanTmpl.ExecuteTemplate(&queryBuf, "GetByKey", map[string]interface{}{
		"bucketName": b.name,
	})
	if nil != e {
		return false, e
	}

	var row *sql.Row = con.QueryRowContext(
		ctx,
		queryBuf.String(),
		&k.key,
	)

	e = row.Scan(&v.key, &v.val)
	switch e {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, e
	}
}

func testPlanGetByKeyDecodedNew(
	buf *testPlanEncoded,
	filterDecoded func(d *testPlanDecoded, f *testPlanFilter) (keep bool),
	b *testPlanDecodedBuilder,
) local.GetByKey[
	*sql.DB,
	testPlanBucket,
	testPlanFilter,
	testPlanKey,
	testPlanDecoded,
] {
	return local.GetByKeyDecodedNew(
		testPlanGetByKeyEncoded,
		func(e *testPlanEncoded) (d testPlanDecoded, err error) { return b.fromEncoded(e) },
		buf,
		filterDecoded,
	)
}

func testPlanDecoded2consumerNew(
	buf *testPlanEncoded,
	filterDecoded func(d *testPlanDecoded, f *testPlanFilter) (keep bool),
	b *testPlanDecodedBuilder,
) local.Got2Consumer[
	*sql.DB,
	testPlanKey,
	testPlanFilter,
	testPlanBucket,
	testPlanDecoded,
] {
	return local.GetByKeysNew(
		testPlanGetKeys,
		testPlanGetByKeyDecodedNew(
			buf,
			filterDecoded,
			b,
		),
	)
}

var testPlanGetDirect local.Got2Consumer[
	*sql.DB,
	testPlanKey,
	testPlanFilter,
	testPlanBucket,
	testPlanDecoded,
] = func(
	ctx context.Context,
	con *sql.DB,
	bucket *testPlanBucket,
	f *testPlanFilter,
	buf *testPlanDecoded,
	consumer func(val *testPlanDecoded, f *testPlanFilter) (stop bool, e error),
) error {
	log.Printf("direct scan\n")
	var queryBuf strings.Builder
	e := testPlanTmpl.ExecuteTemplate(&queryBuf, "GetAllDirectLimited", map[string]interface{}{
		"bucketName": bucket.name,
	})
	if nil != e {
		return e
	}

	rows, e := con.QueryContext(ctx, queryBuf.String())
	if nil != e {
		return e
	}
	defer rows.Close()

	var builder testPlanDecodedBuilder
	var consumerEncoded local.IterConsumerFiltered[
		testPlanEncoded,
		testPlanFilter,
	] = local.ConsumerDecodedNew(
		consumer,
		func(encoded *testPlanEncoded) (decoded testPlanDecoded, e error) {
			return builder.fromEncoded(encoded)
		},
		func(encoded *testPlanEncoded, f *testPlanFilter) (keep bool) { return true },
	)
	var consumeMany func(
		iter *sql.Rows,
		f *testPlanFilter,
		icf local.IterConsumerFiltered[testPlanEncoded, testPlanFilter],
		buf *testPlanEncoded,
	) (stop bool, e error) = local.IterConsumeManyFilteredNew[
		*sql.Rows,
		testPlanEncoded,
		testPlanFilter,
	](
		func(iter *sql.Rows) (hasNext bool) { return iter.Next() },
		func(iter *sql.Rows, val *testPlanEncoded) error { return iter.Scan(&val.key, &val.val) },
		func(iter *sql.Rows) error { return iter.Err() },
	)
	var encodedBuf testPlanEncoded
	_, e = consumeMany(
		rows,
		f,
		consumerEncoded,
		&encodedBuf,
	)
	return e
}

func testPlanDoDirectScan(f *testPlanFilter) (directScan bool) { return -1 == f.bloom }

func planSample() {
	const pgxConnStr string = ""
	db, e := sql.Open("pgx", pgxConnStr)
	if nil != e {
		log.Fatalf("Unable to open: %v\n", e)
	}
	defer db.Close()

	var bucket testPlanBucket = testPlanBucket{
		name: "kvs_2023_01_08_cafef00ddeadbeafface864299792458",
	}

	var flatItems []testPlanFlat
	var unpackedConsumer local.IterConsumerFiltered[testPlanFlat, testPlanFilter] = func(
		val *testPlanFlat,
		f *testPlanFilter,
	) (stop bool, e error) {
		flatItems = append(flatItems, *val)
		return
	}
	var consumer local.IterConsumerFiltered[
		testPlanDecoded,
		testPlanFilter,
	] = testPlanConsumerUnpackedNew(
		unpackedConsumer,
		func(p *testPlanDecoded, f *testPlanFilter) (keep bool) { return true },
		&testPlanFlatBuilder{},
	)

	var buf testPlanEncoded
	var bufDecoded testPlanDecoded
	var got2consumerDecoded local.Got2Consumer[
		*sql.DB,
		testPlanKey,
		testPlanFilter,
		testPlanBucket,
		testPlanDecoded,
	] = testPlanDecoded2consumerNew(
		&buf,
		func(d *testPlanDecoded, f *testPlanFilter) (keep bool) { return true },
		&testPlanDecodedBuilder{},
	)
	var getByPlan func(
		ctx context.Context,
		con *sql.DB,
		b *testPlanBucket,
		f *testPlanFilter,
		buf *testPlanDecoded,
		consumer func(val *testPlanDecoded, f *testPlanFilter) (stop bool, e error),
	) error = local.GetWithPlanNew(
		got2consumerDecoded,
		testPlanGetDirect,
		testPlanDoDirectScan,
	)
	var filter testPlanFilter = testPlanFilter{}.
		WithBloomString(os.Getenv("ENV_BLOOM"))
	e = getByPlan(
		context.Background(),
		db,
		&bucket,
		&filter,
		&bufDecoded,
		consumer,
	)
	if nil != e {
		log.Fatal(e)
	}

	log.Printf("raw len: %v\n", len(flatItems))
}
