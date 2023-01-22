package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
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
	e = json.Unmarshal(raw.val, b.buf1)
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

var got2consumerEncoded local.Got2Consumer[
	*sql.DB,
	testPlanKey,
	testPlanFilter,
	testPlanBucket,
	testPlanEncoded,
] = local.GetByKeysNew(testPlanGetKeys, testPlanGetByKeyEncoded)

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

	var raws []testPlanEncoded
	consumer := func(encoded *testPlanEncoded, f *testPlanFilter) (stop bool, e error) {
		raws = append(raws, *encoded)
		return
	}
	var buf testPlanEncoded
	e = got2consumerEncoded(
		context.Background(),
		db,
		&bucket,
		nil,
		&buf,
		consumer,
	)
	if nil != e {
		log.Fatal(e)
	}

	log.Printf("raw len: %v\n", len(raws))
}
