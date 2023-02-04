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

type testFilterLocal struct {
	timeHmLbi string
	timeHmUbi string
	bloom     uint64
}

func (f testFilterLocal) CheckTimeHm(timeHm string) bool {
	return (f.timeHmLbi <= timeHm) && (timeHm <= f.timeHmUbi)
}

func (f testFilterLocal) CheckBloom(other uint64) bool { return (f.bloom & other) == f.bloom }

var pgxQueryTmpl *template.Template = template.Must(template.New("pgx").Parse(`
    {{define "GetAll"}}
        SELECT
            key::BYTEA,
            val::BYTEA
        FROM {{.}}
    {{end}}
`))

type sampleRow struct {
	key []byte
	val []byte
}

func (s sampleRow) toPacked() (p samplePackedRow, e error) {
	p.key = string(s.key)
	p.val = s.val
	return
}

func sampleRowFromRaw(raw *sql.Rows) (row sampleRow, e error) {
	var key []byte
	var val []byte
	e = raw.Scan(&key, &val)
	if nil != e {
		return
	}

	row.key = key
	row.val = val
	return
}

type samplePackedRow struct {
	key string
	val []byte
}

func (p samplePackedRow) toValue() (v []sampleParsedVal, e error) {
	e = json.Unmarshal(p.val, &v)
	return
}

func (p samplePackedRow) unpack() (u []sampleUnpackedRow, e error) {
	v, e := p.toValue()
	if nil != e {
		return nil, e
	}
	for _, val := range v {
		var second uint8 = val.Second
		var unpacked sampleUnpackedRow
		unpacked.timeHms = p.key + "," + strconv.FormatInt(int64(second), 10)
		unpacked.value = val
		u = append(u, unpacked)
	}
	return
}

type sampleParsedVal struct {
	Second uint8  `json:"second"`
	SeqNo  int32  `json:"seqno"`
	Bloom  uint64 `json:"bloom"`
}

type sampleUnpackedRow struct {
	timeHms string
	value   sampleParsedVal
}

func pgxGetAllNew(d *sql.DB) func(context.Context, local.Bucket) (rows []sampleRow, e error) {
	return func(ctx context.Context, b local.Bucket) (rows []sampleRow, e error) {
		var buf strings.Builder
		e = pgxQueryTmpl.ExecuteTemplate(
			&buf,
			"GetAll",
			b.AsString(),
		)
		if nil != e {
			return nil, e
		}
		raws, e := d.QueryContext(ctx, buf.String())
		if nil != e {
			return nil, e
		}
		defer raws.Close()

		for raws.Next() {
			row, e := sampleRowFromRaw(raws)
			if nil != e {
				return nil, e
			}
			rows = append(rows, row)
		}
		e = raws.Err()
		return
	}
}

func getAllPackedNew(
	getAll func(context.Context, local.Bucket) (rows []sampleRow, e error),
) func(context.Context, local.Bucket) (packed []samplePackedRow, e error) {
	var decode local.Decode[sampleRow, samplePackedRow] = func(
		encoded sampleRow,
	) (decoded samplePackedRow, e error) {
		return encoded.toPacked()
	}
	return decode.NewAll(getAll)
}

func getAllUnpackedNew(
	getAllPacked func(context.Context, local.Bucket) (packed []samplePackedRow, e error),
) func(context.Context, local.Bucket) (unpacked []sampleUnpackedRow, e error) {
	var unpack local.Unpack[samplePackedRow, sampleUnpackedRow] = func(
		packed samplePackedRow,
	) (unpacked []sampleUnpackedRow, e error) {
		return packed.unpack()
	}
	return unpack.NewAll(getAllPacked)
}

func localSample() {
	const pgxConnStr string = ""
	db, e := sql.Open("pgx", pgxConnStr)
	if nil != e {
		log.Fatalf("%v\n", e)
	}
	defer db.Close()

	var getAll func(context.Context, local.Bucket) ([]sampleRow, error) = pgxGetAllNew(db)
	var getAllPacked func(context.Context, local.Bucket) (
		[]samplePackedRow,
		error,
	) = getAllPackedNew(getAll)
	var getAllUnpacked func(context.Context, local.Bucket) (
		[]sampleUnpackedRow,
		error,
	) = getAllUnpackedNew(getAllPacked)

	var filterByTime local.LocalFilter[sampleUnpackedRow, testFilterLocal] = func(
		unpacked sampleUnpackedRow,
		f testFilterLocal,
	) (keep bool) {
		var timeHms string = unpacked.timeHms
		var splited []string = strings.SplitN(timeHms, ",", 2)
		return f.CheckTimeHm(splited[0])
	}

	var filterByBloom local.LocalFilter[sampleUnpackedRow, testFilterLocal] = func(
		unpacked sampleUnpackedRow,
		f testFilterLocal,
	) (keep bool) {
		var bloom uint64 = unpacked.value.Bloom
		return f.CheckBloom(bloom)
	}

	var filterLocal local.LocalFilter[sampleUnpackedRow, testFilterLocal] = filterByTime.
		And(filterByBloom)

	var filterRemote func(context.Context, local.Bucket, testFilterLocal) (
		[]sampleUnpackedRow,
		error,
	) = local.FilterRemoteNew(
		getAllUnpacked,
		nil,
		local.LocalFilterNew(filterLocal),
		func(_ testFilterLocal) (useRemoteFilter bool) { return false },
	)

	var filter testFilterLocal = testFilterLocal{
		timeHmLbi: "08:23",
		timeHmUbi: "24:00",
		bloom:     0x0000000000000005,
	}

	unpacked, e := filterRemote(
		context.Background(),
		local.BucketNew("items_2023_01_08_cafef00ddeadbeafface864299792458"),
		filter,
	)
	if nil != e {
		log.Fatalf("%v\n", e)
	}

	log.Printf("unpacked len: %v\n", len(unpacked))
}
