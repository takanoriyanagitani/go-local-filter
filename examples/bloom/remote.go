package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"text/template"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/takanoriyanagitani/go-local-filter/v2"
)

type testFilterRemote struct {
	timeHmLbi string
	timeHmUbi string
	bloom     uint64
}

func (f testFilterRemote) LowerBoundAsBytes() []byte { return []byte(f.timeHmLbi) }
func (f testFilterRemote) UpperBoundAsBytes() []byte { return []byte(f.timeHmUbi) }

func (f testFilterRemote) CheckTimeHm(timeHm string) bool {
	return (f.timeHmLbi <= timeHm) && (timeHm <= f.timeHmUbi)
}

func (f testFilterRemote) CheckBloom(other uint64) bool { return (f.bloom & other) == f.bloom }

var pgxQueryTmplRemote *template.Template = template.Must(template.New("pgx").Parse(`
    {{define "GetByKey"}}
        SELECT
            key::BYTEA,
            val::BYTEA
        FROM {{.}}
        WHERE key = $1::BYTEA
        LIMIT 1
    {{end}}

    {{define "GetByRange"}}
        SELECT
            key::BYTEA,
            val::BYTEA
        FROM {{.}}
        WHERE
            key BETWEEN $1::BYTEA AND $2::BYTEA
    {{end}}
`))

func pgxGetByRangeNew(
	d *sql.DB,
) func(context.Context, local.Bucket, testFilterRemote) (rows []sampleRow, e error) {
	return func(
		ctx context.Context,
		b local.Bucket,
		f testFilterRemote,
	) (rows []sampleRow, e error) {
		var buf strings.Builder

		e = pgxQueryTmplRemote.ExecuteTemplate(&buf, "GetByRange", b.AsString())
		if nil != e {
			return nil, e
		}

		var lowerBound []byte = f.LowerBoundAsBytes()
		var upperBound []byte = f.UpperBoundAsBytes()

		raws, e := d.QueryContext(ctx, buf.String(), lowerBound, upperBound)
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

func getByRangePackedNew(
	remote func(context.Context, local.Bucket, testFilterRemote) (rows []sampleRow, e error),
) func(context.Context, local.Bucket, testFilterRemote) (packed []samplePackedRow, e error) {
	var decode local.Decode[sampleRow, samplePackedRow] = func(
		encoded sampleRow,
	) (decoded samplePackedRow, e error) {
		return encoded.toPacked()
	}
	return local.RemoteFilterNewDecoded(decode, remote)
}

func getByRangeUnpackedNew(
	remote func(context.Context, local.Bucket, testFilterRemote) (packed []samplePackedRow, e error),
) func(context.Context, local.Bucket, testFilterRemote) (unpacked []sampleUnpackedRow, e error) {
	var unpack local.Unpack[samplePackedRow, sampleUnpackedRow] = func(
		packed samplePackedRow,
	) (unpacked []sampleUnpackedRow, e error) {
		return packed.unpack()
	}
	return local.RemoteFilterNewUnpacked(unpack, remote)
}

func remoteSample() {
	const pgxConnStr string = ""
	db, e := sql.Open("pgx", pgxConnStr)
	if nil != e {
		log.Fatal(e)
	}
	defer db.Close()

	var getByRange func(
		context.Context,
		local.Bucket,
		testFilterRemote,
	) ([]sampleRow, error) = pgxGetByRangeNew(db)

	var getByRangePacked func(
		context.Context,
		local.Bucket,
		testFilterRemote,
	) ([]samplePackedRow, error) = getByRangePackedNew(getByRange)

	var getByRangeUnpacked func(
		context.Context,
		local.Bucket,
		testFilterRemote,
	) ([]sampleUnpackedRow, error) = getByRangeUnpackedNew(getByRangePacked)

	var filterRemote func(
		context.Context,
		local.Bucket,
		testFilterRemote,
	) ([]sampleUnpackedRow, error) = local.FilterRemoteNew(
		nil,
		getByRangeUnpacked,
		nil,
		func(_ testFilterRemote) (useRemoteFilter bool) { return true },
	)

	var filter testFilterRemote = testFilterRemote{
		timeHmLbi: "07:23",
		timeHmUbi: "24:00",
		bloom:     0x0000000000000005,
	}

	unpacked, e := filterRemote(
		context.Background(),
		local.BucketNew("items_2023_01_08_cafef00ddeadbeafface864299792458"),
		filter,
	)

	if nil != e {
		log.Fatal(e)
	}

	log.Printf("unpacked len: %v\n", len(unpacked))
}
