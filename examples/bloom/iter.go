package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/takanoriyanagitani/go-local-filter/v2"
)

type sampleIterPackedRow struct {
	key []byte
	val []byte
}

type sampleIterUnpackedRowBuilder struct{ buf []sampleIterDecodedRow }

func (b *sampleIterUnpackedRowBuilder) parsePacked(
	p *sampleIterPackedRow,
) (unpacked []sampleIterDecodedRow, e error) {
	e = json.Unmarshal(p.val, &b.buf)
	if nil != e {
		e = fmt.Errorf("Invalid json bytes: %v", e)
		log.Printf("raw bytes: %s\n", p.val)
	}
	return b.buf, e
}

func (b *sampleIterUnpackedRowBuilder) toUnpacked(
	p *sampleIterPackedRow,
	buf []sampleIterUnpackedRow,
) (unpacked []sampleIterUnpackedRow, e error) {
	decoded, e := b.parsePacked(p)
	if nil != e {
		return nil, e
	}
	buf = buf[:0]
	for _, decodedItem := range decoded {
		buf = append(buf, decodedItem.unpack(p.key))
	}
	return buf, nil
}

type sampleIterUnpackedRows []sampleIterUnpackedRow

func (rows sampleIterUnpackedRows) toWriter(w io.Writer) error {
	for _, row := range rows {
		e := row.toWriter(w)
		if nil != e {
			return e
		}
	}
	return nil
}

func (b *sampleIterUnpackedRowBuilder) NewUnpacker(
	buf []sampleIterUnpackedRow,
) func(packed *sampleIterPackedRow) (unpacked sampleIterUnpackedRows, e error) {
	return func(p *sampleIterPackedRow) (sampleIterUnpackedRows, error) {
		return b.toUnpacked(p, buf)
	}
}

func iterWriterNew(w io.Writer) local.IterConsumer[sampleIterUnpackedRows] {
	return func(val *sampleIterUnpackedRows) (stop bool, e error) { return false, val.toWriter(w) }
}

type SimpleIter[I, T any] struct {
	next func(iter I) bool
	get  func(iter I, buf *T) error
	err  func(iter I) error
}

func SimpleIterNewMust[I, T any](
	next func(iter I) bool,
	get func(iter I, buf *T) error,
	err func(iter I) error,
) SimpleIter[I, T] {
	if nil == next || nil == get || nil == err {
		panic("Invalid argument")
	}
	return SimpleIter[I, T]{
		next,
		get,
		err,
	}
}

func Iter2wtrNew[I, F any](
	iter SimpleIter[I, sampleIterPackedRow],
	builder *sampleIterUnpackedRowBuilder,
	buf []sampleIterUnpackedRow,
	filterCoarse func(packed *sampleIterPackedRow, filter *F) (keep bool),
	filterFine func(unpacked *sampleIterUnpackedRows, filter *F) (keep bool),
	w io.Writer,
) func(
	ctx context.Context,
	iter I,
	buf *sampleIterPackedRow,
	filter *F,
) error {
	return local.Iter2ConsumerNewUnpacked(
		iter.next,
		iter.get,
		iter.err,
		builder.NewUnpacker(buf),
		filterCoarse,
		filterFine,
		iterWriterNew(w),
	)
}

type sampleIterDecodedRow struct {
	Bloom  uint64 `json:"bloom"`
	SeqNo  int32  `json:"seqno"`
	Second uint8  `json:"second"`
}

func (d sampleIterDecodedRow) toWriter(w io.Writer) error {
	var enc *json.Encoder = json.NewEncoder(w)
	return enc.Encode(&d)
}

func (d sampleIterDecodedRow) unpack(key []byte) (unpacked sampleIterUnpackedRow) {
	copy(unpacked.hm[:], key)
	unpacked.decoded = d
	return
}

type sampleIterUnpackedRow struct {
	hm      [5]byte
	decoded sampleIterDecodedRow
}

func (u *sampleIterUnpackedRow) writeTime(w io.Writer) error {
	var hmLen int = int(len(u.hm))
	_, e := fmt.Fprintf(w, "key len: %v\n", hmLen)
	return e
}

func (u *sampleIterUnpackedRow) writeDecoded(w io.Writer) error { return u.decoded.toWriter(w) }

func (u *sampleIterUnpackedRow) toWriter(w io.Writer) error {
	return error1st([]func() error{
		func() error { return u.writeTime(w) },
		func() error { return u.writeDecoded(w) },
	})
}

type sampleIterFilter struct {
	coarseTime [5]uint8
	bloom      uint64
}

func iterSample() {
	const pgxConnStr string = ""
	db, e := sql.Open("pgx", pgxConnStr)
	if nil != e {
		log.Fatal(e)
	}
	defer db.Close()

	var buf strings.Builder

	e = pgxQueryTmpl.ExecuteTemplate(
		&buf,
		"GetAll",
		"items_2023_01_08_cafef00ddeadbeafface864299792458",
	)
	if nil != e {
		log.Fatal(e)
	}

	rows, e := db.QueryContext(
		context.Background(),
		buf.String(),
	)
	if nil != e {
		log.Fatal(e)
	}
	defer rows.Close()

	var dbIter SimpleIter[*sql.Rows, sampleIterPackedRow] = SimpleIterNewMust(
		func(iter *sql.Rows) (hasNext bool) { return iter.Next() },
		func(iter *sql.Rows, buf *sampleIterPackedRow) error {
			return iter.Scan(&buf.key, &buf.val)
		},
		func(iter *sql.Rows) error { return iter.Err() },
	)

	var iter2wtr func(
		ctx context.Context,
		iter *sql.Rows,
		buf *sampleIterPackedRow,
		filter *sampleIterFilter,
	) error = Iter2wtrNew(
		dbIter,
		&sampleIterUnpackedRowBuilder{
			buf: make([]sampleIterDecodedRow, 0, 512),
		},
		make([]sampleIterUnpackedRow, 0, 512),
		func(packed *sampleIterPackedRow, f *sampleIterFilter) (keep bool) {
			return 0 < bytes.Compare(packed.key, f.coarseTime[:])
		},
		func(unpacked *sampleIterUnpackedRows, f *sampleIterFilter) (keep bool) {
			return 2 < len(*unpacked)
		},
		os.Stdout,
	)

	e = iter2wtr(
		context.Background(),
		rows,
		&sampleIterPackedRow{
			key: nil,
			val: nil,
		},
		&sampleIterFilter{
			coarseTime: [5]uint8{ '2', '3', ':', '5', '7' },
			bloom: 0x3776,
		},
	)

	if nil != e {
		log.Fatal(e)
	}
}
