package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/takanoriyanagitani/go-local-filter/v2"
)

type testBucketFilterB struct{ bucketName string }

func testBucketFilterBucketNew() *testBucketFilterB { return &testBucketFilterB{bucketName: ""} }

type testBucketFilterF struct{ filterBloom uint64 }

type testBucketFilterK struct{ key int32 }

type testBucketFilterBucketSet struct{ set map[string]interface{} }

func testBucketFilterBucketSetNew() *testBucketFilterBucketSet {
	return &testBucketFilterBucketSet{set: make(map[string]interface{})}
}

func (s *testBucketFilterBucketSet) NewIterConsumer() local.IterConsumerFiltered[
	testBucketFilterB, testBucketFilterF,
] {
	return func(val *testBucketFilterB, _filter *testBucketFilterF) (stop bool, e error) {
		s.set[val.bucketName] = nil
		return false, nil
	}
}

var testBucketFilterRows2BucketConsumer func(
	rows *sql.Rows,
	filter *testBucketFilterF,
	consumer local.IterConsumerFiltered[testBucketFilterB, testBucketFilterF],
	buf *testBucketFilterB,
) (stop bool, e error) = local.IterConsumeManyFilteredNew[
	*sql.Rows, testBucketFilterB, testBucketFilterF,
](
	func(iter *sql.Rows) (hasNext bool) { return iter.Next() },
	func(iter *sql.Rows, val *testBucketFilterB) error { return iter.Scan(&val.bucketName) },
	func(iter *sql.Rows) error { return iter.Err() },
)

func testBucketFilterGetKeysNew(
	getKeys local.GetKeys[*sql.DB, testBucketFilterB, testBucketFilterF, testBucketFilterK],
	checkBucket func(
		ctx context.Context,
		con *sql.DB,
		bucket *testBucketFilterB,
		filter *testBucketFilterF,
	) (checkMe bool),
) local.GetKeys[*sql.DB, testBucketFilterB, testBucketFilterF, testBucketFilterK] {
	return getKeys.WithBucketFilter(checkBucket)
}

const testBucketFilterGetBucketsQuery string = `
	SELECT table_name::TEXT
	FROM information_schema.tables
	WHERE table_schema='public'
	ORDER BY table_name
`

func testBucketFilterBucketCheckerNewFromMap(m map[string]interface{}) func(
	context.Context,
	*sql.DB,
	*testBucketFilterB,
	*testBucketFilterF,
) (checkMe bool) {
	return func(
		_ctx context.Context,
		_ *sql.DB,
		bucket *testBucketFilterB,
		filter *testBucketFilterF,
	) (checkMe bool) {
		_, bucketFound := m[bucket.bucketName]
		return bucketFound
	}
}

func testBucketFilterBuckets2map(
	ctx context.Context,
	d *sql.DB,
) (bucketSet map[string]interface{}, e error) {
	var set *testBucketFilterBucketSet = testBucketFilterBucketSetNew()
	var consumer local.IterConsumerFiltered[
		testBucketFilterB,
		testBucketFilterF,
	] = set.NewIterConsumer()
	rows, e := d.QueryContext(ctx, testBucketFilterGetBucketsQuery)
	if nil != e {
		return bucketSet, e
	}
	defer rows.Close()
	_, e = testBucketFilterRows2BucketConsumer(
		rows,
		&testBucketFilterF{},
		consumer,
		testBucketFilterBucketNew(),
	)
	if nil != e {
		return bucketSet, e
	}
	return set.set, nil
}

func bucketFilterSample() {
	const pgxConnStr string = ""
	db, e := sql.Open("pgx", pgxConnStr)
	if nil != e {
		log.Fatalf("Unable to open: %v\n", e)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	e = db.PingContext(ctx)
	if nil != e {
		log.Fatalf("Unable to connect: %v\n", e)
	}

	set, e := testBucketFilterBuckets2map(context.Background(), db)
	if nil != e {
		log.Fatalf("Unable to get buckets info: %v\n", e)
	}
	log.Printf("set size: %v\n", len(set))

	var bucketChecker func(
		context.Context,
		*sql.DB,
		*testBucketFilterB,
		*testBucketFilterF,
	) (checkMe bool) = testBucketFilterBucketCheckerNewFromMap(set)

	var dummyQueryCount uint64 = 0
	var getKeys local.GetKeys[
		*sql.DB,
		testBucketFilterB,
		testBucketFilterF,
		testBucketFilterK,
	] = func(
		_ctx context.Context,
		_db *sql.DB,
		bucket *testBucketFilterB,
		filter *testBucketFilterF,
	) (keys []testBucketFilterK, e error) {
		dummyQueryCount += 1
		return
	}

	var getKeysAfterCheck local.GetKeys[
		*sql.DB,
		testBucketFilterB,
		testBucketFilterF,
		testBucketFilterK,
	] = getKeys.WithBucketFilter(bucketChecker)

	keys, e := getKeysAfterCheck(
		context.Background(),
		db,
		&testBucketFilterB{bucketName: "not_exist"},
		&testBucketFilterF{},
	)
	if nil != e {
		log.Fatalf("Unable to get keys: %v\n", e)
	}

	log.Printf("key len: %v\n", len(keys))
	log.Printf("dummy query count: %v\n", dummyQueryCount)

	keys, e = getKeysAfterCheck(
		context.Background(),
		db,
		&testBucketFilterB{bucketName: "kvs_2023_01_08_cafef00ddeadbeafface864299792458"},
		&testBucketFilterF{},
	)
	if nil != e {
		log.Fatalf("Unable to get keys: %v\n", e)
	}

	log.Printf("key len: %v\n", len(keys))
	log.Printf("dummy query count: %v\n", dummyQueryCount)
}
