#!/bin/sh

export PGUSER=postgres

export PGDATABASE=go_local_filter_example

PGDATABASE=postgres \
  psql \
    --no-align \
    --tuples-only \
    --command="
      SELECT datname
      FROM pg_database
      WHERE datname = 'go_local_filter_example'
    " \
  | grep --silent '^go_local_filter_example$' \
  || exec printf 'example database %s not found.\n' "${PGDATABASE}"

psql \
  --command="
    CREATE TABLE IF NOT EXISTS items_2023_01_08_cafef00ddeadbeafface864299792458(
        key BYTEA NOT NULL,
        val BYTEA NOT NULL,
        CONSTRAINT items_2023_01_08_cafef00ddeadbeafface864299792458_pkc
        PRIMARY KEY(key)
    )
  " \
  || exec printf 'Unable to create a test table.\n'

psql \
  --command="
    INSERT INTO items_2023_01_08_cafef00ddeadbeafface864299792458 AS tgt
    VALUES
        ('07:22'::BYTEA, JSON_BUILD_ARRAY(
            JSON_BUILD_OBJECT('second',  0, 'seqno', 1, 'bloom', 299792458),
            JSON_BUILD_OBJECT('second', 42, 'seqno', 2, 'bloom', 3776),
            JSON_BUILD_OBJECT('second', 43, 'seqno', 3, 'bloom', 599),
            JSON_BUILD_OBJECT('second', 43, 'seqno', 4, 'bloom', 634),
            JSON_BUILD_OBJECT('second', 59, 'seqno', 5, 'bloom', 333)
        )::TEXT::BYTEA),
        ('23:58'::BYTEA, JSON_BUILD_ARRAY(
            JSON_BUILD_OBJECT('second',  0, 'seqno',  6, 'bloom', 299792458),
            JSON_BUILD_OBJECT('second', 59, 'seqno', 10, 'bloom', 333)
        )::TEXT::BYTEA),
        ('23:59'::BYTEA, JSON_BUILD_ARRAY(
            JSON_BUILD_OBJECT('second',  0, 'seqno',  6, 'bloom', 299792458),
            JSON_BUILD_OBJECT('second', 42, 'seqno',  7, 'bloom', 3776),
            JSON_BUILD_OBJECT('second', 42, 'seqno',  8, 'bloom', 3776),
            JSON_BUILD_OBJECT('second', 43, 'seqno',  9, 'bloom', 599),
            JSON_BUILD_OBJECT('second', 43, 'seqno', 11, 'bloom', 634),
            JSON_BUILD_OBJECT('second', 59, 'seqno', 10, 'bloom', 333)
        )::TEXT::BYTEA)
    ON CONFLICT ON CONSTRAINT items_2023_01_08_cafef00ddeadbeafface864299792458_pkc
    DO UPDATE
    SET val = EXCLUDED.val
    WHERE tgt.val <> EXCLUDED.val
  " \
  || exec printf 'Unable to populate a test table.\n'

psql \
  --command="
    CREATE TABLE IF NOT EXISTS kvs_2023_01_08_cafef00ddeadbeafface864299792458(
        key BYTEA NOT NULL,
        val BYTEA NOT NULL,
        rowid SERIAL NOT NULL,
        CONSTRAINT kvs_2023_01_08_cafef00ddeadbeafface864299792458_pkc
        PRIMARY KEY(key)
    );

    CREATE UNIQUE INDEX IF NOT EXISTS kvs_2023_01_08_cafef00ddeadbeafface864299792458_ux
    ON kvs_2023_01_08_cafef00ddeadbeafface864299792458(rowid)
    ;
  " \
  || exec printf 'Unable to create a test table.\n'

echo 'creating 131,072 rows...'
psql \
  --command="
    INSERT INTO kvs_2023_01_08_cafef00ddeadbeafface864299792458 AS tgt (key,val)
    SELECT
        key::TEXT::BYTEA,
        JSON_BUILD_ARRAY(
            JSON_BUILD_OBJECT('second', 0, 'seqno', 42, 'bloom', 634),
            JSON_BUILD_OBJECT('second', 2, 'seqno', 43, 'bloom', 333),
            JSON_BUILD_OBJECT('second', 4, 'seqno', 44, 'bloom', 3776),
            JSON_BUILD_OBJECT('second', 6, 'seqno', 47, 'bloom', 599)
        )::TEXT::BYTEA
    FROM (SELECT GENERATE_SERIES(1,131072) AS key) AS t
    ON CONFLICT ON CONSTRAINT kvs_2023_01_08_cafef00ddeadbeafface864299792458_pkc
    DO UPDATE
    SET val = EXCLUDED.val
    WHERE tgt.val <> EXCLUDED.val
  " \
  || exec printf 'Unable to populate a test table.\n'

./bloom
