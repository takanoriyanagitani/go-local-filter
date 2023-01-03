#!/bin/sh

out=./coverage.txt

go test \
  -race \
  -coverprofile=coverage.txt \
  -covermode=atomic \
  ./...

go tool \
  cover \
  -html="${out}" \
  -o coverage.html
