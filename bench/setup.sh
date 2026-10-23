#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

florinutz_ref=${FLORINUTZ_PGCDC_REF:-adea859fc6237aac5cc76262cb0fedfa271f2186}

for image in quay.io/debezium/server:3.7.0.Final sequin/sequin:v0.14.6 motoserver/moto:5.2.4 postgres:16 redis:7 alpine:3.22 golang:1.25; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image"
done

mkdir -p bin build
docker run --rm -v "$PWD/benchctl:/src:ro" -v "$PWD/bin:/out" -w /src -e CGO_ENABLED=0 golang:1.25 go build -o /out/benchctl .

if [ ! -d build/pgcdc ]; then
  git clone -q https://github.com/florinutz/pgcdc build/pgcdc
fi
git -C build/pgcdc fetch -q origin
git -C build/pgcdc checkout -q "$florinutz_ref"
docker build -q -t "${FLORINUTZ_PGCDC_IMAGE:-cdcbench/pgcdc:bench}" build/pgcdc
