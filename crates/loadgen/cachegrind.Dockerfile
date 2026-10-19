# syntax=docker/dockerfile:1

FROM rust:1.99.0-slim-trixie@sha256:2752b332db73fdbb7dc576f06c82ed1f312005784ef913d7e04a28f5f55dc581 AS build
ARG TARGETARCH
ENV RUSTUP_TOOLCHAIN=1.99.0
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/src/target,id=cachegrind-target-${TARGETARCH} \
    cargo build --release --locked --package pg-cdc \
    && cargo build --release --locked --package pg-cdc --no-default-features --target-dir target/baseline \
    && cp target/release/pg-cdc /usr/local/bin/pg-cdc-default \
    && cp target/baseline/release/pg-cdc /usr/local/bin/pg-cdc-baseline

FROM debian:trixie-20260918-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
RUN apt-get update \
    && apt-get install --yes --no-install-recommends valgrind=1:3.24.0-3 libjemalloc2=5.3.0-3 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dpkg -L libjemalloc2 | grep 'libjemalloc.so.2$')" /usr/local/lib/libjemalloc.so.2
COPY --from=build /usr/local/bin/pg-cdc-default /usr/local/bin/pg-cdc-baseline /usr/local/bin/
