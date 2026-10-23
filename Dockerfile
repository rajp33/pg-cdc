# syntax=docker/dockerfile:1

FROM rust:1.99.0-slim-trixie@sha256:2752b332db73fdbb7dc576f06c82ed1f312005784ef913d7e04a28f5f55dc581 AS build
ARG TARGETARCH
ENV RUSTUP_TOOLCHAIN=1.99.0
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/src/target,id=cargo-target-${TARGETARCH} \
    cargo build --release --locked --package pg-cdc \
    && cp target/release/pg-cdc /usr/local/bin/pg-cdc

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:b7e81cb1af38d9345680e68bcf564d388f96304e25222030fc8c051faf8565b3
COPY --from=build /usr/local/bin/pg-cdc /usr/local/bin/pg-cdc
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/pg-cdc", "health"]
ENTRYPOINT ["/usr/local/bin/pg-cdc"]
CMD ["run", "--config", "/etc/pg-cdc/config.yaml"]
