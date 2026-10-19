# syntax=docker/dockerfile:1

FROM rust:1.99.1-slim-trixie@sha256:4df2ae8cfdfebdfc13a65da7f253b73d68983dc0339f2719bc17a279c913b13d AS build
ARG TARGETARCH
ENV RUSTUP_TOOLCHAIN=1.99.0
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/src/target,id=cargo-target-${TARGETARCH} \
    cargo build --release --locked --package pg-cdc \
    && cp target/release/pg-cdc /usr/local/bin/pg-cdc

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:e792ab3d241a468a4fd7519ddbbebe66b49b5f365771716ea688ad40b6c6f1c2
COPY --from=build /usr/local/bin/pg-cdc /usr/local/bin/pg-cdc
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/pg-cdc", "health"]
ENTRYPOINT ["/usr/local/bin/pg-cdc"]
CMD ["run", "--config", "/etc/pg-cdc/config.yaml"]
