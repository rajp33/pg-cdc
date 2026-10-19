# Engineering standards

These rules bind every change. `mise run check` enforces most of them.

## Toolchain

- Rust 1.99.0, pinned in `rust-toolchain.toml` with rustfmt, clippy and llvm-tools-preview. Edition 2024; workspace `rust-version = "1.99"`.
- Nightly is used only for cargo-fuzz. `fuzz/` sits outside the workspace.
- Dev tools are pinned to exact versions in `mise.toml`. Every gate is a mise task, and CI installs the same tools with jdx/mise-action and runs the same tasks. Run `mise trust && mise install` once per checkout.

## The gate

`mise run check` runs, in order: `cargo fmt --check`, `taplo fmt --check`, `typos`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo clippy -p pg-cdc --no-default-features --lib --bins -- -D warnings` (so the build without the `otel` feature always compiles), `cargo doc --workspace --no-deps --all-features` with `RUSTDOCFLAGS="-D warnings"`, `cargo nextest run --workspace --all-features` (Docker-backed integration tests included), `cargo deny check` and `cargo machete`. Work is not done until it passes.

## Lints

- Lints live in `[workspace.lints]`; every crate sets `[lints] workspace = true`.
- Rust: `unsafe_code` forbid, `unused_must_use` deny, `missing_debug_implementations` and `unreachable_pub` warn, `rust_2018_idioms` warn.
- Clippy: `all` and `pedantic` deny; `module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc` and `must_use_candidate` allowed; `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`, `print_stdout`, `print_stderr` and `indexing_slicing` deny. `clippy.toml` allows unwrap, expect, panic, print and indexing in tests.
- An exception is `#[expect(<lint>, reason = "...")]` at the narrowest scope. Never weaken a lint to pass the gate.

## Supply chain

- `deny.toml` denies vulnerable and yanked crates, allows only Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-3.0, Zlib, CDLA-Permissive-2.0 and MPL-2.0, allows only crates.io, and bans wildcard versions, OpenSSL, native-tls and aws-lc.
- `Cargo.lock` is committed. TLS is rustls with the ring provider.
- Dependencies are declared once in `[workspace.dependencies]` at exact versions, with `default-features = false` where practical.

## Code

- No code comments, doc comments included, except one that states a genuine exception or anomaly the code cannot express. History and rationale belong in commit messages and pull requests.
- `thiserror` for library errors; `anyhow` only in `main` and the CLI. No panics outside tests.
- tokio for async; no blocking in async code; no lock held across `.await`. `tracing` for logs.
- Never commit secrets and never log credentials.

## Tests

- cargo-nextest runs everything. Unit tests sit beside the code; integration tests live in `crates/*/tests` and use `crates/testkit` (package `pg-cdc-testkit`).
- proptest for property tests; fuzz targets live in `fuzz/`.
- Tests assert behavior, are deterministic and print nothing on success.
- Coverage floors, enforced in CI by `mise run cov`: `pg-cdc-decoder` at least 90% of lines; the workspace at least 80%, excluding testkit and loadgen. Both floors are enforced once `crates/decoder` exists; until then `mise run cov` only prints the report.
- The testkit labels every container it starts `pg-cdc-testkit.created=<Unix seconds>` and, at most once every 10 minutes per machine, removes labeled containers older than an hour, so containers leaked by a killed test run do not pile up.
- Chaos and load runs stay out of the gate: `mise run chaos-smoke`, `mise run chaos` and `mise run load` (see [Local development](#local-development) and [docs/load.md](load.md)). `crates/loadgen` (package `pg-cdc-loadgen`, binary `loadgen`) is a CLI, uses `anyhow`, and is excluded from coverage.

## Local development

```sh
mise trust && mise install
mise run check
```

Integration tests need Docker; `PGCDC_TEST_PG_VERSION` selects Postgres 14 to 18 (default 17).

Outside the gate:

- `mise run chaos-smoke` runs the chaos harness for 15 s (seed 1) against two debug instances, Postgres and moto, and checks that nothing was lost and per-key order held; CI runs it as its own job.
- `mise run chaos` runs it on a release build with `CHAOS_SEED` (default: the current Unix time) for about 5 minutes: the opening takeovers, 3 minutes of chaos and at most 1 minute to settle. Whatever is still parked or not yet delivered then must be in S3 or retained by the slot.
- `mise run load` builds the release and `--no-default-features` binaries and runs the load scenarios into `target/load`; `LOAD_SCENARIO=ceiling|sns|overhead|memory|outage|brownout|handover` runs one. `cargo run -p pg-cdc-loadgen -- report --report-dir target/load --out summary.md` writes the summary table. Results are in [docs/load.md](load.md).
- `cargo run -p pg-cdc-loadgen -- write --database <db> --user <user> --slot <slot> --rate 1000 --duration 60s` writes the chaos workload (upserts into `loadgen_items` from one connection) against any Postgres, with the password from `PGPASSWORD`; given `--slot`, it first creates the table with FULL identity, the `loadgen` publication and each slot.

## CI

`.github/workflows/ci.yml` runs on pull requests, pushes to `main` and manual dispatch, with one concurrency group per ref and top-level `contents: read`. Third-party actions are pinned to commit SHAs. Jobs: `lint` (the gate minus tests, plus actionlint), `test` (Postgres 17), `pg-matrix` (Postgres 14, 15, 16 and 18), `coverage`, `chaos-smoke` (the 15-second chaos run, uploading `target/chaos` when it fails) and `fuzz-smoke` (60 seconds per target). The `image` job builds the release image for linux/amd64 and linux/arm64 on native runners and smoke-tests each. On pushes to `main` and `v*` tags, once every other job has passed, `image-push` pushes each architecture to `ghcr.io/theia-vision-inc/pg-cdc` by digest, with an SBOM and build provenance, and `image-publish` merges the multi-arch manifest, tags it and signs it with cosign. Dependabot updates cargo, GitHub Actions and the Dockerfile's base images weekly. The repository is public, so no job needs a secret beyond the workflow's own `GITHUB_TOKEN`, which only the image push uses.

## Commits

Conventional commits (`feat`, `fix`, `test`, `refactor`, `perf`, `build`, `ci`, `docs`, `chore`), small and focused. Commits written with Claude end with its `Co-Authored-By` and `Claude-Session` lines.
