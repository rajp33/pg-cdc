# Engineering standards

These rules bind every change. `mise run check` enforces most of them.

## Toolchain

- Rust 1.99.0, pinned in `rust-toolchain.toml` with rustfmt, clippy and llvm-tools-preview. Edition 2024; workspace `rust-version = "1.99"`.
- Nightly is used only for cargo-fuzz. `fuzz/` sits outside the workspace.
- Dev tools are pinned to exact versions in `mise.toml`. Every gate is a mise task, and CI installs the same tools with jdx/mise-action and runs the same tasks. Run `mise trust && mise install` once per checkout.

## The gate

`mise run check` runs, in order: `cargo fmt --check`, `taplo fmt --check`, `typos`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`, `cargo nextest run --workspace --all-features` (Docker-backed integration tests included), `cargo deny check` and `cargo machete`. Work is not done until it passes.

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
- Coverage floors, enforced in CI by `mise run cov`: `pg-cdc-decoder` at least 90% of lines; the workspace at least 80%, excluding testkit, loadgen and the vendored replication crate.

## CI

`.github/workflows/ci.yml` runs on pull requests, pushes to `main` and manual dispatch, with one concurrency group per ref and top-level `contents: read`. Third-party actions are pinned to commit SHAs. Jobs: `lint` (the gate minus tests, plus actionlint), `test` (Postgres 17), `pg-matrix` (Postgres 14, 15, 16 and 18), `coverage` and `fuzz-smoke` (60 seconds per target). Dependabot updates cargo and GitHub Actions weekly. The repository is public, so no job needs a secret.

## Commits

Conventional commits (`feat`, `fix`, `test`, `refactor`, `perf`, `build`, `ci`, `docs`, `chore`), small and focused. Commits written with Claude end with its `Co-Authored-By` and `Claude-Session` lines.
