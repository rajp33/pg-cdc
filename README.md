# pg-cdc

pg-cdc streams row changes for the tables in one Postgres publication through a logical replication slot to a sink. Delivery is at least once, and pg-cdc never writes to the source database.

Status: milestone 1 of the [design](docs/superpowers/specs/2026-10-14-pg-cdc-design.md). Changes stream to stdout as JSON lines, and the slot is acknowledged after each transaction is written.

## Develop

```sh
mise trust && mise install
mise run check
```

`mise run check` is the only gate. Integration tests need Docker; `PGCDC_TEST_PG_VERSION` selects Postgres 14 to 18 (default 17). See [docs/engineering.md](docs/engineering.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option. Contributions are dual licensed the same way.
