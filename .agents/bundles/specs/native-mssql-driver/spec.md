---
type: Spec
flow_id: native-mssql-driver
title: "Standalone Rust SQL Server Wheel (vertebra-mssql)"
description: "Standalone PyO3 crate src/rs/vertebra-mssql (vertebra._mssql + vertebra-core[mssql]) built on tiberius (rustls, TDS 7.4/8.0, sp_executesql RPC, token stream, BulkLoad, ATTENTION cancellation, VTBARROW ShmCapsule ArrowCursor), replacing the MissingDependencyError guard with the native client."
status: stable
state: in_progress
has_chapters: false
parent_prd: native-data-platform
depends_on_flows:
  - driver-fail-loud
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 3
plan_commit: fd571e3a
state_revision: 2
current_task: "native-mssql-driver:t02"
last_verified_checkpoint: fd571e3a
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-09T14:58:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: mssql-rs
    resource: src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders
    title: "@P1 placeholder rewrite helper, the surviving MSSQL codec in vertebra-db"
  - id: fail-loud-baseline
    resource: .agents/bundles/specs/driver-fail-loud/spec.md
    title: Upstream flow that replaces the MSSQL sqlite3 stand-in with a MissingDependencyError guard and deletes TDS framing
  - id: mssql-py
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: Python SyncMSSQLDriver and AsyncMSSQLDriver target cutover module
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
tags:
  - rust
  - mssql
  - tds
  - driver
  - wheel
---

# Flow Specification: Standalone Rust SQL Server Wheel (`vertebra-mssql`)

<!-- truth: start -->
- **Zero-ODBC Pure-Rust Wheel**: `src/rs/vertebra-mssql` (`vertebra._mssql`, `vertebra-core[mssql]`) builds on `tiberius 0.13.0` (`rustls`) with zero `msodbcsql18` or `unixODBC` C/C++ dependencies; `tiberius` owns TDS framing and `vertebra-db` keeps only `rewrite_mssql_placeholders` [^mssql-rs].
- **TDS 7.4/8.0 & `VTBARROW`**: `SyncMSSQLClient` and `AsyncMSSQLClient` implement `PRELOGIN`, TLS-in-TDS / TDS 8.0, `sp_executesql` RPC `@P1` binding, `BulkLoad`, `ATTENTION` cancellation, and `VTBARROW` `ShmCapsule` results [^py-async-rs].
- **Guard Replacement**: `SyncMSSQLDriver` and `AsyncMSSQLDriver` resolve `vertebra._mssql` via `ensure_module("vertebra._mssql", package="vertebra-core[mssql]", extra="mssql")` with no `sqlite3` fallback and pass `test_driver_conformance.py` against SQL Server [^mssql-py] [^fail-loud-baseline].
<!-- truth: end -->

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Fail-loud baseline & codec cleanup.** `vertebra-db` retains only `rewrite_mssql_placeholders` [^mssql-rs] while legacy TDS header codecs (`encode_tds_packet`, `decode_tds_packet`, `TdsPacketType`, `MSSQL_DIALECT_TAG`, `MssqlPool`) and `encode_tds_sql_batch` are removed [^fail-loud-baseline].
- **SQL Server / MSSQL (`src/rs/vertebra-mssql/` -> `vertebra._mssql` + `vertebra-core[mssql]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `tiberius` `0.13.0` (`default-features = false`, `tds73`, `rustls`, `chrono`, `rust_decimal`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_mssql.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].mssql`, wheel isolation `exclude` globs, and `areas.toml` registration so `vertebra-core` stays free of TDS wire crates and requires zero `msodbcsql18` or `unixODBC` C/C++ packages.
- **Explicit `Sync*` / `Async*` clients & Arrow SHM transfer.** Expose `SyncMSSQLClient` (`SyncCompletionQueue`) and `AsyncMSSQLClient` (`AsyncCompletionQueue` via `vertebra_db::py_async` with `"_vertebra_mssql_completion_drainer"`), token stream -> `VTBARROW` `ShmCapsule` `ArrowCursor` decoding, Cloud SQL for SQL Server `:3307` mTLS dialer integration, `BulkLoadRequest` `load_from_arrow`, and out-of-band `ATTENTION` cancellation [^py-async-rs].

## Scope

1. **Crate scaffold, connection & authentication (`src/rs/vertebra-mssql/`).**
   - `SyncMSSQLClient` and `AsyncMSSQLClient` in `vertebra._mssql`; TDS 7.4 / 8.0 `PRELOGIN`, TLS-in-TDS / strict TLS negotiation (`EncryptionLevel`), `LOGIN7` SQL authentication, `ENVCHANGE` routing, and Cloud SQL `:3307` mTLS support.
   - Clean up legacy TDS packet codecs (`encode_tds_packet`, `decode_tds_packet`, `TdsPacketType`, `MSSQL_DIALECT_TAG`, `MssqlPool`, `encode_tds_sql_batch`) so only `rewrite_mssql_placeholders` remains in `vertebra-db`.
2. **Protocol, RPC, transactions, cancellation & errors.**
   - SQL batch and `sp_executesql` RPC execution with `@P1` parameter rewriting (`vertebra_db::mssql::rewrite_mssql_placeholders`), transaction lifecycle (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`, `ROLLBACK TRANSACTION`, `SAVE TRANSACTION`), out-of-band `ATTENTION` packet cancellation, and TDS error number (`2601`/`2627`/`547`/`515` -> `IntegrityError`, `1205` -> `SerializationFailure`, `1222`/timeout -> `QueryCancelledError`, etc.) -> `DatabaseError` hierarchy mapping.
3. **Codecs, `VTBARROW` `ShmCapsule` & bulk load.**
   - Codecs for `numeric`/`decimal`/`money`, `datetime`/`datetime2`/`datetimeoffset`/`date`/`time`, `uniqueidentifier` (`UUID`), `(n)varchar(max)` PLP, `varbinary`, `xml`, and `json`, plus `VTBARROW` `ShmCapsule` (`select_to_arrow`, `stream_arrow_batches`), `BulkLoadRequest` `load_from_arrow`, and `BrokerEngineBackend` registration.
4. **Python driver cutover, conformance & documentation.**
   - Cut `src/py/vertebra/db/adapters/mssql/driver.py` (`SyncMSSQLDriver`, `AsyncMSSQLDriver`) over to `ensure_module("vertebra._mssql", package="vertebra-core[mssql]", extra="mssql")` delegating to `SyncMSSQLClient` and `AsyncMSSQLClient`, removing all `sqlite3` stand-in and `memory://` execution paths, forwarding `dialer` on `AsyncMSSQLDriver`, updating unit and conformance tests, and refreshing docs and OKF knowledge chapters.

## Public Seams

`vertebra._mssql` (`SyncMSSQLClient`, `AsyncMSSQLClient`), `SyncMSSQLDriver`, and `AsyncMSSQLDriver`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`), `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`), and [`driver-fail-loud`](../driver-fail-loud/spec.md).

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for SQL Server 2022/2025 via `pytest-databases` and verifies `MissingDependencyError` fail-loud semantics when `vertebra._mssql` is absent.
- Golden wire/loopback and unit tests cover TDS `PRELOGIN`/`LOGIN7`, `sp_executesql` RPC `@P1` binding, transactions and savepoints, `ATTENTION` cancel, `VTBARROW` `ShmCapsule`, `BulkLoad`, and TDS error -> `DatabaseError` mapping.
- Wheel isolation check confirms `vertebra-mssql` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._mssql`, `cargo test -p vertebra-mssql`, `cargo clippy -p vertebra-mssql`, `ruff`, `mypy`, `pytest`, and `review_setup validate` pass.

## Tasks

- [x] [t01: Scaffold `vertebra-mssql` crate, TDS `PRELOGIN`/TLS/`LOGIN7`, and Cloud SQL `:3307` mTLS](tasks/t01-vertebra-mssql-crate-prelogin-and-tls.md)
- [~] [t02: Implement MSSQL SQL batch, `sp_executesql` RPC, transactions, `ATTENTION` cancellation, and `DatabaseError` mapping](tasks/t02-mssql-rpc-transactions-attention-and-errors.md)
- [ ] [t03: Implement MSSQL codecs, `VTBARROW` `ShmCapsule` export, `BulkLoad` `load_from_arrow`, and `BrokerEngineBackend` registration](tasks/t03-mssql-codecs-vtbarrow-and-bulk-load.md)
- [ ] [t04: Cut Python `SyncMSSQLDriver` and `AsyncMSSQLDriver` over to `vertebra._mssql`, update tests, and refresh docs](tasks/t04-python-mssql-driver-cutover-and-docs.md)

[^mssql-rs]: `src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders` — `@P1` placeholder rewrite helper, the surviving MSSQL codec in vertebra-db.
[^fail-loud-baseline]: `.agents/bundles/specs/driver-fail-loud/spec.md` — Upstream flow that replaces the MSSQL `sqlite3` stand-in with a `MissingDependencyError` guard and deletes TDS framing.
[^mssql-py]: `src/py/vertebra/db/adapters/mssql/driver.py` — Python SyncMSSQLDriver and AsyncMSSQLDriver target cutover module.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
