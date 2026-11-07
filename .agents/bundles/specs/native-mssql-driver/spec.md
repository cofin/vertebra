---
type: Spec
flow_id: native-mssql-driver
title: Standalone Rust SQL Server Wheel (vertebra-mssql)
description: Standalone PyO3 crate src/rs/vertebra-mssql (vertebra._mssql + vertebra-core[mssql]) built on tiberius (rustls, TDS 7.4/8.0, sp_executesql RPC, token stream, BulkLoad, ATTENTION cancellation, VTBARROW ShmCapsule ArrowCursor), replacing the sqlite3 stand-in.
state: draft
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: "2026-11-06T06:30:00Z"
tags: [rust, mssql, tds, driver, wheel]
---

# Flow Charter: Standalone Rust SQL Server Wheel (`vertebra-mssql`)

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **SQL Server / MSSQL (`src/rs/vertebra-mssql/` -> `vertebra._mssql` + `vertebra-core[mssql]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `tiberius` `0.13.0` (`default-features = false`, `tds73`, `rustls`, `chrono`, `rust_decimal`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_mssql.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].mssql`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration so `vertebra-core` stays free of TDS wire crates and requires zero `msodbcsql18` or `unixODBC` C/C++ packages.
- **Explicit `Sync*` / `Async*` clients & Arrow SHM transfer.** Expose `SyncMSSQLClient` (`SyncCompletionQueue`) and `AsyncMSSQLClient` (`AsyncCompletionQueue` via `vertebra_db::py_async` with `"_vertebra_mssql_completion_drainer"`), token stream -> `VTBARROW` `ShmCapsule` `ArrowCursor` decoding, Cloud SQL for SQL Server `:3307` mTLS dialer integration, `BulkLoadRequest` `load_from_arrow`, and out-of-band `ATTENTION` cancellation.

## Scope

1. **Crate scaffold, connection & authentication (`src/rs/vertebra-mssql/`).**
   - `SyncMSSQLClient` and `AsyncMSSQLClient` in `vertebra._mssql`; TDS 7.4 / 8.0 `PRELOGIN`, TLS-in-TDS / strict TLS 8.0 negotiation, `LOGIN7` SQL authentication, `ENVCHANGE` routing, and Cloud SQL `:3307` mTLS support.
2. **Protocol, codecs & Arrow.**
   - SQL batch and `sp_executesql` RPC execution with `@P1` parameter rewriting (`vertebra_db::mssql::rewrite_mssql_placeholders`), transaction descriptors, `ATTENTION` cancellation, `BrokerEngineBackend` registration, and TDS error number -> `DatabaseError` mapping (`vertebra._vertebra`).
   - Codecs for `numeric`/`decimal`/`money`, `datetime2`/`datetimeoffset`, `uniqueidentifier`, `(n)varchar(max)` PLP, `varbinary`, `xml`, and `json`, plus `BulkLoad` `load_from_arrow` and `VTBARROW` `ShmCapsule` `select_to_arrow` / `stream_arrow_batches`.
3. **Python driver cutover.**
   - Cut `vertebra/db/adapters/mssql/driver.py` (`SyncMSSQLDriver`, `AsyncMSSQLDriver`) over to `ensure_module("vertebra._mssql", package="vertebra-core[mssql]", extra="mssql")`, removing `sqlite3.connect(":memory:")` and stand-in fallbacks, and activating `mssql` in `src/py/tests/integration/db/test_driver_conformance.py`.

## Public Seams

`vertebra._mssql` (`SyncMSSQLClient`, `AsyncMSSQLClient`), `SyncMSSQLDriver`, and `AsyncMSSQLDriver`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`) and `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`).

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for SQL Server 2022/2025 via `pytest-databases`.
- Golden wire/unit tests cover TDS `PRELOGIN`/`LOGIN7`, `sp_executesql` RPC `@P1` binding, `ATTENTION` cancel, `BulkLoad`, and TDS error -> `DatabaseError` mapping.
- Wheel isolation check confirms `vertebra-mssql` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._mssql`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-mssql` (`vertebra._mssql`, `_mssql.pyi`, `[mssql]` extra + `uv.lock` + `release.yaml`/`ci.yml`), TDS `PRELOGIN`/TLS/`LOGIN7`, and Cloud SQL mTLS integration.
- t02 MSSQL SQL batch, `sp_executesql` RPC, transactions, `ATTENTION` cancel, and `DatabaseError` mapping.
- t03 MSSQL codecs, `VTBARROW` `ShmCapsule` export, `BulkLoad` `load_from_arrow`, and `BrokerEngineBackend` registration.
- t04 Python `SyncMSSQLDriver` / `AsyncMSSQLDriver` cutover, `sqlite3` stand-in removal, conformance activation, and docs/OKF update.
