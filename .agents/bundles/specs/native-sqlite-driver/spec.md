---
type: Spec
flow_id: native-sqlite-driver
title: Standalone Rust SQLite Wheel (vertebra-sqlite)
description: Standalone PyO3 crate src/rs/vertebra-sqlite (vertebra._sqlite + vertebra-core[sqlite]) built on bundled rusqlite with dedicated per-connection worker threads, WAL/mmap pragmas, sqlite3_interrupt cancellation, and zero-copy VTBARROW ShmCapsule results, replacing Python stdlib sqlite3.
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
tags: [rust, sqlite, driver, wheel]
---

# Flow Charter: Standalone Rust SQLite Wheel (`vertebra-sqlite`)

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **SQLite (`src/rs/vertebra-sqlite/` -> `vertebra._sqlite` + `vertebra-core[sqlite]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `rusqlite` `0.41.1` (`bundled`, `hooks`, `serialize`, `blob`, `column_decltype`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_sqlite.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].sqlite`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration.
- **Explicit `Sync*` / `Async*` clients & Arrow SHM transfer.** Run each pooled connection on a dedicated Rust worker thread bridged through `SyncCompletionQueue` (`SyncSQLiteClient`) and `AsyncCompletionQueue` + `Notifier` (`AsyncSQLiteClient` via `vertebra_db::py_async` with `"_vertebra_sqlite_completion_drainer"`), `sqlite3_interrupt()` out-of-band cancellation, and direct `sqlite3_column_*` -> `VTBARROW` `ShmCapsule` `ArrowCursor` building safe for free-threaded `3.14t`/`3.15t`.

## Scope

1. **Crate scaffold, pragmas & worker pool (`src/rs/vertebra-sqlite/`).**
   - `SyncSQLiteClient` and `AsyncSQLiteClient` in `vertebra._sqlite`; WAL mode, `mmap_size`, `busy_timeout`, foreign keys, SQLite 3.45+ `JSONB` support, prepared statement cache, multi-reader / serialized-writer pool, and `BrokerEngineBackend` registration.
2. **Execution, cancellation, error mapping & Arrow.**
   - Positional and named binds, `execute`, `execute_many`, `BEGIN IMMEDIATE`/`DEFERRED` transactions and savepoints, `sqlite3_interrupt()` `cancel()`, SQLite extended error code -> `DatabaseError` mapping (`vertebra._vertebra`), and `VTBARROW` `ShmCapsule` `select_to_arrow` / `stream_arrow_batches` / `load_from_arrow`.
3. **Python driver cutover.**
   - Cut `vertebra/db/adapters/sqlite/driver.py` (`SyncSQLiteDriver`, `AsyncSQLiteDriver`) over to `ensure_module("vertebra._sqlite", package="vertebra-core[sqlite]", extra="sqlite")`, replacing Python stdlib `sqlite3` and verifying `sqlite` in `src/py/tests/integration/db/test_driver_conformance.py`.

## Public Seams

`vertebra._sqlite` (`SyncSQLiteClient`, `AsyncSQLiteClient`), `SyncSQLiteDriver`, and `AsyncSQLiteDriver`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`) and `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`).

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for SQLite (`:memory:` and file-backed WAL) via `vertebra._sqlite`.
- Concurrency and cancellation tests verify `sqlite3_interrupt()` aborts long-running recursive CTE queries and free-threaded `3.14t`/`3.15t` multi-reader queries run without GIL contention.
- Wheel isolation check confirms `vertebra-sqlite` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._sqlite`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `_sqlite.pyi`, `[sqlite]` extra + `uv.lock` + `release.yaml`/`ci.yml`), `rusqlite` worker threads, WAL/mmap pragmas, transactions, and `sqlite3_interrupt` cancellation.
- t02 SQLite prepared statement cache, `DatabaseError` extended-code mapping, `VTBARROW` `ShmCapsule` builder/importer, and `BrokerEngineBackend` registration.
- t03 Python `SyncSQLiteDriver` / `AsyncSQLiteDriver` cutover, stdlib `sqlite3` removal, conformance verification, and docs/OKF update.
