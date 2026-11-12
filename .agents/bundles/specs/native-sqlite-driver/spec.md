---
type: Spec
flow_id: native-sqlite-driver
title: "Standalone Rust SQLite Wheel (vertebra-sqlite)"
description: "Standalone PyO3 crate src/rs/vertebra-sqlite (vertebra._sqlite + vertebra-core[sqlite]) built on bundled rusqlite with dedicated per-connection worker threads, WAL/mmap pragmas, sqlite3_interrupt cancellation, and zero-copy VTBARROW ShmCapsule results, replacing Python stdlib sqlite3 in the SQLite adapter."
status: draft
state: draft
has_chapters: false
parent_prd: native-data-platform
depends_on_flows:
  - db-layout-and-contract
  - db-engine-primitives
  - driver-fail-loud
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 3
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified: []
sources:
  - id: sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
    title: SQLite configuration and initialization pragma helpers in vertebra-db
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Completed fail-loud baseline removing SqlitePool and its msgpack row codecs from vertebra-db
  - id: sqlite-py
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: Python SyncSQLiteDriver and AsyncSQLiteDriver executing through stdlib sqlite3
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
  - id: sa-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: SQLAlchemy DB-API connection that opens its own sqlite3 connection for SQLite URLs
tags:
  - rust
  - sqlite
  - driver
  - wheel
---

# Flow Charter: Standalone Rust SQLite Wheel (`vertebra-sqlite`)

<!-- truth: start -->
- **Current State**: `SyncSQLiteDriver` and `AsyncSQLiteDriver` execute through Python stdlib `sqlite3`; `vertebra-db` keeps `SqliteConfig::initialization_pragmas` as the shared pragma source after `driver-fail-loud` deleted `SqlitePool` [^sqlite-py] [^sqlite-rs] [^fail-loud-baseline].
- **Bundled Pure-FFI Wheel (target state)**: `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `vertebra-core[sqlite]`) wraps `rusqlite 0.41.1` (`bundled`) with `#[pymodule(gil_used = false)]`.
- **GIL-Free Worker Threads & `sqlite3_interrupt` (target state)**: `SyncSQLiteClient` and `AsyncSQLiteClient` run per-connection worker threads over `SyncCompletionQueue` / `AsyncCompletionQueue` with WAL/mmap pragmas, `sqlite3_interrupt()` cancellation, and direct `VTBARROW` `ShmCapsule` encoding [^py-async-rs].
- **Stdlib `sqlite3` Removal (target state)**: `SyncSQLiteDriver` and `AsyncSQLiteDriver` execute exclusively through `vertebra._sqlite`.
<!-- truth: end -->

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Fail-loud baseline.** The merged `driver-fail-loud` baseline (`af1b3d30`, [knowledge](../../knowledge/domains/data/database-drivers-and-dialers.md)) deletes `SqlitePool` (with `encode_msgpack_rows` / `decode_msgpack_rows` and the `rmp-serde` dependency) from `vertebra-db`; `SqliteConfig::initialization_pragmas` stays as the shared pragma source this crate consumes [^fail-loud-baseline] [^sqlite-rs].
- **SQLite (`src/rs/vertebra-sqlite/` -> `vertebra._sqlite` + `vertebra-core[sqlite]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `rusqlite` `0.41.1` (`bundled`, `hooks`, `serialize`, `blob`, `column_decltype`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_sqlite.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].sqlite`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration, following the merged DuckDB, MySQL, SQL Server, and Oracle wheel layouts.
- **Explicit `Sync*` / `Async*` clients & Arrow SHM transfer.** Run each pooled connection on a dedicated Rust worker thread bridged through `SyncCompletionQueue` (`SyncSQLiteClient`) and `AsyncCompletionQueue` + `Notifier` (`AsyncSQLiteClient` via `vertebra_db::py_async` with `"_vertebra_sqlite_completion_drainer"`), `sqlite3_interrupt()` out-of-band cancellation, and direct `sqlite3_column_*` -> `VTBARROW` `ShmCapsule` `ArrowCursor` building safe for free-threaded `3.14t`/`3.15t`.
- **Broker registration.** Cross-wheel broker hosting follows the decision `broker-pool-sharing` records for per-`cdylib` registries; this flow does not rely on the process-global `vertebra_db` broker backend map, which each standalone wheel links separately.

## Scope

1. **Crate scaffold, pragmas & worker pool (`src/rs/vertebra-sqlite/`).**
   - `SyncSQLiteClient` and `AsyncSQLiteClient` in `vertebra._sqlite`; WAL mode, `mmap_size`, `busy_timeout`, foreign keys, SQLite 3.45+ `JSONB` support, prepared statement cache, and a multi-reader / serialized-writer pool.
2. **Execution, cancellation, error mapping & Arrow.**
   - Positional and named binds, `execute`, `execute_many`, `BEGIN IMMEDIATE`/`DEFERRED` transactions and savepoints, `sqlite3_interrupt()` `cancel()`, SQLite extended error code -> `DatabaseError` mapping (`vertebra._vertebra`), and `VTBARROW` `ShmCapsule` `select_to_arrow` / `stream_arrow_batches` / `load_from_arrow`.
3. **Python driver cutover.**
   - Cut `vertebra/db/adapters/sqlite/driver.py` (`SyncSQLiteDriver`, `AsyncSQLiteDriver`) over to `ensure_module("vertebra._sqlite", package="vertebra-core[sqlite]", extra="sqlite")`, replacing Python stdlib `sqlite3` and verifying `sqlite` in `src/py/tests/integration/db/test_driver_conformance.py`.
   - The SQLAlchemy DB-API's own `sqlite3` connection in `extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` belongs to `sqlalchemy-and-django-integration`; this flow leaves the DB-API executing through the SQLite driver it cuts over [^sa-dbapi].
4. **Docs & OKF.** Update `docs/` driver and installation pages and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, and record caveats in a SQLite limitations chapter under `.agents/bundles/knowledge/domains/data/`.

## Public Seams

`vertebra._sqlite` (`SyncSQLiteClient`, `AsyncSQLiteClient`), `SyncSQLiteDriver`, and `AsyncSQLiteDriver`.

## Dependencies

Flow dependencies (all merged): `db-layout-and-contract` (`21c99f95`), integration-remediation `db-engine-primitives` (`80ff8a65`), and `driver-fail-loud` (`af1b3d30`), which deletes `SqlitePool`. This flow starts from `main @ c8722c76` and does not wait on `native-postgres-driver`.

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for SQLite (`:memory:` and file-backed WAL) via `vertebra._sqlite`.
- Concurrency and cancellation tests verify `sqlite3_interrupt()` aborts long-running recursive CTE queries and free-threaded `3.14t`/`3.15t` multi-reader queries run without GIL contention.
- `git grep -n "import sqlite3" -- src/py/vertebra/db/adapters/sqlite` returns nothing.
- Wheel isolation check confirms `vertebra-sqlite` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._sqlite`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `_sqlite.pyi`, `[sqlite]` extra + `uv.lock` + `release.yaml`/`ci.yml`), `rusqlite` worker threads, WAL/mmap pragmas, transactions, and `sqlite3_interrupt` cancellation.
- t02 SQLite prepared statement cache, `DatabaseError` extended-code mapping, and `VTBARROW` `ShmCapsule` builder/importer.
- t03 Python `SyncSQLiteDriver` / `AsyncSQLiteDriver` cutover, stdlib `sqlite3` removal from the adapter, conformance verification, and docs/OKF update.

[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — SQLite configuration and initialization pragma helpers in vertebra-db.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud baseline removing `SqlitePool` and its msgpack row codecs from `vertebra-db`.
[^sqlite-py]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Python SyncSQLiteDriver and AsyncSQLiteDriver executing through stdlib `sqlite3`.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — SQLAlchemy DB-API connection that opens its own `sqlite3` connection for SQLite URLs.
