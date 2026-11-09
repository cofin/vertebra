---
type: Spec
flow_id: native-duckdb-driver
title: "Standalone Rust DuckDB Wheel (vertebra-duckdb)"
description: "Standalone PyO3 crate src/rs/vertebra-duckdb (vertebra._duckdb + vertebra-core[duckdb]) built on bundled duckdb with arrow 60 -> VTBARROW ShmCapsule streaming, Appender bulk load, GCS ADC secret rotation, and interrupt cancellation, replacing the PyPI duckdb package with vertebra._duckdb."
status: draft
state: draft
has_chapters: false
parent_prd: native-data-platform
depends_on_flows:
  - driver-fail-loud
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig
    title: DuckDB configuration and bootstrap pragma primitives in vertebra-db
  - id: fail-loud-baseline
    resource: .agents/bundles/specs/driver-fail-loud/spec.md
    title: Upstream flow that removes the DuckDB sqlite3 fallback and deletes DuckdbBrokerPool
  - id: duckdb-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: Python SyncDuckDBDriver and AsyncDuckDBDriver target cutover module
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql
    title: GCS secret SQL builder slated for relocation to DuckDBCore
tags:
  - rust
  - duckdb
  - arrow
  - driver
  - wheel
---

# Flow Charter: Standalone Rust DuckDB Wheel (`vertebra-duckdb`)

<!-- truth: start -->
- **Isolated Amalgamation & `arrow 60` Wheel**: `src/rs/vertebra-duckdb` (`vertebra._duckdb`, `vertebra-core[duckdb]`) isolates bundled `duckdb` and `arrow 60` from `vertebra-core` and `vertebra-oracle` (`arrow 59`) [^duckdb-rs].
- **Zero-Copy `VTBARROW` & `Appender`**: `SyncDuckDBClient` and `AsyncDuckDBClient` stream `arrow 60` batches into `VTBARROW` `ShmCapsule` `ArrowCursor`, ingest via `Appender`, and rotate GCS ADC secrets on `DuckDBCore.build_gcs_secret_sql` [^sqlspec-storage].
- **Package Replacement**: `driver-fail-loud` removes the `sqlite3` fallback, so `SyncDuckDBDriver` and `AsyncDuckDBDriver` require the PyPI `duckdb` package and raise `MissingDependencyError` without it; this flow's cutover replaces PyPI `duckdb` with `vertebra._duckdb` [^duckdb-py] [^fail-loud-baseline].
<!-- truth: end -->

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Fail-loud baseline.** [`driver-fail-loud`](../driver-fail-loud/spec.md) runs first: the DuckDB drivers execute only through the PyPI `duckdb` package, which lives in the GIL-only `[dependency-groups].duckdb` group that `make install`, `make py-test`, and `make coverage` install on GIL interpreters; DuckDB tests skip through `pytest.importorskip("duckdb")` on free-threaded interpreters, `DuckdbBrokerPool` is deleted from `vertebra-db`, and `DuckdbConfig::bootstrap_statements` stays as the shared pragma source [^fail-loud-baseline] [^duckdb-rs].

- **DuckDB (`src/rs/vertebra-duckdb/` -> `vertebra._duckdb` + `vertebra-core[duckdb]`).** Isolate `duckdb` `1.20000.1` (`bundled`, `appender-arrow`, `arrow 60`) in its own workspace PyO3 `cdylib` + `rlib` crate (`#[pymodule(gil_used = false)]`, `src/py/vertebra/_duckdb.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].duckdb`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration) so `vertebra-core` stays free of the C++ DuckDB amalgamation and `arrow 60` never collides with `vertebra-oracle` (`arrow 59`).
- **Explicit `Sync*` / `Async*` clients & Arrow SHM transfer.** Expose `SyncDuckDBClient` (`SyncCompletionQueue`) and `AsyncDuckDBClient` (`AsyncCompletionQueue` + `Notifier` via `vertebra_db::py_async` with `"_vertebra_duckdb_completion_drainer"`), serializing `arrow 60` `RecordBatch` streams directly into `VTBARROW` `ShmCapsule` `ArrowCursor` frames and loading Arrow batches via DuckDB `Appender`.

## Scope

1. **Crate scaffold, pragmas & worker pool (`src/rs/vertebra-duckdb/`).**
   - `SyncDuckDBClient` and `AsyncDuckDBClient` in `vertebra._duckdb`; single-writer / multi-connection session management, DuckDB 2.0-safe pragmas (`parquet_metadata_cache`, `httpfs_connection_caching`, `enable_http_metadata_cache`, cgroup-clamped `max_memory`/`threads`), `interrupt()` `cancel()`, `DatabaseError` mapping (`vertebra._vertebra`), and `BrokerEngineBackend` registration.
2. **Arrow SHM streaming, Appender load & GCS secret rotation.**
   - Zero-copy `arrow 60` `RecordBatch` -> `VTBARROW` `ShmCapsule` `ArrowCursor` (`select_to_arrow`, `stream_arrow_batches`), `Appender` `load_from_arrow`, and GCS ADC secret bootstrap (`DuckDBCore.build_gcs_secret_sql`, relocated from `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` with a delegating wrapper retained in `extensions/sqlspec/storage.py` until `sqlspec-adapter-parity` removes generic engine helpers) paired with per-session `applied_token_generation` rotation via `SyncGcpTokenCache` / `AsyncGcpTokenCache`.
3. **Python driver cutover.**
   - Cut `vertebra/db/adapters/duckdb/driver.py` (`SyncDuckDBDriver`, `AsyncDuckDBDriver`) from the PyPI `duckdb` package to `ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")`, removing the PyPI `duckdb` import path and the `[dependency-groups].duckdb` group (and its `bench` inclusion and Makefile `--group duckdb` wiring) once no test needs it, and activating `duckdb` in `src/py/tests/integration/db/test_driver_conformance.py`.

## Public Seams

`vertebra._duckdb` (`SyncDuckDBClient`, `AsyncDuckDBClient`), `SyncDuckDBDriver`, `AsyncDuckDBDriver`, and `DuckDBCore.build_gcs_secret_sql`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`), `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`), and [`driver-fail-loud`](../driver-fail-loud/spec.md), which removes the DuckDB `sqlite3` fallback and deletes `DuckdbBrokerPool`; this flow starts from that baseline.

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for DuckDB via `vertebra._duckdb`.
- Arrow streaming, `Appender` bulk load, `interrupt()` cancellation, and GCS `token_generation` secret rotation unit/integration tests pass through `vertebra._duckdb` with no PyPI `duckdb` import.
- Wheel isolation check confirms `vertebra-duckdb` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._duckdb`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-duckdb` (`vertebra._duckdb`, `_duckdb.pyi`, `[duckdb]` extra + `uv.lock` + `release.yaml`/`ci.yml`), session pool, pragmas, transactions, `interrupt()` cancel, and `DatabaseError` mapping.
- t02 `arrow 60` -> `VTBARROW` `ShmCapsule` streaming, `Appender` `load_from_arrow`, `BrokerEngineBackend` registration, and `DuckDBCore.build_gcs_secret_sql` relocation.
- t03 Python `SyncDuckDBDriver` / `AsyncDuckDBDriver` cutover from PyPI `duckdb` to `vertebra._duckdb`, conformance activation, and docs/OKF update.

[^duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig` — DuckDB configuration and bootstrap pragma primitives in vertebra-db.
[^fail-loud-baseline]: `.agents/bundles/specs/driver-fail-loud/spec.md` — Upstream flow that removes the DuckDB `sqlite3` fallback and deletes `DuckdbBrokerPool`.
[^duckdb-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Python SyncDuckDBDriver and AsyncDuckDBDriver target cutover module.
[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` — GCS secret SQL builder slated for relocation to DuckDBCore.
