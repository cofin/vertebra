---
type: Spec
flow_id: db-layout-and-contract
title: db/adapters and db/common Layout, Driver Contract, and Conformance Suite
description: Move engine code to vertebra/db/adapters/<engine>, engine-agnostic code to vertebra/db/common, define one driver contract and capability protocols, make the registry polymorphic, enforce the db import boundary, and add a cross-engine driver conformance suite.
state: draft
parent_prd: native-data-platform
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-03T04:05:00Z
tags: [db, layout, contract, registry, conformance]
---

# Flow Charter: db/adapters and db/common Layout, Driver Contract, and Conformance Suite

## Charter

This flow sets the structure every native driver and integration builds on.

## Scope

1. **Layout.**
   - Move `vertebra/db/<engine>/` → `vertebra/db/adapters/<engine>/` for postgres, mysql, sqlite, oracle, mssql, duckdb, bigquery, spanner, redis, valkey, and datafusion (from `db/datafusion.py`).
   - Move `vertebra/db/driver/` → `vertebra/db/common/driver/`, `db/_registry.py` → `db/common/registry.py`, `db/_arrow.py` → `db/common/arrow.py`, and `db/dialer/` → `db/common/dialer/`.
   - `db/__init__.py` exports only the curated public names.
   - The Rust `vertebra-db/src/` gains `<engine>/` modules and `common/` (lease table, statement cache, Arrow SHM, framing helpers).
2. **Driver contract** (`db/common/driver/`).
   - `SyncDatabaseDriver`/`AsyncDatabaseDriver` with `execute`, `execute_many`, `select`, `select_one`, `select_one_or_none`, `select_value`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, `begin`/`commit`/`rollback`, a `transaction()` context, `cancel()`, and `close()`.
   - A typed exception hierarchy with engine error codes.
   - Remove the `# type: ignore[override]` signature drift across the engine drivers.
3. **Capability protocols.** `PipelineCapable`, `NotificationSource`, `ChangeSource`, `QueueStore`, and `EmbeddingPushdown`, with msgspec record types.
4. **Registry polymorphism.**
   - `BaseDatabaseConfig` gains `url_schemes: ClassVar[tuple[str, ...]]`, `from_url(url, **overrides)`, `pool_key()`, and `driver_classes: ClassVar[tuple[type, type]]`.
   - `DatabaseRegistry.parse_url`, `pool_fingerprint`, `_build_async_driver`, and `_build_sync_driver` become table lookups.
   - `provide_driver(name_or_config, *, sync=False)` replaces `provide_bigquery_driver`/`provide_spanner_driver`, including the re-exports in `extensions/litestar/__init__.py`.
5. **Import boundary.** A layout test asserts that no module under `vertebra/db/` imports `vertebra.agent`, `vertebra.framework`, `vertebra.extensions`, or `vertebra.server`.
6. **Shared Rust connection core.** `vertebra-db/src/common/connection.rs`:
   - a `Connection` trait;
   - TCP/Unix stream + rustls TLS, reusing the `grpc.rs` connector;
   - `dialer.rs` integration;
   - cancellation;
   - a typed error → Python exception map.

   It runs on the panic- and cancel-safe completion bridge from `gcp-wire-protocol-correctness`. Every native engine client builds on it.
7. **DataFusion.** `db/datafusion.py::execute_dicts` raises DataFusion errors instead of re-running the SQL on `sqlite3`.
8. **Conformance suite.**
   - `src/py/tests/integration/db/test_driver_conformance.py` is parameterized over engines through `pytest-databases`. It covers types (NULL, NUMERIC precision, timestamps with time zones, Unicode, bytes, JSON, vectors), transactions, errors, Arrow, cancellation, and pipelining for engines that are `PipelineCapable`.
   - Engines not yet native are `xfail(strict=True)` with the owning chapter named.

## Public Seams

`vertebra.db.common.driver.*`, `vertebra.db.adapters.<engine>.*`, `BaseDatabaseConfig.from_url/pool_key`, and `provide_driver`.

## Dependencies

integration-remediation `critical-correctness-fixes`.

## Acceptance Boundary

- Imports resolve from the new paths, with no aliases left at the old paths.
- The registry is table-driven.
- The import gate passes.
- The conformance suite runs, with pending engines reported as `xfail`.
- `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 Python package move + public exports.
- t02 Rust module layout.
- t03 Driver contract + exception hierarchy.
- t04 Capability protocols.
- t05 Registry polymorphism + `provide_driver`.
- t06 Import gate.
- t07 Conformance suite scaffold.
