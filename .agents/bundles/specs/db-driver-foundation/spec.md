---
type: Spec
flow_id: db-driver-foundation
title: Real Driver I/O, Native Pipelining, Registry and Capability Primitives
description: Make every vertebra.db driver talk to its real database, expose native psycopg and python-oracledb pipelining, replace registry isinstance chains with config polymorphism, define capability protocols, relocate engine code out of generic modules, and enforce the db import boundary.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-03T03:30:00Z
tags: [db, drivers, pipeline, registry, layering]
---

# Flow Charter: Real Driver I/O, Native Pipelining, Registry and Capability Primitives

## Charter

This flow makes `vertebra.db` the foundation the extensions build on. Every driver reaches its real database. Drivers expose native pipelining. Configs own their URL schemes, pool keys, and driver classes. Small engine-local primitives replace the engine code currently sitting in generic modules. `vertebra.db` imports nothing above itself.

## Scope

1. **Real driver I/O** (Open Question Q4; the recommended default is shown).
   - `db/oracle/driver.py::SyncOracleDriver`/`AsyncOracleDriver` execute through python-oracledb Thin (sync `Connection`, async `AsyncConnection`, pool support) instead of `sqlite3.connect(":memory:")`. They keep the Rust OSON/VECTOR/TTC piggyback codecs in `db/oracle/core.py`.
   - `db/mssql/driver.py::SyncMSSQLDriver`/`AsyncMSSQLDriver` execute through `mssql-python`.
   - `db/duckdb/driver.py::SyncDuckDBDriver` requires `duckdb` (raising `MissingDependencyError`) and has no SQLite fallback.
   - `db/postgres/driver.py`:
     - Sync and async use psycopg 3 (`psycopg_pool.ConnectionPool` / `AsyncConnectionPool`).
     - `AsyncPostgreSQLDriver._ensure_async_pool` returns a pool, not a module.
     - The `memory://` / `_in_memory_rows` branches are removed.
   - `db/mysql/driver.py` loses its `memory://` / `_in_memory_rows` branches. `db/redis/driver.py` loses its `memory://` mode.
2. **Native pipelining.**
   - `vertebra/db/driver/` gains a `PipelineCapable` protocol: `pipeline()` returns a context manager that queues `execute(sql, params)` / `fetch_all(sql, params)` operations and returns results in order. It has sync and async forms.
   - Postgres implements it with psycopg pipeline mode.
   - Oracle implements it with `oracledb.create_pipeline()` + `AsyncConnection.run_pipeline()`. Against servers older than 23ai the operations run sequentially; the sync form runs the same operation list sequentially and documents that.
   - Delete `db/pipeline.py` (`QueryPipeline`, `CTEStep`, `RelOverlay`) and its exports from `db/__init__.py`.
   - Delete `docs/usage/query-pipelines.md` and replace the references in `docs/usage/databases.md`, `docs/usage/extensions.md`, `docs/index.md`, and `docs/reference/index.md` with the pipeline API.
3. **Registry polymorphism.**
   - `db/driver/_common.py::BaseDatabaseConfig` gains `url_schemes: ClassVar[tuple[str, ...]]`, `from_url(url, **overrides)`, `pool_key()`, and `driver_classes: ClassVar[tuple[type, type]]`. Each engine config implements them.
   - `db/_registry.py::DatabaseRegistry.parse_url`, `pool_fingerprint`, `_build_async_driver`, and `_build_sync_driver` become lookups over the registered config classes.
   - `provide_bigquery_driver`/`provide_spanner_driver` are replaced by `provide_driver(name_or_config, *, sync=False)`. The re-exports in `extensions/litestar/__init__.py` are updated.
4. **Capability protocols.** `vertebra/db/driver/` defines `NotificationSource`, `ChangeSource`, `QueueStore`, and `EmbeddingPushdown` as `Protocol`s with msgspec record types. Chapter 5 implements `QueueStore`; this flow implements the rest.
5. **Relocate engine code.**
   - `db/cdc.py` keeps only the generic `ChangeEvent`.
   - Postgres `pg_logical_emit_message`, the slot/publication settings, and NOTIFY spill move to `db/postgres/notify.py`.
   - The Spanner change-stream classes move to `db/spanner/change_stream.py`, implementing `ChangeSource`.
   - Oracle TxEventQ moves to `db/oracle/notify.py`, and Redis/Valkey pub/sub to `db/redis/notify.py`.
   - `db/channels.py::DatabaseChannelBackend` is deleted. Chapter 5 adds the per-engine Litestar channel backends.
   - The pushdown SQL builders in `embeddings/_bigquery.py`, `_spanner.py`, and the Oracle/AlloyDB parts of `_google.py` move to `db/<engine>/embeddings.py`, implementing `EmbeddingPushdown`. `embeddings/` keeps the generic `Embedder`, the remote/local embedders, and the broker.
   - `db/mcp_tools.py` moves to `extensions/` as BigQuery/Spanner `ToolProvider` modules (the protocol comes from Chapter 7). Until Chapter 7 lands, it stays as one module under `vertebra.agent`, with no `vertebra.db` dependency on agent/framework.
6. **Import boundary.** A test in the canonical layout test module asserts that no module under `vertebra/db/` imports `vertebra.agent`, `vertebra.framework`, `vertebra.extensions`, or `vertebra.server`.

## Out of Scope

- sqlspec adapter rework (Chapter 4), queue stores (Chapter 5), and Rust module splits (Chapter 8).

## Public Seams

`vertebra.db.driver.{PipelineCapable,NotificationSource,ChangeSource,QueueStore,EmbeddingPushdown}`, `BaseDatabaseConfig.from_url/pool_key/url_schemes/driver_classes`, `driver.pipeline()`, and `provide_driver`.

## Dependencies

Chapters 1 and 2.

## Acceptance Boundary

- `pytest-databases` integration tests for Postgres, Oracle, MSSQL, MySQL, and DuckDB hit real servers.
- A pipeline test on Postgres shows one network flush per pipeline.
- The registry is table-driven, the import gate passes, and `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 Registry polymorphism + `provide_driver`.
- t02 Capability protocols + import gate.
- t03 Postgres psycopg 3 sync/async, fakes removed.
- t04 Oracle python-oracledb Thin sync/async.
- t05 MSSQL `mssql-python`, DuckDB required, MySQL/Redis fakes removed.
- t06 `driver.pipeline()` on Postgres/Oracle, `QueryPipeline` deleted, docs updated.
- t07 CDC/notify relocation and `DatabaseChannelBackend` removal.
- t08 Embedding pushdown and DB tool relocation.
