---
type: Spec
flow_id: sqlspec-adapter-parity
title: "Feature-Complete SQLSpec Adapters"
description: "Deliver full-parity per-engine SQLSpec adapter packages across all 8 engines on sqlspec>=0.71.1 without MyPyC monkeypatching or cross-adapter shared SQL helpers, fixing MySQL qmark binds and Spanner sqlglot dialects, cutting provider.py over to VTBARROW ArrowCursor, and adding vertebra.storage.ArtifactReference."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-sqlspec-core-driver-and-artifact-ref
  - ch02-sqlspec-postgres
  - ch03-sqlspec-sqlite
  - ch04-sqlspec-mysql
  - ch05-sqlspec-oracle
  - ch06-sqlspec-mssql
  - ch07-sqlspec-duckdb
  - ch08-sqlspec-bigquery
  - ch09-sqlspec-spanner
  - ch10-sqlspec-cutover-storage-events-docs
current_chapter: ch01-sqlspec-core-driver-and-artifact-ref
current_task: "sqlspec-adapter-parity:ch01:t01"
parent_prd: integration-remediation
depends_on_flows:
  - broker-pool-sharing
  - native-sqlite-driver
  - native-redis-driver
plan_revision: 3
plan_commit: 18baadcd
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-12T23:50:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:50:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:50:00Z"
    commit: 18baadcd
tags:
  - sqlspec
  - adapters
  - adk
  - events
  - litestar
sources:
  - id: pyproject-sqlspec
    resource: pyproject.toml
    title: "sqlspec floor and optional adk extra"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Monolithic SQLSpec adapter definitions slated for per-engine split"
  - id: sqlspec-bq
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: "Current BigQuery SQLSpec module"
  - id: sqlspec-spn
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: "Current Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import"
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py
    title: "SQLSpecSyncProvider and SQLSpecAsyncProvider slated for VTBARROW ArrowCursor cutover"
  - id: sqlspec-plugin
    resource: src/py/vertebra/extensions/sqlspec/plugin.py
    title: "SQLSpecPlugin lifecycle and session configuration"
  - id: storage-store
    resource: src/py/vertebra/storage/_store.py
    title: "Native storage metadata structs where ArtifactReference is defined"
---

# Specification: Feature-Complete SQLSpec Adapters

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/sqlspec/adapters.py` overrides `execute`/`select`, sets MySQL execution binds to `%s` instead of `?`, lacks relational/DuckDB data dictionaries, defines `SharedArrowBuffer`/`SharedArrowTable` in `provider.py`, and sets invalid `sqlglot` dialect names (`"googlesql"`/`"spangres"`) plus a cross-engine BigQuery `ObjectRef` import in `spanner.py` [^sqlspec-adapters] [^sqlspec-spn] [^sqlspec-provider].
- **Zero Monkeypatching on `sqlspec>=0.71.1`**: `[project.optional-dependencies].sqlspec` bumps to `sqlspec>=0.71.1` with optional `adk = ["google-adk>=1.0.0"]`; `_compat.py` does NOT exist and never patches MyPyC-compiled `sqlspec` functions—each config declares `adapter_name = "vertebra_<engine>"` and explicit `adk_store_class` / `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` attributes [^pyproject-sqlspec].
- **Zero Cross-Adapter Shared SQL Helpers**: Per-engine packages `extensions/sqlspec/adapters/{postgres,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner}/` duplicate adapter-family SQL/parameter helpers strictly inside each adapter's own `core.py`.
- **`VTBARROW` & `ArtifactReference`**: `provider.py` cuts over to `VTBARROW` `ArrowCursor` frames, and offloaded ADK artifacts share `vertebra.storage.ArtifactReference` (`storage/_store.py`) while native BigQuery Object Tables retain `ObjectRef` [^sqlspec-bq] [^storage-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Vertebra's SQLSpec extension must provide 1:1 feature parity with upstream `sqlspec>=0.71.1` adapters across all 8 SQL/analytical engines (`postgres`, `sqlite`, `mysql`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`) while executing exclusively on Vertebra's Rust-backed drivers [^sqlspec-adapters] [^sqlspec-plugin].

Primary-source verification against `sqlspec==0.71.1` (MyPyC-compiled) confirms:
1. Upstream `resolve_adapter_name` and `resolve_extension_store_class` natively honor `adapter_name = "vertebra_<engine>"` and explicit `*_store_class` attributes on config classes—so `_compat.py` monkeypatching of C-compiled `sqlspec` functions is both unnecessary and ineffective.
2. `MySQLSyncDriver`/`MySQLAsyncDriver` currently emit `%s` (`POSITIONAL_PYFORMAT`), breaking `vertebra._mysql` binary `?` prepared statements; `spanner.py` sets `driver.dialect` to `"googlesql"` / `"spangres"`, which fail `sqlglot` parsing unless mapped to `"spanner"` and `"postgres"`.
3. `google.adk` is imported eagerly by `sqlspec.extensions.adk.{memory,artifact}`, requiring an optional `adk = ["google-adk>=1.0.0"]` extra and PEP 562 lazy loading in every adapter `__init__.py`.

**Non-Goals**: First-party queues and `litestar-queues` backends (`queues-tasks-and-channels`), SQLAlchemy/Django DB backends (`sqlalchemy-and-django-integration`), and object-storage multipart checksums (`storage-integrity-and-streaming`).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (`sqlspec>=0.71.1`, `adk` Extra & Zero Monkeypatching)**: Bump `sqlspec>=0.71.1`, add `adk = ["google-adk>=1.0.0"]`, forbid `_compat.py`, and declare `adapter_name = "vertebra_<engine>"` plus explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` attributes on every `<Engine>SyncConfig` / `<Engine>AsyncConfig` (and `SpangresSyncConfig` / `SpangresAsyncConfig`).
- **R2 (`_driver.py` Dispatch Contract & Parameter Fidelity)**: Inherit `execute`/`select*` and `@final dispatch_statement_execution` from `SyncDriverAdapterBase`/`AsyncDriverAdapterBase`; implement `dispatch_execute*`, `query_with_columns` (preserving columns on zero-row `SELECT`/`RETURNING`), `execute_stack` over `SyncPipelineCapable`/`AsyncPipelineCapable`, non-mutating `StatementConfig` merge, `TypedParameter` unwrapping, `DatabaseError` -> `sqlspec.exceptions.*` mapping, and reference-counted `DatabaseRegistry` pool `close()`.
- **R3 (Engine Placeholder & Dialect Fixes, Zero Shared Helpers)**: PostgreSQL `$1` (`NUMERIC`), SQLite `?`/`:name`, MySQL `?` (`ParameterStyle.QMARK`), Oracle `:name`/`:1`, MSSQL `? -> @P1` (`dialect="tsql"`), DuckDB `?`/`$1` (2.0+ lexical `COPY` order + `fetchall()` for Arrow-divergent types), BigQuery `@name` (`NAMED_AT`), Spanner GoogleSQL `dialect="spanner"` (`@name`) and Spangres `dialect="postgres"` (`$1 -> p1`). Duplicate adapter-family helpers inside each `adapters/<engine>/core.py` (zero cross-adapter SQL helper modules).
- **R4 (`SyncDataDictionaryBase` / `AsyncDataDictionaryBase`)**: Implement `<Engine>SyncDataDictionary` and `<Engine>AsyncDataDictionary` across all 8 engines reusing upstream `sqlspec/data_dictionary/dialects/<engine>/sql/*.sql`.
- **R5 (SDK-Free ADK, Events & Litestar Stores + `ArtifactReference`)**: Subclass `BaseSyncADKStore`/`BaseAsyncADKStore`, `Base*ADKMemoryStore`, `Base*ADKArtifactStore`, `BaseEventQueueStore`, and `BaseSQLSpecStore` with zero third-party DB SDK imports; define `vertebra.storage.ArtifactReference` (`uri`, `version`, `content_type`, `size`, `updated`, `signed_url`) for shared artifact surfaces while retaining `ObjectRef` on native BigQuery Object Tables (zero BigQuery imports in Spanner).
- **R6–R8 (`VTBARROW` Provider, `ObjectStoreBase`, `ChangeFeed*EventBackend` & Clean Cutover)**: Replace `SharedArrowBuffer`/`SharedArrowTable` with `VTBARROW` `ArrowCursor`, remove `gc=False` on container structs (`BrokerRpcRequest`, `BrokerRpcResponse`, `ObjectMeta`, `PutResult`), subclass `ObjectStoreBase` in `extensions/sqlspec/storage.py`, checkpoint/spill in `extensions/sqlspec/events.py`, delete legacy `adapters.py`/`bigquery.py`/`spanner.py`, and sync docs/OKF.

## 3. Documentation & OKF Knowledge Plan

- Public documentation (`ch10:t04`): `docs/usage/extensions.md` and `docs/reference/extensions.md` (`pymdownx.tabbed` install tabs, 4–6 node conceptual Mermaid diagrams).
- OKF knowledge chapters (`ch10:t04`, each `<= 200` lines): [sqlspec-alchemy-and-dialects.md](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md) and [database-driver-limitations.md](../../knowledge/domains/data/database-driver-limitations.md).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`, `R2`, `R6` Core `_driver.py`, `VTBARROW` & `ArtifactReference` | `ch01` (`t01`–`t03`) | `pyproject.toml`, `db/driver/{_sync,_async}.py`, `extensions/sqlspec/{_driver,provider}.py`, `storage/_store.py` | `pytest test_sqlspec_shm_proxy.py` |
| `R3`–`R5` PostgreSQL & SQLite adapter packages | `ch02`–`ch03` (`6` tasks) | `extensions/sqlspec/adapters/{postgres,sqlite}/*` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R3`–`R5` MySQL (`?` fix), Oracle & MSSQL packages | `ch04`–`ch06` (`9` tasks) | `extensions/sqlspec/adapters/{mysql,oracle,mssql}/*` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R3`–`R5` DuckDB, BigQuery & Spanner (`sqlglot` fix) | `ch07`–`ch09` (`9` tasks) | `extensions/sqlspec/adapters/{duckdb,bigquery,spanner}/*` | `pytest test_sqlspec_{bigquery,spanner}.py` |
| `R7`–`R8` `ObjectStoreBase`, CDC events, cutover & docs | `ch10` (`t01`–`t04`) | `extensions/sqlspec/{storage,events,plugin,__init__}.py`, `docs/`, `sqlspec-alchemy-and-dialects.md` | `review_setup.py validate` |

## 5. Implementation Plan

- [ ] [ch01-sqlspec-core-driver-and-artifact-ref](chapters/ch01-sqlspec-core-driver-and-artifact-ref/spec.md) — Bump `sqlspec>=0.71.1` + `adk` extra, implement `_driver.py` dispatch bases, `VTBARROW` `provider.py` cutover, and `vertebra.storage.ArtifactReference`.
- [ ] [ch02-sqlspec-postgres](chapters/ch02-sqlspec-postgres/spec.md) — Deliver `adapters/postgres/` (`$1` `NUMERIC`, Arrow `COPY`, portals, `DataDictionary`, ADK/Events/Litestar stores, `adapter_name='vertebra_postgres'`).
- [ ] [ch03-sqlspec-sqlite](chapters/ch03-sqlspec-sqlite/spec.md) — Deliver `adapters/sqlite/` (`QMARK` + `NAMED_COLON`, WAL pragmas, `DataDictionary`, `FTS5` ADK/Events/Litestar stores, `adapter_name='vertebra_sqlite'`).
- [ ] [ch04-sqlspec-mysql](chapters/ch04-sqlspec-mysql/spec.md) — Deliver `adapters/mysql/` (fixing `%s` -> `ParameterStyle.QMARK` `?`, MariaDB/MySQL `DataDictionary`, ADK/Events/Litestar stores, `adapter_name='vertebra_mysql'`).
- [ ] [ch05-sqlspec-oracle](chapters/ch05-sqlspec-oracle/spec.md) — Deliver `adapters/oracle/` (`:name`/`:1` binds, 23ai `VECTOR`/`OSON` `DataDictionary`, TxEventQ Events/ADK/Litestar stores, `adapter_name='vertebra_oracle'`).
- [ ] [ch06-sqlspec-mssql](chapters/ch06-sqlspec-mssql/spec.md) — Deliver `adapters/mssql/` (`dialect='tsql'`, `? -> @P1`, `MERGE HOLDLOCK` ADK/Events/Litestar stores, `DataDictionary`, `adapter_name='vertebra_mssql'`).
- [ ] [ch07-sqlspec-duckdb](chapters/ch07-sqlspec-duckdb/spec.md) — Deliver `adapters/duckdb/` (DuckDB 2.0+ `COPY` ordering, Arrow-divergent `fetchall()` fidelity, `DataDictionary`, ADK/Events/Litestar stores, `adapter_name='vertebra_duckdb'`).
- [ ] [ch08-sqlspec-bigquery](chapters/ch08-sqlspec-bigquery/spec.md) — Deliver `adapters/bigquery/` (`@name` binds, unsplit `BEGIN...END`, `VECTOR_SEARCH` ADK + `ArtifactReference`, `DataDictionary`, `adapter_name='vertebra_bigquery'`).
- [ ] [ch09-sqlspec-spanner](chapters/ch09-sqlspec-spanner/spec.md) — Deliver `adapters/spanner/` (`sqlglot` `'spanner'`/`'postgres'` dialect fix, ScaNN/RRF ADK + `ArtifactReference`, sharded stores, `adapter_name='vertebra_spanner'`).
- [ ] [ch10-sqlspec-cutover-storage-events-docs](chapters/ch10-sqlspec-cutover-storage-events-docs/spec.md) — Upgrade `ObjectStoreBackend(ObjectStoreBase)`, harden `ChangeFeed*EventBackend`, delete `{adapters,bigquery,spanner}.py`, and sync docs/OKF.

## 6. Current State & Remaining Plan Snapshot

- **Current Frontier**: `state: planned` at `ch01-sqlspec-core-driver-and-artifact-ref` (`sqlspec-adapter-parity:ch01:t01`, `0/31` tasks completed across `10` chapters; `ch02`–`ch09` are mutually independent after `ch01`).
- **Upstream Flow Gates**: Requires `broker-pool-sharing` (pooled `Proxy*Driver` and `VTBARROW` lease lifecycles), `native-sqlite-driver`, and `native-redis-driver`.
- **Full `/flow:finish` Gate**: `make lint-all && make py-test && make docs`.

[^pyproject-sqlspec]: `pyproject.toml` — sqlspec floor and optional adk extra.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Monolithic SQLSpec adapter definitions slated for per-engine split.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current BigQuery SQLSpec module.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py` — SQLSpecSyncProvider and SQLSpecAsyncProvider slated for VTBARROW ArrowCursor cutover.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py` — SQLSpecPlugin lifecycle and session configuration.
[^storage-store]: `src/py/vertebra/storage/_store.py` — Native storage metadata structs where ArtifactReference is defined.
