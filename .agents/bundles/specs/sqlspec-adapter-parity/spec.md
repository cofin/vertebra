---
type: Spec
flow_id: sqlspec-adapter-parity
title: Feature-Complete SQLSpec Adapters
description: Deliver full-parity per-engine SQLSpec adapter packages across all 8 engines on sqlspec>=0.71.1 without MyPyC monkeypatching or cross-adapter shared SQL helpers, fixing MySQL qmark binds and Spanner sqlglot dialects, cutting provider.py over to VTBARROW ArrowCursor, and adding vertebra.storage.ArtifactReference.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-sqlspec-core-driver-and-artifact-ref
  - ch02-sqlspec-relational-adapters
  - ch03-sqlspec-enterprise-and-cloud-adapters
  - ch04-sqlspec-cutover-storage-events-docs
current_chapter: ch01-sqlspec-core-driver-and-artifact-ref
current_task: sqlspec-adapter-parity:ch01:t01
parent_prd: integration-remediation
depends_on_flows:
  - sqlalchemy-and-django-integration
plan_revision: 5
plan_commit: 473e1f2e
created_at: '2026-11-03T03:30:00Z'
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-15T20:55:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:55:00Z'
    commit: 246d7d0e
  - by: agent:flow-archive-realign
    at: '2026-11-15T17:52:00Z'
    commit: 246d7d0e
tags:
  - sqlspec
  - adapters
  - adk
  - events
  - litestar
sources:
  - id: pyproject-sqlspec
    resource: pyproject.toml
    title: sqlspec floor and optional adk extra
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: Monolithic SQLSpec adapter definitions slated for per-engine split
  - id: sqlspec-bq
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: Current BigQuery SQLSpec module
  - id: sqlspec-spn
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: Current Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py
    title: SQLSpecSyncProvider and SQLSpecAsyncProvider slated for VTBARROW ArrowCursor cutover
  - id: sqlspec-plugin
    resource: src/py/vertebra/extensions/sqlspec/plugin.py
    title: SQLSpecPlugin lifecycle and session configuration
  - id: storage-store
    resource: src/py/vertebra/storage/_store.py
    title: Native storage metadata structs where ArtifactReference is defined
---

# Specification: Feature-Complete SQLSpec Adapters

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `extensions/sqlspec/adapters.py` overrides `execute`/`select`, sets MySQL execution binds to `%s` instead of `?`, lacks relational/DuckDB data dictionaries, defines `SharedArrowBuffer`/`SharedArrowTable` in `provider.py`, and exposes `"googlesql"` (not a `sqlglot` dialect) as the Spanner driver/config `dialect` attribute while `_spanner_statement_config` already uses `"spanner"`/`"spangres"` (both registered by sqlspec's `sqlglot.dialects` entry points), plus a cross-engine BigQuery `ObjectRef` import in `spanner.py` [^sqlspec-adapters] [^sqlspec-spn] [^sqlspec-provider].
- **Zero Monkeypatching on `sqlspec>=0.71.1`**: `[project.optional-dependencies].sqlspec` bumps to `sqlspec>=0.71.1` with optional `adk = ["google-adk>=1.0.0"]`; `_compat.py` does NOT exist and never patches MyPyC-compiled `sqlspec` functions—each config declares `adapter_name = "vertebra_<engine>"` and explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class` attributes (with `<Engine>SyncADKArtifactStore` / `<Engine>AsyncADKArtifactStore` exported in `adk.py`) [^pyproject-sqlspec].
- **Zero Cross-Adapter Shared SQL Helpers**: Per-engine packages `extensions/sqlspec/adapters/{postgres,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner}/` duplicate adapter-family SQL/parameter helpers strictly inside each adapter's own `core.py`.
- **`VTBARROW` & `ArtifactReference`**: `provider.py` cuts over to `VTBARROW` `ArrowCursor` frames, and offloaded ADK artifacts share `vertebra.storage.ArtifactReference` (`storage/_store.py`) while native BigQuery Object Tables retain `ObjectRef` [^sqlspec-bq] [^storage-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Vertebra's SQLSpec extension must provide 1:1 feature parity with upstream `sqlspec>=0.71.1` adapters across all 8 SQL/analytical engines (`postgres`, `sqlite`, `mysql`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`) while executing exclusively on Vertebra's Rust-backed drivers [^sqlspec-adapters] [^sqlspec-plugin].

Primary-source verification against `sqlspec==0.71.1` (MyPyC-compiled) confirms:
1. Upstream `resolve_adapter_name` and `resolve_extension_store_class` natively honor `adapter_name = "vertebra_<engine>"` and explicit `*_store_class` attributes on config classes—so `_compat.py` monkeypatching of C-compiled `sqlspec` functions is both unnecessary and ineffective.
2. `MySQLSyncDriver`/`MySQLAsyncDriver` currently emit `%s` (`POSITIONAL_PYFORMAT`), breaking `vertebra._mysql` binary `?` prepared statements; `spanner.py` sets the driver/config `dialect` attribute to `"googlesql"`, which is not a `sqlglot` dialect, while `StatementConfig` already uses sqlspec's registered `"spanner"` and `"spangres"` dialects.
3. `google.adk` is imported eagerly by `sqlspec.extensions.adk.{memory,artifact}`, requiring an optional `adk = ["google-adk>=1.0.0"]` extra and PEP 562 lazy loading in every adapter `__init__.py`.

**Non-Goals**: First-party queues and `litestar-queues` backends (`queues-tasks-and-channels`), SQLAlchemy/Django DB backends (`sqlalchemy-and-django-integration`), and object-storage multipart checksums (`storage-integrity-and-streaming`).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (`sqlspec>=0.71.1`, `adk` Extra & Zero Monkeypatching)**: Bump `sqlspec>=0.71.1`, add `adk = ["google-adk>=1.0.0"]`, forbid `_compat.py`, and declare `adapter_name = "vertebra_<engine>"` plus explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class` attributes on every `<Engine>SyncConfig` / `<Engine>AsyncConfig` (and `SpangresSyncConfig` / `SpangresAsyncConfig`), exporting `*ADKArtifactStore` for `SQLSpecArtifactService`.
- **R2 (`_driver.py` Dispatch Contract & Parameter Fidelity)**: Inherit `execute`/`select*` and `@final dispatch_statement_execution` from `SyncDriverAdapterBase`/`AsyncDriverAdapterBase`; implement `dispatch_execute*`, `query_with_columns` (delegating to existing `execute_with_description` + `hydrate_rows` to preserve columns on zero-row `SELECT`/`RETURNING`), `execute_stack` over `SyncPipelineCapable`/`AsyncPipelineCapable`, non-mutating `StatementConfig` merge, `TypedParameter` unwrapping, `DatabaseError` -> `sqlspec.exceptions.*` mapping, and reference-counted `DatabaseRegistry` pool `close()`.
- **R3 (Engine Placeholder & Dialect Fixes, Zero Shared Helpers)**: PostgreSQL `$1` (`NUMERIC`), SQLite `?`/`:name`, MySQL `?` (`ParameterStyle.QMARK`), Oracle `:name`/`:1`, MSSQL `? -> @P1` (`dialect="tsql"`), DuckDB `?`/`$1` (2.0+ lexical `COPY` order + `fetchall()` for Arrow-divergent types), BigQuery `@name` (`NAMED_AT`), Spanner GoogleSQL `dialect="spanner"` (`@name`) and Spangres `dialect="spangres"` (sqlspec's registered `Spangres` dialect, `$1 -> p1`). Duplicate adapter-family helpers inside each `adapters/<engine>/core.py` (zero cross-adapter SQL helper modules).
- **R4 (`SyncDataDictionaryBase` / `AsyncDataDictionaryBase`)**: Add 12 `<Engine>SyncDataDictionary` / `<Engine>AsyncDataDictionary` classes for the six engines without one (PostgreSQL, SQLite, MySQL, Oracle, MSSQL, DuckDB) and rebase the 4 existing BigQuery/Spanner classes onto `SyncDataDictionaryBase`/`AsyncDataDictionaryBase`, reusing upstream `sqlspec/data_dictionary/dialects/<engine>/sql/*.sql`.
- **R5 (SDK-Free ADK, Events & Litestar Stores + `ArtifactReference`)**: Subclass `BaseSyncADKStore`/`BaseAsyncADKStore`, `Base*ADKMemoryStore`, `Base*ADKArtifactStore`, `BaseEventQueueStore`, and `BaseSQLSpecStore` with zero third-party DB SDK imports; define `vertebra.storage.ArtifactReference` (`uri`, `version`, `content_type`, `size`, `updated`, `signed_url`) for shared artifact surfaces while retaining `ObjectRef` on native BigQuery Object Tables (zero BigQuery imports in Spanner).
- **R6–R8 (`VTBARROW` Provider, `ObjectStoreBase`, `ChangeFeed*EventBackend` & Clean Cutover)**: Replace `SharedArrowBuffer`/`SharedArrowTable` with `VTBARROW` `ArrowCursor`, remove `gc=False` on container structs (`ObjectMeta`, `PutResult`; `BrokerRpcRequest`/`BrokerRpcResponse` already omit it), rebase `ObjectStoreBackend` onto `ObjectStoreBase` in `extensions/sqlspec/storage.py`, checkpoint/spill in `extensions/sqlspec/events.py`, delete legacy `adapters.py`/`bigquery.py`/`spanner.py`, and sync docs/OKF.

## 3. Documentation & OKF Knowledge Plan

- Public documentation (`ch04:t04`): `docs/usage/extensions.md` and `docs/reference/extensions.md` (`pymdownx.tabbed` install tabs, 4–6 node conceptual Mermaid diagrams).
- OKF knowledge chapters (`ch04:t04`, each `<= 200` lines): [sqlspec-alchemy-and-dialects.md](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md) and [database-driver-limitations.md](../../knowledge/domains/data/database-driver-limitations.md).

## 4. Requirement-to-Task Traceability Matrix

| Chapter | Title | Tasks | Verification |
| --- | --- | --- | --- |
| `ch01` | Chapter 01: SQLSpec Dispatch Foundation, VTBARROW Provider & Storage ArtifactReference | `3` tasks | `pytest test_native_db_drivers.py` |
| `ch02` | Chapter 02: Per-Engine SQLSpec Adapter Packages for PostgreSQL, SQLite, MySQL, and Oracle | `4` tasks | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `ch03` | Chapter 03: Per-Engine SQLSpec Adapter Packages for SQL Server, DuckDB, BigQuery, and Spanner | `4` tasks | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `ch04` | Chapter 04: ObjectStoreBase Parity, ChangeFeed Event Backends, Legacy Cutover & Docs/OKF | `4` tasks | `pytest test_data_storage_integrations.py` |

## 5. Implementation Plan

- [ ] [ch01-sqlspec-core-driver-and-artifact-ref](chapters/ch01-sqlspec-core-driver-and-artifact-ref/spec.md) — Bump sqlspec>=0.71.1 + adk extra, implement MyPyC-safe BaseSyncDriver / BaseAsyncDriver dispatch in _driver.py, cut provider.py over to VTBARROW ArrowCursor, and define vertebra.storage.ArtifactReference.
- [ ] [ch02-sqlspec-relational-adapters](chapters/ch02-sqlspec-relational-adapters/spec.md) — Deliver feature-complete per-engine SQLSpec packages (core, sync/async drivers, DataDictionary, ADK/Events/Litestar stores, and explicit adapter_name configs) for PostgreSQL, SQLite, MySQL (fixing ? qmark binds), and Oracle.
- [ ] [ch03-sqlspec-enterprise-and-cloud-adapters](chapters/ch03-sqlspec-enterprise-and-cloud-adapters/spec.md) — Deliver feature-complete per-engine SQLSpec packages for SQL Server, DuckDB, BigQuery, and Spanner (fixing sqlglot 'spanner'/'postgres' dialect names and removing cross-engine BigQuery ObjectRef imports).
- [ ] [ch04-sqlspec-cutover-storage-events-docs](chapters/ch04-sqlspec-cutover-storage-events-docs/spec.md) — Upgrade ObjectStoreBackend(ObjectStoreBase), harden ChangeFeed*EventBackend with durable checkpoint/spill, delete monolithic {adapters,bigquery,spanner}.py, and synchronize docs and OKF chapters.

## 6. Current State & Remaining Plan Snapshot

- **Current Frontier**: `state: planned` at `ch01-sqlspec-core-driver-and-artifact-ref` (`sqlspec-adapter-parity:ch01:t01`, `0/15` tasks completed across `4` chapters; `ch02`–`ch03` are independent in scope after `ch01`; their tasks run serially because they share `test_sqlalchemy_and_sqlspec_vtb.py`, and `ch04` starts after both).
- **Upstream Flow Gates**: `broker-pool-sharing` (pooled `Proxy*Driver` and `VTBARROW` lease lifecycles) is complete and archived at `246d7d0e`; awaits `native-sqlite-driver`, `native-redis-driver` (`SyncNotificationSource`/`AsyncNotificationSource` names), and `sqlalchemy-and-django-integration` (shared `pyproject.toml`, `docs/usage/extensions.md`, `docs/reference/extensions.md`, `sqlspec-alchemy-and-dialects.md`, and `database-driver-limitations.md` edits).
- **Full `/flow:finish` Gate**: `make lint-all && make py-test && make docs`.

[^pyproject-sqlspec]: `pyproject.toml` — sqlspec floor and optional adk extra.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Monolithic SQLSpec adapter definitions slated for per-engine split.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current BigQuery SQLSpec module.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py` — SQLSpecSyncProvider and SQLSpecAsyncProvider slated for VTBARROW ArrowCursor cutover.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py` — SQLSpecPlugin lifecycle and session configuration.
[^storage-store]: `src/py/vertebra/storage/_store.py` — Native storage metadata structs where ArtifactReference is defined.
