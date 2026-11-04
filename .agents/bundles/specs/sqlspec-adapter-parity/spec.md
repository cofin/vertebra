---
type: Spec
flow_id: sqlspec-adapter-parity
title: Feature-Complete SQLSpec Adapters
description: Restructure Vertebra SQLSpec adapters to mirror upstream adapter packages, implement the SQLSpec driver dispatch contract, subclass upstream ADK/events/Litestar stores and Sync/Async data dictionaries, add a temporary adapter-name resolver shim, and fix shared-pool, shim, and Spanner store defects.
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
updated_at: 2026-11-04T01:17:00Z
tags: [sqlspec, adapters, adk, events, litestar]
---

# Flow Charter: Feature-Complete SQLSpec Adapters

## Charter

Vertebra's SQLSpec adapters must be feature-complete in the same way upstream adapters are: driver dispatch contract, data dictionary, ADK session/memory/artifact stores, events queue store, Litestar session store, Arrow, and pipelining through `execute_stack`. They run on Vertebra's Rust-accelerated drivers under `vertebra/db/adapters/<engine>/` while preserving upstream `sqlspec` naming (`<Engine>SyncConfig` / `<Engine>AsyncConfig`, `<Engine>SyncDriver` / `<Engine>AsyncDriver`, `BigQuerySyncDataDictionary` / `BigQueryAsyncDataDictionary`, `SpannerSyncDataDictionary` / `SpannerAsyncDataDictionary`, `SQLSpecSyncProvider` / `SQLSpecAsyncProvider`). Schemas match upstream so `SQLSpecSessionService`, events channels, and extension migrations work unchanged.

## Scope

1. **Layout.** Create `extensions/sqlspec/adapters/<engine>/{__init__,config,driver,core,data_dictionary}.py` plus `adk/`, `events/`, and `litestar/` store modules for `postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, and `spanner`. Generic modules (`plugin.py`, `provider.py`, `proxy.py`, `storage.py`, `events.py`) hold zero engine code, removing the delegating `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` wrapper (whose SQL builder relocated onto `DuckDBCore.build_gcs_secret_sql` in `native-embedded-and-cache-drivers:t02`).
2. **Driver dispatch contract.**
   - Shared bases in `extensions/sqlspec/_driver.py` implement `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `with_cursor`, exception mapping over `DatabaseError`, and `SQLResult`, removing the `execute`/`select`/`select_one` overrides in `adapters.py::BaseSyncDriver`/`BaseAsyncDriver`.
   - Preserve `**kwargs` parameters across dispatch hooks, and carry `SyncPipelineCapable.pipeline()` / `AsyncPipelineCapable.pipeline()` dispatch (wired in `db-engine-primitives:t06`) into `execute_stack` with upstream SQLSpec fallback when `isinstance(self.native_driver, SyncPipelineCapable)` / `isinstance(self.native_driver, AsyncPipelineCapable)` is `False`.
   - Duplicate any dialect-specific SQL/parameter helpers inside each `extensions/sqlspec/adapters/<engine>/core.py` (never create cross-adapter helper modules).
3. **Data dictionaries.** Each adapter's `data_dictionary` returns an upstream-compatible `<Engine>SyncDataDictionary` (`SyncDataDictionaryBase`) / `<Engine>AsyncDataDictionary` (`AsyncDataDictionaryBase`) implementation reusing upstream per-engine metadata SQL.
4. **Stores.**
   - ADK, memory, artifact, events, and Litestar stores subclass upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, `Base*ADKMemoryStore`, `Base*ADKArtifactStore`, `BaseEventQueueStore`, `BaseSQLSpecStore`) with upstream DDL, overriding only SDK-touching hooks to execute through the Vertebra driver.
   - BigQuery memory search uses `VECTOR_SEARCH` / `SEARCH`.
   - Spanner stores use a stored `MOD(FARM_FINGERPRINT(...))` shard column, `BYTES` values, no invalid TTL on `updated_at`, and dialect-aware GoogleSQL / Spangres parameters.
   - Async stores execute natively via async driver methods.
5. **Resolver shim.**
   - Configs declare `adapter_name: ClassVar[str]` and explicit store classes.
   - `extensions/sqlspec/_compat.py` wraps upstream resolvers so they honor `adapter_name`/MRO (`events._hints.resolve_adapter_name`, `adk._config_utils`, and Litestar/events migration store lookups), installed idempotently by `SQLSpecPlugin`.
   - A pinning test asserts upstream still lacks native resolution so `_compat.py` is deleted as soon as [sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876) ships.
6. **Defect fixes.**
   - `BigQueryConnectionShim.query` honors `job_config.query_parameters`, and `BigQueryAsyncConfig.create_connection` reuses a single driver.
   - `SyncAdapterConfig.close` / `AsyncAdapterConfig.close` release `DatabaseRegistry`-shared drivers via reference counting instead of closing pools held by sibling configs, and `provide_session` yields isolated per-session transaction state.
   - Remove `plugin.py::_inject_sqlspec_pool_defaults`, the `spanner.py` -> `vertebra.db.adapters.bigquery.ObjectRef` cross-engine import, duplicate `_cosine_similarity`, duplicate `connection_instance` assignment, sync/async `resolve_*_config` duplication, and standalone `_hydrate_*` functions (encapsulating them on their owning classes).
7. **Broker Arrow frames.** Replace `extensions/sqlspec/provider.py::{SharedArrowBuffer,SharedArrowTable}` (`SQLSpecSyncProvider` / `SQLSpecAsyncProvider`) with the Rust `VTBARROW` SHM writer/reader (`ArrowCursor`), tying SHM unlink strictly to lease lifecycle.

## Out of Scope

- Queue backends (Chapter 5 `queues-tasks-and-channels`), SQLAlchemy/Django (Chapter 6 `sqlalchemy-and-django-integration`), `ObjectStoreProtocol` streaming parity (`native-data-platform/storage-completeness`), and upstream `sqlspec` repository changes ([sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876)).

## Public Seams

`vertebra.extensions.sqlspec.adapters.<engine>.{<Engine>SyncConfig,<Engine>AsyncConfig}`, `SQLSpecSyncProvider`, `SQLSpecAsyncProvider`, `adapter_name`, and `SQLSpecPlugin`.

## Dependencies

`native-data-platform` Chapters 1–6 (`db-layout-and-contract`, `native-postgres-driver`, `broker-pool-sharing`, `native-oracle-driver`, `native-mysql-mssql-drivers`, and `native-embedded-and-cache-drivers` on `refactor/sync-async-names @ 08ac03a`) and `integration-remediation` `db-engine-primitives`.

## Acceptance Boundary

- Upstream store contract tests pass against Vertebra configs across supported engines.
- `SQLSpecSessionService` and extension migrations run cleanly on Vertebra BigQuery and Spanner configs.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Draft Task Outline

- t01 Adapter package layout, `build_duckdb_gcs_secret_sql` wrapper removal, and generic-module cleanup.
- t02 Dispatch-contract driver bases and `execute_stack` `SyncPipelineCapable` / `AsyncPipelineCapable` integration.
- t03 Per-engine `<Engine>SyncDataDictionary` / `<Engine>AsyncDataDictionary` implementations.
- t04 `_compat.py` resolver shim + `adapter_name` + pinning test.
- t05 BigQuery stores on upstream bases + connection shim fixes.
- t06 Spanner stores on upstream bases + DDL/parameter fixes.
- t07 Remaining engine stores + `DatabaseRegistry` shared-pool refcount.
- t08 Rust `VTBARROW` replacement for `provider.py::{SharedArrowBuffer,SharedArrowTable}` + docs.
