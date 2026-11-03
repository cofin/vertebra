---
type: Spec
flow_id: sqlspec-adapter-parity
title: Feature-Complete SQLSpec Adapters
description: Restructure vertebra sqlspec adapters to mirror upstream adapter packages, implement the sqlspec driver dispatch contract, subclass upstream ADK/events/litestar stores and data dictionaries, add a temporary adapter-name resolver shim, and fix shared-pool, shim, and Spanner store defects.
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
tags: [sqlspec, adapters, adk, events, litestar]
---

# Flow Charter: Feature-Complete SQLSpec Adapters

## Charter

Vertebra's sqlspec adapters must be feature-complete in the same way upstream adapters are: driver, data dictionary, ADK session/memory/artifact stores, events queue store, Litestar session store, Arrow, and pipelining through `execute_stack`. They run on vertebra's Rust-accelerated drivers. Schemas match upstream, so `SQLSpecSessionService`, the events channel, and the extension migrations work unchanged.

## Scope

1. **Layout.** Create `extensions/sqlspec/adapters/<engine>/{__init__,config,driver,core,data_dictionary}.py` plus `adk/`, `events/`, and `litestar/` store modules for postgres, mysql, sqlite, oracle, mssql, duckdb, bigquery, and spanner. Generic modules (`plugin.py`, `provider.py`, `proxy.py`, `storage.py`, `events.py`) hold no engine code. `storage.py::build_duckdb_gcs_secret_sql` moves to the duckdb adapter `core.py`.
2. **Driver contract.**
   - Shared bases in `extensions/sqlspec/_driver.py` implement `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `with_cursor`, exception mapping, and `SQLResult`. The `execute`/`select`/`select_one` overrides in `adapters.py::BaseSyncDriver`/`BaseAsyncDriver` are removed.
   - `**kwargs` parameters are honored.
   - `execute_stack` uses `driver.pipeline()` where the engine is `PipelineCapable`, and falls back to the sqlspec default otherwise.
3. **Data dictionaries.** Each adapter's `data_dictionary` returns an upstream-compatible `SyncDataDictionaryBase`/`AsyncDataDictionaryBase` implementation, reusing the upstream per-engine dictionary SQL.
4. **Stores.**
   - ADK, memory, artifact, events, and Litestar stores subclass the upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, `Base*ADKMemoryStore`, `Base*ADKArtifactStore`, `BaseEventQueueStore`, `BaseSQLSpecStore`) with upstream DDL. Only the SDK-touching hooks are overridden, and they route through the vertebra driver.
   - BigQuery memory search uses `VECTOR_SEARCH`/`SEARCH`.
   - The Spanner stores use a stored `MOD(FARM_FINGERPRINT(...))` shard column, BYTES values, no TTL on `updated_at`, and dialect-aware parameters.
   - Async stores are natively async.
5. **Resolver shim.**
   - Configs declare `adapter_name: ClassVar[str]` and explicit store classes.
   - `extensions/sqlspec/_compat.py` wraps the upstream resolvers so they honor `adapter_name`/MRO: events `_hints.resolve_adapter_name`, the `adk/_config_utils` store lookups, and the litestar/events migration store lookups. `VertebraSQLSpecPlugin` installs it once.
   - A test asserts upstream still lacks the fix, so the shim is deleted when [sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876) ships.
6. **Defect fixes.**
   - `BigQueryConnectionShim.query` honors `job_config.query_parameters`.
   - `BigQueryAsyncConfig.create_connection` reuses a single driver.
   - `SyncAdapterConfig.close`/`AsyncAdapterConfig.close` release registry-shared drivers through a refcount.
   - `provide_session` yields per-session transaction state.
   - Remove `plugin.py::_inject_sqlspec_pool_defaults`, the `spanner.py`→`db.bigquery::ObjectRef` import, the duplicate `_cosine_similarity`, the duplicate `connection_instance` assignment, and the module-level `_hydrate_*` helper sprawl (moved onto owning classes).
   - Collapse the sync/async config copies (`resolve_*_config`) into shared code.
7. **Arrow.** `provider.py::SharedArrowBuffer`/`SharedArrowTable` are replaced by the Rust VTBARROW SHM writer/reader, with unlink tied to the lease lifecycle.

## Out of Scope

- Queue backends (Chapter 5), SQLAlchemy (Chapter 6), and upstream sqlspec code changes (tracked in #876).

## Public Seams

`vertebra.extensions.sqlspec.adapters.<engine>.{<Engine>SyncConfig,<Engine>AsyncConfig}`, `adapter_name`, and `VertebraSQLSpecPlugin`.

## Dependencies

native-data-platform `db-layout-and-contract`, `native-postgres-driver`, `broker-pool-sharing` (proxy configs), and `native-oracle-driver`; integration-remediation `db-engine-primitives`.

## Acceptance Boundary

- Upstream store contract tests run against the vertebra configs for every engine.
- `SQLSpecSessionService` works with the vertebra BigQuery and Spanner configs.
- Extension migrations run.
- `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 Adapter package layout and generic-module cleanup.
- t02 Dispatch-contract driver bases and `execute_stack` pipelining.
- t03 Data dictionaries.
- t04 Resolver shim + `adapter_name`.
- t05 BigQuery stores on upstream bases + shim fixes.
- t06 Spanner stores on upstream bases + DDL fixes.
- t07 Remaining engine stores + shared-pool refcount.
- t08 Rust VTBARROW for shared Arrow + cleanup.
