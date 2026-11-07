---
type: Spec
flow_id: sqlspec-adapter-parity
title: Feature-Complete SQLSpec Adapters
description: Restructure Vertebra SQLSpec adapters to mirror upstream adapter packages, implement the SQLSpec driver dispatch contract, subclass upstream ADK/events/Litestar stores and Sync/Async data dictionaries, add a temporary adapter-name resolver shim, and fix shared-pool, shim, and Spanner store defects.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: "2026-11-05T06:20:28Z"
tags: [sqlspec, adapters, adk, events, litestar]
---

# Flow Charter: Feature-Complete SQLSpec Adapters

## Charter

Vertebra's SQLSpec adapters must be feature-complete in the same way upstream adapters are: driver dispatch contract, data dictionary, ADK session/memory/artifact stores, events queue store, Litestar session store, Arrow, and pipelining through `execute_stack`. They run on Vertebra's Rust-accelerated drivers under `vertebra/db/adapters/<engine>/` while preserving upstream `sqlspec` naming (`<Engine>SyncConfig` / `<Engine>AsyncConfig`, `<Engine>SyncDriver` / `<Engine>AsyncDriver`, `BigQuerySyncDataDictionary` / `BigQueryAsyncDataDictionary`, `SpannerSyncDataDictionary` / `SpannerAsyncDataDictionary`, `SQLSpecSyncProvider` / `SQLSpecAsyncProvider`). SQLSpec driver classes adapt SQLSpec dispatch and result contracts; native transport, engine configuration, SQL embedding pushdown, notifications, and change-stream primitives remain in `vertebra.db.adapters.<engine>`. Schemas match upstream so `SQLSpecSessionService`, events channels, and extension migrations work unchanged.

## Scope

1. **Layout and dependency ownership.** Create `extensions/sqlspec/adapters/<engine>/{__init__,config,driver,core,data_dictionary}.py` plus `adk/`, `events/`, and `litestar/` store modules for `postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, and `spanner`. SQLSpec adapter `config.py` owns SQLSpec configuration and store declarations; `driver.py` owns SQLSpec dispatch and result adaptation; `data_dictionary.py` owns SQLSpec metadata contracts; adapter-local `core.py` owns only SQLSpec-specific dialect/parameter translation. Native driver/config classes and reusable engine primitives remain in `db/adapters/<engine>/`; SQLSpec wrappers call them instead of copying their behavior. Generic modules (`plugin.py`, `provider.py`, `proxy.py`, `storage.py`, `events.py`) hold zero engine code, removing the delegating `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` wrapper (whose SQL builder relocated onto `DuckDBCore.build_gcs_secret_sql` in `native-duckdb-driver:t02`).
2. **Driver dispatch contract.**
   - Shared bases in `extensions/sqlspec/_driver.py` implement `dispatch_execute`, `dispatch_execute_many`, `dispatch_execute_script`, `with_cursor`, exception mapping over `DatabaseError`, and `SQLResult`, removing the `execute`/`select`/`select_one` overrides in `adapters.py::BaseSyncDriver`/`BaseAsyncDriver`.
   - Preserve `**kwargs` parameters across dispatch hooks, and carry `SyncPipelineCapable.pipeline()` / `AsyncPipelineCapable.pipeline()` dispatch (wired in `db-engine-primitives:t06`) into `execute_stack` with upstream SQLSpec fallback when `isinstance(self.native_driver, SyncPipelineCapable)` / `isinstance(self.native_driver, AsyncPipelineCapable)` is `False`.
   - Keep SQLSpec-specific dialect SQL/parameter helpers inside each `extensions/sqlspec/adapters/<engine>/core.py` (never create cross-adapter helper modules). Reuse existing native adapter primitives for native engine operations; this rule does not duplicate native wire, configuration, embedding, notification, or storage behavior inside the extension.
3. **Data dictionaries.** Each adapter's `data_dictionary` returns an upstream-compatible `<Engine>SyncDataDictionary` (`SyncDataDictionaryBase`) / `<Engine>AsyncDataDictionary` (`AsyncDataDictionaryBase`) implementation reusing upstream per-engine metadata SQL.
4. **Stores.**
   - ADK, memory, artifact, events, and Litestar stores subclass upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, `Base*ADKMemoryStore`, `Base*ADKArtifactStore`, `BaseEventQueueStore`, `BaseSQLSpecStore`) with upstream DDL, overriding only SDK-touching hooks to execute through the Vertebra driver.
   - BigQuery memory search uses `VECTOR_SEARCH` / `SEARCH`.
   - Spanner stores use a stored `MOD(FARM_FINGERPRINT(...))` shard column, `BYTES` values, no invalid TTL on `updated_at`, and dialect-aware GoogleSQL / Spangres parameters.
   - Async stores execute natively via async driver methods.
   - Shared artifact metadata uses `vertebra.storage.ArtifactReference` with the storage-neutral fields already shared by BigQuery and Spanner artifact stores: `uri`, `version`, `content_type`, `size`, `updated`, and `signed_url`. Define the immutable value alongside `ObjectMeta` in `storage/_store.py` and export it from `storage/__init__.py`. `ObjectMeta` keeps its existing backend metadata contract; it lacks the artifact URI, content type, and signed-URL semantics. `ArtifactReference` contains no SQL, engine configuration, SQLSpec dependency, or object-store I/O.
   - Retain the engine-specific `vertebra.db.adapters.bigquery.ObjectRef` for BigQuery Object Table results and their BigQuery semantics. BigQuery artifact stores explicitly translate native object-table results to `ArtifactReference` when needed; Spanner artifact stores construct the storage value directly and never import a BigQuery adapter. Upstream store hooks keep their required upstream return shapes; use `ArtifactReference` only for the extension's shared artifact-reference surface.
5. **Resolver shim.**
   - Configs declare `adapter_name: ClassVar[str]` and explicit store classes.
   - `extensions/sqlspec/_compat.py` wraps upstream resolvers so they honor `adapter_name`/MRO (`events._hints.resolve_adapter_name`, `adk._config_utils`, and Litestar/events migration store lookups), installed idempotently by `SQLSpecPlugin`.
   - A pinning test asserts upstream still lacks native resolution so `_compat.py` is deleted as soon as [sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876) ships.
6. **Defect fixes.**
   - `BigQueryConnectionShim.query` honors `job_config.query_parameters`, and `BigQueryAsyncConfig.create_connection` reuses a single driver.
   - `SyncAdapterConfig.close` / `AsyncAdapterConfig.close` release `DatabaseRegistry`-shared drivers via reference counting instead of closing pools held by sibling configs, and `provide_session` yields isolated per-session transaction state.
   - Remove `plugin.py::_inject_sqlspec_pool_defaults`, the `spanner.py` -> `vertebra.db.adapters.bigquery.ObjectRef` cross-engine import through the storage-owned artifact-reference contract above, duplicate `_cosine_similarity`, duplicate `connection_instance` assignment, sync/async `resolve_*_config` duplication, and standalone `_hydrate_*` functions (encapsulating them on their owning classes). SQLSpec config resolution constructs the owning native config or delegates URL resolution to `DatabaseRegistry`; shared extension modules contain no vendor dispatch.
7. **Broker Arrow frames.** Replace `extensions/sqlspec/provider.py::{SharedArrowBuffer,SharedArrowTable}` (`SQLSpecSyncProvider` / `SQLSpecAsyncProvider`) with the Rust `VTBARROW` SHM writer/reader (`ArrowCursor`), tying SHM unlink strictly to lease lifecycle.

## Out of Scope

- Queue backends (Chapter 5 `queues-tasks-and-channels`), SQLAlchemy/Django (Chapter 6 `sqlalchemy-and-django-integration`), `ObjectStoreProtocol` streaming parity (`native-data-platform/storage-completeness`), and upstream `sqlspec` repository changes ([sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876)).

## Public Seams

`vertebra.extensions.sqlspec.adapters.<engine>.{<Engine>SyncConfig,<Engine>AsyncConfig}`, `SQLSpecSyncProvider`, `SQLSpecAsyncProvider`, `adapter_name`, `SQLSpecPlugin`, and storage-neutral `vertebra.storage.ArtifactReference` for the shared artifact-reference surface. BigQuery Object Table APIs retain `vertebra.db.adapters.bigquery.ObjectRef`.

## Dependencies

`native-data-platform` Chapters 1–8 (`db-layout-and-contract`, `native-postgres-driver`, `broker-pool-sharing`, `native-oracle-driver`, `native-mysql-driver`, `native-mssql-driver`, `native-sqlite-driver`, and `native-duckdb-driver` off `main @ 80ff8a65`) and `integration-remediation` `db-engine-primitives` (`main @ 80ff8a65`).

## Contract Research Before Task Refinement

- Verify each native engine profile's default SQLSpec placeholder execution against its owning native driver: PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, BigQuery, and both Spanner GoogleSQL and Spangres. Trace native placeholder syntax, positional/named parameter packing, upstream preparation, and actual parameterized execution rather than assuming the SQL dialect name selects a compatible default profile.
- Verify upstream prepared-statement classification across those profiles for SELECT, DML, DDL, CTEs, and result-bearing DML such as `RETURNING`, including empty results and stack fallback paths. Use upstream prepared metadata and dispatch contracts; record a concrete profile/classification gap before changing behavior. The current `db-engine-primitives:t06` supplies minimum statement-stack compatibility; broader profile parity belongs to this flow.
- Preserve explicit caller `StatementConfig`, parameter-style, and preparation/transformer configuration. Native-compatible defaults apply only when a caller omits the corresponding configuration; adapter construction and stack preparation never replace an explicit override. Cover both omitted defaults and explicit overrides in consumer execution checks.
- Resolve the profiles against the supported installed upstream SQLSpec version and its source/MyPyC execution boundary before refining the worksheets. Preserve native exception propagation, dispatch keyword arguments, and lazy optional driver dependencies. Do not introduce reparsing, mandatory optional engine imports, or a new profile/result API to mask an upstream contract gap.

## Acceptance Boundary

- Upstream store contract tests pass against Vertebra configs across supported engines.
- Parameterized native execution verifies omitted placeholder defaults and upstream prepared classification across all supported engine profiles, including Spanner GoogleSQL/Spangres, empty results, result-bearing DML, native pipeline dispatch, and upstream fallback. Explicit caller statement/profile/parameter configuration remains effective. Relevant source and installed MyPyC SQLSpec checks preserve native error propagation and optional dependency loading; unit preparation checks alone do not establish native execution parity.
- Artifact-reference tests prove BigQuery and Spanner artifact surfaces produce the same storage-neutral metadata and signed-URL semantics, preserve upstream hook return shapes, and retain BigQuery `ObjectRef` for native Object Table operations. The Spanner SQLSpec adapter imports no BigQuery adapter modules.
- Adapter ownership checks prove generic SQLSpec modules contain no vendor dispatch or engine SQL, native DB adapters import no SQLSpec/framework/agent code, and SQLSpec-specific helpers stay inside their owning adapter. Broker dialect IDs remain protocol metadata; checks target dependency direction and executable engine behavior rather than engine-name substring bans.
- `SQLSpecSessionService` and extension migrations run cleanly on Vertebra BigQuery and Spanner configs.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Draft Task Outline

- t01 Adapter package layout with explicit native-versus-SQLSpec ownership, `build_duckdb_gcs_secret_sql` wrapper removal, and generic-module cleanup.
- t02 Dispatch-contract driver bases, verified native placeholder defaults and upstream prepared classification across engine profiles with explicit caller configuration preserved, and `execute_stack` `SyncPipelineCapable` / `AsyncPipelineCapable` integration.
- t03 Per-engine `<Engine>SyncDataDictionary` / `<Engine>AsyncDataDictionary` implementations.
- t04 `_compat.py` resolver shim + `adapter_name` + pinning test.
- t05 BigQuery stores on upstream bases, connection shim fixes, and storage-owned `ArtifactReference` use on the shared artifact surface while retaining native BigQuery `ObjectRef`.
- t06 Spanner stores on upstream bases, DDL/parameter fixes, and direct storage-owned artifact-reference construction without cross-engine imports.
- t07 Remaining engine stores + `DatabaseRegistry` shared-pool refcount.
- t08 Rust `VTBARROW` replacement for `provider.py::{SharedArrowBuffer,SharedArrowTable}` + docs.
