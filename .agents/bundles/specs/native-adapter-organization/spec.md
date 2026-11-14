---
type: Spec
flow_id: native-adapter-organization
title: "Uniform Native Adapter Module Organization and DataFusion Cleanup"
description: "Decompose monolithic BigQuery, Spanner, Oracle, SQL Server, MySQL, and DuckDB adapter modules into uniform domain files, remove unimplemented DataFusion pgwire_fdw config fields and vendor wrappers, and synchronize adapter documentation and OKF chapters."
status: stable
state: in_progress
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
current_task: null
parent_prd: native-data-platform
depends_on_flows:
  - native-postgres-driver
plan_revision: 2
plan_commit: 1b191388
created_at: "2026-11-12T23:45:00Z"
updated_at: "2026-11-14T18:43:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-14T18:43:00Z"
    commit: 1b191388
tags:
  - database
  - adapters
  - bigquery
  - spanner
  - datafusion
  - architecture
sources:
  - id: bq-config
    resource: src/py/vertebra/db/adapters/bigquery/config.py::BigQueryConfig
    title: "BigQueryConfig mixed with ObjectRef, ObjectTableConfig, and BigQueryTableIdentifier"
  - id: bq-core
    resource: src/py/vertebra/db/adapters/bigquery/core.py::BigQueryCore
    title: "BigQueryCore mixed with Object Table store fallback helpers"
  - id: bq-driver
    resource: src/py/vertebra/db/adapters/bigquery/driver.py::SyncBigQueryDriver
    title: "Monolithic BigQuery sync and async driver module"
  - id: sp-config
    resource: src/py/vertebra/db/adapters/spanner/config.py::SpannerConfig
    title: "SpannerConfig mixed with StalenessBound, Mutation, and MutationGroup"
  - id: sp-driver
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver
    title: "Monolithic Spanner snapshot, transaction, and sync/async driver module"
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: "Monolithic 2340-line Oracle pool, pipeline, and sync/async driver module"
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: "Monolithic MySQL sync/async driver module paired with loose core.py helpers"
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: "Monolithic SQL Server sync/async driver module"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: "Monolithic DuckDB sync/async driver and cursor module with loose module helpers"
  - id: df-config
    resource: src/py/vertebra/db/adapters/datafusion/config.py::DataFusionConfig
    title: "DataFusionConfig carrying unimplemented pgwire_fdw fields"
  - id: df-driver
    resource: src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine
    title: "DataFusionEngine carrying redundant BigQuery and Spanner registration wrappers"
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Cross-engine database drivers and adapter organization OKF chapter"
---

# Specification: Uniform Native Adapter Module Organization and DataFusion Cleanup

<!-- truth: start -->
- **Baseline (`1b191388`)**: `bigquery/config.py` (`619L`) and `spanner/config.py` (`543L`) mix connection configs with `ObjectRef`/`ObjectTableConfig` and `StalenessBound`/`Mutation`/`MutationGroup`; `bigquery`, `spanner` (`2,011L`), `oracle` (`2,340L`), `mysql` (`1,244L`), `duckdb` (`1,251L`), and `mssql` (`855L`) bundle sync/async drivers (and Oracle pools/pipelines) into monolithic `driver.py` files; and `DataFusionConfig` / `DataFusionEngine` carry unimplemented `pgwire_fdw` fields and vendor-specific registration wrappers [^bq-config] [^sp-config] [^ora-driver] [^df-config] [^df-driver].
- **Target Architecture**: Every native database adapter under `src/py/vertebra/db/adapters/<engine>/` follows uniform single-responsibility module boundaries (`config.py`, `core.py`, `_sync.py`, `_async.py`, thin `driver.py` barrel, `pipeline.py`, `object_tables.py`, `mutations.py`, and lazy `__init__.py` exports with zero re-export shims in `config.py`), while `DataFusionConfig` supports only implemented `mode="in_process"` and `DataFusionEngine` registers upstream queries strictly via `register_arrow_table(name, cursor)`.
- **Scope & Boundaries**: Ready in the immediate wave after archived `native-postgres-driver` ([`postgres-native-driver.md`](../../knowledge/domains/data/postgres-native-driver.md)) with zero overlap against `sqlite` or `redis`/`valkey`; preserves public `vertebra.db.adapters.<engine>` lazy exports (`__all__` / `_EXPORT_MAP`) consumed by SQLSpec and SQLAlchemy extensions [^okf-drivers].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Current Baseline
Across `src/py/vertebra/db/adapters/`, `postgres` establishes clean module boundaries (`config.py`, `options.py`, `binds.py`, `core.py`, `types.py`, `arrow.py`, `pipeline.py`, `_sync.py`, `_async.py`, and an 11-line `driver.py` barrel), whereas earlier companion-wheel adapters carry structural debt:
1. **BigQuery (`bigquery/`)**: `config.py` mixes `BigQueryConfig` and `BigQueryDryRunResult` with `ObjectRef`, `ObjectTableConfig` (performing duck-typed `getattr` signing and `inspect.isawaitable`), and `BigQueryTableIdentifier`; `core.py` embeds Object Table fallback store listing; and `driver.py` (`1,041L`) combines `SyncBigQueryDriver` and `AsyncBigQueryDriver` [^bq-config] [^bq-core] [^bq-driver].
2. **Spanner (`spanner/`)**: `config.py` mixes `SpannerConfig` with `StalenessBound`, `Mutation`, and `MutationGroup`; `core.py` holds mutation coercion/chunking helpers; and `driver.py` (`2,011L`) combines `SyncSpannerSnapshot`, `AsyncSpannerSnapshot`, `SyncSpannerTransaction`, `AsyncSpannerTransaction`, `SyncSpannerDriver`, and `AsyncSpannerDriver` [^sp-config] [^sp-driver].
3. **Oracle, MySQL, SQL Server & DuckDB (`oracle/`, `mysql/`, `mssql/`, `duckdb/`)**: `oracle/driver.py` (`2,340L`) embeds `SyncOraclePool`, `AsyncOraclePool`, `SyncOraclePipeline`, `AsyncOraclePipeline`, and both drivers plus module-level `_...` functions [^ora-driver]; `mysql`, `mssql`, and `duckdb` combine sync/async drivers in `driver.py` alongside loose module-level helper functions in `mysql/core.py`, `mssql/{config,driver}.py`, and `duckdb/{config,core,driver}.py` [^mysql-driver] [^mssql-driver] [^duckdb-driver].
4. **DataFusion (`datafusion/`)**: `DataFusionConfig` advertises `mode="pgwire_fdw"`, `pgwire_host`, `pgwire_port`, and `require_mtls_for_pgwire` with zero runtime listener implementation [^df-config], and `DataFusionEngine` carries four vendor-specific `register_bigquery_*` / `register_spanner_*` wrappers duplicating `driver.select_to_arrow(...)` + `register_arrow_table(name, cursor)` [^df-driver].

### 1.2 Non-Goals
- Modifying Rust companion extension crates (`src/rs/vertebra-*`) or `.pyi` stubs.
- Retaining backward-compatibility re-export shims inside `bigquery/config.py` or `spanner/config.py` (package-root `__init__.py` `_EXPORT_MAP` preserves public `from vertebra.db.adapters.<engine> import ...` imports).
- Implementing a Rust `pgwire` server inside `DataFusionEngine` (`DataFusionEngine` remains an embedded `in_process` Arrow federation engine exempt from server-client driver metrics).

## 2. Requirements, Concrete Contracts & Architecture

- **`R1` BigQuery & Spanner Domain Value Extraction & Driver Split (`t01`)**:
  - Create `src/py/vertebra/db/adapters/bigquery/object_tables.py` owning `ObjectRef(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)` and `ObjectTableConfig(FrozenBaseStruct, frozen=True, kw_only=True)` with explicit `hydrate_object_refs_sync(refs, store: SyncObjectStore)` / `hydrate_object_refs_async(refs, store: AsyncObjectStore)` and class-encapsulated SQL/fallback listing (zero `inspect.isawaitable` or broad `except Exception`). Move `BigQueryTableIdentifier` into `bigquery/core.py`, split `bigquery/driver.py` into `_sync.py::SyncBigQueryDriver` + `_async.py::AsyncBigQueryDriver` + 12-line `driver.py` barrel, and update `bigquery/{driver,embeddings,tools,__init__}.py` imports.
  - Create `src/py/vertebra/db/adapters/spanner/mutations.py` owning `StalenessBound`, `Mutation`, and `MutationGroup` with class-encapsulated `Mutation.coerce(...)`, `MutationGroup.coerce(...)`, and `Mutation.chunk_by_cell_limit(...)`. Extract `SyncSpannerSnapshot`, `AsyncSpannerSnapshot`, `SyncSpannerTransaction`, and `AsyncSpannerTransaction` into `spanner/transactions.py`, split `spanner/driver.py` into `_sync.py` + `_async.py` + `driver.py` barrel, and update `spanner/{core,tools,__init__}.py`.
- **`R2` Oracle, MySQL, SQL Server & DuckDB Decomposition & Class Encapsulation (`t02`)**:
  - Extract `SyncOraclePipeline` and `AsyncOraclePipeline` into `src/py/vertebra/db/adapters/oracle/pipeline.py`, split `oracle/driver.py` into `_sync.py` (`SyncOraclePool`, `SyncOracleDriver`) + `_async.py` (`AsyncOraclePool`, `AsyncOracleDriver`) + 16-line `driver.py` barrel, fold loose `_...` helpers onto `OracleConfig`, `OracleCore`, and `OracleNotificationSource` (with public-before-private ordering in `notify.py`), and remove module-level alias shims from `oracle/core.py` and `oracle/__init__.py` (updating test callers to `OracleCore.*`).
  - Split `mssql/driver.py`, `mysql/driver.py`, and `duckdb/driver.py` into `_sync.py` + `_async.py` + 12-line `driver.py` barrels (`_SyncDuckDBCursor` in `duckdb/_sync.py`, `_AsyncDuckDBCursor` in `duckdb/_async.py`), consolidate loose module-level functions onto `MySQLCore`, `MSSQLConfig`, `DuckDBSecretConfig`, and `DuckDBCore` while preserving `mysql/core.py` public exports via staticmethod bindings, and enforce public-before-private member ordering in `mssql/{core,pipeline}.py`.
- **`R3` `DataFusionConfig` `pgwire_fdw` Removal & `DataFusionEngine` Cleanup (`t03`)**:
  - Narrow `type DataFusionMode = Literal["in_process"]` in `datafusion/config.py`, delete `pgwire_host`, `pgwire_port`, and `require_mtls_for_pgwire` fields and `pool_key()` entries, and raise `ImproperConfigurationError` when `DataFusionConfig.from_url` or `__post_init__` receives `mode="pgwire_fdw"` or `host:port` netlocs.
  - Delete `register_bigquery_table`, `register_bigquery_query`, `register_spanner_table`, and `register_spanner_query` from `DataFusionEngine` (`datafusion/driver.py`), migrating unit/conformance test callers to `engine.register_arrow_table(name, driver.select_to_arrow(...))`.
- **`R4` Documentation & OKF Knowledge Synchronization (`t04`)**:
  - Update `docs/usage/analytics-and-cdc.md` DataFusion federation snippets to use `select_to_arrow` + `register_arrow_table` (`mode="in_process"` only; already free of legacy `register_bigquery_*` / `pgwire_fdw` text on `HEAD`) and synchronize [`database-drivers-and-dialers.md`](../../knowledge/domains/data/database-drivers-and-dialers.md) and [`query-pipeline-cdc-and-tenancy.md`](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) (`<= 200` lines each).

## 3. Documentation & OKF Knowledge Plan

- `docs/usage/analytics-and-cdc.md` (`t04`): Ensure DataFusion federation snippets document `driver.select_to_arrow(...)` + `engine.register_arrow_table(...)` and `mode="in_process"` (no `register_bigquery_*` / `register_spanner_*` or `pgwire_fdw` mentions exist on `HEAD`).
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`t04`, `<= 200L`): Document uniform per-adapter module responsibilities (`config.py`, `core.py`, `_sync.py`, `_async.py`, `driver.py` barrel, `pipeline.py`, `arrow.py`, `types.py`, `object_tables.py`, `mutations.py`) and `DataFusionEngine`'s embedded exemption.
- `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`t04`, `<= 200L`): Synchronize `oracle/pipeline.py`, `spanner/mutations.py`, `bigquery/object_tables.py`, and `DataFusionEngine` federation references.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` BigQuery & Spanner splits | `t01` | `src/py/vertebra/db/adapters/{bigquery,spanner}/**` | `pytest src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py` |
| `R2` Oracle/MySQL/MSSQL/DuckDB splits | `t02` | `src/py/vertebra/db/adapters/{oracle,mysql,mssql,duckdb}/**` | `pytest src/py/tests/unit/db/test_{oracle_adapter,mysql_driver,mssql_driver,duckdb_driver}.py` |
| `R3` DataFusion `pgwire_fdw` & wrapper removal | `t03` | `src/py/vertebra/db/adapters/datafusion/{config,driver,__init__}.py` | `pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/integration/db/test_driver_conformance.py -k datafusion` |
| `R4` Docs & OKF sync | `t04` | `docs/usage/analytics-and-cdc.md`, `.agents/bundles/knowledge/domains/data/*.md` | `make docs && make review-check` |

## 5. Implementation Plan

- [x] [838c54d1] [t01-bigquery-and-spanner-module-split.md](tasks/t01-bigquery-and-spanner-module-split.md) — Extract BigQuery `object_tables.py` and Spanner `mutations.py`/`transactions.py`, and split both `driver.py` modules into `_sync.py` and `_async.py` barrels.
- [x] [838c54d1] [t02-relational-and-duckdb-driver-decomposition.md](tasks/t02-relational-and-duckdb-driver-decomposition.md) — Extract `oracle/pipeline.py`, split `oracle`, `mysql`, `mssql`, and `duckdb` `driver.py` modules into `_sync.py`/`_async.py` barrels, and encapsulate loose module helpers.
- [x] [838c54d1] [t03-datafusion-wrapper-and-pgwire-cleanup.md](tasks/t03-datafusion-wrapper-and-pgwire-cleanup.md) — Remove unimplemented `pgwire_fdw` fields from `DataFusionConfig` and vendor registration wrappers from `DataFusionEngine`.
- [x] [838c54d1] [t04-adapter-layout-docs-and-okf-sync.md](tasks/t04-adapter-layout-docs-and-okf-sync.md) — Synchronize `analytics-and-cdc.md`, `database-drivers-and-dialers.md`, and `query-pipeline-cdc-and-tenancy.md`.

## 6. Current State & Remaining Plan Snapshot

- **State**: `in_progress` (`4/4` tasks completed at `838c54d1`; `[3/3 files documented]`; awaiting zero-context review and `/flow:finish` + `/flow:archive`).
- **Full `/flow:finish` Verification**: `PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters && make lint-all && make py-test && make docs && make review-check`.

[^bq-config]: `src/py/vertebra/db/adapters/bigquery/config.py::BigQueryConfig` — BigQueryConfig mixed with ObjectRef, ObjectTableConfig, and BigQueryTableIdentifier.
[^bq-core]: `src/py/vertebra/db/adapters/bigquery/core.py::BigQueryCore` — BigQueryCore mixed with Object Table store fallback helpers.
[^bq-driver]: `src/py/vertebra/db/adapters/bigquery/driver.py::SyncBigQueryDriver` — Monolithic BigQuery sync and async driver module.
[^sp-config]: `src/py/vertebra/db/adapters/spanner/config.py::SpannerConfig` — SpannerConfig mixed with StalenessBound, Mutation, and MutationGroup.
[^sp-driver]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver` — Monolithic Spanner snapshot, transaction, and sync/async driver module.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Monolithic 2340-line Oracle pool, pipeline, and sync/async driver module.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — Monolithic MySQL sync/async driver module paired with loose core.py helpers.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — Monolithic SQL Server sync/async driver module.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — Monolithic DuckDB sync/async driver and cursor module with loose module helpers.
[^df-config]: `src/py/vertebra/db/adapters/datafusion/config.py::DataFusionConfig` — DataFusionConfig carrying unimplemented pgwire_fdw fields.
[^df-driver]: `src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine` — DataFusionEngine carrying redundant BigQuery and Spanner registration wrappers.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Cross-engine database drivers and adapter organization OKF chapter.
