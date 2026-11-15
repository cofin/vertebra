---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-enterprise-and-cloud-adapters
title: 'Chapter 03: Per-Engine SQLSpec Adapter Packages for SQL Server, DuckDB, BigQuery, and Cloud Spanner'
description: Deliver feature-complete per-engine SQLSpec packages for SQL Server, DuckDB, BigQuery, and Cloud Spanner (fixing sqlglot 'spanner'/'postgres' dialect names and removing cross-engine BigQuery ObjectRef imports).
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- sqlspec
- mssql
- adk
- events
- litestar
- duckdb
- bigquery
- spanner
sources:
- id: mssql-drv
  resource: src/py/vertebra/db/adapters/mssql/driver.py
  title: Native SQL Server (MSSQL) sync and async drivers
- id: mssql-core
  resource: src/py/vertebra/db/adapters/mssql/core.py
  title: Native SQL Server (MSSQL) core parameter and wire helpers
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Existing MSSQL SQLSpec adapter
- id: duckdb-drv
  resource: src/py/vertebra/db/adapters/duckdb/driver.py
  title: Native DuckDB sync and async drivers
- id: duckdb-core
  resource: src/py/vertebra/db/adapters/duckdb/core.py
  title: Native DuckDB core parameter and wire helpers
- id: bigquery-drv
  resource: src/py/vertebra/db/adapters/bigquery/driver.py
  title: Native Google BigQuery sync and async drivers
- id: bigquery-core
  resource: src/py/vertebra/db/adapters/bigquery/core.py
  title: Native Google BigQuery core parameter and wire helpers
- id: sqlspec-bq
  resource: src/py/vertebra/extensions/sqlspec/bigquery.py
  title: Current top-level BigQuery SQLSpec module
- id: spanner-drv
  resource: src/py/vertebra/db/adapters/spanner/driver.py
  title: Native Google Cloud Spanner & Spangres sync and async drivers
- id: spanner-core
  resource: src/py/vertebra/db/adapters/spanner/core.py
  title: Native Google Cloud Spanner & Spangres core parameter and wire helpers
- id: sqlspec-spn
  resource: src/py/vertebra/extensions/sqlspec/spanner.py
  title: Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import
---

# Chapter 03: Per-Engine SQLSpec Adapter Packages for SQL Server, DuckDB, BigQuery, and Cloud Spanner

<!-- truth: start -->
- **Baseline (`18baadcd`)**: SQL Server (MSSQL) SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/mssql/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/mssql/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/mssql/driver.py` with `adapter_name = 'vertebra_mssql'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^mssql-drv] [^mssql-core].
- **Baseline (`18baadcd`)**: DuckDB SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/duckdb/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/duckdb/driver.py` with `adapter_name = 'vertebra_duckdb'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^duckdb-drv] [^duckdb-core].
- **Baseline (`18baadcd`)**: Google BigQuery SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/bigquery/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-bq].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/bigquery/driver.py` with `adapter_name = 'vertebra_bigquery'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^bigquery-drv] [^bigquery-core].
- **Baseline (`18baadcd`)**: Google Cloud Spanner & Spangres SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/spanner/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-spn].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/spanner/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/spanner/driver.py` with `adapter_name = 'vertebra_spanner'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^spanner-drv] [^spanner-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/mssql/` SQLSpec adapter package over Vertebra's native SQL Server (MSSQL) driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py` [^mssql-drv] [^mssql-core].

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/duckdb/` SQLSpec adapter package over Vertebra's native DuckDB driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py` [^duckdb-drv] [^duckdb-core].

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/bigquery/` SQLSpec adapter package over Vertebra's native Google BigQuery driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py` [^bigquery-drv] [^bigquery-core].

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/spanner/` SQLSpec adapter package over Vertebra's native Google Cloud Spanner & Spangres driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py` [^spanner-drv] [^spanner-core].

## 2. Requirements, Concrete Contracts & Architecture

- **Driver & DataDictionary**: Set `StatementConfig(dialect='tsql')` with `ParameterStyle.QMARK` (`?` positional tuples rewritten by `MSSQLCore.rewrite_placeholders` to `@P1..@Pn` for `vertebra._mssql`), `SAVE TRANSACTION` / `ROLLBACK TRANSACTION` savepoints, error `2601`/`2627`/`547`/`1205` mapping, and `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary`. Keep all T-SQL helpers strictly local to `adapters/mssql/core.py`.
- **Sub-Extension Stores**: Implement SDK-free `MSSQLSyncADKStore`/`MSSQLAsyncADKStore` (`MERGE ... WITH (HOLDLOCK)`, `NVARCHAR(MAX)` JSON, `DATETIMEOFFSET(6)`), `MSSQLSyncADKMemoryStore`/`MSSQLAsyncADKMemoryStore`, `MSSQLSyncADKArtifactStore`/`MSSQLAsyncADKArtifactStore`, `MSSQLEventQueueStore` (`IF OBJECT_ID(...) IS NULL` table/index DDL guards), and `MSSQLLitestarStore` (`VARBINARY(MAX)`).
- **Config & Lazy Exports**: Define `MSSQLSyncConfig` and `MSSQLAsyncConfig` declaring `adapter_name = 'vertebra_mssql'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.

```python
class MSSQLSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "tsql"

class MSSQLAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "tsql"

class MSSQLSyncDriver(BaseSyncDriver):
    dialect = "tsql"

class MSSQLAsyncDriver(BaseAsyncDriver):
    dialect = "tsql"

class MSSQLSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_mssql"
    driver_type = MSSQLSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class MSSQLAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_mssql"
    driver_type = MSSQLAsyncDriver
```

- **Driver & DataDictionary**: Set `StatementConfig(dialect='duckdb')` supporting `{ParameterStyle.QMARK, ParameterStyle.NUMERIC}` (`?` and `$1`), DuckDB 2.0+ lexical `COPY (SELECT ... ?) TO ?` parameter ordering, native Python `fetchall()` preservation for Arrow-divergent types (`HUGEINT`, `UHUGEINT`, `BIT`, `TIMETZ`, `INTERVAL`, `MAP`) alongside zero-copy `select_to_arrow` / `load_from_arrow`, and `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary`. Keep all helpers local to `adapters/duckdb/core.py`.
- **Sub-Extension Stores**: Implement `DuckDBSyncADKStore`/`DuckDBAsyncADKStore`, `array_cosine_similarity` / `fts` `DuckDBSyncADKMemoryStore`/`DuckDBAsyncADKMemoryStore`, `DuckDBSyncADKArtifactStore`/`DuckDBAsyncADKArtifactStore`, `DuckDBEventQueueStore`, and `DuckDBLitestarStore` (`ON CONFLICT DO UPDATE`).
- **Config & Lazy Exports**: Define `DuckDBSyncConfig` and `DuckDBAsyncConfig` declaring `adapter_name = 'vertebra_duckdb'`, isolated `:memory:<name>` database handling, and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.

```python
class DuckDBSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "duckdb"

class DuckDBAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "duckdb"

class DuckDBSyncDriver(BaseSyncDriver):
    dialect = "duckdb"

class DuckDBAsyncDriver(BaseAsyncDriver):
    dialect = "duckdb"

class DuckDBSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_duckdb"
    driver_type = DuckDBSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class DuckDBAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_duckdb"
    driver_type = DuckDBAsyncDriver
```

- **Driver & DataDictionary**: Lift BigQuery SQLSpec logic onto `BaseSyncDriver`/`BaseAsyncDriver` with `StatementConfig(dialect='bigquery')` and `ParameterStyle.NAMED_AT` (`@name`, preserving `TypedParameter` `STRUCT`/`ARRAY`/`NUMERIC`/`GEOGRAPHY`/`JSON` hints), unsplit procedural `BEGIN ... END` script execution, `session_id` transactions, `dry_run`, zero-copy `VTBARROW` `select_to_arrow`, Storage Write `PENDING` atomic `load_from_arrow`, and `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary`. Keep all helpers local to `adapters/bigquery/core.py`.
- **Sub-Extension Stores**: Implement zero-SDK `BigQuerySyncADKStore`/`BigQueryAsyncADKStore`, `VECTOR_SEARCH` / `SEARCH` `BigQuerySyncADKMemoryStore`/`BigQueryAsyncADKMemoryStore`, `BigQuerySyncADKArtifactStore`/`BigQueryAsyncADKArtifactStore` translating native `vertebra.db.adapters.bigquery.ObjectRef` into `vertebra.storage.ArtifactReference` on the shared artifact surface (while keeping `ObjectRef` on native Object Tables), partitioned/clustered `BigQueryEventQueueStore`, and `BigQueryLitestarStore` (`MERGE`).
- **Config & Lazy Exports**: Define `BigQuerySyncConfig` and `BigQueryAsyncConfig` declaring `adapter_name = 'vertebra_bigquery'`, fixing `BigQueryConnectionShim.query` to honor `job_config.query_parameters` and `BigQueryAsyncConfig.create_connection` to reuse a single driver, plus explicit `*_store_class` attributes and PEP 562 lazy `__init__.py`.

```python
class BigQuerySyncDataDictionary(SyncDataDictionaryBase):
    dialect = "bigquery"

class BigQueryAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "bigquery"

class BigQuerySyncDriver(BaseSyncDriver):
    dialect = "bigquery"

class BigQueryAsyncDriver(BaseAsyncDriver):
    dialect = "bigquery"

class BigQuerySyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_bigquery"
    driver_type = BigQuerySyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class BigQueryAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_bigquery"
    driver_type = BigQueryAsyncDriver
```

- **Driver & DataDictionary**: Fix invalid `sqlglot` dialect names by setting `StatementConfig.dialect = 'spanner'` (`ParameterStyle.NAMED_AT` `@name`, `TypeCode.JSON` and `TypedParameter(semantic_name='ARRAY<FLOAT32>')`) when `spanner_dialect == 'googlesql'` and `StatementConfig.dialect = 'postgres'` (`ParameterStyle.NUMERIC` `$1 -> p1`) when `spanner_dialect == 'spangres'`. Support `execute_partitioned_dml`, `ExecuteBatchDml` `execute_stack`, and `SpannerSyncDataDictionary`/`SpannerAsyncDataDictionary` routing `googlesql` vs `postgresql` catalog SQL. Keep helpers strictly inside `adapters/spanner/core.py`.
- **Sub-Extension Stores**: Implement zero-SDK `SpannerSyncADKStore`/`SpannerAsyncADKStore` (`provide_session(transaction=False)` lock-free snapshots on reads, `transaction=True` on writes), ScaNN/FTS/RRF/PropertyGraph `SpannerSyncADKMemoryStore`/`SpannerAsyncADKMemoryStore`, `SpannerSyncADKArtifactStore`/`SpannerAsyncADKArtifactStore` constructing `vertebra.storage.ArtifactReference` directly with ZERO `vertebra.db.adapters.bigquery` imports, hash-sharded `MOD(FARM_FINGERPRINT(...))` `BYTES` `SpannerEventQueueStore` (valid `ROW DELETION POLICY` / `TTL INTERVAL`, no invalid TTL on `updated_at`), and `SpannerLitestarStore`.
- **Config & Lazy Exports**: Define `SpannerSyncConfig`/`SpannerAsyncConfig` and `SpangresSyncConfig`/`SpangresAsyncConfig` declaring `adapter_name = 'vertebra_spanner'`, isolated per-session transaction state on `provide_session`, and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.

```python
class SpannerSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "spanner"

class SpannerAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "spanner"

class SpannerSyncDriver(BaseSyncDriver):
    dialect = "spanner"

class SpannerAsyncDriver(BaseAsyncDriver):
    dialect = "spanner"

class SpannerSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_spanner"
    driver_type = SpannerSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class SpannerAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_spanner"
    driver_type = SpannerAsyncDriver
```

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch04-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Deliver Feature-Complete SQL Server SQLSpec Adapter Package | `src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t02` | Deliver Feature-Complete DuckDB SQLSpec Adapter Package | `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/duckdb/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t03` | Deliver Feature-Complete BigQuery SQLSpec Adapter Package | `src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlspec_bigquery.py` |
| `t04` | Deliver Feature-Complete Cloud Spanner & Spangres SQLSpec Adapter Package | `src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/data_dictionary.py` (+5 more) | `src/py/tests/unit/extensions/test_sqlspec_spanner.py` |

## 5. Implementation Plan

- [ ] [t01-mssql-sqlspec-adapter.md](tasks/t01-mssql-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/mssql/ (dialect='tsql', ? -> @P1, MERGE HOLDLOCK ADK/Events/Litestar stores, DataDictionary, adapter_name='vertebra_mssql').
- [ ] [t02-duckdb-sqlspec-adapter.md](tasks/t02-duckdb-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/duckdb/ (DuckDB 2.0+ lexical COPY ordering, Arrow-divergent fetchall() fidelity, DataDictionary, ADK/Events/Litestar stores, adapter_name='vertebra_duckdb').
- [ ] [t03-bigquery-sqlspec-adapter.md](tasks/t03-bigquery-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/bigquery/ (@name binds, unsplit BEGIN...END, VECTOR_SEARCH ADK + ArtifactReference/ObjectRef, DataDictionary, adapter_name='vertebra_bigquery').
- [ ] [t04-spanner-sqlspec-adapter.md](tasks/t04-spanner-sqlspec-adapter.md) — Build extensions/sqlspec/adapters/spanner/ (sqlglot 'spanner'/'postgres' dialect fix, ScaNN/RRF ADK + ArtifactReference, sharded stores, adapter_name='vertebra_spanner').

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-sqlspec-enterprise-and-cloud-adapters` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py
```

[^mssql-drv]: `src/py/vertebra/db/adapters/mssql/driver.py` — Native SQL Server (MSSQL) sync and async drivers.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py` — Native SQL Server (MSSQL) core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing MSSQL SQLSpec adapter.
[^duckdb-drv]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Native DuckDB sync and async drivers.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py` — Native DuckDB core parameter and wire helpers.
[^bigquery-drv]: `src/py/vertebra/db/adapters/bigquery/driver.py` — Native Google BigQuery sync and async drivers.
[^bigquery-core]: `src/py/vertebra/db/adapters/bigquery/core.py` — Native Google BigQuery core parameter and wire helpers.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current top-level BigQuery SQLSpec module.
[^spanner-drv]: `src/py/vertebra/db/adapters/spanner/driver.py` — Native Google Cloud Spanner & Spangres sync and async drivers.
[^spanner-core]: `src/py/vertebra/db/adapters/spanner/core.py` — Native Google Cloud Spanner & Spangres core parameter and wire helpers.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
