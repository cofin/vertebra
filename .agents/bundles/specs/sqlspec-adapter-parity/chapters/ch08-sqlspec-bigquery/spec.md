---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch08-sqlspec-bigquery
title: "Chapter 08: SQLSpec Google BigQuery Adapter Package (bigquery)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/bigquery/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_bigquery' configs)."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
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
  - bigquery
  - adk
  - events
  - litestar
sources:
  - id: bigquery-drv
    resource: src/py/vertebra/db/adapters/bigquery/driver.py
    title: "Native Google BigQuery sync and async drivers"
  - id: bigquery-core
    resource: src/py/vertebra/db/adapters/bigquery/core.py
    title: "Native Google BigQuery core parameter and wire helpers"
  - id: sqlspec-bq
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: "Current top-level BigQuery SQLSpec module"
---

# Chapter 08: SQLSpec Google BigQuery Adapter Package (bigquery)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Google BigQuery SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/bigquery/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-bq].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/bigquery/driver.py` with `adapter_name = 'vertebra_bigquery'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^bigquery-drv] [^bigquery-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/bigquery/` SQLSpec adapter package over Vertebra's native Google BigQuery driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py` [^bigquery-drv] [^bigquery-core].

## 2. Requirements, Concrete Contracts & Architecture

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

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| Google BigQuery driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{core,driver,data_dictionary}.py` | `pytest test_sqlspec_bigquery.py` |
| Google BigQuery ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{adk,events,litestar}.py` | `pytest test_sqlspec_bigquery.py` |
| Google BigQuery configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{config,__init__}.py` | `pytest test_sqlspec_bigquery.py` |

## 5. Implementation Plan

- [ ] [t01-bigquery-driver-and-dictionary.md](tasks/t01-bigquery-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{core,driver,data_dictionary}.py`.
- [ ] [t02-bigquery-extension-stores.md](tasks/t02-bigquery-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{adk,events,litestar}.py`.
- [ ] [t03-bigquery-config-and-barrel.md](tasks/t03-bigquery-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{config,__init__}.py` (`adapter_name = 'vertebra_bigquery'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch08-sqlspec-bigquery` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_bigquery.py
```

[^bigquery-drv]: `src/py/vertebra/db/adapters/bigquery/driver.py` — Native Google BigQuery sync and async drivers.
[^bigquery-core]: `src/py/vertebra/db/adapters/bigquery/core.py` — Native Google BigQuery core parameter and wire helpers.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current top-level BigQuery SQLSpec module.
