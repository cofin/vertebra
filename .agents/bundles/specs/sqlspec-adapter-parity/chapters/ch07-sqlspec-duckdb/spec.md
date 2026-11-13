---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch07-sqlspec-duckdb
title: "Chapter 07: SQLSpec DuckDB Adapter Package (duckdb)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/duckdb/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_duckdb' configs)."
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
  - duckdb
  - adk
  - events
  - litestar
sources:
  - id: duckdb-drv
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: "Native DuckDB sync and async drivers"
  - id: duckdb-core
    resource: src/py/vertebra/db/adapters/duckdb/core.py
    title: "Native DuckDB core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing DuckDB SQLSpec adapter"
---

# Chapter 07: SQLSpec DuckDB Adapter Package (duckdb)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: DuckDB SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/duckdb/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/duckdb/driver.py` with `adapter_name = 'vertebra_duckdb'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^duckdb-drv] [^duckdb-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/duckdb/` SQLSpec adapter package over Vertebra's native DuckDB driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py` [^duckdb-drv] [^duckdb-core].

## 2. Requirements, Concrete Contracts & Architecture

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

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| DuckDB driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{core,driver,data_dictionary}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| DuckDB ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{adk,events,litestar}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| DuckDB configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{config,__init__}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-duckdb-driver-and-dictionary.md](tasks/t01-duckdb-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{core,driver,data_dictionary}.py`.
- [ ] [t02-duckdb-extension-stores.md](tasks/t02-duckdb-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{adk,events,litestar}.py`.
- [ ] [t03-duckdb-config-and-barrel.md](tasks/t03-duckdb-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{config,__init__}.py` (`adapter_name = 'vertebra_duckdb'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch07-sqlspec-duckdb` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^duckdb-drv]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Native DuckDB sync and async drivers.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py` — Native DuckDB core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing DuckDB SQLSpec adapter.
