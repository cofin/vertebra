---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch09-sqlspec-spanner
title: "Chapter 09: SQLSpec Google Cloud Spanner & Spangres Adapter Package (spanner)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/spanner/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_spanner' configs)."
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
  - spanner
  - adk
  - events
  - litestar
sources:
  - id: spanner-drv
    resource: src/py/vertebra/db/adapters/spanner/driver.py
    title: "Native Google Cloud Spanner & Spangres sync and async drivers"
  - id: spanner-core
    resource: src/py/vertebra/db/adapters/spanner/core.py
    title: "Native Google Cloud Spanner & Spangres core parameter and wire helpers"
  - id: sqlspec-spn
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: "Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import"
---

# Chapter 09: SQLSpec Google Cloud Spanner & Spangres Adapter Package (spanner)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Google Cloud Spanner & Spangres SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/spanner/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-spn].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/spanner/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/spanner/driver.py` with `adapter_name = 'vertebra_spanner'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^spanner-drv] [^spanner-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/spanner/` SQLSpec adapter package over Vertebra's native Google Cloud Spanner & Spangres driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py` [^spanner-drv] [^spanner-core].

## 2. Requirements, Concrete Contracts & Architecture

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

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| Google Cloud Spanner & Spangres driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/spanner/{core,driver,data_dictionary}.py` | `pytest test_sqlspec_spanner.py` |
| Google Cloud Spanner & Spangres ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/spanner/{adk,events,litestar}.py` | `pytest test_sqlspec_spanner.py` |
| Google Cloud Spanner & Spangres configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/spanner/{config,__init__}.py` | `pytest test_sqlspec_spanner.py` |

## 5. Implementation Plan

- [ ] [t01-spanner-driver-and-dictionary.md](tasks/t01-spanner-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{core,driver,data_dictionary}.py`.
- [ ] [t02-spanner-extension-stores.md](tasks/t02-spanner-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{adk,events,litestar}.py`.
- [ ] [t03-spanner-config-and-barrel.md](tasks/t03-spanner-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{config,__init__}.py` (`adapter_name = 'vertebra_spanner'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch09-sqlspec-spanner` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py
```

[^spanner-drv]: `src/py/vertebra/db/adapters/spanner/driver.py` — Native Google Cloud Spanner & Spangres sync and async drivers.
[^spanner-core]: `src/py/vertebra/db/adapters/spanner/core.py` — Native Google Cloud Spanner & Spangres core parameter and wire helpers.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
