---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch05-sqlspec-oracle
title: "Chapter 05: SQLSpec Oracle Adapter Package (oracle)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/oracle/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_oracle' configs)."
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
  - oracle
  - adk
  - events
  - litestar
sources:
  - id: oracle-drv
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "Native Oracle sync and async drivers"
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py
    title: "Native Oracle core parameter and wire helpers"
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: "Oracle TxEventQ notification source"
---

# Chapter 05: SQLSpec Oracle Adapter Package (oracle)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Oracle SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/oracle/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^ora-notify].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/oracle/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/oracle/driver.py` with `adapter_name = 'vertebra_oracle'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^oracle-drv] [^oracle-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/oracle/` SQLSpec adapter package over Vertebra's native Oracle driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py` [^oracle-drv] [^oracle-core].

## 2. Requirements, Concrete Contracts & Architecture

- **Driver & DataDictionary**: Set `StatementConfig(dialect='oracle')` supporting `{ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON}` (`:name` dicts and `:1` tuples), unwrap `TypedParameter` before `OracleCore` JSON/`array('f')` `VECTOR` encoding, map `ORA-00001`/`ORA-02291`/`ORA-00060`/`ORA-08177`, and implement `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` (23ai `OSON` JSON vs 19c `BLOB CHECK`). Keep all helpers local to `adapters/oracle/core.py`.
- **Sub-Extension Stores**: Implement SDK-free `OracleSyncADKStore`/`OracleAsyncADKStore` (`MERGE INTO` upserts, `ORA-00955`/`ORA-00942` DDL guards), 23ai `VECTOR_DISTANCE`/RRF `OracleSyncADKMemoryStore`/`OracleAsyncADKMemoryStore`, `OracleSyncADKArtifactStore`/`OracleAsyncADKArtifactStore`, `OracleEventQueueStore` + `OracleSyncEventsBackend`/`OracleAsyncEventsBackend` over `SyncOracleNotificationSource`/`AsyncOracleNotificationSource` (zero `oracledb` imports), and `OracleLitestarStore`.
- **Config & Lazy Exports**: Define `OracleSyncConfig` and `OracleAsyncConfig` declaring `adapter_name = 'vertebra_oracle'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.

```python
class OracleSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "oracle"

class OracleAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "oracle"

class OracleSyncDriver(BaseSyncDriver):
    dialect = "oracle"

class OracleAsyncDriver(BaseAsyncDriver):
    dialect = "oracle"

class OracleSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_oracle"
    driver_type = OracleSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class OracleAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_oracle"
    driver_type = OracleAsyncDriver
```

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| Oracle driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/oracle/{core,driver,data_dictionary}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| Oracle ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/oracle/{adk,events,litestar}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| Oracle configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/oracle/{config,__init__}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-oracle-driver-and-dictionary.md](tasks/t01-oracle-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{core,driver,data_dictionary}.py`.
- [ ] [t02-oracle-extension-stores.md](tasks/t02-oracle-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{adk,events,litestar}.py`.
- [ ] [t03-oracle-config-and-barrel.md](tasks/t03-oracle-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{config,__init__}.py` (`adapter_name = 'vertebra_oracle'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch05-sqlspec-oracle` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^oracle-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle sync and async drivers.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py` — Native Oracle core parameter and wire helpers.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification source.
