---
type: Task
id: "sqlspec-adapter-parity:ch05-sqlspec-oracle:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch05-sqlspec-oracle
title: "Implement SQLSpec Oracle ADK, Events & Litestar Extension Stores"
description: "Implement SDK-free src/py/vertebra/extensions/sqlspec/adapters/oracle/{adk,events,litestar}.py subclassing upstream SQLSpec store bases."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/litestar.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/db/adapters/oracle/notify.py
estimated_loc: 245
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
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: "Storage-neutral ArtifactReference value struct"
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: "Oracle TxEventQ notification source"
---

# Task `t02`: Implement SQLSpec Oracle ADK, Events & Litestar Extension Stores

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^oracle-drv] [^storage-art] [^ora-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py` — Oracle SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

## Contracts & Edge Cases

- Implement SDK-free `OracleSyncADKStore`/`OracleAsyncADKStore` (`MERGE INTO` upserts, `ORA-00955`/`ORA-00942` DDL guards), 23ai `VECTOR_DISTANCE`/RRF `OracleSyncADKMemoryStore`/`OracleAsyncADKMemoryStore`, `OracleSyncADKArtifactStore`/`OracleAsyncADKArtifactStore`, `OracleEventQueueStore` + `OracleSyncEventsBackend`/`OracleAsyncEventsBackend` over `SyncOracleNotificationSource`/`AsyncOracleNotificationSource` (zero `oracledb` imports), and `OracleLitestarStore`.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

## Test Matrix

- Parametrized unit/contract tests for `OracleSyncADKStore`/`OracleAsyncADKStore`, memory search, `ArtifactReference` metadata, `OracleEventQueueStore`, and `OracleLitestarStore` TTL/upsert/delete.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/sqlspec/adapters/oracle/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^oracle-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle sync and async drivers.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification source.
