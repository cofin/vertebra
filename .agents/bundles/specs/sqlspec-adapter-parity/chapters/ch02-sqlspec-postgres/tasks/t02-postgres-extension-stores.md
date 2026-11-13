---
type: Task
id: "sqlspec-adapter-parity:ch02-sqlspec-postgres:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-postgres
title: "Implement SQLSpec Postgres ADK, Events & Litestar Extension Stores"
description: "Implement SDK-free src/py/vertebra/extensions/sqlspec/adapters/postgres/{adk,events,litestar}.py subclassing upstream SQLSpec store bases."
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
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/litestar.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/db/adapters/postgres/notify.py
estimated_loc: 245
tags:
  - sqlspec
  - postgres
  - adk
  - events
  - litestar
sources:
  - id: postgres-drv
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "Native PostgreSQL sync and async drivers"
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: "Storage-neutral ArtifactReference value struct"
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: "PostgreSQL notification source"
---

# Task `t02`: Implement SQLSpec Postgres ADK, Events & Litestar Extension Stores

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^postgres-drv] [^storage-art] [^pg-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py` — PostgreSQL SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

## Contracts & Edge Cases

- Subclass SDK-free upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, pgvector cosine/HNSW + `tsvector` `BaseSyncADKMemoryStore`/`BaseAsyncADKMemoryStore`, `BaseSyncADKArtifactStore`/`BaseAsyncADKArtifactStore` returning `ArtifactReference` on the shared surface, `BaseEventQueueStore` + `PostgresSyncEventsBackend`/`PostgresAsyncEventsBackend` over `SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`, and `BaseSQLSpecStore` `ON CONFLICT (key) DO UPDATE`) with zero `psycopg` imports.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

## Test Matrix

- Parametrized unit/contract tests for `PostgresSyncADKStore`/`PostgresAsyncADKStore`, memory search, `ArtifactReference` metadata, `PostgresEventQueueStore`, and `PostgresLitestarStore` TTL/upsert/delete.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/sqlspec/adapters/postgres/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^postgres-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL sync and async drivers.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source.
