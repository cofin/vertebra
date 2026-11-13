---
type: Task
id: "sqlspec-adapter-parity:ch07-sqlspec-duckdb:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch07-sqlspec-duckdb
title: "Implement SQLSpec DuckDB ADK, Events & Litestar Extension Stores"
description: "Implement SDK-free src/py/vertebra/extensions/sqlspec/adapters/duckdb/{adk,events,litestar}.py subclassing upstream SQLSpec store bases."
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
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/litestar.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
estimated_loc: 245
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
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: "Storage-neutral ArtifactReference value struct"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing DuckDB SQLSpec adapter"
---

# Task `t02`: Implement SQLSpec DuckDB ADK, Events & Litestar Extension Stores

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^duckdb-drv] [^storage-art] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py` — DuckDB SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

## Contracts & Edge Cases

- Implement `DuckDBSyncADKStore`/`DuckDBAsyncADKStore`, `array_cosine_similarity` / `fts` `DuckDBSyncADKMemoryStore`/`DuckDBAsyncADKMemoryStore`, `DuckDBSyncADKArtifactStore`/`DuckDBAsyncADKArtifactStore`, `DuckDBEventQueueStore`, and `DuckDBLitestarStore` (`ON CONFLICT DO UPDATE`).
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

## Test Matrix

- Parametrized unit/contract tests for `DuckDBSyncADKStore`/`DuckDBAsyncADKStore`, memory search, `ArtifactReference` metadata, `DuckDBEventQueueStore`, and `DuckDBLitestarStore` TTL/upsert/delete.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^duckdb-drv]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Native DuckDB sync and async drivers.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing DuckDB SQLSpec adapter.
