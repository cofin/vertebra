---
type: Task
id: "sqlspec-adapter-parity:ch08-sqlspec-bigquery:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch08-sqlspec-bigquery
title: "Implement SQLSpec BigQuery ADK, Events & Litestar Extension Stores"
description: "Implement SDK-free src/py/vertebra/extensions/sqlspec/adapters/bigquery/{adk,events,litestar}.py subclassing upstream SQLSpec store bases."
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
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/litestar.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/bigquery.py
estimated_loc: 245
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
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: "Storage-neutral ArtifactReference value struct"
  - id: sqlspec-bq
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: "Current top-level BigQuery SQLSpec module"
---

# Task `t02`: Implement SQLSpec BigQuery ADK, Events & Litestar Extension Stores

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^bigquery-drv] [^storage-art] [^sqlspec-bq].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py` — Google BigQuery SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

## Contracts & Edge Cases

- Implement zero-SDK `BigQuerySyncADKStore`/`BigQueryAsyncADKStore`, `VECTOR_SEARCH` / `SEARCH` `BigQuerySyncADKMemoryStore`/`BigQueryAsyncADKMemoryStore`, `BigQuerySyncADKArtifactStore`/`BigQueryAsyncADKArtifactStore` translating native `vertebra.db.adapters.bigquery.ObjectRef` into `vertebra.storage.ArtifactReference` on the shared artifact surface (while keeping `ObjectRef` on native Object Tables), partitioned/clustered `BigQueryEventQueueStore`, and `BigQueryLitestarStore` (`MERGE`).
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

## Test Matrix

- Parametrized unit/contract tests for `BigQuerySyncADKStore`/`BigQueryAsyncADKStore`, memory search, `ArtifactReference` metadata, `BigQueryEventQueueStore`, and `BigQueryLitestarStore` TTL/upsert/delete.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_bigquery.py
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^bigquery-drv]: `src/py/vertebra/db/adapters/bigquery/driver.py` — Native Google BigQuery sync and async drivers.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current top-level BigQuery SQLSpec module.
