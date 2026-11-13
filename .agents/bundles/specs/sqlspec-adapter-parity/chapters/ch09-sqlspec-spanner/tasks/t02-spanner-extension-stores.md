---
type: Task
id: "sqlspec-adapter-parity:ch09-sqlspec-spanner:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch09-sqlspec-spanner
title: "Implement SQLSpec Spanner ADK, Events & Litestar Extension Stores"
description: "Implement SDK-free src/py/vertebra/extensions/sqlspec/adapters/spanner/{adk,events,litestar}.py subclassing upstream SQLSpec store bases."
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
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/litestar.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
estimated_loc: 245
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
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: "Storage-neutral ArtifactReference value struct"
  - id: sqlspec-spn
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: "Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import"
---

# Task `t02`: Implement SQLSpec Spanner ADK, Events & Litestar Extension Stores

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^spanner-drv] [^storage-art] [^sqlspec-spn].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py` — Google Cloud Spanner & Spangres SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

## Contracts & Edge Cases

- Implement zero-SDK `SpannerSyncADKStore`/`SpannerAsyncADKStore` (`provide_session(transaction=False)` lock-free snapshots on reads, `transaction=True` on writes), ScaNN/FTS/RRF/PropertyGraph `SpannerSyncADKMemoryStore`/`SpannerAsyncADKMemoryStore`, `SpannerSyncADKArtifactStore`/`SpannerAsyncADKArtifactStore` constructing `vertebra.storage.ArtifactReference` directly with ZERO `vertebra.db.adapters.bigquery` imports, hash-sharded `MOD(FARM_FINGERPRINT(...))` `BYTES` `SpannerEventQueueStore` (valid `ROW DELETION POLICY` / `TTL INTERVAL`, no invalid TTL on `updated_at`), and `SpannerLitestarStore`.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

## Test Matrix

- Parametrized unit/contract tests for `SpannerSyncADKStore`/`SpannerAsyncADKStore`, memory search, `ArtifactReference` metadata, `SpannerEventQueueStore`, and `SpannerLitestarStore` TTL/upsert/delete.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/sqlspec/adapters/spanner/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^spanner-drv]: `src/py/vertebra/db/adapters/spanner/driver.py` — Native Google Cloud Spanner & Spangres sync and async drivers.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
