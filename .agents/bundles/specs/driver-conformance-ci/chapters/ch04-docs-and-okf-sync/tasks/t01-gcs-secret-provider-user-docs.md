---
type: Task
id: "driver-conformance-ci:ch04:t01"
flow_id: driver-conformance-ci
chapter_id: ch04-docs-and-okf-sync
title: "Document GCS bearer secret providers and DuckDB rotation in storage and analytics docs"
description: "Add SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider usage and API reference directives across storage.md, analytics-and-cdc.md, and docs/reference/storage.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_revision: 4
plan_commit: 37972f2a
depends_on: []
files:
  - docs/usage/storage.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/storage.md
tests: []
read_set:
  - docs/usage/storage.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/storage.md
  - src/py/vertebra/storage/_secrets.py
  - src/py/vertebra/db/adapters/duckdb/config.py
estimated_loc: 155
tags:
  - documentation
  - storage
  - duckdb
  - gcs
sources:
  - id: doc-storage
    resource: docs/usage/storage.md
    title: "Object storage usage documentation"
  - id: doc-analytics
    resource: docs/usage/analytics-and-cdc.md
    title: "Analytics and DuckDB secrets documentation"
  - id: ref-storage
    resource: docs/reference/storage.md
    title: "vertebra.storage API reference page"
---

# Task `t01`: Document GCS Bearer Secret Providers and DuckDB Rotation in Storage and Analytics Docs

## Objective & Scope Boundary

Document `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` with `DuckDBSecretConfig(secret_type="gcs")` across `docs/usage/storage.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/storage.md` per [`../spec.md` §2](../spec.md) [^doc-storage] [^doc-analytics] [^ref-storage]. Out of scope: `docs/usage/databases.md` or OKF files (`t02`–`t04`).

## Bounded Read Manifest

- `docs/usage/{storage,analytics-and-cdc}.md` and `docs/reference/storage.md` — Existing GCS token cache and DuckDB secret sections.
- `src/py/vertebra/storage/_secrets.py` and `src/py/vertebra/db/adapters/duckdb/config.py` — Exact public signatures and validation rules.

## Contracts & Edge Cases

- Show concise inline `DuckDBConfig` + `DuckDBSecretConfig(name="gcs_lake", secret_type="gcs", value_provider=SyncGcsBearerSecretProvider.from_config(GCSConfig(bucket="lake")), scope="gs://lake/")` usage without single-use config variables or `frozen=True, gc=False` struct boilerplate.
- Document `persistent=False` requirement with `value_provider`, positive finite `refresh_interval_seconds`, and automatic `(HTTP 401)`/`(HTTP 403)` invalidation (re-raising without implicit query retry).
- Render `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` in `docs/reference/storage.md` within `verify_docs_artifacts.py` ceilings (`H2+H3 <= 14`, HTML `<= 120,000` bytes).

## Test Matrix

- Zensical site build, docstring leak scan, and `verify_docs_artifacts.py` TOC/byte verification via `make docs`.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` are documented in usage guides and rendered in `docs/reference/storage.md`.
- [ ] `make docs` exits `0` within all TOC and HTML byte budgets.

[^doc-storage]: `docs/usage/storage.md` — Object storage usage documentation.
[^doc-analytics]: `docs/usage/analytics-and-cdc.md` — Analytics and DuckDB secrets documentation.
[^ref-storage]: `docs/reference/storage.md` — `vertebra.storage` API reference page.
