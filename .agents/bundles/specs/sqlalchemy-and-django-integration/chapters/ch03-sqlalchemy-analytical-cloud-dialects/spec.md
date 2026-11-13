---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-sqlalchemy-analytical-cloud-dialects
title: "Chapter 03: SQLAlchemy Analytical & Cloud Dialects (DuckDB, BigQuery, Spanner, Spangres) & Entry Points"
description: "Split DuckDB, BigQuery, Spanner, and Spangres dialects onto upstream dialect bases, delete monolithic dialects.py, and repoint all 36 entry points."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - sqlalchemy
  - duckdb
  - bigquery
  - spanner
  - spangres
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Monolithic SQLAlchemy dialect module"
  - id: entry-points
    resource: pyproject.toml
    title: "SQLAlchemy dialect entry points"
  - id: bq-config
    resource: src/py/vertebra/db/adapters/bigquery/config.py
    title: "BigQueryConfig metadata"
  - id: spn-config
    resource: src/py/vertebra/db/adapters/spanner/config.py
    title: "SpannerConfig metadata"
---

# Chapter 03: SQLAlchemy Analytical & Cloud Dialects (DuckDB, BigQuery, Spanner, Spangres) & Entry Points

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `DuckDB`, `BigQuery`, `Spanner`, and `Spangres` dialects hand-roll partial `DefaultDialect` stubs inside `dialects.py`, and `pyproject.toml` points all 36 entry points at `vertebra.extensions.sqlalchemy.dialects` [^sa-dialects] [^entry-points].
- **Target Architecture**: `dialects/{duckdb,bigquery,spanner}.py` subclass `duckdb_engine.Dialect`, `sqlalchemy_bigquery.BigQueryDialect`, and `google.cloud.sqlalchemy_spanner.SpannerDialect` over `vertebra.db.dbapi`, `dialects.py` is deleted, and all 36 entry points target `Sync<Engine>Dialect` / `Async<Engine>Dialect` [^bq-config] [^spn-config].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter completes the per-engine SQLAlchemy dialect split across `DuckDB`, `BigQuery`, `Spanner`, and `Spangres`, deletes monolithic `dialects.py`, and repoints all 36 `<engine>+{vertebra,vtb,vertebra_async,vtb_async}` entry points in `pyproject.toml` [^entry-points].

## 2. Requirements, Concrete Contracts & Architecture

- **R6 (Analytical & Cloud Upstream Bases)**: Subclass `duckdb_engine.Dialect`, `sqlalchemy_bigquery.BigQueryDialect`, and `google.cloud.sqlalchemy_spanner.SpannerDialect` (guarded via `ensure_module` -> `MissingDependencyError`).
- **R7 (36 Entry Points & Clean Cutover)**: Keep all 4 driver names per engine (`+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) across all 9 engines (`36` total) targeting `Sync<Engine>Dialect` / `Async<Engine>Dialect`, and delete `dialects.py`.

```python
class SyncDuckDBDialect(_SyncDialectMixin, DuckDBEngineDialect): ...
class AsyncDuckDBDialect(_AsyncDialectMixin, DuckDBEngineDialect): ...
class SyncBigQueryDialect(_SyncDialectMixin, UpstreamBigQueryDialect): ...
class AsyncBigQueryDialect(_AsyncDialectMixin, UpstreamBigQueryDialect): ...
class SyncSpannerDialect(_SyncDialectMixin, UpstreamSpannerDialect): ...
class AsyncSpannerDialect(_AsyncDialectMixin, UpstreamSpannerDialect): ...
class SyncSpangresDialect(_SyncDialectMixin, UpstreamSpannerDialect): ...
class AsyncSpangresDialect(_AsyncDialectMixin, UpstreamSpannerDialect): ...

def register_sqlalchemy_dialects() -> None: ...
```

## 3. Documentation & OKF Knowledge Plan

Entry-point mappings and optional extra install tables are documented in `ch07-sqlalchemy-django-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R6` DuckDB & BigQuery dialects | `t01` | `dialects/{duckdb,bigquery}.py`, `bigquery/config.py` | `pytest test_gcp_adapters_and_sqlalchemy.py` |
| `R6` Spanner & Spangres dialects | `t02` | `dialects/spanner.py`, `spanner/config.py` | `pytest test_gcp_adapters_and_sqlalchemy.py` |
| `R7` Entry points & `dialects.py` deletion | `t03` | `dialects/__init__.py`, `sqlalchemy/__init__.py`, `pyproject.toml` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-duckdb-and-bigquery-dialects.md](tasks/t01-duckdb-and-bigquery-dialects.md) — Implement `SyncDuckDBDialect` / `AsyncDuckDBDialect` and `SyncBigQueryDialect` / `AsyncBigQueryDialect`.
- [ ] [t02-spanner-and-spangres-dialects.md](tasks/t02-spanner-and-spangres-dialects.md) — Implement `SyncSpannerDialect` / `AsyncSpannerDialect` and `SyncSpangresDialect` / `AsyncSpangresDialect`.
- [ ] [t03-dialect-barrel-and-entrypoints.md](tasks/t03-dialect-barrel-and-entrypoints.md) — Wire `dialects/__init__.py`, delete monolithic `dialects.py`, and repoint all 36 `sqlalchemy.dialects` entry points.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-sqlalchemy-analytical-cloud-dialects` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Monolithic SQLAlchemy dialect module.
[^entry-points]: `pyproject.toml` — SQLAlchemy dialect entry points.
[^bq-config]: `src/py/vertebra/db/adapters/bigquery/config.py` — BigQueryConfig metadata.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig metadata.
