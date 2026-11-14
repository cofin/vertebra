---
type: Task
id: "native-adapter-organization:t01"
flow_id: native-adapter-organization
title: "Extract BigQuery object_tables.py and Spanner mutations.py and split driver barrels"
description: "Separate BigQuery Object Table helpers and Spanner mutation/transaction types from connection configs and split both driver.py modules into _sync.py and _async.py barrels."
status: stable
state: completed
commit: 838c54d1
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/db/adapters/bigquery/object_tables.py
  - src/py/vertebra/db/adapters/spanner/mutations.py
  - src/py/vertebra/db/adapters/bigquery/driver.py
tests:
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_spanner.py
read_set:
  - src/py/vertebra/db/adapters/bigquery/config.py
  - src/py/vertebra/db/adapters/bigquery/core.py
  - src/py/vertebra/db/adapters/bigquery/driver.py
  - src/py/vertebra/db/adapters/spanner/config.py
  - src/py/vertebra/db/adapters/spanner/core.py
  - src/py/vertebra/db/adapters/spanner/driver.py
estimated_loc: 240
tags:
  - database
  - bigquery
  - spanner
  - refactoring
sources:
  - id: bq-config
    resource: src/py/vertebra/db/adapters/bigquery/object_tables.py::ObjectTableConfig
    title: "ObjectRef and ObjectTableConfig extracted into bigquery/object_tables.py"
  - id: sp-config
    resource: src/py/vertebra/db/adapters/spanner/mutations.py::Mutation
    title: "StalenessBound, Mutation, and MutationGroup extracted into spanner/mutations.py"
---

# Task `t01`: Extract BigQuery `object_tables.py` and Spanner `mutations.py` and Split Driver Barrels

## Objective & Scope Boundary

Implement `R1` in [`../spec.md` §2](../spec.md) by extracting BigQuery Object Table types/helpers into `bigquery/object_tables.py`, Spanner consistency/mutation types into `spanner/mutations.py` and snapshots/transactions into `spanner/transactions.py`, and splitting both `driver.py` files into `_sync.py` + `_async.py` with 12-line `driver.py` barrels [^bq-config] [^sp-config]. Out of scope: Oracle, MySQL, SQL Server, DuckDB (`t02`), and DataFusion (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/bigquery/{config,core,driver}.py` (plus 1-line import updates in `bigquery/{__init__,embeddings,tools}.py`).
- `src/py/vertebra/db/adapters/spanner/{config,core,driver}.py` (plus 1-line import updates in `spanner/{__init__,tools}.py`).

## Contracts & Edge Cases

- **BigQuery (`bigquery/object_tables.py`, `config.py`, `core.py`, `_sync.py`, `_async.py`, `driver.py`, `__init__.py`)**:
  - Move `ObjectRef` and `ObjectTableConfig` into `object_tables.py`, typing `hydrate_object_refs_sync(refs, store: SyncObjectStore)` and `hydrate_object_refs_async(refs, store: AsyncObjectStore)` explicitly and encapsulating `build_select_sql`, `list_from_store_sync`, and `list_from_store_async` without `inspect.isawaitable` or broad `except Exception` blocks.
  - Move `BigQueryTableIdentifier` into `bigquery/core.py`, remove moved classes from `bigquery/config.py` with zero re-export shims, split `SyncBigQueryDriver` (`_sync.py`) and `AsyncBigQueryDriver` (`_async.py`) behind `bigquery/driver.py`, and update `bigquery/{__init__,embeddings,tools}.py`.
- **Spanner (`spanner/mutations.py`, `transactions.py`, `config.py`, `core.py`, `_sync.py`, `_async.py`, `driver.py`, `__init__.py`)**:
  - Move `StalenessBound`, `Mutation`, and `MutationGroup` plus `Mutation.coerce`, `MutationGroup.coerce`, and `Mutation.chunk_by_cell_limit` into `spanner/mutations.py` (removing them from `spanner/config.py` and `spanner/core.py` with zero shims).
  - Move `SyncSpannerSnapshot`, `AsyncSpannerSnapshot`, `SyncSpannerTransaction`, and `AsyncSpannerTransaction` into `spanner/transactions.py`, split `SyncSpannerDriver` (`_sync.py`) and `AsyncSpannerDriver` (`_async.py`) behind `spanner/driver.py`, and update `spanner/{__init__,tools}.py`.

## Test Matrix

- Parameterized unit assertions in `test_bigquery.py` and `test_spanner.py` verifying lazy package imports (`from vertebra.db.adapters.bigquery import ObjectRef, ObjectTableConfig`, `from vertebra.db.adapters.spanner import Mutation, MutationGroup, StalenessBound`), `AttributeError` when importing moved symbols from `bigquery.config` or `spanner.config`, typed sync/async `ObjectTableConfig` signing, and `Mutation.chunk_by_cell_limit` bounds.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/bigquery src/py/vertebra/db/adapters/spanner src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py && uv run --no-sync mypy src/py/vertebra/db/adapters/bigquery src/py/vertebra/db/adapters/spanner src/py/vertebra/extensions/sqlspec
```

## Acceptance Criteria

- [x] `bigquery/config.py` and `spanner/config.py` hold only connection configuration structs (`BigQueryConfig`, `BigQueryDryRunResult`, `SpannerConfig`) with zero compatibility shims.
- [x] `bigquery/driver.py` and `spanner/driver.py` are clean barrels over `_sync.py`, `_async.py`, and `transactions.py`, and all unit, Ruff, and Mypy checks pass.

[^bq-config]: `src/py/vertebra/db/adapters/bigquery/object_tables.py::ObjectTableConfig` — ObjectRef and ObjectTableConfig extracted into bigquery/object_tables.py.
[^sp-config]: `src/py/vertebra/db/adapters/spanner/mutations.py::Mutation` — StalenessBound, Mutation, and MutationGroup extracted into spanner/mutations.py.
