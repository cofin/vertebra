---
type: Task
id: "native-duckdb-driver:t05"
title: "Implement SyncDuckDBPipeline and AsyncDuckDBPipeline with delayed commit and savepoint nesting"
description: "Create src/py/vertebra/db/adapters/duckdb/pipeline.py implementing SyncDuckDBPipeline and AsyncDuckDBPipeline with queued batch execution, savepoint nesting when inside an active transaction, delayed commit on clean context exit, and automatic rollback on failure."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-duckdb-driver:t04"
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/duckdb/__init__.py
files:
  - src/py/vertebra/db/adapters/duckdb/pipeline.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/duckdb/__init__.py
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 220
plan_revision: 4
plan_commit: d7a629b1
state_revision: 1
claimed_by: agent
claimed_at: "2026-11-09T15:55:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T16:31:00Z"
last_verified_commit: 1add9b98
verification_evidence: "pytest src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/integration/db/test_driver_conformance.py -k duckdb"
created_at: "2026-11-09T15:46:00Z"
updated_at: "2026-11-09T16:31:00Z"
commit: null
sources:
  - id: duckdb-pipeline-py
    resource: src/py/vertebra/db/adapters/duckdb/pipeline.py
    title: SyncDuckDBPipeline and AsyncDuckDBPipeline delayed-commit batch pipeline module
  - id: duckdb-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: Python SyncDuckDBDriver and AsyncDuckDBDriver target cutover module
tags:
  - python
  - duckdb
  - pipeline
  - transaction
---

# Task t05: Implement `SyncDuckDBPipeline` and `AsyncDuckDBPipeline` with Delayed Commit and Savepoint Nesting

## Objective & Scope Boundary

Implement `SyncDuckDBPipeline` and `AsyncDuckDBPipeline` in `src/py/vertebra/db/adapters/duckdb/pipeline.py`, wire `SyncPipelineCapable` / `AsyncPipelineCapable` `.pipeline(transaction=True)` on `SyncDuckDBDriver` and `AsyncDuckDBDriver` in `src/py/vertebra/db/adapters/duckdb/driver.py`, and export both pipeline classes in `src/py/vertebra/db/adapters/duckdb/__init__.py` [^duckdb-pipeline-py] [^duckdb-py].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/duckdb/pipeline.py`:
  - `SyncDuckDBPipeline`: context manager queuing `execute(sql, parameters=None)` and `select(sql, parameters=None, *, schema_type=None)` operations; executes queued operations when `results()` is called, using a top-level transaction (`begin()` with delayed `commit()` on clean `__exit__` and `rollback()` on exception) when `driver.in_transaction` is `False`, or a nested savepoint (`savepoint("_vtb_pipe_<n>")`, `release_savepoint` on clean `__exit__`, `rollback_to_savepoint` on exception) when `driver.in_transaction` is `True`.
  - `AsyncDuckDBPipeline`: async context manager with identical delayed-commit and savepoint-nesting semantics (`__aenter__`, `__aexit__`, `execute`, `select`, `await results()`).

## Acceptance Criteria

- [x] `SyncDuckDBDriver` satisfies `isinstance(driver, SyncPipelineCapable)` and `AsyncDuckDBDriver` satisfies `isinstance(driver, AsyncPipelineCapable)`.
- [x] `SyncDuckDBPipeline` and `AsyncDuckDBPipeline` execute queued `execute` and `select` operations atomically in `results()`, delay `commit()` / `release_savepoint()` until clean context exit (`__exit__` / `__aexit__`), and roll back if an error occurs either during `results()` or inside the `with` / `async with` block after `results()`.
- [x] Nested pipelines inside an outer transaction use savepoints so a failed inner pipeline rolls back only its own operations without aborting the outer transaction.

[^duckdb-pipeline-py]: `src/py/vertebra/db/adapters/duckdb/pipeline.py` — `SyncDuckDBPipeline` and `AsyncDuckDBPipeline` delayed-commit batch pipeline module.
[^duckdb-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Python `SyncDuckDBDriver` and `AsyncDuckDBDriver` target cutover module.
