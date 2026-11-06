---
type: Task
id: db-engine-primitives:t06
title: Delete vertebra/db/pipeline.py, route SQLSpec execute_stack via SyncPipelineCapable and AsyncPipelineCapable, and update documentation
description: "Delete `src/py/vertebra/db/pipeline.py` (`SyncQueryPipeline`, `AsyncQueryPipeline`, `CTEStep`, `RelOverlay`, `PipelineDialect`), update `execute_stack` in `vertebra.extensions.sqlspec.{adapters,bigquery,spanner}` to dispatch via `isinstance(self.native_driver, SyncPipelineCapable)` (sync) and `isinstance(self.native_driver, AsyncPipelineCapable)` (async) when supported and sequential/transactional execution otherwise, delete `docs/usage/query-pipelines.md` with a redirect in `zensical.toml`, and synchronize `README.md`, `docs/`, and OKF `sources:`."
state: pending
priority: P1
verification_strategy: characterization
depends_on:
  - db-engine-primitives:t03
  - db-engine-primitives:t04
files:
  - src/py/vertebra/db/pipeline.py
  - src/py/vertebra/extensions/sqlspec/__init__.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlspec/bigquery.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
  - README.md
  - zensical.toml
  - docs/index.md
  - docs/usage/index.md
  - docs/usage/databases.md
  - docs/usage/extensions.md
  - docs/usage/query-pipelines.md
  - docs/reference/index.md
  - docs/reference/db.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/domains/index.md
tests:
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_pg_gateway.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
  - src/py/tests/unit/docs/test_examples.py
plan_revision: 4
plan_commit: "f741701"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:49:00Z
updated_at: 2026-11-04T01:12:00Z
commit: null
tags: [db, pipeline, sqlspec, docs, clean-break]
---

## Objective

Delete `src/py/vertebra/db/pipeline.py` (`SyncQueryPipeline`, `AsyncQueryPipeline`, `CTEStep`, `RelOverlay`, `PipelineDialect`), update `execute_stack` in `vertebra.extensions.sqlspec.{adapters,bigquery,spanner}` to dispatch via `isinstance(self.native_driver, SyncPipelineCapable)` (sync) and `isinstance(self.native_driver, AsyncPipelineCapable)` (async) when supported and sequential/transactional execution otherwise, delete `docs/usage/query-pipelines.md` with a redirect in `zensical.toml`, and synchronize `README.md`, `docs/`, and OKF `sources:`.

## Context

- **Verification strategy (`characterization`)**: Characterize `execute_stack` across `SyncPipelineCapable` / `AsyncPipelineCapable` native drivers (batching all operations in one `driver.pipeline()` context) and non-pipeline drivers (transactional/sequential execution, Spanner `execute_batch_dml` for pure DML stacks, and `continue_on_error` error capture without silent `with suppress(Exception):`), then delete `db/pipeline.py` and verify `make lint-all` (`make review-check`) and `make docs`.
- **Architectural invariants**:
  - Never compile CTE strings or swallow compilation errors inside `execute_stack`.
  - Exact `StatementStack` -> `SyncPipelineCapable` / `AsyncPipelineCapable` and sequential fallback mapping in `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack` (`src/py/vertebra/extensions/sqlspec/adapters.py`):
    - Note that `sqlspec.core.result._base.StackResult` is mypyc-compiled (`StackResult(result=..., rows_affected=..., error=..., warning=..., metadata=...)`, with no `rows=` keyword argument) and `StatementStack` pushes SQL queries with `op.method == "execute"` (`ALLOWED_METHODS = ("execute", "execute_many", "execute_script", "execute_arrow")`).
    - When `isinstance(self.native_driver, SyncPipelineCapable)` (in `BaseSyncDriver.execute_stack`) or `isinstance(self.native_driver, AsyncPipelineCapable)` (in `BaseAsyncDriver.execute_stack`) and `not continue_on_error` and `len(operations) > 1` and `all(getattr(op, "method", "execute") == "execute" for op in operations)`, enter `with self.native_driver.pipeline() as pipe:` (`async with self.native_driver.pipeline() as pipe:` in async), inspect each normalized `(sql_text, params)` operation's leading SQL keyword (`SELECT` or `WITH` -> `pipe.select(sql_text, params)`, otherwise -> `pipe.execute(sql_text, params)`), collect `raw_results = pipe.results()` (`await pipe.results()` in async), and return `tuple(StackResult.from_sql_result(SQLResult(statement=SQL(sql_text), data=r, rows_affected=len(r), operation_type="SELECT")) if isinstance(r, list) else StackResult(rows_affected=int(r)) for (sql_text, _), r in zip(normalized_ops, raw_results, strict=True))`.
    - In the transactional sequential fallback (used for non-pipeline drivers, `continue_on_error=True`, single-operation stacks, or stacks containing `"execute_many"` / `"execute_script"` / `"execute_arrow"`), dispatch each operation by `op.method`: `"execute"` with leading `SELECT`/`WITH` calls `rows = self.select(sql_text, *args)` (`await self.select(...)`) -> `StackResult.from_sql_result(SQLResult(statement=SQL(sql_text), data=rows, rows_affected=len(rows), operation_type="SELECT"))`; DML `"execute"` calls `rc = self.execute(sql_text, *args)` -> `StackResult(rows_affected=rc)`; `"execute_many"` calls `rc = self.execute_many(sql_text, *args)` -> `StackResult(rows_affected=rc)`; `"execute_script"` calls `res = self.execute_script(sql_text, *args)` -> `StackResult.from_sql_result(res)` (or `StackResult(rows_affected=int(res))`); and `"execute_arrow"` calls `arrow_res = self.select_to_arrow(sql_text, *args)` -> `StackResult.from_arrow_result(arrow_res)`.
  - Update `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`sources:` frontmatter), `.agents/bundles/product/product.md`, and `.agents/bundles/knowledge/index.md` in the same commit that deletes `src/py/vertebra/db/pipeline.py` so `make review-check` inside `make lint-all` passes immediately.
  - Remove `"Query Pipelines & CTE Stacking" = "usage/query-pipelines.md"` from `zensical.toml` `nav` and add `"usage/query-pipelines.md" = "usage/databases.md"` under `[project.plugins.redirects.redirect_maps]`.

## Steps

1. **Characterize `execute_stack` with `SyncPipelineCapable` / `AsyncPipelineCapable` and assert `vertebra.db.pipeline` removal**:
   - In `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_sqlspec_bigquery.py`, and `src/py/tests/unit/extensions/test_sqlspec_spanner.py`:
     - Add tests with deterministic `SyncPipelineCapable` and `AsyncPipelineCapable` driver doubles verifying `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack` enter `self.native_driver.pipeline()` once when supported, `not continue_on_error`, and `len(operations) > 1`, routing `SELECT`/`WITH` statements to `pipe.select(...)` and DML statements to `pipe.execute(...)`, and falling back to transactional sequential execution otherwise without swallowing driver errors.
     - Update `test_spanner_sync_config_sessions_connection_shim_and_data_dictionary` in `src/py/tests/unit/extensions/test_sqlspec_spanner.py` to pass two independent `SELECT` statements (`SELECT user_id, order_id, total FROM child_orders` and `SELECT COUNT(*) AS cnt FROM child_orders`) instead of the legacy CTE step reference `FROM step_0`, and assert both `stack_out[0]` and `stack_out[1]` return expected rows while pure DML stacks continue using `execute_batch_dml`.
     - Assert `importlib.import_module("vertebra.db.pipeline")` raises `ModuleNotFoundError`, `hasattr(vertebra.extensions.sqlspec, "SyncQueryPipeline") is False`, and `hasattr(vertebra.extensions.sqlspec, "AsyncQueryPipeline") is False`.
   - Remove obsolete `SyncQueryPipeline` / `AsyncQueryPipeline` / `CTEStep` / `RelOverlay` unit tests from `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` and `src/py/tests/unit/db/test_pg_gateway.py`.

2. **Delete `db/pipeline.py`, update `extensions/sqlspec`, and update `README.md`, `docs/`, `zensical.toml`, and OKF `sources:`**:
   - Delete `src/py/vertebra/db/pipeline.py` (`git rm`).
   - Remove `SyncQueryPipeline`, `AsyncQueryPipeline`, and `PipelineDialect` from `src/py/vertebra/extensions/sqlspec/__init__.py`, `adapters.py`, `bigquery.py` (including dead `self._pipeline` attributes), and `spanner.py`, wiring the `SyncPipelineCapable` / `AsyncPipelineCapable` branches in `execute_stack`.
   - Delete `docs/usage/query-pipelines.md` (`git rm`), update `zensical.toml` (`nav` + `[project.plugins.redirects.redirect_maps]` `"usage/query-pipelines.md" = "usage/databases.md"`), update `README.md` (replacing `SyncQueryPipeline` / `AsyncQueryPipeline` with native `SyncPipelineCapable` / `AsyncPipelineCapable` `driver.pipeline()`), update `docs/index.md`, `docs/usage/index.md`, `docs/usage/databases.md`, `docs/usage/extensions.md`, `docs/reference/index.md`, and `docs/reference/db.md`, and update `.agents/bundles/product/product.md` (replacing `SyncQueryPipeline`/`AsyncQueryPipeline`, `vertebra.db` `ChannelBus`, and `vertebra.db` `register_gcp_database_tools` references inside the bounded truth block with `SyncPipelineCapable`/`AsyncPipelineCapable`, `vertebra.server._channels.ChannelBus` / `vertebra.framework.channels.ChannelBus`, and `vertebra.agent.tools`), `.agents/bundles/knowledge/index.md`, `.agents/bundles/knowledge/domains/index.md`, and `sources:` in `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`.

3. **Run verification gates**:
   - Run `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/docs/test_examples.py`, `make lint-all`, and `make docs`.

## Verification

- **Initial**:
  - `uv run pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -k pipeline_capable` -> fails before the `SyncPipelineCapable` / `AsyncPipelineCapable` branch is wired in `execute_stack`.
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/docs/test_examples.py` -> exits `0`.
  - `make lint-all && make docs` -> exits `0`.

## Acceptance Criteria

- [ ] R6: `src/py/vertebra/db/pipeline.py` and `docs/usage/query-pipelines.md` are deleted, `zensical.toml` redirects `usage/query-pipelines.md` to `usage/databases.md`, `README.md` and OKF `sources:` carry zero stale `SyncQueryPipeline`/`AsyncQueryPipeline`/`db/pipeline.py` references, `execute_stack` dispatches via `SyncPipelineCapable` / `AsyncPipelineCapable` without `with suppress(Exception):`, and `make lint-all` + `make docs` exit `0`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
