---
type: Task
id: db-engine-primitives:t06
title: Delete vertebra/db/pipeline.py, route SQLSpec execute_stack via SyncPipelineCapable and AsyncPipelineCapable, and update documentation
description: "Delete `src/py/vertebra/db/pipeline.py` (`SyncQueryPipeline`, `AsyncQueryPipeline`, `CTEStep`, `RelOverlay`, `PipelineDialect`), update `execute_stack` in `vertebra.extensions.sqlspec.{adapters,bigquery,spanner}` to dispatch via `isinstance(self.native_driver, SyncPipelineCapable)` (sync) and `isinstance(self.native_driver, AsyncPipelineCapable)` (async) when supported and sequential/transactional execution otherwise, delete `docs/usage/query-pipelines.md` with a redirect in `zensical.toml`, and synchronize `README.md`, `docs/`, and OKF `sources:`."
state: "in_progress"
priority: P1
verification_strategy: integration_acceptance
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
plan_revision: 8
plan_commit: "b41aae46213761f36af0709c0a3ce6d26b2793e0"
state_revision: 1
claimed_by: "/root/db_t06"
claimed_at: "2026-11-05T02:38:31Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:49:00Z
updated_at: "2026-11-05T02:59:14Z"
commit: null
tags: [db, pipeline, sqlspec, docs, clean-break]
---

## Objective

Delete `src/py/vertebra/db/pipeline.py` (`SyncQueryPipeline`, `AsyncQueryPipeline`, `CTEStep`, `RelOverlay`, `PipelineDialect`), update `execute_stack` in `vertebra.extensions.sqlspec.{adapters,bigquery,spanner}` to dispatch via `isinstance(self.native_driver, SyncPipelineCapable)` (sync) and `isinstance(self.native_driver, AsyncPipelineCapable)` (async) when supported and sequential/transactional execution otherwise, delete `docs/usage/query-pipelines.md` with a redirect in `zensical.toml`, and synchronize `README.md`, `docs/`, and the complete OKF pipeline description and provenance.

## Context

- **Verification strategy (`integration_acceptance`)**: Capture a passing baseline of existing public stack behavior before refactoring. Compose real SQLSpec statement processing with native pipeline capabilities and transactional/sequential fallback, verifying ordered rows and affected counts, parameter preservation, rollback and continue-on-error behavior, negative driver states, and the requested clean-break compiler removal. Do not manufacture an initial failing test for deletion.
- **Architectural invariants**:
  - Never compile CTE strings or swallow compilation errors inside `execute_stack`.
  - Exact `StatementStack` -> `SyncPipelineCapable` / `AsyncPipelineCapable` and sequential fallback mapping in `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack` (`src/py/vertebra/extensions/sqlspec/adapters.py`):
    - Note that `sqlspec.core.result._base.StackResult` is mypyc-compiled (`StackResult(result=..., rows_affected=..., error=..., warning=..., metadata=...)`, with no `rows=` keyword argument) and `StatementStack` pushes SQL queries with `op.method == "execute"` (`ALLOWED_METHODS = ("execute", "execute_many", "execute_script", "execute_arrow")`).
    - When the existing native-capability eligibility conditions hold (`not continue_on_error`, multiple operations, and every method is `execute`), prepare/compile each operation once through existing SQLSpec processing and retain the prepared `SQL` object with compiled SQL and bound parameters. Classify row-producing operations using `prepared_statement.returns_rows()`; leading comments, CTE DML, and DML `RETURNING` must retain upstream semantics. Queue `pipe.select(...)` for row-producing operations and `pipe.execute(...)` otherwise, flush with `pipe.results()` (await asynchronously), and build ordered `StackResult` values from actual rows/counts using the retained prepared statement. Enforce equal operation/result counts and propagate failures; never reconstruct or reparse SQL text for classification or result metadata.
    - In transactional/sequential fallback, preserve upstream operation argument/keyword preparation and dispatch by `op.method`. For `execute`, use the prepared statement's `returns_rows()` metadata to retain row results versus affected counts; support `execute_many`, `execute_script`, and `execute_arrow` using their normal result contracts. Use real statement/results objects and preserve bound parameters, ordered outputs, fail-fast rollback, and `continue_on_error` capture without swallowing errors or guessing from SQL prefixes.
  - Default statement configuration preserves native transport binds while retaining metadata from the same prepared statement. `BaseSyncDriver` and `BaseAsyncDriver` provide a private class-owned default-config hook consumed by direct construction and `SyncAdapterConfig` / `AsyncAdapterConfig` session construction. Concrete PostgreSQL sync uses positional pyformat execution, PostgreSQL async numeric execution, and MySQL sync/async positional pyformat execution with QMARK input processing; other generic defaults preserve detected native formats. Do not add generic engine-name dispatch, optional upstream adapter imports, a profile registry, raw/metadata SQL splitting, or heuristic classification. Explicit caller `StatementConfig` values remain authoritative.
  - BigQuery and Spanner wrappers own their parser and bind defaults for direct and config-created sync/async sessions. BigQuery uses the supported BigQuery parser and NAMED_AT parameters. GoogleSQL Spanner uses the supported `spanner` parser and NAMED_AT parameters; Spangres retains matching parser/bind conventions derived from resolved native configuration before base initialization. Verify exact native SQL, bind values, row results and DML counts through external client ports, including PostgreSQL/MySQL default sessions and explicit configuration preservation. Broader adapter-profile parity remains in its owning follow-up flow.
  - Update `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`description`, opening prose, pipeline section, `sources:`, inline links, and provenance footnotes), `.agents/bundles/product/product.md`, and `.agents/bundles/knowledge/index.md` in the same commit that deletes `src/py/vertebra/db/pipeline.py`. The chapter describes native capability-based pipelining and contains no references to the deleted compiler or source path before `t07` begins.
  - Remove `"Query Pipelines & CTE Stacking" = "usage/query-pipelines.md"` from `zensical.toml` `nav` and add `"usage/query-pipelines.md" = "usage/databases.md"` under `[project.plugins.redirects.redirect_maps]`.

## Steps

1. **Capture the passing public stack baseline, then add native capability, fallback, and removal acceptance coverage**:
   - In `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_sqlspec_bigquery.py`, and `src/py/tests/unit/extensions/test_sqlspec_spanner.py`:
     - Exercise the actual SQLSpec drivers with deterministic native capability/database ports (using a real local database where available): verify one pipeline context for eligible stacks, upstream row-producing classification for leading comments/CTE DML/RETURNING, correct binds and ordered rows/counts, and transactional/sequential fallback with observable rollback and error propagation. Do not mock the SQLSpec SUT or its statement/result processing.
     - Update `test_spanner_sync_config_sessions_connection_shim_and_data_dictionary` in `src/py/tests/unit/extensions/test_sqlspec_spanner.py` to pass two independent `SELECT` statements (`SELECT user_id, order_id, total FROM child_orders` and `SELECT COUNT(*) AS cnt FROM child_orders`) instead of the legacy CTE step reference `FROM step_0`, and assert both `stack_out[0]` and `stack_out[1]` return expected rows while pure DML stacks continue using `execute_batch_dml`.
     - Assert `importlib.import_module("vertebra.db.pipeline")` raises `ModuleNotFoundError`, `hasattr(vertebra.extensions.sqlspec, "SyncQueryPipeline") is False`, and `hasattr(vertebra.extensions.sqlspec, "AsyncQueryPipeline") is False`.
   - Remove obsolete `SyncQueryPipeline` / `AsyncQueryPipeline` / `CTEStep` / `RelOverlay` unit tests from `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` and `src/py/tests/unit/db/test_pg_gateway.py`.

2. **Delete `db/pipeline.py`, update `extensions/sqlspec`, and update `README.md`, `docs/`, `zensical.toml`, and OKF `sources:`**:
   - Delete `src/py/vertebra/db/pipeline.py` (`git rm`).
   - Remove `SyncQueryPipeline`, `AsyncQueryPipeline`, and `PipelineDialect` from `src/py/vertebra/extensions/sqlspec/__init__.py`, `adapters.py`, `bigquery.py` (including dead `self._pipeline` attributes), and `spanner.py`, wiring the `SyncPipelineCapable` / `AsyncPipelineCapable` branches in `execute_stack`.
   - Delete `docs/usage/query-pipelines.md` (`git rm`), update `zensical.toml` (`nav` + `[project.plugins.redirects.redirect_maps]` `"usage/query-pipelines.md" = "usage/databases.md"`), update `README.md` (replacing `SyncQueryPipeline` / `AsyncQueryPipeline` with native `SyncPipelineCapable` / `AsyncPipelineCapable` `driver.pipeline()`), update `docs/index.md`, `docs/usage/index.md`, `docs/usage/databases.md`, `docs/usage/extensions.md`, `docs/reference/index.md`, and `docs/reference/db.md`, and update `.agents/bundles/product/product.md` (replacing `SyncQueryPipeline`/`AsyncQueryPipeline`, `vertebra.db` `ChannelBus`, and `vertebra.db` `register_gcp_database_tools` references inside the bounded truth block with `SyncPipelineCapable`/`AsyncPipelineCapable`, `vertebra.server._channels.ChannelBus` / `vertebra.framework.channels.ChannelBus`, and `vertebra.agent.tools`), `.agents/bundles/knowledge/index.md`, `.agents/bundles/knowledge/domains/index.md`, and `sources:` in `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`.
   - Replace `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md::Single-Round-Trip CTE Pipelines` with a native pipelining section describing `SyncPipelineCapable` / `AsyncPipelineCapable`, `driver.pipeline()`, ordered results, and the supported-stack versus transactional/sequential fallback conditions implemented by `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack`. Remove the `CTEStep`, `RelOverlay`, `SyncQueryPipeline`, and `AsyncQueryPipeline` tables and compilation claims; remove the `src-db-pipeline-py` source resource, every inline link to `db/pipeline.py`, and its provenance footnote. Update the frontmatter description and opening paragraph to describe generic CDC/tenancy, adapter-owned primitives, and native pipeline capabilities; cite `db/driver/capabilities.py` and `extensions/sqlspec/adapters.py` with matching source resources and provenance footnotes. Keep the existing bounded-spill and relocated-Spanner documentation accurate and leave the RLS API change to `t07`.

3. **Run verification gates**:
   - Run `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/docs/test_examples.py`, `make lint-all`, and `make docs`.

## Verification

- **Initial (GREEN baseline)**:
  - Run the complete final focused pytest command below before production edits; it exits `0` on existing public stack behavior. Record this baseline, then add native-capability and negative-state acceptance cases before changing their implementation.
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/docs/test_examples.py` -> exits `0`.
  - `make lint-all && make docs` -> exits `0`.
  - `rg -n 'SyncQueryPipeline|AsyncQueryPipeline|CTEStep|RelOverlay|db/pipeline[.]py' .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` -> exits `1` with no matches, covering prose, inline links, source resources, and provenance footnotes.

## Acceptance Criteria

- [x] R6: `src/py/vertebra/db/pipeline.py` and `docs/usage/query-pipelines.md` are deleted, `zensical.toml` redirects `usage/query-pipelines.md` to `usage/databases.md`, `README.md` and the data knowledge chapter carry zero stale `SyncQueryPipeline`/`AsyncQueryPipeline`/`CTEStep`/`RelOverlay`/`db/pipeline.py` references across descriptions, prose, inline links, `sources:`, and provenance footnotes, `execute_stack` dispatches via `SyncPipelineCapable` / `AsyncPipelineCapable` without `with suppress(Exception):`, and `make lint-all` + `make docs` exit `0`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->

- [2026-11-05] A pre-deletion baseline passes117 cases. Verification is integration acceptance, matching existing-contract composition; the former failing-initial instruction contradicted characterization. Installed SQLSpec metadata proves prefix guessing misclassifies leading-comment SELECT, CTE UPDATE, and INSERT RETURNING, so retain prepared statement returns_rows() semantics and avoid reparsing.

- [2026-11-05] Actual native acceptance exposes default placeholder rewriting and an unsupported googlesql parser identity. Class-owned execution defaults preserve native binds and prepared row metadata; GCP wrappers own supported parser defaults. Profile probes distinguish MySQL pyformat preservation without classification from a QMARK-input/native-output profile that retains both.

- [2026-11-05] Pre-deletion baseline117 passes. Final focused integration acceptance173 passes with the optional real psycopg converter enabled; prepared statement operation types, native binds, batch filters/configuration, Arrow keyword/prebound parameters and GCP cursor normalization are verified. Correctness review reproduces Arrow/batch preparation failures before their fixes. Scoped Ruff/MyPy pass. Final aggregate checkpoint runs after these source corrections.

- [2026-11-05] Final focused acceptance175 passes without skips, including existing BigQuery/Spanner Arrow options forwarded through class-owned option declarations. Actual optional psycopg converter regressions run explicitly. Scoped Ruff, MyPy and diff checks pass; no generic engine dispatch is introduced.

- [2026-11-05] Final Phase2 aggregate exits0: make py-test1981 passed69 skipped170 xfailed, make lint-all, make docs and make review-check. All adopted scoped correctness findings reverified clean.
