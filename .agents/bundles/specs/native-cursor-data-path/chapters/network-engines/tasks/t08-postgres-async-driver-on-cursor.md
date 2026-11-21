---
type: Task
id: "native-cursor-data-path:network:t08"
chapter_id: network-engines
title: "Asynchronous PostgreSQL driver on the native cursor with leases at checkout"
description: "AsyncPostgreSQLDriver query methods call the native cursor, the broker lease moves to client checkout, and project_query_rows is deleted."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t07"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver"
  - "src/py/vertebra/db/adapters/postgres/_async.py:350-420"
  - "src/py/vertebra/db/adapters/postgres/_async.py:830-900"
  - "src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver"
  - "src/py/vertebra/db/adapters/postgres/core.py:232-246"
files:
  - "src/py/vertebra/db/adapters/postgres/_async.py"
  - "src/py/vertebra/db/adapters/postgres/core.py"
tests:
  - "src/py/tests/unit/db/test_postgres_native_adapters.py"
estimated_loc: 230
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: pg-async-driver
    resource: "src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver"
    title: "Async native driver query path"
tags: [postgres, python, driver, async]
---

# Asynchronous PostgreSQL driver on the native cursor with leases at checkout

## Objective & Scope Boundary

The async PostgreSQL driver gets the same change as the sync driver in t07. After it, no Python caller uses the one-shot `query` method on either pool class. Task t09 removes those methods.

Out of scope:

- Arrow, COPY, pipeline, listener, and replication paths.
- `SyncPostgresClient.query` and `AsyncPostgresClient.query`. Pipelines and multi-statement helpers still use them.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/postgres/_async.py:830-900` is the async `_run_query`.
- `src/py/vertebra/db/adapters/postgres/_async.py:350-420` are the public query methods.
- `src/py/vertebra/db/adapters/postgres/_sync.py` after t07 is the reference for names and order of operations.

## Contracts & Edge Cases

- Mirror every contract in t07 with `async def` methods: `cursor()` stays a plain method returning `AsyncPostgresCursor`; `_execute_cursor`, `_execute_on_pool`, `execute`, `execute_with_description`, and `execute_many` are coroutines that await the cursor.
- Lease calls use the helpers the async driver uses today for its broker (`_acquire_broker_lease` and `_release_broker_lease` on the async base). They move to `_acquire_pool_client` and `_release_pooled_client` exactly as in t07: the async acquire is awaited, the release callable is attached with `client.attach_lease(...)`, and the driver stores no lease.
- `cursor` takes keyword-only `lazy_begin: bool = False` and passes it to the client cursor. The driver's own query methods use the default.
- Delete `PostgreSQLCore.project_query_rows` and the `_project_public_row` import from `core.py` once `_async.py` no longer calls it.
- After this task `git grep -n '_query_on_pool' -- src/py` returns nothing, and no file under the PostgreSQL adapter package calls `query` on a pool object.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `await driver.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Command | `await driver.execute_with_description('SET ...')` | `([], ())` |
| execute_many | Three sets | Returns `3` |
| No broker | Ten statements with `broker=None` | Recording broker double never called |
| Lease per checkout | Transaction with five statements | One acquire, one release |
| Lease released on cancel | Cancel a task inside a checkout | Acquire count equals release count |
| Acquire refused at checkout | Broker double whose `acquire` raises `RuntimeError` | The statement succeeds; zero releases |
| Error class | Duplicate key | `IntegrityError` |
| Concurrent tasks | Twenty tasks calling `select` on one driver | All succeed with correct rows |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q`
- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_async.py src/py/vertebra/db/adapters/postgres/core.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/_async.py src/py/vertebra/db/adapters/postgres/core.py`
- `test -z "$(git grep -n 'project_query_rows' -- src/py)"`
- `test -z "$(git grep -n '_query_on_pool' -- src/py)"`
- `test -z "$(git grep -n '_lookup_or_cache_statement' -- src/py/vertebra/db/adapters/postgres)"`

## Acceptance Criteria

- [ ] No file under the PostgreSQL adapter package calls `_lookup_or_cache_statement`.
- [ ] `project_query_rows` has no reference under `src/py`.
