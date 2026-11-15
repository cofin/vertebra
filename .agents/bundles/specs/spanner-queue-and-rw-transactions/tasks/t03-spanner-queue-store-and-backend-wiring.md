---
type: Task
id: spanner-queue-and-rw-transactions:t03
flow_id: spanner-queue-and-rw-transactions
title: Implement Sync/AsyncSpannerQueueStore and Remediate SpannerQueueBackend
description: Build SyncSpannerQueueStore and AsyncSpannerQueueStore across GoogleSQL and Spangres, fix SpannerQueueBackend claim/lease/leak defects, and auto-wire QueueService.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- spanner-queue-and-rw-transactions:t02
files:
- src/py/vertebra/db/adapters/spanner/queue.py
- src/py/vertebra/db/adapters/spanner/__init__.py
- src/py/vertebra/framework/queues/spanner.py
- src/py/vertebra/framework/queues/base.py
tests:
- src/py/tests/unit/db/test_spanner_queue_and_cdc.py
- src/py/tests/unit/framework/test_distributed_queues.py
read_set:
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/db/adapters/spanner/_sync.py
- src/py/vertebra/db/adapters/spanner/_async.py
- src/py/vertebra/db/adapters/spanner/core.py
- src/py/vertebra/framework/queues/spanner.py
- src/py/vertebra/framework/queues/base.py
estimated_loc: 245
tags:
- spanner
- spangres
- queues
- transactions
sources:
- id: spn-driver
  resource: src/py/vertebra/db/adapters/spanner/_sync.py::SyncSpannerDriver
  title: SyncSpannerDriver and AsyncSpannerDriver run_in_transaction
- id: queue-cap
  resource: src/py/vertebra/db/driver/capabilities.py
  title: SyncQueueStore and AsyncQueueStore protocols
- id: queue-spanner
  resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
  title: SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL
- id: queue-base
  resource: src/py/vertebra/framework/queues/base.py::QueueService
  title: QueueService backend registry
---

# Task `t03`: Implement Sync/AsyncSpannerQueueStore and Remediate SpannerQueueBackend

## Objective & Scope Boundary

Create `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` in `src/py/vertebra/db/adapters/spanner/queue.py` executing atomic select-then-update claims inside `run_in_transaction` across GoogleSQL and Spangres, export them via `spanner/__init__.py`, fix `src/py/vertebra/framework/queues/spanner.py` by delegating to `SyncSpannerQueueStore` / `AsyncSpannerQueueStore`, and auto-wire `SpannerQueueBackend` in `QueueService` (`framework/queues/base.py`) per [`../spec.md`](../spec.md) [^spn-driver] [^queue-cap] [^queue-spanner] [^queue-base].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore` and `AsyncQueueStore` protocols.
- `src/py/vertebra/db/adapters/spanner/_sync.py` & `_async.py` — interactive `run_in_transaction` sync/async driver methods.
- `src/py/vertebra/db/adapters/spanner/core.py` — GoogleSQL `@param` and Spangres `$1` parameter encoding.
- `src/py/vertebra/framework/queues/spanner.py` — broken `dequeue()`, GoogleSQL-only SQL, unconditional `acquire_lease()`, and unbounded `_receipt_index`.
- `src/py/vertebra/framework/queues/base.py` — `QueueService` backend registry (`QueueBackendSettings(kind="spanner")`).

## Contracts & Edge Cases

- Implement `SyncSpannerQueueStore` (`SyncQueueStore`) and `AsyncSpannerQueueStore` (`AsyncQueueStore`) over `SyncSpannerDriver` and `AsyncSpannerDriver` with deterministic hash-prefixed `shard_id` keys, executing `SELECT ... FOR UPDATE` followed by `UPDATE vertebra_queue_jobs SET status = 'running', attempt = attempt + 1, locked_until = ..., consumer_name = ..., receipt_handle = ... THEN RETURN *` (`RETURNING *` on Spangres) atomically inside `driver.run_in_transaction(...)`.
- Provide dialect-specific SQL for both GoogleSQL (`@param`, `@{FORCE_INDEX=...}`, `UNNEST(@lanes)`, `INSERT OR UPDATE`) and Spangres (`$1`, `ANY($1)`, `ON CONFLICT DO UPDATE`), plus conditional `acquire_lease` (`expires_at <= now OR owner_id = effective_owner`) so active cron/maintenance leases are never stolen.
- Auto-construct `SyncSpannerQueueStore` / `AsyncSpannerQueueStore` from configured `SyncSpannerDriver` / `AsyncSpannerDriver` or `spanner://` / `spangres://` DSNs when `store=` / `executor=` is omitted (probing `vertebra-core[spanner]`), eliminate the 6-arg call to 3-param `CLAIM_SPANNER_JOBS_SQL`, replace the unbounded `_receipt_index` dict with deterministic `(shard_id, task_id, attempt)` receipt tokens so timed-out tasks never leak memory, and verify `QueueService(QueueBackendSettings(kind='spanner'))`, `litestar-queues` `StoreQueueBackend`, and Django `TaskBackend` execute cleanly.

## Test Matrix

- Parametrized GoogleSQL (`spanner://`) and Spangres (`spangres://`) unit tests verifying atomic `run_in_transaction` `SELECT ... FOR UPDATE` -> `UPDATE ... THEN RETURN`/`RETURNING` claim, `SpannerAbortedError` contention retry, attempt-fenced `ack`/`nack`/`heartbeat`, DLQ transitions, non-stealing `acquire_lease`, auto-store construction, bounded memory on unacknowledged timeouts, `StoreQueueBackend`, and sync Django `enqueue`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/framework/test_distributed_queues.py
```

## Acceptance Criteria

- [ ] `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` across both GoogleSQL and Spangres with zero framework imports.
- [ ] `SpannerQueueBackend` executes atomic select-then-update claims across GoogleSQL and Spangres with zero parameter mismatches, lease theft, or `_receipt_index` memory leaks.
- [ ] Zero regressions in the fast verification gate.

[^spn-driver]: `src/py/vertebra/db/adapters/spanner/_sync.py::SyncSpannerDriver` — SyncSpannerDriver and AsyncSpannerDriver run_in_transaction.
[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py` — SyncQueueStore and AsyncQueueStore protocols.
[^queue-spanner]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL.
[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — QueueService backend registry.
