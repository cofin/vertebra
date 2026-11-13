---
type: Task
id: "spanner-queue-and-rw-transactions:ch02-spanner-queue-store-and-docs:t02"
flow_id: spanner-queue-and-rw-transactions
chapter_id: ch02-spanner-queue-store-and-docs
title: "Remediate SpannerQueueBackend Defects and Auto-Wire QueueService, litestar-queues & Django"
description: "Fix 6-arg CLAIM_SPANNER_JOBS_SQL call, missing UPDATE, Spangres SQL, lease stealing, and _receipt_index leak in framework/queues/spanner.py."
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
  - src/py/vertebra/framework/queues/spanner.py
  - src/py/vertebra/framework/queues/base.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
read_set:
  - src/py/vertebra/framework/queues/spanner.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/db/adapters/spanner/queue.py
  - src/py/vertebra/extensions/litestar/queues/backend.py
estimated_loc: 220
tags:
  - spanner
  - spangres
  - queues
sources:
  - id: queue-spanner
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: "SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL"
  - id: queue-base
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: "QueueService backend registry"
---

# Task `t02`: Remediate SpannerQueueBackend Defects and Auto-Wire QueueService, litestar-queues & Django

## Objective & Scope Boundary

Fix `src/py/vertebra/framework/queues/spanner.py` by delegating to `SyncSpannerQueueStore` / `AsyncSpannerQueueStore` and auto-wiring `SpannerQueueBackend` in `QueueService` per [`../spec.md`](../spec.md) [^queue-spanner] [^queue-base].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/spanner.py` — broken `dequeue()`, GoogleSQL-only SQL, unconditional `acquire_lease()`, and unbounded `_receipt_index`.
- `src/py/vertebra/db/adapters/spanner/queue.py` — `SyncSpannerQueueStore` and `AsyncSpannerQueueStore`.

## Contracts & Edge Cases

- Auto-construct `SyncSpannerQueueStore` / `AsyncSpannerQueueStore` from configured `SyncSpannerDriver` / `AsyncSpannerDriver` or `spanner://` / `spangres://` DSNs when `store=` / `executor=` is omitted (probing `vertebra-core[spanner]`).
- Eliminate the 6-arg call to 3-param `CLAIM_SPANNER_JOBS_SQL`, execute atomic select-then-update claims across GoogleSQL and Spangres, enforce conditional `acquire_lease`, and replace the unbounded `_receipt_index` dict with deterministic `(shard_id, task_id, attempt)` receipt tokens so timed-out tasks never leak memory.
- Verify `QueueService(QueueBackendSettings(kind='spanner'))`, `litestar-queues` `StoreQueueBackend`, and Django `TaskBackend` work seamlessly over `SyncSpannerQueueStore` / `AsyncSpannerQueueStore`.

## Test Matrix

- Parametrized GoogleSQL (`spanner://`) and Spangres (`spangres://`) tests covering auto-store construction, atomic dequeue/ack/nack, lease contention, bounded memory on unacknowledged timeouts, `StoreQueueBackend`, and sync Django `enqueue`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/framework/test_distributed_queues.py
```

## Acceptance Criteria

- [ ] `SpannerQueueBackend` executes atomic select-then-update claims across GoogleSQL and Spangres with zero parameter mismatches, lease theft, or `_receipt_index` memory leaks.
- [ ] Zero regressions in the chapter fast verification gate.

[^queue-spanner]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL.
[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — QueueService backend registry.
