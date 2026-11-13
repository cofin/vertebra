---
type: Task
id: "spanner-queue-and-rw-transactions:ch02-spanner-queue-store-and-docs:t01"
flow_id: spanner-queue-and-rw-transactions
chapter_id: ch02-spanner-queue-store-and-docs
title: "Implement SyncSpannerQueueStore and AsyncSpannerQueueStore Over Atomic ReadWrite Transactions"
description: "Build SyncSpannerQueueStore and AsyncSpannerQueueStore in db/adapters/spanner/queue.py supporting GoogleSQL (@param) and Spangres ($1)."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-interactive-rw-transactions:t03"
files:
  - src/py/vertebra/db/adapters/spanner/queue.py
  - src/py/vertebra/db/adapters/spanner/__init__.py
tests:
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/db/adapters/spanner/core.py
  - src/py/vertebra/framework/queues/spanner.py
estimated_loc: 245
tags:
  - spanner
  - spangres
  - queues
  - transactions
sources:
  - id: spn-driver
    resource: src/py/vertebra/db/adapters/spanner/driver.py
    title: "SyncSpannerDriver and AsyncSpannerDriver run_in_transaction"
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py
    title: "SyncQueueStore and AsyncQueueStore protocols"
---

# Task `t01`: Implement SyncSpannerQueueStore and AsyncSpannerQueueStore Over Atomic ReadWrite Transactions

## Objective & Scope Boundary

Create `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` in `src/py/vertebra/db/adapters/spanner/queue.py` executing atomic select-then-update claims inside `run_in_transaction` across GoogleSQL and Spangres per [`../spec.md`](../spec.md) [^spn-driver] [^queue-cap].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore` and `AsyncQueueStore` protocols.
- `src/py/vertebra/db/adapters/spanner/{driver,core}.py` — interactive `run_in_transaction`, GoogleSQL `@param`, and Spangres `$1` parameter encoding.

## Contracts & Edge Cases

- Implement `SyncSpannerQueueStore` (`SyncQueueStore`) and `AsyncSpannerQueueStore` (`AsyncQueueStore`) over `SyncSpannerDriver` and `AsyncSpannerDriver` with deterministic hash-prefixed `shard_id` keys.
- `claim()` executes `SELECT ... FOR UPDATE` followed by `UPDATE vertebra_queue_jobs SET status = 'running', attempt = attempt + 1, locked_until = ..., consumer_name = ..., receipt_handle = ... THEN RETURN *` (`RETURNING *` on Spangres) atomically inside `driver.run_in_transaction(...)`.
- Provide dialect-specific SQL for both GoogleSQL (`@param`, `@{FORCE_INDEX=...}`, `UNNEST(@lanes)`, `INSERT OR UPDATE`) and Spangres (`$1`, `ANY($1)`, `ON CONFLICT DO UPDATE`), plus conditional `acquire_lease` (`expires_at <= now OR owner_id = effective_owner`) so active cron/maintenance leases are never stolen.

## Test Matrix

- Parametrized GoogleSQL and Spangres unit tests verifying atomic `run_in_transaction` `SELECT ... FOR UPDATE` -> `UPDATE ... THEN RETURN`/`RETURNING` claim, `SpannerAbortedError` contention retry, attempt-fenced `ack`/`nack`/`heartbeat`, DLQ transitions, and non-stealing `acquire_lease`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/framework/test_distributed_queues.py
```

## Acceptance Criteria

- [ ] `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` across both GoogleSQL and Spangres with zero framework imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^spn-driver]: `src/py/vertebra/db/adapters/spanner/driver.py` — SyncSpannerDriver and AsyncSpannerDriver run_in_transaction.
[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py` — SyncQueueStore and AsyncQueueStore protocols.
