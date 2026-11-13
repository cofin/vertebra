---
type: Spec
flow_id: spanner-queue-and-rw-transactions
chapter_id: ch02-spanner-queue-store-and-docs
title: "Chapter 02: Sync/Async SpannerQueueStore, SpannerQueueBackend Remediation & Docs/OKF"
description: "Implement SyncSpannerQueueStore and AsyncSpannerQueueStore over atomic ReadWrite transactions across GoogleSQL and Spangres, remediate SpannerQueueBackend defects, and sync docs/OKF."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:58:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:58:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:58:00Z"
    commit: 18baadcd
tags:
  - spanner
  - spangres
  - queues
  - okf
sources:
  - id: queue-spanner
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: "Broken SpannerQueueBackend implementation"
  - id: okf-gcp
    resource: .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
    title: "Cloud Spanner driver OKF chapter"
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: "Distributed queues OKF chapter"
  - id: doc-qr
    resource: docs/usage/queues-and-realtime.md
    title: "Queues usage documentation"
---

# Chapter 02: Sync/Async SpannerQueueStore, SpannerQueueBackend Remediation & Docs/OKF

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `framework/queues/spanner.py` passes 6 positional arguments to 3-parameter `CLAIM_SPANNER_JOBS_SQL`, never runs `UPDATE_CLAIM_SPANNER_JOBS_SQL`, hardcodes GoogleSQL `@param` syntax even on Spangres, steals active cron leases via unconditional `INSERT OR UPDATE`, and leaks memory in `_receipt_index` [^queue-spanner].
- **Target Architecture**: `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` execute atomic select-then-update claims inside `run_in_transaction` across GoogleSQL (`@param`) and Spangres (`$1`), auto-wired into `SpannerQueueBackend`, `QueueService`, `StoreQueueBackend`, and Django `TaskBackend` [^okf-gcp] [^okf-queues] [^doc-qr].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` on top of Chapter 01's interactive `ReadWrite` transactions, fixes every defect in `SpannerQueueBackend`, and synchronizes documentation and OKF chapters [^queue-spanner].

## 2. Requirements, Concrete Contracts & Architecture

- **R4 (`SyncSpannerQueueStore` & `AsyncSpannerQueueStore`)**: Atomic `SELECT ... FOR UPDATE` -> `UPDATE ... THEN RETURN *` (`RETURNING *` on Spangres) inside `run_in_transaction`, supporting GoogleSQL and Spangres, attempt fencing, DLQ, and conditional `acquire_lease`.
- **R5 (`SpannerQueueBackend` Auto-Store & Defect Fixes)**: Auto-construct native stores from configured Spanner drivers, eliminate the 6-arg bug, lease theft, and `_receipt_index` leak.
- **R6 (Docs & OKF Sync)**: Sync `gcp-bigquery-and-spanner-drivers.md`, `distributed-queues-and-email.md`, and `docs/usage/queues-and-realtime.md`.

```python
class SyncSpannerQueueStore:
    def __init__(
        self,
        driver: SyncSpannerDriver,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        shard_count: int = 16,
        cleanup_on_ack: bool = False,
    ) -> None: ...
    def claim(self, queues: Sequence[str], *, limit: int = 1, worker_id: str, lease_seconds: float = 30.0) -> list[QueueEntry]: ...

class AsyncSpannerQueueStore:
    def __init__(
        self,
        driver: AsyncSpannerDriver,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
        shard_count: int = 16,
        cleanup_on_ack: bool = False,
    ) -> None: ...
    async def claim(self, queues: Sequence[str], *, limit: int = 1, worker_id: str, lease_seconds: float = 30.0) -> list[QueueEntry]: ...
```

## 3. Documentation & OKF Knowledge Plan

Updates `docs/usage/queues-and-realtime.md`, [gcp-bigquery-and-spanner-drivers.md](../../../../knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md), and [distributed-queues-and-email.md](../../../../knowledge/domains/framework/distributed-queues-and-email.md), keeping every OKF chapter `<= 200` lines.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R4` `Sync/AsyncSpannerQueueStore` | `t01` | `db/adapters/spanner/{queue,__init__}.py` | `pytest test_spanner_queue_and_cdc.py` |
| `R5` `SpannerQueueBackend` fixes & wiring | `t02` | `framework/queues/{spanner,base}.py` | `pytest test_distributed_queues.py` |
| `R6` Docs & OKF sync | `t03` | `gcp-bigquery-and-spanner-drivers.md`, `distributed-queues-and-email.md`, `queues-and-realtime.md` | `review_setup.py validate` |

## 5. Implementation Plan

- [ ] [t01-sync-async-spanner-queue-store.md](tasks/t01-sync-async-spanner-queue-store.md) — Implement `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` over `run_in_transaction` across GoogleSQL and Spangres.
- [ ] [t02-spanner-queue-backend-remediation-and-wiring.md](tasks/t02-spanner-queue-backend-remediation-and-wiring.md) — Fix `SpannerQueueBackend` defects, auto-wire `Sync/AsyncSpannerQueueStore`, and remove `_receipt_index` leak.
- [ ] [t03-spanner-rw-and-queue-docs-okf-sync.md](tasks/t03-spanner-rw-and-queue-docs-okf-sync.md) — Sync `gcp-bigquery-and-spanner-drivers.md`, `distributed-queues-and-email.md`, and `docs/usage/queues-and-realtime.md`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-spanner-queue-store-and-docs` is `planned` behind [`../../spec.md`](../../spec.md) prerequisite `queues-tasks-and-channels`.

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner src/py/vertebra/framework/queues && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/framework/test_distributed_queues.py
```

[^queue-spanner]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Broken SpannerQueueBackend implementation.
[^okf-gcp]: `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — Cloud Spanner driver OKF chapter.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues OKF chapter.
[^doc-qr]: `docs/usage/queues-and-realtime.md` — Queues usage documentation.
