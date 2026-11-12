---
type: Task
id: "gcp-embeddings-and-identity:ch03:t02"
chapter_id: ch03-google-embedder-runtime
title: Keep micro-batch queues per running event loop
description: Replace the shared _pending/_flush_task pair with lock-guarded per-loop _MicroBatchQueue entries, resolve futures only on their own loop, propagate drain cancellation, and drop gc=False from the future-holding pending item struct.
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch03:t01"
read_set:
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_base.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch03-google-embedder-runtime/spec.md
files:
  - src/py/vertebra/embeddings/_google.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 140
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
commit: null
sources:
  - id: py-microbatch
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder._drain_micro_batches
    title: Shared micro-batch drain loop and _PendingMicroBatchItem at HEAD
  - id: py-portal
    resource: src/py/vertebra/embeddings/_base.py::Embedder._run_sync
    title: Persistent portal loop used by *_sync methods inside running loops
tags:
  - embeddings
  - asyncio
  - free-threaded
---

# Task t02: Keep Micro-Batch Queues per Running Event Loop

## Objective & Scope Boundary

In `src/py/vertebra/embeddings/_google.py`, move micro-batch state from instance-wide `_pending` / `_flush_task` to a lock-guarded mapping from running loop to `_MicroBatchQueue` on `_GoogleEmbedder` [^py-microbatch]. `Embedder._run_sync` runs coroutines on a separate portal loop when called inside a running loop, and free-threaded builds can call one embedder from several threads with their own loops [^py-portal]. Each loop gets its own queue and drain task, and a queue entry is removed once it drains empty.

Out of scope: batching thresholds and token estimates (unchanged), wire formats.

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_google.py::_PendingMicroBatchItem`, `::_GoogleEmbedder._embed_single_impl`, `::_GoogleEmbedder._drain_micro_batches` (moved in `t01`).
- `src/py/vertebra/embeddings/_base.py::Embedder._run_sync`, `::Embedder._ensure_sync_portal_loop`.
- `src/py/tests/unit/embeddings/test_google_embedders.py::test_vertex_embedder_grpc_microbatching_adc_headers_and_401_retry`.
- Chapter spec.

## Symbol & Signature Contracts

```python
class _PendingMicroBatchItem(msgspec.Struct):
    """Queued singular embedding request waiting in an event-loop micro-batch queue."""
    text: str
    task: str
    title: str | None
    estimated_tokens: int
    future: asyncio.Future[VectorView]

class _MicroBatchQueue(msgspec.Struct):
    """Pending micro-batch items and the drain task owned by one event loop."""
    pending: list[_PendingMicroBatchItem]
    flush_task: asyncio.Task[None] | None = None

class _GoogleEmbedder(Embedder):
    _queues: dict[asyncio.AbstractEventLoop, _MicroBatchQueue]    # instance attribute set in __init__
    _queues_lock: threading.Lock                                    # instance attribute set in __init__
    async def _embed_single_impl(self, text: str, *, task: str = "query", title: str | None = None) -> VectorView: ...
    async def _drain_micro_batches(self, loop: asyncio.AbstractEventLoop, queue: _MicroBatchQueue) -> None: ...
```

`_embed_single_impl` (window `> 0`): under `_queues_lock`, get or create the queue for `asyncio.get_running_loop()`, append the item, and start `loop.create_task(self._drain_micro_batches(loop, queue))` when `flush_task` is `None` or done. `_drain_micro_batches` sleeps the window, then pops chunks from `queue.pending` (under the lock) with the existing task/size/token rules, awaits `_embed_batch_impl`, and resolves futures. When `queue.pending` is empty at loop end, it removes the queue from `_queues` under the lock (only if the mapping still points at this queue object).

## Edge Cases & Error Semantics

- `Exception` from `_embed_batch_impl` sets that exception on every unresolved future in the chunk (existing behavior).
- `asyncio.CancelledError` (or any `BaseException`) in the drain task cancels every unresolved future still in the chunk and in `queue.pending`, removes the queue, and re-raises.
- Futures already cancelled by their callers are skipped (`future.done()`).
- `coalesce_window_ms <= 0` bypasses queues entirely (existing behavior).
- `_PendingMicroBatchItem` drops `gc=False` because it holds an `asyncio.Future`, which can form reference cycles through callbacks.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/embeddings/test_google_embedders.py` (injected transport recording `asyncio.get_running_loop()` per call).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_micro_batch_queues_are_per_event_loop` | One `VertexEmbedder` (`coalesce_window_ms=20.0`, `_FakeTokenCache`); inside a running test loop, `asyncio.gather(embedder.embed_query("a"), asyncio.to_thread(embedder.embed_query_sync, "b"))` | Both return `VectorView`s; transport records `2` calls from `2` distinct loop objects; `embedder._queues == {}` afterwards |
| `test_micro_batch_coalesces_within_one_loop` | `asyncio.gather` of `embed_query("x")`, `embed_query("y")`, `embed_query("z")` with window `10.0` | Exactly `1` transport call carrying `3` texts |
| `test_micro_batch_drain_cancellation_propagates` | Transport awaits an `asyncio.Event` that never fires; after `20 ms` cancel the drain task found in `embedder._queues[loop].flush_task` | Pending `embed_query` raises `asyncio.CancelledError`; `embedder._queues == {}` |
| `test_micro_batch_errors_fan_out` | Transport returns `(500, {}, b"boom")` for a `2`-item chunk | Both callers raise `EmbeddingError` containing `HTTP 500` |

## Verification

```bash
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q -k micro_batch
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q
uv run ruff check src/py/vertebra/embeddings/_google.py src/py/tests/unit/embeddings/test_google_embedders.py
uv run mypy src/py/vertebra/embeddings
```

Write the tests first (Red: the cross-loop case hangs or raises `RuntimeError` about a future attached to a different loop; bound it with `asyncio.wait_for(..., 2.0)`), implement, re-run. Commit as `fix(embeddings): keep micro-batch queues per event loop`.

## Acceptance Criteria

- [ ] Each running loop has its own queue and drain task; entries are removed once drained (R9).
- [ ] Drain cancellation cancels pending futures; errors fan out as `EmbeddingError` (R9).
- [ ] `_PendingMicroBatchItem` no longer declares `gc=False` (R9).

## Notes & Discoveries

- None yet.

[^py-microbatch]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder._drain_micro_batches` — Shared drain loop at HEAD.
[^py-portal]: `src/py/vertebra/embeddings/_base.py::Embedder._run_sync` — Portal loop that creates the second loop.
