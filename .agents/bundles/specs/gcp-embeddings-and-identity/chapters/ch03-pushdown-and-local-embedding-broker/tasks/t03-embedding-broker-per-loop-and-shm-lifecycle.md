---
type: Task
id: "gcp-embeddings-and-identity:ch03:t03"
flow_id: gcp-embeddings-and-identity
chapter_id: ch03-pushdown-and-local-embedding-broker
title: "Isolate EmbeddingBroker State Per Event Loop and Enforce Sidecar SHM Cleanup"
description: "Replace cross-loop shared asyncio primitives on EmbeddingBroker with per-event-loop _BrokerLoopState and ensure /dev/shm/vtb-qvec-* segments are unlinked on both normal and error paths."
status: stable
state: completed
commit: 5771c8e3
last_verified_commit: 5771c8e3
last_verified_at: "2026-11-13T05:20:15Z"
verification_evidence: "ruff + mypy (12 files) + pytest (404 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/embeddings/_broker.py
tests:
  - src/py/tests/unit/embeddings/test_embedding_broker.py
read_set:
  - src/py/vertebra/embeddings/_broker.py
  - src/py/vertebra/embeddings/_base.py
  - src/rs/vertebra-py/src/vector.rs
  - src/py/tests/unit/embeddings/test_embedding_broker.py
estimated_loc: 150
tags:
  - embeddings
  - broker
  - asyncio
  - shm
sources:
  - id: py-broker
    resource: src/py/vertebra/embeddings/_broker.py::EmbeddingBroker
    title: "EmbeddingBroker shared _pending, _flush_task, _subprocess_lock, and sidecar SHM RPC"
---

# Task `t03`: Isolate `EmbeddingBroker` State Per Event Loop and Enforce Sidecar SHM Cleanup

## Objective & Scope Boundary

Refactor `EmbeddingBroker` in `src/py/vertebra/embeddings/_broker.py` so multi-loop and free-threaded `3.14t`/`3.15t` callers never share `asyncio` queues, tasks, locks, or subprocess transports across event loops, and guarantee `/dev/shm/vtb-qvec-*` cleanup on all sidecar paths per [`../spec.md` §2](../spec.md) [^py-broker]. Out of scope: `GemmaEmbedder` precision forwarding (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_broker.py` — `EmbeddingBroker.{submit_single,submit_batch,submit_batch_in_subprocess,close}` and sidecar worker [^py-broker].
- `src/py/vertebra/embeddings/_base.py` — `Embedder._run_sync` portal interaction.
- `src/rs/vertebra-py/src/vector.rs` — `VectorBatchView::open_shm` unlink behavior.
- `src/py/tests/unit/embeddings/test_embedding_broker.py` — broker batching and sidecar tests.

## Contracts & Edge Cases

- Introduce `_BrokerLoopState(msgspec.Struct, gc=False)` holding `pending: deque[_QueuedBrokerRequest]`, `subprocess_lock: asyncio.Lock`, `flush_task: asyncio.Task[None] | None = None`, and `subprocess: asyncio.subprocess.Process | None = None`, stored in a `threading.Lock`-protected `dict[asyncio.AbstractEventLoop, _BrokerLoopState]` (`_state_for_loop(loop)`), pruning entries for closed loops.
- Never instantiate `asyncio.Lock()` in `EmbeddingBroker.__init__` (which may run outside any event loop); construct loop-bound primitives inside `_state_for_loop(asyncio.get_running_loop())`.
- In `submit_batch_in_subprocess`, wrap the sidecar request/response exchange so any exception, EOF, or `resp.error` unlinks `/dev/shm/{shm_name.lstrip('/')}` if left behind on POSIX `/dev/shm`, and reset `state.subprocess = None` when the sidecar exits or times out so subsequent calls respawn cleanly.

## Test Matrix

- Add tests in `src/py/tests/unit/embeddings/test_embedding_broker.py`: constructing `EmbeddingBroker` outside a running loop then running concurrent `submit_single` + `submit_batch` across two separate `asyncio` event loops on threads with zero cross-loop `RuntimeError`; sidecar `submit_batch_in_subprocess` success leaving zero `/dev/shm/vtb-qvec-*` files; injected sidecar error and abrupt EOF cleaning up `/dev/shm/vtb-qvec-*` and recovering on the next request.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/embeddings/_broker.py src/py/tests/unit/embeddings/test_embedding_broker.py && uv run --no-sync mypy src/py/vertebra/embeddings/_broker.py && uv run --no-sync pytest src/py/tests/unit/embeddings/test_embedding_broker.py -q
```

## Acceptance Criteria

- [x] `EmbeddingBroker` isolates `asyncio` queues, tasks, locks, and sidecars per running event loop and leaves zero leaked `/dev/shm/vtb-qvec-*` segments on success or failure (`R12`).
- [x] Zero regressions in `test_embedding_broker.py`.

[^py-broker]: `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker` — `EmbeddingBroker` shared `_pending`, `_flush_task`, `_subprocess_lock`, and sidecar SHM RPC.
