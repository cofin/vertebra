---
title: "Chapter 8 — Hot-Path Performance & Benchmark Suite Hardening"
description: "Optimize SPSC/MPMC shared-memory rings, pooled SHM batch deserialization, zero-allocation PyO3 response submission, single-wake Python worker dispatch, and Criterion/end-to-end benchmark gates."
flow_id: hot-path-performance
parent_spec: vertebra-design-quality
chapter: 8
state: closed
work_kind: refactor
state_revision: 1
plan_revision: 2
plan_commit: ""
updated_at: 2026-10-22T00:00:00Z
depends_on_chapters: [6, 7]
tags: [refactor, performance, rust, pyo3, benchmarks]
---

# Chapter 8 — Hot-Path Performance & Benchmark Suite Hardening

## Problem & Context

Across the Rust IPC rings (`vertebra-core`), PyO3 worker bridge (`vertebra-py`), Python batch runner (`vertebra.server`), and benchmark suites (`vertebra-core/benches/`, `vertebra-http/benches/`, `tools/benchmarks/`), several avoidable cache-line loads, heap allocations, per-request closures, and benchmark harness flaws limit peak throughput and mask regressions:

1. **SPSC Shared-Memory Ring (`CORE-10`)**:
   - `SpscRing::try_send` and `SpscRing::try_send_with` in `src/rs/vertebra-core/src/ring/spsc.rs` perform an `Ordering::Acquire` load of the consumer's `read_head` cache line on every send, even when cached local knowledge proves the ring has free slots. Similarly, `try_recv_view` and `try_recv_with_flags` load the producer's `write_head` cache line on every receive even when unread slots remain from the previous observation.
2. **MPMC Contention & Idle CPU Spin (`CORE-41`)**:
   - `MpmcRing::send`, `recv`, `recv_timeout`, and `recv_timeout_with` in `src/rs/vertebra-core/src/ring/mpmc.rs` spin indefinitely with `crossbeam_utils::Backoff::snooze()` without ever parking on `ShmHeader.wait_read` / `wait_write` via `platform::wait_on_address_timeout`, even though `MpmcRing::initialize` already initializes `wait_read`, `wait_write`, `send_waiters`, and `recv_waiters` in `ShmHeader`.
3. **SHM Worker Batch Deserialization Allocations (`CORE-20`)**:
   - In `Backend::InProcess`, `BatchBridge::seal_batch_locked` recycles `(Vec<u8>, Vec<TransportNode>)` buffers through `BatchBufferPool`. In `Backend::SharedMemory`, `BatchSnapshot::deserialize_from` (called from `Transport::poll_batch` and `Transport::poll_batch_async`) allocates a fresh `vec![TransportNode::default(); node_count]` and `data[slab_start..].to_vec()` on every batch unless wired to `BatchBufferPool` in `src/rs/vertebra-core/src/transport/batch.rs` (from Chapter 2 Task 2.6).
4. **Redundant Header Re-Encoding, `Py<Response>` Materialization & Wire Slot Borrows (`PYB-06`, `PYB-07`)**:
   - `Response` (`src/rs/vertebra-py/src/response.rs`) stores `headers: Vec<(Vec<u8>, Vec<u8>)>`, which is re-encoded via `encode_headers_compact` on response submission in SHM mode (`src/rs/vertebra-py/src/transport.rs`). While `BridgeResponse.headers_raw: Bytes` was completed in Task 2.7, Task 8.4 focuses on `ResponseSlot::encode_into` / `decode_view` zero-copy borrow in `src/rs/vertebra-core/src/transport/response_slot.rs` and `src/rs/vertebra-py/src/transport.rs`.
   - `_dispatch_batch` in `src/py/vertebra/server/_dispatch.py` and `worker.py` constructs a Python `Response` pyclass instance for every request only for `submit_response` to immediately borrow it, clone its fields into `BridgeResponse` or the SHM ring, and drop the Python object (`PYB-07`).
5. **Per-Request Closure Allocations & Multi-Wake Dispatch in `server` (`PYB-20`)**:
   - `_dispatch_batch` in `src/py/vertebra/server/_dispatch.py` and `worker.py` allocates a fresh `_dispatch_sync`, `_dispatch_async`, or `_dispatch_rsgi` closure per request, calls `executor.submit` per sync request instead of batching, and calls `loop.call_soon_threadsafe(loop.create_task, coro)` in a loop when `scheduler` is a raw event loop.
6. **Criterion & End-to-End Benchmark Flaws (`CORE-40`, `HTTP-41`, `TST-08`)**:
   - `src/rs/vertebra-core/benches/spsc.rs:100-150` (which absorbed `spin_wait.rs` in Task 2.2) timed `std::thread::spawn` + `thread::sleep` + `join` inside `b.iter()`, measuring OS thread creation instead of ring wakeup latency; `ring_throughput.rs:13-23` constructs a `Vec::from_raw_parts` over an alignment-64 allocation (`CORE-40`); unified ring benchmarks run in `benches/ring_bench.rs`.
   - `src/rs/vertebra-http/benches/server_throughput.rs:197-211,341-370` leaks background responder threads across `bench_bridge_dispatch` and `bench_bridge_dispatch_adaptive` that compete for the global `BatchBridge` (`HTTP-41`).
   - `tools/benchmarks/targets.py:95-100` (`_ft_run`) silently returns `["echo", "3.14t server binary not found..."]` when `src/rs/target-ft/release/vertebra-server` is missing (`TST-08`).

## Scope

### In Scope
- Findings `CORE-10`, `CORE-20`, `CORE-40`, `CORE-41`, `HTTP-41`, `PYB-06`, `PYB-07`, `PYB-20`, `TST-08`.
- Caching `read_head`/`write_head` inside `SpscRing` (`ring/spsc.rs`).
- Adding bounded spin and futex/ulock parking to `MpmcRing` (`ring/mpmc.rs`).
- Adding `BatchSnapshot::deserialize_from_pooled` backed by `BatchBufferPool` in `transport/batch.rs`.
- `ResponseSlot::encode_into` / `decode_view` zero-copy borrow in `transport/response_slot.rs` and `vertebra-py/src/transport.rs`.
- `submit_response_raw` and `submit_batch_raw` PyO3 fastpaths in `vertebra-py/src/transport.rs` and `_dispatch_batch` in `server/_dispatch.py` and `worker.py`.
- Pre-compiling handler runner callables and batching `asyncio` loop wakeups.
- Hardening Criterion benchmarks and fixing `tools/benchmarks/targets.py`.

### Out of Scope
- Rewriting the HTTP/1.1 or HTTP/2 state machines (owned by Chapter 3).
- Rewriting ASGI middleware chains or Litestar plugins (owned by Chapter 5).
- Modifying test runner harness or test file directory structure (owned by Chapter 9).

## Constraints & Invariants

- Must adhere to Constraint 1 (Zero-Slop Code & Documentation), Constraint 4 (Python 3.12+/3.14t Polyglot Architecture & msgspec Discipline), Constraint 5 (Rust 2024 & PyO3 `0.23` Discipline), Constraint 8 (Shared-Memory Safety), Constraint 9 (Hot-Path Performance Gate), Constraint 11 (Polyglot Rebuild Discipline).

## Architectural Design

1. **SPSC Cached Cursors (`CORE-10`)**:
   - Store `cached_read_head: AtomicU64` and `cached_write_head: AtomicU64` on `SpscRing` in `src/rs/vertebra-core/src/ring/spsc.rs`. In `try_send` / `try_send_with`, check `next == self.cached_read_head.load(Ordering::Relaxed)` first and only load the shared `ShmHeader.read_head` with `Ordering::Acquire` when the cached head indicates full. Mirror the optimization in `try_recv_view` / `try_recv_with_flags` using `cached_write_head`.
2. **MPMC Bounded Spin + Futex/Ulock Parking (`CORE-41`)**:
   - In `MpmcRing::send`, `recv`, `recv_timeout`, and `recv_timeout_with` in `src/rs/vertebra-core/src/ring/mpmc.rs`, spin with `Backoff::snooze()` until `backoff.is_completed()`, then enter the `SeqCst` sleeper/waker handshake on `wait_read` / `wait_write` (`send_waiters` / `recv_waiters`) and wake waiting peers in `try_send` / `try_send_with` / `try_recv` / `try_recv_with`.
3. **Pooled SHM `BatchSnapshot` Deserialization (`CORE-20`)**:
   - Attach a worker-side `Arc<BatchBufferPool>` to `RingPair` (`RingPair::buffer_pool`) and add `BatchSnapshot::deserialize_from_pooled(data: &[u8], pool: &Arc<BatchBufferPool>) -> Result<Arc<Self>, TransportError>` in `src/rs/vertebra-core/src/transport/batch.rs` that takes recycled `(slab_buf, nodes_buf)` vectors from `pool`, populates them in-place via `resize` + `copy_nonoverlapping`, and returns `BatchSnapshot::new(slab_buf, nodes_buf, flags, Some(Arc::clone(pool)))`.
4. **Pre-Encoded Compact Response Headers & Direct Raw Submission (`PYB-06`, `PYB-07`)**:
   - Store `headers_raw: Bytes` directly inside `vertebra_py::response::Response` so headers are encoded once at `Response::new` / `set_header` time, and focus on `ResponseSlot::encode_into` / `decode_view` zero-copy borrow in `src/rs/vertebra-core/src/transport/response_slot.rs` and `src/rs/vertebra-py/src/transport.rs`.
   - Add `submit_response_raw(py, request_id: u64, status: u16, body: &[u8], headers_raw: Option<&[u8]>)` and `submit_batch_raw` in `src/rs/vertebra-py/src/transport.rs` so `server/{_dispatch,worker}.py` can submit `(200, b"", body)` for `bytes`/`str`/JSON handler returns without allocating a `Py<Response>` object when no custom `Response` was returned.
5. **Pre-Compiled Worker Dispatch Wrappers (`PYB-20`)**:
   - Extend `_handler_dispatch_spec` in `src/py/vertebra/server/_dispatch.py` to pre-compile and cache the async/sync runner callables per handler, batch sync handler submissions onto the executor when `len(sync_native) > 1`, and use a single `loop.call_soon_threadsafe` wake when scheduling coroutine batches onto a raw `asyncio.AbstractEventLoop`.

## File Ownership Boundary

| Path | Ownership |
| --- | --- |
| `src/rs/vertebra-core/src/ring/{spsc,mpmc,mod,slot}.rs` | Tasks `8.1`, `8.2` |
| `src/rs/vertebra-core/src/transport/{batch,ring_pair,response_slot}.rs` | Tasks `8.3`, `8.4` |
| `src/rs/vertebra-core/src/bridge.rs`, `src/rs/vertebra-py/src/{response,transport,lib}.rs` | Tasks `8.4`, `8.5` |
| `src/py/vertebra/server/{_dispatch.py,worker.py}` | Tasks `8.5`, `8.6` |
| `src/rs/vertebra-core/benches/*.rs`, `src/rs/vertebra-http/benches/server_throughput.rs` | Tasks `8.1`, `8.2`, `8.3`, `8.7` |
| `tools/benchmarks/targets.py` | Task `8.8` |

## Task Checklist

- [x] **8.1** — Cache remote `read_head`/`write_head` cursors in `SpscRing` to eliminate cross-core cache-line bounces (`tasks/8.1.md`, `characterization`, `depends_on: []`)
- [x] **8.2** — Add bounded-spin futex/ulock parking to `MpmcRing` blocking operations (`tasks/8.2.md`, `regression_tdd`, `depends_on: [8.1]`)
- [x] **8.3** — Pool `BatchSnapshot` slab and node buffers across SHM worker deserialization (`tasks/8.3.md`, `characterization`, `depends_on: [8.2]`)
- [x] **8.4** — Pre-encode compact headers and zero-copy `ResponseSlot` encode/decode borrow (`tasks/8.4.md`, `characterization`, `depends_on: [8.3]`)
- [x] **8.5** — Add direct `submit_response_raw` / `submit_batch_raw` PyO3 fastpath bypassing `Py<Response>` allocation (`tasks/8.5.md`, `characterization`, `depends_on: [8.4]`)
- [x] **8.6** — Pre-compile handler runner callables and collapse multi-wake dispatch in `vertebra.server` (`tasks/8.6.md`, `characterization`, `depends_on: [8.5]`)
- [x] **8.7** — Fix Criterion benchmark measurement flaws in `vertebra-core` and `vertebra-http` (`tasks/8.7.md`, `characterization`, `depends_on: [8.1, 8.3, 8.4]`)
- [x] **8.8** — Enforce fail-fast binary preflight in `tools/benchmarks/targets.py` and run the full performance gate (`tasks/8.8.md`, `regression_tdd`, `depends_on: [8.6, 8.7]`)

## Verification

```bash
cd src/rs && cargo test --workspace --features tls,compression
cd src/rs && cargo bench -p vertebra-core --bench spsc -- --test
cd src/rs && cargo bench -p vertebra-core --bench ring_bench -- --test
cd src/rs && cargo bench -p vertebra-core --bench ring_throughput -- --test
cd src/rs && cargo bench -p vertebra-core --bench shm_transport -- --test
cd src/rs && cargo bench -p vertebra-http --bench server_throughput -- --test
make py-test
make lint-all
```
