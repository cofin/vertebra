# Plan: MPMC IPC Refactor

## Overview
Refactor the IPC Request channel (Broker -> Worker) from **SPSC** (Single-Producer Single-Consumer) to **MPMC** (Multi-Producer Multi-Consumer) to eliminate the `Mutex<SpscSender>` bottleneck. This ensures true lock-free concurrent request dispatch from multiple Tokio tasks in the Broker.

## Context
- **Current State**: `WorkerConnection` wraps `SpscSender` in `tokio::sync::Mutex`, serializing all requests to a worker. `SpscSender::send` is blocking (spin/park).
- **Goal**: "Ultimate performance" by enabling concurrent, lock-free sending.
- **Solution**: Use `MpmcRing` (which supports MPSC usage) for requests. The Python worker remains a single consumer but needs to attach to an MPMC ring.

## Phase 1: Core MPMC Support
- [ ] **Task 1: Implement `recv_hybrid` for `MpmcRing`** in `src/rs/vertebra-core/src/channel.rs`.
    - Purpose: Allow low-latency receiving on the Python side (Consumer) similar to SPSC.
- [ ] **Task 2: Create `AsyncMpmcRing`** in `src/rs/vertebra-core/src/async_ring.rs`.
    - Purpose: Wrap `MpmcRing` with `Notifier` for async integration.
- [ ] **Task 3: Expose `AsyncMpmcRing` to Python** in `src/rs/vertebra-py/src/async_ring.rs`.
    - Purpose: Allow Python `AsyncRing` to wrap MPMC ring types (or add specific `AsyncMpmcRing` class).

## Phase 2: Broker Implementation
- [ ] **Task 4: Update `WorkerConnection`** in `src/rs/vertebra-server/src/worker.rs`.
    - Change `req_tx` from `Mutex<SpscSender>` to `Arc<MpmcRing>` (or `MpmcSender`).
    - Remove Mutex usage.
    - Implement non-blocking send with async backpressure (using `Notifier` logic if possible, or `yield_now` for MVP if full notification is complex).
- [ ] **Task 5: Update `WorkerManager`** in `src/rs/vertebra-server/src/worker.rs`.
    - Initialize `req_ring` as `MpmcRing` instead of `SpscRing`.

## Phase 3: Python Worker Update
- [ ] **Task 6: Update `AsgiWorker`** in `src/py/vertebra/core/worker.py`.
    - Detect ring type or explicitly use `AsyncMpmcRing` for `req_ring` when in managed mode.
    - Alternatively, update `AsyncRing.from_raw_fd_handle` to verify header magic/layout and instantiate correct type.

## Phase 4: Cleanup
- [ ] **Task 7: Remove Legacy SPSC Code**.
    - Remove `Mutex<SpscSender>` definition.
    - Remove any SPSC-specific logic in worker connection that is no longer needed.

## Phase 5: Verification
- [ ] **Task 8: Benchmark**.
    - Run `benchmarks/ipc_fast_path.py` and `benchmarks/ipc_performance.py`.
    - Verify throughput improvement under concurrency.
