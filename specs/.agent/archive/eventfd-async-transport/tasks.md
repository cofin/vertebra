# PRD-10: Eventfd Async Transport - Task Breakdown

## Overview

This document breaks down the PRD into atomic, testable tasks for implementation.

## Phase 1: Rust Core (Estimated: 3-4 days)

**Status:** COMPLETE (2026-01-19)

### Task 1.1: Implement Notification Primitive
- [x] Create `src/rs/vertebra-core/src/notify.rs`
- [x] Implement `Notifier` struct with eventfd (Linux)
- [x] Implement `Notifier` struct with pipe (macOS/Windows)
- [x] Implement `new()`, `as_raw_fd()`, `notify()`, `clear()` methods
- [x] Add `has_pending()` for non-blocking notification check
- [x] Add unit tests in `src/rs/vertebra-core/src/notify.rs` (mod tests)
- [x] Verify: `cargo test notify` passes

**Acceptance Criteria**:
- [x] Notifier creates valid FD on Linux (eventfd) and macOS (pipe)
- [x] `notify()` signals the FD
- [x] `clear()` drains pending notifications
- [x] FD is pollable with epoll/kqueue

### Task 1.2: Implement Buffer Pool
- [x] Create `src/rs/vertebra-core/src/buffer.rs` (integrated with async_ring)
- [x] Implement `BufferPool` struct with pre-allocated buffers
- [x] Implement `new(capacity, slot_size)`, `acquire()`, `release()` methods
- [x] Use `Mutex<Vec<Option<Box<[u8]>>>>` for thread-safe access
- [x] Add unit tests
- [x] Verify: `cargo test buffer` passes

**Acceptance Criteria**:
- [x] Pool pre-allocates specified number of buffers
- [x] `acquire()` returns buffer or None if exhausted
- [x] `release()` returns buffer to pool
- [x] Thread-safe under concurrent access

### Task 1.3: Implement Async Ring Buffer
- [x] Create `src/rs/vertebra-core/src/async_ring.rs`
- [x] Implement `AsyncRing` wrapping existing `SpscRing` + `Notifier`
- [x] Implement `new()`, `notifier_fd()`, `send()`, `try_recv()`, `clear_notification()`
- [x] Implement `PooledAsyncRing` with integrated buffer pool
- [x] Implement `AsyncChannelPair` for bidirectional request/response channels
- [x] Add `has_pending_notification()` method
- [x] Add `stats()` method for ring statistics
- [x] Integrate with existing ring buffer infrastructure
- [x] Add unit tests
- [x] Verify: `cargo test async_ring` passes

**Acceptance Criteria**:
- [x] `send()` writes to ring and signals notifier
- [x] `try_recv()` returns data or None (non-blocking)
- [x] `notifier_fd()` returns pollable FD
- [x] Works with existing SPSC ring implementation

### Task 1.4: Update Rust Module Structure
- [x] Add `pub mod notify;` to `src/rs/vertebra-core/src/lib.rs`
- [x] Add `pub mod buffer;` to `src/rs/vertebra-core/src/lib.rs`
- [x] Add `pub mod async_ring;` to `src/rs/vertebra-core/src/lib.rs`
- [x] Export public types
- [x] Verify: `cargo build` succeeds
- [x] Verify: `cargo test` passes (all existing tests + new)

## Phase 2: PyO3 Bindings (Estimated: 2-3 days)

**Status:** COMPLETE (2026-01-19)

### Task 2.1: Create PyAsyncRing Binding
- [x] Create `src/rs/vertebra-py/src/async_ring.rs`
- [x] Implement `#[pyclass] PyAsyncRing`
- [x] Expose `notifier_fd() -> i32`
- [x] Expose `send(data: &[u8]) -> PyResult<()>` with GIL release
- [x] Expose `try_send()` non-blocking variant
- [x] Expose `try_recv(py: Python) -> PyResult<Option<PyBytes>>`
- [x] Expose `try_recv_with_flags()` for flags support
- [x] Expose `clear_notification()` and `has_pending_notification()`
- [x] Expose `stats()` for ring statistics
- [x] Implement `PyAsyncChannel` for request/response pairs
- [x] Implement `PyPooledAsyncRing` for zero-allocation recv
- [x] Integrate buffer pool for zero-allocation recv

### Task 2.2: Update PyO3 Module
- [x] Add `mod async_ring;` to `src/rs/vertebra-py/src/lib.rs`
- [x] Register `PyAsyncRing`, `PyAsyncChannel`, `PyPooledAsyncRing` in module init
- [x] Verify: `maturin develop` succeeds
- [x] Write Python smoke test

### Task 2.3: Python Integration Test
- [x] Create `src/py/tests/test_pyo3_async_ring.py`
- [x] Test `PyAsyncRing.notifier_fd()` returns valid FD
- [x] Test `send()` and `try_recv()` roundtrip
- [x] Test `clear_notification()` behavior
- [x] Verify: `uv run pytest src/py/tests/test_pyo3_async_ring.py` passes

## Phase 3: Python Async Layer (Estimated: 2-3 days)

**Status:** COMPLETE (2026-01-19)

### Task 3.1: Implement AsyncShmTransport
- [x] Create `src/py/vertebra/_internal/async_transport.py`
- [x] Implement `AsyncShmTransport` class
- [x] Implement `__init__(ring: PyAsyncRing)`
- [x] Implement `async send(data: bytes)` - blocking with GIL release
- [x] Implement `async recv() -> bytes` with `loop.add_reader()` (fast path + slow path)
- [x] Implement `try_send()` and `try_recv()` non-blocking variants
- [x] Implement `recv_with_flags()` for flags support
- [x] Implement `_on_readable()` callback
- [x] Implement `close()` cleanup
- [x] Add docstrings

### Task 3.2: Implement PollingTransport (Fallback)
- [x] Add `PollingTransport` class to `async_transport.py`
- [x] Implement single polling thread with `asyncio.Queue`
- [x] Implement 100μs polling interval
- [x] Implement thread-safe queue push
- [x] Add graceful shutdown

### Task 3.3: Implement Transport Factory
- [x] Add `create_transport(ring: PyAsyncRing) -> AsyncShmTransport | PollingTransport`
- [x] Auto-detect platform and select appropriate transport
- [x] Linux: AsyncShmTransport (eventfd)
- [x] macOS/Windows: PollingTransport (fallback)
- [x] Add environment variable override for testing

### Task 3.4: Python Async Transport Tests
- [x] Create `src/py/tests/test_async_transport.py`
- [x] Test `AsyncShmTransport` send/recv roundtrip
- [x] Test `PollingTransport` send/recv roundtrip
- [x] Test transport factory selection
- [x] Test concurrent sends/recvs
- [x] Test cleanup on close
- [x] Verify: `uv run pytest src/py/tests/test_async_transport.py` passes

## Phase 4: Migration (Estimated: 3-4 days)

**Status:** IN PROGRESS - Infrastructure ready, 23 call sites remain

### Task 4.1: Migrate broker.py
- [ ] Import `AsyncShmTransport` / `create_transport`
- [ ] Replace `asyncio.to_thread(self._send_control, ...)` with `await self._transport.send(...)`
- [ ] Replace `asyncio.to_thread(self._request.send, ...)` with `await self._transport.send(...)`
- [ ] Replace `asyncio.to_thread(self._response.recv, ...)` with `await self._transport.recv()`
- [ ] Update `BrokerClient.__init__` to create transport
- [ ] Update `BrokerClient.close()` to close transport
- [ ] Verify: `uv run pytest src/py/tests/test_broker_client.py` passes
- [ ] Verify: `uv run pytest src/py/tests/test_broker_pool.py` passes

**Current asyncio.to_thread locations in broker.py:**
- Line 188: control send
- Line 230: request send
- Line 334: response recv

### Task 4.2: Migrate channel.py
- [ ] Locate `ipc/channel.py` (exists)
- [ ] Replace `asyncio.to_thread` patterns (lines 299, 345, 371)
- [ ] Update tests
- [ ] Verify: related tests pass

### Task 4.3: Migrate worker.py
- [ ] Update `AsgiWorker` to use new transport
- [ ] Replace all `asyncio.to_thread(ring.recv, ...)` calls
- [ ] Replace all `asyncio.to_thread(ring.send, ...)` calls
- [ ] Update transport lifecycle management
- [ ] Verify: `uv run pytest src/py/tests/test_asgi_worker.py` passes

### Task 4.4: Migrate store.py
- [ ] Update storage store to use new transport
- [ ] Replace `asyncio.to_thread` patterns (line 523)
- [ ] Verify: `uv run pytest src/py/tests/test_storage_store.py` passes

### Task 4.5: Migrate provider.py
- [ ] Update SQLSpec provider to use new transport
- [ ] Replace `asyncio.to_thread` patterns (line 60)
- [ ] Verify: related tests pass

### Task 4.6: Update All Tests
- [ ] Update `test_asgi_worker.py` - remove `asyncio.to_thread` usage
- [ ] Update `test_broker_client.py` - remove `asyncio.to_thread` usage
- [ ] Update `test_broker_pool.py` - remove `asyncio.to_thread` usage
- [ ] Update any other affected test files (16 test files total)
- [ ] Verify: `make test` passes (all 87+ tests)

**Note:** 23 asyncio.to_thread call sites identified across production code and tests.

## Phase 5: Cleanup (Estimated: 1-2 days)

### Task 5.1: Delete CFFI Bindings
- [ ] Delete `src/py/vertebra/_internal/shm.py`
- [ ] Remove any CFFI imports from other files
- [ ] Update `__init__.py` files if needed
- [ ] Verify: No import errors

### Task 5.2: Update Type Stubs
- [ ] Update `src/py/vertebra/_internal/_typing.py`
- [ ] Remove CFFI-related stubs (ShmRingStub, etc.) or update them
- [ ] Add stubs for new PyO3 types if needed
- [ ] Update `__all__` exports
- [ ] Verify: `make lint-all` passes

### Task 5.3: Remove Dead Code
- [ ] Search for any remaining `asyncio.to_thread` in IPC code
- [ ] Remove unused imports
- [ ] Remove commented-out code
- [ ] Verify: No lint warnings about unused code

### Task 5.4: Final Verification
- [ ] Run `make test` - all tests pass
- [ ] Run `make lint-all` - no errors
- [ ] Run `make js-test` - JavaScript tests still pass
- [ ] Run `make rs-test` - Rust tests pass

## Phase 6: Benchmarking (Estimated: 1 day)

### Task 6.1: Create Benchmark Suite
- [ ] Create `src/py/benchmarks/bench_ipc.py`
- [ ] Benchmark: IPC round-trip latency (send + recv)
- [ ] Benchmark: Sustained throughput (requests/second)
- [ ] Benchmark: Memory allocation rate

### Task 6.2: Run Benchmarks
- [ ] Baseline: Measure current performance (if possible)
- [ ] New: Measure with eventfd transport
- [ ] Compare against targets:
  - [ ] Latency: <100μs ✓/✗
  - [ ] Throughput: >50K req/s ✓/✗
  - [ ] Allocation: <1MB/s ✓/✗

### Task 6.3: Document Results
- [ ] Record benchmark results in `specs/active/eventfd-async-transport/benchmarks.md`
- [ ] Note any performance gaps
- [ ] Identify optimization opportunities if targets not met

## Phase 7: Documentation (Estimated: 0.5 days)

### Task 7.1: Update Project Documentation
- [ ] Update `docs/progress.md` with completed work
- [ ] Update `docs/arch.md` if architecture description changed
- [ ] Add inline documentation to new code

### Task 7.2: Update CLAUDE.md (if needed)
- [ ] Review for outdated information
- [ ] Update any IPC-related guidance

## Summary

| Phase | Tasks | Status | Notes |
|-------|-------|--------|-------|
| Phase 1: Rust Core | 4 | ✅ COMPLETE | notify.rs, async_ring.rs, buffer.rs |
| Phase 2: PyO3 Bindings | 3 | ✅ COMPLETE | PyAsyncRing, PyAsyncChannel, PyPooledAsyncRing |
| Phase 3: Python Async Layer | 4 | ✅ COMPLETE | AsyncShmTransport, PollingTransport, factory |
| Phase 4: Migration | 6 | ⚠️ IN PROGRESS | 23 asyncio.to_thread call sites remain |
| Phase 5: Cleanup | 4 | ⏳ PENDING | Depends on Phase 4 completion |
| Phase 6: Benchmarking | 3 | ⏳ PENDING | Infrastructure ready |
| Phase 7: Documentation | 2 | ⏳ PENDING | - |

**Progress:** ~75% complete (infrastructure done, migration remaining)

## Definition of Done

- [ ] All tasks checked off
- [ ] `make test` passes (87+ tests)
- [ ] `make lint-all` passes
- [ ] `make js-test` passes
- [ ] `make rs-test` passes
- [ ] Benchmarks meet targets
- [ ] No `asyncio.to_thread` in IPC code paths (23 remaining)
- [ ] `shm.py` (CFFI) deleted - **Note:** Will keep for fallback, new PyO3 path is primary
- [ ] Documentation updated

**Infrastructure Readiness:**
- [x] Rust async ring with eventfd notification
- [x] PyO3 bindings with GIL-releasing operations
- [x] Python AsyncShmTransport with loop.add_reader()
- [x] Platform fallback with PollingTransport
