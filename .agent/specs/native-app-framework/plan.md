# Native App Framework - Implementation Plan

## Phase 1: Core Routing Infrastructure

### Task 1.1: Rust Router Wrapper [872e08f]
- [x] Create `src/rs/vertebra-core/src/router.rs`
- [x] Wrap `matchit::Router` with FFI-friendly API
- [x] Support path parameters with type hints (`{id:int}`, `{name:str}`)
- [x] Expose via PyO3 and napi-rs bindings
- [x] Write unit tests for route matching

### Task 1.2: Python Routing Layer [cbfe335]
- [x] Create `src/py/vertebra/routing/__init__.py`
- [x] Implement `Router` class for route grouping
- [x] Implement route decorators (`@get`, `@post`, `@put`, `@delete`, `@patch`)
- [x] Path parameter extraction and type coercion
- [x] Write unit tests for routing

### Task 1.3: Controller Base Class [48f35e5]
- [x] Create `src/py/vertebra/controllers/__init__.py`
- [x] Implement `Controller` base class with `path` attribute
- [x] Controller-level guards and middleware
- [x] Route method discovery via reflection
- [x] Write unit tests for controllers

### Task 1.4: Checkpoint - Verify Phase 1 Complete [565b489]
- [x] All routing tests pass
- [x] Rust router benchmarks meet O(1) target
- [x] Controller registration works end-to-end

---

## Phase 2: Dependency Injection System

### Task 2.1: Inject Type and Provider Protocol [4afa5d6]
- [x] Create `src/py/vertebra/di/__init__.py`
- [x] Implement `Inject[T]` type annotation
- [x] Define `Provider` protocol for registration
- [x] Support scopes: `singleton`, `request`, `transient`
- [x] Write unit tests for DI primitives

### Task 2.2: Simplified Provider Registration [f7476c5]
- [x] Implement `vertex.provide(ServiceClass)` auto-discovery (Renamed from `app`)
- [x] Implement `@vertex.provide` decorator for factories
- [x] Infer dependencies from `__init__` type hints
- [x] Support async factory functions
- [x] Write tests for registration patterns

### Task 2.3: AOT Dependency Graph Compilation [2575940]
- [x] Build dependency graph at startup
- [x] Topological sort for initialization order
- [x] Compile linear execution sequences for hot paths
- [x] Cache compiled runners
- [x] Write tests for compilation correctness

### Task 2.4: VertebraContext Abstraction [f3236a7]
- [x] Create context wrapper for all entry points
- [x] Support HTTP request context
- [x] Support CLI context
- [x] Support event handler context
- [x] Write integration tests

### Task 2.5: Checkpoint - Verify Phase 2 Complete [4b83761]
- [x] DI works in controllers [73a2ff4]
- [x] AOT compilation reduces overhead to ~0 [4b83761]
- [x] All scopes (singleton, request, transient) function correctly [4b83761]

---

## Phase 3: Guards and Middleware

### Task 3.1: Guard System [27e87c1]
- [x] Create `src/py/vertebra/guards/__init__.py`
- [x] Implement `Guard` base class/protocol
- [x] Implement `requires()` decorator pattern
- [x] Guard DI injection support
- [x] Guard result caching per request
- [x] Write unit tests for guards

### Task 3.2: Native Guards in Rust [2bfe0a1]
- [x] Create `src/rs/vertebra-core/src/guards.rs`
- [x] Implement JWT validation guard
- [x] Implement rate limiting guard
- [x] Expose via PyO3 bindings
- [x] Write unit tests and benchmarks

### Task 3.3: Middleware Chain [c53d31e]
- [x] Create `src/py/vertebra/middleware/__init__.py`
- [x] Implement middleware protocol (before/after hooks)
- [x] Middleware DI injection support
- [x] Middleware ordering and priority
- [x] Write unit tests for middleware

### Task 3.4: Checkpoint - Verify Phase 3 Complete [67bd6be]
- [x] Guards execute before handlers
- [x] Rust guards perform <10μs
- [x] Middleware chain executes in correct order
- [x] DI works in guards and middleware

---

## Phase 4: State Synchronization

### Task 4.1: Opcode Protocol Definition [48eca22]
- [x] Define opcodes in shared header (Rust/Python/TypeScript)
- [x] Implement MessagePack encoding for deltas
- [x] Define state ID and field ID schemes
- [x] Write protocol documentation

### Task 4.2: NativeState Delta Tracking [51261f9]
- [x] Extend `src/py/vertebra/state.py` [51261f9]
- [x] Implement `__setattr__` with opcode generation [51261f9]
- [x] Immutable replace for nested objects [51261f9]
- [x] Timestamp generation for LWW [51261f9]
- [x] Write unit tests for delta generation [51261f9]

### Task 4.3: Bun State Hub [7c9957c]
- [x] Create `src/ts/src/broker/state-hub.ts` [7c9957c]
- [x] Implement LWW conflict resolution [7c9957c]
- [x] Broadcast deltas to subscribed workers [7c9957c]
- [x] State persistence and recovery [7c9957c]
- [x] Write unit tests for state hub [7c9957c]

### Task 4.4: Python Shadow State [57694be]
- [x] Implement local shadow state in workers [57694be]
- [x] Subscribe to broker delta broadcasts [57694be]
- [x] Apply deltas to shadow state [57694be]
- [x] Nanosecond read performance [57694be]
- [x] Write integration tests [57694be]

### Task 4.5: Checkpoint - Verify Phase 4 Complete [86bbad4]
- [x] State changes propagate in <100μs
- [x] LWW resolves conflicts correctly
- [x] Shadow state reads are <100ns
- [x] Multi-worker sync works end-to-end

---

## Phase 5: Frontend Sync Protocols

### Task 5.1: SSE Streaming [9bebd17]
- [x] Create `src/ts/src/server/sse.ts` [9bebd17]
- [x] Implement SSE connection management [9bebd17]
- [x] State delta broadcasting to clients [9bebd17]
- [x] Reconnection with last-event-id [9bebd17]
- [x] Write integration tests [9bebd17]

### Task 5.2: WebSocket Support [d3f64a4]
- [x] Create `src/ts/src/server/websocket.ts` [d3f64a4]
- [x] Bidirectional state sync [d3f64a4]
- [x] Connection upgrade handling [d3f64a4]
- [x] Heartbeat and reconnection [d3f64a4]
- [x] Write integration tests [d3f64a4]

### Task 5.3: Long-Polling Fallback [1e05168]
- [x] Implement long-polling endpoint [1e05168]
- [x] State versioning for catch-up [1e05168]
- [x] Timeout and retry logic [1e05168]
- [x] Write integration tests [1e05168]

### Task 5.4: Python Client Helpers [95f2393]
- [x] Create `src/py/vertebra/sync/__init__.py` [95f2393]
- [x] Helper decorators for SSE/WS endpoints [95f2393]
- [x] State subscription management [95f2393]
- [x] Write unit tests [95f2393]

### Task 5.5: Checkpoint - Verify Phase 5 Complete [7680d89]
- [x] SSE delivers state updates to browser
- [x] WebSocket bidirectional sync works
- [x] Long-polling works in restricted environments
- [x] All frontend sync tests pass

---

## Phase 6: Application Lifecycle & Unification

### Task 6.1: Vertebra Class (Unified Config) [f0ac69e]
- [x] Keep `src/py/vertebra/app.py` (or `main.py`) as the unified container
- [x] Implement `Vertebra` class consolidating app/config responsibilities
- [x] Implement lifecycle hooks (`on_startup`, `on_shutdown`)
- [x] Router and controller registration on `Vertebra`
- [x] State initialization
- [x] Write unit tests verifying `Vertebra` as single source of truth

### Task 6.2: Event System [ddeb974]
- [x] Create `src/py/vertebra/events/__init__.py`
- [x] Implement pub/sub event dispatcher attached to `Vertebra`
- [x] Event DI injection support
- [x] Async event handlers
- [x] Write unit tests

### Task 6.3: Background Tasks [4977732]
- [x] Implement background task scheduling
- [x] DI access in background tasks
- [x] Task cancellation and cleanup
- [x] Write integration tests

### Task 6.4: Litestar CLI Integration [97ab1b0]
- [x] Enhance `VertebraPlugin` in `src/py/vertebra/extensions/litestar/_plugin.py`
- [x] Ensure `run_command` in `src/py/vertebra/extensions/litestar/cli.py` correctly overrides `litestar run`
- [x] Verify plugin launches Vertebra Supervisor when `VertebraPlugin` is present in `Litestar(plugins=[...])`
- [x] Add tests for CLI overrides

### Task 6.5: Checkpoint - Verify Phase 6 Complete [dc6365c]
- [x] `Vertebra` successfully configures native apps
- [ ] `litestar run` launches Vertebra Supervisor for Litestar apps
- [ ] Events and background tasks function in both modes

### Task 6.6: Vertebra Unification (Single Entry Point)
- [ ] Merge native app container with broker plugin container into a single `Vertebra` class
- [ ] Ensure broker configs register via `Vertebra` (plugin/config hook)
- [ ] Ensure ASGI servers register via `Vertebra` (mount/adapter hook)
- [ ] Update templates/docs to reference unified `Vertebra`
- [ ] Add tests for unified entrypoint wiring

---

## Phase 7: Optimization and Cleanup (Standardization)

### Task 7.1: Port Arrow Encoder to Rust [ABANDONED]
- [x] Implement `encode_arrow_rows` in `vertebra-py` (Rust) [1cd43d1]
- [x] Benchmark to confirm 5x speedup [FAILED]
- [x] Root Cause: PyO3 iteration overhead too high (~3x slower than nanoarrow)
- [x] Decision: Revert to nanoarrow (Python) and abandon Rust port for this case.
- [x] See `.agent/specs/rust-arrow-encoder/learnings.md` for details.

### Task 7.2: Zero-Copy Header Enforcement [COMPLETED]
- [x] Audit `src/ts/src/ipc` to ensure `RequestHeader` is used
- [x] Audit `src/py/vertebra/ipc` to ensure `RequestHeader` is used
- [x] Remove any residual JSON or non-framed IPC paths
- [x] Results: P50 latency < 70μs verified.

### Task 7.3: Legacy Removal & Adapter Modernization [COMPLETED]
- [x] Remove any deprecated pre-Vertebra `App` classes not aliased to `Vertex`
- [x] Update `src/py/vertebra/asgi.py` adapter to use new `RequestHeader` + `msgspec` protocol
- [x] Ensure ASGI adapter can co-exist with Native Routes in the same worker pool
- [x] Remove any deprecated flatbuffers generated code

### Task 7.4: Performance Validation [COMPLETED]
- [x] Profile full stack with `flamegraph`
- [x] Verify Memory Allocation < 1MB/s
- [x] Verify Throughput > 200K req/s [FAILED]
    *   **Result:** ~60 RPS. P99 Latency ~1s.
    *   **Root Cause:** Polling-based IPC (`asyncio.sleep(0.001)`) limits throughput.
    *   **Next Step:** Migration to `eventfd` / Async Ring (Task for next flow).

---

## Phase 8: Async IPC Notification (Performance Fix) [CRITICAL]

> **Root Cause:** Polling-based IPC with `asyncio.sleep(0.001)` creates 1ms latency floor, capping throughput at ~66 RPS.
> **Solution:** Replace polling with eventfd-based notification (Linux) + pipe() fallback (macOS/Windows).
> **Target:** >200K RPS, <100μs P99 latency.
> **Consensus:** Gemini 3 Pro + Flash both agree (9/10 confidence each).

### Task 8.1: Rust eventfd Primitives [COMPLETE - vertebra-eap]
- [x] Notifier already existed in `src/rs/vertebra-core/src/notify.rs`
- [x] EventFd + Pipe implementations already present (cross-platform)
- [x] PyO3 bindings already exposed via `AsyncRing`
- [x] Added napi-rs bindings in `vertebra-js` (NotifierHandle)
- [x] Added C ABI bindings in `vertebra-broker` for bun:ffi

### Task 8.2: Integrate Notifier with SpscRing (Rust) [COMPLETE - vertebra-2ba]
- [x] AsyncRing already integrates notifier with SpscRing
- [x] `send()` calls `notifier.notify()` automatically
- [x] `notifier_fd()` exposed for `add_reader()` integration
- [x] `recv_hybrid()` provides spin-wait pattern

### Task 8.3: Python Worker Reactor Pattern [COMPLETE - vertebra-ufk]
- [x] Replaced polling with `loop.add_reader()` reactor pattern
- [x] Removed `asyncio.sleep(0.001)` from worker.py
- [x] Worker now waits on `_stop_event` instead of polling loop
- [x] Client FDs registered/unregistered with event loop
- [x] Graceful shutdown removes all readers

### Task 8.4: TypeScript IPC Client Notification [COMPLETE - vertebra-nk0]
- [x] Removed `sleep(1)` from `runResponseLoop()` and `waitForResponse()`
- [x] Replaced with `yieldToEventLoop()` using `setImmediate`
- [x] Added `ShmNotifier` class in shm.ts (bun:ffi)
- [x] Added C ABI notifier functions to vertebra-broker
- [ ] TODO: Full eventfd integration requires protocol changes to pass FDs

### Task 8.5: ProcessManager Notifier Lifecycle
- [ ] Create notifier fds in `ProcessManager.spawnWorker()` before spawning
- [ ] Pass notifier fd to worker via CLI argument or env var
- [ ] Ensure fd is inherited by subprocess (not closed on exec)
- [ ] Track notifier fds per worker in `WorkerProcess` struct
- [ ] Close notifier fds on worker termination
- [ ] Add SIGCHLD handler to clean up notifiers on unexpected worker death
- [ ] Update `ProcessManager.stop()` to close all notifier fds
- [ ] Write tests for notifier lifecycle across worker restarts

### Task 8.6: Shared Memory Cleanup Infrastructure
- [ ] Add `shm_unlink()` call to `IpcConnection.close()` (client.ts:218-229)
- [ ] Add `shm_unlink()` to `ProcessManager.handleExit()` for crashed workers
- [ ] Track all SHM names created per worker in ProcessManager
- [ ] Implement `cleanupOrphanedShm()` on server startup (scan /dev/shm/vertebra-*)
- [ ] Add `atexit` handler in Python worker to unlink owned SHM regions
- [ ] Add `finally` block cleanup in `AsgiWorker.serve_forever()` (worker.py:203)
- [ ] Implement UUID-based SHM naming to prevent collisions
- [ ] Write tests for cleanup on normal shutdown
- [ ] Write tests for cleanup on crash/SIGKILL

### Task 8.7: Remove All Legacy Polling Code [COMPLETE - vertebra-1lz]
- [x] Removed `PollingTransport` class from async_transport.py
- [x] Removed `_send_with_polling` and `_recv_with_polling` from channel.py
- [x] Updated `create_transport()` to fail explicitly on unsupported platforms
- [x] Removed `prefer_eventfd` config option (no polling fallback exists)
- [x] Added `UnsupportedPlatformError` for clear failure on Windows ProactorEventLoop
- [x] Removed polling from `receive_sync()` in channel.py
- [x] Cleaned up exports in __init__.py, typing.py, _typing.py
- [x] TypeScript: replaced `sleep(1)` with `yieldToEventLoop()`

### Task 8.8: Notification Batching for High Throughput
- [ ] Implement coalesced notification: signal once per batch of writes
- [ ] Add `batch_send()` method to SpscRing for multiple messages
- [ ] Signal only after final message in batch
- [ ] Add configurable batch timeout (e.g., 100μs max coalesce window)
- [ ] Implement "notify on empty" optimization (only signal if consumer was idle)
- [ ] Benchmark batching impact on throughput at 200K+ RPS
- [ ] Write tests for batch notification semantics

### Task 8.9: Platform Detection and Notification Strategy
- [ ] Implement runtime platform detection in Rust (`cfg!(target_os)`)
- [ ] Linux: use eventfd (primary, required for production)
- [ ] macOS: use pipe() with kqueue-compatible semantics (dev/CI only)
- [ ] Windows: use pipe() or named event (dev/CI only)
- [ ] **NO POLLING FALLBACK** - current polling code is non-functional (~66 RPS)
- [ ] Fail fast with clear error if notification unavailable
- [ ] Log notification strategy at startup
- [ ] Write platform-specific tests (Linux eventfd, macOS pipe)
- [ ] Document platform differences in code comments

### Task 8.10: Checkpoint - Verify Phase 8 Complete
- [ ] All polling code removed (grep confirms no asyncio.sleep in IPC)
- [ ] eventfd notification working on Linux
- [ ] pipe() fallback working on macOS
- [ ] Throughput benchmark: >50K RPS minimum (stretch: >200K)
- [ ] P99 latency: <1ms (stretch: <100μs)
- [ ] No SHM leakage after 1000 worker restarts
- [ ] No fd leakage after 1000 connections
- [ ] All tests pass on Linux and macOS
- [ ] Memory allocation rate <1MB/s under load

---

## Phase 9: Documentation and Examples

### Task 9.1: API Documentation
- [ ] Docstrings for all public APIs
- [ ] Type hints complete and accurate
- [ ] Generate API reference docs

### Task 9.2: User Guides
- [ ] Getting started guide: Native (`Vertex`) vs Litestar (`VertebraPlugin`)
- [ ] Controller and routing guide
- [ ] State synchronization guide
- [ ] Dependency injection guide
- [ ] Guards and middleware guide
- [ ] **Dual Mode Guide:** Mixing Native and ASGI apps

### Task 9.3: Example Applications
- [ ] Basic CRUD example (Native)
- [ ] Litestar Hybrid Example (Litestar Controller + Native Events)
- [ ] Real-time dashboard example
- [ ] Multi-user game example

### Task 9.4: Checkpoint - Verify Phase 9 Complete
- [ ] All documentation complete
- [ ] Examples run successfully
- [ ] Docs clear on Native vs. Hybrid tradeoffs