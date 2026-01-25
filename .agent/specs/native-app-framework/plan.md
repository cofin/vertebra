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

## Phase 7: Optimization and Cleanup (Clean Break)

### Task 7.1: Port Arrow Encoder to Rust
- [~] Implement `encode_arrow_rows` in `vertebra-py` (Rust)
- [ ] Accept `List[Tuple]` from Python
- [ ] Pivot to Arrow Columnar Memory in Rust
- [ ] Replace Python implementation in `vertebra.resources.arrow`
- [ ] Benchmark to confirm 5x speedup

### Task 7.2: Zero-Copy Header Enforcement
- [ ] Audit `src/ts/src/ipc` to ensure `RequestHeader` is used
- [ ] Audit `src/py/vertebra/ipc` to ensure `RequestHeader` is used
- [ ] Remove any residual JSON or non-framed IPC paths

### Task 7.3: Legacy Removal & Adapter Modernization
- [ ] Remove any deprecated pre-Vertebra `App` classes not aliased to `Vertex`
- [ ] Update `src/py/vertebra/asgi.py` adapter to use new `RequestHeader` + `msgspec` protocol
- [ ] Ensure ASGI adapter can co-exist with Native Routes in the same worker pool
- [ ] Remove any deprecated flatbuffers generated code

### Task 7.4: Performance Validation
- [ ] Profile full stack with `flamegraph`
- [ ] Verify Memory Allocation < 1MB/s
- [ ] Verify Throughput > 200K req/s

---

## Phase 8: Documentation and Examples

### Task 8.1: API Documentation
- [ ] Docstrings for all public APIs
- [ ] Type hints complete and accurate
- [ ] Generate API reference docs

### Task 8.2: User Guides
- [ ] Getting started guide: Native (`Vertex`) vs Litestar (`VertebraPlugin`)
- [ ] Controller and routing guide
- [ ] State synchronization guide
- [ ] Dependency injection guide
- [ ] Guards and middleware guide
- [ ] **Dual Mode Guide:** Mixing Native and ASGI apps

### Task 8.3: Example Applications
- [ ] Basic CRUD example (Native)
- [ ] Litestar Hybrid Example (Litestar Controller + Native Events)
- [ ] Real-time dashboard example
- [ ] Multi-user game example

### Task 8.4: Checkpoint - Verify Phase 8 Complete
- [ ] All documentation complete
- [ ] Examples run successfully
- [ ] Docs clear on Native vs. Hybrid tradeoffs
