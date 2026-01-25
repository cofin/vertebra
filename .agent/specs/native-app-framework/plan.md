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

### Task 2.1: Inject Type and Provider Protocol
- [ ] Create `src/py/vertebra/di/__init__.py`
- [ ] Implement `Inject[T]` type annotation
- [ ] Define `Provider` protocol for registration
- [ ] Support scopes: `singleton`, `request`, `transient`
- [ ] Write unit tests for DI primitives

### Task 2.2: Simplified Provider Registration
- [ ] Implement `app.provide(ServiceClass)` auto-discovery
- [ ] Implement `@app.provide` decorator for factories
- [ ] Infer dependencies from `__init__` type hints
- [ ] Support async factory functions
- [ ] Write tests for registration patterns

### Task 2.3: AOT Dependency Graph Compilation
- [ ] Build dependency graph at startup
- [ ] Topological sort for initialization order
- [ ] Compile linear execution sequences for hot paths
- [ ] Cache compiled runners
- [ ] Write tests for compilation correctness

### Task 2.4: VertebraContext Abstraction
- [ ] Create context wrapper for all entry points
- [ ] Support HTTP request context
- [ ] Support CLI context
- [ ] Support event handler context
- [ ] Write integration tests

### Task 2.5: Checkpoint - Verify Phase 2 Complete
- [ ] DI works in controllers
- [ ] AOT compilation reduces overhead to ~0
- [ ] All scopes (singleton, request, transient) function correctly

---

## Phase 3: Guards and Middleware

### Task 3.1: Guard System
- [ ] Create `src/py/vertebra/guards/__init__.py`
- [ ] Implement `Guard` base class/protocol
- [ ] Implement `requires()` decorator pattern
- [ ] Guard DI injection support
- [ ] Guard result caching per request
- [ ] Write unit tests for guards

### Task 3.2: Native Guards in Rust
- [ ] Create `src/rs/vertebra-core/src/guards.rs`
- [ ] Implement JWT validation guard
- [ ] Implement rate limiting guard
- [ ] Expose via PyO3 bindings
- [ ] Write unit tests and benchmarks

### Task 3.3: Middleware Chain
- [ ] Create `src/py/vertebra/middleware/__init__.py`
- [ ] Implement middleware protocol (before/after hooks)
- [ ] Middleware DI injection support
- [ ] Middleware ordering and priority
- [ ] Write unit tests for middleware

### Task 3.4: Checkpoint - Verify Phase 3 Complete
- [ ] Guards execute before handlers
- [ ] Rust guards perform <10μs
- [ ] Middleware chain executes in correct order
- [ ] DI works in guards and middleware

---

## Phase 4: State Synchronization

### Task 4.1: Opcode Protocol Definition
- [ ] Define opcodes in shared header (Rust/Python/TypeScript)
- [ ] Implement MessagePack encoding for deltas
- [ ] Define state ID and field ID schemes
- [ ] Write protocol documentation

### Task 4.2: NativeState Delta Tracking
- [ ] Extend `src/py/vertebra/state.py`
- [ ] Implement `__setattr__` with opcode generation
- [ ] Immutable replace for nested objects
- [ ] Timestamp generation for LWW
- [ ] Write unit tests for delta generation

### Task 4.3: Bun State Hub
- [ ] Create `src/ts/src/broker/state-hub.ts`
- [ ] Implement LWW conflict resolution
- [ ] Broadcast deltas to subscribed workers
- [ ] State persistence and recovery
- [ ] Write unit tests for state hub

### Task 4.4: Python Shadow State
- [ ] Implement local shadow state in workers
- [ ] Subscribe to broker delta broadcasts
- [ ] Apply deltas to shadow state
- [ ] Nanosecond read performance
- [ ] Write integration tests

### Task 4.5: Checkpoint - Verify Phase 4 Complete
- [ ] State changes propagate in <100μs
- [ ] LWW resolves conflicts correctly
- [ ] Shadow state reads are <100ns
- [ ] Multi-worker sync works end-to-end

---

## Phase 5: Frontend Sync Protocols

### Task 5.1: SSE Streaming
- [ ] Create `src/ts/src/server/sse.ts`
- [ ] Implement SSE connection management
- [ ] State delta broadcasting to clients
- [ ] Reconnection with last-event-id
- [ ] Write integration tests

### Task 5.2: WebSocket Support
- [ ] Create `src/ts/src/server/websocket.ts`
- [ ] Bidirectional state sync
- [ ] Connection upgrade handling
- [ ] Heartbeat and reconnection
- [ ] Write integration tests

### Task 5.3: Long-Polling Fallback
- [ ] Implement long-polling endpoint
- [ ] State versioning for catch-up
- [ ] Timeout and retry logic
- [ ] Write integration tests

### Task 5.4: Python Client Helpers
- [ ] Create `src/py/vertebra/sync/__init__.py`
- [ ] Helper decorators for SSE/WS endpoints
- [ ] State subscription management
- [ ] Write unit tests

### Task 5.5: Checkpoint - Verify Phase 5 Complete
- [ ] SSE delivers state updates to browser
- [ ] WebSocket bidirectional sync works
- [ ] Long-polling works in restricted environments
- [ ] All frontend sync tests pass

---

## Phase 6: Application Lifecycle

### Task 6.1: VertebraApp Class
- [ ] Create `src/py/vertebra/app.py`
- [ ] Implement lifecycle hooks (`on_startup`, `on_shutdown`)
- [ ] Router and controller registration
- [ ] State initialization
- [ ] Write unit tests

### Task 6.2: Event System
- [ ] Create `src/py/vertebra/events/__init__.py`
- [ ] Implement pub/sub event dispatcher
- [ ] Event DI injection support
- [ ] Async event handlers
- [ ] Write unit tests

### Task 6.3: Background Tasks
- [ ] Implement background task scheduling
- [ ] DI access in background tasks
- [ ] Task cancellation and cleanup
- [ ] Write integration tests

### Task 6.4: CLI Integration
- [ ] Extend `vertebra serve` for native apps
- [ ] Support `vertebra run` for background tasks
- [ ] DI context in CLI commands
- [ ] Write integration tests

### Task 6.5: Checkpoint - Verify Phase 6 Complete
- [ ] App starts and shuts down cleanly
- [ ] Events dispatch to handlers
- [ ] Background tasks run with DI
- [ ] CLI commands have DI access

---

## Phase 7: Integration and Performance

### Task 7.1: End-to-End Integration Tests
- [ ] Full app lifecycle tests
- [ ] Multi-worker state sync tests
- [ ] Frontend sync protocol tests
- [ ] Guard and middleware chain tests
- [ ] Write comprehensive test suite

### Task 7.2: Performance Benchmarks
- [ ] Route matching benchmark (target: O(1))
- [ ] DI resolution benchmark (target: ~0 overhead)
- [ ] State sync latency (target: <100μs)
- [ ] Throughput benchmark (target: >50K req/s)
- [ ] Memory allocation rate (target: <1MB/s)

### Task 7.3: Performance Optimization
- [ ] Profile and identify bottlenecks
- [ ] Optimize hot paths based on benchmarks
- [ ] Reduce FFI boundary crossings
- [ ] Batch operations where possible

### Task 7.4: Checkpoint - Verify Phase 7 Complete
- [ ] All integration tests pass
- [ ] Performance targets met
- [ ] No memory leaks under load
- [ ] >90% test coverage

---

## Phase 8: Documentation and Examples

### Task 8.1: API Documentation
- [ ] Docstrings for all public APIs
- [ ] Type hints complete and accurate
- [ ] Generate API reference docs

### Task 8.2: User Guides
- [ ] Getting started guide
- [ ] Controller and routing guide
- [ ] State synchronization guide
- [ ] Dependency injection guide
- [ ] Guards and middleware guide

### Task 8.3: Example Applications
- [ ] Basic CRUD example
- [ ] Real-time dashboard example
- [ ] Multi-user game example
- [ ] CLI tool with DI example

### Task 8.4: Migration Guide
- [ ] Litestar to Vertebra Native migration
- [ ] ASGI to Native migration
- [ ] Common patterns and differences

### Task 8.5: Checkpoint - Verify Phase 8 Complete
- [ ] All documentation complete
- [ ] Examples run successfully
- [ ] Migration guide covers common cases

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Route matching | O(1) | Benchmark suite |
| DI overhead | ~0 | Benchmark vs direct call |
| State sync | <100μs | Integration test timing |
| Throughput (simple) | >200K req/s | Load test (no state sync) |
| Throughput (with sync) | >150K req/s | Load test (with state deltas) |
| Test coverage | >90% | pytest-cov |
| Memory allocation | <1MB/s | heaptrack under load |

**Baseline comparison:** Granian ASGI ~70-100K req/s. Native must exceed by 2-3x to justify the complexity.

## Dependencies Between Phases

```
Phase 1 (Routing) ──┐
                    ├──> Phase 3 (Guards) ──┐
Phase 2 (DI) ───────┘                       │
                                            ├──> Phase 6 (Lifecycle) ──> Phase 7 (Integration)
Phase 4 (State) ────────────────────────────┤
                                            │
Phase 5 (Frontend Sync) ────────────────────┘
```

- Phase 1 and 2 can run in parallel
- Phase 3 depends on Phase 1 and 2
- Phase 4 and 5 can run in parallel with Phase 3
- Phase 6 depends on all previous phases
- Phase 7 integrates everything
- Phase 8 can start after Phase 6
