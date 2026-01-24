# Native Litestar Integration (Direct Protocol)

## Overview
This PRD defines a high-performance, custom integration between Vertebra and Litestar that bypasses the standard ASGI interface. Inspired by the performance gains seen in Granian/RSGI and Socketify, we will leverage Litestar's internal APIs and Vertebra's shared memory architecture to eliminate the overhead of the ASGI event loop, scope dictionary creation, and dynamic middleware traversal. We will also implement a "Direct" WebSocket/SSE path that leverages the Bun/Rust broker for elite concurrency, bypassing standard ASGI WebSocket limitations.

## Research Reference
- **Granian/RSGI:** Demonstrates that avoiding `asyncio` for protocol handling and using a Rust-driven event loop can yield 5.7x speedups for CPU-bound tasks and 80% higher concurrency for WebSockets.
- **Socketify:** Validates the "server-side optimization" model.

## Design Decisions & Alternatives
*Recorded for future reference and potential pivoting.*

### 1. Integration Depth
- **Selected:** **Deep Integration (Direct Instantiation).** Construct internals directly from shared memory.
- *Alternatives:*
    - Server-Side Optimization (Socketify-style): Tight loop application runner.
    - Custom Protocol: Async typed protocol (like efficient WSGI).

### 2. URL Routing
- **Selected:** **Rust-Side Resolution (Mirroring).** Pre-calculate routing table and push to Rust. Rust identifies the target handler index, skipping Python trie lookup.
- *Alternatives:*
    - Python-Side Optimization: Call `app.router.resolve_route()` directly.
    - Hybrid/JIT: Cache hot paths in Rust dynamically.

### 3. Data Binding & DI
- **Selected:** **Rust-Driven Direct Invocation.** Rust parses payloads (JSON/Arrow) to Python primitives and constructs `**kwargs` for the handler, bypassing dynamic parsing.
- *Alternatives:*
    - Accelerated DI: Feed raw data to a streamlined DI container.
    - Lazy Binding: Wrapper object that deserializes on access.

### 4. Request Lifecycle (Middleware/Guards)
- **Selected:** **Pre-Compiled "Flattened" Pipeline.** Generate a single optimized runner function per route at startup that inlines guards, dependencies, and the handler.
- *Alternatives:*
    - Rust-Orchestrated: Rust state machine triggers Python callbacks.
    - Opt-in "Fast Lane": Default to zero-middleware, opt-in for layers.

## Functional Requirements
### Must Have
- [ ] **Route Reflection:** Mechanism to traverse a Litestar app at startup, extract all routes, and map them to integer IDs.
- [ ] **Rust Router:** A high-performance router implementation in `vertebra-core` (using `matchit`) that maps URLs to these IDs.
- [ ] **Pipeline Compiler:** A Python utility that generates a "flattened" async function for each route, comprising its specific guards, dependencies, and handler logic.
- [ ] **Direct Invoker:** A runtime loop in `vertebra-py` that accepts a Route ID and Payload from shared memory and `await`s the corresponding compiled pipeline function.
- [ ] **Elite WebSocket Support:** A direct WebSocket path where the Bun broker terminates connections and streams frames to Python via the Ring Buffer (bypassing ASGI `websocket.receive` loops).
- [ ] **Legacy Fallback:** Ability to mount a standard ASGI app alongside the optimized routes (for 3rd party ASGI middleware compatibility).

### Should Have
- [ ] **Msgspec Integration:** Native support for `msgspec` structs in the Rust-to-Python data binding layer.
- [ ] **Dependency Graph Optimization:** "Tree shaking" for DI—identifying dependencies that don't depend on the request scope and resolving them once at startup.

### Nice to Have
- [ ] **Hot Reloading:** Ability to re-compile the routing table and pipelines without dropping connections.

## Non-Functional Requirements
- **Performance:** Throughput (RPS) must exceed standard Uvicorn/Litestar benchmark by at least 50% for "Hello World" and JSON echo cases.
- **Latency:** P99 latency should be reduced by avoiding the asyncio task creation overhead for the ASGI scope.
- **WebSocket Concurrency:** Support 10k+ concurrent idle connections per worker (limited only by OS/memory).
- **Compatibility:** Public-facing Litestar API (Routes, Controllers, DTOs) must remain unchanged for the developer.

## Technical Approach
### Recommended Implementation
1.  **Startup Phase (Python):**
    -   Introspect `app.routes`.
    -   Build a `RouteMap` struct to send to Rust.
    -   For each route, inspect the `Handler` signature.
    -   Dynamically `exec` (or construct via AST) a specialized `runner_{id}(shared_buffer)` function.
2.  **Runtime Phase (Rust):**
    -   Receive HTTP request headers/path.
    -   Match path -> Route ID.
    -   Write body to Shared Memory Ring Buffer.
    -   Signal Python Worker with `(Route ID, Buffer Offset)`.
3.  **WebSocket Protocol:**
    -   Bun Broker accepts Upgrade.
    -   Assigns Connection ID.
    -   Forwards "Open", "Message", "Close" events as distinct opcodes to Ring Buffer.
    -   Python handler is called directly with the payload (no `while True: await receive()`).

### Files to Modify
-   `src/rs/vertebra-core/`: Add routing logic (likely adding `matchit` crate) and WebSocket opcode definitions.
-   `src/py/vertebra/`: Add `compiler.py` (logic to flatten Litestar layers) and `integration.py`.
-   `src/py/vertebra/main.py`: Update worker loop to handle "Direct" and "WebSocket" message types.

## Acceptance Criteria
- [ ] A Litestar application running on Vertebra passes the standard Litestar test suite (ensuring logic correctness).
- [ ] "Hello World" benchmark shows significant RPS increase vs Uvicorn.
- [ ] WebSocket benchmark (echo) shows higher throughput/concurrency than Uvicorn.
- [ ] Routing table is correctly mirrored in Rust; changes in Python routes reflect in Rust after restart.
- [ ] Guards and Dependencies execute in the correct order within the compiled pipeline.

## Dependencies
-   **Litestar:** We are leveraging internal APIs; strict version pinning or vendoring strategies might be needed if internals change frequently.
-   **matchit (Rust):** For high-speed routing.

## Risk Assessment
### Identified Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Litestar Internal API Changes | High | High | Add integration tests that run against Litestar `main` nightly. |
| Complexity of Pipeline Compilation | Medium | Medium | Start with simple handlers (no deps), iterate to add DI/Guards. |
| Debugging Difficulty | Medium | Low | Ensure generated code can be dumped to disk for inspection. |

### Recovery Strategy
**Rollback Trigger:** Significant regression in compatibility (tests failing) or inability to support common Litestar features (like DTOs) reliably.
**Rollback Steps:**
1. Disable the `native=True` flag in the Vertebra config.
2. Revert to standard ASGI adapter mode (which remains the default fallback).

## Open Questions
- Can we effectively compile deeply nested dependencies without implementing a full Python AST transformer?
