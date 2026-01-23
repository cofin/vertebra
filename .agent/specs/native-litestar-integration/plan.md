# Implementation Plan: Native Litestar Integration

This plan outlines the phases to implement a custom, high-performance integration between Vertebra and Litestar, bypassing standard ASGI for both HTTP and WebSockets.

## Phase 1: Route Reflection and Rust Routing
**Goal:** Extract Litestar's routing table at startup and mirror it in a high-performance Rust router.

- [x] **Task: Define Route Reflection Logic** [215ca1a]
    - [ ] Write tests for extracting route paths, handler IDs, and metadata from a `Litestar` app instance.
    - [ ] Implement `vertebra.integration.litestar.reflect_routes` to generate a serializable `RouteMap`.
- [x] **Task: Implement Rust-Side Router (`matchit`)** [c91e0c0]
    - [ ] Write Rust unit tests in `vertebra-core` for path matching and ID retrieval.
    - [ ] Integrate `matchit` crate into `vertebra-core`.
    - [ ] Implement `Router::match_path(&self, path: &str) -> Option<RouteId>`.
- [x] **Task: Bridge RouteMap at Startup** [27a264f]
    - [ ] Write integration tests for passing the `RouteMap` from Python to Rust during worker initialization.
    - [ ] Update `vertebra-py` and `vertebra-js` bindings to accept and load the routing table.
- [x] **Task: Checkpoint - Verify Route Mirroring** [583c230]
- [ ] **Task: Flow - User Manual Verification 'Phase 1: Routing Mirror' (Protocol in workflow.md)**

## Phase 2: The Pipeline Compiler [~]
**Goal:** Flatten Litestar's layered architecture (guards, dependencies) into optimized Python runner functions.

- [x] **Task: Implement Guard Flattening** [5ad4e60]
    - [ ] Write tests for a handler with multiple guards (app, controller, and route level).
    - [ ] Implement code generation that inlines guard checks into a single `async` function.
- [x] **Task: Implement Dependency Injection Compilation** [067790c]
    - [ ] Write tests for handlers with nested dependencies.
    - [ ] Implement a "DI Resolver" that pre-calculates the resolution order and generates code to fetch/instantiate dependencies.
- [x] **Task: Implement Specialized Runner Generation** [a36b432]
    - [ ] Write tests for handlers returning different types (JSON, Msgspec, Arrow).
    - [ ] Implement the `PipelineCompiler` to generate the final `runner_{id}` function using `exec` or AST construction.
- [x] **Task: Checkpoint - Verify Compiled Pipelines** [f1d1c31]
- [ ] **Task: Flow - User Manual Verification 'Phase 2: Pipeline Compiler' (Protocol in workflow.md)**

## Phase 3: Direct Invoker Runtime (HTTP) [~]
**Goal:** Implement the high-speed worker loop that executes compiled handlers directly from the Ring Buffer.

- [x] **Task: Update Worker Opcodes and Loop** [f8b183f]
    - [ ] Write tests for the `vertebra-py` worker receiving a `DIRECT_HTTP` opcode.
    - [ ] Implement the optimized loop in `vertebra.main` that bypasses ASGI task creation for native routes.
- [ ] **Task: Direct Data Binding (Rust-to-Python)**
    - [ ] Write tests for passing JSON payloads directly into handler `**kwargs`.
    - [ ] Optimize the shared-memory-to-Python-primitive mapping in `vertebra-py`.
- [ ] **Task: Legacy ASGI Fallback**
    - [ ] Write tests for an app containing both "Native" and "Legacy" routes.
    - [ ] Implement routing logic that switches between Direct Invoker and the standard ASGI adapter based on the route ID.
- [ ] **Task: Checkpoint - Verify End-to-End HTTP**
- [ ] **Task: Flow - User Manual Verification 'Phase 3: Direct HTTP' (Protocol in workflow.md)**

## Phase 4: Elite WebSocket and SSE Support
**Goal:** Implement a direct, non-ASGI path for WebSockets and streaming events.

- [ ] **Task: Define WebSocket/SSE Opcodes**
    - [ ] Update `vertebra-core` and `vertebra-js` with `WS_OPEN`, `WS_MESSAGE`, `WS_CLOSE`, and `SSE_DATA` opcodes.
- [ ] **Task: Bun Broker WebSocket Forwarding**
    - [ ] Write Bun unit tests for proxying uWebSockets events into the Ring Buffer.
    - [ ] Implement frame forwarding in the TS Broker logic.
- [ ] **Task: Python Direct WebSocket Invocation**
    - [ ] Write tests for a Litestar WebSocket handler being called "per-message" by Vertebra.
    - [ ] Implement the Python worker logic to handle streaming opcodes without an internal `while True` loop.
- [ ] **Task: Checkpoint - Verify Elite WebSockets**
- [ ] **Task: Flow - User Manual Verification 'Phase 4: Elite Streaming' (Protocol in workflow.md)**

## Phase 5: Integration, Benchmarking, and Polish
**Goal:** Ensure 100% compatibility and prove the performance gains.

- [ ] **Task: Full Litestar Compatibility Test Suite**
    - [ ] Run the complete Litestar test suite against a Vertebra-hosted app.
    - [ ] Fix any edge cases in DI or Middleware ordering.
- [ ] **Task: Performance Benchmarking**
    - [ ] Run `wrk` / `k6` benchmarks for Hello World, JSON, and WebSockets.
    - [ ] Compare RPS and P99 latency against Uvicorn + Litestar.
- [ ] **Task: Documentation and CLI Updates**
    - [ ] Update `docs/` with details on the Direct Protocol.
    - [ ] Update `vertebra serve` to enable/disable native integration via flag.
- [ ] **Task: Checkpoint - Final Review**
- [ ] **Task: Flow - User Manual Verification 'Phase 5: Final Polish' (Protocol in workflow.md)**
