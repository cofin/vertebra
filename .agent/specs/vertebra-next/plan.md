# Plan: Vertebra Next Implementation

## Phase 1: Foundation (The Litecore Port)
**Goal:** Establish the IPC/Worker foundation using `litecore` as the reference implementation. This creates the "Slow Path" infrastructure.

- [x] Task: Scaffold `vertebra-engine` crate [commit: HEAD]
    - Initialize Rust crate.
    - Add dependencies: `tokio`, `hyper`, `libc`, `thiserror`, `crossbeam-utils`, `pyo3`.
- [x] Task: Port `litecore::shm` [commit: HEAD]
    - Implement `ShmRegion` for Linux (`memfd_create`) and Unix (`shm_open`).
    - Verify memory mapping and sealing logic.
- [x] Task: Port `litecore::buffer` [commit: HEAD]
    - Define `RingLayout`, `SlotHeader`, `ShmHeader` structs.
    - Ensure alignment matches Python `struct` expectations.
- [x] Task: Port `litecore::sync` [commit: HEAD]
    - Implement Futex-based synchronization (`wait`, `wake`).
    - Ensure cross-platform compatibility (parking_lot fallback if needed).
- [x] Task: Port `litecore::channel` [commit: HEAD]
    - Implement `SpscRing` (Single-Producer Single-Consumer).
    - Implement `MpmcRing` (Multi-Producer Multi-Consumer).
    - Implement `SlotView` for zero-copy access.
- [x] Task: Port `litecore::worker` [commit: HEAD]
    - Implement `WorkerConfig` and `WorkerManager`.
    - Implement process spawning with FD passing (`Command::pre_exec`).
    - Implement health checks and heartbeat monitoring.
- [ ] Task: Flow - User Manual Verification 'Phase 1: Foundation' (Protocol in workflow.md)

## Phase 2: The Embedded Executor (Hybrid Layer)
**Goal:** Add the "Fast Path" using PyO3 to run Python code directly in the Rust process.

- [ ] Task: Initialize Embedded Python
    - Configure `pyo3` to initialize the interpreter.
    - Set up `sys.path` to include user application.
- [ ] Task: Implement `EmbeddedWorker`
    - Create a Rust struct that holds the PyO3 application reference.
    - Implement a thread-safe wrapper (GIL handling).
- [ ] Task: Implement `DirectExecutor` (Request Mapping)
    - Map `hyper::Request<Incoming>` to ASGI Scope (`PyDict`).
    - Implement `ZeroCopyBody` for request body streaming.
- [ ] Task: Implement `DirectExecutor` (Response Mapping)
    - Implement ASGI `send` callable in Rust.
    - Map ASGI response messages to `hyper::Response`.
- [ ] Task: Verify GIL Behavior
    - Benchmark simple "Hello World" to ensure it matches Granian performance.
    - Stress test to ensure Rust async runtime is not blocked by Python GC.
- [ ] Task: Flow - User Manual Verification 'Phase 2: Embedded Executor' (Protocol in workflow.md)

## Phase 3: Router & Integration
**Goal:** Unified routing between Fast and Slow paths.

- [ ] Task: Implement `OptimisticRouter`
    - Port `matchit` router logic.
    - Define `RouteManifest` struct for config.
- [ ] Task: Configuration Parsing
    - Implement `vertebra.toml` or `pyproject.toml` parser.
    - Allow routes to be tagged as `embedded` or `worker`.
- [ ] Task: Implement Dispatcher
    - Create `Dispatcher` service in Hyper.
    - Route requests to either `DirectExecutor` or `WorkerManager`.
    - Implement "Spillover" logic (optional: send to worker if GIL saturated).
- [ ] Task: Unified Response Handling
    - Ensure both paths return compatible `hyper::Response` types.
- [ ] Task: Flow - User Manual Verification 'Phase 3: Router & Integration' (Protocol in workflow.md)

## Phase 4: Frontend & Type Bridge
**Goal:** Native "Zero-Node" asset bundling and real-time type synchronization.

- [ ] Task: Implement `RolldownExecutor`
    - Integrate `rolldown` crate.
    - Implement `DevEngine` startup and lifecycle.
    - Wire `HmrOptions` for WebSocket HMR.
- [ ] Task: Port `TypeBridge` Extractor
    - Port AST extraction logic from `vertebra`.
    - Add support for `attrs` classes (per research).
    - Implement `TypeBridgeWatcher` for file changes.
- [ ] Task: Implement `VirtualModuleLoader`
    - Create Rolldown plugin to serve `virtual:vertebra-types`.
    - Connect `TypeBridgeWatcher` updates to Rolldown invalidation.
- [ ] Task: Flow - User Manual Verification 'Phase 4: Frontend & Type Bridge' (Protocol in workflow.md)

## Phase 5: Cleanup & Migration
**Goal:** Remove legacy components replaced by the new architecture.

- [ ] Task: Audit and Remove `vertebra-server` (Legacy Supervisor)
- [ ] Task: Audit and Remove `vertebra-broker` (Legacy Broker)
- [ ] Task: Remove Legacy Python IPC Code (`vertebra.core.supervisor` / `vertebra.transport`)
- [ ] Task: Clean up Workspace (`Cargo.toml`, `.gitignore`)
- [ ] Task: Flow - User Manual Verification 'Phase 5: Cleanup & Migration' (Protocol in workflow.md)