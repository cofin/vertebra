# Plan: Vertebra Next Implementation

## Phase 1: Foundation (The Litecore Port)
**Goal:** Establish the IPC/Worker foundation using `litecore` as the reference implementation. This creates the "Slow Path" infrastructure.

- [x] Task: Scaffold `vertebra-engine` crate [commit: 9fe9179]
    - Initialize Rust crate.
    - Add dependencies: `tokio`, `hyper`, `libc`, `thiserror`, `crossbeam-utils`, `pyo3`.
- [x] Task: Port `litecore::shm` [commit: 9fe9179]
    - Implement `ShmRegion` for Linux (`memfd_create`) and Unix (`shm_open`).
    - Verify memory mapping and sealing logic.
- [x] Task: Port `litecore::buffer` [commit: 9fe9179]
    - Define `RingLayout`, `SlotHeader`, `ShmHeader` structs.
    - Ensure alignment matches Python `struct` expectations.
- [x] Task: Port `litecore::sync` [commit: 9fe9179]
    - Implement Futex-based synchronization (`wait`, `wake`).
    - Ensure cross-platform compatibility (parking_lot fallback if needed).
- [x] Task: Port `litecore::channel` [commit: 9fe9179]
    - Implement `SpscRing` (Single-Producer Single-Consumer).
    - Implement `MpmcRing` (Multi-Producer Multi-Consumer).
    - Implement `SlotView` for zero-copy access.
- [x] Task: Port `litecore::worker` [commit: 9fe9179]
    - Implement `WorkerConfig` and `WorkerManager`.
    - Implement process spawning with FD passing (`Command::pre_exec`).
    - Implement health checks and heartbeat monitoring.
- [ ] Task: Flow - User Manual Verification 'Phase 1: Foundation' (Protocol in workflow.md)

## Phase 2: The Embedded Executor (Hybrid Layer)
**Goal:** Add the "Fast Path" using PyO3 to run Python code directly in the Rust process.

- [x] Task: Initialize Embedded Python [commit: HEAD]
    - Configure `pyo3` to initialize the interpreter.
    - Set up `sys.path` to include user application.
- [x] Task: Implement `EmbeddedWorker` [commit: <HASH>]
    - Create a Rust struct that holds the PyO3 application reference.
    - Implement a thread-safe wrapper (GIL handling).
- [x] Task: Implement `DirectExecutor` (Request Mapping) [commit: <HASH>]
    - Map `hyper::Request<Incoming>` to ASGI Scope (`PyDict`).
    - Implement `ZeroCopyBody` for request body streaming.
- [x] Task: Implement `DirectExecutor` (Response Mapping) [commit: <HASH>]
    - Implement ASGI `send` callable in Rust.
    - Map ASGI response messages to `hyper::Response`.
- [x] Task: Implement `EmbeddedService` wiring [commit: <HASH>]
    - Wire EmbeddedWorker, DirectExecutor, and Hyper Service.
- [x] Task: Verify GIL Behavior [commit: <HASH>]
    - Benchmark simple "Hello World" to ensure it matches Granian performance.
    - Stress test to ensure Rust async runtime is not blocked by Python GC.
    - *Note: Encountered 'no running event loop' error during benchmark. Created follow-up task.*
- [x] Task: Debug `pyo3-async-runtimes` loop issue [commit: <HASH>]
    - Investigate why `RuntimeError('no running event loop')` occurs despite `set_event_loop`.
    - Ensure correct initialization of async-runtimes.
- [x] Task: Flow - User Manual Verification 'Phase 2: Embedded Executor' (Protocol in workflow.md) [checkpoint: <HASH>]

## Phase 3: Performance Deep Dive & Optimization
**Goal:** Diagnose and fix the 1600 RPS bottleneck. Target >20k RPS minimum.

- [ ] Task: Research Granian Loop Architecture
    - Analyze how Granian manages the event loop and threads.
    - Determine if we are missing a critical initialization step or architecture pattern.
- [ ] Task: Experiment - Raw PyO3 Overhead
    - Benchmark `EmbeddedWorker` using synchronous `call1` (no `asyncio`) to establish a baseline.
    - Measure the cost of `run_coroutine_threadsafe` vs `call_method1`.
- [ ] Task: Experiment - Multi-Threaded Embedded Workers
    - Implement a `WorkerPool` in Rust that holds multiple `EmbeddedWorker` instances (each with a dedicated thread/loop).
    - Dispatch Hyper requests to the pool (round-robin or channel).
- [ ] Task: Optimize "Hello World"
    - Implement the winning strategy from experiments.
    - Verify performance exceeds 20k RPS in release mode.
- [ ] Task: Flow - User Manual Verification 'Phase 3: Performance Deep Dive' (Protocol in workflow.md)

## Phase 4: Router & Integration
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
- [ ] Task: Flow - User Manual Verification 'Phase 4: Router & Integration' (Protocol in workflow.md)

## Phase 5: Frontend & Type Bridge
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
- [ ] Task: Flow - User Manual Verification 'Phase 5: Frontend & Type Bridge' (Protocol in workflow.md)

## Phase 6: Cleanup & Migration
**Goal:** Remove legacy components replaced by the new architecture.

- [ ] Task: Audit and Remove `vertebra-server` (Legacy Supervisor)
- [ ] Task: Audit and Remove `vertebra-broker` (Legacy Broker)
- [ ] Task: Remove Legacy Python IPC Code (`vertebra.core.supervisor` / `vertebra.transport`)
- [ ] Task: Clean up Workspace (`Cargo.toml`, `.gitignore`)
- [ ] Task: Flow - User Manual Verification 'Phase 6: Cleanup & Migration' (Protocol in workflow.md)
