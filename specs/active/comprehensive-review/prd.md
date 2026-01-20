# PRD: Comprehensive Review & Performance Optimization

## 1. Problem Statement

Vertebra aims to be an ultra-performant framework for Python/TypeScript cross-language applications. While the core IPC primitives (SPSC Shared Memory Rings) are implemented, several architectural components are either missing, misaligned with the documentation, or suboptimal for high-performance workloads.

### Key Issues
1.  **Incomplete Rust Core**: `MpmcRing` is currently in-memory only, preventing efficient multi-producer/multi-consumer patterns across processes.
2.  **Missing Rust Supervisor**: The process supervisor is implemented in Python, incurring GIL overhead and lacking the robustness of a native implementation.
3.  **Suboptimal Data Plane**: Python drivers perform expensive data conversions (JSON/MsgPack to Python lists/tuples) instead of leveraging zero-copy Arrow IPC for large result sets.
4.  **Inconsistent Sync API**: Synchronous drivers wrap asynchronous IPC calls, leading to unnecessary event loop overhead.
5.  **Observability Gaps**: Missing standardized metrics and health monitoring endpoints.
6.  **Type Bridge Limitations**: Lack of Hot Module Replacement (HMR) and limited metadata extraction for complex types.

## 2. Goals & Objectives

1.  **Complete the IPC Core**: Port `MpmcRing` to shared memory.
2.  **Native Supervisor**: Implement a GIL-free Rust supervisor for process lifecycle management.
3.  **True Zero-Copy Drivers**: Full integration of Arrow IPC for both query parameters (`execute_many`) and result sets.
4.  **Native Sync Client**: Implement a truly synchronous `SyncBrokerClient` in Python (using CFFI/Rust bindings) to avoid `asyncio.run` overhead in sync contexts.
5.  **Naming Parity**: Enforce explicit `Sync` and `Async` prefixes for all classes where both synchronous and asynchronous versions exist or are planned (e.g., `SyncBrokerClient` and `AsyncBrokerClient`).
6.  **Observability**: Prometheus-compatible metrics endpoint in the Bun broker.
7.  **Developer Experience**: High-performance Type Bridge with Vite HMR support.

## 3. Targeted Components

-   **`src/rs/vertebra-core`**: `MpmcRing` shared-memory implementation.
-   **`src/rs/vertebra-py`**: Native Supervisor implementation.
-   **`src/py/vertebra/drivers`**: Update to support Arrow-native results and sync-native IPC.
-   **`src/ts/src/broker`**: Implementation of metrics, Arrow data-plane handlers, and improved scheduling.
-   **`src/py/vertebra/types`**: Type Bridge HMR and enhanced extraction.

## 4. Proposed Implementation Plan

### Phase 1: IPC Core & Infrastructure (Rust)
-   [ ] Implement `MpmcRing` using shared memory and sequence-based synchronization.
-   [ ] Implement native `Supervisor` in Rust, handling SIGTERM/SIGKILL and process groups.
-   [ ] Expose `Supervisor` to Python via `vertebra._vertebra`.

### Phase 2: High-Performance Data Plane
-   [ ] Update Bun broker to support Arrow RecordBatch serialization for database results.
-   [ ] Update `BrokerClient` (Python) to support Arrow for `execute_many` parameters.
-   [ ] Implement `VertebraArrowCursor` in Python for direct pyarrow-to-dataframe/numpy integration.
-   [ ] Implement `SyncBrokerClient` using Rust's blocking IPC calls to avoid `asyncio` overhead.

### Phase 3: Observability & Reliability
-   [ ] Implement `/__metrics__` in Bun using `prom-client`.
-   [ ] Add starvation tracking and fairness metrics to the `FairScheduler`.
-   [ ] Implement heartbeat mechanism between Python workers and Bun broker.

### Phase 4: Developer Experience & Type Bridge
-   [ ] Implement Vite HMR plugin for `vertebra-types`.
-   [ ] Add support for `Annotated` metadata extraction (e.g., `msgspec.Meta`).
-   [ ] Improve forward-reference resolution and circular dependency handling in TS generation.

## 5. Acceptance Criteria

-   **Performance**: `MpmcRing` (shared memory) throughput > 1M msgs/sec.
-   **Zero-Copy**: Database results > 10MB bypass MessagePack and use Arrow IPC.
-   **Robustness**: Supervisor correctly reaps zombies and handles process crashes without leaking shared memory.
-   **Usability**: Vite HMR updates types in < 100ms on Python file change.
-   **Compatibility**: Sync drivers work without a running `asyncio` loop and show < 5% overhead vs native async.

## 6. Recovery & Verification Plan

### Verification
-   Benchmarks: Compare Arrow vs MessagePack for large datasets.
-   Integrations: Ensure existing Litestar/SQLSpec tests pass.
-   Stress Tests: Run supervisor under heavy process churn.

### Recovery
-   If `MpmcRing` shared-memory proves too complex for MVP, fallback to SPSC pairs with a central dispatcher.
-   If native Supervisor has signal issues on Windows, retain Python implementation as a fallback.