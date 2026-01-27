# Master PRD: Vertebra v2 - Native Rust Core Rewrite

## Context
Vertebra is evolving from a hybrid Python/Bun architecture to a **Native Rust Core**. The current Bun-based Broker is a performance bottleneck and deployment complexity. This rewrite aims to exceed Granian's performance by moving the entire HTTP, IPC, and Resource Management layer into Rust, while keeping the developer experience Python-centric.

## North Star Metrics
1.  **Throughput:** Exceed Granian's RPS benchmarks.
2.  **Deployment:** Single `pip install` or binary (PyApp) - no external runtimes.
3.  **Features:** Native HTTP/2, HTTP/3, gRPC.
4.  **Cleanup:** 100% removal of legacy `src/ts` Broker code.

## Global Constraints
- **Rust Stack:** Tokio, Hyper, Axum (optional), Arrow (IPC).
- **Python:** 3.12+, implementation via `vertebra.server` C-extension.
- **IPC:** Shared Memory Ring Buffers (Zero-Copy).
- **No Node/Bun Runtime:** The core server must be self-contained.

## Roadmap (Chapters)

### Phase 1: Foundation & Networking
1.  **Flow: `server-foundation`**
    -   **Goal:** Boot a Rust-based HTTP server from Python.
    -   **Tech:** `pyo3`, `tokio`, `hyper`.
    -   **Deliverable:** `python app.py` starts a high-perf Rust HTTP server that responds "Hello".

2.  **Flow: `ipc-worker-bridge`**
    -   **Goal:** Establish the Control & Data Plane between Rust Server and Python Workers.
    -   **Tech:** Shared Memory Ring Buffers, Signal Handling (`eventfd`).
    -   **Deliverable:** Rust server can spawn Python workers and exchange raw bytes.

3.  **Flow: `python-asgi-adapter`**
    -   **Goal:** Connect the Rust IPC to the Python ASGI interface.
    -   **Tech:** `msgspec`, `vertebra.worker`.
    -   **Deliverable:** Full Request/Response cycle (Rust -> IPC -> Python ASGI -> IPC -> Rust).

### Phase 2: The New Broker (Resources)
4.  **Flow: `rust-broker-resources`**
    -   **Goal:** Reimplement Connection Pooling and Fairness Scheduler in Rust.
    -   **Tech:** `sqlx` (or `deadpool`), `dashmap`.
    -   **Deliverable:** Python workers can lease DB connections from the Rust Broker.

### Phase 3: Frontend & Polish
5.  **Flow: `frontend-native-bridge`**
    -   **Goal:** Native integration with Rspack/Vite for asset bundling.
    -   **Tech:** Rust `Command` management, Static file serving.
    -   **Deliverable:** "One File" experience serving a React/Vue app.

6.  **Flow: `advanced-networking`**
    -   **Goal:** Enable HTTP/2, HTTP/3, gRPC, SSE.
    -   **Tech:** `h2`, `h3`, `tonic`.
    -   **Deliverable:** Full modern web support.

### Phase 4: Release
7.  **Flow: `cleanup-and-packaging`**
    -   **Goal:** Delete `src/ts` and finalize PyApp packaging.
    -   **Deliverable:** The Great Purge and v2.0 Release.

## Cleanup Inventory (To Be Deleted)
*Analysis of `src/ts` indicates the following targets for deletion in Flow 7:*
- `src/ts/src/broker/*` (Entire directory)
- `src/ts/src/server/*` (Entire directory)
- `src/ts/src/ipc/*` (Replaced by Rust implementation)
- `src/ts/index.ts`
- `src/rs/vertebra-js` (Likely obsolete if no longer binding to Bun)
