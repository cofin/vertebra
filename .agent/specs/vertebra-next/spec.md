# Spec: Vertebra Next (Hybrid Runtime)

**Flow ID:** `vertebra-next`
**Status:** In Progress
**Context:** The "Hybrid Model" (Path B) to beat Granian/Uvicorn.

## 1. Architecture Overview
The system transitions from a simple "Supervisor" model to a **Dual-Path Execution Engine**. The Rust binary acts as the primary entry point (Hyper-based HTTP server) and manages both an internal Python interpreter and external Python worker processes.

### 1.1 Component 1: The Embedded Fast Path (In-Process)
*   **Stack:** `hyper` (Rust) → `PyO3` → `PyO3-Asyncio` → `ASGI App` (Python).
*   **Mechanism:** Direct memory mapping of request/response buffers via PyO3. No serialization overhead.
*   **Constraint:** Shares the Global Interpreter Lock (GIL) with the main thread's internal Python state.
*   **Use Case:** Low-latency API routes, middleware, and I/O-bound logic.

### 1.2 Component 2: The IPC Slow Path (Multi-Process)
*   **Stack:** `hyper` (Rust) → `Litecore IPC Ring` (Shared Memory) → `ProcessManager` → `Python Worker`.
*   **Source:** Ported directly from the `litecore` project's `Ring` and `IPC` modules.
*   **Mechanism:** Requests are serialized (via Msgpack or similar) into a circular ring buffer. Workers poll the ring, execute the logic in an isolated process, and write results back.
*   **Use Case:** CPU-intensive tasks, blocking operations, or code requiring strict memory isolation.

### 1.3 Component 3: Native Frontend Engine (Rolldown)
*   **Stack:** `Rolldown` (Rust) → `DevEngine` → Browser HMR.
*   **Mechanism:** Embedded Rolldown bundler replaces Vite/Node.js for asset serving and HMR.
*   **Use Case:** "Zero-Node" development experience for React/Vue/Svelte frontends.

### 1.4 Component 4: Type Bridge Integration
*   **Stack:** `AST Extractor` (Python) → `TypeBridgeWatcher` → `Rolldown` (Rust).
*   **Mechanism:** Python file changes trigger AST analysis. Generated TypeScript definitions are pushed to Rolldown's virtual module system, triggering browser HMR.
*   **Enhancements:** Support for `attrs`, `cattrs`, and `msgspec` structures.

## 2. Requirements

### R1: Foundation (Litecore Port)
- Re-implement `litecore` IPC primitives (Ring, Slab, Shm) in `vertebra-engine`.
- Implement `WorkerSupervisor` to manage lifecycle of Python child processes.
- Establish base `Worker` trait.

### R2: Embedded Executor
- Integrate `PyO3` to host ASGI app directly.
- Implement `DirectExecutor` mapping `hyper::Request` to `ASGI`.
- Handle GIL orchestration (ensure async loop isn't blocked).

### R3: Router Integration
- `DispatchTable` marking routes as `@fast_path` or `@worker_path`.
- Load-aware dispatching (spillover to workers).

### R4: Frontend & Type Bridge
- Implement `RolldownExecutor` with HMR support.
- Port and enhance `TypeBridge` (AST-based) to support `attrs`.
- Wire TypeBridge updates to Rolldown for real-time type sync.