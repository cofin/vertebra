# Research: Vertebra Performance Frontier - Beyond Litecore and Spikard

## Executive Summary

- **Litecore Learning**: ASGI is inherently slow (~25k-88k req/s) because the protocol itself forces per-request Python object allocation (`scope`), multiple async coroutine calls (`receive`, `send`), and dictionary-based message passing. WSGI actually outperformed ASGI in Litecore (~198k req/s) because Litecore built a highly specialized, synchronous Rust-backed executor that bypassed the asyncio event loop.
- **Spikard Insight**: Built by the author of Litestar, `spikard` uses a Rust core (Axum/Tokio/Tower) with `PyO3` zero-copy bindings and `msgspec` to replace Uvicorn/Hypercorn. It demonstrates that moving the *entire* HTTP stack (routing, middleware, validation) into Rust is the baseline for next-generation performance.
- **The "Next Level" Move**: To exceed both, we must stop treating ASGI as the primary contract. The "Next Level" is a **Protocol-Agnostic Native Core** that uses **Batch-Oriented FFI** (amortizing the `Python::attach` cost) and **Lazy, Zero-Object Marshalling** (where Python never sees a `Request` or `Scope` object unless it specifically asks for a field).
- **Vertebra's Unique Advantage**: Unlike Litecore or Spikard, Vertebra has a **Shared Memory Data Plane**. We can improve top-end performance by having the Rust Broker stream large response bodies (from DB or Cache) directly to the client's socket via shared memory, completely bypassing the Python worker for the *data* payload after the Python worker has authorized the request.

---

## 1.0 Codebase Analysis

### Existing Patterns in Vertebra
- **Broker Architecture**: Vertebra uses a centralized Rust broker managing resource pools.
- **Shared Memory IPC**: Communicates with Python workers via `SpscRing` and `MpmcRing` buffers in `/dev/shm`.
- **Current Bottleneck**: The server currently routes HTTP requests through the broker to a worker, which then processes and returns the result. This adds a hop compared to `litecore`'s direct `Hyper -> PyO3 -> Python` path.

### Reusable Components
- `vertebra-core`: High-performance ring buffers and shared memory management.
- `vertebra-py`: Python bindings for the IPC substrate.

---

## 2.0 Library Documentation Research

### Spikard (GitHub: Goldziher/spikard)
- **Runtime**: Rust (Axum + Tokio).
- **Bindings**: PyO3.
- **Validation**: `msgspec` (First-class), Pydantic v2 (Optional).
- **Philosophy**: Replace the Python ASGI server with a native Rust runtime that handles everything except the user's business logic.
- **Observation**: It still follows a "one request, one call" model, which keeps FFI overhead per-request.

### Litecore (Prior Art)
- **Specialized WSGI**: Proved that synchronous, lock-free, Rust-backed executors are faster for Python than generic async-loop-based adapters.
- **Native Routing**: Proved that moving route matching to Rust saves significant Python cycles.

---

## 3.0 Prior Art & Solutions

### Top-End ASGI Performance Killers
1. **Async Context Switching**: The `await send()` and `await receive()` calls in ASGI are full coroutine yields.
2. **Object Inflation**: Creating a `scope` dict and multiple message dicts per request is expensive in Python.
3. **FFI Boundary Crossing**: Calling into Python from Rust once per request is the standard, but it's expensive due to GIL/Interpreter state management.

### The Recommended "Next Level" Approach

| Feature | Current Best (Spikard/Litecore) | Next Level (Vertebra) |
|---------|---------------------------------|-----------------------|
| **Routing** | Native Rust Router | **JIT-Compiled Native Router** with Route-ID handoff. |
| **FFI Frequency** | 1 Call per Request | **Batch FFI**: 1 Call per *Batch* of Requests (Amortizes `Python::attach`). |
| **Request Object** | PyO3 Wrapper or Dict | **Zero-Object Proxy**: Python receives a Raw Memory Pointer/Handle; fields are lazily fetched from Rust only on access. |
| **Response Body** | Python -> Rust Copy | **SHM Direct-to-Socket**: Rust Broker streams from Shared Memory directly to the NIC, bypassing Python's `send()` entirely for the body. |
| **Protocol** | ASGI / WSGI / RSGI | **Unified Batch Native**: A internal bridge that *emulates* ASGI only for legacy compatibility, but uses a raw synchronous batch-callback for high-perf routes. |

---

## 4.0 Risk Assessment

### Technical Risks
- **Complexity of Batching**: Implementing a batch-aware Python framework (middleware, error handling) is harder than a per-request model.
- **SHM Security**: Streaming directly from SHM to sockets requires careful bounds checking to prevent memory leaks or info disclosures.
- **GIL Contention**: Even with a Rust core, Python code is still GIL-bound. Moving more to Rust is the only way to scale further without multiple processes.

### Mitigation
- Start with a "Direct Path" for 200 OK responses.
- Implement a "Hybrid Worker" that can handle both ASGI (legacy) and Native Batch (performance).

---

## 5.0 Recovery & Recovery Planning

**Rollback Plan**: If the Native Batch path fails, we can fall back to the existing ASGI-adapter model in `vertebra-server`.
**Checkpoint**: Implement the Native Router in Rust first, then the Batch FFI, then the SHM streaming.

---

## 6.0 Research Synthesis

### How to improve top-end ASGI performance?
1. **Bypass the Protocol**: Don't use `scope`, `receive`, or `send` inside your own framework. Use a specialized "Internal Request" object that mimics the `msgspec.Struct` or a C-extension handle.
2. **Lazy Scope**: Only populate the `scope` dictionary if a middleware or user handler actually accesses it.
3. **Task-Group Scheduling**: Instead of spawning a single coroutine per request, spawn a "Batch Task" that processes multiple requests in a loop, reducing the `asyncio.create_task` overhead.

### Where would I go next?
I would build a **"Ghost Framework"**:
- The "framework" is 90% Rust code that has been AOT-compiled.
- The Python side is just a "Scripting Layer" that defines the business logic.
- When a request comes in, the Rust core does the routing, validation, and authorization.
- It then calls into Python using a **Vectorized FFI** (passing 10-100 requests at once).
- The Python code returns the results, and the Rust core handles the serialization.

### Spikard as a reference:
Spikard is the current "State of the Art" for "Rust Server + Python Handlers". To beat it, Vertebra must exploit its **Broker** and **Shared Memory** to move the *Data Plane* entirely out of Python.
