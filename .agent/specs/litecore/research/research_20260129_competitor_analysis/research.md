# Research: Competitor Analysis & High-Performance Strategy

## 1. Executive Summary
-   **Current Landscape:** **Granian** is the performance leader, leveraging **RSGI** to bypass Python's asyncio loop overhead. **Uvicorn** is the standard but limited by Python execution speed. **Robyn** uses a custom Rust runtime but breaks ASGI compatibility.
-   **The 200k RPS Barrier:** To exceed 200k RPS (Hello World), we must avoid creating Python `dict` scopes and `await` calls for every byte. **RSGI** or a custom **"Direct Dispatch"** protocol is required.
-   **Spikard Inspiration:** **Spikard** demonstrates that a shared Rust core (`axum`/`tokio`) can power multiple languages (Python, TS, Ruby) with consistent APIs. Its usage of `msgspec` (Python) and `napi-rs` (Node) for zero-copy parallels our goals.
-   **Litecore Strategy:** "Granian + Vite + Spikard". Adopt Granian's RSGI threading model for raw speed, Spikard's polyglot architectural pattern, but differentiate with **"Zero-Node" Asset Bundling** (Rolldown) and **Polyglot Middleware** capabilities.
-   **GIL Management:** Critical 2026 pattern: Detach from GIL for *all* HTTP/DB I/O. Use `pyo3-asyncio` (v0.20+) or its successor to drive Python futures on the Tokio runtime.

## 2. Competitor Deep Dive

### 2.1 Granian (The Benchmark)
*   **Architecture:** Rust (Hyper/Tokio). Supports WSGI, ASGI, and **RSGI**.
*   **Secret Sauce:** **RSGI**. Instead of converting HTTP requests to complex ASGI dictionaries (expensive in Python), RSGI exposes Rust structs directly to Python via PyO3.
*   **Strengths:** Correctness (HTTP/1 & 2), Performance (50k-100k+ RPS), Threading model (multi-threaded runtime vs multi-process).
*   **Weaknesses:** "Just a server". No asset bundling, no dev server integration, standard pip install.

### 2.2 Uvicorn (The Standard)
*   **Architecture:** Python + Cython (`uvloop`, `httptools`).
*   **Bottleneck:** Every request requires Python loop iterations. Protocol parsing calls into Python callback.
*   **Strengths:** Stability, Ecosystem compatibility.
*   **Weaknesses:** Ceiling on RPS due to Python overhead.

### 2.3 Robyn (The Rebel)
*   **Architecture:** Custom Rust runtime.
*   **Weaknesses:** Historically non-standard/buggy ASGI. Ecosystem friction.

### 2.4 Spikard (The Polyglot)
*   **Architecture:** Rust Core (`axum`, `tokio`, `tower`) with thin bindings for Python (`pyo3`), Node (`napi-rs`), Ruby (`magnus`), PHP (`ext-php-rs`).
*   **Secret Sauce:** Unified Middleware & Validation. It enforces a consistent API across languages.
*   **Optimization:** Uses `msgspec` for Zero-Copy JSON in Python and `napi-rs` buffer mapping in Node.
*   **Relevance:** Spikard proves the viability of a "Rust Core, Many Languages" strategy. `litecore` is essentially "Spikard for Python + Frontend Bundling".

## 3. The Power of RSGI (Rust Server Gateway Interface)
*   **ASGI Overhead:**
    1.  Parse HTTP (Rust/C) -> Create Python `dict` (Scope) -> `await app(scope, receive, send)`.
    2.  `receive()` creates `bytes` object.
    3.  `send()` waits for event loop.
*   **RSGI Optimization:**
    1.  Parse HTTP (Rust) -> Call Python Function with `Request` object (Rust Struct wrapper).
    2.  No `await` loop for the body unless requested.
    3.  Direct byte-stream access.
*   **Litecore Decision:** We should support **RSGI** natively. It allows "Hello World" to be nearly pure Rust speed.

## 4. Litecore Differentiation: "The Full Stack Engine"
While Granian is a "Better Uvicorn", Litecore is "Next.js for Python".
1.  **Bundling:** We embed `rolldown`. Granian doesn't know about frontend.
2.  **Dev Experience:** `litecore serve` manages the frontend dev server + HMR.
3.  **One Binary:** `litecore build` produces a deployable artifact.

## 5. Performance Checklist (2026 Best Practices)
1.  **Memory Allocator:** Use `mimalloc` or `jemalloc` in Rust binary.
2.  **PyO3 Optimization:**
    *   Use `Bound<'py, T>` API (PyO3 0.21+).
    *   Avoid `extract()`; use `downcast()` or `unsafe` APIs where validity is guaranteed.
    *   **Disable Garbage Collection** (`gc.disable()`) during critical loop phases if safe.
3.  **Tokio Tuning:** Use `tokio::runtime::Builder::new_multi_thread()` with `worker_threads` tuned to CPU cores. Pin Python workers to specific cores if possible (Core Affinity).

## 6. Recommended Architecture for Litecore
```rust
// The "Hyper-Engine"
struct LitecoreServer {
    // Hyper H1/H2/H3 service
    // Manages Python Interpreter (PyO3)
}

impl Service for LitecoreServer {
    fn call(&self, req: Request) -> Future {
        // 1. Fast Path: Static Assets (Reflinked/Mmap)
        if let Some(asset) = self.assets.get(req.path()) {
            return response(asset);
        }
        
        // 2. RSGI Dispatch (if app supports it)
        if self.app_type == AppType::RSGI {
            return self.dispatch_rsgi(req);
        }
        
        // 3. ASGI Fallback
        return self.dispatch_asgi(req);
    }
}
```
