# Research: Litecore Architecture & Layout

## 1. Crate Structure
Litecore is a Rust binary that links against the Python library.

```text
/home/cody/code/c/vertebra/
├── crates/
│   └── litecore/
│       ├── Cargo.toml
│       ├── build.rs        (For embedding defaults)
│       ├── src/
│       │   ├── main.rs     (CLI)
│       │   ├── runtime/    (Tokio/Hyper)
│       │   ├── bridge/     (ASGI/PyO3)
│       │   │   ├── conversion.rs
│       │   │   └── executor.rs
│       │   ├── bundler/    (Rolldown)
│       │   └── fs/         (Virtual Filesystem)
```

## 2. Python Embedding Strategy
*   **PyO3 `auto-initialize`:** We will use PyO3's ability to initialize the interpreter.
*   **GIL Management:**
    *   The main Tokio loop runs **without** the GIL.
    *   GIL is acquired *only* when:
        1.  Converting the Request to ASGI Scope.
        2.  Pollling the Python Coroutine.
    *   **Optimization:** Use `pyo3_asyncio` (or its v0.20 successor) to drive Python futures on the Tokio event loop efficiently.

## 3. ASGI Bridge Details
*   **Scope Creation:** To beat Uvicorn, we must minimize dictionary allocation.
    *   *Idea:* Pre-allocate or reuse PyDicts if possible (though difficult due to GIL).
    *   *Standard:* Use `PyDict::new` and strict typing.
*   **Body Streaming:**
    *   Rust Body -> Channel -> Python `receive()`.
    *   Python `send()` -> Channel -> Rust Body.
    *   Use `tokio::sync::mpsc` for bridging the streams.

## 4. Rolldown Integration
*   **Input:** `frontend/index.html` (entry point).
*   **Output:** `InMemoryFS` struct (`HashMap<Path, Bytes>`).
*   **Serving:** `axum::routing::get` matches path, looks up in `InMemoryFS`.
*   **Compression:** Gzip/Brotli compress assets *during build* and store compressed bytes. Serve with `Content-Encoding` header directly. Zero runtime compression cost.

## 5. Executor Trait (For Dev Mode)
```rust
trait FrontendExecutor {
    async fn start(&self, port: u16) -> Result<Child>;
    fn build(&self) -> Result<()>;
}
// Implementations: NodeExecutor (Vite), RolldownExecutor (Internal)
```
