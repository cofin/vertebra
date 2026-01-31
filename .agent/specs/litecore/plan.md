# Plan: Litecore Implementation

## Phase 1: The Hyper-Engine (Foundation)
**Goal:** Run a minimal RSGI/ASGI app inside Rust with high performance.

### 1.1 Scaffold & Runtime
- [ ] **Task 1.1.1:** Initialize `litecore` crate.
    -   `Cargo.toml`: `pyo3 = { version = "0.23", features = ["auto-initialize", "abi3-py312"] }`, `tokio = { version = "1", features = ["full"] }`, `hyper = { version = "1", features = ["full"] }`, `clap`, `tracing`, `tower`.
    -   Define `main.rs` with `#[tokio::main]`.
    -   Setup `tracing-subscriber` for structured logging.
- [ ] **Task 1.1.2:** Python Embedding.
    -   Implement `Python::with_gil` initialization block.
    -   Create `LitecoreRuntime` struct that holds the `Py<PyAny>` app reference.
    -   **Constraint:** Ensure `PYTHONPATH` includes the current working directory.

### 1.2 RSGI Implementation (P0 - Priority)
- [ ] **Task 1.2.1:** Define RSGI Protocol (Rust).
    -   Struct `RsgiRequest`: `method: String`, `uri: String`, `headers: Vec<(String, String)>`, `body: PyObject` (bytes or stream).
    -   Struct `RsgiResponse`: `status: u16`, `headers: Vec<(String, String)>`, `body: BodyEnum`.
- [ ] **Task 1.2.2:** Implement RSGI Bridge.
    -   Create `dispatch_rsgi(py: Python, app: &PyAny, req: Request<Incoming>) -> PyResult<Response<BoxBody>>`.
    -   **Optimization:** Use `msgspec` patterns for zero-copy header extraction if possible, or just optimized `PyBytes` creation.
    -   Implement `release_gil` during the body stream consumption.

### 1.3 ASGI Fallback
- [ ] **Task 1.3.1:** ASGI Scope Builder.
    -   Map `hyper::Request` to ASGI `scope` dict.
    -   **Critical:** Optimize string allocations. Use interned strings for common keys (`type`, `http_version`, `method`).

- [ ] **Task 1.4:** Plugin System.
    -   Define `StatePlugin` trait in Rust.
    -   Implement `Extensions` map for Request state.
    -   Expose extensions to Python via RSGI/ASGI bridge.

## Phase 2: Protocols & Performance
**Goal:** Feature parity with Uvicorn + Granian speed.

### 2.1 WebSockets
- [ ] **Task 2.1.1:** Handshake & Upgrade.
    -   Detect `Upgrade: websocket`.
    -   Use `hyper-tungstenite` or `tokio-tungstenite`.
- [ ] **Task 2.1.2:** RSGI WebSocket.
    -   Define `RsgiWebsocket` struct with `recv_bytes`, `recv_text`, `send_bytes`, `send_text`.
    -   Expose directly to Python handler.

### 2.2 HTTP/2 & HTTP/3
- [ ] **Task 2.2.1:** H2 Support.
    -   Enable `http2` feature in `hyper::server::conn::auto::Builder`.
- [ ] **Task 2.2.2:** H3 (QUIC) Exploration.
    -   Investigate `h3` crate integration. *Deferred to Phase 2.5.*

## Phase 3: The Bundler (Rolldown)
**Goal:** "Zero-Node" frontend building.

### 3.1 Rolldown Integration
- [ ] **Task 3.1.1:** Add `rolldown` crate.
    -   Create `bundler` module.
    -   Define `BundlerConfig` struct matching `pyproject.toml` [tool.litecore.frontend].
- [ ] **Task 3.1.2:** In-Memory Build.
    -   Invoke `rolldown::Bundler` on `frontend/index.html`.
    -   Capture output assets (JS/CSS) into `HashMap<String, Bytes>`.

### 3.2 Virtual File System (VFS)
- [ ] **Task 3.2.1:** `AssetStore` Trait.
    -   Implementations: `DiskStore` (dev), `MemoryStore` (prod/onefile).
- [ ] **Task 3.2.2:** Static Serving.
    -   Create Axum/Hyper service to serve from `AssetStore`.
    -   Implement ETag / Cache-Control logic.

## Phase 4: Developer Experience
**Goal:** `litecore serve` handles everything.

### 4.1 Configuration
- [ ] **Task 4.1.1:** TOML Parser.
    -   Use `toml` crate to parse `pyproject.toml`.
    -   Extract `[tool.litecore]` and `[tool.litecore.frontend]`.

### 4.2 The "Pluggable Executor"
- [ ] **Task 4.2.1:** `FrontendExecutor` Trait.
    -   `async fn start(&self, port: u16) -> Result<ProcessHandle>`.
    -   `fn build(&self) -> Result<()>`.
- [ ] **Task 4.2.2:** Implementations.
    -   `NodeExecutor`: Spawns `npm run dev` / `vite`.
    -   `RolldownExecutor`: Runs internal watcher.
- [ ] **Task 4.2.3:** Port Discovery.
    -   Find random free port.
    -   Pass to executor via `PORT` env var.
- [ ] **Task 4.2.4:** Reverse Proxy.
    -   Proxy `localhost:8000/*` -> `localhost:<frontend_port>/*`.
    -   Except `/api/*` -> Python.

## Phase 5: One-File Distribution
**Goal:** `litecore build` = Single Binary.

### 5.1 PyApp Integration
- [ ] **Task 5.1.1:** PyApp Config.
    -   Generate `pyapp.toml` configuration dynamically.
- [ ] **Task 5.1.2:** Asset Embedding.
    -   Compress bundled assets (`brotli`).
    -   Append to the binary (or include in the PyApp payload).
    -   Update `MemoryStore` to read from embedded location.
