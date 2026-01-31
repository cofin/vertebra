# Product: Litecore (The Polyglot Server)

## 1. Vision
**Litecore** is the ultimate "Batteries-Included" Runtime for modern Python web development. It fuses the raw performance of a Rust HTTP server with the developer experience of a next-gen frontend toolchain.

**The Goal:** Make Python + React/Vue/Svelte development as seamless as `Next.js` or `Nuxt`, but with a Python backend.

## 2. Core Architecture

### 2.1 The "Hyper-Engine" (In-Process)
*   **Architecture:** Embeds the Python interpreter (PyO3) directly into a Rust binary.
*   **Server:** Built on `hyper`, `h3` (HTTP/3), and `tokio`.
*   **Protocols:** Native support for:
    *   **RSGI (Rust Server Gateway Interface):** Direct-dispatch protocol bypassing ASGI overhead for max performance.
    *   **ASGI (Standard):** Fallback for compatibility.
    *   HTTP/1.1, HTTP/2 (h2c/h2), HTTP/3 (QUIC).
    *   WebSockets (Tungstenite).
    *   Server-Sent Events (SSE).
    *   gRPC (Tonic).
*   **Optimization:** Inspired by Spikard, uses `msgspec` for Zero-Copy JSON handling and shared memory structures where possible.
*   **Lifecycle:**
    1.  Rust starts -> Loads Config.
    2.  Rust initializes Python Interpreter.
    3.  Rust loads `app` module.
    4.  Rust starts Hyper listeners.
    5.  Request -> Rust -> PyO3 -> ASGI Scope -> Rust.

### 2.2 Native Asset Bundling (Zero-Node Build)
*   **Problem:** Building a Python + JS app requires a complex toolchain.
*   **Solution:** Litecore embeds **Rolldown** (Rust Bundler) via the `rolldown` crate.
*   **Virtual FS:**
    *   Assets are bundled into a `AssetArchive` (e.g., a compressed binary blob appended to the executable or a separate `.pak` file).
    *   Rust serves these assets from memory/mmap using a `VirtualFileSystem` abstraction.
    *   **Benefits:** Zero IO syscalls for small assets, aggressive caching (ETag/Immutable).

### 2.3 The "One File" Distribution
*   **Output:** A single binary (or self-contained PyApp) that contains:
    *   The Litecore Runtime (Rust).
    *   The Python Application (Bytecode).
    *   The Bundled Static Assets (Rolldown Output).
*   **Execution:** `./myapp` runs the server, serves assets from memory, and handles API requests.

### 2.6 The Polyglot Build Engine
Litecore replaces external build scripts (Makefiles, npm scripts) with internal Rust logic.
*   **Dev Mode:** The Engine supervises both the Python Worker and the Frontend Dev Server (Vite/Rspack). It proxies requests transparently (`/api` -> Python, `/` -> Vite).
*   **Automatic Port Management:** Vertebra finds a free port, assigns it to the frontend executor (via `PORT` env var or arguments), and configures its internal proxy automatically. No manual `dev_url` configuration is required.
*   **Build Mode:** The Engine directly invokes the **Rolldown** Rust API to bundle the frontend assets. This eliminates the need for a Node.js process during the bundle step (though `node_modules` must be present). It collects the static assets and bundles them into the final distribution.
*   **One File Distribution:** The final output is a single binary (or PyApp) containing the Rust Engine, Python Bytecode, and **Compressed JS Assets**. No separate "frontend deployment" required.

### 2.7 Plugin Architecture (Optional Integrations)
*   **The Problem:** Monolithic servers are hard to extend.
*   **The Solution:** Litecore exposes a Rust `StatePlugin` trait.
*   **Mechanism:**
    *   Plugins initialize at startup (e.g., connecting to `vertebra-ipc` ring).
    *   Plugins inject resources into the **Request Extensions** (RSGI) or **ASGI Scope**.
*   **Use Case:** Connecting `litecore` to `vertebra-ipc` allows Python workers to access the shared Data Pool with zero initialization cost per request.

## 3. Configuration (`pyproject.toml`)

```toml
[tool.litecore]
app = "main:app"
workers = "auto" # or number
port = 8000

[tool.litecore.frontend]
source = "./frontend"
dist = "./dist"
executor = "node" # "node" (Vite) or "rolldown" (Internal)
dev_command = "npm run dev"
build_command = "npm run build" # Ignored if executor="rolldown"
```

## 4. File Structure Proposal

```text
litecore/
├── Cargo.toml
├── src/
│   ├── main.rs             # CLI Entrypoint
│   ├── server/             # Hyper/Axum Server
│   │   ├── mod.rs
│   │   ├── http.rs
│   │   └── ws.rs
│   ├── bridge/             # Python/ASGI Bridge
│   │   ├── mod.rs
│   │   └── asgi.rs         # Scope builder
│   ├── assets/             # Asset Manager
│   │   ├── bundler.rs      # Rolldown integration
│   │   └── serving.rs      # Static file handler
│   └── config.rs           # TOML parser
└── py_src/                 # Litecore Python helper package
    └── litecore/
```

## 5. Success Metrics
*   **Throughput:** > 100k RPS for "Hello World" (vs Uvicorn ~20-30k).
*   **Startup:** < 100ms boot time.
*   **Bundle Size:** < 20MB overhead.