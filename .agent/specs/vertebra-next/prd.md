# Vertebra Next: The "Zero-Overhead" Architecture

## 1. Vision
**Vertebra Next** is a radical simplification and optimization of the Python web server model. It abandons the traditional "Python Server" concept (ASGI/WSGI) in favor of a **"Logic Processor"** model.

**The Core Thesis:** Python is slow at I/O and Concurrency, but excellent at Business Logic. Therefore, **Python should never touch a socket, a file handle, or a database connection.**

Vertebra Next is a **Rust Hyper-Engine** that manages the entire lifecycle of an HTTP request, database transaction, and static asset. It "borrows" the Python interpreter only for the milliseconds required to compute business logic.

It is also a **Full-Stack Build Engine**. It doesn't just run Python; it manages the frontend build chain (Vite/Rspack) and bundles the resulting assets into a single, self-contained binary deployment, mimicking the "One File" experience of Go or Go-embedded-JS apps.

## 2. Core Pillars

### 2.1 The "Phantom" IPC (Zero-Copy 2.0)
*   **Current (v2):** Python copies data into/out of Ring Buffers.
*   **Next:** Python **maps** Ring Buffer slots directly into `memoryview` or `pyarrow.Table`.
*   **Mechanism:**
    *   Request Body: Rust writes to SHM. Python receives a `ReadOnlyBuffer`. Zero Copy.
    *   Response Body: Python writes to SHM (pre-allocated slot). Rust sends to socket. Zero Copy.
    *   Large Data (SQL): Database Driver (Rust) writes Arrow batches to SHM. Python maps them as Pandas/Polars DataFrames. **True Zero Copy.**

### 2.2 The "Engine" Concept (Deprecating "Broker")
The term "Broker" implies a middleman. "Engine" implies the runtime.
*   **Single Binary:** `vertebra` is a compiled Rust binary.
*   **Lifecycle:** `vertebra` starts, spawns `uv` managed Python workers, establishing SHM links.
*   **Hot Path:** 99% of CPU time is spent in Rust (Parsing HTTP, Routing, SSL, DB I/O). Python is only woken up for the handler.

### 2.3 "Optimistic" Routing & Dispatch
*   **Rust Router:** `matchit` in Rust handles *all* routing.
*   **Static Assets:** Served entirely by Rust (using `sendfile` / Reflinking) from a global CAS cache. Python never sees a static file request.
*   **Auth at the Edge:** JWT/Session validation happens in Rust. Invalid requests are rejected before Python wakes up.

### 2.4 The "Data Plane" (Arrow First)
*   **Standard:** All SQL/Redis results > 1KB are returned as Arrow IPC streams.
*   **Integration:** First-class support for Pydantic v2 (Rust core) and Msgspec.

### 2.5 The Polyglot Build Engine
Vertebra replaces external build scripts (Makefiles, npm scripts) with internal Rust logic.
*   **Dev Mode:** The Engine supervises both the Python Worker and the Frontend Dev Server (Vite/Rspack). It proxies requests transparently (`/api` -> Python, `/` -> Vite).
*   **Automatic Port Management:** Vertebra finds a free port, assigns it to the frontend executor (via `PORT` env var or arguments), and configures its internal proxy automatically. No manual `dev_url` configuration is required.
*   **Build Mode:** The Engine directly invokes the **Rolldown** Rust API to bundle the frontend assets. This eliminates the need for a Node.js process during the bundle step (though `node_modules` must be present). It collects the static assets and bundles them into the final distribution.
*   **One File Distribution:** The final output is a single binary (or PyApp) containing the Rust Engine, Python Bytecode, and **Compressed JS Assets**. No separate "frontend deployment" required.

### 2.6 Middleware Mode (SSR Integration)
While Vertebra is designed to be the "Main" server, it can also run as a **Middleware Library** inside other server frameworks (Next.js, Nuxt, Astro, Express).
*   **Use Case:** Adding high-performance Python logic (AI, Data Processing) to an existing Node.js/SSR application.
*   **Mechanism:**
    *   **HTTP Proxy Mode:** The Node server forwards specific routes (e.g. `/api/python/*`) to a local Vertebra instance managed as a subprocess.
    *   **FFI Mode (Advanced):** Using NAPI-RS, the Vertebra Engine runs *inside* the Node process as a native module, exposing a `handleRequest(req)` function that dispatches directly to the shared memory ring buffer. This eliminates the loopback TCP overhead.
*   **Integration:** We provide `vertebra-node` (npm package) and `vertebra-express` / `vertebra-next` adapters that wrap this logic.

## 3. Architecture

```mermaid
graph TD
    User[HTTP Client] -->|TCP/HTTP3| Engine[Vertebra Engine (Rust)]
    
    subgraph "The Engine (Rust)"
        Net[Hyper / Tokio]
        Router[Matchit Router]
        Auth[Auth Validator]
        Cache[Asset Cache (Reflinked)]
        DB[Connection Pools (SQL/Redis)]
        Builder[Build/Asset Manager]
    end
    
    subgraph "External Tools"
        Vite[Vite/Rspack (Node/Rust)]
    end
    
    subgraph "Shared Memory"
        RingReq[Request Ring (MPMC)]
        RingResp[Response Ring (SPSC)]
        DataPool[Arrow Data Pool]
    end
    
    subgraph "Python Worker"
        Logic[Business Logic]
        SDK[Vertebra SDK]
    end
    
    User --> Net
    Net --> Auth
    Auth --> Router
    Router -->|Static| Cache
    Router -->|Dynamic| RingReq
    
    Builder -.->|Supervises| Vite
    Vite -.->|Builds Assets| Cache
    
    RingReq --> SDK
    SDK --> Logic
    Logic --> SDK
    SDK --> RingResp
    RingResp --> Net
    
    Logic -.->|DB Query| RingReq
    RingReq -.-> DB
    DB -.->|Arrow Batch| DataPool
    DataPool -.->|Zero Copy Map| Logic
```

## 4. Migration from v2
*   **API Compatibility:** High. The `vertebra` Python package remains the interface.
*   **Configuration:** Moves from Python files to `vertebra.toml` (or `pyproject.toml` tool section) for Engine config.
*   **Dependency:** Removes `vertebra-server` (crate) complexity from user view. User just installs `pip install vertebra`.

## 5. Success Metrics
1.  **Latency:** p99 < 2ms for full DB roundtrip (vs 10ms+ standard).
2.  **Throughput:** Saturation of 10Gbps link with Python logic.
3.  **DX:** "It just works". No compilation, no "building" the server. `uv` integration out of the box.

## 6. Implementation Stages
1.  **Engine Prime:** Standalone Rust binary that can spawn a generic process.
2.  **The Bridge:** Shared Memory layout finalization (Arrow-native).
3.  **The SDK:** Python `vertebra` package rewrite to be a thin wrapper around SHM.
4.  **The Integrations:** SQLSpec / Litestar adapters.
