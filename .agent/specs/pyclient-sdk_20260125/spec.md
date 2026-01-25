# Product Specification: Vertebra WASM SDK

## 1. Overview
The **Vertebra WASM SDK** (`vertebra-wasm`) is a lightweight Python library designed to run within **Pyodide** (WASM) environments. It enables browser-based Python applications to communicate efficiently with a Vertebra backend using **Apache Arrow IPC** streams over HTTP/WebSocket.

This SDK eliminates the need for manual `fetch` handling in PyScript/Pyodide apps and provides a type-safe, high-performance data bridge compatible with Vertebra's existing architecture.

### 1.1 Goals
- **Zero-Copy-ish Data Transfer:** Leverage `nanoarrow` to read Arrow IPC streams directly from the browser heap without heavy deserialization overhead.
- **Lightweight Footprint:** Target <2MB download size for the SDK core (excluding Pyodide itself).
- **Shared Type System:** Allow frontend Python code to import and use the exact same `msgspec` / `pydantic` models as the backend.
- **Developer Experience:** Simple `Client` class with `connect()`, `call()`, and `subscribe()` methods.

### 1.2 Non-Goals
- **Full Vertebra Node:** The browser client is a *leaf node*, not a full Broker/Worker participant. It does not manage shared memory pools.
- **Node.js Support:** Primary target is Browser/WASM. Node.js support is secondary (nice to have, but not a driver).

## 2. Architecture

### 2.1 Transport Layer
- **HTTP/1.1 & HTTP/2:** For unary Request/Response (RPC style).
- **WebSocket:** **Required** for streaming subscriptions, real-time updates, and bidirectional data. Must support auto-reconnection and heartbeats.
- **Server-Sent Events (SSE):** **Required** for lightweight, unidirectional server-to-client updates. Useful for firewalled environments or simple event streams.
- **Serialization:**
  - **Control Plane (Metadata):** JSON (via `msgspec` built-in to Pyodide).
  - **Data Plane (Payloads):** Apache Arrow IPC Streams (application/vnd.apache.arrow.stream).

### 2.2 Stack
- **Runtime:** Pyodide (v0.24+).
- **Core Dependencies:**
  - `nanoarrow` (for IPC reading).
  - `msgspec` (for JSON/Struct handling).
- **Optional Dependencies:**
  - `pydantic` (if user models use it, though `msgspec` is preferred).

### 2.3 Shared Model Bundling
To ensure type safety, the Vertebra Server will provide a new endpoint:
`GET /__vertebra/sdk.whl`
This dynamically generated (or cached) wheel will contain:
1.  The `vertebra_wasm` library.
2.  The application's shared types (exported from `src/py/vertebra/types` or similar).

### 2.4 Build Integration
- **Monorepo Strategy:** Use **`uv` Workspaces**.
- **Members:**
  - Root (or `src/py/vertebra`): Main server (Python + Rust extension).
  - `src/wasm`: WASM SDK (Pure Python).
- **Package Location:** `src/wasm`.
- **Build System:** `uv build` (Standard pure-Python wheel).
- **Versioning:** Synchronized with core `vertebra` package.
- **Distribution:** Hosted on PyPI as `vertebra-wasm` AND bundled by the Vertebra server for local dev/offline use.

## 3. User Experience

### 3.1 Installation (PyScript)
```html
<py-config>
    packages = [
        "nanoarrow",
        "msgspec",
        "/__vertebra/sdk.whl"  <!-- Auto-hosted by backend -->
    ]
</py-config>
```

### 3.2 Usage
```python
from vertebra_wasm import Client
from my_app.types import User

async def main():
    client = Client("http://localhost:8000")
    
    # RPC Call (JSON)
    user = await client.rpc("get_user", {"id": 1}, response_type=User)
    print(f"Hello {user.name}")
    
    # Data Call (Arrow)
    # Returns a nanoarrow.ArrayStream
    arrow_stream = await client.fetch_arrow("get_large_dataset")
    for batch in arrow_stream:
        print(f"Got batch with {batch.length} rows")
```

## 4. Technical Constraints
- **Socket Limits:** Browser connections are limited. The client must multiplex requests over a single WebSocket or use HTTP/2 where possible.
- **Thread Blocking:** Pyodide is single-threaded (mostly). Large Arrow parsing must yield to the event loop or use `await` points effectively.

## 5. Risks & Mitigations
- **Risk:** `nanoarrow` Python bindings might have missing features compared to `pyarrow`.
  - *Mitigation:* We only need `ipc.open_stream`. If missing, we will contribute to `nanoarrow` or wrap the C-interface directly using `ctypes`/`cffi`.
- **Risk:** Browser memory limits for WASM.
  - *Mitigation:* Stream processing (batch-by-batch) instead of loading full tables into RAM.

## 6. Success Metrics
- SDK size < 2MB.
- Successful roundtrip of 1M rows via Arrow IPC in < 1s (network permitting).
- `msgspec` models shared verbatim between backend and frontend.
