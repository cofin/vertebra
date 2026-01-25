# Research: WASM for Python (PyScript) & Runtime Exploration

## Executive Summary
This research explores integrating WASM-based Python (PyScript/Pyodide) into Vertebra to enable rich client-side Python logic that communicates efficiently with the backend.

- **PyScript/Pyodide is viable** for running the application's "frontend" logic in Python, leveraging `pyarrow` for efficient binary communication with the Vertebra backend.
- **Communication Bridge:** The optimal path is **Apache Arrow over HTTP/WebSocket**. This aligns with Vertebra's existing Arrow data plane.
- **Workerd (Cloudflare Workers)** is a deployment target for "Edge Python" but introduces complexity for local development of stateful, shared-memory systems like Vertebra. It is best viewed as a *deployment environment* rather than a local runtime utility for this specific use case.
- **Pingora** (Rust Proxy) is powerful for load balancing but is orthogonal to the goal of "running Python in the browser." It does not solve the client-side execution or type-sharing problem.
- **Type Safety Strategy:** Unlike the TypeScript bridge (which generates `.d.ts`), the "WASM Bridge" should focus on **packaging and serving shared Python model definitions** (Pydantic/msgspec) directly to the browser to ensure the client and server share the exact same source of truth.

## Codebase Analysis

### Current Architecture
- **Backend:** Python (Litestar-style) + Rust IPC + Bun Broker.
- **Data Plane:** Heavy reliance on Shared Memory and Arrow for inter-process communication.
- **Type Bridge:** Currently extracts Python types to generate TypeScript definitions (`docs/architecture/type-bridge.md`).
- **Monorepo Management:** Currently uses a root `pyproject.toml`. Integrating `uv` workspaces is highly recommended for managing the main server and the new client SDK as separate but related packages. This allows for a shared lockfile but distinct package definitions.

### Integration Points
1.  **Shared Models:** The `src/py/vertebra/types` or application-level models need to be accessible to the PyScript environment.
2.  **Broker Protocol:** The Bun broker currently speaks HTTP/WS. The PyScript client would act as another "client" consuming these endpoints.
3.  **Serialization:** Vertebra already uses `msgspec` and `Arrow`. Pyodide supports `micropip` to install `pyarrow` and `msgspec` (if pure python or wheel available), enabling efficient binary ser/des directly in the browser.

## Library & Tool Documentation

### PyScript / Pyodide
- **Runtime:** Runs CPython in WASM.
- **Communication:** Uses browser `fetch` API. Can consume binary streams.
- **Packages:** Can install pure Python packages and those with WASM wheels (numpy, pandas, pyarrow) via `micropip`.
- **Constraint:** No direct socket access; must use WebSockets or HTTP.

### Nanoarrow
- **Role:** Lightweight Arrow implementation (C-based with Python bindings).
- **Size:** ~1MB (vs ~48MB for PyArrow).
- **Capabilities:** Fully supports reading Arrow IPC streams standalone (without PyArrow).
- **Fit:** **Superior** choice for the WASM client due to drastic size reduction while maintaining binary compatibility with the backend's Arrow streams.

### Python Modeling Library Support (WASM)
- **`msgspec`**: **Supported** (Built-in to Pyodide since v0.24.1). This is the preferred choice for Vertebra due to speed and zero-dependency install in Pyodide.
- **`attrs` / `cattrs`**: **Supported**. `attrs` is built-in; `cattrs` is pure Python (installable via `micropip`).
- **`dataclasses`**: **Supported** (Standard Library).
- **`pydantic` v2**: **Supported**. `pydantic-core` has WASM wheels. However, `msgspec` is recommended for the "Vertebra PyClient" to avoid the larger weight of the Pydantic Rust core if not strictly necessary.

### Pingora (Rust Proxy)
- **Role:** High-performance proxy framework (load balancing, routing).
- **Fit:** Could replace Nginx/HAProxy in front of the Bun Broker, but doesn't help run Python *in the browser*.

### Workerd (CF Workers Runtime)
- **Role:** Runtime for JavaScript and WASM (including Python).
- **Fit:** Excellent for deploying "Edge" logic.
- **Constraint:** Vertebra's core relies on Linux Shared Memory (`/dev/shm`), which is not available in standard Workerd environments. Workerd is better suited for stateless edge logic or lightweight coordination, not running the full Vertebra core.

## Prior Art & Patterns

### Binary Data Fetching (Arrow)
*Pattern:* Backend serves `application/vnd.apache.arrow.stream`. Frontend (Pyodide) fetches blob, feeds to `pyarrow.ipc.open_stream`.
*Benefit:* Zero-copy deserialization (conceptually) within the WASM heap, extremely fast for dataframes.

### Shared Logic
*Pattern:* "Isomorphic Python". Putting Pydantic models in a `common/` package.
*Implementation:* Build a standard Python wheel of the `common` logic, host it on the Vertebra server, and have PyScript install it on boot via `micropip`.

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **Payload Size** | High | High | Pyodide runtime + PyArrow is heavy (~10MB+). Use efficient caching/CDN and compression (Brotli). |
| **Startup Latency** | High | Medium | "Cold start" of WASM environment. Show loading UI. Less critical for internal/rich apps than public websites. |
| **Dependency Hell** | Medium | High | Some Python packages (extensions) won't work in WASM. Must verify `msgspec`/`pyarrow` WASM wheel availability. |
| **Browser Compatibility** | Low | Medium | WASM is widely supported, but memory limits vary by device (mobile). |

## Recommended Approach

**"Vertebra PyClient SDK" (Nanoarrow Edition)**

1.  **Bridge:** Create a lightweight Python client library (`vertebra-client-py`) designed to run in Pyodide.
2.  **Transport:** Implement `fetch` wrappers that handle Vertebra's Arrow IPC format.
3.  **Serialization:** Use **`nanoarrow`** instead of `pyarrow` for the client-side IPC layer to minimize WASM bundle size (~1MB payload).
4.  **Distribution:** The Vertebra server should expose a `/__vertebra/sdk.whl` endpoint that bundles shared models and the client SDK.
5.  **Developer Exp:** Users define models once in Python. The server auto-packages them. The frontend (PyScript) imports them directly.

**Do not use Workerd/Pingora for the core local loop** currently, as they add complexity without solving the specific "Python in Browser" need better than a standard browser+server setup.
