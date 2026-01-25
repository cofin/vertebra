# Implementation Plan: Vertebra PyClient SDK

## Phase 1: Core SDK & Nanoarrow Integration
**Goal:** Can fetch an Arrow stream from the backend and parse it in Pyodide using `nanoarrow`.

- [x] **Task 1: Configure UV Workspace**
  - Modify root `pyproject.toml` to define `[tool.uv.workspace]` with members `["src/wasm"]`.
  - Create `src/wasm/` directory structure.
  - Create `src/wasm/pyproject.toml` for `vertebra-wasm`.
  - Define dependencies: `msgspec`, `nanoarrow`.
  - Update `Makefile` to include a `wasm-build` target.
- [ ] **Task 2: Implement `ArrowClient` Transport**
  - Implement `fetch_buffer(url)` using Pyodide's `pyodide.http.pyfetch` (or `pyscript.fetch`).
  - Implement `read_stream(buffer)` using `nanoarrow.ipc`.
- [ ] **Task 3: Integration Test (Mock)**
  - Create a simple HTML harness that loads Pyodide and the local SDK wheel.
  - Serve a static `.arrow` file.
  - Verify `nanoarrow` can read the schema and batch length in the browser console.

## Phase 2: RPC, Real-time & Type Bridging
**Goal:** End-to-end typed RPC calls and robust real-time subscriptions.

- [ ] **Task 4: Implement `RpcClient`**
  - `call(method, params, response_model)` method.
  - Use `msgspec.json.encode` / `decode` for payloads.
- [ ] **Task 5: Implement Real-time Transports (WS & SSE)**
  - **WebSocket:** Implement `WebSocketSubscription` with auto-reconnection and heartbeats.
  - **SSE:** Implement `SSESubscription` wrapping browser `EventSource` (via `js` module) for robust unidirectional streaming.
  - Ensure unified `subscribe()` API can abstract over either transport.
- [ ] **Task 6: Server-Side SDK Bundler**
  - Add a route `/__vertebra/sdk.whl` to the Vertebra Python server.
  - Implement logic to build (or serve pre-built) SDK wheel.

## Phase 3: Developer Experience & Polish
**Goal:** Clean APIs and documentation.

- [ ] **Task 7: PyScript Example App**
  - Create `examples/pyscript-dashboard/`.
  - Backend: Serves random data via Arrow.
  - Frontend: Plots data using a WASM-compatible plotting lib (e.g., specific matplotlib backend or just passing data to JS charting).
- [ ] **Task 8: Documentation**
  - Write `docs/guides/wasm-client.md`.

## Phase 4: Optimization (Optional)
*No optional tasks currently defined.*
