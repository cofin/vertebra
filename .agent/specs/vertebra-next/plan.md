# Plan: Vertebra Next Implementation

## Phase 1: The Engine Foundation
**Goal:** Create the standalone `vertebra` binary (Rust) that replaces the current `vertebra-server` + Supervisor.
- [ ] **Task 1.1:** Scaffold `vertebra-engine` crate.
    -   Hyper 1.0 / Tokio / Tower setup.
    -   Config parsing (`pyproject.toml` support).
    -   Process lifecycle (spawn `uv run python ...`).
- [ ] **Task 1.2:** Implement "Optimistic Router" in Rust.
    -   Port `matchit` logic.
    -   Define "Route Manifest" protocol (Python sends route table to Rust at startup).
- [ ] **Task 1.3:** Static Asset "Reflink" Cache.
    -   Implement `mmap` / `sendfile` serving.
    -   Global CAS cache logic (inspired by `uv`).

## Phase 2: The Data Plane (Zero-Copy IPC)
**Goal:** Implement the "Phantom IPC" layer where Python maps Rust memory.
- [ ] **Task 2.1:** Redefine Shared Memory Layout.
    -   Header: `Magic`, `Version`, `State`.
    -   Data: `Arrow Buffer Region` (Aligned).
    -   Rings: Control + Data rings.
- [ ] **Task 2.2:** Rust `Arrow` Integration.
    -   Implement `polars-arrow` or `arrow-rs` conversion for SQL results.
- [ ] **Task 2.3:** Python `Arrow` Mapping.
    -   Use `pyarrow.RecordBatch.from_address` (or similar C-API hook) to map SHM without copy.

## Phase 3: The Python SDK (Thin Wrapper)
**Goal:** Rewrite `src/py/vertebra` to be a lightweight logic adapter.
- [ ] **Task 3.1:** `VertebraRequest` object backed by `memoryview`.
    -   Lazy header parsing.
    -   Zero-copy body access.
- [ ] **Task 3.2:** `VertebraResponse` builder.
    -   Direct write to SHM slot.
- [ ] **Task 3.3:** Remove legacy `asyncio` loop handling?
    -   *Decision point:* Do we need `uvloop` if we are just processing logic? Maybe yes for `async def` compatibility.

## Phase 4: Integrations & Polish
- [ ] **Task 4.1:** SQLSpec Adapter (Arrow Mode).
- [ ] **Task 4.2:** Litestar "Native" Adapter vNext.
- [ ] **Task 4.3:** `uv` Integration (auto-install Python/deps).

## Phase 5: Frontend Engine & Bundling
**Goal:** Implement the "One File" experience.
- [ ] **Task 5.1:** Frontend Executor & Proxy.
    -   `FrontendExecutor` Trait definition.
    -   Port Discovery: Find free TCP port.
    -   Env Injection: Pass `VERTEBRA_FRONTEND_PORT` to child process.
    -   Rust `hyper` reverse proxy to the assigned port.
    -   WebSocket forwarding for HMR.
- [ ] **Task 5.2:** Rolldown Integration.
    -   Integrate `rolldown` crate for in-process JS/TS bundling.
    -   Implement "Zero-Node" build step (reading from `node_modules`).
    -   Logic to compress and embed assets into the Rust binary (or adjacent `.pak` file).
- [ ] **Task 5.3:** Prod Asset Server.
    -   Serve embedded/reflinking assets with aggressive caching headers.

## Phase 6: Node.js Integration (Middleware Mode)
**Goal:** Run Vertebra inside Next.js / Express.
- [ ] **Task 6.1:** `vertebra-node` NAPI Crate.
    -   Expose `Engine` struct to Node via NAPI-RS.
    -   Implement `engine.dispatch(req_buffer)` -> Ring Buffer -> Python.
- [ ] **Task 6.2:** Middleware Adapters.
    -   `createMiddleware()` helper for Express/Connect.
    -   Next.js API Route handler wrapper.

## Risks & Mitigations
*   **Risk:** Python `memoryview` safety.
    *   *Mitigation:* Strict Reference Counting in Rust. Python holds a "Lease" token.
*   **Risk:** Arrow complexity for simple users.
    *   *Mitigation:* Transparent fallback to `dict` (MsgPack) for small queries. Arrow is opt-in or transparent optimization.
*   **Risk:** Node.js dependency for frontend.
    *   *Mitigation:* Use `uv` to manage Node via plugins (if possible) or strictly check environment. Long term: Move to Rust-based bundlers (Rspack) managed by the Engine.
