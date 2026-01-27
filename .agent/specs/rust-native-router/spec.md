# Rust Native Router

## Overview

A high-performance native application router for Vertebra that bypasses ASGI overhead. It leverages Rust's `matchit` router (via `vertebra-server`) to dispatch requests directly to Python handlers for maximum throughput.

## Context (Migration)
Formerly `native-app-framework`. The original spec relied on Bun to route requests. The new architecture moves routing entirely into the Rust `vertebra-server` crate.

## Functional Requirements

### Must Have
- [ ] **Unified Routing Table:** Python defines routes; Rust (`vertebra-server`) executes matching.
- [ ] **Zero-Copy Dispatch:** Rust parses the path, matches the route, and passes the `handler_id` to Python via shared memory (avoiding string passing for route arguments where possible).
- [ ] **Dual Mode:**
    - [ ] `Native`: Rust -> Match -> Python Handler (Fast path)
    - [ ] `ASGI`: Rust -> Match (or fallback) -> ASGI Adapter -> Starlette/Litestar (Compatibility path)
- [ ] **Type-Safe Params:** Path parameters extracted by Rust and passed as typed arguments to Python.

## Technical Approach
- Use `matchit` crate in `vertebra-server`.
- Python `vertebra.Router` builds the tree and serializes it to Rust at startup.
- IPC opcodes for `NativeDispatch` vs `AsgiDispatch`.