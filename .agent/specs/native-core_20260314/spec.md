# Flow Spec: Native Core (Ghost Framework)
## Goal
Implement the high-performance Vectorized FFI and Zero-Object Proxy architecture in Rust and Python.

## Context
This is the heart of the rewrite. Rust owns the HTTP server and routing. It batches requests and hands memory pointers to Python. Python accesses request fields lazily.

## Plan
### Phase 1: IPC Substrate (Rust)
- [ ] Task 1: Implement `src/rs/vertebra-core/src/shm.rs` and `ring.rs` for lock-free queues.
- [ ] Task 2: Implement `src/rs/vertebra-core/src/batch.rs` (Arena allocators for request batches).
- [ ] Task 3: Implement `src/rs/vertebra-core/src/proxy.rs` (C-ABI structs for Requests/Responses).

### Phase 2: HTTP Server & Router (Rust)
- [ ] Task 4: Implement `src/rs/vertebra-http/src/server.rs` (Hyper/Tokio accept loop).
- [ ] Task 5: Implement `src/rs/vertebra-http/src/router.rs` (matchit-based native routing resolving to Handler IDs).
- [ ] Task 6: Implement `src/rs/vertebra-http/src/collector.rs` (Batches incoming HTTP requests into Arenas).

### Phase 3: FFI Bridge (PyO3)
- [ ] Task 7: Implement `src/rs/vertebra-py/src/request_proxy.rs`. Create the `VertebraRequest` Python class in Rust that points to the Arena memory. Implement lazy getters (`.method`, `.path`, etc.).
- [ ] Task 8: Implement `src/rs/vertebra-py/src/batch_api.rs` (`get_batch` and `submit_batch`).
- [ ] Task 9: Integrate the FFI into `src/py/vertebra/core/runner.py` to complete the loop.
