## [2026-01-27] - Phase 4 Completion: Python ASGI Adapter

- **Implemented:**
  - `AsgiWorker` updated to support managed mode (spawned by Rust supervisor with FDs).
  - `WorkerConfig` extended with `req_shm_fd`, `resp_shm_fd`, `shm_size`.
  - `AsyncRing.from_raw_fd_handle` exposed via PyO3 to map existing FDs.
  - `test_asgi_adapter.py` verification script (simulates managed IPC with named rings for testability).
- **Files changed:**
  - `src/py/vertebra/core/worker.py`
  - `src/rs/vertebra-py/src/async_ring.rs`
  - `src/rs/vertebra-core/src/async_ring.rs`
  - `tools/test_asgi_adapter.py`
- **Learnings:**
  - `AsyncRing` in Python logic acts as a bidirectional channel (wrapping TWO rings for client/server), but the Rust `AsyncRing` struct wraps ONE `SpscRing`.
  - The Python `AsgiWorker` uses `AsyncRing` objects that correspond to `AsyncRing` in Rust.
  - `ClientState` holds `request` and `response` rings.
  - For managed mode, we map FDs to `ClientState` rings directly, bypassing the "Control Ring" registration flow.
- **Gotchas:**
  - `shm_unlink` must NOT be called by the worker in managed mode (server owns lifecycle).
  - `msgspec` encoding of headers must align with Rust `RequestHeader` layout (16 bytes).
