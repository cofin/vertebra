## [2026-01-27] - Phase 4 Completion: IPC Worker Bridge

- **Implemented:**
  - `WorkerManager` in `vertebra-server` (Rust) which manages IPC rings and worker processes.
  - `start_http_server` updated to accept configuration and start workers.
  - Verification script `tools/test_ipc_worker.py` confirms successful worker spawning and FD passing.
- **Files changed:**
  - `src/rs/vertebra-server/src/worker.rs`: New module.
  - `src/rs/vertebra-server/src/lib.rs`: Integration.
  - `src/rs/vertebra-py/src/lib.rs`: Updated binding.
  - `src/py/vertebra/server/worker.py`: New dummy worker.
- **Learnings:**
  - `vertebra_core::notify` does not export `InheritableNotifier`; `Notifier` is already inheritable on Linux/Unix (no O_CLOEXEC on pipe/eventfd).
  - `maturin develop` must be run from project root to correctly resolve python source layout for editable installs.
  - `ShmRegion::handle()` returns `Option<RawFd>` on Unix, used to pass FDs to workers.
