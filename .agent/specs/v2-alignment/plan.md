# Plan: V2 Alignment

## Phase 1: Archival of Obsolete Specs
Some specs are purely about Bun internals (like `ipc-startup-fix` for Bun's process manager) and are now completely irrelevant.

- [x] **Task: Archive `ipc-startup-fix`**
    - Reason: Bun ProcessManager is deleted. Rust Supervisor handles this.
- [x] **Task: Archive `pluggable-runtime-layer`**
    - Reason: We have chosen Rust as the *only* runtime. Pluggability is no longer a goal.
- [x] **Task: Archive `ipc-performance-optimization`**
    - Reason: Focused on Bun event loops. Rust `tokio` handles this natively.
- [x] **Task: Archive `asgi-server-benchmarks`**
    - Reason: These were specific to the Bun implementation. We need new Rust benchmarks (tracked in `advanced-networking` or a new flow).

## Phase 2: Rewrite for Rust
These specs contain valuable features that need to be ported to the Rust architecture.

- [x] **Task: Rewrite `native-app-framework` -> `rust-native-router`**
    - Goal: Port the "Native Route" concept to use `matchit` in Rust directly.
    - Action: Rename folder, update spec to target Rust `axum`/`hyper`.
- [x] **Task: Rewrite `cli-broker-status-ipc` -> `rust-broker-introspection`**
    - Goal: CLI needs to query Rust Broker for stats.
    - Action: Define a new IPC message `Op::Status` handled by the Rust Broker.
- [x] **Task: Rewrite `native-multipart-fallback` -> `rust-multipart-streaming`**
    - Goal: Rust server must handle `multipart/form-data` efficiently.
    - Action: Investigate `mule` or `axum_extra::extract::Multipart` integration.

## Phase 3: Final Cleanup
- [x] **Task: Verify `src/ts` Deletion**
    - Ensure no stray TS files remain.
    - [verified] Directory is gone.
- [x] **Task: Update `flows.md`**
    - Remove the "STALE" section.
