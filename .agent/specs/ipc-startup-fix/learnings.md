# Learnings: ipc-startup-fix

> Append-only log of discoveries during implementation.
> Format: [timestamp] - Phase/Task - Learning

## Session Log
- [2026-01-25] - Init - Verified track state. Codebase seems to be ahead of `tasks.md` status, or rather, the work was largely done but not formally closed in Flow.
- [2026-01-25] - Phase 1 - Verified `src/rs/vertebra-core/src/shm.rs` implements unlink-before-create with O_EXCL.
- [2026-01-25] - Phase 2 - Verified `src/rs/vertebra-py/src/supervisor.rs` cleans up stale shm and handles process management environment variables.
- [2026-01-25] - Phase 3 - Verified `src/ts/src/server/status.ts` implements real IPC connectivity checks via `vertebra.ping`.
- [2026-01-25] - Phase 4 - Verified Broker IPC status handler and Python `StatusClient`.
- [2026-01-25] - Phase 5 - Confirmed `httpx` moved to test dependencies in `pyproject.toml`.

## Patterns Discovered
- **Rust/Bun IPC Health Check**: Sending a special "ping" scope/message type (`vertebra.ping`) that bypasses the ASGI app allows for extremely fast health checks without app overhead.
- **O_EXCL for Atomic Shm**: Using `shm_unlink` followed by `shm_open(..., O_CREAT | O_EXCL)` is a robust way to handle stale shared memory files from crashed processes.

## Gotchas & Warnings
- **CLI Status Client**: The Python `StatusClient` expects the broker to wrap the result in an `ok: true, result: ...` envelope, which matches the implementation.

## Decisions Made
- Proceeding to Validation phase directly as implementation appears complete.
