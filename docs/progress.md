# Vertebra Progress & Next Steps

**Last updated:** 2026-01-17

## Current Focus

- PRD-1 Rust IPC core layer (completed).

## Progress (PRD-0)

### Completed
- Standardized TypeScript/Bun layout to `src/ts` across PRDs and repo layout.
- Enforced Python 3.12+ baseline in tooling and documentation.
- Switched Python build backend to `maturin` (PyO3 bindings in `src/rs/vertebra-py`).
- Created Rust workspace (`vertebra-core`, `vertebra-py`, `vertebra-js`).
- Added Bun/TypeScript workspace scaffold with tests and minimal stubs.
- Updated Makefile for cross-language install/upgrade/build/test/lint with fail-fast behavior.
- Added architecture + conventions docs (`docs/arch.md`, `CLAUDE.md`, `README.md`).
- Completed `make test-all` and `uv lock --upgrade`.

### Verified
- `make install` completed successfully on 2026-01-17.
- `make test-all` completed successfully on 2026-01-17.
- `uv lock --upgrade` completed successfully on 2026-01-17.

## Progress (PRD-1)

### Completed
- Shared memory backends for Linux/macOS/Windows with named Unix support and crash-safe headers.
- SPSC shared-memory ring + MPMC ring (epoch/sequence-based) with backpressure.
- Buffer pool + buffer channel (zero-copy metadata handoff) and Arrow IPC helpers.
- Cross-platform wait/wake shim with file-lock fallback and shared mutex/rwlock/condvar.
- Rust benchmarks (SPSC ping-pong + throughput, MPMC baseline).
- PyO3 + napi bindings (ShmRegion, BufferPool, SPSC/MPMC, BufferChannel).
- Multi-process tests for shared memory and shared mutex.
- CI workflow for Rust build/test/lint + nightly Miri.
- Python IPC examples and README docs.

### Verified
- `cargo test --workspace` (CARGO_HOME local) passed on 2026-01-17.
- `cargo doc --workspace --no-deps` passed on 2026-01-17.
- `uv run pytest src/py/tests` passed on 2026-01-17.
- `bun test` passed on 2026-01-17.

## Next Steps
### Start PRD-2
- Build Python-first API surface and type bridge (AST + HMR).
- Align Litestar-style API patterns with type generation.
