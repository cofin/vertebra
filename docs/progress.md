# Vertebra Progress & Next Steps

**Last updated:** 2026-01-17

## Current Focus

- PRD-0 foundation and repository cleanup.

## Progress (PRD-0)

### Completed
- Standardized TypeScript/Bun layout to `src/ts` across PRDs and repo layout.
- Enforced Python 3.12+ baseline in tooling and documentation.
- Switched Python build backend to `maturin` (PyO3 bindings in `src/rs/vertebra-py`).
- Created Rust workspace (`vertebra-core`, `vertebra-py`, `vertebra-js`).
- Added Bun/TypeScript workspace scaffold with tests and minimal stubs.
- Updated Makefile for cross-language install/upgrade/build/test/lint with fail-fast behavior.
- Added architecture + conventions docs (`docs/arch.md`, `CLAUDE.md`, `README.md`).

### Verified
- `make install` completed successfully on 2026-01-17.
- `make test-all` pending after recent test harness fixes.

## Next Steps

### Finish PRD-0
- Re-run `make test-all` to confirm a clean cross-language test run.
- Regenerate `uv.lock` (`uv lock --upgrade`) to capture the 3.12 baseline and current deps.
- Log any remaining DMA references as explicit “known legacy issues” if discovered.

### Prepare PRD-1
- Define IPC abstraction boundaries for Unix (UDS + shared memory) and Windows (named pipes + shared memory).
- Validate zero-copy pathways and the hybrid serialization model (Arrow for large payloads, msgspec/MessagePack for control/data plane).
