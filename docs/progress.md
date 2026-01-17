# Vertebra Progress & Next Steps

**Last updated:** 2026-01-17

## Current Focus

- PRD-2 Python bindings + type bridge (in progress).

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
- Timed wait OS shim (futex/ulock/WaitOnAddress) wired into SPSC timeout paths.
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
- `cargo clippy --all-targets --all-features -- -D warnings` passed on 2026-01-17.
- `cargo fmt --all` completed on 2026-01-17.

## Progress (PRD-2)

### Completed
- Implemented `@procedure` decorator with registry integration and Litestar opt metadata.
- Added thread-safe `ProcedureRegistry` + `ProcedureMetadata` dataclass.
- Built Type IR (primitive, array, object, union, literal, tuple, generic, raw/unknown).
- Implemented TypeExtractor for primitives, collections, unions/optionals, literals, and AST cache.
- Added msgspec.Struct AST extraction with nested type references.
- Added optional msgspec.inspect metadata merge (best-effort when msgspec is installed).
- Added pydantic BaseModel AST extraction + runtime metadata merge (Field description/alias).
- Added JSON Schema fallback for pydantic models with $ref resolution.
- Added discriminator metadata support for pydantic unions via JSON Schema.
- Added dataclass and TypedDict AST extraction with Required/NotRequired handling.
- Added Generic[T] extraction with TS generics + TypeVar bounds/constraints emitted as `extends`.
- Added forward-reference resolution for local and imported types with AST/import caches.
- Added cycle detection for recursive types and cache invalidation tests.
- Captured Annotated metadata in Type IR and rendered comments in TypeScript output.
- Added TypeScript generator (interfaces + namespace grouping + topo sort + validation + formatting helpers).
- Added serializer protocol with msgspec + pydantic backends, registry helpers, and auto-detection.

### Verified
- `uv run pytest src/py/tests` passed on 2026-01-17.

## Next Steps
### Continue PRD-2
- Phase 3: PyO3 IPC channel bindings + async integration.
- Phase 3: BufferView zero-copy handoff and msgspec buffer integration.
- HMR dev server + file watching for in-memory type updates.
