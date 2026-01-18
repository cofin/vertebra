# Vertebra Progress & Next Steps

**Last updated:** 2026-01-18

## Current Focus

- PRD-3 Bun server foundations + IPC protocol (in progress).
- PRD-4 Bun-backed drivers Phase 1 foundation (in progress).

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
- Added IPC Channel wrapper with request/response helpers and async wrappers.
- Added PyO3 Channel bindings + Buffer buffer-protocol exports for zero-copy views.
- Released GIL around blocking PyO3 channel operations to avoid cross-thread deadlocks.
- Added BufferView view tracking + automatic release before send to prevent active-view errors.
- Added BufferView + ZeroCopyChannel helpers for msgspec decoding.
- Made TypeScript validation optional when no compiler is available (no noisy skips).
- Added Channel close semantics + contextual errors (procedure_id + stage).
- Added async IPC + end-to-end procedure flow tests.
- Added Python serialization + IPC latency benchmark scripts.
- Documented zero-copy vs fallback behavior and benchmark entrypoints.
- Added Type Bridge endpoint + ASGI app + debounced file watcher.
- Added Vite type polling plugin (virtual module, HMR reload).
- Added Bun server + client scaffolding (Bun-only).
- Added napi-rs unit tests and Rust validation scripts (tsan/valgrind/coverage).
- Added MessagePack codec + Bun IPC client for driver transport.
- Added driver protocol specification and API docs (protocol + drivers).

### Verified

- `uv run pytest src/py/tests` passed on 2026-01-18.
- `uv run pytest src/py/tests` passed on 2026-01-18 (SQLite driver test added).
- `bun test` passed on 2026-01-18.
- `cargo test -p vertebra-broker` passed on 2026-01-18.

## Progress (PRD-3)

### Completed

- Added initial Bun server scaffolding (`src/ts/src/server`).
- Added Protocol Buffer schema placeholders for ASGI/config/metrics (`src/ts/src/proto`).
- Added Bun server config loader, router, and worker-pool stub.
- Added Bun IPC client (MessagePack) and ASGI bridge wrapper.
- Added Python ASGI worker harness with IPC framing and tests.

## Progress (PRD-4)

### Completed

- Defined TypeScript driver protocol messages and MessagePack serialization helpers.
- Added Python MessagePack codec for driver transport.
- Added DB-API base driver protocols and exception hierarchy.
- Added baseline type conversion helpers (Postgres/MySQL/SQLite + JSON/UUID).
- Added Bun runtime guard + IPC client skeleton for driver transport.
- Added unit tests for driver exceptions, MessagePack codec, IPC guard, and type conversions.
- Added IPC client retry + pooling and request/response tests.
- Added Bun SQLite driver server (TCP) and Python SQLite driver/client scaffolding.
- Added async SQLite driver IPC test harness (mock server).
- Added Bun Postgres driver server + Python Postgres driver (sync/async) with IPC tests.

## Progress (PRD-5)

### Completed

- Initialized `vertebra-broker` crate and workspace wiring.
- Added broker config parsing (TOML + env substitution) with unit tests.
- Added IPC server and pool manager stubs for broker scaffolding.
- Added broker protocol framing, bincode serialization, and TCP/UDS/named-pipe IPC server.
- Added broker IPC echo integration test.
- Added broker JSON logging toggle via VERTEBRA_LOG_JSON.
- Added Python broker client (msgpack codec) with unit tests.

## Progress (PRD-7)

### Completed

- Added MCP TypeScript server skeleton with JSON-RPC handler.
- Added MCP IPC bridge stub for future transport wiring.

## Next Steps

### PRD-3

- Implement Bun server config loader, router, and worker pool.
- Build IPC bridge (TS side) with connection pool + MessagePack/Protobuf transport toggle.
- Add Python-side IPC worker harness for ASGI apps.

### PRD-4

- Implement Bun-only IPC server handlers for database requests.
- Add base Postgres driver (sync + async) and pool with tests.
- Add protocol compliance tests + integration scaffolding (pytest-databases).

### PRD-5/6/7

- Scaffold vertebra-broker crate and IPC client for resource pools.
- Add integration test harness + docs/benchmarks for cross-language flows.
- Start MCP server skeleton in `src/ts/src/mcp` with schema registry + transport.
