# Vertebra Progress & Next Steps

**Last updated:** 2026-01-19

## Current Focus

- Phase 3 driver ecosystem (SQLSpec proxy + provider host tests, fsspec local tests).
- MCP integration planning (stdio + SSE + WebSocket).
- Phase 5 distribution planning (supervisor + broker embedding).

## Progress (PRD-0)

### Completed

- Marked legacy driver IPC as deprecated; shared-memory broker supersedes it.
- Standardized TypeScript/Bun layout to `src/ts` across PRDs and repo layout.
- Enforced Python 3.12+ baseline in tooling and documentation.
- Switched Python build backend to `maturin` (PyO3 bindings in `src/rs/vertebra-py`).
- Created Rust workspace (`vertebra-core`, `vertebra-py`, `vertebra-js`).
- Added Bun/TypeScript workspace scaffold with tests and minimal stubs.
- Updated Makefile for cross-language install/upgrade/build/test/lint with fail-fast behavior.
- Added `make lint` (ruff fix + ruff format + Biome format + rustfmt) and `make lint-all` for full checks.
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
- `uv run pytest src/py/tests` passed on 2026-01-18 (broker pool + transaction tests added).

## Progress (PRD-3)

### Completed

- Added initial Bun server scaffolding (`src/ts/src/server`).
- Added Protocol Buffer schema placeholders for ASGI/config/metrics (`src/ts/src/proto`).
- Implemented Bun server config loader with env + config file support (`defineConfig`, `resolveServerConfig`).
- Implemented round-robin worker pool with per-worker IPC pools + shutdown hooks.
- Added Bun worker manager to spawn Python ASGI workers, restart on exit, and run health checks.
- Updated ASGI bridge to use pooled IPC requester + error handling (pathsend/zerocopysend supported).
- Added Bun IPC client (MessagePack) and ASGI bridge wrapper.
- Added Python ASGI worker harness with IPC framing, lifespan support, and tests.
- Added Bun ⇄ Python IPC integration tests for multi-worker routing + pathsend.
- Added streaming IPC frames (response.start/body) with backpressure in the ASGI bridge.
- Wired Bun server router to ASGI bridge helper (`createAsgiServer`).
- Implemented request-body streaming + `http.disconnect` propagation in the ASGI bridge and worker.
- Added WebSocket upgrade path with IPC `websocket.*` frames and ASGI websocket scope handling.
- Added `/__metrics__` endpoint with prom-client metrics and JSON request logging + trace id injection.
- Added `UnifiedMetricsRegistry` to aggregate broker + worker metrics.
- Added graceful shutdown with drain timeout for Bun server.
- Added Bun server entrypoint (`src/ts/src/server/serve.ts`).
- Added Python CLI `vertebra serve`, Litestar plugin, and Go Fast banner.
- Added supervisor to launch Bun engine + broker as a single process group.
- Added SQLSpec-style optional dependency handling and MissingDependencyError pattern.

### Verified

- `make install` completed successfully on 2026-01-19.
- `bun test` passed on 2026-01-19.

## Progress (PRD-4)

### Completed

- Defined TypeScript driver protocol messages and MessagePack serialization helpers.
- Added Python MessagePack codec for driver transport.
- Added DB-API base driver protocols and exception hierarchy (Sync/Async split).
- Added baseline type conversion helpers (Postgres/MySQL/SQLite + JSON/UUID).
- Added Bun runtime guard + IPC client skeleton for driver transport.
- Added unit tests for driver exceptions, MessagePack codec, IPC guard, and type conversions.
- Added IPC client retry + pooling and request/response tests.
- Added early driver server scaffolding (SQLite/Postgres); superseded by shared-memory broker.
- Added async SQLite driver IPC test harness (legacy).
- Added Postgres pool support + transaction context (sync/async) with tests.
- Added separate Sync/Async driver classes (connections/cursors/pools) for Postgres/MySQL/SQLite/Redis.
- Added Redis driver server (Bun) and Python Redis driver (sync/async) scaffolding.
- Added filesystem IPC server (local/S3/GCS); later routed storage through broker shared memory.
- Added object-store protocol (sync/async) with Local/S3/GCS stores and URL-based registry.
- Added byte-range reads and list-with-delimiter semantics for storage backends.
- Added streaming helpers for storage backends (iter_chunks/put_stream + upload/download to path).
- Added signed URL support for storage backends with graceful fallback when unsupported.
- Updated fsspec shim to wrap object-store protocol.
- Added Biome config for TypeScript formatting (`src/ts/biome.json`) and Biome dev dependency.
- Added Docker/Podman container runtime helper for future integration tests (`tools/lib/container.py`).

## Progress (PRD-5)

### Completed

- Initialized `vertebra-broker` crate and workspace wiring.
- Added broker config parsing (TOML + env substitution) with unit tests.
- Added IPC server and pool manager stubs for broker scaffolding.
- Added broker protocol framing + bincode serialization (legacy; superseded by shared-memory broker).
- Added broker IPC echo integration test.
- Added broker JSON logging toggle via VERTEBRA_LOG_JSON.
- Added Python broker client (msgpack codec) with unit tests.
- Added broker pool abstraction with async context manager and pooling tests.
- Added shared-memory C ABI for ring buffers (create/open/send/recv + stats + errors).
- Added Bun broker with fairness scheduler and shared-memory request loop.
- Added Bun SQL pools (Postgres/MySQL/SQLite), Redis pool, and storage pools (local/S3/GCS).
- Added provider registry + routing with Arrow IPC payloads for large results.

## Progress (PRD-7)

### Completed

- Added MCP TypeScript server skeleton with JSON-RPC handler.
- Added MCP IPC bridge stub for future transport wiring.

## Progress (Phase 3: ORM Extensions)

### Completed

- Added broker-backed SQLSpec adapters (Postgres/MySQL/SQLite).
- Added broker proxy configs to wrap arbitrary SQLSpec adapters.
- Added SQLSpec provider host with Arrow IPC encoding and correlation ID propagation.
- Routed storage clients (filesystem/object store/fsspec) through the broker shared-memory API.

## Progress (PRD-8: Production Readiness Consolidation)

### Completed

- Refactored TypeScript pool classes in `serve.ts` to extend `BasePoolAdapter`.
- Unified `PoolStats` type to single source in `base-pool.ts` (removed duplicates from `client.ts` and `serve.ts`).
- Renamed Python `resources/ipc.py` to `broker.py` and updated all imports (8 files).
- Added `ChannelHandle` to `vertebra-js` with bidirectional SPSC rings for request/response patterns.
- Added `createChannelPair()` factory for anonymous paired channels.
- Added `createNamed()` and `openNamed()` factories for Unix named shared memory.
- Added `ChannelRole` enum (Client/Server) replacing error-prone string matching.
- Added `unlink()` lifecycle method for named SHM cleanup.
- Added public `shm_unlink_named()` function to `vertebra-core/src/shm.rs`.

### Verified

- `cargo check --package vertebra-js` passed on 2026-01-19.

### Supervisor (T1.6.*)

- Implemented process supervisor with full lifecycle management in `src/py/vertebra/supervisor.py`.
- Added `ProcessState` and `SupervisorState` enums for state machine tracking.
- Added `RestartPolicy` with exponential backoff (configurable base/max delay, reset period).
- Added `HealthCheckConfig` for future health check integration.
- Added process group management (`start_new_session=True`) for clean Unix shutdown.
- Added signal propagation (SIGTERM, SIGINT, SIGHUP) to all managed processes.
- Implemented structured shutdown sequencing: drain → wait → force kill.
- Wired `vertebra serve` to use supervisor with `--drain-timeout` and `--shutdown-timeout` CLI flags.
- Added 10 unit tests covering restart policy, state transitions, signal propagation, and process lifecycle.

### Verified (Supervisor)

- `uv run pytest src/py/tests/test_supervisor.py` passed on 2026-01-19 (10 tests).

## Next Steps
### Phase 3 (ORM + Storage)

- Add SQLSpec integration tests (postgres/mysql/sqlite + proxy adapters).
- Add fsspec compliance tests (local backend default; S3/GCS optional/skip).
- Add container-based integration tests using `tools/lib/container.py`.
- Defer SQLAlchemy dialect + Advanced Alchemy until SQLSpec is stable.

### Phase 4 (MCP)

- Implement MCP stdio + SSE + WebSocket transports.

### Phase 4 (MCP)

- Implement stdio + SSE + WebSocket transports.
- Wire Python registry + tool execution over IPC.
