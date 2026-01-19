# PRD-8 Task Breakdown

**Total Tasks:** 95+
**Estimated Effort:** 11 weeks

## Phase 1: Server Completion (PRD-3 Gaps)

### 1.1 Request Body Streaming
- [x] **T1.1.1** Add `http.request` frame type to `AsgiIpcFrame` union in `src/ts/src/ipc/asgi.ts`
- [x] **T1.1.2** Modify `AsgiBridge.handle()` to use `ReadableStream` reader for chunked body
- [x] **T1.1.3** Implement request frame emission during body streaming
- [x] **T1.1.4** Update Python worker to handle interleaved request/response frames
- [x] **T1.1.5** Add backpressure handling via IPC channel flow control
- [ ] **T1.1.6** Write unit tests for chunked request bodies
- [ ] **T1.1.7** Write integration test for large file upload streaming

### 1.2 HTTP Disconnect Propagation
- [x] **T1.2.1** Add `http.disconnect` frame type to `AsgiIpcFrame`
- [x] **T1.2.2** Track request abort via `AbortController` in ASGI bridge
- [x] **T1.2.3** Emit disconnect frame on client abort
- [x] **T1.2.4** Python worker cancels coroutine on disconnect
- [ ] **T1.2.5** Write test for client abort during response streaming

### 1.3 WebSocket/SSE Bridging
- [x] **T1.3.1** Add `websocket.*` frame types to `AsgiIpcFrame`
- [x] **T1.3.2** Detect WebSocket upgrade in router
- [x] **T1.3.3** Implement WebSocket accept and bidirectional stream
- [x] **T1.3.4** Forward `websocket.receive` to Python worker
- [x] **T1.3.5** Forward `websocket.send` from Python to client
- [x] **T1.3.6** Implement close handshake
- [ ] **T1.3.7** Add SSE support (unidirectional streaming)
- [ ] **T1.3.8** Write WebSocket echo test
- [ ] **T1.3.9** Write SSE event stream test

### 1.4 Metrics Infrastructure
- [x] **T1.4.1** Create `src/ts/src/server/metrics.ts` with metric types
- [x] **T1.4.2** Add lightweight metrics library (or custom implementation)
- [x] **T1.4.3** Instrument request handler with timing
- [ ] **T1.4.4** Add worker pool utilization metric
- [ ] **T1.4.5** Add IPC latency histogram
- [x] **T1.4.6** Expose `/__metrics__` endpoint
- [ ] **T1.4.7** Add health check timing metric
- [ ] **T1.4.8** Write test for metrics endpoint format

### 1.5 Graceful Shutdown
- [x] **T1.5.1** Add `drainTimeoutMs` config option
- [x] **T1.5.2** On SIGTERM, stop accepting new connections
- [x] **T1.5.3** Wait for in-flight request drain
- [x] **T1.5.4** Send graceful SIGTERM to workers
- [x] **T1.5.5** Force SIGKILL after timeout
- [ ] **T1.5.6** Write shutdown sequence test

### 1.6 Supervisor / Process Manager
- [x] **T1.6.1** Create supervisor module (Python/Rust) for Bun + broker lifecycle
- [x] **T1.6.2** Wire `vertebra serve` to supervisor (child process management)
- [x] **T1.6.3** Add signal propagation + structured shutdown sequencing
- [x] **T1.6.4** Add health checks + restart policy hooks
- [x] **T1.6.5** Write supervisor integration test

## Phase 2: Resource Management (PRD-5 Gaps)

### 2.1 Pool Fairness Scheduler
- [ ] **T2.1.1** Define `FairnessPolicy` enum in Rust (FIFO, Weighted, Priority)
- [ ] **T2.1.2** Implement waiter queue with FIFO ordering
- [ ] **T2.1.3** Implement weighted fair queue variant
- [ ] **T2.1.4** Implement priority queue variant
- [ ] **T2.1.5** Add lease tracking with `LeaseToken`
- [ ] **T2.1.6** Implement timeout-based lease eviction
- [ ] **T2.1.7** Wire fairness into broker protocol
- [ ] **T2.1.8** Update Python client to use fairness hints
- [ ] **T2.1.9** Write fairness distribution tests

### 2.2 Lifecycle Hooks
- [ ] **T2.2.1** Create `src/py/vertebra/resources/hooks.py` with protocol
- [ ] **T2.2.2** Add `on_acquire` hook invocation in `BrokerPool.acquire()`
- [ ] **T2.2.3** Add `on_release` hook invocation in `BrokerPool.release()`
- [ ] **T2.2.4** Add `on_validate` hook for health checks
- [ ] **T2.2.5** Add `on_evict` hook for timeout cleanup
- [ ] **T2.2.6** Write hook invocation tests

### 2.3 Pool Observability
- [ ] **T2.3.1** Create `src/rs/vertebra-broker/src/metrics.rs`
- [ ] **T2.3.2** Add pool size metric (current/max)
- [ ] **T2.3.3** Add wait time histogram
- [ ] **T2.3.4** Add lease duration histogram
- [ ] **T2.3.5** Add eviction counter by reason
- [ ] **T2.3.6** Add error counter by type
- [ ] **T2.3.7** Expose metrics via broker protocol `metrics` command
- [ ] **T2.3.8** Create `src/py/vertebra/resources/metrics.py` for aggregation
- [ ] **T2.3.9** Write metrics collection test

## Phase 3: ORM Extensions (SQLSpec & Advanced Alchemy)

Per `specs/active/orm-extensions/` - unified config with `provide_session` returning IPC-wrapped sessions.
All execution tunnels through same high-performance IPC bridge to DB Node.

### 3.1 Unified Configuration Layer
- [ ] **T3.1.1** Create `src/py/vertebra/extensions/` package structure
- [ ] **T3.1.2** Create `_typing.py` shims for optional deps (SQLSpec, SQLAlchemy, AA)
- [ ] **T3.1.3** Implement `VertebraDBConfig[T]` generic over SQLSpec/SQLAlchemy configs
- [ ] **T3.1.4** Implement `provide_session()` returning `IPCSession`
- [ ] **T3.1.5** Implement `provide_async_session()` returning `AsyncIPCSession`
- [ ] **T3.1.6** Implement `provide_connection()` context manager
- [ ] **T3.1.7** Add dialect inference from underlying config
- [ ] **T3.1.8** Write config unit tests

### 3.2 SQLSpec IPC Adapter (16+ Dialects)
- [ ] **T3.2.1** Add `sqlspec` as optional dependency in `pyproject.toml`
- [ ] **T3.2.2** Create `src/py/vertebra/extensions/sqlspec/adapter.py`
- [ ] **T3.2.3** Implement `AsyncIPCDriver(AsyncDriverAdapterBase)`
- [ ] **T3.2.4** Implement `dispatch_execute` - serialize (sql, params, dialect) to IPC
- [ ] **T3.2.5** Implement `dispatch_execute_many` for batch operations
- [ ] **T3.2.6** Implement `dispatch_special_handling` for DB-specific ops (COPY, bulk)
- [ ] **T3.2.7** Implement `select_to_arrow` - zero-copy Arrow from shared memory
- [ ] **T3.2.8** Implement `to_pandas()`, `to_polars()` result methods
- [ ] **T3.2.9** Implement `write_to_storage_async()` via IPC to storage backend
- [ ] **T3.2.10** Implement transaction control: `begin()`, `commit()`, `rollback()`
- [ ] **T3.2.11** Implement `SyncIPCDriver(SyncDriverAdapterBase)` for sync contexts
- [ ] **T3.2.12** Create `IPCConfig(AsyncDatabaseConfig)` for SQLSpec registry
- [ ] **T3.2.13** Create `IPCConnection` and `IPCCursor` classes
- [ ] **T3.2.14** Write SQLSpec adapter unit tests (all dialects)

### 3.3 SQLAlchemy Dialect & DBAPI
- [ ] **T3.3.1** Add `sqlalchemy>=2.0` as optional dependency in `pyproject.toml`
- [ ] **T3.3.2** Create `src/py/vertebra/extensions/sqlalchemy/dbapi.py`
- [ ] **T3.3.3** Implement `VertebraConnection` (PEP 249) with IPC client
- [ ] **T3.3.4** Implement `VertebraCursor` with `execute()`, `fetch*()` over IPC
- [ ] **T3.3.5** Implement transaction methods: `commit()`, `rollback()`
- [ ] **T3.3.6** Create `src/py/vertebra/extensions/sqlalchemy/dialect.py`
- [ ] **T3.3.7** Implement `VertebraDialect` for `vertebra+ipc://<pool>?dialect=<dialect>`
- [ ] **T3.3.8** Support all SQLSpec dialects via URL query parameter
- [ ] **T3.3.9** Register `sqlalchemy.dialects` entrypoint in `pyproject.toml`
- [ ] **T3.3.10** Write SQLAlchemy dialect tests

### 3.4 Advanced Alchemy Extension (Last)
- [ ] **T3.4.1** Add `advanced-alchemy` as optional dependency in `pyproject.toml`
- [ ] **T3.4.2** Create `src/py/vertebra/extensions/alchemy/config.py`
- [ ] **T3.4.3** Implement `VertebraAlchemyConfig` (auto-builds vertebra+ipc:// URL)
- [ ] **T3.4.4** Create `src/py/vertebra/extensions/alchemy/plugin.py`
- [ ] **T3.4.5** Implement `VertebraAlchemyPlugin` extending AA's Litestar plugin
- [ ] **T3.4.6** Wire File Object Storage `on_file_save` through IPC
- [ ] **T3.4.7** Wire File Object Storage `on_file_delete` through IPC
- [ ] **T3.4.8** Verify Repository pattern works with IPC sessions
- [ ] **T3.4.9** Verify Service pattern works with IPC sessions
- [ ] **T3.4.10** Write Advanced Alchemy extension tests

### 3.5 DB Node Driver Execution
- [ ] **T3.5.1** Define dialect-to-SQLSpec-driver mapping (16+ adapters)
- [ ] **T3.5.2** Add `EXECUTE` message type to broker protocol with dialect field
- [ ] **T3.5.3** Add `EXECUTE_ARROW` message type for Arrow result requests
- [ ] **T3.5.4** Add `WRITE_STORAGE` message type for storage operations
- [ ] **T3.5.5** Implement pool config lookup for driver selection
- [ ] **T3.5.6** Execute queries via real SQLSpec driver (asyncpg, psycopg, etc.)
- [ ] **T3.5.7** Implement MsgPack result serialization
- [ ] **T3.5.8** Implement Arrow buffer result transport via shared memory
- [ ] **T3.5.9** Implement storage write operations (Parquet, CSV, JSON)
- [ ] **T3.5.10** Transaction boundary tracking for connection affinity
- [ ] **T3.5.11** Write broker execution tests

### 3.6 Storage Integration
- [ ] **T3.6.1** Wire SQLSpec `write_to_storage_*` through IPC client
- [ ] **T3.6.2** Support Parquet, CSV, JSON export formats
- [ ] **T3.6.3** Wire AA File Object lifecycle hooks through IPC
- [ ] **T3.6.4** Support local, S3, GCS storage backends
- [ ] **T3.6.5** Write storage integration tests

### 3.7 Container Integration Tests
- [ ] **T3.7.1** Create `src/py/tests/integration/conftest.py` with fixtures
- [ ] **T3.7.2** Add Postgres container fixture using `tools/lib/container.py`
- [ ] **T3.7.3** Add MySQL container fixture
- [ ] **T3.7.4** Add SQLite fixture (no container needed)
- [ ] **T3.7.5** Add DuckDB fixture (no container needed)
- [ ] **T3.7.6** Add Redis container fixture
- [ ] **T3.7.7** Write SQLSpec `IPCConfig` integration test (Postgres)
- [ ] **T3.7.8** Write SQLSpec `IPCConfig` integration test (MySQL)
- [ ] **T3.7.9** Write SQLAlchemy `vertebra+ipc://` integration test
- [ ] **T3.7.10** Write Advanced Alchemy Repository/Service integration test
- [ ] **T3.7.11** Verify zero-copy Arrow transport end-to-end
- [ ] **T3.7.12** Test storage write via SQLSpec and AA
- [ ] **T3.7.13** Add CI workflow for integration tests

## Phase 4: MCP Integration (PRD-7 Gaps)

### 4.1 Full Protocol Implementation
- [ ] **T4.1.1** Add `tools/list` handler
- [ ] **T4.1.2** Add `resources/list` handler
- [ ] **T4.1.3** Add `resources/read` handler
- [ ] **T4.1.4** Add `resources/subscribe` handler (optional)
- [ ] **T4.1.5** Add `prompts/list` handler
- [ ] **T4.1.6** Add `prompts/get` handler
- [ ] **T4.1.7** Add `logging/setLevel` handler
- [ ] **T4.1.8** Write protocol compliance tests

### 4.2 Transport Abstraction
- [ ] **T4.2.1** Create `src/ts/src/mcp/transport.ts` with interface
- [ ] **T4.2.2** Implement `StdioTransport`
- [ ] **T4.2.3** Implement `HttpSseTransport`
- [ ] **T4.2.4** Implement `WebSocketTransport`
- [ ] **T4.2.5** Add transport negotiation in server
- [ ] **T4.2.6** Write transport tests

### 4.3 IPC Bridge Implementation
- [ ] **T4.3.1** Implement `IpcBridge.callTool()` using existing IPC client
- [ ] **T4.3.2** Map MCP tool names to procedure IDs
- [ ] **T4.3.3** Add schema validation using Type Bridge IR
- [ ] **T4.3.4** Implement error translation (Python → MCP error codes)
- [ ] **T4.3.5** Write IPC bridge tests

## Phase 5: Documentation & Examples (PRD-6 Gaps)

### 5.1 Getting Started Guide
- [ ] **T5.1.1** Write installation section (uv, Bun, Rust)
- [ ] **T5.1.2** Write Hello World tutorial
- [ ] **T5.1.3** Write procedure definition guide
- [ ] **T5.1.4** Write type generation guide
- [ ] **T5.1.5** Write database access guide

### 5.2 API Reference
- [ ] **T5.2.1** Configure sphinx-autodoc for Python
- [ ] **T5.2.2** Configure typedoc for TypeScript
- [ ] **T5.2.3** Write Rust contributor docs

### 5.3 Performance Benchmarks
- [ ] **T5.3.1** Create `benchmarks/` directory structure
- [ ] **T5.3.2** Implement HTTP throughput benchmark
- [ ] **T5.3.3** Implement IPC latency benchmark
- [ ] **T5.3.4** Implement memory usage benchmark
- [ ] **T5.3.5** Write benchmark results documentation

### 5.4 Migration Guide
- [ ] **T5.4.1** Write uvicorn migration section
- [ ] **T5.4.2** Write Gunicorn migration section
- [ ] **T5.4.3** Write FastAPI comparison section

## Task Summary by Priority

### Critical Path (Blocks Production)
1. T1.4.4–T1.4.7 (Unified Metrics Wiring)
2. ~~T1.6.* (Supervisor / process manager)~~ ✅ Complete
3. T2.1.* (Pool Fairness)

### High Priority (Core Functionality)
1. T1.3.7–T1.3.9 (SSE support + tests)
2. T2.2.* (Lifecycle Hooks)
3. T3.1.* (Unified Config Layer)
4. T3.2.* (SQLSpec IPC Adapter)
5. T3.5.* (DB Node Driver Execution)
6. T4.1.* (MCP Protocol)

### Medium Priority (Quality & Polish)
1. T3.6.* (Storage Integration)
2. T3.7.* (Container Integration Tests)
3. T4.2.* (MCP Transports)

### Deferred Until Last
1. T3.3.* (SQLAlchemy Dialect)
2. T3.4.* (Advanced Alchemy Extension)

### Lower Priority (Can Parallel)
1. T5.* (Documentation)
2. T2.3.* (Observability)
3. T4.3.* (IPC Bridge)

## Dependencies

```
T1.1 → T1.2 (disconnect requires streaming frames)
T1.3 → T1.4 (WebSocket metrics)
T2.1 → T2.2 (hooks need fairness base)
T2.2 → T2.3 (observability hooks)
T3.1 → T3.2, T3.3, T3.4 (config layer needed first)
T3.2 → T3.5 (SQLSpec adapter needs DB Node execution)
T3.3 → T3.5 (SQLAlchemy dialect needs DB Node execution)
T3.4 → T3.3 (AA extension depends on SQLAlchemy dialect)
T3.5 → T3.6 (storage integration needs DB Node)
T3.6 → T3.7 (integration tests need storage)
T4.1 → T4.2 (protocol before transports)
T4.2 → T4.3 (bridge uses transports)
T5.* → All others (docs last)
```
