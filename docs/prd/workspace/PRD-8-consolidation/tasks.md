# PRD-8 Task Breakdown

**Total Tasks:** 67
**Estimated Effort:** 10 weeks

## Phase 1: Server Completion (PRD-3 Gaps)

### 1.1 Request Body Streaming
- [ ] **T1.1.1** Add `http.request` frame type to `AsgiIpcFrame` union in `src/ts/src/ipc/asgi.ts`
- [ ] **T1.1.2** Modify `AsgiBridge.handle()` to use `ReadableStream` reader for chunked body
- [ ] **T1.1.3** Implement request frame emission during body streaming
- [ ] **T1.1.4** Update Python worker to handle interleaved request/response frames
- [ ] **T1.1.5** Add backpressure handling via IPC channel flow control
- [ ] **T1.1.6** Write unit tests for chunked request bodies
- [ ] **T1.1.7** Write integration test for large file upload streaming

### 1.2 HTTP Disconnect Propagation
- [ ] **T1.2.1** Add `http.disconnect` frame type to `AsgiIpcFrame`
- [ ] **T1.2.2** Track request abort via `AbortController` in ASGI bridge
- [ ] **T1.2.3** Emit disconnect frame on client abort
- [ ] **T1.2.4** Python worker cancels coroutine on disconnect
- [ ] **T1.2.5** Write test for client abort during response streaming

### 1.3 WebSocket/SSE Bridging
- [ ] **T1.3.1** Add `websocket.*` frame types to `AsgiIpcFrame`
- [ ] **T1.3.2** Detect WebSocket upgrade in router
- [ ] **T1.3.3** Implement WebSocket accept and bidirectional stream
- [ ] **T1.3.4** Forward `websocket.receive` to Python worker
- [ ] **T1.3.5** Forward `websocket.send` from Python to client
- [ ] **T1.3.6** Implement close handshake
- [ ] **T1.3.7** Add SSE support (unidirectional streaming)
- [ ] **T1.3.8** Write WebSocket echo test
- [ ] **T1.3.9** Write SSE event stream test

### 1.4 Metrics Infrastructure
- [ ] **T1.4.1** Create `src/ts/src/server/metrics.ts` with metric types
- [ ] **T1.4.2** Add lightweight metrics library (or custom implementation)
- [ ] **T1.4.3** Instrument request handler with timing
- [ ] **T1.4.4** Add worker pool utilization metric
- [ ] **T1.4.5** Add IPC latency histogram
- [ ] **T1.4.6** Expose `/__metrics__` endpoint
- [ ] **T1.4.7** Add health check timing metric
- [ ] **T1.4.8** Write test for metrics endpoint format

### 1.5 Graceful Shutdown
- [ ] **T1.5.1** Add `drainTimeoutMs` config option
- [ ] **T1.5.2** On SIGTERM, stop accepting new connections
- [ ] **T1.5.3** Wait for in-flight request drain
- [ ] **T1.5.4** Send graceful SIGTERM to workers
- [ ] **T1.5.5** Force SIGKILL after timeout
- [ ] **T1.5.6** Write shutdown sequence test

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

## Phase 3: Driver Integration (PRD-4 Gaps)

### 3.1 SQLSpec Adapter Layer
- [ ] **T3.1.1** Add `sqlspec` as optional dependency in `pyproject.toml`
- [ ] **T3.1.2** Create `src/py/vertebra/drivers/sqlspec.py`
- [ ] **T3.1.3** Implement `VertebraPostgresDriver` adapter
- [ ] **T3.1.4** Implement `VertebraMySQLDriver` adapter
- [ ] **T3.1.5** Implement `VertebraSQLiteDriver` adapter
- [ ] **T3.1.6** Write SQLSpec protocol compliance tests

### 3.2 Container Integration Tests
- [ ] **T3.2.1** Create `src/py/tests/integration/conftest.py` with fixtures
- [ ] **T3.2.2** Add Postgres container fixture using `tools/lib/container.py`
- [ ] **T3.2.3** Add MySQL container fixture
- [ ] **T3.2.4** Add Redis container fixture
- [ ] **T3.2.5** Write Postgres full IPC integration test
- [ ] **T3.2.6** Write MySQL full IPC integration test
- [ ] **T3.2.7** Write Redis full IPC integration test
- [ ] **T3.2.8** Add CI workflow for integration tests

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
1. T1.5.* (Graceful Shutdown)
2. T1.4.* (Metrics)
3. T2.1.* (Pool Fairness)

### High Priority (Core Functionality)
1. T1.1.* (Request Body Streaming)
2. T1.2.* (HTTP Disconnect)
3. T2.2.* (Lifecycle Hooks)
4. T4.1.* (MCP Protocol)

### Medium Priority (Quality & Polish)
1. T1.3.* (WebSocket/SSE)
2. T3.1.* (SQLSpec Adapter)
3. T3.2.* (Integration Tests)
4. T4.2.* (MCP Transports)

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
T3.2 → T3.1 (tests need adapters)
T4.1 → T4.2 (protocol before transports)
T4.2 → T4.3 (bridge uses transports)
T5.* → All others (docs last)
```
