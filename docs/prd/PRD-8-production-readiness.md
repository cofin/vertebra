# PRD-8: Production Readiness Consolidation

**Status:** Draft
**Created:** 2026-01-18
**Author:** AI Assistant
**Dependencies:** PRD-1 (complete), PRD-2 (complete), PRD-3 (partial), PRD-4 (partial), PRD-5 (partial), PRD-6 (minimal), PRD-7 (minimal)

## Executive Summary

This PRD consolidates all outstanding work across PRDs 3-7 into a unified production readiness initiative. The Vertebra project has solid foundations—the Rust IPC layer (PRD-1) and Python bindings (PRD-2) are complete—but significant gaps remain in the server layer, resource management, driver integration, documentation, and MCP support.

This document specifies the remaining work with a focus on:
- **Maximum performance**: Zero-copy paths, memory management, latency optimization
- **Production safety**: Graceful degradation, observability, health monitoring
- **Developer experience**: Clear APIs, comprehensive docs, debugging tools

## Problem Statement

### Current State

Vertebra's multi-language architecture (Python 3.12+, Bun/TypeScript, Rust 1.75+) provides a high-performance application server with zero-copy IPC. However, the system is not production-ready:

1. **Server Layer (PRD-3, 70%)**: Missing request body streaming, WebSocket support, Protobuf IPC, and metrics infrastructure
2. **Resource Management (PRD-5, 20%)**: Pool fairness, lifecycle hooks, and observability are unimplemented (11 stub locations identified)
3. **Driver Integration (PRD-4, 75%)**: SQLSpec adapter layer and container-based integration tests missing
4. **MCP Support (PRD-7, 10%)**: Skeleton only—full protocol, transports, and security unimplemented
5. **Documentation (PRD-6, 5%)**: No examples, benchmarks, or migration guides

### Impact

Without completion:
- Cannot safely deploy in production (no graceful shutdown, no metrics)
- Cannot integrate with AI workflows (MCP incomplete)
- Cannot demonstrate value (no benchmarks or examples)
- Cannot onboard developers (no documentation)

## Goals and Non-Goals

### Goals

1. Complete the Bun HTTP server with full ASGI feature parity
2. Implement production-grade resource pooling with fairness guarantees
3. Deliver SQLSpec-compatible database drivers with integration tests
4. Implement full MCP protocol for AI assistant integration
5. Provide comprehensive documentation, examples, and benchmarks

### Non-Goals

1. gRPC or other RPC protocols beyond HTTP and MCP
2. Distributed tracing (OpenTelemetry) in this phase
3. Kubernetes operator or custom controllers
4. Native ARM64 Windows support (x64 + ARM64 Linux/macOS only)

## Technical Specification

### Phase 1: Server Completion (PRD-3 Gaps)

#### 1.1 Request Body Streaming

**Current:** Request body is fully buffered before forwarding to Python workers.

**Target:** Stream request body chunks as `http.request` ASGI events.

```typescript
// src/ts/src/ipc/asgi.ts - New frame type
export type AsgiIpcFrame =
  | { type: "http.request"; body?: Uint8Array; more_body?: boolean }
  | /* existing frame types */;
```

**Implementation:**
1. Modify `AsgiBridge.handle()` to use `request.body.getReader()` for chunked reads
2. Add `http.request` frame emission during streaming
3. Python worker receives interleaved request/response frames
4. Backpressure via IPC channel `desiredSize` checks

**Files:** `src/ts/src/ipc/asgi.ts`, `src/py/vertebra/server/worker.py`

#### 1.2 HTTP Disconnect Propagation

**Current:** Client disconnect not communicated to Python workers.

**Target:** Send `http.disconnect` frame when client aborts request.

**Implementation:**
1. Track request abort signal via `AbortController`
2. On abort, send `{ type: "http.disconnect", id }` frame
3. Python worker cancels in-flight coroutine

**Files:** `src/ts/src/ipc/asgi.ts`, `src/py/vertebra/server/worker.py`

#### 1.3 WebSocket/SSE Bridging

**Current:** No WebSocket or SSE support in ASGI bridge.

**Target:** Full bidirectional WebSocket with ASGI `websocket.*` events.

```typescript
// New frame types
| { type: "websocket.connect"; id: string }
| { type: "websocket.receive"; id: string; bytes?: Uint8Array; text?: string }
| { type: "websocket.disconnect"; id: string; code?: number }
| { type: "websocket.send"; id: string; bytes?: Uint8Array; text?: string }
| { type: "websocket.close"; id: string; code?: number; reason?: string }
```

**Implementation:**
1. Detect WebSocket upgrade via `Upgrade: websocket` header
2. Accept upgrade, establish bidirectional IPC stream
3. Forward `websocket.receive` events to Python
4. Forward `websocket.send` responses to client
5. Handle close handshake properly

**Files:** `src/ts/src/ipc/asgi.ts`, `src/ts/src/server/router.ts`, `src/py/vertebra/server/worker.py`

#### 1.4 Metrics Infrastructure

**Current:** No metrics collection.

**Target:** Prometheus-compatible metrics endpoint.

```typescript
// src/ts/src/server/metrics.ts
export interface ServerMetrics {
  requestsTotal: Counter;
  requestDurationSeconds: Histogram;
  activeConnections: Gauge;
  workerPoolUtilization: Gauge;
  ipcLatencySeconds: Histogram;
}
```

**Implementation:**
1. Add `prom-client` or similar lightweight metrics library
2. Instrument request handler with timing
3. Instrument worker pool with utilization tracking
4. Expose `/__metrics__` endpoint
5. Add worker health check timing

**Files:** New `src/ts/src/server/metrics.ts`, modify `src/ts/src/server/index.ts`

#### 1.5 Graceful Shutdown

**Current:** SIGTERM kills workers immediately.

**Target:** Drain in-flight requests before shutdown.

**Implementation:**
1. On SIGTERM, stop accepting new connections
2. Wait for `drainTimeoutMs` (configurable) for in-flight requests
3. Send SIGTERM to workers with grace period
4. Force SIGKILL after `shutdownTimeoutMs`
5. Exit cleanly

**Files:** `src/ts/src/server/index.ts`, `src/ts/src/server/worker-manager.ts`

### Phase 2: Resource Management (PRD-5 Gaps)

#### 2.1 Pool Fairness Scheduler

**Current:** `pool_manager.rs` is a 9-line trait stub.

**Target:** Pluggable fairness algorithms with lease management.

```rust
// src/rs/vertebra-broker/src/pool_manager.rs
pub enum FairnessPolicy {
    Fifo,
    WeightedFair { weights: HashMap<String, u32> },
    Priority { levels: u8 },
}

pub struct PoolManager {
    config: PoolConfig,
    policy: FairnessPolicy,
    waiters: VecDeque<Waiter>,
    leases: HashMap<LeaseToken, Lease>,
}

impl PoolManager {
    pub fn acquire(&self, client_id: &str, timeout: Duration) -> Result<LeaseToken, PoolError>;
    pub fn release(&self, token: LeaseToken) -> Result<(), PoolError>;
    pub fn health_check(&self) -> HealthStatus;
}
```

**Implementation:**
1. Define `FairnessPolicy` enum with FIFO, weighted, priority variants
2. Implement waiter queue with policy-based ordering
3. Add lease tracking with timeout enforcement
4. Implement eviction for expired leases
5. Add metrics for wait time, utilization, eviction rate

**Files:** `src/rs/vertebra-broker/src/pool_manager.rs`, `src/rs/vertebra-broker/src/protocol.rs`

#### 2.2 Lifecycle Hooks

**Current:** No hooks for acquire/release/validate.

**Target:** Extensible hook system for resource lifecycle.

```python
# src/py/vertebra/resources/hooks.py
class LifecycleHooks(Protocol):
    async def on_acquire(self, pool: str, token: str) -> None: ...
    async def on_release(self, pool: str, token: str) -> None: ...
    async def on_validate(self, pool: str, token: str) -> bool: ...
    async def on_evict(self, pool: str, token: str, reason: str) -> None: ...
```

**Implementation:**
1. Define hook protocol in Python
2. Wire hooks into `BrokerPool.acquire()` and `release()`
3. Add validation hook for periodic health checks
4. Add eviction hook for cleanup on timeout

**Files:** New `src/py/vertebra/resources/hooks.py`, modify `src/py/vertebra/resources/pool.py`

#### 2.3 Observability

**Current:** No pool metrics.

**Target:** Comprehensive pool observability.

**Metrics:**
- `pool_size` (current/max)
- `pool_wait_seconds` (histogram)
- `pool_lease_duration_seconds` (histogram)
- `pool_evictions_total` (counter by reason)
- `pool_errors_total` (counter by type)

**Implementation:**
1. Add metrics emission in Rust broker
2. Expose via broker protocol as `metrics` command
3. Python client aggregates and exposes
4. Optional push to Prometheus Pushgateway

**Files:** `src/rs/vertebra-broker/src/metrics.rs` (new), `src/py/vertebra/resources/metrics.py` (new)

### Phase 3: Driver Integration (PRD-4 Gaps)

#### 3.1 SQLSpec Adapter Layer

**Current:** Drivers implement custom DB-API-like interface.

**Target:** SQLSpec-compatible adapter for unified query execution.

```python
# src/py/vertebra/drivers/sqlspec.py
from sqlspec import AsyncDriver

class VertebraPostgresDriver(AsyncDriver):
    async def execute(self, query: str, params: dict | None = None) -> Result:
        # Delegate to existing PostgresAsyncConnection
        ...
```

**Implementation:**
1. Add `sqlspec` optional dependency
2. Create adapter classes for each driver (Postgres, MySQL, SQLite)
3. Implement SQLSpec `AsyncDriver` protocol
4. Add unit tests for SQLSpec compliance

**Files:** New `src/py/vertebra/drivers/sqlspec.py`, modify `pyproject.toml`

#### 3.2 Container Integration Tests

**Current:** No real database integration tests.

**Target:** Test against real databases in containers.

**Implementation:**
1. Use existing `tools/lib/container.py` runtime abstraction
2. Create pytest fixtures for Postgres, MySQL, Redis
3. Add integration test suite exercising full IPC path
4. Run in CI with Docker/Podman

**Files:** New `src/py/tests/integration/`, modify `.github/workflows/`

### Phase 4: MCP Integration (PRD-7 Gaps)

#### 4.1 Full Protocol Implementation

**Current:** JSON-RPC handler with basic tool dispatch.

**Target:** Complete MCP 1.0 specification support.

**Components:**
- `tools/list`, `tools/call` (partial)
- `resources/list`, `resources/read`, `resources/subscribe`
- `prompts/list`, `prompts/get`
- `logging/setLevel`
- `sampling/createMessage` (server-initiated)

```typescript
// src/ts/src/mcp/server.ts - Enhanced
export class McpServer {
  private tools = new Map<string, McpTool>();
  private resources = new Map<string, McpResource>();
  private prompts = new Map<string, McpPrompt>();

  // Protocol handlers
  async handleToolsList(): Promise<ToolsListResult>;
  async handleToolsCall(params: ToolsCallParams): Promise<ToolsCallResult>;
  async handleResourcesList(): Promise<ResourcesListResult>;
  async handleResourcesRead(params: ResourcesReadParams): Promise<ResourcesReadResult>;
  // ...
}
```

**Files:** `src/ts/src/mcp/server.ts`, new `src/ts/src/mcp/resources.ts`, `src/ts/src/mcp/prompts.ts`

#### 4.2 Transport Abstraction

**Current:** No transport implementation.

**Target:** Support stdio, HTTP SSE, and WebSocket transports.

```typescript
// src/ts/src/mcp/transport.ts
export interface McpTransport {
  send(message: JsonRpcMessage): Promise<void>;
  receive(): AsyncIterable<JsonRpcMessage>;
  close(): Promise<void>;
}

export class StdioTransport implements McpTransport { /* ... */ }
export class HttpSseTransport implements McpTransport { /* ... */ }
export class WebSocketTransport implements McpTransport { /* ... */ }
```

**Implementation:**
1. Define `McpTransport` interface
2. Implement stdio for CLI tools
3. Implement HTTP SSE for web clients
4. Implement WebSocket for bidirectional
5. Add transport negotiation in server startup

**Files:** New `src/ts/src/mcp/transport.ts`, modify `src/ts/src/mcp/server.ts`

#### 4.3 IPC Bridge Implementation

**Current:** `IpcBridge.callTool()` throws "not configured".

**Target:** Route MCP tool calls through Python worker IPC.

**Implementation:**
1. Use existing IPC client infrastructure
2. Map MCP tool calls to procedure invocations
3. Schema validation using Type Bridge IR
4. Error translation (Python exception → MCP error)

**Files:** `src/ts/src/mcp/ipc-bridge.ts`, `src/py/vertebra/mcp/__init__.py` (new)

### Phase 5: Documentation & Examples (PRD-6 Gaps)

#### 5.1 Getting Started Guide

**Content:**
1. Installation (uv, Bun, Rust toolchain)
2. Hello World (Python app + Bun server)
3. Adding procedures
4. Type generation
5. Database access

**File:** `docs/getting-started.md`

#### 5.2 API Reference

**Content:**
1. Python: `@procedure`, `TypeExtractor`, drivers, resources
2. TypeScript: `BunServer`, `AsgiBridge`, `McpServer`
3. Rust: `vertebra-core` (for contributors)

**Generated:** Via sphinx-autodoc + typedoc

#### 5.3 Performance Benchmarks

**Benchmarks:**
1. HTTP throughput (requests/sec vs uvicorn)
2. IPC latency (p50/p95/p99)
3. Memory usage under load
4. Worker scaling efficiency

**Files:** `benchmarks/`, `docs/benchmarks.md`

#### 5.4 Migration Guide

**Content:**
1. From uvicorn: config mapping, ASGI compatibility
2. From Gunicorn: worker model differences
3. From FastAPI: procedure vs route patterns

**File:** `docs/migration.md`

## Implementation Phases

### Milestone 1: Server Foundation (Weeks 1-2)
- [ ] Request body streaming
- [ ] HTTP disconnect propagation
- [ ] Graceful shutdown
- [ ] Basic metrics endpoint

### Milestone 2: Resource Management (Weeks 3-4)
- [ ] Pool fairness scheduler (FIFO + weighted)
- [ ] Lifecycle hooks
- [ ] Pool metrics

### Milestone 3: Driver & Testing (Weeks 5-6)
- [ ] SQLSpec adapter layer
- [ ] Container integration test harness
- [ ] Driver integration tests

### Milestone 4: MCP Complete (Weeks 7-8)
- [ ] Full protocol implementation
- [ ] Transport abstraction (stdio, SSE, WebSocket)
- [ ] IPC bridge integration

### Milestone 5: Documentation (Week 9)
- [ ] Getting started guide
- [ ] API reference generation
- [ ] Benchmarks
- [ ] Migration guide

### Milestone 6: Production Hardening (Week 10)
- [ ] WebSocket/SSE support
- [ ] Security review
- [ ] Performance optimization
- [ ] Release preparation

## Success Metrics

| Metric | Target |
|--------|--------|
| HTTP throughput | ≥ 50,000 req/s (hello world) |
| IPC latency p99 | ≤ 1ms |
| Memory per worker | ≤ 50MB idle |
| Pool fairness variance | ≤ 10% under load |
| Test coverage | ≥ 80% |
| Documentation | All public APIs documented |

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Protobuf codegen complexity | Medium | Defer to post-MVP, keep MessagePack |
| MCP spec changes | Low | Pin to MCP 1.0, track updates |
| Container test flakiness | Medium | Retry logic, health waits |
| Performance regression | High | Benchmark CI gate |

## Open Questions

1. **Protobuf timeline**: Should Protobuf IPC be MVP or post-MVP?
   - Recommendation: Post-MVP (MessagePack is sufficient)

2. **MCP transport priority**: Which transport first?
   - Recommendation: stdio (simplest), then HTTP SSE

3. **SQLSpec version**: Which SQLSpec version to target?
   - Recommendation: Latest stable, with version pin

## Appendix

### A. File Inventory

**New Files:**
- `src/ts/src/server/metrics.ts`
- `src/ts/src/mcp/transport.ts`
- `src/ts/src/mcp/resources.ts`
- `src/ts/src/mcp/prompts.ts`
- `src/py/vertebra/resources/hooks.py`
- `src/py/vertebra/resources/metrics.py`
- `src/py/vertebra/drivers/sqlspec.py`
- `src/py/vertebra/mcp/__init__.py`
- `src/rs/vertebra-broker/src/metrics.rs`
- `src/py/tests/integration/`
- `docs/getting-started.md`
- `docs/migration.md`
- `docs/benchmarks.md`
- `benchmarks/`

**Modified Files:**
- `src/ts/src/ipc/asgi.ts`
- `src/ts/src/server/index.ts`
- `src/ts/src/server/worker-manager.ts`
- `src/ts/src/mcp/server.ts`
- `src/ts/src/mcp/ipc-bridge.ts`
- `src/py/vertebra/server/worker.py`
- `src/py/vertebra/resources/pool.py`
- `src/rs/vertebra-broker/src/pool_manager.rs`
- `src/rs/vertebra-broker/src/protocol.rs`
- `pyproject.toml`
- `.github/workflows/`

### B. Dependency Additions

**Python:**
- `sqlspec` (optional)
- `prometheus-client` (optional)

**TypeScript:**
- Metrics library TBD (prom-client or custom)

### C. Testing Strategy

1. **Unit tests**: All new modules (pytest, bun test)
2. **Integration tests**: IPC paths with real databases
3. **Performance tests**: Benchmark suite with CI gate
4. **Compatibility tests**: SQLSpec compliance, MCP protocol
