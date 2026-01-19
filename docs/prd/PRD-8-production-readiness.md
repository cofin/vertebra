# PRD-8: Production Readiness Consolidation

**Status:** In Progress
**Created:** 2026-01-18
**Last Updated:** 2026-01-19
**Author:** AI Assistant
**Dependencies:** PRD-1 (complete), PRD-2 (complete), PRD-3 (partial), PRD-4 (partial), PRD-5 (partial), PRD-6 (minimal), PRD-7 (minimal)

## Executive Summary

This PRD consolidates all outstanding work across PRDs 3-7 into a unified production readiness initiative. The Vertebra project has solid foundations—the Rust IPC layer (PRD-1) and Python bindings (PRD-2) are complete—but significant gaps remain in the server layer, resource management, driver integration, documentation, and MCP support.

This document specifies the remaining work with a focus on:

- **Maximum performance**: Zero-copy paths, memory management, latency optimization
- **Production safety**: Graceful degradation, observability, health monitoring
- **Developer experience**: Clear APIs, comprehensive docs, debugging tools

**Update (2026-01-19)**: The driver/broker data plane has moved to a **shared-memory broker**
design (no sockets/TCP) with **C ABI + CFFI** as the mandatory transport. The broker is the
central authority for SQL **and storage** access. See `specs/active/shared-memory-broker/`.
Any socket-based driver protocol references in earlier PRDs are now deprecated.

### Decision Log (2026-01-19)

- **Protobuf IPC**: Deferred to post-MVP; Python uses `betterproto`, TS uses `protobuf-es` once Bun support is ready.
- **MCP transports**: Implement stdio + SSE + WebSocket (Bun native).
- **SQLSpec/Advanced Alchemy versions**: Pin minimums to the latest stable at implementation time.
- **fsspec tests**: Local backend only by default; S3/GCS are optional/skip unless configured.
- **Single-file distribution**: Linux + macOS only for initial release; Windows later.
- **Metrics**: `prom-client` approved on Bun side; align labels with server metrics.

## Problem Statement

### Current State

Vertebra's multi-language architecture (Python 3.12+, Bun/TypeScript, Rust 1.75+) provides a high-performance application server with zero-copy IPC. However, the system is not production-ready:

1. **Server Layer (PRD-3, ~95%)**: Streaming, WebSockets, metrics endpoint, JSON logging, CLI, unified metrics aggregation, and supervisor are implemented. Protobuf IPC stays post-MVP.
2. **Resource Management (PRD-5, ~60%)**: Shared-memory broker + fairness scheduler landed; observability and registration API are pending.
3. **Driver Integration (PRD-4, ~85%)**: Shared-memory broker transport, SQLSpec adapter layer, and broker-backed storage are implemented; container-based integration tests are missing.
4. **MCP Support (PRD-7, 10%)**: Skeleton only—full protocol, transports, and security unimplemented
5. **Documentation (PRD-6, 5%)**: No examples, benchmarks, or migration guides

### Impact

Without completion:

- Cannot safely deploy in production (no unified metrics aggregation or supervisor)
- Cannot integrate with AI workflows (MCP incomplete)
- Cannot demonstrate value (no benchmarks or examples)
- Cannot onboard developers (no documentation)

## Goals and Non-Goals

### Goals

1. Complete the Bun HTTP server with full ASGI feature parity
2. Implement production-grade resource pooling with fairness guarantees
3. Deliver SQLSpec-compatible database drivers **and storage adapters** with integration tests
4. Implement full MCP protocol for AI assistant integration
5. Provide comprehensive documentation, examples, and benchmarks

### Non-Goals

1. gRPC or other RPC protocols beyond HTTP and MCP
2. Distributed tracing (OpenTelemetry) in this phase
3. Kubernetes operator or custom controllers
4. Windows packaging in this phase (Linux + macOS only)
5. Native ARM64 Windows support (x64 + ARM64 Linux/macOS only)

## Technical Specification

### Phase 1: Server Completion (PRD-3 Gaps)

**Status:** Complete. Protobuf IPC stays post-MVP; HTTP/2/HTTP/3 validation remains a follow-up.

#### 1.0 IPC Protocol (MVP vs Post-MVP)

**MVP:** MessagePack remains the default IPC codec.
**Post-MVP:** Protobuf IPC stays planned, with Python bindings via `betterproto` and TS via `protobuf-es` (pending Bun support).

#### 1.1 Request Body Streaming

**Status:** Implemented (2026-01-19). Request bodies stream as `http.request` ASGI events.

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

**Status:** Implemented (2026-01-19). Client disconnects emit `http.disconnect` frames.

**Implementation:**

1. Track request abort signal via `AbortController`
2. On abort, send `{ type: "http.disconnect", id }` frame
3. Python worker cancels in-flight coroutine

**Files:** `src/ts/src/ipc/asgi.ts`, `src/py/vertebra/server/worker.py`

#### 1.3 WebSocket/SSE Bridging

**Status:** WebSocket bridging implemented (2026-01-19). SSE support is still pending.

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

**Status:** Prometheus endpoint + request metrics implemented. Unified aggregation (broker + worker stats) and IPC latency wiring remain pending.

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

1. Use `prom-client` for Bun metrics
2. Instrument request handler with timing
3. Instrument worker pool with utilization tracking
4. Expose `/__metrics__` endpoint
5. Add worker health check timing

**Files:** New `src/ts/src/server/metrics.ts`, modify `src/ts/src/server/index.ts`

#### 1.5 Structured Logging & Trace Context

**Status:** Bun-side JSON logging + trace ID injection implemented. SQLSpec correlation ID mapping will land with the SQLSpec extension.

**Implementation:**
1. Emit JSON logs from Bun, Broker, and Python worker processes
2. Propagate `X-Request-ID` / `traceparent` across IPC
3. Map SQLSpec correlation IDs into the same `trace_id` field when present

#### 1.6 Python CLI Entry Point

**Status:** Implemented (2026-01-19). `vertebra serve` spawns Bun and forwards signals.

**Implementation:**

1. Create `vertebra.cli` module with `serve` command
2. Detect `bun` binary or prompt installation
3. Spawn Bun server process with correct arguments
4. Forward signals (SIGTERM/SIGINT) to Bun process

**Files:** `src/py/vertebra/cli.py`, `src/py/vertebra/__main__.py`

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

### Phase 3: ORM Extensions (SQLSpec first, Advanced Alchemy last)

This phase implements provider plugins for SQLSpec first, then adds Advanced Alchemy last. SQLAlchemy dialect work is deferred until the end.

**Key Principle:** Use a typed wrapper for SQLSpec configs so `provide_session()` returns a broker-backed proxy while preserving dialect/type metadata (shared-memory C ABI; no sockets).

#### 3.1 Plugin Registration API

```python
from vertebra import Vertebra
from vertebra.resources import ProviderConfig
from vertebra.extensions.sqlspec.provider import SqlSpecProvider
from sqlspec.adapters.asyncpg import AsyncpgConfig
from advanced_alchemy.config import SQLAlchemyAsyncConfig
from advanced_alchemy.providers import AdvancedAlchemyProvider

sqlspec_config = AsyncpgConfig(
    host="localhost", port=5432, database="mydb",
    user="user", password="pass", pool_size=20,
)
sqlspec_provider = SqlSpecProvider(
    sqlspec_config,
    ProviderConfig(pool="postgres", driver="asyncpg"),
)

aa_config = SQLAlchemyAsyncConfig(
    connection_string="postgresql+asyncpg://localhost/db",
)
aa_provider = AdvancedAlchemyProvider(
    aa_config,
    ProviderConfig(pool="main", driver="advanced-alchemy"),
)

app = Vertebra(plugins=[sqlspec_provider, aa_provider])
```

**Implementation:**

1. Add plugin list support on `Vertebra(...)`
2. Add `register_plugin(...)` for provider instances
4. Hook into config's `provide_session()` to route through broker (CFFI/shared memory)
5. Provide a generic wrapper for SQLSpec configs (typed `IPCConfig[T]`)
6. Pin minimum versions to the latest stable SQLSpec + Advanced Alchemy at implementation time

#### 3.2 SQLSpec Integration

All 16+ SQLSpec adapters work directly:

```python
# Use registered config normally
async with sqlspec_config.provide_session() as session:
    result = await session.execute("SELECT * FROM users")
    df = await result.to_pandas()
    table = await result.to_arrow()  # Zero-copy via shared memory
```

**Features:**
- All 16+ adapters (asyncpg, psycopg, duckdb, oracledb, bigquery, etc.)
- Zero-copy Arrow results via shared memory
- `write_to_storage_async()` works transparently
- Transaction boundaries tracked for connection affinity
- SQLSpec correlation IDs mapped into trace context

#### 3.3 Advanced Alchemy Integration

Repository/Service patterns work unchanged:

```python
# Use registered config normally
class UserRepository(SQLAlchemyAsyncRepository[User]):
    model_type = User

class UserService(SQLAlchemyAsyncRepositoryService[User]):
    repository_type = UserRepository

# With Litestar plugin
plugin = VertebraPlugin(alchemy_configs={"main": aa_config})
app = Litestar(plugins=[plugin, SQLAlchemyPlugin(config=aa_config)])
```

**Features:**
- Repository/Service patterns work unchanged
- File Object Storage routes through shared-memory broker
- Litestar plugin integration for DI

#### 3.4 Plain SQLAlchemy (Exposed Drivers Only)

For plain SQLAlchemy users, Vertebra exposes internal drivers only:

| Dialect URL | Internal Driver |
|-------------|-----------------|
| `vertebra+postgres://` | Bun postgres |
| `vertebra+mysql://` | Rust sqlx-mysql |
| `vertebra+sqlite://` | Rust sqlx-sqlite |

```python
from sqlalchemy import create_engine

# Can use exposed drivers
engine = create_engine("vertebra+postgres://localhost/db")
```

**Limitation:** Plain SQLAlchemy users **cannot** register custom adapters with the broker. For that, use Advanced Alchemy.

#### 3.5 Container Integration Tests

**Infra:** Use existing `tools/lib/container.py` runtime abstraction.

**Coverage:**
- SQLSpec with asyncpg against real Postgres
- SQLSpec with DuckDB (no container needed)
- Advanced Alchemy Repository/Service patterns
- Zero-copy Arrow transport verification
- Storage write operations

**Files:** New `src/py/tests/integration/`, modify `.github/workflows/`

#### 3.6 fsspec Integration (Local-first)

**Target:** `fsspec` adapter backed by `vertebra.storage`.

**Implementation:**
1. Implement `fsspec.AbstractFileSystem` subclass
2. Cover core methods (`open`, `ls`, `info`, `exists`, `mkdir`, `rm`)
3. Local backend tests required
4. S3/GCS tests optional and skipped/xfail unless configured

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

### Phase 5: Single-File Distribution & Supervisor

#### 5.1 Unified Supervisor (Rust)

**Target:** `vertebra.rust.init_supervisor()` manages broker + Bun sidecar.

**Implementation:**
1. Embed `vertebra-broker` in `vertebra-py` and start it in a background Rust thread
2. Spawn the compiled Bun engine as a subprocess
3. Forward signals and manage clean shutdown
4. Expose `init_supervisor()` via PyO3

#### 5.2 Build Tooling

**Target:** `vertebra build --one-file` using `pyapp`.

**Implementation:**
1. Compile Bun engine into `vertebra/bin/vertebra-engine`
2. Build wheel via Maturin/Hatch
3. Bundle using `pyapp`

**Targets:** Linux + macOS for initial release; Windows later.

### Phase 6: Documentation & Examples (PRD-6 Gaps)

#### 6.1 Getting Started Guide

**Content:**

1. Installation (uv, Bun, Rust toolchain)
2. Hello World (Python app + Bun server)
3. Adding procedures
4. Type generation
5. Database access

**File:** `docs/getting-started.md`

#### 6.2 API Reference

**Content:**

1. Python: `@procedure`, `TypeExtractor`, drivers, resources
2. TypeScript: `BunServer`, `AsgiBridge`, `McpServer`
3. Rust: `vertebra-core` (for contributors)

**Generated:** Via sphinx-autodoc + typedoc

#### 6.3 Performance Benchmarks

**Benchmarks:**

1. HTTP throughput (requests/sec vs uvicorn)
2. IPC latency (p50/p95/p99)
3. Memory usage under load
4. Worker scaling efficiency

**Files:** `benchmarks/`, `docs/benchmarks.md`

#### 6.4 Migration Guide

**Content:**

1. From uvicorn: config mapping, ASGI compatibility
2. From Gunicorn: worker model differences
3. From FastAPI: procedure vs route patterns

**File:** `docs/migration.md`

#### 6.5 Cookbook

**Content:**

1. Common patterns (FastAPI integration, Background Tasks)
2. Real-world examples (Chatbot, Data Processing)

**File:** `docs/cookbook.md`

### Phase 7: Code Quality & Polish

#### 7.1 Strict Typing & Anti-Patterns

**Current:** ~120 mypy errors, wide usage of `from __future__ import annotations`, `Optional` legacy syntax.

**Target:** Clean, strict-mode compliant codebase.

**Implementation:**

1. Remove `from __future__ import annotations` where not strictly required by circular refs
2. Modernize `Optional[T]` to `T | None`
3. Fix all `mypy` errors in `src/py`
4. Enforce strict type checking in CI

#### 7.2 API Surface Polish

**Current:** `vertebra/__init__.py` exposes minimal API.

**Target:** Top-level exports for drivers and storage.

**Implementation:**

1. Export `vertebra.drivers` and `vertebra.storage` symbols in root `__init__.py`
2. Ensure cleaner import paths for users

**Files:** `src/py/vertebra/__init__.py`

## Implementation Phases

### Milestone 1: Server Foundation (Weeks 1-2)

- [x] Request body streaming
- [x] HTTP disconnect propagation
- [x] Graceful shutdown
- [x] Basic metrics endpoint
- [x] Python `vertebra serve` CLI

### Milestone 2: Resource Management (Weeks 3-4)

- [ ] Pool fairness scheduler (FIFO + weighted)
- [ ] Lifecycle hooks
- [ ] Pool metrics

### Milestone 3: ORM Extensions (Weeks 5-7)

- [ ] Unified `VertebraDBConfig` with `provide_session` IPC wrapper
- [ ] SQLSpec `AsyncIPCDriver` + `SyncIPCDriver` adapters (all 16+ dialects)
- [ ] SQLSpec Arrow zero-copy via shared memory handles
- [ ] SQLSpec storage integration (`write_to_storage_async`)
- [ ] fsspec adapter (local backend) + optional S3/GCS tests (skipped unless configured)
- [ ] SQLAlchemy `vertebra+ipc://` dialect & PEP 249 DBAPI (defer to last)
- [ ] Advanced Alchemy `VertebraAlchemyPlugin` with File Object Storage (last)
- [ ] DB Node dialect-to-driver mapping (SQLSpec backend execution)
- [ ] Container integration tests (Postgres, MySQL, SQLite, DuckDB, Redis)

### Milestone 4: MCP Complete (Weeks 7-8)

- [ ] Full protocol implementation
- [ ] Transport abstraction (stdio, SSE, WebSocket)
- [ ] IPC bridge integration

### Milestone 5: Single-File Distribution (Week 9)

- [ ] Rust supervisor embedded in `vertebra-py`
- [ ] Bun engine compilation script
- [ ] `vertebra build --one-file` via `pyapp`
- [ ] Linux + macOS artifacts

### Milestone 6: Documentation (Week 10)

- [ ] Getting started guide
- [ ] API reference generation
- [ ] Benchmarks
- [ ] Migration guide
- [ ] Cookbook

### Milestone 7: Code Quality & Hardening (Week 11)

- [ ] Strict type audit (mypy)
- [ ] Anti-pattern cleanup
- [ ] API surface polish
- [ ] SSE support
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
| Code Quality | 0 mypy errors, 0 lint violations |

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Protobuf codegen complexity | Medium | Defer to post-MVP, keep MessagePack (use `betterproto` later) |
| MCP spec changes | Low | Pin to MCP 1.0, track updates |
| Container test flakiness | Medium | Retry logic, health waits |
| Performance regression | High | Benchmark CI gate |

## Decisions

1. **Protobuf timeline**: Post-MVP. MessagePack remains MVP; Protobuf will use `betterproto`.
2. **MCP transports**: Implement stdio + SSE + WebSocket (Bun runtime supported).
3. **SQLSpec/Advanced Alchemy versions**: Pin minimums to latest stable at implementation time.
4. **fsspec tests**: Local backend required; S3/GCS tests are optional and skipped/xfail unless configured.
5. **Single-file distribution targets**: Linux + macOS first; Windows later.
6. **Metrics library**: Use `prom-client` on Bun, align with Python `prometheus-client`.
7. **Tracing**: Map SQLSpec correlation IDs into unified trace context.

## Appendix

### A. File Inventory

**New Files:**

- `src/ts/src/server/metrics.ts`
- `src/ts/src/mcp/transport.ts`
- `src/ts/src/mcp/resources.ts`
- `src/ts/src/mcp/prompts.ts`
- `src/py/vertebra/resources/hooks.py`
- `src/py/vertebra/resources/metrics.py`
- `src/py/vertebra/extensions/__init__.py`
- `src/py/vertebra/extensions/_typing.py`
- `src/py/vertebra/extensions/config.py`
- `src/py/vertebra/extensions/sqlspec/__init__.py`
- `src/py/vertebra/extensions/sqlspec/adapter.py`
- `src/py/vertebra/extensions/sqlspec/config.py`
- `src/py/vertebra/extensions/sqlalchemy/__init__.py`
- `src/py/vertebra/extensions/sqlalchemy/dialect.py`
- `src/py/vertebra/extensions/sqlalchemy/dbapi.py`
- `src/py/vertebra/extensions/alchemy/__init__.py`
- `src/py/vertebra/extensions/alchemy/config.py`
- `src/py/vertebra/extensions/alchemy/plugin.py`
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

- `sqlspec` (optional) - for SQLSpec IPC adapter (16+ dialects supported)
- `sqlalchemy>=2.0` (optional) - for SQLAlchemy dialect
- `advanced-alchemy` (optional) - for AA Repository/Service patterns
- `pyarrow` (optional) - for zero-copy Arrow result transport
- `prometheus-client` (optional) - for metrics export
- `betterproto` (post-MVP) - for Protobuf IPC bindings

**TypeScript:**

- `prom-client` - metrics export

### C. Testing Strategy

1. **Unit tests**: All new modules (pytest, bun test)
2. **Integration tests**: Full IPC path with real databases (Postgres, MySQL, Redis)
3. **ORM compatibility tests**: SQLSpec adapter, SQLAlchemy dialect, Advanced Alchemy services
4. **Performance tests**: Benchmark suite with CI gate
5. **Protocol tests**: MCP compliance
