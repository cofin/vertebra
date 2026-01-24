# PRD: Production Readiness Consolidation

**Slug:** production-readiness
**Status:** In Progress
**Date:** 2026-01-18
**Author:** Cody (AI Assistant)
**Dependencies:** PRD-1, PRD-2

## 1. Introduction

Vertebra has established a high-performance core (Rust IPC) and a flexible language bridge (Python/TypeScript). However, it lacks the "Last Mile" features required for production adoption: robust server features (streaming, WebSockets), ecosystem integration (SQLSpec, fsspec), global resource management, and a polished developer experience.

This specification defines the roadmap to **Vertebra v1.0 (Release Candidate)**, focusing on stability, compatibility, and developer ergonomics.

## 2. Problem Statement

### 2.1 Functional Gaps
- **Server**: HTTP/2/HTTP/3 validation is pending. Protobuf IPC remains post-MVP.
- **Drivers**: Proprietary API prevents use with ORMs. `sqlspec` support is missing. `fsspec` support is unverified.
- **Resources**: Connection pooling needs full observability and test coverage.
- **MCP**: The AI interface is a skeleton, preventing self-hosting capabilities.

### 2.2 Quality Gaps
- **UX**: `vertebra serve` exists; packaging and distribution polishing remains.
- **Code**: Widespread legacy typing (`Optional`), `future` imports, and strict mode violations.
- **Docs**: No "Getting Started" or cookbook.

## 3. User Stories

1.  **As a Developer**, I want to run `vertebra serve app:app` so that I don't have to manage the Bun runtime manually.
2.  **As a Developer**, I want to use `sqlspec` or an ORM so that I don't have to rewrite my database layer.
3.  **As a Data Scientist**, I want to stream large files to S3 via `fsspec` so that I can process datasets efficiently.
4.  **As an AI Engineer**, I want Vertebra to expose my tools via MCP so that I can connect them to Claude/Gemini.
5.  **As an SRE**, I want graceful shutdown and metrics so that I can deploy safely to Kubernetes.

## 4. Technical Requirements

**See `design/` directory for detailed architecture specifications.**

### Phase 1: Server Foundation & CLI
**Design Spec**: [`design/server-architecture.md`](./design/server-architecture.md)

#### 1.0 Litestar Plugin
- **Class**: `VertebraPlugin`.
- **Integration**: Implements `CLIPluginProtocol` to inject `run` command.
- **Behavior**: Matches `litestar-granian` UX (drop-in replacement).

#### 1.1 Python CLI (`vertebra serve`) & Startup UX
- **Command**: `vertebra serve`.
- **Banner**: "Go Fast" style summary (ASCII art, port, worker count, pools) printed on startup.
- **Supervisor**: Python/Rust process manager handles the lifecycle.

#### 1.2 Request Streaming
- **IPC Frame**: `http.request` with `body` (bytes) and `more_body` (bool).
- **Mechanism**:
    - TypeScript: Read from `request.body` reader.
    - Python: `await receive()` yields body chunks.
    - Backpressure: IPC channel saturation blocks the sender.

#### 1.2a IPC Protocol (MVP vs Post-MVP)
- **MVP**: MessagePack remains the default IPC codec.
- **Post-MVP**: Protobuf IPC is planned but deferred; Python bindings use `betterproto`, TS uses `protobuf-es` (pending Bun support).

#### 1.3 Graceful Shutdown
- **Behavior**: On SIGTERM, stop accepting connections. Allow `drain_timeout` (default 5s) for active requests to complete.
- **IPC**: Send `lifespan.shutdown` to workers.

#### 1.4 Unified Observability
- **Metrics**: `GET /__metrics__` (Prometheus) aggregates Server + Broker + Worker stats.
- **Logging**: Structured JSON logs with trace ID propagation across IPC.
- **Tracing**: Accept `X-Request-ID`/`traceparent`, and propagate SQLSpec correlation IDs into the same trace context.
- **Implementation**: Use `prom-client` on the Bun side, align with Python `prometheus-client` naming.

### Phase 2: Resource Management (Broker)
**Design Spec**: [`design/resource-broker.md`](./design/resource-broker.md)

#### 2.1 Vertebra Broker Integration
- **Architecture**: `vertebra-broker` runs as a sidecar or child process.
- **Transport**: Shared-memory C ABI (CFFI in Python, bun:ffi in Bun); no sockets.
- **Fairness**: Implement FIFO and Weighted Fair Queuing in the broker.
- **Client**: `BrokerClient` (Python) connects via shared memory.

#### 2.2 Pool Observability
- **Metrics**: Pool size, wait time, lease duration.
- **Command**: `broker stats` (IPC).

### Phase 3: ORM Extensions & Drivers
**Design Spec**: [`design/driver-ecosystem.md`](./design/driver-ecosystem.md)

#### 3.1 Plugin-Based Registration
- **SQLSpec**: register provider plugins via `Vertebra(plugins=[SqlSpecProvider(...)])` or `app.register_plugin(...)`.
- **Advanced Alchemy**: plug in via provider plugins (last).
- **Proxy wrapper**: Client-side configs use a broker proxy wrapper to route through shared memory; provider processes run the original config.
- **Version baseline**: Pin minimums to the latest stable SQLSpec + Advanced Alchemy available at implementation time.

#### 3.2 SQLSpec Integration
- **All 16+ adapters**: asyncpg, psycopg, duckdb, oracledb, bigquery, etc.
- **Zero-copy Arrow**: Results via shared memory.
- **Storage passthrough**: `write_to_storage_async()` works transparently.

#### 3.3 Advanced Alchemy Integration
- **Repository/Service patterns**: Work unchanged with registered configs.
- **File Object Storage**: Routes through IPC to storage backends.
- **Litestar plugin**: Integrates with AA's Litestar plugin for DI.

#### 3.4 Plain SQLAlchemy (Exposed Drivers Only)
- **Dialects**: `vertebra+postgres://`, `vertebra+mysql://`, `vertebra+sqlite://`.
- **Limitation**: Can only use Vertebra's internal drivers (Bun postgres, Rust sqlx).
- **For custom adapters**: Use Advanced Alchemy provider plugins.

#### 3.5 Container Tests
- **Infra**: Use `tools/lib/container.py` for container fixtures.
- **Coverage**: SQLSpec + AA against real Postgres/MySQL/Redis.

#### 3.6 fsspec Integration (Local-first)
- **Adapter**: Implement an `fsspec` filesystem backed by `vertebra.storage`.
- **Tests**: Local backend only by default; S3/GCS tests are optional and skipped unless configured.

### Phase 4: MCP Implementation
**Design Spec**: [`design/mcp-integration.md`](./design/mcp-integration.md)

#### 4.1 Protocol
- **Features**: Resources, Prompts, Tools (List/Call).
- **Transports**: Stdio (CLI), SSE (Web), WebSocket (Bun native).

#### 4.2 Bridge
- **Routing**: `call_tool` -> IPC -> Python Procedure.
- **Validation**: Use Type Bridge IR for schema validation.

### Phase 5: Single-File Distribution & Supervisor
**Design Spec**: [`design/single-file-dist.md`](./design/single-file-dist.md)

#### 5.1 Architecture: "Unified Supervisor"
- **Wrapper**: `pyapp` creates the standalone executable.
- **Hook**: Python Entry Point invokes `vertebra.rust.init_supervisor()`.
- **Components**:
    - **Broker**: Embedded Rust thread (no separate binary).
    - **Engine**: Bundled Bun binary (spawned sidecar).
    - **App**: Python runtime.

#### 5.2 Build Command
- `vertebra build --one-file`: Orchestrates Bun compilation, Wheel building, and PyApp bundling.
- **Targets**: Linux + macOS for initial release; Windows later.

### Phase 6: Code Quality & Polish

#### 6.1 Strict Typing
- **Audit**: Run `mypy --strict src/py`.
- **Fixes**: Add missing return types, fix `Any` leaks.
- **Modernization**: `Optional[T]` -> `T | None`. Remove `from __future__`.

#### 6.2 API Exports
- **Root**: `vertebra.drivers`, `vertebra.storage`, `vertebra.connect`.

## 5. Acceptance Criteria

### Performance
- [ ] **Throughput**: >50k req/sec (Hello World).
- [ ] **Latency**: P99 < 1ms (IPC overhead).
- [ ] **Memory**: <50MB per idle worker.

### Functional
- [ ] `vertebra serve` starts the app.
- [ ] `curl -T large_file` works (Streaming).
- [ ] `Ctrl+C` triggers graceful shutdown.
- [ ] `sqlspec` adapter works with a sample query.
- [ ] `fsspec` adapter works with `pandas.read_csv` against local backend.
- [ ] `vertebra build` produces a working standalone binary.

### Quality
- [ ] `mypy` reports 0 errors.
- [ ] `ruff` reports 0 violations.
- [ ] Test coverage > 85%.

## 6. Milestones

1.  **M1: Server Core** (CLI, Streaming, Shutdown) - *Week 1*
2.  **M2: Ecosystem** (SQLSpec, fsspec, Container Tests) - *Week 2*
3.  **M3: Resources** (Broker, Pooling) - *Week 3*
4.  **M4: MCP Integration** (Agent, Protocol) - *Week 4*
5.  **M5: Distribution** (Single File, CI) - *Week 5*
6.  **M6: Polish** (Typing, Docs) - *Week 6*
