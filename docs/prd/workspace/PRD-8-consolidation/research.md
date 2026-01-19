# PRD-8 Consolidation Research

**Created:** 2026-01-18
**Status:** In Progress

## Overview

This document captures comprehensive research findings from analyzing all existing PRDs (PRD-0 through PRD-7) and the complete codebase across Python, TypeScript/Bun, and Rust implementations. The goal is to identify all outstanding work, technical debt, and gaps that must be addressed to achieve production readiness.

## Codebase Analysis Summary

### Language Distribution

| Language | Location | Lines of Code | Primary Purpose |
|----------|----------|---------------|-----------------|
| Python | `src/py/vertebra/` | ~7,144 | Procedures, types, drivers, server |
| TypeScript | `src/ts/src/` | ~3,663 | Bun HTTP server, IPC, MCP |
| Rust | `src/rs/` | ~4,535 | Core IPC, bindings, broker |

### Implementation Status by PRD

#### PRD-0: Foundation (100% Complete)
- Workspace structure finalized
- Build tooling operational (`make install`, `make test-all`)
- Python 3.12+ baseline enforced
- Cross-language lint/format pipeline active

#### PRD-1: Rust IPC Layer (100% Complete)
- Shared memory backends for Linux/macOS/Windows
- SPSC/MPMC ring buffers with backpressure
- Buffer pool + zero-copy channel
- Cross-platform sync primitives (futex/ulock/WaitOnAddress)
- PyO3 + napi-rs bindings complete

#### PRD-2: Python Bindings (95% Complete)
- `@procedure` decorator + registry
- Complete Type IR (primitive, array, object, union, literal, tuple, generic)
- TypeExtractor for all major type systems (msgspec, pydantic, dataclass, TypedDict)
- TypeScript generator with namespace grouping
- IPC Channel with zero-copy views
- **Gap:** Minor edge cases in discriminator unions

#### PRD-3: Bun Server (70% Complete)
**Completed:**
- Server config loader with env + file support
- Round-robin worker pool with per-worker IPC
- Worker manager with health checks + exponential backoff restart
- ASGI bridge with streaming + backpressure
- pathsend/zerocopysend file response support

**Outstanding:**
1. Protobuf IPC codegen and codec (MessagePack remains default)
2. Request body streaming to Python workers
3. `http.disconnect` propagation
4. SSE IPC bridging frames (WebSocket IPC frames implemented)
5. HTTP/2, HTTP/3, h2c upgrade validation
6. Graceful drain/rolling restart
7. Metrics wiring (latency, throughput, error rates)

#### PRD-4: Bun Drivers (75% Complete)
**Completed:**
- Driver protocol messages + MessagePack serialization
- DB-API base protocols + exception hierarchy
- Type conversion helpers (Postgres/MySQL/SQLite + JSON/UUID)
- IPC client retry + pooling
- Sync/Async driver classes for Postgres/MySQL/SQLite/Redis
- Filesystem IPC server + Python clients (local/S3/GCS)
- Object-store protocol with byte-range + streaming

**Outstanding:**
1. SQLSpec adapter layer for driver unification
2. Compliance test suites (DB-API, fsspec)
3. Integration test harness using container runtime
4. Driver connection lifecycle docs/examples

#### PRD-5: Resource Management (20% Complete)
**Completed:**
- Shared-memory C ABI library implemented
- Bun broker config parsing (TOML + env substitution)
- Shared-memory broker request framing
- Python broker client with msgpack codec
- Basic BrokerPool with async context manager

**Outstanding (Critical - 11 NotImplementedError stubs):**
1. Pool fairness scheduler (FIFO, weighted, priority)
2. Resource lifecycle hooks (on_acquire, on_release, on_validate)
3. Connection validation + health monitoring
4. Pool size autoscaling based on load
5. Lease timeout enforcement + eviction
6. Observability metrics (pool utilization, wait times, errors)
7. Distributed resource coordination
8. Transaction boundary awareness
9. Resource prewarming strategies
10. Graceful shutdown with drain
11. Cross-process pool sharing

#### PRD-6: Integration (5% Complete)
**Completed:**
- Architecture docs (`docs/arch.md`)
- Progress tracking (`docs/progress.md`)

**Outstanding (Almost entirely unstarted):**
1. End-to-end integration examples (Python + Bun + Rust)
2. Performance benchmarks (throughput, latency, memory)
3. Migration guides from uvicorn/gunicorn
4. API reference documentation
5. Deployment guides (Docker, Kubernetes, Cloud Run)
6. Troubleshooting runbooks
7. Security hardening checklist

#### PRD-7: MCP Integration (10% Complete)
**Completed:**
- MCP server skeleton with JSON-RPC handler
- Basic tool registration interface

**Outstanding:**
1. Full MCP protocol implementation (resources, prompts, roots)
2. Schema registry for tool input/output validation
3. Transport abstraction (stdio, HTTP SSE, WebSocket)
4. Integration with Type Bridge for schema generation
5. Tool discovery and documentation
6. Error handling and retry semantics
7. Connection management and lifecycle
8. Security model (capability-based access control)

## Critical Architectural Gaps

### 1. Protobuf IPC (PRD-3 Blocker)
The PRD specifies Protobuf for the IPC control plane, but only MessagePack exists. Required:
- Schema definitions in `src/ts/src/proto/`
- Code generation pipeline (buf, protobuf-ts)
- Codec implementation in TypeScript
- Python encoder/decoder
- Migration path from MessagePack

### 2. Resource Pool Fairness (PRD-5 Core)
The `pool_manager.rs` trait is 9 lines with no implementation:
```rust
pub trait PoolManager: Send + Sync {
    fn name(&self) -> &str;
    fn config(&self) -> &PoolConfig;
    fn health_check(&self) -> Result<(), BrokerError>;
}
```

Required: fairness algorithms (FIFO, weighted fair queue, priority queue), lease management, eviction policies.

### 3. MCP Tool Registry (PRD-7 Foundation)
Current skeleton (`src/ts/src/mcp/server.ts`) is 52 lines with basic JSON-RPC. Missing:
- Tool schema validation using Type Bridge IR
- Resource/prompt protocol handlers
- Transport negotiation
- Security context propagation

### 4. Observability Gap
Partial observability is in place (Bun JSON logs + Prometheus endpoint). Remaining gaps:
- Unified metrics aggregation (broker + worker stats)
- Worker pool utilization, health check timing, restart counters
- Resource broker metrics (lease duration, pool exhaustion, timeout rates)
- Driver layer metrics (query timing, connection pool stats)

## Performance Considerations

### Memory Management
1. **Zero-copy paths established:** Buffer pool + channel in Rust core
2. **Gap:** Python side still copies on msgspec decode in some paths
3. **Gap:** No memory pressure backpropagation from workers to server

### Latency
1. **Current:** Round-robin worker selection (no locality awareness)
2. **Gap:** No connection affinity for transaction-bound requests
3. **Gap:** No request prioritization in worker queue

### Throughput
1. **Current:** Per-worker IPC pools with streaming
2. **Gap:** No batching for small requests
3. **Gap:** No adaptive concurrency limits

## Developer Experience Gaps

### Missing Tooling
1. CLI for broker/worker management
2. Debug/inspect mode for IPC traffic
3. Local development hot-reload for driver changes
4. Performance profiling integration

### Documentation Gaps
1. No "getting started" guide
2. No architectural decision records (ADRs)
3. No runbook for common issues
4. No contribution guidelines

## Dependencies and Risk Assessment

### External Dependencies
| Dependency | Purpose | Risk Level |
|------------|---------|------------|
| Bun runtime | HTTP server | Low (stable, growing adoption) |
| PyO3 | Rust-Python FFI | Low (mature, well-maintained) |
| napi-rs | Rust-JS FFI | Low (mature, Bun-compatible) |
| msgspec | Python serialization | Low (fast, stable) |
| protobuf-ts | TS Protobuf | Medium (less adoption than protobuf-es) |

### Technical Debt
1. **DMA references** in comments (legacy, should be removed per CLAUDE.md)
2. **Stub implementations** across resource module (11 NotImplementedError)
3. **Incomplete error types** in driver exception hierarchy
4. **Missing type exports** in some TypeScript modules

## Prioritized Work Streams

### Stream A: Server Completion (PRD-3)
**Priority:** HIGH - Blocking production use
**Effort:** Medium
**Dependencies:** None

### Stream B: Resource Management (PRD-5)
**Priority:** HIGH - Core infrastructure
**Effort:** High
**Dependencies:** Broker crate foundations

### Stream C: Driver Polish (PRD-4)
**Priority:** MEDIUM - Usability improvement
**Effort:** Medium
**Dependencies:** Container runtime for tests

### Stream D: MCP Integration (PRD-7)
**Priority:** MEDIUM - AI workflow integration
**Effort:** High
**Dependencies:** Type Bridge, Transport layer

### Stream E: Integration & Docs (PRD-6)
**Priority:** LOW (can parallel)
**Effort:** Medium
**Dependencies:** All other streams for examples

## Conclusion

The Vertebra project has solid foundations (PRD-0, PRD-1, PRD-2) but significant work remains in:
1. **Server layer** - streaming, protocols, observability
2. **Resource management** - the entire fairness/lifecycle system
3. **MCP** - full protocol implementation
4. **Integration** - examples, benchmarks, documentation

The recommended approach is a phased consolidation PRD that addresses these gaps in dependency order, with clear milestones and acceptance criteria for production readiness.
