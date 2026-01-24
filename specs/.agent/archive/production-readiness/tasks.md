# Tasks: Production Readiness

**Master Task List**

This document tracks the high-level progress of the Production Readiness initiative. Detailed task breakdowns are available in the `phases/` directory.

## Phase 1: Server Completion
*Focus: Plugin, Streaming, WebSockets, Unified Metrics, Startup UX*
[Detailed Tasks](./phases/phase-1-server.md)

- [x] **P1.0**: Litestar Plugin
- [x] **P1.1 - P1.6**: Streaming & Protocols
- [x] **P1.7 - P1.12**: Unified Metrics & Logging
- [x] **P1.13 - P1.16**: Lifecycle & "Go Fast" CLI

## Phase 2: Resource Management
*Focus: Shared-memory broker, fairness, pooling*
[Detailed Tasks](./phases/phase-2-resources.md)
**Status:** Complete (2026-01-19).

- [x] **P2.1 - P2.4**: Broker Core
- [x] **P2.5 - P2.7**: Scheduler & Fairness
- [x] **P2.8 - P2.9**: Python Integration (CFFI client + SQLSpec provider)
- [x] **P2.10**: Generic `register_plugin()` API
- [x] **P2.11 - P2.12**: Observability

## Phase 3: ORM Extensions & Drivers
*Focus: SQLSpec (Sync/Async), broker-backed storage (fsspec), SQLAlchemy, Advanced Alchemy*
[Detailed Tasks](./phases/phase-3-drivers.md)
**Note:** SQLAlchemy dialect + Advanced Alchemy are deferred until the end of Phase 3.

- [ ] **P3.1**: Unified Config + registration helpers
- [x] **P3.2**: SQLSpec broker adapters (Postgres/MySQL/SQLite) + proxy wrapper
  - Implemented: `BrokerProxySyncConfig`, `BrokerProxyAsyncConfig`
  - Implemented: `BrokerProxySyncDriver`, `BrokerProxyAsyncDriver`
  - Dialect-specific configs in `postgres/`, `mysql/`, `sqlite/`
- [x] **P3.3**: SQLSpec provider host + integration tests
  - Implemented: `SqlSpecProvider` in `provider.py`
  - Handles: acquire, release, execute, execute_many, command, begin, commit, rollback
  - Arrow region pooling for large result sets
- [ ] **P3.4**: SQLAlchemy & AA (last)
- [ ] **P3.5**: DB Node & Tests
- [x] **P3.6**: Broker-backed fsspec Adapter (local backend wired; tests pending)
  - Implemented: `SyncFileSystemClient`, `AsyncFileSystemClient`
  - Routes all ops through broker via shared memory

## Phase 4: MCP Integration
*Focus: AI Agent Protocol*
[Detailed Tasks](./phases/phase-4-mcp.md)
**Status:** Stub Only - Core infrastructure exists but transports and registration not implemented

- [ ] **P4.1 - P4.5**: Bun Server MCP Support (Stdio, SSE, WebSocket)
  - `src/ts/src/mcp/server.ts` exists with basic `McpServer` class skeleton
  - `registerTool()` and JSON-RPC request handling implemented
  - Transports (Stdio, SSE, WebSocket) NOT implemented
- [ ] **P4.6 - P4.8**: Python Registration & IPC
  - `src/ts/src/mcp/ipc-bridge.ts` is a stub (throws "not configured")
- [ ] **P4.9 - P4.10**: Schema Generation

## Phase 5: Single-File Distribution & Supervisor
*Focus: PyApp, Bun Compile, Rust Supervisor*
[Detailed Tasks](./phases/phase-5-distribution.md)

- [ ] **P5.1 - P5.3**: Rust Supervisor & Broker Embedding
- [ ] **P5.4 - P5.5**: Engine Compilation
- [ ] **P5.6 - P5.7**: Build Tooling
- [ ] **P5.8**: CI/CD Artifacts

## Phase 6: Code Quality & Polish
*Focus: Typing, Anti-patterns, Docs*

- [ ] **P6.1**: Strict Type Audit (mypy)
- [ ] **P6.2**: Remove `from __future__` imports
- [ ] **P6.3**: Clean up API exports (`vertebra/__init__.py`)
- [ ] **P6.4**: Create "Cookbook" documentation
