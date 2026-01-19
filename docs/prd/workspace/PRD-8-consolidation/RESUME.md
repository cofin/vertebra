# PRD-8 Implementation Resume Guide

## Quick Context

**PRD-8: Production Readiness Consolidation** addresses all outstanding work across PRDs 3-7 to achieve production readiness for Vertebra.

**Key Documents:**

- Main PRD: `docs/prd/PRD-8-production-readiness.md`
- Research: `docs/prd/workspace/PRD-8-consolidation/research.md`
- Tasks: `docs/prd/workspace/PRD-8-consolidation/tasks.md`

## Current State Summary

| Area | Status | Key Files |
|------|--------|-----------|
| PRD-3 Server | 90% | `src/ts/src/server/`, `src/ts/src/ipc/asgi.ts` |
| PRD-4 Drivers | 75% | `src/py/vertebra/drivers/`, `src/ts/src/drivers/` |
| PRD-5 Resources | 20% | `src/rs/vertebra-broker/`, `src/py/vertebra/resources/` |
| PRD-6 Integration | 5% | `docs/` |
| PRD-7 MCP | 10% | `src/ts/src/mcp/` |

## Where to Start

### Option A: Critical Path First

Start with tasks that unblock production deployment:

1. **Unified Metrics (T1.4.4–T1.4.7 + P1.8/P1.9)**: `src/ts/src/server/metrics.ts`
2. **Supervisor (P1.16)**: Python/Rust process manager
3. **Pool Fairness (T2.1.*)**: `src/rs/vertebra-broker/src/pool_manager.rs`

### Option B: Feature Complete

Start with tasks that complete core functionality:

1. **SSE Support (T1.3.7 + T1.3.9)**: `src/ts/src/ipc/asgi.ts`, `src/py/vertebra/server/worker.py`
2. **MCP Protocol (T4.1.*)**: `src/ts/src/mcp/server.ts`
3. **Lifecycle Hooks (T2.2.*)**: New `src/py/vertebra/resources/hooks.py`

### Option C: ORM Extensions (SQLSpec + Advanced Alchemy)

Start with tasks that enable database integration:

1. **Unified Config (T3.1.*)**: New `src/py/vertebra/extensions/config.py`
2. **SQLSpec IPC Adapter (T3.2.*)**: New `src/py/vertebra/extensions/sqlspec/adapter.py`
3. **SQLAlchemy Dialect (T3.3.*)**: Defer until final pass
4. **Advanced Alchemy Plugin (T3.4.*)**: Defer until final pass

### Option D: Testing Foundation

Start with tasks that improve test coverage:

1. **Container Tests (T3.7.*)**: New `src/py/tests/integration/`

## Key Architectural Decisions

1. **IPC Protocol**: MessagePack for MVP (Protobuf deferred)
2. **MCP Transport**: stdio + HTTP SSE + WebSocket (Bun native)
3. **Pool Fairness**: FIFO default, weighted/priority optional
4. **Metrics**: Prometheus-compatible format
5. **ORM Extensions**: Unified config with `provide_session` returning IPC-wrapped sessions
6. **SQLSpec Integration**: `AsyncIPCDriver` implementing `AsyncDriverAdapterBase`, supports all 16+ dialects
7. **SQLAlchemy Integration**: `vertebra+ipc://<pool>?dialect=<dialect>` URL scheme
8. **Advanced Alchemy**: Works via SQLAlchemy layer + File Object Storage hooks
9. **Arrow Transport**: Zero-copy via shared memory buffer handles
10. **Storage**: SQLSpec's `write_to_storage` and AA's File Object both route through IPC

## Commands Reference

```bash
# Install dependencies
make install

# Run all tests
make test-all

# Run specific tests
uv run pytest src/py/tests/
bun test
cargo test --workspace

# Lint and format
make lint

# Build Rust crates
make rs-build
```

## Known Issues to Address

1. **pool_manager.rs** is 9 lines - needs full implementation
2. **IpcBridge.callTool()** throws "not configured" - needs implementation
3. **Unified metrics aggregation** missing (broker + worker stats)
4. **Supervisor/process manager** missing
5. **SSE support** missing
6. **No ORM extensions** - SQLSpec, SQLAlchemy, Advanced Alchemy integration missing
7. **No dialect-to-driver mapping** in DB Node for real query execution
8. **No Arrow zero-copy** result transport implemented
9. **No storage integration** for query results via IPC

## File Locations Quick Reference

### TypeScript (Bun)

- Server: `src/ts/src/server/`
- IPC: `src/ts/src/ipc/`
- MCP: `src/ts/src/mcp/`
- Drivers: `src/ts/src/drivers/`
- Tests: `src/ts/src/**/*.test.ts`

### Python

- Procedures: `src/py/vertebra/procedures/`
- Types: `src/py/vertebra/types/`
- Drivers: `src/py/vertebra/drivers/`
- Resources: `src/py/vertebra/resources/`
- Server: `src/py/vertebra/server/`
- Extensions: `src/py/vertebra/extensions/` (NEW - SQLSpec, SQLAlchemy, AA)
- Tests: `src/py/tests/`

### Rust

- Core: `src/rs/vertebra-core/`
- Python bindings: `src/rs/vertebra-py/`
- JS bindings: `src/rs/vertebra-js/`
- Broker: `src/rs/vertebra-broker/`

## Testing Strategy

1. **Unit tests**: pytest (Python), bun test (TS), cargo test (Rust)
2. **Integration tests**: `src/py/tests/integration/` (requires containers)
3. **IPC tests**: `src/ts/src/ipc/*.integration.test.ts`

## Progress Tracking

Update `docs/progress.md` after completing milestones:

- Mark tasks complete with date
- Add verification notes
- Document any deviations from PRD

## Contact Points

- Architecture: `docs/arch.md`
- Conventions: `CLAUDE.md`
- Progress: `docs/progress.md`
