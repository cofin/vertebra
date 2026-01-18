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
| PRD-3 Server | 70% | `src/ts/src/server/`, `src/ts/src/ipc/asgi.ts` |
| PRD-4 Drivers | 75% | `src/py/vertebra/drivers/`, `src/ts/src/drivers/` |
| PRD-5 Resources | 20% | `src/rs/vertebra-broker/`, `src/py/vertebra/resources/` |
| PRD-6 Integration | 5% | `docs/` |
| PRD-7 MCP | 10% | `src/ts/src/mcp/` |

## Where to Start

### Option A: Critical Path First
Start with tasks that unblock production deployment:
1. **Graceful Shutdown (T1.5.*)**: `src/ts/src/server/index.ts`, `worker-manager.ts`
2. **Metrics (T1.4.*)**: New `src/ts/src/server/metrics.ts`
3. **Pool Fairness (T2.1.*)**: `src/rs/vertebra-broker/src/pool_manager.rs`

### Option B: Feature Complete
Start with tasks that complete core functionality:
1. **Request Streaming (T1.1.*)**: `src/ts/src/ipc/asgi.ts`
2. **MCP Protocol (T4.1.*)**: `src/ts/src/mcp/server.ts`
3. **Lifecycle Hooks (T2.2.*)**: New `src/py/vertebra/resources/hooks.py`

### Option C: Testing Foundation
Start with tasks that improve test coverage:
1. **Container Tests (T3.2.*)**: New `src/py/tests/integration/`
2. **SQLSpec Adapter (T3.1.*)**: New `src/py/vertebra/drivers/sqlspec.py`

## Key Architectural Decisions

1. **IPC Protocol**: MessagePack for MVP (Protobuf deferred)
2. **MCP Transport**: stdio first, then HTTP SSE
3. **Pool Fairness**: FIFO default, weighted/priority optional
4. **Metrics**: Prometheus-compatible format

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
3. **No request body streaming** - body fully buffered
4. **No graceful shutdown** - SIGTERM kills immediately
5. **No metrics** - no observability infrastructure

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
