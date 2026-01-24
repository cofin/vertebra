# Recovery Guide: Server & Runtime

**PRD**: `/home/cody/code/c/vertebra/specs/active/server/prd.md`
**Tasks**: `/home/cody/code/c/vertebra/specs/active/server/tasks.md`
**Research**: `/home/cody/code/c/vertebra/specs/active/server/research/plan.md`

---

## Quick Context

This is a **consolidated PRD** combining Server & Runtime work for Vertebra 1.0. It covers:

1. **MCP Protocol Implementation** (75% of scope)
   - JSON-RPC 2.0 server with stdio, SSE, WebSocket transports
   - Python `@mcp_tool` decorator
   - IPC bridge routing tool calls via shared memory

2. **Pool Health & Lifecycle Management** (15% of scope)
   - Periodic health checks (30s)
   - Lifecycle hooks: on_acquire, on_release, on_error, on_timeout
   - Idle connection reaper (5 minutes)

3. **Unified Observability** (10% of scope)
   - Single `/__metrics__` endpoint
   - Prometheus format with 20+ metrics
   - Grafana dashboard

**Total Duration**: 11 weeks
**Total Tasks**: 80+
**Critical Path**: Phase 1 -> Phase 2 -> Phase 3 (MCP chain)

---

## Current State

### Completed
- Shared-memory broker operational (Bun process)
- Ring buffer IPC primitives (Rust C ABI)
- Python CFFI bindings to shared memory
- Basic MCP server class (15% complete)
- Connection pool implementations

### In Progress
- MCP stdio transport
- Tool registration and invocation

### Not Started
- `@mcp_tool` Python decorator
- SSE and WebSocket transports
- Pool health checks
- Lifecycle hooks
- Metrics endpoint

---

## Key Files

### TypeScript Implementation

**MCP Server** (existing, needs enhancement):
- `/home/cody/code/c/vertebra/src/ts/src/mcp/server.ts`

**MCP Transports** (all new):
- `/home/cody/code/c/vertebra/src/ts/src/mcp/transports/stdio.ts`
- `/home/cody/code/c/vertebra/src/ts/src/mcp/transports/sse.ts`
- `/home/cody/code/c/vertebra/src/ts/src/mcp/transports/websocket.ts`

**MCP Support** (all new):
- `/home/cody/code/c/vertebra/src/ts/src/mcp/ipc-bridge.ts`
- `/home/cody/code/c/vertebra/src/ts/src/mcp/schema-registry.ts`
- `/home/cody/code/c/vertebra/src/ts/src/mcp/metrics.ts`

**Broker Enhancement**:
- `/home/cody/code/c/vertebra/src/ts/src/broker/base-pool.ts` (add health checks + hooks)
- `/home/cody/code/c/vertebra/src/ts/src/broker/serve.ts` (implement health checks)
- `/home/cody/code/c/vertebra/src/ts/src/broker/metrics.ts` (NEW)

**Server Observability**:
- `/home/cody/code/c/vertebra/src/ts/src/server/metrics.ts` (NEW)
- `/home/cody/code/c/vertebra/src/ts/src/server/prometheus.ts` (NEW)

### Python Implementation

**MCP Module** (all new):
- `/home/cody/code/c/vertebra/src/py/vertebra/mcp/__init__.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/mcp/decorators.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/mcp/registry.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/mcp/schema.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/mcp/handler.py`
- `/home/cody/code/c/vertebra/src/py/vertebra/mcp/resources.py`

### Tests

**TypeScript**:
- `/home/cody/code/c/vertebra/src/ts/tests/integration/mcp.test.ts` (NEW)
- `/home/cody/code/c/vertebra/src/ts/tests/integration/pool-health.test.ts` (NEW)
- `/home/cody/code/c/vertebra/src/ts/tests/integration/metrics.test.ts` (NEW)
- `/home/cody/code/c/vertebra/src/ts/tests/unit/lifecycle-hooks.test.ts` (NEW)

**Python**:
- `/home/cody/code/c/vertebra/src/py/tests/test_mcp_integration.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_mcp_decorator.py` (NEW)
- `/home/cody/code/c/vertebra/src/py/tests/test_mcp_schema.py` (NEW)

---

## Architecture Principles

### 1. Zero-Socket Tool Invocation
MCP tool calls route through shared-memory IPC. Target <5ms P99 overhead.

### 2. Type-Safe Schema Generation
Python type hints -> JSON Schema via Type Bridge IR. Automatic, no manual schema definition.

### 3. Multi-Transport Support
- **stdio**: Claude Desktop (local subprocess)
- **SSE**: Browser clients (HTTP streaming)
- **WebSocket**: Bidirectional real-time

### 4. Health-Checked Pools
- Periodic checks every 30 seconds
- Pre-lease validation before returning connection
- Idle reaping after 5 minutes

### 5. Single Pane of Glass
`/__metrics__` endpoint aggregates all subsystem statistics in Prometheus format.

---

## Implementation Order

### Phase 1: MCP Foundation (Week 1-2)

**Tasks**:
1. Create stdio transport
2. Enhance MCP server (tools/list, tools/call, initialize)
3. Create schema registry
4. Integration test with Claude Desktop

**Start Here**:
```bash
cd /home/cody/code/c/vertebra
git checkout -b feat/server-mcp-foundation

# Task 1.1.1: Create stdio transport
mkdir -p src/ts/src/mcp/transports
touch src/ts/src/mcp/transports/stdio.ts
```

**Acceptance**:
- stdio transport works
- `tools/list` returns registered tools
- `tools/call` executes echo tool
- Integration test passes

---

### Phase 2: Python Integration (Week 3-4)

**Tasks**:
1. Create `@mcp_tool` decorator
2. Implement schema generation
3. Create IPC bridge
4. End-to-end test

**Start Here**:
```bash
mkdir -p src/py/vertebra/mcp
touch src/py/vertebra/mcp/__init__.py
touch src/py/vertebra/mcp/decorators.py
touch src/py/vertebra/mcp/registry.py
touch src/py/vertebra/mcp/schema.py
touch src/py/vertebra/mcp/handler.py
```

**Decorator Pattern**:
```python
from vertebra.mcp import mcp_tool

@mcp_tool()
async def query_database(query: str, limit: int = 100) -> list[dict]:
    """Execute a SQL query against the database."""
    async with get_pool("postgres").acquire() as conn:
        rows = await conn.fetch(query, limit)
        return [dict(row) for row in rows]
```

**Acceptance**:
- `@mcp_tool` decorator registers functions
- Schema generation works
- IPC bridge routes calls to Python
- End-to-end test passes

---

### Phase 3: Advanced MCP (Week 5-6)

**Tasks**:
1. SSE transport
2. WebSocket transport
3. Resource support
4. Progress notifications

**Start Here**:
```bash
touch src/ts/src/mcp/transports/sse.ts
touch src/ts/src/mcp/transports/websocket.ts
touch src/py/vertebra/mcp/resources.py
```

**Acceptance**:
- SSE transport works with browser clients
- WebSocket transport handles bidirectional messages
- Resources can be discovered and read
- Progress notifications work

---

### Phase 4: Pool Health Checks (Week 7-8)

**Tasks**:
1. Health check loop (30s interval)
2. Pre-lease and post-release validation
3. Health check metrics
4. Integration tests with real database

**Start Here**:
```bash
# Modify existing file
code src/ts/src/broker/base-pool.ts
```

**Health Checker Interface**:
```typescript
protected abstract runHealthCheck(): Promise<void>;

protected async runHealthCheck(): Promise<void> {
  const idleConnections = this.getIdleConnections();
  for (const conn of idleConnections) {
    try {
      await conn.execute("SELECT 1");
    } catch {
      await this.removeConnection(conn.token);
    }
  }
}
```

**Acceptance**:
- Health checks run every 30s
- Unhealthy connections removed
- Pre-lease validation catches bad connections
- Metrics exposed

---

### Phase 5: Pool Lifecycle Hooks (Week 8-9)

**Tasks**:
1. Define hook interface
2. Wire hooks into pool lifecycle
3. Implement idle reaper
4. Add lifecycle tests

**Hook Interface**:
```typescript
interface LifecycleHooks {
  onAcquire?: (token: string) => Promise<void>;
  onRelease?: (token: string) => Promise<void>;
  onError?: (token: string, error: Error) => Promise<void>;
  onTimeout?: (token: string) => Promise<void>;
}
```

**Acceptance**:
- Hooks called at correct lifecycle points
- Idle connections reaped after 5 minutes
- Metrics track lifecycle events

---

### Phase 6: Unified Observability (Week 9-10)

**Tasks**:
1. Create `/__metrics__` endpoint
2. Implement Prometheus formatter
3. Create metrics collectors
4. Grafana dashboard

**Start Here**:
```bash
touch src/ts/src/server/metrics.ts
touch src/ts/src/server/prometheus.ts
touch src/ts/src/broker/metrics.ts
touch src/ts/src/mcp/metrics.ts
```

**Acceptance**:
- `/__metrics__` returns all statistics
- Prometheus format valid
- 20+ metrics present
- Response time <50ms

---

### Phase 7: Production Hardening (Week 10-11)

**Tasks**:
1. Error handling audit
2. Documentation
3. Example applications
4. Performance benchmarks

**Acceptance**:
- All error cases handled
- Documentation complete
- Examples run successfully
- Performance targets met

---

## Testing Strategy

### Unit Tests

Run after each task group:
```bash
# TypeScript tests
make js-test

# Python tests
make test

# Single test
cd src/ts && bun test src/mcp/transports/stdio.test.ts
uv run pytest src/py/tests/test_mcp_decorator.py -v
```

### Integration Tests

Run after each phase:
```bash
# MCP integration
cd src/ts && bun test src/tests/integration/mcp.test.ts

# Pool health integration (requires Docker)
uv run pytest src/py/tests -m integration --integration
```

### Performance Benchmarks

Run after phase completion:
```bash
# Target metrics:
# - MCP tool invocation: P99 < 5ms
# - Pool acquisition: P99 < 2ms
# - Metrics endpoint: P99 < 50ms
# - Throughput: > 1000 tool calls/sec
```

---

## Common Commands

### Development

```bash
# Install dependencies
make install

# Format code
make lint

# Type checking
make mypy
make js-type-check

# Run all tests
make test-all
```

### MCP Testing

```bash
# Test with stdio
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | bun run src/ts/src/mcp/cli.ts

# Test with Claude Desktop
# Add to Claude Desktop config:
{
  "mcpServers": {
    "vertebra": {
      "command": "vertebra",
      "args": ["mcp"]
    }
  }
}
```

### Debugging

```bash
# Check shared memory
ls -lh /dev/shm/ | grep vertebra

# Clean stale shared memory
rm -rf /dev/shm/vertebra-*

# Monitor broker
cd src/ts && bun run src/broker/index.ts

# Tail logs
tail -f /var/log/vertebra/mcp.log
```

---

## Success Metrics

### Functional Requirements
- [ ] Claude Desktop can discover and call Vertebra tools
- [ ] All three transports (stdio, SSE, WebSocket) work
- [ ] Python `@mcp_tool` decorator generates correct schemas
- [ ] Health checks detect and remove bad connections within 30s
- [ ] `/__metrics__` endpoint returns aggregated statistics

### Quality Requirements
- [ ] 0 TypeScript type errors
- [ ] 0 Python mypy errors (strict mode)
- [ ] Test coverage > 85%

### Performance Requirements
- [ ] MCP Tool Invocation (P99): < 5ms
- [ ] Pool Acquisition (P99): < 2ms
- [ ] Metrics Endpoint (P99): < 50ms
- [ ] Throughput: > 1000 tool calls/sec

---

## Dependencies

**TypeScript/Bun**:
- Bun runtime (latest)
- No additional NPM packages (native JSON-RPC, WebSocket)

**Python**:
- `msgspec` - MessagePack serialization (already installed)
- `anyio` - Async compatibility (already installed)

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Type Bridge IR inadequate for schema generation | Extend IR with missing types early (Phase 2) |
| MCP spec changes | Lock to specific spec version, document |
| IPC overhead >5ms | Benchmark early, optimize hot paths |
| Health check false positives | Tune check frequency, add retry logic |
| Metrics endpoint performance | Cache for 5s, optimize collection |

---

## Related Specifications

- **Database & ORM**: `/home/cody/code/c/vertebra/specs/active/database/` - SQLSpec, SQLAlchemy integration
- **Archived Specs**: `/home/cody/code/c/vertebra/specs/archive/` - Previous PRDs consolidated into this spec

---

## Next Steps

1. **Choose Phase**: Start with Phase 1 (MCP Foundation) for new work
2. **Create Branch**: `git checkout -b feat/server-phase-N`
3. **Read PRD Section**: Review relevant phase in PRD
4. **Check Tasks**: Use tasks.md for detailed steps
5. **Write Tests First**: TDD for all new code
6. **Iterate**: Complete task groups, run tests, commit

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Ready for Implementation

**Quick Resume Command**:
```bash
cd /home/cody/code/c/vertebra
cat specs/active/server/recovery.md  # Read this file
cat specs/active/server/prd.md       # Read full PRD
cat specs/active/server/tasks.md     # Check task breakdown
```
