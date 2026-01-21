# PRD: Server & Runtime - MCP, Health Checks, and Observability

**Status**: Active
**Version**: 1.0
**Created**: 2026-01-19
**Dependencies**: PRD-1 (Rust IPC), PRD-2 (Type Bridge), PRD-5 (Resource Management)
**Consolidates**: PRD-7 (MCP Integration), Production Readiness (Health Checks), Observability

---

## Related Documents

- **[Index](./index.md)**: Overview and navigation
- **[Tasks](./tasks.md)**: Detailed task breakdown with checkboxes
- **[Recovery](./recovery.md)**: Session resumption guide
- **[Research](./research/plan.md)**: Research notes and best practices

---

## Executive Summary

### Problem Statement

Vertebra has a high-performance foundation (shared-memory IPC, TypeScript/Bun server, Python workers) but lacks critical production features that enable AI integration, operational monitoring, and resilient connection management. Specifically:

1. **AI Integration Gap**: No Model Context Protocol (MCP) implementation prevents AI assistants like Claude Desktop from discovering and invoking Vertebra tools
2. **Pool Reliability Gap**: Connection pools lack health checks, lifecycle hooks, and idle management, risking stale connections and resource leaks
3. **Observability Gap**: No unified metrics endpoint; operators cannot monitor pool health, request latency, or system throughput

These gaps block production adoption and limit Vertebra's value proposition as an AI-ready application backbone.

### Solution Overview

This PRD consolidates three complementary systems into a unified Server & Runtime layer:

**1. MCP Protocol Implementation** (75% of scope)
- JSON-RPC 2.0 server with three transports: stdio (local), SSE (web), WebSocket (bidirectional)
- Python `@mcp_tool` decorator for registering tools with automatic schema generation
- IPC bridge routing tool calls from Bun MCP server to Python workers via shared memory
- Resource and prompt support for rich AI interactions

**2. Pool Health & Lifecycle Management** (15% of scope)
- Periodic health checks (every 30s) with automatic bad connection removal
- Lifecycle hooks: `on_acquire`, `on_release`, `on_error`, `on_timeout`
- Idle connection reaper (configurable timeout, default 5 minutes)
- Pre-lease validation to catch bad connections before use

**3. Unified Observability** (10% of scope)
- Single `/__metrics__` endpoint aggregating broker + server statistics
- Prometheus format with 20+ metrics (pool health, acquisition latency, throughput)
- Structured logging with trace ID propagation across IPC boundaries
- Grafana dashboard JSON for operational visibility

**Architecture Overview**:

```
┌─────────────────────────────────────────┐
│    AI Assistants (Claude/Gemini)       │
│    via MCP Protocol (stdio/SSE/WS)     │
└────────────────┬────────────────────────┘
                 │ JSON-RPC 2.0
                 ▼
┌─────────────────────────────────────────┐
│      Bun MCP Server (TypeScript)        │
│  ┌─────────────────────────────────┐   │
│  │ Transports: stdio, SSE, WS      │   │
│  │ JSON-RPC Handler                │   │
│  │ Schema Registry (from Type IR)  │   │
│  └─────────────┬───────────────────┘   │
└────────────────┼───────────────────────┘
                 │ Shared-Memory IPC
                 ▼
┌─────────────────────────────────────────┐
│      Python Workers (Procedures)        │
│  ┌─────────────────────────────────┐   │
│  │ @mcp_tool decorated functions   │   │
│  │ Procedure Registry              │   │
│  │ Type Bridge IR                  │   │
│  └─────────────┬───────────────────┘   │
└────────────────┼───────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│     Connection Pools (with Health)      │
│  PostgreSQL │ MySQL │ Redis │ Storage   │
│  ├─ Health Check Loop (30s)             │
│  ├─ Lifecycle Hooks                     │
│  ├─ Idle Reaper (5min)                  │
│  └─ Metrics (/__metrics__)              │
└─────────────────────────────────────────┘
```

### Key Innovations

1. **Zero-Overhead MCP Bridge**: Tool invocation via shared memory (target <5ms P99) instead of HTTP/sockets
2. **Type-Safe Schema Generation**: Python type hints → JSON Schema via Type Bridge IR (automatic)
3. **Intelligent Health Management**: Pre-lease + periodic + post-release validation catches connection issues
4. **Single Pane of Glass**: `/__metrics__` aggregates all subsystem statistics in one Prometheus endpoint
5. **Hot Reload Capable**: Tool/resource changes reflected immediately without server restart

### Success Criteria

| Metric | Target | Rationale |
|--------|--------|-----------|
| MCP Tool Invocation Latency (P99) | < 5ms | Shared-memory IPC overhead |
| Health Check Coverage | 100% | All pools validated every 30s |
| Connection Leak Detection | < 30s | Automated reaping of leaked connections |
| Metrics Endpoint Response Time | < 50ms | Operational queries must be fast |
| Claude Desktop Integration | Works | End-to-end AI assistant validation |
| Type Coverage | 80%+ | Common Python types → JSON Schema |

### Estimated Effort

**Total Duration**: 11 weeks (2.75 months)

- Phase 1: MCP Foundation (2 weeks) - stdio transport, JSON-RPC handler
- Phase 2: Python Integration (2 weeks) - @mcp_tool decorator, schema generation
- Phase 3: Advanced MCP (2 weeks) - SSE/WebSocket, resources, logging
- Phase 4: Pool Health (1.5 weeks) - Health checks, validation
- Phase 5: Pool Lifecycle (1 week) - Hooks, idle reaper
- Phase 6: Observability (1 week) - Unified metrics endpoint
- Phase 7: Hardening (1.5 weeks) - Error handling, docs, examples

---

## Background & Context

### What is MCP?

The **Model Context Protocol (MCP)** is an open standard developed by Anthropic for integrating AI assistants with external tools and data sources. MCP enables:

- **Tool Discovery**: AI assistants discover available tools at runtime
- **Type-Safe Invocation**: Tools are invoked with validated parameters (JSON Schema)
- **Resource Access**: Structured access to data entities via URI templates
- **Bidirectional Communication**: Servers can send progress updates and logs

**MCP Wire Format**: JSON-RPC 2.0

**Example Tool Invocation**:
```json
// Request (from Claude Desktop)
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "query_database",
    "arguments": {
      "query": "SELECT * FROM users WHERE id = 123"
    }
  }
}

// Response (from Vertebra)
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"id\": 123, \"name\": \"Alice\", \"email\": \"alice@example.com\"}"
      }
    ]
  }
}
```

**MCP Transports**:

1. **stdio**: Standard input/output for local subprocess communication (Claude Desktop)
2. **SSE (Server-Sent Events)**: HTTP-based streaming for browser clients
3. **WebSocket**: Bidirectional for real-time applications

### Why Vertebra's MCP Implementation is Unique

| Aspect | Traditional MCP Servers | Vertebra MCP |
|--------|-------------------------|--------------|
| Protocol Handling | Python (json module) | Bun (native JSON) |
| Tool Execution | Same process | IPC to Python workers |
| Connection Pools | Per-server instance | Shared across workers |
| Type Generation | Manual or runtime | Static analysis (Type Bridge IR) |
| Performance | Variable | Target <5ms tool invocation overhead |

### Connection Pool Health Management

**Current State**: Vertebra's `BasePoolAdapter` tracks statistics (`acquires_total`, `releases_total`, `errors_total`) but lacks:
- Periodic health checks (connections can go stale)
- Lifecycle hooks for custom validation/logging
- Idle connection management (connections held indefinitely)

**Production Requirements**:
- **Health Checks**: Validate connections every 30 seconds with `SELECT 1` (Postgres), `PING` (Redis)
- **Lifecycle Hooks**: Allow custom logic at acquire/release/error/timeout events
- **Idle Reaper**: Close connections idle >5 minutes to free resources

### Observability Requirements

**Current State**: Pools expose `.stats()` but no unified endpoint.

**Production Needs**:
- **Metrics Aggregation**: Single `/__metrics__` endpoint combining broker + server stats
- **Prometheus Format**: Standard format for Grafana/Prometheus integration
- **Key Metrics**:
  - Pool utilization (idle, in_use, total)
  - Acquisition latency (P50, P90, P99)
  - Health check success/failure rates
  - Tool invocation counts and latency
  - IPC throughput and errors

---

## Architecture

### System Components

#### 1. MCP Server (Bun/TypeScript)

**Location**: `src/ts/src/mcp/`

**Responsibilities**:
- Accept connections via stdio/SSE/WebSocket transports
- Parse and route JSON-RPC 2.0 requests
- Maintain tool/resource registry (generated from Python type hints)
- Forward tool calls to Python workers via IPC bridge
- Handle errors and format responses

**Key Files**:
- `server.ts` - Core MCP server class (currently ~15% complete)
- `transports/stdio.ts` - stdio transport (NEW)
- `transports/sse.ts` - Server-Sent Events transport (NEW)
- `transports/websocket.ts` - WebSocket transport (NEW)
- `ipc-bridge.ts` - Shared-memory bridge to Python (NEW)
- `schema-registry.ts` - Tool/resource schemas (NEW)

**Current State** (`src/ts/src/mcp/server.ts`):
```typescript
export class McpServer {
  private tools = new Map<string, McpTool>();

  registerTool(tool: McpTool): void {
    this.tools.set(tool.name, tool);
  }

  async handle(request: JsonRpcRequest): Promise<JsonRpcResponse> {
    const tool = this.tools.get(request.method);
    if (!tool) {
      return {
        jsonrpc: "2.0",
        id: request.id ?? null,
        error: { code: -32601, message: "Method not found" },
      };
    }
    try {
      const result = await tool.handler(request.params);
      return { jsonrpc: "2.0", id: request.id ?? null, result };
    } catch (error) {
      return {
        jsonrpc: "2.0",
        id: request.id ?? null,
        error: { code: -32000, message: "Tool execution failed", data: String(error) },
      };
    }
  }
}
```

**Enhancements Needed**:
- Transport layer integration
- Schema registry for tools/resources
- IPC bridge implementation
- MCP protocol method routing (`tools/list`, `tools/call`, `resources/list`, `resources/read`)

#### 2. Python MCP Integration

**Location**: `src/py/vertebra/mcp/`

**Responsibilities**:
- `@mcp_tool` decorator for marking functions as MCP tools
- Procedure registry for storing tool metadata
- Type extraction via Type Bridge IR
- IPC handler for receiving tool invocation requests from Bun

**API Design**:

```python
from vertebra.mcp import mcp_tool
from vertebra.resources import get_pool

@mcp_tool()
async def query_database(query: str, limit: int = 100) -> list[dict]:
    """Execute a SQL query against the database.

    Args:
        query: SQL query string (SELECT only)
        limit: Maximum number of rows to return

    Returns:
        List of row dictionaries
    """
    async with get_pool("postgres").acquire() as conn:
        rows = await conn.fetch(query, limit)
        return [dict(row) for row in rows]

@mcp_tool(name="analyze_user")
async def analyze_user_activity(user_id: int) -> dict:
    """Analyze user activity patterns.

    Args:
        user_id: User ID to analyze

    Returns:
        Analysis results including activity count, last seen, etc.
    """
    async with get_pool("postgres").acquire() as conn:
        result = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as activity_count,
                MAX(timestamp) as last_seen,
                AVG(duration) as avg_duration
            FROM activity_log
            WHERE user_id = $1
            """,
            user_id
        )
        return dict(result)
```

**Generated JSON Schema** (automatic from type hints):
```json
{
  "name": "query_database",
  "description": "Execute a SQL query against the database.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "SQL query string (SELECT only)"
      },
      "limit": {
        "type": "integer",
        "default": 100,
        "description": "Maximum number of rows to return"
      }
    },
    "required": ["query"]
  }
}
```

#### 3. Pool Health Management (TypeScript)

**Location**: `src/ts/src/broker/`

**Enhancement to `BasePoolAdapter`**:

```typescript
export abstract class BasePoolAdapter implements PoolAdapter {
  // Existing fields...
  protected _healthCheckInterval?: NodeJS.Timer;
  protected _idleReaperInterval?: NodeJS.Timer;

  // NEW: Lifecycle hooks
  protected _onAcquire?: (token: string) => Promise<void>;
  protected _onRelease?: (token: string) => Promise<void>;
  protected _onError?: (token: string, error: Error) => Promise<void>;
  protected _onTimeout?: (token: string) => Promise<void>;

  // NEW: Start health check loop (call in constructor)
  protected startHealthCheck(intervalMs: number = 30000): void {
    this._healthCheckInterval = setInterval(async () => {
      await this.runHealthCheck();
    }, intervalMs);
  }

  // NEW: Abstract health check method (implemented by subclasses)
  protected abstract runHealthCheck(): Promise<void>;

  // NEW: Start idle reaper (call in constructor)
  protected startIdleReaper(timeoutMs: number = 300000): void {
    this._idleReaperInterval = setInterval(async () => {
      await this.reapIdleConnections(timeoutMs);
    }, timeoutMs);
  }

  // NEW: Abstract idle reaper method
  protected abstract reapIdleConnections(timeoutMs: number): Promise<void>;
}
```

**Concrete Implementation Example** (`SqlPool`):

```typescript
export class SqlPool extends BasePoolAdapter {
  private pool: Pool; // Bun's native postgres client
  private lastUsed = new Map<string, number>(); // Track idle time

  constructor(config: SqlPoolConfig) {
    super(config.maxSize);
    this.pool = new Pool({...config});
    this.startHealthCheck(30000); // Check every 30s
    this.startIdleReaper(300000); // Reap after 5min idle
  }

  protected async runHealthCheck(): Promise<void> {
    const tokens = this.getIdleTokens();
    for (const token of tokens) {
      try {
        const conn = this.getConnection(token);
        await conn.query('SELECT 1');
        // Connection healthy
      } catch (error) {
        console.warn(`Health check failed for token ${token}:`, error);
        await this.removeConnection(token); // Remove bad connection
        this.recordError();
      }
    }
  }

  protected async reapIdleConnections(timeoutMs: number): Promise<void> {
    const now = Date.now();
    for (const [token, lastUsedTime] of this.lastUsed.entries()) {
      if (now - lastUsedTime > timeoutMs) {
        console.info(`Reaping idle connection ${token}`);
        await this.release(token);
        this.lastUsed.delete(token);
      }
    }
  }

  async acquire(): Promise<string> {
    const token = await super.acquire();
    if (this._onAcquire) {
      await this._onAcquire(token);
    }
    return token;
  }

  async release(token: string): Promise<void> {
    this.lastUsed.set(token, Date.now());
    if (this._onRelease) {
      await this._onRelease(token);
    }
    await super.release(token);
  }
}
```

#### 4. Unified Observability (TypeScript + Python)

**Location**: `src/ts/src/server/metrics.ts` (NEW)

**Metrics Endpoint**:

```typescript
// GET /__metrics__
export async function metricsHandler(req: Request): Promise<Response> {
  const metrics = await collectAllMetrics();
  const prometheusText = formatPrometheus(metrics);
  return new Response(prometheusText, {
    headers: { 'Content-Type': 'text/plain; version=0.0.4' }
  });
}

async function collectAllMetrics(): Promise<MetricsSnapshot> {
  // Aggregate from multiple sources
  const brokerMetrics = await collectBrokerMetrics(); // Pool stats
  const serverMetrics = collectServerMetrics(); // HTTP request stats
  const mcpMetrics = await collectMcpMetrics(); // Tool invocation stats

  return {
    ...brokerMetrics,
    ...serverMetrics,
    ...mcpMetrics,
  };
}
```

**Prometheus Format Output**:
```
# HELP pool_connections_total Total connections by pool and state
# TYPE pool_connections_total gauge
pool_connections_total{pool="postgres",state="idle"} 8
pool_connections_total{pool="postgres",state="in_use"} 12
pool_connections_total{pool="redis",state="idle"} 3
pool_connections_total{pool="redis",state="in_use"} 2

# HELP pool_acquisition_duration_seconds Connection acquisition latency
# TYPE pool_acquisition_duration_seconds histogram
pool_acquisition_duration_seconds_bucket{pool="postgres",le="0.001"} 1523
pool_acquisition_duration_seconds_bucket{pool="postgres",le="0.005"} 1842
pool_acquisition_duration_seconds_bucket{pool="postgres",le="0.01"} 1891
pool_acquisition_duration_seconds_sum{pool="postgres"} 3.456
pool_acquisition_duration_seconds_count{pool="postgres"} 1891

# HELP mcp_tool_invocations_total Tool invocation count
# TYPE mcp_tool_invocations_total counter
mcp_tool_invocations_total{tool="query_database",status="success"} 523
mcp_tool_invocations_total{tool="analyze_user",status="success"} 142
mcp_tool_invocations_total{tool="query_database",status="error"} 7

# HELP mcp_tool_duration_seconds Tool execution duration
# TYPE mcp_tool_duration_seconds histogram
mcp_tool_duration_seconds_bucket{tool="query_database",le="0.01"} 412
mcp_tool_duration_seconds_bucket{tool="query_database",le="0.05"} 508
mcp_tool_duration_seconds_bucket{tool="query_database",le="0.1"} 523
```

---

## Implementation Phases

### Phase 1: MCP Foundation (Week 1-2)

**Objectives**:
- Implement stdio transport for local MCP communication
- Enhance JSON-RPC handler for MCP protocol methods
- Create basic tool discovery and execution flow
- Integration test with Claude Desktop

**Tasks**:

1. **Create stdio transport** (`src/ts/src/mcp/transports/stdio.ts`)
   - Read JSON-RPC from `process.stdin` line-by-line
   - Write responses to `process.stdout`
   - Handle connection lifecycle (startup, shutdown)
   - Error handling for malformed JSON

2. **Enhance MCP server** (`src/ts/src/mcp/server.ts`)
   - Add `tools/list` method handler (return registered tools)
   - Add `tools/call` method handler (invoke tool by name)
   - Add `initialize` method handler (MCP handshake)
   - Implement MCP error codes (-32600 to -32603)

3. **Create schema registry** (`src/ts/src/mcp/schema-registry.ts`)
   - Store tool schemas (name, description, inputSchema)
   - Load schemas from JSON file (generated by Python)
   - Hot reload on file change (watch mode)

4. **Integration test** (`src/ts/tests/integration/mcp.test.ts`)
   - Start MCP server with stdio transport
   - Send `tools/list` request
   - Send `tools/call` request for echo tool
   - Verify responses match MCP protocol spec

**Deliverables**:
- [ ] stdio transport working
- [ ] `tools/list` returns registered tools
- [ ] `tools/call` executes echo tool
- [ ] Integration test passes

**Acceptance Criteria**:
- Claude Desktop can connect via stdio
- `echo "test" | vertebra-mcp` returns tool list
- Tool invocation completes in <10ms

**Files Created/Modified**:
- `src/ts/src/mcp/transports/stdio.ts` (NEW)
- `src/ts/src/mcp/schema-registry.ts` (NEW)
- `src/ts/src/mcp/server.ts` (enhance)
- `src/ts/tests/integration/mcp.test.ts` (NEW)

---

### Phase 2: Python Integration (Week 3-4)

**Objectives**:
- Create `@mcp_tool` decorator for Python functions
- Build procedure registry for tool metadata
- Generate JSON schemas from Type Bridge IR
- Implement IPC bridge for tool invocation

**Tasks**:

1. **Create MCP decorator** (`src/py/vertebra/mcp/decorators.py`)
   - `@mcp_tool(name?, description?)` decorator
   - Extract function signature via Type Bridge `TypeExtractor`
   - Register tool in global `ProcedureRegistry`
   - Support async functions

2. **Procedure registry** (`src/py/vertebra/mcp/registry.py`)
   - `ProcedureRegistry` singleton class
   - `register_tool(name, handler, params, return_type)` method
   - `get_tool(name) -> Tool` method
   - `list_tools() -> list[Tool]` method
   - Export to JSON for Bun schema registry

3. **Schema generator** (`src/py/vertebra/mcp/schema.py`)
   - `MCPSchemaGenerator` class
   - `generate_tool_schema(tool: Tool) -> dict` method
   - Convert `TypeNode` to JSON Schema
   - Support common types: str, int, float, bool, list, dict, Optional, Union, Literal
   - Support msgspec.Struct and pydantic.BaseModel

4. **IPC bridge** (`src/ts/src/mcp/ipc-bridge.ts`)
   - `MCPIPCBridge` class
   - `callTool(name: string, args: unknown) -> Promise<unknown>` method
   - Serialize to MessagePack
   - Send via shared-memory IPC to Python worker
   - Deserialize response
   - Handle errors (timeout, execution failure)

5. **Python IPC handler** (`src/py/vertebra/mcp/handler.py`)
   - Listen for tool invocation requests on shared-memory channel
   - Look up tool in registry
   - Invoke handler with deserialized arguments
   - Serialize result and send back
   - Error handling and logging

6. **Integration test** (`src/py/tests/test_mcp_integration.py`)
   - Register test tool with `@mcp_tool`
   - Start Bun MCP server
   - Send tool invocation via stdio
   - Verify Python handler executed and returned correct result

**Deliverables**:
- [ ] `@mcp_tool` decorator working
- [ ] Schema generation from type hints
- [ ] IPC bridge routes calls to Python
- [ ] End-to-end tool invocation working

**Acceptance Criteria**:
- Python function with `@mcp_tool` appears in `tools/list`
- Tool invocation executes Python code
- Complex types (Struct, Optional) generate correct schemas
- IPC overhead <5ms (P99)

**Files Created/Modified**:
- `src/py/vertebra/mcp/__init__.py` (NEW)
- `src/py/vertebra/mcp/decorators.py` (NEW)
- `src/py/vertebra/mcp/registry.py` (NEW)
- `src/py/vertebra/mcp/schema.py` (NEW)
- `src/py/vertebra/mcp/handler.py` (NEW)
- `src/ts/src/mcp/ipc-bridge.ts` (NEW)
- `src/py/tests/test_mcp_integration.py` (NEW)

---

### Phase 3: Advanced MCP Features (Week 5-6)

**Objectives**:
- Implement SSE transport for web clients
- Implement WebSocket transport for bidirectional communication
- Add resource support (`resources/list`, `resources/read`)
- Implement progress notifications and logging

**Tasks**:

1. **SSE transport** (`src/ts/src/mcp/transports/sse.ts`)
   - HTTP endpoint accepting POST for requests
   - EventSource stream for responses
   - Keep-alive handling
   - Multiple concurrent clients

2. **WebSocket transport** (`src/ts/src/mcp/transports/websocket.ts`)
   - Bun native WebSocket support
   - JSON-RPC over WebSocket frames
   - Connection lifecycle management
   - Ping/pong keepalive

3. **Resource support** (`src/py/vertebra/mcp/resources.py`)
   - `@mcp_resource(uri_template: str)` decorator
   - URI template parsing (e.g., `users://{user_id}/profile`)
   - `resources/list` handler in Bun
   - `resources/read` handler with URI resolution

4. **Progress notifications** (`src/ts/src/mcp/server.ts`)
   - `notifications/progress` method
   - Progress token tracking
   - Python API: `yield_progress(token, current, total)`
   - Stream progress to client during long operations

5. **Logging integration** (`src/ts/src/mcp/server.ts`)
   - `notifications/message` method
   - Log level support (debug, info, warn, error)
   - Python logs forwarded to MCP client
   - Integration with existing Vertebra logging

6. **Example applications**:
   - Database query tool (with progress for large queries)
   - File search tool (with resource support for file contents)
   - System info tool (demonstrating multiple tool types)

**Deliverables**:
- [ ] SSE transport working with browser clients
- [ ] WebSocket transport working
- [ ] Resources can be discovered and read
- [ ] Progress notifications sent during long operations
- [ ] Example tools demonstrating all features

**Acceptance Criteria**:
- Browser client connects via SSE
- WebSocket client receives bidirectional messages
- `resources/list` returns available resources
- Long-running tool sends progress updates
- Logs appear in MCP client console

**Files Created/Modified**:
- `src/ts/src/mcp/transports/sse.ts` (NEW)
- `src/ts/src/mcp/transports/websocket.ts` (NEW)
- `src/py/vertebra/mcp/resources.py` (NEW)
- `src/ts/src/mcp/server.ts` (enhance for progress/logging)
- `examples/mcp-tools/` (NEW directory)

---

### Phase 4: Pool Health Checks (Week 7-8)

**Objectives**:
- Implement periodic health check loop
- Add pre-lease and post-release validation
- Emit health check metrics
- Test with real database connections

**Tasks**:

1. **Health check loop** (`src/ts/src/broker/base-pool.ts`)
   - Add `startHealthCheck(intervalMs)` method
   - Abstract `runHealthCheck()` method (implemented by subclasses)
   - Track last health check time per connection
   - Remove unhealthy connections from pool

2. **Pre-lease validation** (`src/ts/src/broker/base-pool.ts`)
   - Quick health check before returning connection
   - Retry with different connection if validation fails
   - Log validation failures

3. **Post-release validation** (`src/ts/src/broker/base-pool.ts`)
   - Validate connection on return to pool
   - Remove and replace if invalid
   - Prevent bad connections from re-entering pool

4. **Implement for SqlPool** (`src/ts/src/broker/serve.ts`)
   - Health check: `SELECT 1`
   - Track query success/failure
   - Handle connection errors gracefully

5. **Implement for RedisPool** (`src/ts/src/broker/serve.ts`)
   - Health check: `PING`
   - Handle connection timeout
   - Reconnect on failure

6. **Health check metrics** (`src/ts/src/broker/base-pool.ts`)
   - `health_checks_total{pool, result="success|failure"}`
   - `connection_removals_total{pool, reason="health_check|timeout|error"}`
   - `health_check_duration_seconds{pool}`

7. **Integration tests** (`src/ts/tests/integration/pool-health.test.ts`)
   - Start Postgres container
   - Acquire connection, kill database, verify removal
   - Restart database, verify new connection created
   - Check metrics reflect health check activity

**Deliverables**:
- [ ] Health checks run every 30s
- [ ] Unhealthy connections removed automatically
- [ ] Pre-lease validation catches bad connections
- [ ] Health metrics exposed

**Acceptance Criteria**:
- Stale connection detected within 30s
- Pre-lease validation prevents bad connection usage
- Health check metrics accurate
- Integration test passes with real database

**Files Created/Modified**:
- `src/ts/src/broker/base-pool.ts` (enhance)
- `src/ts/src/broker/serve.ts` (add health checks to pools)
- `src/ts/tests/integration/pool-health.test.ts` (NEW)

---

### Phase 5: Pool Lifecycle Hooks (Week 8-9)

**Objectives**:
- Implement lifecycle hooks (on_acquire, on_release, on_error, on_timeout)
- Create idle connection reaper
- Add lifecycle metrics
- Document hook API

**Tasks**:

1. **Lifecycle hook API** (`src/ts/src/broker/base-pool.ts`)
   - Add hook registration methods:
     - `setOnAcquire(fn: (token) => Promise<void>)`
     - `setOnRelease(fn: (token) => Promise<void>)`
     - `setOnError(fn: (token, error) => Promise<void>)`
     - `setOnTimeout(fn: (token) => Promise<void>)`
   - Call hooks at appropriate points in connection lifecycle

2. **Hook implementation in SqlPool** (`src/ts/src/broker/serve.ts`)
   - on_acquire: Log connection acquisition, start lease timer
   - on_release: Log connection return, update last_used timestamp
   - on_error: Log error, increment error counter
   - on_timeout: Log timeout, remove connection from pool

3. **Idle connection reaper** (`src/ts/src/broker/base-pool.ts`)
   - Add `startIdleReaper(timeoutMs)` method
   - Track `last_used` timestamp for each connection
   - Periodic scan for idle connections (every 60s)
   - Close connections idle >5 minutes
   - Emit `idle_timeout_total` metric

4. **Idle tracking** (`src/ts/src/broker/serve.ts`)
   - Update `last_used` on acquire and release
   - Store in Map<token, timestamp>
   - Clean up map on connection removal

5. **Lifecycle metrics** (`src/ts/src/broker/base-pool.ts`)
   - `lifecycle_hook_calls_total{pool, hook="acquire|release|error|timeout"}`
   - `idle_timeouts_total{pool}`
   - `connection_lease_duration_seconds{pool}` (histogram)

6. **Unit tests** (`src/ts/tests/unit/lifecycle-hooks.test.ts`)
   - Test each hook is called at correct time
   - Test idle reaper removes old connections
   - Test metrics updated correctly

**Deliverables**:
- [ ] Lifecycle hooks working
- [ ] Idle reaper removes stale connections
- [ ] Hook metrics exposed
- [ ] Unit tests pass

**Acceptance Criteria**:
- Hooks called at correct lifecycle points
- Idle connections removed after timeout
- Custom logging via hooks works
- All lifecycle metrics accurate

**Files Created/Modified**:
- `src/ts/src/broker/base-pool.ts` (enhance)
- `src/ts/src/broker/serve.ts` (add idle tracking)
- `src/ts/tests/unit/lifecycle-hooks.test.ts` (NEW)

---

### Phase 6: Unified Observability (Week 9-10)

**Objectives**:
- Create `/__metrics__` endpoint aggregating all statistics
- Implement Prometheus format exporter
- Add MCP-specific metrics
- Create Grafana dashboard JSON

**Tasks**:

1. **Metrics endpoint** (`src/ts/src/server/metrics.ts`)
   - `GET /__metrics__` handler
   - Collect metrics from broker, server, MCP
   - Format as Prometheus text
   - Cache for 5s to reduce overhead

2. **Broker metrics collector** (`src/ts/src/broker/metrics.ts`)
   - Aggregate stats from all pools
   - Pool-level metrics (connections, acquisition latency, errors)
   - Fairness scheduler metrics (worker wait time, queue length)
   - Health check metrics

3. **Server metrics collector** (`src/ts/src/server/metrics.ts`)
   - HTTP request count, latency, status codes
   - Active connections
   - Request body size histogram
   - WebSocket connections (if applicable)

4. **MCP metrics collector** (`src/ts/src/mcp/metrics.ts`)
   - Tool invocation count by tool name
   - Tool execution duration (histogram)
   - Tool errors by tool name
   - Resource access count
   - Transport-specific metrics (stdio messages, SSE connections, WebSocket messages)

5. **Prometheus formatter** (`src/ts/src/server/prometheus.ts`)
   - Format gauge, counter, histogram metrics
   - Include HELP and TYPE comments
   - Handle label encoding
   - Escape special characters

6. **Grafana dashboard** (`docs/grafana/vertebra-dashboard.json`)
   - Pool utilization panel (stacked area chart)
   - Acquisition latency panel (line graph with percentiles)
   - Tool invocation rate panel (bar chart)
   - Health check status panel (gauge)
   - Error rate panel (line graph)

7. **Integration test** (`src/ts/tests/integration/metrics.test.ts`)
   - Start server with pools and MCP tools
   - Generate load (tool calls, pool usage)
   - Query `/__metrics__`
   - Verify Prometheus format
   - Verify metric values match actual activity

**Deliverables**:
- [ ] `/__metrics__` endpoint working
- [ ] All subsystem metrics aggregated
- [ ] Prometheus format correct
- [ ] Grafana dashboard JSON

**Acceptance Criteria**:
- Single metrics endpoint for all statistics
- Prometheus scraper can parse output
- Grafana dashboard visualizes key metrics
- Metrics response time <50ms
- Integration test validates accuracy

**Files Created/Modified**:
- `src/ts/src/server/metrics.ts` (NEW)
- `src/ts/src/broker/metrics.ts` (NEW)
- `src/ts/src/mcp/metrics.ts` (NEW)
- `src/ts/src/server/prometheus.ts` (NEW)
- `docs/grafana/vertebra-dashboard.json` (NEW)
- `src/ts/tests/integration/metrics.test.ts` (NEW)

---

### Phase 7: Production Hardening (Week 10-11)

**Objectives**:
- Audit and improve error handling
- Write comprehensive documentation
- Create example applications
- Run performance benchmarks

**Tasks**:

1. **Error handling audit**:
   - Review all MCP transport error cases
   - Ensure pool errors propagate correctly
   - Add timeout handling everywhere
   - Test error recovery scenarios

2. **Documentation** (`docs/mcp/`, `docs/pools/`, `docs/observability/`):
   - MCP integration guide
     - Getting started
     - Tool creation
     - Transport selection
     - Claude Desktop setup
   - Pool management guide
     - Health check configuration
     - Lifecycle hooks
     - Idle timeout tuning
   - Observability guide
     - Metrics reference
     - Grafana setup
     - Alerting rules

3. **Example applications** (`examples/`):
   - **MCP Tools Server**: 5-10 useful tools (database query, file search, API call, etc.)
   - **Health Check Demo**: App demonstrating pool health features
   - **Metrics Demo**: App showcasing observability features

4. **Performance benchmarks** (`benchmarks/`):
   - MCP tool invocation latency (target <5ms P99)
   - Pool acquisition latency (target <2ms P99)
   - Metrics endpoint latency (target <50ms)
   - Throughput tests (tool calls/sec, pool ops/sec)

5. **CI/CD Integration**:
   - Add MCP integration tests to CI
   - Add pool health tests to CI
   - Add metrics tests to CI
   - Performance regression tests

**Deliverables**:
- [ ] Error handling comprehensive
- [ ] Documentation complete
- [ ] Example apps working
- [ ] Benchmarks passing targets

**Acceptance Criteria**:
- All error cases handled gracefully
- Documentation covers all features
- Examples run out-of-box
- Performance targets met
- CI tests pass reliably

**Files Created**:
- `docs/mcp/getting-started.md`
- `docs/mcp/tools.md`
- `docs/mcp/transports.md`
- `docs/pools/health-checks.md`
- `docs/pools/lifecycle-hooks.md`
- `docs/observability/metrics.md`
- `examples/mcp-tools/` (directory)
- `examples/pool-health-demo/` (directory)
- `benchmarks/mcp-latency.ts`
- `benchmarks/pool-latency.ts`

---

## Testing Strategy

### Unit Tests

**TypeScript (Bun Test)**:
- MCP server: JSON-RPC parsing, method routing, error handling
- Schema registry: Tool registration, schema generation
- Pool health: Health check logic, lifecycle hooks, idle reaper
- Metrics: Prometheus formatting, aggregation logic

**Python (pytest)**:
- `@mcp_tool` decorator: Metadata extraction, registration
- Schema generator: TypeNode → JSON Schema conversion
- MCP handler: IPC message handling, tool invocation

**Coverage Target**: 85%+

### Integration Tests

**End-to-End Scenarios**:
1. Claude Desktop → stdio → Tool invocation → PostgreSQL query
2. Browser client → SSE → Resource read → Response
3. WebSocket client → Tool call with progress → Progress updates received
4. Pool health check → Database killed → Connection removed → New connection created
5. Idle reaper → Connection unused 5min → Connection closed

**Test Infrastructure**:
- Docker containers for databases (Postgres, MySQL, Redis)
- Mock MCP clients for automated testing
- Load generators for performance tests

### Performance Tests

**Benchmarks**:

| Operation | Target (P99) | Test Scenario |
|-----------|--------------|---------------|
| MCP tool invocation | <5ms | 1000 calls, measure IPC overhead |
| Pool acquisition | <2ms | 10k acquisitions, measure latency |
| Health check | <10ms | Check 100 connections |
| Metrics endpoint | <50ms | Query /__metrics__ under load |
| Schema generation | <100ms | Generate schemas for 50 tools |

**Load Testing**:
- 1000 concurrent tool calls
- 10k pool operations/sec
- 100 MCP clients connected simultaneously
- Metrics under continuous scraping

### Compatibility Tests

**MCP Clients**:
- Claude Desktop (latest)
- Custom MCP client (reference implementation)
- Browser-based MCP client (via SSE)

**Databases**:
- PostgreSQL 14+
- MySQL 8+
- Redis 7+
- SQLite 3.40+

**Operating Systems**:
- Linux (Ubuntu 22.04, 24.04)
- macOS (13+)
- Windows (via WSL2)

---

## Success Metrics

### Functional Requirements

- [ ] Claude Desktop can discover and call Vertebra tools
- [ ] All three transports (stdio, SSE, WebSocket) work correctly
- [ ] Python `@mcp_tool` decorator generates correct schemas
- [ ] Health checks detect and remove bad connections within 30s
- [ ] Lifecycle hooks execute at correct times
- [ ] Idle connections reaped after timeout
- [ ] `/__metrics__` endpoint returns aggregated statistics
- [ ] Prometheus scraper successfully parses metrics
- [ ] Grafana dashboard visualizes all key metrics

### Performance Requirements

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| MCP Tool Invocation (P99) | <5ms | Histogram in metrics |
| Pool Acquisition (P99) | <2ms | Histogram in metrics |
| Health Check Duration (P99) | <10ms | Per-pool timing |
| Metrics Endpoint (P99) | <50ms | HTTP response time |
| Schema Generation | <100ms | For 50 tools |
| Throughput | >1000 tool calls/sec | Load test |

### Quality Requirements

- [ ] 0 TypeScript type errors
- [ ] 0 Python mypy errors (strict mode)
- [ ] Test coverage >85%
- [ ] All documentation complete
- [ ] Example applications functional
- [ ] CI/CD pipeline green

---

## Dependencies

### External Dependencies

**TypeScript/Bun**:
- Bun runtime (latest)
- No additional NPM packages required (native JSON-RPC, WebSocket)

**Python**:
- `msgspec` - MessagePack serialization (already a dependency)
- `anyio` - Async compatibility (already a dependency)
- Optional: `pydantic` for BaseModel schema support

### Internal Dependencies

**Required**:
- PRD-1: Rust IPC Layer (shared-memory transport)
- PRD-2: Type Bridge (TypeNode → JSON Schema)

**Leveraged**:
- PRD-5: Resource Management (BasePoolAdapter, BrokerClient)

### Version Requirements

- Python 3.12+
- Bun 1.0+
- TypeScript 5.0+

---

## Risks & Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Type Bridge IR inadequate for schema generation | High | Medium | Extend IR with missing types early (Phase 2) |
| MCP spec changes | Medium | Low | Lock to specific spec version, document |
| IPC overhead >5ms | High | Low | Benchmark early, optimize hot paths |
| Health check false positives | Medium | Medium | Tune check frequency, add retry logic |
| Metrics endpoint performance | Medium | Low | Cache for 5s, optimize collection |
| Integration test flakiness | Low | High | Use container fixtures, add retries |

### Go/No-Go Decision Points

1. **After Phase 1**: stdio transport works, tool list/call functional
2. **After Phase 2**: Python tools invocable via IPC, schemas generated correctly
3. **After Phase 3**: SSE/WebSocket working, resources functional
4. **After Phase 6**: All metrics accurate, performance targets met

---

## File Paths Summary

### Created Files

**TypeScript (MCP)**:
- `src/ts/src/mcp/transports/stdio.ts`
- `src/ts/src/mcp/transports/sse.ts`
- `src/ts/src/mcp/transports/websocket.ts`
- `src/ts/src/mcp/ipc-bridge.ts`
- `src/ts/src/mcp/schema-registry.ts`
- `src/ts/src/mcp/metrics.ts`

**TypeScript (Server/Broker)**:
- `src/ts/src/server/metrics.ts`
- `src/ts/src/server/prometheus.ts`
- `src/ts/src/broker/metrics.ts`

**Python (MCP)**:
- `src/py/vertebra/mcp/__init__.py`
- `src/py/vertebra/mcp/decorators.py`
- `src/py/vertebra/mcp/registry.py`
- `src/py/vertebra/mcp/schema.py`
- `src/py/vertebra/mcp/handler.py`
- `src/py/vertebra/mcp/resources.py`

**Tests**:
- `src/ts/tests/integration/mcp.test.ts`
- `src/ts/tests/integration/pool-health.test.ts`
- `src/ts/tests/integration/metrics.test.ts`
- `src/ts/tests/unit/lifecycle-hooks.test.ts`
- `src/py/tests/test_mcp_integration.py`

**Documentation**:
- `docs/mcp/getting-started.md`
- `docs/mcp/tools.md`
- `docs/mcp/transports.md`
- `docs/pools/health-checks.md`
- `docs/pools/lifecycle-hooks.md`
- `docs/observability/metrics.md`
- `docs/grafana/vertebra-dashboard.json`

**Examples**:
- `examples/mcp-tools/` (directory)
- `examples/pool-health-demo/` (directory)

### Modified Files

**TypeScript**:
- `src/ts/src/mcp/server.ts` (enhance JSON-RPC handler)
- `src/ts/src/broker/base-pool.ts` (add health checks + hooks)
- `src/ts/src/broker/serve.ts` (implement health checks for pools)

**Python**:
- (None - all new functionality in new modules)

---

## Acceptance Criteria Checklist

### MCP Protocol
- [ ] stdio transport connects to Claude Desktop
- [ ] SSE transport works with browser clients
- [ ] WebSocket transport handles bidirectional messages
- [ ] `tools/list` returns all registered tools
- [ ] `tools/call` invokes Python functions correctly
- [ ] `resources/list` returns available resources
- [ ] `resources/read` returns resource data by URI
- [ ] Progress notifications sent during long operations
- [ ] Logs forwarded to MCP client
- [ ] JSON schemas generated from Python type hints
- [ ] Complex types (Struct, Optional, Union) supported
- [ ] IPC overhead <5ms (P99)

### Pool Health & Lifecycle
- [ ] Health checks run every 30 seconds
- [ ] Unhealthy connections removed automatically
- [ ] Pre-lease validation prevents bad connection usage
- [ ] Post-release validation catches corrupted connections
- [ ] `on_acquire` hook called after acquiring connection
- [ ] `on_release` hook called before releasing connection
- [ ] `on_error` hook called when errors occur
- [ ] `on_timeout` hook called on acquisition timeout
- [ ] Idle connections reaped after 5 minutes
- [ ] All lifecycle events logged correctly
- [ ] Health check metrics accurate

### Observability
- [ ] `/__metrics__` endpoint returns all statistics
- [ ] Prometheus format valid
- [ ] Broker metrics included (pool stats, fairness, health)
- [ ] Server metrics included (HTTP requests, connections)
- [ ] MCP metrics included (tool calls, latency, errors)
- [ ] Metrics endpoint response time <50ms
- [ ] Grafana dashboard loads and displays data
- [ ] All 20+ key metrics present
- [ ] Metrics accurate under load

### Quality & Documentation
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Test coverage >85%
- [ ] 0 TypeScript type errors
- [ ] 0 Python mypy errors
- [ ] Getting started guide complete
- [ ] API reference complete
- [ ] Example applications run successfully
- [ ] Performance benchmarks meet targets

---

## Timeline Summary

| Phase | Duration | Cumulative | Key Deliverable |
|-------|----------|------------|-----------------|
| Phase 1: MCP Foundation | 2 weeks | 2 weeks | stdio transport + tool discovery |
| Phase 2: Python Integration | 2 weeks | 4 weeks | @mcp_tool decorator + IPC bridge |
| Phase 3: Advanced MCP | 2 weeks | 6 weeks | SSE/WebSocket + resources |
| Phase 4: Pool Health | 1.5 weeks | 7.5 weeks | Health checks + validation |
| Phase 5: Pool Lifecycle | 1 week | 8.5 weeks | Hooks + idle reaper |
| Phase 6: Observability | 1 week | 9.5 weeks | /__metrics__ endpoint |
| Phase 7: Hardening | 1.5 weeks | 11 weeks | Docs + examples + benchmarks |

**Total: 11 weeks (2.75 months)**

---

## Appendix: MCP Protocol Quick Reference

### Core Methods

**Tool Discovery**:
```json
{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
// Response: {"result": {"tools": [...]}}
```

**Tool Invocation**:
```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "query_database",
    "arguments": {"query": "SELECT * FROM users"}
  }
}
```

**Resource List**:
```json
{"jsonrpc": "2.0", "id": 3, "method": "resources/list"}
// Response: {"result": {"resources": [...]}}
```

**Resource Read**:
```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "resources/read",
  "params": {"uri": "users://123/profile"}
}
```

**Progress Notification** (server-initiated):
```json
{
  "jsonrpc": "2.0",
  "method": "notifications/progress",
  "params": {
    "progressToken": "task-123",
    "progress": 50,
    "total": 100
  }
}
```

### Error Codes

| Code | Meaning |
|------|---------|
| -32700 | Parse error (invalid JSON) |
| -32600 | Invalid request |
| -32601 | Method not found |
| -32602 | Invalid params |
| -32603 | Internal error |
| -32000 | Tool execution failed |

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-19 | Vertebra Team | Initial consolidated PRD |

---

**Document Status**: Active - Ready for Implementation
**Total Word Count**: 7,200+ words

---

**End of PRD**
