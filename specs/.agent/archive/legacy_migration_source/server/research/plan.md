# Research Plan: Server & Runtime

**PRD**: `/home/cody/code/c/vertebra/specs/active/server/prd.md`
**Date**: 2026-01-19
**Word Count**: 2,000+ words

---

## 1. MCP Protocol Specification

### 1.1 Protocol Overview

**Model Context Protocol (MCP)** is an open standard by Anthropic for AI assistant integration.

**Key Concepts**:
- **Tools**: Functions AI assistants can call with typed parameters
- **Resources**: Structured data entities accessible via URI templates
- **Prompts**: Pre-defined prompt templates (not implemented in this PRD)
- **Transport**: Communication layer (stdio, SSE, WebSocket)

**Wire Format**: JSON-RPC 2.0

### 1.2 Core Methods

**Initialization**:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {
      "tools": {}
    },
    "clientInfo": {
      "name": "claude-desktop",
      "version": "1.0.0"
    }
  }
}
```

**Tool Discovery**:
```json
{"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
```

**Tool Invocation**:
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "query_database",
    "arguments": {"query": "SELECT * FROM users"}
  }
}
```

**Resource Discovery**:
```json
{"jsonrpc": "2.0", "id": 4, "method": "resources/list"}
```

**Resource Read**:
```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "resources/read",
  "params": {"uri": "users://123/profile"}
}
```

### 1.3 Error Codes

| Code | Meaning | Usage |
|------|---------|-------|
| -32700 | Parse error | Invalid JSON |
| -32600 | Invalid request | Missing required fields |
| -32601 | Method not found | Unknown method |
| -32602 | Invalid params | Invalid parameters |
| -32603 | Internal error | Server error |
| -32000 | Tool execution failed | Tool-specific error |

### 1.4 Transports

**stdio (Standard I/O)**:
- Used by Claude Desktop for local subprocesses
- Newline-delimited JSON-RPC messages
- Simplest to implement

**SSE (Server-Sent Events)**:
- HTTP-based, works in browsers
- POST for requests, EventSource for responses
- Requires keep-alive handling

**WebSocket**:
- Bidirectional, real-time
- JSON-RPC over WebSocket frames
- Most complex, most capable

---

## 2. JSON-RPC 2.0 Implementation

### 2.1 Request Structure

```typescript
interface JsonRpcRequest {
  jsonrpc: "2.0";
  id: string | number | null;
  method: string;
  params?: unknown;
}
```

### 2.2 Response Structure

```typescript
interface JsonRpcResponse {
  jsonrpc: "2.0";
  id: string | number | null;
  result?: unknown;
  error?: {
    code: number;
    message: string;
    data?: unknown;
  };
}
```

### 2.3 Notification (No Response)

```typescript
interface JsonRpcNotification {
  jsonrpc: "2.0";
  method: string;
  params?: unknown;
  // No id field = no response expected
}
```

### 2.4 Batch Requests

JSON-RPC 2.0 supports batching (array of requests), but MCP currently doesn't use this.

### 2.5 Implementation Notes

**Bun-Specific**:
- Use native JSON parsing (faster than libraries)
- Use `process.stdin` / `process.stdout` for stdio
- Use `Bun.serve()` for SSE/WebSocket

**Error Handling**:
- Always include `jsonrpc: "2.0"` in responses
- Include `id` from request (or `null` for parse errors)
- Use standard error codes

---

## 3. Pool Health Check Patterns

### 3.1 Health Check Strategies

**Periodic Validation**:
- Run health checks every N seconds (default: 30s)
- Check idle connections only (don't interrupt active queries)
- Remove unhealthy connections from pool

**Pre-Lease Validation**:
- Quick check before returning connection to caller
- Retry with different connection if validation fails
- Log validation failures for monitoring

**Post-Release Validation**:
- Validate connection on return to pool
- Remove corrupted connections immediately
- Prevents bad connections from re-entering pool

### 3.2 Health Check Implementations

**PostgreSQL**:
```sql
SELECT 1
```
Expected: Single row with value 1

**MySQL**:
```sql
SELECT 1
```
Expected: Single row with value 1

**SQLite**:
```sql
SELECT 1
```
Expected: Single row with value 1

**Redis**:
```
PING
```
Expected: "PONG"

### 3.3 Configuration Options

```typescript
interface HealthCheckConfig {
  intervalMs: number;      // Check interval (default: 30000)
  timeoutMs: number;       // Check timeout (default: 5000)
  retries: number;         // Retries before removal (default: 2)
  validateOnAcquire: boolean;  // Pre-lease validation (default: true)
  validateOnRelease: boolean;  // Post-release validation (default: false)
}
```

### 3.4 Idle Connection Management

**Idle Timeout**:
- Track `last_used` timestamp per connection
- Close connections idle > timeout (default: 5 minutes)
- Run reaper every 60 seconds

**Minimum Pool Size**:
- Never drop below minimum pool size
- Create new connections to replace reaped ones

**Connection Lifecycle**:
1. Connection created
2. Connection acquired (update `last_used`)
3. Connection released (update `last_used`)
4. Idle timeout reached -> connection closed
5. New connection created if below minimum

---

## 4. Prometheus Metrics Format

### 4.1 Metric Types

**Counter**: Cumulative value that only increases
```
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
```

**Gauge**: Value that can go up or down
```
# HELP pool_connections_total Pool connections by state
# TYPE pool_connections_total gauge
pool_connections_total{pool="postgres",state="idle"} 5
pool_connections_total{pool="postgres",state="in_use"} 3
```

**Histogram**: Distribution of values
```
# HELP request_duration_seconds Request duration
# TYPE request_duration_seconds histogram
request_duration_seconds_bucket{le="0.01"} 100
request_duration_seconds_bucket{le="0.05"} 200
request_duration_seconds_bucket{le="0.1"} 250
request_duration_seconds_bucket{le="+Inf"} 260
request_duration_seconds_sum 12.5
request_duration_seconds_count 260
```

### 4.2 Vertebra Metrics

**Pool Metrics**:
- `pool_connections_total{pool,state}` - gauge
- `pool_acquisition_duration_seconds{pool}` - histogram
- `pool_errors_total{pool,type}` - counter
- `pool_health_checks_total{pool,result}` - counter
- `pool_idle_timeouts_total{pool}` - counter

**MCP Metrics**:
- `mcp_tool_invocations_total{tool,status}` - counter
- `mcp_tool_duration_seconds{tool}` - histogram
- `mcp_resource_reads_total{resource}` - counter
- `mcp_transport_connections{transport}` - gauge
- `mcp_transport_messages_total{transport,direction}` - counter

**Server Metrics**:
- `http_requests_total{method,status}` - counter
- `http_request_duration_seconds{method}` - histogram
- `http_active_connections` - gauge

### 4.3 Label Guidelines

**Cardinality**:
- Keep label cardinality low (avoid user IDs, request IDs)
- Pool names, tool names are acceptable
- Status codes are acceptable

**Naming**:
- Use lowercase, underscores
- End with unit (`_seconds`, `_bytes`, `_total`)
- Be descriptive but concise

### 4.4 Endpoint Implementation

```typescript
async function metricsHandler(): Promise<Response> {
  const metrics = await collectAllMetrics();
  const text = formatPrometheus(metrics);

  return new Response(text, {
    headers: { "Content-Type": "text/plain; version=0.0.4" }
  });
}
```

---

## 5. Performance Considerations

### 5.1 IPC Overhead Target

**Target**: <5ms P99 for MCP tool invocation

**Breakdown**:
- JSON-RPC parsing: <0.5ms
- IPC send (shared memory): <0.5ms
- Python handler lookup: <0.5ms
- Tool execution: variable
- IPC receive: <0.5ms
- JSON-RPC response: <0.5ms

**Optimization Strategies**:
- Reuse shared memory regions
- Pre-allocate message buffers
- Avoid unnecessary copies

### 5.2 Pool Acquisition Target

**Target**: <2ms P99 for connection acquisition

**Breakdown**:
- Lock acquisition: <0.1ms
- Connection selection: <0.1ms
- Pre-lease validation: <1ms (if enabled)
- Return to caller: <0.1ms

**Optimization Strategies**:
- Use lock-free data structures where possible
- Pre-warm connection pool on startup
- Validate lazily (skip if recently validated)

### 5.3 Metrics Endpoint Target

**Target**: <50ms P99 for `/__metrics__` response

**Strategies**:
- Cache metrics for 5 seconds
- Collect metrics incrementally (update counters in place)
- Format on demand (don't pre-format)

### 5.4 Throughput Target

**Target**: >1000 tool calls/second

**Strategies**:
- Async tool execution
- Connection pooling
- Batch operations where applicable

---

## 6. Type Bridge Integration

### 6.1 Python Type Extraction

The Type Bridge (`src/py/vertebra/types/`) provides:
- `TypeExtractor`: Extract type hints from functions
- `TypeNode`: Intermediate representation for types
- `TypeScriptGenerator`: Convert TypeNode to TypeScript

**For MCP, we need**:
- TypeNode to JSON Schema converter
- Support for complex types (Struct, BaseModel)

### 6.2 JSON Schema Generation

**TypeNode to JSON Schema mapping**:

| TypeNode | JSON Schema |
|----------|-------------|
| `StringNode` | `{"type": "string"}` |
| `IntNode` | `{"type": "integer"}` |
| `FloatNode` | `{"type": "number"}` |
| `BoolNode` | `{"type": "boolean"}` |
| `ListNode` | `{"type": "array", "items": ...}` |
| `DictNode` | `{"type": "object", ...}` |
| `OptionalNode` | Add `null` to type |
| `UnionNode` | `{"anyOf": [...]}` |
| `LiteralNode` | `{"enum": [...]}` |
| `StructNode` | `{"type": "object", "properties": ...}` |

### 6.3 Example Conversion

**Python function**:
```python
@mcp_tool()
async def query_database(query: str, limit: int = 100) -> list[dict]:
    """Execute a SQL query."""
    ...
```

**Generated JSON Schema**:
```json
{
  "name": "query_database",
  "description": "Execute a SQL query.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "query": {"type": "string"},
      "limit": {"type": "integer", "default": 100}
    },
    "required": ["query"]
  }
}
```

---

## 7. Existing Codebase Analysis

### 7.1 MCP Server (Current State)

**File**: `src/ts/src/mcp/server.ts`

**Current Implementation**:
```typescript
export class McpServer {
  private tools = new Map<string, McpTool>();

  registerTool(tool: McpTool): void {
    this.tools.set(tool.name, tool);
  }

  async handle(request: JsonRpcRequest): Promise<JsonRpcResponse> {
    // Basic routing - needs enhancement
  }
}
```

**Missing**:
- Transport layer integration
- Schema registry
- IPC bridge
- MCP protocol methods (initialize, tools/list, etc.)

### 7.2 Broker Pools (Current State)

**File**: `src/ts/src/broker/base-pool.ts`

**Current Implementation**:
- Abstract `BasePoolAdapter` class
- `acquire()` / `release()` methods
- Basic statistics tracking

**Missing**:
- Health check loop
- Lifecycle hooks
- Idle connection management
- Health metrics

### 7.3 Shared Memory IPC

**Files**:
- `src/rs/vertebra-core/` - Rust ring buffers
- `src/py/vertebra/_internal/shm.py` - Python CFFI
- `src/ts/src/broker/shm.ts` - Bun FFI

**Status**: Fully implemented, ready for MCP integration

---

## 8. Implementation Priorities

### 8.1 Critical Path

1. **Phase 1**: MCP Foundation (stdio transport)
   - Blocks all MCP functionality
   - Enables Claude Desktop integration

2. **Phase 2**: Python Integration (@mcp_tool)
   - Blocks useful tool development
   - Enables schema generation

3. **Phase 4**: Pool Health Checks
   - Can run in parallel with MCP phases
   - Independent of MCP functionality

### 8.2 Risk Mitigation

**Type Bridge IR Gaps**:
- Test schema generation early with complex types
- Extend IR if missing types discovered

**IPC Performance**:
- Benchmark after Phase 2
- Optimize hot paths if needed

**MCP Spec Changes**:
- Lock to specific protocol version
- Document version in PRD

---

## 9. References

**MCP Specification**:
- https://spec.modelcontextprotocol.io/specification/2024-11-05/

**JSON-RPC 2.0**:
- https://www.jsonrpc.org/specification

**Prometheus Format**:
- https://prometheus.io/docs/instrumenting/exposition_formats/

**Bun Documentation**:
- https://bun.sh/docs

---

**Document Version**: 1.0
**Word Count**: 2,000+ words
**Last Updated**: 2026-01-19
**Status**: Research Complete
