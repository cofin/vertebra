# Tasks: Server & Runtime

**PRD**: `/home/cody/code/c/vertebra/specs/active/server/prd.md`
**Status**: Active
**Transport**: Shared-memory C ABI via broker (no sockets)

---

## Phase 1: MCP Foundation (Week 1-2)

### Task Group 1.1: stdio Transport

**Priority**: P0 (Core MCP functionality)
**Dependencies**: None

- [x] **Task 1.1.1**: Create stdio transport module
  - Read JSON-RPC from `process.stdin` line-by-line
  - Write responses to `process.stdout`
  - File: `src/ts/src/mcp/transports/stdio.ts` (NEW)

- [x] **Task 1.1.2**: Implement connection lifecycle
  - Handle startup initialization
  - Handle graceful shutdown
  - Error handling for malformed JSON
  - File: `src/ts/src/mcp/transports/stdio.ts`

- [x] **Task 1.1.3**: Add stdio transport tests
  - Test message parsing
  - Test response formatting
  - Test error handling
  - File: `src/ts/tests/unit/stdio-transport.test.ts` (NEW)

**Acceptance**:
- stdio transport reads/writes JSON-RPC messages
- Handles connection lifecycle correctly
- Tests pass

---

### Task Group 1.2: MCP Server Enhancement

**Priority**: P0
**Dependencies**: Task 1.1

- [x] **Task 1.2.1**: Add `initialize` method handler
  - MCP handshake protocol
  - Return capabilities and protocol version
  - File: `src/ts/src/mcp/server.ts`

- [x] **Task 1.2.2**: Add `tools/list` method handler
  - Return registered tools with schemas
  - File: `src/ts/src/mcp/server.ts`

- [x] **Task 1.2.3**: Add `tools/call` method handler
  - Invoke tool by name with arguments
  - Return tool result or error
  - File: `src/ts/src/mcp/server.ts`

- [x] **Task 1.2.4**: Implement MCP error codes
  - -32700: Parse error
  - -32600: Invalid request
  - -32601: Method not found
  - -32602: Invalid params
  - -32603: Internal error
  - -32000: Tool execution failed
  - File: `src/ts/src/mcp/server.ts`

**Acceptance**:
- MCP handshake works
- `tools/list` returns registered tools
- `tools/call` invokes tools correctly
- Error codes match MCP spec

---

### Task Group 1.3: Schema Registry

**Priority**: P0
**Dependencies**: None

- [x] **Task 1.3.1**: Create schema registry module
  - Store tool schemas (name, description, inputSchema)
  - Load schemas from JSON file
  - File: `src/ts/src/mcp/schema-registry.ts` (NEW)

- [x] **Task 1.3.2**: Implement hot reload
  - Watch schema file for changes
  - Reload schemas without restart
  - File: `src/ts/src/mcp/schema-registry.ts`

- [x] **Task 1.3.3**: Add schema registry tests
  - Test schema loading
  - Test hot reload
  - File: `src/ts/tests/unit/schema-registry.test.ts` (NEW)

**Acceptance**:
- Schemas load from JSON file
- Hot reload works
- Tests pass

---

### Task Group 1.4: Integration Test

**Priority**: P0
**Dependencies**: Tasks 1.1, 1.2, 1.3

- [x] **Task 1.4.1**: Create MCP integration test
  - Start MCP server with stdio transport
  - Send `initialize` request
  - Send `tools/list` request
  - Send `tools/call` request for echo tool
  - Verify responses match MCP spec
  - File: `src/ts/tests/integration/mcp.test.ts` (NEW)

**Acceptance**:
- Integration test passes
- Claude Desktop can connect via stdio
- Tool invocation completes in <10ms

### Task Group 1.5: Stability Fixes

**Priority**: P0
**Dependencies**: None

- [x] **Task 1.5.1**: Fix router.test.ts FFI symbol mismatch
  - Investigate `vtb_last_error_message` symbol missing in `libvertebra_js.so`
  - Fix build configuration or symbol export in Rust
  - Verify `router.test.ts` passes
  - File: `src/rs/vertebra-js/src/lib.rs` (likely)

---

**Phase 1 Checkpoint**: [checkpoint: 4c4f712]

## Phase 2: Python Integration (Week 3-4)

### Task Group 2.1: MCP Decorator

**Priority**: P0
**Dependencies**: Phase 1

- [x] **Task 2.1.0**: Clean legacy stub
  - Delete `src/py/vertebra/mcp.py` (if exists) to allow package creation
  - Create package directory `src/py/vertebra/mcp/`

- [x] **Task 2.1.1**: Create MCP package structure
  - Files: `__init__.py`, `decorators.py`, `registry.py`
  - Location: `src/py/vertebra/mcp/`

- [x] **Task 2.1.2**: Implement `@mcp_tool` decorator
  - Accept optional `name`, `description` parameters
  - Extract function signature via Type Bridge
  - Register tool in ProcedureRegistry
  - Support async functions
  - File: `src/py/vertebra/mcp/decorators.py` (NEW)

- [x] **Task 2.1.3**: Implement ProcedureRegistry
  - `register_tool(name, handler, params, return_type)` method
  - `get_tool(name) -> Tool` method
  - `list_tools() -> list[Tool]` method
  - Export to JSON for Bun schema registry
  - File: `src/py/vertebra/mcp/registry.py` (NEW)

- [x] **Task 2.1.4**: Add decorator tests
  - Test tool registration
  - Test metadata extraction
  - Test async function support
  - File: `src/py/tests/test_mcp_decorator.py` (NEW)

**Acceptance**:
- `@mcp_tool` decorator registers functions
- Metadata correctly extracted
- Tests pass

---

### Task Group 2.2: Schema Generation

**Priority**: P0
**Dependencies**: Task 2.1

- [x] **Task 2.2.1**: Create schema generator module
  - `MCPSchemaGenerator` class
  - `generate_tool_schema(tool: Tool) -> dict` method
  - File: `src/py/vertebra/mcp/schema.py` (NEW)

- [x] **Task 2.2.2**: Implement TypeNode to JSON Schema conversion
  - Support: str, int, float, bool
  - Support: list, dict
  - Support: Optional, Union, Literal
  - File: `src/py/vertebra/mcp/schema.py`

- [x] **Task 2.2.3**: Add support for complex types
  - Support: msgspec.Struct
  - Support: pydantic.BaseModel
  - Support: dataclass
  - File: `src/py/vertebra/mcp/schema.py`

- [x] **Task 2.2.4**: Add schema generation tests
  - Test basic types
  - Test complex types
  - Test nested types
  - File: `src/py/tests/test_mcp_schema.py` (NEW)

**Acceptance**:
- JSON schemas generated from type hints
- Complex types (Struct, BaseModel) supported
- Tests pass

---

### Task Group 2.3: IPC Bridge

**Priority**: P0
**Dependencies**: Tasks 2.1, 2.2

- [x] **Task 2.3.1**: Create IPC bridge module (TypeScript)
  - `MCPIPCBridge` class
  - `callTool(name: string, args: unknown) -> Promise<unknown>`
  - Serialize to MessagePack, send via shared memory
  - File: `src/ts/src/mcp/ipc-bridge.ts` (NEW)

- [x] **Task 2.3.2**: Create IPC handler module (Python)
  - Listen for tool invocation requests
  - Look up tool in registry
  - Invoke handler with deserialized arguments
  - Serialize result and send back
  - File: `src/py/vertebra/mcp/handler.py` (NEW)

- [x] **Task 2.3.3**: Add error handling
  - Handle tool not found
  - Handle execution timeout
  - Handle serialization errors
  - Files: `ipc-bridge.ts`, `handler.py`

- [x] **Task 2.3.4**: Add IPC bridge tests
  - Test successful invocation
  - Test error handling
  - Test timeout handling
  - File: `src/ts/tests/unit/ipc-bridge.test.ts` (NEW)

- [x] **Task 2.3.5**: Serialization Audit
  - Ensure `vertebra.serializers` is used instead of raw `msgspec` in Python handler.
  - Verify JS/TS interop with map-like structs.

**Acceptance**:
- Tool calls route to Python via IPC
- Error handling works
- IPC overhead <5ms (P99)

---

### Task Group 2.4: End-to-End Test

**Priority**: P0
**Dependencies**: Tasks 2.1, 2.2, 2.3

- [x] **Task 2.4.1**: Create end-to-end integration test
  - Register test tool with `@mcp_tool`
  - Start Bun MCP server
  - Send tool invocation via stdio
  - Verify Python handler executed
  - File: `src/py/tests/test_mcp_integration.py` (NEW)

**Phase 2 Checkpoint**: [checkpoint: a4fb979] (Refactored in 25ea379)

## Phase 3: Advanced MCP Features (Week 5-6)

### Task Group 3.1: SSE Transport

**Priority**: P1
**Dependencies**: Phase 1

- [x] **Task 3.1.1**: Create SSE transport module
  - HTTP endpoint accepting POST for requests
  - EventSource stream for responses
  - File: `src/ts/src/mcp/transports/sse.ts` (NEW)

- [x] **Task 3.1.2**: Implement keep-alive handling
  - Periodic keep-alive messages
  - Connection timeout handling
  - File: `src/ts/src/mcp/transports/sse.ts`

- [x] **Task 3.1.3**: Support multiple concurrent clients
  - Client session management
  - Request routing
  - File: `src/ts/src/mcp/transports/sse.ts`

- [x] **Task 3.1.4**: Add SSE transport tests
  - Test connection lifecycle
  - Test message streaming
  - Test multiple clients
  - File: `src/ts/tests/integration/sse.test.ts` (NEW)

**Acceptance**:
- SSE transport works with browser clients
- Keep-alive prevents timeouts
- Multiple clients supported

---

### Task Group 3.2: WebSocket Transport

**Priority**: P1
**Dependencies**: Phase 1

- [x] **Task 3.2.1**: Create WebSocket transport module
  - Bun native WebSocket support
  - JSON-RPC over WebSocket frames
  - File: `src/ts/src/mcp/transports/websocket.ts` (NEW)

- [x] **Task 3.2.2**: Implement connection lifecycle
  - Connection management
  - Ping/pong keepalive
  - Graceful disconnect
  - File: `src/ts/src/mcp/transports/websocket.ts`

- [x] **Task 3.2.3**: Add WebSocket transport tests
  - Test bidirectional messaging
  - Test connection lifecycle
  - File: `src/ts/tests/integration/websocket.test.ts` (NEW)

**Acceptance**:
- WebSocket transport handles bidirectional messages
- Connection lifecycle managed correctly
- Tests pass

---

### Task Group 3.3: Resource Support

**Priority**: P1
**Dependencies**: Phase 2

- [x] **Task 3.3.1**: Create `@mcp_resource` decorator
  - Accept URI template parameter
  - Register resource in ProcedureRegistry
  - File: `src/py/vertebra/mcp/resources.py` (NEW)

- [x] **Task 3.3.2**: Implement `resources/list` handler
  - Return registered resources with URI templates
  - File: `src/ts/src/mcp/server.ts`

- [x] **Task 3.3.3**: Implement `resources/read` handler
  - Parse URI and extract parameters
  - Invoke Python resource handler
  - Return resource data
  - File: `src/ts/src/mcp/server.ts`

- [x] **Task 3.3.4**: Add resource tests
  - Test resource registration
  - Test URI parsing
  - Test resource reading
  - File: `src/py/tests/test_mcp_resources.py` (NEW)

**Acceptance**:
- Resources can be discovered and read
- URI templates work correctly
- Tests pass

---

### Task Group 3.4: Progress Notifications

**Priority**: P2
**Dependencies**: Phase 2

- [x] **Task 3.4.1**: Implement `notifications/progress` method
  - Progress token tracking
  - Server-initiated notifications
  - File: `src/ts/src/mcp/server.ts`

- [x] **Task 3.4.2**: Add Python progress API
  - `yield_progress(token, current, total)` function
  - Integration with IPC handler
  - File: `src/py/vertebra/mcp/handler.py`

- [x] **Task 3.4.3**: Add progress notification tests
  - Test progress updates during long operations
  - File: `src/ts/tests/integration/progress.test.ts` (NEW)

**Phase 3 Checkpoint**: [checkpoint: dec698a]

## Phase 4: Pool Health Checks (Week 7-8)

### Task Group 4.1: Health Check Loop

**Priority**: P1
**Dependencies**: None

- [x] **Task 4.1.1**: Add health check infrastructure to BasePoolAdapter
  - `startHealthCheck(intervalMs)` method
  - Abstract `runHealthCheck()` method
  - Track last health check time per connection
  - File: `src/ts/src/broker/base-pool.ts`

- [x] **Task 4.1.2**: Implement PostgresHealthChecker
  - Execute `SELECT 1`
  - Return true if successful
  - File: `src/ts/src/broker/serve.ts`

- [x] **Task 4.1.3**: Implement MySqlHealthChecker
  - Execute `SELECT 1`
  - Return true if successful
  - File: `src/ts/src/broker/serve.ts`

- [x] **Task 4.1.4**: Implement SqliteHealthChecker
  - Execute `SELECT 1`
  - Return true if successful
  - File: `src/ts/src/broker/serve.ts`

- [x] **Task 4.1.5**: Implement RedisHealthChecker
  - Execute `PING`
  - Return true if response is "PONG"
  - File: `src/ts/src/broker/serve.ts`

**Acceptance**:
- Health checks run every 30s
- Unhealthy connections removed
- All pool types supported

---

### Task Group 4.2: Pre-lease Validation

**Priority**: P1
**Dependencies**: Task 4.1

- [x] **Task 4.2.1**: Implement pre-lease validation
  - Quick health check before returning connection
  - Retry with different connection if validation fails
  - Log validation failures
  - File: `src/ts/src/broker/base-pool.ts`

- [x] **Task 4.2.2**: Implement post-release validation
  - Validate connection on return to pool
  - Remove and replace if invalid
  - File: `src/ts/src/broker/base-pool.ts`

- [x] **Task 4.2.3**: Add validation tests
  - Test pre-lease validation catches bad connections
  - Test post-release validation removes corrupted connections
  - File: `src/ts/tests/unit/pool-validation.test.ts` (NEW)

**Acceptance**:
- Pre-lease validation prevents bad connection usage
- Post-release validation catches corrupted connections
- Tests pass

---

### Task Group 4.3: Health Check Metrics

**Priority**: P1
**Dependencies**: Task 4.1

- [x] **Task 4.3.1**: Add health check metrics
  - `health_checks_total{pool, result="success|failure"}`
  - `connection_removals_total{pool, reason="health_check|timeout|error"}`
  - `health_check_duration_seconds{pool}`
  - File: `src/ts/src/broker/base-pool.ts`

- [x] **Task 4.3.2**: Add integration tests with real database
  - Start Postgres container
  - Acquire connection, kill database, verify removal
  - Restart database, verify new connection created
  - File: `src/ts/tests/integration/pool-health.test.ts` (NEW)

**Acceptance**:
- Health metrics exposed
- Integration test with real database passes

**Phase 4 Checkpoint**: [checkpoint: b018c6a]

## Phase 5: Pool Lifecycle Hooks (Week 8-9)

### Task Group 5.1: Hook API

**Priority**: P1
**Dependencies**: None

- [x] **Task 5.1.1**: Define lifecycle hook interface
  - `onAcquire(token: string) -> Promise<void>`
  - `onRelease(token: string) -> Promise<void>`
  - `onError(token: string, error: Error) -> Promise<void>`
  - `onTimeout(token: string) -> Promise<void>`
  - File: `src/ts/src/broker/base-pool.ts`

- [x] **Task 5.1.2**: Add hook registration methods
  - `setOnAcquire(fn)`, `setOnRelease(fn)`, etc.
  - File: `src/ts/src/broker/base-pool.ts`

- [x] **Task 5.1.3**: Wire hooks into pool lifecycle
  - Call `onAcquire` after acquiring connection
  - Call `onRelease` before releasing connection
  - Call `onError` on connection error
  - Call `onTimeout` on acquire timeout
  - File: `src/ts/src/broker/base-pool.ts`

**Acceptance**:
- All hook types registered correctly
- Hooks called at correct lifecycle points

---

### Task Group 5.2: Idle Reaper

**Priority**: P1
**Dependencies**: None

- [x] **Task 5.2.1**: Add idle timeout tracking
  - Track `last_used` timestamp per connection
  - Update on acquire and release
  - File: `src/ts/src/broker/serve.ts`

- [x] **Task 5.2.2**: Implement idle reaper
  - `startIdleReaper(timeoutMs)` method
  - Periodic scan (every 60s)
  - Close connections idle >5 minutes
  - File: `src/ts/src/broker/base-pool.ts`

- [x] **Task 5.2.3**: Add idle metrics
  - `idle_timeouts_total{pool}`
  - `connection_lease_duration_seconds{pool}` (histogram)
  - File: `src/ts/src/broker/base-pool.ts`

**Acceptance**:
- Idle connections reaped after timeout
- Metrics track idle timeouts

---

### Task Group 5.3: Lifecycle Tests

**Priority**: P1
**Dependencies**: Tasks 5.1, 5.2

- [x] **Task 5.3.1**: Add lifecycle hook unit tests
  - Test each hook is called at correct time
  - Test hook execution order
  - File: `src/ts/tests/unit/lifecycle-hooks.test.ts` (NEW)

- [x] **Task 5.3.2**: Add idle reaper tests
  - Test idle connections removed after timeout
  - Test metrics updated correctly
  - File: `src/ts/tests/unit/idle-reaper.test.ts` (NEW)

**Acceptance**:
- All lifecycle tests pass

**Phase 5 Checkpoint**: [checkpoint: 8faf09c]

## Phase 6: Unified Observability (Week 9-10)

### Task Group 6.1: Metrics Endpoint

**Priority**: P1
**Dependencies**: Phases 4, 5

- [x] **Task 6.1.1**: Create metrics endpoint handler
  - `GET /__metrics__` route
  - Collect metrics from broker, server, MCP
  - File: `src/ts/src/server/metrics.ts` (NEW)

- [x] **Task 6.1.2**: Implement Prometheus formatter
  - Format gauge, counter, histogram metrics
  - Include HELP and TYPE comments
  - Handle label encoding
  - File: `src/ts/src/server/prometheus.ts` (NEW)

- [x] **Task 6.1.3**: Add metrics caching
  - Cache for 5s to reduce overhead
  - File: `src/ts/src/server/metrics.ts`

**Acceptance**:
- `/__metrics__` endpoint works
- Prometheus format valid
- Response time <50ms

---

### Task Group 6.2: Metrics Collectors

**Priority**: P1
**Dependencies**: Task 6.1

- [x] **Task 6.2.1**: Create broker metrics collector
  - Pool-level metrics (connections, acquisition latency, errors)
  - Fairness scheduler metrics (worker wait time, queue length)
  - Health check metrics
  - File: `src/ts/src/broker/metrics.ts` (NEW)

- [x] **Task 6.2.2**: Create server metrics collector
  - HTTP request count, latency, status codes
  - Active connections
  - Request body size histogram
  - File: `src/ts/src/server/metrics.ts`

- [x] **Task 6.2.3**: Create MCP metrics collector
  - Tool invocation count by tool name
  - Tool execution duration (histogram)
  - Tool errors by tool name
  - Transport-specific metrics
  - File: `src/ts/src/mcp/metrics.ts` (NEW)

**Acceptance**:
- All subsystem metrics aggregated
- 20+ metrics present

---

### Task Group 6.3: Grafana Dashboard

**Priority**: P2
**Dependencies**: Task 6.2

- [x] **Task 6.3.1**: Create Grafana dashboard JSON
  - Pool utilization panel
  - Acquisition latency panel
  - Tool invocation rate panel
  - Health check status panel
  - Error rate panel
  - File: `docs/grafana/vertebra-dashboard.json` (NEW)

- [x] **Task 6.3.2**: Add metrics integration test
  - Generate load (tool calls, pool usage)
  - Query `/__metrics__`
  - Verify Prometheus format
  - Verify metric values match actual activity
  - File: `src/ts/tests/integration/metrics.test.ts` (NEW)

**Acceptance**:
- Grafana dashboard loads and displays data
- Integration test validates accuracy

---

## Phase 7: Production Hardening (Week 10-11)

### Task Group 7.1: Error Handling Audit

**Priority**: P0
**Dependencies**: Phases 1-6

- [ ] **Task 7.1.1**: Audit MCP transport error cases
  - Review stdio, SSE, WebSocket error handling
  - Add missing error cases
  - Test error recovery scenarios

- [ ] **Task 7.1.2**: Audit pool error propagation
  - Ensure errors propagate correctly
  - Add timeout handling everywhere
  - Test error recovery scenarios

**Acceptance**:
- All error cases handled gracefully
- Error recovery works

---

### Task Group 7.2: Documentation

**Priority**: P1
**Dependencies**: All phases

- [ ] **Task 7.2.1**: Write MCP integration guide
  - Getting started
  - Tool creation with `@mcp_tool`
  - Transport selection
  - Claude Desktop setup
  - File: `docs/mcp/getting-started.md` (NEW)

- [ ] **Task 7.2.2**: Write pool management guide
  - Health check configuration
  - Lifecycle hooks
  - Idle timeout tuning
  - File: `docs/pools/health-checks.md` (NEW)

- [ ] **Task 7.2.3**: Write observability guide
  - Metrics reference
  - Grafana setup
  - Alerting rules
  - File: `docs/observability/metrics.md` (NEW)

**Acceptance**:
- Documentation complete
- Getting started guides work

---

### Task Group 7.3: Example Applications

**Priority**: P1
**Dependencies**: All phases

- [ ] **Task 7.3.1**: Create MCP tools example
  - 5-10 useful tools (database query, file search, API call)
  - File: `examples/mcp-tools/` (NEW directory)

- [ ] **Task 7.3.2**: Create health check demo
  - App demonstrating pool health features
  - File: `examples/pool-health-demo/` (NEW directory)

**Acceptance**:
- Example apps run successfully

---

### Task Group 7.4: Performance Benchmarks

**Priority**: P1
**Dependencies**: All phases

- [ ] **Task 7.4.1**: Create MCP latency benchmark
  - Target: <5ms P99 for tool invocation
  - File: `benchmarks/mcp-latency.ts` (NEW)

- [ ] **Task 7.4.2**: Create pool latency benchmark
  - Target: <2ms P99 for acquisition
  - File: `benchmarks/pool-latency.ts` (NEW)

- [ ] **Task 7.4.3**: Run benchmarks and document results
  - Verify targets met
  - Document in performance guide

**Acceptance**:
- Benchmarks passing targets
- Performance documented

---

## Summary

**Total Tasks**: 80+
**Estimated Duration**: 11 weeks
**Critical Path**: Phase 1 -> Phase 2 -> Phase 3 (MCP chain)

**Dependencies**:
- Phase 1 (MCP Foundation): No dependencies
- Phase 2 (Python Integration): Depends on Phase 1
- Phase 3 (Advanced MCP): Depends on Phases 1, 2
- Phase 4 (Pool Health): No dependencies (parallel with MCP phases)
- Phase 5 (Pool Lifecycle): No dependencies (parallel with MCP phases)
- Phase 6 (Observability): Depends on Phases 4, 5
- Phase 7 (Hardening): Depends on all phases

**Testing Strategy**:
- Unit tests after each task group
- Integration tests after each phase
- Performance benchmarks after completion

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Ready for Implementation
