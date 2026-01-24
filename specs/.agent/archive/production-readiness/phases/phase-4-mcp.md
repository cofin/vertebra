# Phase 4: MCP Integration

**Goal**: Enable AI Agent usage.

## Tasks

### Bun Server
- [ ] **P4.1**: Complete `McpServer` in `src/ts/src/mcp/server.ts`.
- [ ] **P4.2**: Implement `ToolsRegistry` and `ResourceRegistry`.
- [ ] **P4.3**: Implement Stdio Transport.
- [ ] **P4.4**: Implement SSE Transport.
- [ ] **P4.5**: Implement WebSocket Transport (Bun native).

### Python <-> Bun
- [ ] **P4.6**: Implement Registration Protocol (Py -> Bun on startup).
- [ ] **P4.7**: Implement `call_tool` IPC handler (Py).
- [ ] **P4.8**: Wire `@tool` / `@resource` decorators to registry.

### Schema
- [ ] **P4.9**: Implement `TypeNode.to_json_schema()` in PRD-2 code.
- [ ] **P4.10**: Auto-generate schemas for registered tools.

## Verification
- Use `inspector` (MCP debugger) or Claude Desktop.
- Connect to `vertebra serve`.
- Verify tools are listed.
- Call tool, verify execution.
