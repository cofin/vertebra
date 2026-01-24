# Design: MCP Integration (Phase 4)

**Focus**: Full MCP Protocol support (PRD-7).

## 1. Architecture

**Bun Server (MCP Host)**:
-   Runs `@modelcontextprotocol/sdk`.
-   Handles `JSON-RPC` over Stdio/SSE/WebSocket.
-   Manages `Tool` and `Resource` lists.

**Python (Tool Provider)**:
-   Defines `@tool` functions.
-   Defines `@resource` accessors.
-   Sends schema to Bun on startup (Registration).
-   Executes tools via IPC.

## 2. Registration Protocol

On startup, Python worker scans for `@tool` / `@resource`.
Sends `mcp.register` IPC frame to Bun:
```json
{
  "tools": [
    {
      "name": "query_db",
      "description": "...",
      "inputSchema": { ... }
    }
  ],
  "resources": [ ... ]
}
```
Bun adds these to its internal MCP registry.

## 3. Tool Invocation

1.  **Client**: `call_tool("query_db", { sql: "..." })`
2.  **Bun**:
    -   Validates args against schema.
    -   Sends IPC `mcp.call_tool`: `{ name: "query_db", args: { ... } }`
3.  **Python**:
    -   Receives IPC.
    -   Locates handler.
    -   Executes `await handler(**args)`.
    -   Returns result.
4.  **Bun**:
    -   Wraps result in MCP `CallToolResult`.
    -   Sends JSON-RPC response.

## 4. Schema Generation

**Source**: Python Type Hints.
**Tool**: `vertebra.types.extractor` (PRD-2).
**Conversion**:
-   `TypeNode` -> `JSON Schema` (Draft 2020-12).
-   `docstrings` -> `description`.
-   `Annotated[T, Field(description="...")]` -> Schema description.

**Gap**: We need a `TypeNode.to_json_schema()` method in `src/py/vertebra/types/ir.py`.

## 5. Transports

**Stdio**:
-   Node `process.stdin` / `process.stdout`.
-   Used for local Claude Desktop integration.

**SSE**:
-   `/sse` endpoint.
-   `/messages` POST endpoint.
-   Used for remote/web clients.

**WebSocket**:
-   `/ws` endpoint (JSON-RPC over WS frames).
-   Used for bidirectional clients (Bun native WebSocket support).
