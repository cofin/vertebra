# PRD-7: MCP Integration

**Status**: Draft
**Version**: 1.0
**Created**: 2026-01-17
**Author**: Vertebra Team
**Dependencies**: PRD-1 (Rust IPC Layer), PRD-2 (Python Bindings), PRD-3 (Bun Server)

> **Cross-PRD Decisions**: See [PRD-0 Cross-PRD Architectural Decisions](./PRD-0-foundation.md#cross-prd-architectural-decisions) for serialization strategy (Protobuf for MCP control plane), Python version (3.12+), and protocol priorities.

---

## Executive Summary

### Problem Statement

AI assistants like Claude Desktop, Cursor, and other MCP-compatible clients need a standardized way to access Python-based tools and resources. The Model Context Protocol (MCP) provides this standard, but existing implementations often lack tight integration with modern Python web frameworks or high-performance IPC. Developers building AI-powered applications need:

1. **Type-safe tool definitions** - Python type annotations should automatically generate MCP schemas
2. **High-performance execution** - Tool invocations should complete in milliseconds, not hundreds of milliseconds
3. **Resource pool integration** - Tools need access to shared database connections and other resources
4. **Framework integration** - Should work seamlessly with existing Python frameworks like Litestar
5. **Multiple transport options** - Support for stdio (local), SSE/HTTP (remote), and future transports

### Solution Overview

PRD-7 introduces native MCP integration for Vertebra using a Bun-based MCP server with low-overhead IPC to Python tool and resource handlers. This architecture combines:

- **Bun MCP Server**: JSON-RPC 2.0 protocol handling, multiple transport support (stdio, SSE, Streamable HTTP)
- **Vertebra IPC Bridge**: Sub-millisecond tool invocation via IPC transport (UDS or named pipes) and Protocol Buffers
- **Python Tool/Resource Handlers**: Decorator-based API (`@vertebra.tool`, `@vertebra.resource`)
- **Type Bridge Integration**: Automatic MCP JSON Schema generation from Python type annotations
- **Resource Pool Support**: Shared database connections, caches, and other resources across tools

```
┌─────────────────────────────────────┐
│       Claude / AI Assistants        │
│         (MCP Client)                │
├─────────────────────────────────────┤
│      Bun MCP Server Layer           │
│  (JSON-RPC, transports, schema)     │
├─────────────────────────────────────┤
│      Vertebra IPC Bridge            │
│    (zero-copy tool invocation)      │
├─────────────────────────────────────┤
│     Python Tool/Resource Handlers   │
│   (@vertebra.tool, @vertebra.resource)│
└─────────────────────────────────────┘
```

### Key Innovations

1. **Hybrid Architecture**: Bun handles MCP protocol overhead, Python handles business logic
2. **Low-Overhead IPC**: Target sub-millisecond tool invocation via shared memory and IPC transport
3. **Type Safety**: Python type annotations → MCP JSON Schema (automatic)
4. **Resource Pool Integration**: Tools can access shared database connections via `get_pool()`
5. **Hot Reload**: Tool/resource changes reflected immediately without server restart
6. **Multi-Transport**: stdio for local development, SSE/HTTP for remote clients

### Assumptions and Constraints

- **Cross-platform target**: Linux/macOS/Windows parity for MCP server transports.
- **IPC transport**: UDS on Unix, named pipes or TCP on Windows.
- **MCP spec drift**: spec updates may require protocol adjustments; lock to specific revision per release.

### Success Criteria

| Metric | Target | Rationale |
|--------|--------|-----------|
| Tool invocation overhead | < 5ms | MCP client to Python execution (validate with benchmarks) |
| Schema generation | < 100ms | Python types to JSON Schema |
| Concurrent tool calls | 100+ | Parallel execution support |
| MCP spec compliance | 100% | Works with all MCP clients |
| Hot reload latency | < 200ms | Developer experience |
| Memory overhead | < 10MB | Per MCP server instance |
| Client compatibility | Claude Desktop, Cursor, etc. | Ecosystem integration |

### Estimated Effort

**Total Duration**: 14-16 weeks (3.5-4 months), subject to MCP spec changes

- Phase 1: MCP Server Core (2 weeks)
- Phase 2: Tool/Resource Registry (2 weeks)
- Phase 3: Schema Generation (3 weeks)
- Phase 4: IPC Bridge Integration (2 weeks)
- Phase 5: Advanced Transports (2 weeks)
- Phase 6: Progress & Logging (2 weeks)
- Phase 7: Production Readiness (3 weeks)

---

## Companion Docs

- Task list: `docs/prd/PRD-7-mcp-integration/tasks.md`
- Recovery guide: `docs/prd/PRD-7-mcp-integration/recovery.md`

## Background & Context

### What is MCP?

The **Model Context Protocol (MCP)** is an open protocol developed to standardize how LLM applications integrate with external data sources and tools. The spec has multiple revisions (notably 2024-11-05, 2025-03-26, 2025-06-18), and transport behavior evolves over time. MCP enables:

- **Tool Discovery**: AI assistants can discover available tools at runtime
- **Type-Safe Invocation**: Tools are invoked with validated parameters
- **Resource Access**: Structured access to data entities via URI templates
- **Bidirectional Communication**: Servers can send progress updates and logging

MCP uses **JSON-RPC 2.0** as its wire format, with multiple transport options:

1. **stdio**: Process communication via standard input/output (local development)
2. **Server-Sent Events (SSE)**: HTTP-based streaming for remote clients
3. **Streamable HTTP**: Modern HTTP POST/GET with optional SSE (per 2025-03-26+ revisions)
4. **WebSocket**: Under consideration (not yet in specification)

### Current MCP Ecosystem

**Official SDKs**:
- [Python SDK](https://github.com/modelcontextprotocol/python-sdk): Official Python implementation
- [TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk): Official TypeScript implementation
- [FastMCP](https://github.com/jlowin/fastmcp): Production-ready Python framework with advanced patterns

**Typical MCP Server Architecture**:
```python
from mcp.server import Server
import mcp.types as types

server = Server("my-server")

@server.tool()
async def query_database(query: str) -> dict:
    """Query the database"""
    return await db.execute(query)

# Server runs in Python, handles JSON-RPC directly
```

**Limitations of Current Approach**:
1. **Performance**: Python JSON-RPC parsing overhead varies and must be benchmarked
2. **Concurrency**: Limited by Python's GIL for CPU-bound tools
3. **Resource Sharing**: Each server instance has separate connection pools
4. **Type Generation**: Manual or runtime-only (no static analysis benefits)

### Why Vertebra's Approach is Different

Vertebra separates the MCP protocol layer (Bun) from tool execution (Python):

| Aspect | Traditional MCP | Vertebra MCP |
|--------|----------------|--------------|
| Protocol Handling | Python | Bun (JavaScriptCore) |
| JSON-RPC Parsing | json module (variable) | Bun native (to be benchmarked) |
| Tool Execution | Same process | IPC to Python workers |
| Resource Pools | Per-server | Shared across workers |
| Type Generation | Runtime or manual | Static analysis (AST) |
| Hot Reload | Server restart | Worker restart only |

**Performance Benefits**:
- **Faster JSON-RPC**: Bun's native JSON parsing vs Python (validate in benchmarks)
- **Low-overhead IPC**: Target < 1ms overhead via shared memory + IPC transport
- **Parallel Execution**: Multiple Python workers handle concurrent tools
- **Shared Resources**: Database connections reused across invocations

**Developer Experience Benefits**:
- **Type Safety**: Python type hints automatically become MCP schemas
- **Framework Integration**: Works with existing Litestar applications
- **Familiar API**: Decorator-based, similar to FastAPI/Litestar patterns
- **Hot Reload**: Change tools without restarting MCP server

---

## MCP Protocol Overview

### Core Concepts

#### 1. Tools

**Tools** are executable functions that AI assistants can call. Tools are **model-controlled**, meaning the LLM decides when to invoke them based on context.

**MCP Tool Schema** (JSON Schema):
```json
{
  "name": "query_users",
  "description": "Search users by name or email",
  "inputSchema": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "Search query"
      }
    },
    "required": ["query"]
  }
}
```

**Tool Invocation Flow**:
1. Client sends `tools/call` JSON-RPC request
2. Server validates parameters against schema
3. Server executes tool function
4. Server returns result or error

#### 2. Resources

**Resources** are data entities accessible via URI templates. Resources are **application-controlled**, meaning the host application decides when to load them into context.

**MCP Resource Schema**:
```json
{
  "uri": "users://12345/profile",
  "name": "User Profile",
  "description": "Detailed user profile information",
  "mimeType": "application/json"
}
```

**Resource URI Templates**:
```
users://{user_id}/profile
database://{db_name}/schema
file://{path}
```

**Resource Access Flow**:
1. Client discovers available resources via `resources/list`
2. Client requests specific resource via `resources/read` with URI
3. Server resolves URI and returns data

#### 3. Prompts (Optional)

**Prompts** are predefined instruction templates for common tasks.

**Example Prompt**:
```json
{
  "name": "code_review",
  "description": "Review code for best practices",
  "arguments": [
    {
      "name": "language",
      "description": "Programming language",
      "required": true
    }
  ]
}
```

#### 4. Logging

Servers can send structured log messages to clients:

```json
{
  "method": "notifications/message",
  "params": {
    "level": "info",
    "logger": "tool.query_users",
    "data": "Executed query in 45ms"
  }
}
```

#### 5. Progress

Long-running tools can report progress:

```json
{
  "method": "notifications/progress",
  "params": {
    "progressToken": "task-123",
    "progress": 50,
    "total": 100
  }
}
```

### JSON-RPC 2.0 Wire Format

MCP uses JSON-RPC 2.0 for all communication:

**Request**:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "query_users",
    "arguments": {
      "query": "john"
    }
  }
}
```

**Success Response**:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[{\"id\": 123, \"name\": \"John Doe\"}]"
      }
    ]
  }
}
```

**Error Response**:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32602,
    "message": "Invalid params: missing 'query'"
  }
}
```

### Transport Layers

#### stdio Transport

**Local development and subprocess communication**:

- **Server**: Reads JSON-RPC from stdin, writes to stdout
- **Client**: Launches server as subprocess, communicates via pipes
- **Use Case**: Claude Desktop, local AI tools

```typescript
// Client launches server
const server = spawn('vertebra-mcp', ['myapp:app'], {
  stdio: ['pipe', 'pipe', 'inherit']
});

// Client sends request to server.stdin
server.stdin.write(JSON.stringify(request) + '\n');

// Server responds on stdout
server.stdout.on('data', (data) => {
  const response = JSON.parse(data);
});
```

#### SSE (Server-Sent Events) Transport

**Remote clients, streaming responses**:

- **Server**: HTTP endpoint accepting POST requests
- **Client**: Sends requests via POST, receives via SSE stream
- **Use Case**: Web-based AI assistants, browser extensions

```typescript
// Client establishes SSE connection
const eventSource = new EventSource('/mcp/sse');
eventSource.onmessage = (event) => {
  const response = JSON.parse(event.data);
};

// Client sends requests via POST
fetch('/mcp/messages', {
  method: 'POST',
  body: JSON.stringify(request)
});
```

#### Streamable HTTP Transport (2026-03-26)

**Modern replacement for HTTP+SSE**:

- **Server**: Standard HTTP POST/GET endpoints
- **Client**: Optional SSE for server-initiated messages
- **Benefits**: Simpler than SSE, better firewall compatibility

```typescript
// Client sends request
const response = await fetch('/mcp', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(request)
});

const result = await response.json();
```

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────┐
│              MCP Clients                            │
│  Claude Desktop, Cursor, Custom AI Apps             │
└────────────────┬────────────────────────────────────┘
                 │ JSON-RPC 2.0 (stdio/SSE/HTTP)
                 ▼
┌─────────────────────────────────────────────────────┐
│           Bun MCP Server (TypeScript)               │
│  ┌─────────────────────────────────────────────┐   │
│  │  Transport Layer                            │   │
│  │  - stdio: subprocess stdin/stdout           │   │
│  │  - SSE: EventSource streaming               │   │
│  │  - Streamable HTTP: POST/GET endpoints      │   │
│  └─────────────────┬───────────────────────────┘   │
│                    │                                 │
│  ┌─────────────────▼───────────────────────────┐   │
│  │  JSON-RPC Handler                           │   │
│  │  - Request parsing and validation           │   │
│  │  - Method routing (tools/call, etc.)        │   │
│  │  - Error handling and response formatting   │   │
│  └─────────────────┬───────────────────────────┘   │
│                    │                                 │
│  ┌─────────────────▼───────────────────────────┐   │
│  │  Schema Registry                            │   │
│  │  - Tool/resource schemas (JSON Schema)      │   │
│  │  - Generated from Python type annotations   │   │
│  │  - Hot reload on Python file changes        │   │
│  └─────────────────┬───────────────────────────┘   │
└────────────────────┼─────────────────────────────────┘
                     │ Vertebra IPC (Protocol Buffers)
                     ▼
┌─────────────────────────────────────────────────────┐
│         Vertebra IPC Bridge                         │
│  ┌─────────────────────────────────────────────┐   │
│  │  IPC Client (TypeScript → Rust)            │   │
│  │  - IPC transport connection pool           │   │
│  │  - Protocol Buffer serialization           │   │
│  │  - Request/response correlation            │   │
│  └─────────────────┬───────────────────────────┘   │
└────────────────────┼─────────────────────────────────┘
                     │ IPC transport + Protobuf
                     ▼
┌─────────────────────────────────────────────────────┐
│       Rust Core IPC Layer (PRD-1)                   │
│  - Zero-copy shared memory                          │
│  - Connection pooling                               │
│  - Request routing to Python workers                │
└────────────────────┬────────────────────────────────┘
                     │ PyO3 FFI
                     ▼
┌─────────────────────────────────────────────────────┐
│         Python Worker Pool                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  │
│  │  Worker 1    │  │  Worker 2    │  │ Worker N │  │
│  │              │  │              │  │          │  │
│  │  Tool/       │  │  Tool/       │  │  Tool/   │  │
│  │  Resource    │  │  Resource    │  │ Resource │  │
│  │  Registry    │  │  Registry    │  │ Registry │  │
│  │              │  │              │  │          │  │
│  │  get_pool()  │  │  get_pool()  │  │get_pool()│  │
│  │   ↓          │  │   ↓          │  │    ↓     │  │
│  └───┼──────────┘  └───┼──────────┘  └────┼─────┘  │
│      └─────────────────┴─────────────────┘         │
│                        │                            │
│              Shared Resource Pools                  │
│         (PostgreSQL, Redis, HTTP clients)           │
└─────────────────────────────────────────────────────┘
```

### Request Flow

**Example: Claude Desktop calls `query_users` tool**

1. **MCP Client → Bun Server** (JSON-RPC):
   ```json
   {
     "jsonrpc": "2.0",
     "id": 42,
     "method": "tools/call",
     "params": {
       "name": "query_users",
       "arguments": { "query": "john" }
     }
   }
   ```

2. **Bun Server Processing**:
   - Parse JSON-RPC request (~0.5ms)
   - Validate `query_users` exists in schema registry
   - Validate arguments against JSON Schema
   - Serialize request to Protocol Buffer format

3. **Bun → Rust IPC Bridge** (Protocol Buffers):
   ```protobuf
   message ToolCallRequest {
     string tool_name = 1;
     map<string, bytes> arguments = 2;
     string request_id = 3;
   }
   ```

4. **Rust IPC Layer**:
   - Receive from IPC transport (~0.1ms)
   - Route to available Python worker
   - Forward via PyO3 FFI

5. **Python Worker**:
   ```python
   @vertebra.tool()
   async def query_users(query: str) -> list[User]:
       async with get_pool("postgres").acquire() as conn:
           return await conn.fetch(
               "SELECT * FROM users WHERE name ILIKE $1",
               f"%{query}%"
           )
   ```
   - Deserialize arguments
   - Invoke tool function
   - Serialize result

6. **Response Path** (reverse):
   - Python → Rust → Bun → MCP Client
   - Total overhead: < 5ms (target)

### Component Architecture

#### 1. Bun MCP Server

**Responsibilities**:
- Accept connections via stdio/SSE/HTTP
- Parse and validate JSON-RPC requests
- Maintain schema registry (tools, resources, prompts)
- Route requests to IPC bridge
- Handle errors and format responses
- Support progress notifications and logging

**Implementation** (TypeScript):
```typescript
// src/ts/mcp/server.ts
import { Server } from '@modelcontextprotocol/sdk/server';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio';
import { MCPError, ErrorCode } from '@modelcontextprotocol/sdk/types';

class VertebraMCPServer {
  private server: Server;
  private ipcBridge: IPCBridge;
  private schemaRegistry: SchemaRegistry;

  async initialize() {
    // Load schemas from Python
    await this.schemaRegistry.load();

    // Register tool handlers
    this.server.setRequestHandler('tools/call', async (request) => {
      const { name, arguments: args } = request.params;

      // Validate arguments against schema
      const tool = this.schemaRegistry.getTool(name);
      this.validateArguments(args, tool.inputSchema);

      // Invoke via IPC
      const result = await this.ipcBridge.callTool(name, args);

      return {
        content: [{ type: 'text', text: JSON.stringify(result) }]
      };
    });

    // Register resource handlers
    this.server.setRequestHandler('resources/read', async (request) => {
      const { uri } = request.params;
      const result = await this.ipcBridge.readResource(uri);

      return {
        contents: [{
          uri,
          mimeType: 'application/json',
          text: JSON.stringify(result)
        }]
      };
    });
  }

  async start(transport: 'stdio' | 'sse' | 'http') {
    const transportInstance = this.createTransport(transport);
    await this.server.connect(transportInstance);
  }
}
```

#### 2. Tool/Resource Registry (Python)

**Responsibilities**:
- Provide decorators for marking tools and resources
- Extract type metadata from Python functions
- Store tool/resource definitions
- Support hot reload

**Implementation**:
```python
# src/py/vertebra/mcp/decorators.py
from typing import Callable, TypeVar, Any
from vertebra.types import TypeExtractor

T = TypeVar('T', bound=Callable[..., Any])

def tool(
    name: str | None = None,
    description: str | None = None,
) -> Callable[[T], T]:
    """Mark a function as an MCP tool.

    Example:
        @tool()
        async def query_users(query: str) -> list[User]:
            '''Search users by name or email'''
            ...
    """
    def decorator(func: T) -> T:
        # Extract metadata
        tool_name = name or func.__name__
        tool_desc = description or func.__doc__ or ""

        # Extract type hints for schema generation
        extractor = TypeExtractor()
        params, return_type = extractor.from_function(func)

        # Register tool
        registry.register_tool(
            name=tool_name,
            description=tool_desc,
            handler=func,
            parameters=params,
            return_type=return_type
        )

        return func

    return decorator

def resource(uri_template: str) -> Callable[[T], T]:
    """Mark a function as an MCP resource.

    Example:
        @resource("users://{user_id}/profile")
        async def get_user_profile(user_id: int) -> UserProfile:
            '''Get detailed user profile'''
            ...
    """
    def decorator(func: T) -> T:
        registry.register_resource(
            uri_template=uri_template,
            description=func.__doc__ or "",
            handler=func
        )
        return func

    return decorator
```

#### 3. Schema Generator

**Responsibilities**:
- Convert Python type annotations to MCP JSON Schema
- Support msgspec.Struct, pydantic.BaseModel, dataclasses
- Handle complex types (Optional, Union, Literal, etc.)
- Cache schemas for performance

**Implementation**:
```python
# src/py/vertebra/mcp/schema.py
from vertebra.types import TypeExtractor, TypeNode
import json

class MCPSchemaGenerator:
    def __init__(self):
        self.extractor = TypeExtractor()

    def generate_tool_schema(
        self,
        name: str,
        description: str,
        parameters: dict[str, TypeNode]
    ) -> dict:
        """Generate MCP tool schema from Python types."""

        # Convert TypeNode to JSON Schema
        input_schema = {
            "type": "object",
            "properties": {},
            "required": []
        }

        for param_name, type_node in parameters.items():
            input_schema["properties"][param_name] = \
                self._type_node_to_json_schema(type_node)
            input_schema["required"].append(param_name)

        return {
            "name": name,
            "description": description,
            "inputSchema": input_schema
        }

    def _type_node_to_json_schema(self, node: TypeNode) -> dict:
        """Convert TypeNode to JSON Schema."""
        if isinstance(node, PrimitiveType):
            return {"type": self._primitive_mapping(node.kind)}
        elif isinstance(node, ArrayType):
            return {
                "type": "array",
                "items": self._type_node_to_json_schema(node.item_type)
            }
        elif isinstance(node, ObjectType):
            properties = {}
            required = []
            for field_name, (field_type, is_required) in node.fields.items():
                properties[field_name] = \
                    self._type_node_to_json_schema(field_type)
                if is_required:
                    required.append(field_name)

            return {
                "type": "object",
                "properties": properties,
                "required": required
            }
        elif isinstance(node, UnionType):
            return {"anyOf": [
                self._type_node_to_json_schema(v) for v in node.variants
            ]}
        # ... handle other type nodes
```

#### 4. IPC Bridge Integration

Leverages existing Vertebra IPC infrastructure (PRD-1):

```typescript
// src/ts/mcp/ipc-bridge.ts
import { IpcClient } from '@vertebra/core';

export class MCPIPCBridge {
  private client: IpcClient;

  async callTool(name: string, args: Record<string, any>): Promise<any> {
    // Serialize to Protocol Buffer
    const request = ToolCallRequest.create({
      tool_name: name,
      arguments: this.serializeArguments(args),
      request_id: generateRequestId()
    });

    // Send via IPC (< 1ms overhead target)
    const response = await this.client.send(request);

    // Deserialize response
    return this.deserializeResult(response);
  }

  async readResource(uri: string): Promise<any> {
    const request = ResourceReadRequest.create({ uri });
    const response = await this.client.send(request);
    return this.deserializeResult(response);
  }
}
```

---

## Python API Design

### Tool Definition

```python
from vertebra import tool
from vertebra.resources import get_pool
from msgspec import Struct

class User(Struct):
    id: int
    name: str
    email: str
    created_at: str

@tool()
async def query_users(query: str, limit: int = 10) -> list[User]:
    """Search users by name or email.

    Args:
        query: Search query (name or email)
        limit: Maximum number of results

    Returns:
        List of matching users
    """
    async with get_pool("postgres").acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, email, created_at
            FROM users
            WHERE name ILIKE $1 OR email ILIKE $1
            LIMIT $2
            """,
            f"%{query}%",
            limit
        )
        return [User(**dict(row)) for row in rows]

@tool(name="create_user")
async def create_user_tool(name: str, email: str) -> User:
    """Create a new user.

    Args:
        name: User's full name
        email: User's email address

    Returns:
        Created user object
    """
    async with get_pool("postgres").acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO users (name, email)
            VALUES ($1, $2)
            RETURNING id, name, email, created_at
            """,
            name, email
        )
        return User(**dict(row))
```

### Resource Definition

```python
from vertebra import resource

@resource("users://{user_id}/profile")
async def get_user_profile(user_id: int) -> dict:
    """Get detailed user profile.

    URI Template: users://{user_id}/profile
    Example: users://123/profile
    """
    async with get_pool("postgres").acquire() as conn:
        user = await conn.fetchrow(
            """
            SELECT u.*, COUNT(p.id) as post_count
            FROM users u
            LEFT JOIN posts p ON p.user_id = u.id
            WHERE u.id = $1
            GROUP BY u.id
            """,
            user_id
        )
        return dict(user)

@resource("database://{db_name}/schema")
async def get_database_schema(db_name: str) -> dict:
    """Get database schema information.

    URI Template: database://{db_name}/schema
    """
    async with get_pool(db_name).acquire() as conn:
        tables = await conn.fetch("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
        """)

        schema = {}
        for row in tables:
            table = row['table_name']
            if table not in schema:
                schema[table] = []
            schema[table].append({
                'column': row['column_name'],
                'type': row['data_type']
            })

        return schema
```

### Resource Pool Integration

```python
from vertebra.resources import get_pool

# PostgreSQL connection pool
async with get_pool("postgres").acquire() as conn:
    result = await conn.fetch("SELECT * FROM users")

# Redis cache pool
async with get_pool("redis").acquire() as redis:
    await redis.set("key", "value", ex=3600)

# HTTP client pool
async with get_pool("http").acquire() as client:
    response = await client.get("https://api.example.com/data")
```

**Configuration**:
```python
# vertebra.config.py
from vertebra import configure_pools

configure_pools({
    "postgres": {
        "url": "postgresql://localhost/mydb",
        "min_size": 2,
        "max_size": 10
    },
    "redis": {
        "url": "redis://localhost:6379",
        "max_connections": 50
    },
    "http": {
        "timeout": 30,
        "max_connections": 100
    }
})
```

---

## Implementation Phases

### Phase 1: MCP Server Core (2 weeks)

**Objectives**:
- Implement basic JSON-RPC 2.0 handler
- Add stdio transport support
- Create tool/resource invocation framework

**Deliverables**:
1. JSON-RPC request parser and validator
2. stdio transport implementation
3. Basic tool call handler (echo test)
4. Error handling and response formatting
5. Unit tests for JSON-RPC compliance
6. Integration test: Claude Desktop connection

**Success Criteria**:
- JSON-RPC 2.0 compliance verified
- stdio transport works with Claude Desktop
- Echo tool demonstrates end-to-end flow
- < 1ms JSON-RPC parsing overhead

**Dependencies**: None

### Phase 2: Tool/Resource Registry (2 weeks)

**Objectives**:
- Implement `@vertebra.tool` and `@vertebra.resource` decorators
- Create registry for storing tool/resource metadata
- Build basic schema generation

**Deliverables**:
1. Tool decorator with metadata extraction
2. Resource decorator with URI template support
3. Tool/resource registry (thread-safe)
4. Basic JSON Schema generation (primitives only)
5. Hot reload mechanism (file watching)
6. CLI command to list registered tools/resources

**Success Criteria**:
- Decorators extract function signatures
- Registry stores metadata correctly
- Hot reload updates registry within 200ms
- Basic schemas generated for simple types

**Dependencies**: Phase 1

### Phase 3: Schema Generation (3 weeks)

**Objectives**:
- Integrate with Vertebra Type Bridge (PRD-2)
- Generate complete JSON Schema from Python types
- Support complex types (Struct, BaseModel, Union, etc.)

**Deliverables**:
1. Integration with TypeExtractor (PRD-2)
2. TypeNode → JSON Schema converter
3. Support for msgspec.Struct
4. Support for pydantic.BaseModel
5. Support for dataclasses
6. Support for complex types (Optional, Union, Literal, list, dict)
7. Schema validation tests
8. Documentation for supported types

**Success Criteria**:
- 80%+ of common Python types supported
- Generated schemas validate correctly
- Round-trip: Python type → JSON Schema → validation
- < 100ms schema generation for 50 tools

**Dependencies**: Phase 2, PRD-2

### Phase 4: IPC Bridge Integration (2 weeks)

**Objectives**:
- Connect MCP server to Vertebra IPC layer
- Implement tool invocation via IPC
- Add resource reading via IPC

**Deliverables**:
1. IPC client in TypeScript (reuse from PRD-3)
2. Tool call request serialization (Protocol Buffers)
3. Python IPC handler for tool invocation
4. Response deserialization
5. Error propagation across IPC boundary
6. Performance benchmarks (IPC overhead measurement)

**Success Criteria**:
- Tool invocation works end-to-end
- IPC overhead < 5ms (p95)
- Error handling preserves stack traces
- Concurrent tool calls supported (10+ parallel)

**Dependencies**: Phase 3, PRD-1

### Phase 5: Advanced Transports (2 weeks)

**Objectives**:
- Implement SSE transport
- Implement Streamable HTTP transport
- Add transport selection via configuration

**Deliverables**:
1. SSE transport implementation
2. Streamable HTTP transport
3. Transport configuration system
4. Multiple concurrent client support
5. Transport-specific error handling
6. Documentation for each transport

**Success Criteria**:
- SSE transport works with browser clients
- Streamable HTTP complies with 2026-03-26 spec
- Can switch transports via config
- Multiple clients can connect simultaneously

**Dependencies**: Phase 4

### Phase 6: Progress & Logging (2 weeks)

**Objectives**:
- Implement progress notifications
- Add logging infrastructure
- Support long-running tools

**Deliverables**:
1. Progress notification system
2. Logging integration (structured logs)
3. Long-running tool examples
4. Progress tracking in Python tools
5. Client-side progress display tests
6. Logging configuration

**Success Criteria**:
- Progress updates sent during tool execution
- Logs appear in MCP client
- Long-running tools (> 5s) show progress
- No performance impact when logging disabled

**Dependencies**: Phase 5

### Phase 7: Production Readiness (3 weeks)

**Objectives**:
- Harden error handling
- Add monitoring and metrics
- Create deployment guides
- Performance optimization

**Deliverables**:
1. Comprehensive error handling
2. Prometheus metrics endpoint
3. Health checks
4. Performance tuning (meet < 5ms target)
5. Production deployment guide
6. Claude Desktop integration guide
7. Example MCP servers
8. Migration guide from FastMCP

**Success Criteria**:
- All success criteria met
- 24-hour stability test passes
- Production deployment tested
- Documentation complete
- Example projects functional

**Dependencies**: Phase 6

### Timeline Summary

| Phase | Duration | Dependencies | Cumulative |
|-------|----------|--------------|------------|
| Phase 1 | 2 weeks | None | 2 weeks |
| Phase 2 | 2 weeks | Phase 1 | 4 weeks |
| Phase 3 | 3 weeks | Phase 2, PRD-2 | 7 weeks |
| Phase 4 | 2 weeks | Phase 3, PRD-1 | 9 weeks |
| Phase 5 | 2 weeks | Phase 4 | 11 weeks |
| Phase 6 | 2 weeks | Phase 5 | 13 weeks |
| Phase 7 | 3 weeks | Phase 6 | 16 weeks |

**Total Estimated Time**: 16 weeks (4 months)

---

## Testing Strategy

### Unit Tests

**TypeScript (Bun Test)**:
- JSON-RPC parsing and validation
- Schema registry operations
- Transport layer (stdio, SSE, HTTP)
- IPC client communication
- Error handling

**Python (pytest)**:
- Decorator functionality
- Schema generation
- Tool/resource invocation
- IPC handler
- Resource pool integration

**Coverage Target**: 85%+ line coverage

### Integration Tests

**End-to-End Flows**:
1. Claude Desktop → stdio → Tool invocation → Response
2. Browser client → SSE → Resource read → Response
3. HTTP client → Streamable HTTP → Tool call → Response
4. Long-running tool with progress updates
5. Error scenarios (invalid params, tool crash, timeout)

**Test Applications**:
- Simple MCP server with 5 tools
- MCP server with database integration
- MCP server with external API calls
- Resource-heavy server (10+ resources)

### Performance Tests

**Benchmarks**:
1. **JSON-RPC Parsing**: < 1ms (p95)
2. **Schema Generation**: < 100ms for 50 tools
3. **IPC Overhead**: < 5ms (p95)
4. **Tool Invocation (simple)**: < 10ms end-to-end
5. **Concurrent Tool Calls**: 100+ parallel without degradation
6. **Hot Reload**: < 200ms

**Load Testing**:
- 1000 tool calls/second sustained
- 100 concurrent clients (SSE/HTTP)
- Memory stability over 24 hours

### Compatibility Tests

**MCP Clients**:
- Claude Desktop (latest)
- Cursor IDE
- Custom MCP client (reference implementation)

**Python Versions**:
- Python 3.12
- Python 3.13

**Operating Systems**:
- Linux (Ubuntu 22.04, 24.04)
- macOS (13+)
- Windows (native target; WSL2 acceptable fallback)

---

## Performance Requirements

### Latency Targets

| Operation | Target (p95) | Rationale |
|-----------|--------------|-----------|
| JSON-RPC parsing | < 1ms | Bun native JSON |
| Schema validation | < 0.5ms | Pre-compiled schemas |
| IPC overhead | < 5ms | IPC transport + Protobuf |
| Tool invocation (simple) | < 10ms | End-to-end excluding Python execution |
| Schema generation | < 100ms | For 50 tools (development) |
| Hot reload | < 200ms | File change to registry update |

### Throughput Targets

| Metric | Target | Rationale |
|--------|--------|-----------|
| Tool calls/second | 1000+ | Multiple workers, concurrent execution |
| Concurrent clients | 100+ | SSE/HTTP transport |
| Concurrent tool executions | 100+ | Limited by Python workers, not MCP server |

### Resource Efficiency

| Metric | Target | Rationale |
|--------|--------|-----------|
| Memory (MCP server) | < 10MB | Bun runtime overhead |
| Memory (per worker) | < 20MB | Python interpreter + application |
| CPU (idle) | < 1% | Event-driven architecture |

---

## Security Considerations

### Input Validation

1. **JSON-RPC Validation**:
   - Schema validation before IPC
   - Parameter type checking
   - Size limits on requests (1MB default)

2. **URI Template Validation**:
   - Prevent path traversal
   - Whitelist allowed resource patterns
   - Sanitize user-provided URI components

3. **Tool Parameter Sanitization**:
   - Type coercion with validation
   - SQL injection prevention (parameterized queries)
   - Command injection prevention (no shell execution)

### Transport Security

1. **stdio Transport**:
   - Local only (same machine)
   - Process isolation via OS

2. **SSE/HTTP Transports**:
   - Optional TLS termination
   - Authentication via headers (Bearer tokens)
   - CORS configuration
   - Rate limiting

### Resource Pool Security

1. **Connection Strings**:
   - Environment variables only
   - No hardcoded credentials
   - Vault integration (optional)

2. **Access Control**:
   - Tools specify required pools
   - Permission checks before acquisition
   - Audit logging for pool access

### Error Handling

1. **Information Leakage**:
   - Sanitize error messages (no stack traces in production)
   - Generic errors for authentication failures
   - Detailed errors only in development mode

2. **DoS Prevention**:
   - Request timeout (30s default)
   - Concurrent request limits
   - Memory limits per tool invocation

---

## Documentation

### User Documentation

1. **Getting Started Guide**:
   - Installation
   - First MCP server
   - Connecting Claude Desktop
   - Basic tool creation

2. **API Reference**:
   - @vertebra.tool decorator
   - @vertebra.resource decorator
   - Resource pool API
   - Configuration options

3. **Type System Guide**:
   - Supported Python types
   - JSON Schema mapping
   - Complex type examples
   - Custom type handlers

4. **Transport Guide**:
   - stdio for local development
   - SSE for browser clients
   - Streamable HTTP for production
   - Configuration examples

### Developer Documentation

1. **Architecture Overview**:
   - Component diagram
   - Data flow
   - IPC protocol

2. **Contributing Guide**:
   - Development setup
   - Running tests
   - Adding transports
   - Extending type support

3. **Performance Tuning**:
   - Benchmarking tools
   - Profiling Python tools
   - Optimizing IPC
   - Resource pool configuration

### Migration Guides

1. **From FastMCP**:
   - API differences
   - Schema generation changes
   - Migration script
   - Common patterns

2. **From Official Python SDK**:
   - Decorator mapping
   - Transport configuration
   - Feature parity table

---

## Research & References

### MCP Protocol Specification

- https://modelcontextprotocol.io/specification/2025-06-18/basic
- https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
- https://github.com/modelcontextprotocol/modelcontextprotocol

### MCP Features & Concepts

- https://modelcontextprotocol.io/specification/2025-06-18/basic/tools
- https://modelcontextprotocol.io/specification/2025-06-18/basic/resources

### Python MCP Implementations

- https://github.com/modelcontextprotocol/python-sdk
- https://github.com/modelcontextprotocol/typescript-sdk

### MCP Transports

- https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
- https://modelcontextprotocol.io/specification/2025-06-18/basic/transports/streamable_http

### Related Research

- https://ai-sdk.dev/docs/ai-sdk-core/mcp-tools

---

## Appendix A: Example MCP Server

### Complete Example

```python
# myapp_mcp.py
from vertebra import tool, resource
from vertebra.resources import get_pool
from msgspec import Struct
from typing import Literal

class User(Struct):
    id: int
    name: str
    email: str
    role: Literal["admin", "user", "guest"]

class SearchResult(Struct):
    users: list[User]
    total: int
    page: int

@tool()
async def search_users(
    query: str,
    role: Literal["admin", "user", "guest"] | None = None,
    page: int = 1,
    page_size: int = 10
) -> SearchResult:
    """Search users with pagination and optional role filter.

    Args:
        query: Search query (matches name or email)
        role: Optional role filter
        page: Page number (1-indexed)
        page_size: Results per page

    Returns:
        Search results with pagination info
    """
    offset = (page - 1) * page_size

    async with get_pool("postgres").acquire() as conn:
        # Build query
        sql = """
            SELECT id, name, email, role
            FROM users
            WHERE (name ILIKE $1 OR email ILIKE $1)
        """
        params = [f"%{query}%"]

        if role:
            sql += " AND role = $2"
            params.append(role)

        # Get total count
        count_sql = f"SELECT COUNT(*) FROM ({sql}) AS subquery"
        total = await conn.fetchval(count_sql, *params)

        # Get page results
        sql += f" LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        params.extend([page_size, offset])

        rows = await conn.fetch(sql, *params)
        users = [User(**dict(row)) for row in rows]

        return SearchResult(
            users=users,
            total=total,
            page=page
        )

@resource("users://{user_id}/profile")
async def get_user_profile(user_id: int) -> dict:
    """Get complete user profile with statistics."""
    async with get_pool("postgres").acquire() as conn:
        profile = await conn.fetchrow("""
            SELECT
                u.*,
                COUNT(DISTINCT p.id) as post_count,
                COUNT(DISTINCT c.id) as comment_count,
                MAX(p.created_at) as last_post_date
            FROM users u
            LEFT JOIN posts p ON p.user_id = u.id
            LEFT JOIN comments c ON c.user_id = u.id
            WHERE u.id = $1
            GROUP BY u.id
        """, user_id)

        return dict(profile)

@tool()
async def create_user(name: str, email: str, role: Literal["user", "guest"] = "user") -> User:
    """Create a new user."""
    async with get_pool("postgres").acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO users (name, email, role)
            VALUES ($1, $2, $3)
            RETURNING id, name, email, role
        """, name, email, role)

        return User(**dict(row))
```

### Running the Server

```bash
# Start with stdio transport (for Claude Desktop)
vertebra-mcp myapp_mcp:app --transport stdio

# Start with HTTP transport (for remote clients)
vertebra-mcp myapp_mcp:app --transport http --host 0.0.0.0 --port 8080
```

### Claude Desktop Configuration

```json
{
  "mcpServers": {
    "my-app": {
      "command": "vertebra-mcp",
      "args": ["myapp_mcp:app", "--transport", "stdio"]
    }
  }
}
```

---

## Appendix B: Generated JSON Schema

### Tool Schema Example

From the `search_users` tool above:

```json
{
  "name": "search_users",
  "description": "Search users with pagination and optional role filter.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "Search query (matches name or email)"
      },
      "role": {
        "anyOf": [
          {
            "type": "string",
            "enum": ["admin", "user", "guest"]
          },
          {
            "type": "null"
          }
        ],
        "description": "Optional role filter"
      },
      "page": {
        "type": "integer",
        "default": 1,
        "description": "Page number (1-indexed)"
      },
      "page_size": {
        "type": "integer",
        "default": 10,
        "description": "Results per page"
      }
    },
    "required": ["query"]
  }
}
```

### Resource Schema Example

```json
{
  "uri": "users://123/profile",
  "name": "User Profile",
  "description": "Get complete user profile with statistics",
  "mimeType": "application/json"
}
```

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-17 | Vertebra Team | Initial PRD |

---

**Approvals**:

- [ ] Technical Lead
- [ ] Product Owner
- [ ] Engineering Manager

**Status**: Draft - Pending Review

---

**End of PRD-7**
