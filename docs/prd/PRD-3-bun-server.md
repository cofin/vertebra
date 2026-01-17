# PRD-3: Bun Server and Protocol Support

**Status**: Draft
**Author**: Vertebra Team
**Created**: 2026-01-17
**Last Updated**: 2026-01-17
**Version**: 1.0

---

## Executive Summary

This PRD outlines the design and implementation of Vertebra's high-performance Bun-based HTTP server with multi-protocol support and zero-copy IPC to Python ASGI applications. The primary goal is to outperform Granian (currently the fastest ASGI server at 50,000 req/s) by leveraging Bun's exceptional performance characteristics (89,000+ req/s in benchmarks) while maintaining compatibility with existing Litestar and ASGI applications.

The architecture consists of three layers: (1) Bun HTTP server handling TLS termination, HTTP parsing, and protocol negotiation, (2) an IPC bridge using Unix Domain Sockets and Protocol Buffers for sub-millisecond overhead, and (3) a Python worker pool running Litestar ASGI handlers. This hybrid approach combines Bun's superior HTTP performance with Python's rich ecosystem and existing application code.

**Success Criteria**:
- Throughput exceeding 50,000 requests/second (Granian baseline)
- IPC bridge overhead under 1 millisecond (p95)
- Support for 10,000+ concurrent connections
- Full protocol support: HTTP/1.1, HTTP/2, HTTP/3, WebSocket, gRPC, JSON-RPC 2.0, Server-Sent Events, and h2c
- Type-safe client generation from Python routes
- Drop-in replacement for Granian/Uvicorn (CLI compatibility)

---

## Background & Motivation

### Current Landscape

Modern Python ASGI servers have reached impressive performance levels, with Granian leading at 50,000 req/s through its Rust implementation. However, several limitations persist:

1. **Protocol limitations**: Most ASGI servers support only HTTP/1.1, HTTP/2, and WebSocket. Advanced protocols like gRPC and JSON-RPC require separate implementations or proxies.
2. **Static file performance**: Python-based servers struggle with static file serving compared to specialized servers.
3. **Type safety**: Cross-language type generation is not standard, leading to potential runtime errors.
4. **Memory efficiency**: Each worker consumes 15-30MB of memory, limiting density.

### Why Bun?

Bun has emerged as a breakthrough JavaScript runtime with exceptional performance characteristics:

- **89,234 req/s** for simple HTTP workloads (vs Node.js 52,341 req/s)
- **1,098,770 WebSocket messages/second** (vs Node.js 179,186) - 6-7x faster
- **JavaScriptCore engine** providing 4x HTTP throughput over V8
- **io_uring on Linux** for optimized I/O operations
- **Built on uWebSockets** for high-performance WebSocket handling
- **Native HTTP/2 support** with server push capabilities
- **gRPC.js compatibility** since v1.0.13

Recent benchmarks (January 2026) show Bun competing directly with Rust frameworks in microservice workloads, with 157ms mean response time and 0% failure rate under heavy load.

### The Hybrid Architecture Advantage

By combining Bun's front-end HTTP performance with Python's ASGI ecosystem, Vertebra aims to:

1. **Maximize throughput**: Bun handles HTTP parsing, TLS termination, and static files at native speed
2. **Minimize latency**: Unix Domain Sockets provide 30-66% lower latency than TCP localhost
3. **Extend protocols**: Native gRPC, JSON-RPC, and advanced protocol support
4. **Preserve compatibility**: Existing Litestar apps work without modification
5. **Enable type safety**: TypeScript code generation from Python routes

### Performance Targets

Based on research into Bun's capabilities and IPC optimization techniques, Vertebra targets:

| Metric | Target | Baseline (Granian) |
|--------|--------|-------------------|
| Throughput (req/s) | 60,000+ | 50,000 |
| Latency p50 (ms) | < 2 | 2-3 |
| Latency p95 (ms) | < 10 | 12-15 |
| IPC overhead (ms) | < 1 | N/A |
| Memory/worker (MB) | ≤ 20 | 15 |
| Concurrent connections | 10,000+ | 5,000+ |
| WebSocket msg/s | 500,000+ | N/A |

The key challenge is ensuring IPC overhead doesn't negate Bun's performance advantage. Using Unix Domain Sockets (2-3μs latency), Protocol Buffers serialization (2x faster than JSON), and connection pooling, we target sub-millisecond IPC overhead.

---

## Technical Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────┐
│                   Bun HTTP Server                       │
│  ┌─────────────────────────────────────────────────┐   │
│  │  TLS Termination & Protocol Negotiation         │   │
│  │  HTTP/1.1 | HTTP/2 | HTTP/3 | WebSocket          │   │
│  └─────────────────────┬───────────────────────────┘   │
│                        │                                 │
│  ┌─────────────────────▼───────────────────────────┐   │
│  │           Request Router                         │   │
│  │  ┌─────────────┐  ┌─────────────────────────┐  │   │
│  │  │ Static Files│  │  API Endpoints          │  │   │
│  │  │ (Bun native)│  │  (→ Python via IPC)     │  │   │
│  │  └─────────────┘  └──────────┬──────────────┘  │   │
│  └──────────────────────────────┼──────────────────┘   │
└─────────────────────────────────┼──────────────────────┘
                                  │
                  ┌───────────────▼───────────────┐
                  │    Vertebra IPC Bridge        │
                  │                               │
                  │  ┌─────────────────────────┐ │
                  │  │  Unix Domain Socket     │ │
                  │  │  Protocol Buffers       │ │
                  │  │  Connection Pool        │ │
                  │  └─────────┬───────────────┘ │
                  └────────────┼───────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                     │
    ┌─────▼──────┐      ┌─────▼──────┐      ┌─────▼──────┐
    │  Worker 1  │      │  Worker 2  │      │  Worker N  │
    │            │      │            │      │            │
    │  asyncio   │      │  asyncio   │      │  asyncio   │
    │  event loop│      │  event loop│      │  event loop│
    │            │      │            │      │            │
    │  Litestar  │      │  Litestar  │      │  Litestar  │
    │  ASGI App  │      │  ASGI App  │      │  ASGI App  │
    └────────────┘      └────────────┘      └────────────┘
```

### Component Architecture

#### 1. Bun HTTP Server

**Responsibilities**:
- Accept incoming connections on configured host:port
- TLS termination using native Bun TLS support
- HTTP protocol parsing (HTTP/1.1, HTTP/2, HTTP/3)
- Protocol negotiation (ALPN for HTTP/2, gRPC)
- WebSocket upgrade handling
- Request routing (static vs dynamic)
- Static file serving with caching
- Response streaming back to clients

**Technology Stack**:
- Bun runtime (v1.0.13+)
- Native HTTP/2 server
- uWebSockets-based WebSocket implementation
- @grpc/grpc-js for gRPC support
- io_uring for optimized Linux I/O

**Key Design Decisions**:
- Single-threaded event loop per Bun process
- Connection pooling to Python workers
- Zero-copy static file serving
- Early request validation to prevent unnecessary IPC

#### 2. IPC Bridge

**Responsibilities**:
- Serialize HTTP requests to Protocol Buffer format
- Transmit requests to Python workers via Unix Domain Sockets
- Manage connection pool to workers
- Deserialize Protocol Buffer responses
- Handle timeouts and failures
- Implement backpressure when workers are saturated
- Maintain bidirectional WebSocket frame forwarding

**Protocol Design**:

Using Protocol Buffers for type-safe, high-performance serialization:

```protobuf
// asgi.proto
syntax = "proto3";

package vertebra.asgi;

message ASGIRequest {
  string type = 1;  // "http" or "websocket"
  string http_version = 2;
  string method = 3;
  string path = 4;
  bytes raw_path = 5;
  bytes query_string = 6;
  map<string, string> headers = 7;
  string scheme = 8;
  string server_host = 9;
  int32 server_port = 10;
  string client_host = 11;
  int32 client_port = 12;
  bytes body = 13;
  bool more_body = 14;
  map<string, bytes> extensions = 15;
  string connection_id = 16;  // for WebSocket
}

message ASGIResponse {
  string type = 1;  // "http.response.start" or "http.response.body"
  int32 status = 2;
  map<string, string> headers = 3;
  bytes body = 4;
  bool more_body = 5;
  map<string, bytes> extensions = 6;
}

message WebSocketMessage {
  string connection_id = 1;
  string type = 2;  // "websocket.send", "websocket.receive", "websocket.close"
  bytes bytes_data = 3;
  string text_data = 4;
  int32 code = 5;
  string reason = 6;
}
```

**IPC Transport**:

Unix Domain Sockets provide optimal performance for same-host IPC:
- **30-66% lower latency** than TCP localhost
- **7x greater throughput** in benchmarks
- **2-3 microseconds latency** vs 3.6μs for TCP loopback
- No IP stack overhead (no headers, checksums, routing)

Socket location: `/tmp/vertebra-{pid}.sock` with permissions 0600 (owner only).

**Connection Pooling**:

Maintain a pool of persistent UDS connections to each worker:
- Initial pool size: 2 connections per worker
- Max pool size: 10 connections per worker
- Connection reuse for request pipelining
- Automatic reconnection on worker restart
- Load balancing: round-robin with least-connections fallback

**Shared Memory Optimization** (Future):

For large request/response bodies (> 1MB threshold):
1. Allocate shared memory segment
2. Write body data to shared memory
3. Send pointer and length in Protocol Buffer message
4. Zero-copy access from Python
5. Release shared memory after processing

This optimization targets file uploads, large JSON payloads, and streaming responses.

#### 3. Python Worker Pool

**Responsibilities**:
- Accept IPC connections from Bun server
- Deserialize Protocol Buffer requests to ASGI dict format
- Invoke Litestar ASGI application
- Serialize ASGI responses to Protocol Buffers
- Handle async I/O efficiently with asyncio event loop
- Implement graceful shutdown on SIGTERM
- Report health status to Bun server

**Worker Architecture**:

Each worker is a separate Python process with:
- Single asyncio event loop
- Unix Domain Socket server listening for IPC
- ASGI application loaded at startup
- Request handler coroutine pool
- Graceful shutdown handler

**Worker Lifecycle**:

```python
async def worker_main(worker_id: int, socket_path: str, app_path: str):
    # Load ASGI application
    app = import_app(app_path)  # e.g., "myapp:app"

    # Create UDS server
    server = await asyncio.start_unix_server(
        handle_connection,
        path=socket_path
    )

    async with server:
        # Register signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, graceful_shutdown)

        # Serve requests
        await server.serve_forever()

async def handle_connection(reader, writer):
    while True:
        # Read Protocol Buffer request
        msg_len = await reader.readexactly(4)
        length = struct.unpack('>I', msg_len)[0]
        pb_data = await reader.readexactly(length)

        request = ASGIRequest.FromString(pb_data)

        # Convert to ASGI scope
        scope = protobuf_to_asgi_scope(request)

        # Invoke ASGI app
        response_queue = asyncio.Queue()
        await app(scope, receive, response_queue.put)

        # Serialize response
        while True:
            response_msg = await response_queue.get()
            pb_response = asgi_to_protobuf(response_msg)

            # Write length-prefixed message
            data = pb_response.SerializeToString()
            writer.write(struct.pack('>I', len(data)))
            writer.write(data)
            await writer.drain()

            if response_msg.get('type') == 'http.response.body' and \
               not response_msg.get('more_body', False):
                break
```

**Worker Pool Management**:

Bun server spawns and manages worker processes:
- Worker count: configurable (default: CPU count)
- Auto-restart on crash with exponential backoff
- Health checks: periodic ping via IPC (5s interval)
- Graceful shutdown: SIGTERM with 30s timeout, then SIGKILL
- Rolling restart: replace workers one at a time

**Resource Limits**:

Per-worker limits enforced via cgroups or ulimit:
- Memory: 512MB default (configurable)
- File descriptors: 1024 (configurable)
- CPU: no limit (shares fairly)
- Request timeout: 30s default (configurable)

#### 4. Protocol Handlers

**HTTP/1.1 & HTTP/2**:

Bun's native HTTP server handles both protocols transparently:
- HTTP/2 negotiation via ALPN (TLS) or h2c (cleartext)
- Server push support (future enhancement)
- Flow control and stream prioritization
- Header compression (HPACK)

**HTTP/3** (Future):

HTTP/3 over QUIC support pending Bun implementation:
- Monitor Bun roadmap for HTTP/3 support
- Alternative: Integrate standalone HTTP/3 library
- Maintain HTTP/2 fallback for compatibility

**WebSocket**:

Leveraging Bun's uWebSockets-based implementation:
- 1,098,770 messages/second throughput
- 48MB memory for 10k connections (vs Node.js 125MB)
- 6ms latency (vs Node.js 15ms)

WebSocket lifecycle:
1. Client sends WebSocket upgrade request
2. Bun validates and accepts upgrade
3. Bun forwards upgrade to Python via IPC
4. Python ASGI app accepts/rejects
5. Bidirectional frame forwarding: Bun ↔ IPC ↔ Python
6. Connection close initiated from either side

**gRPC**:

Using @grpc/grpc-js library with Bun's HTTP/2 support:
- Protocol Buffers service definitions
- Server reflection for introspection
- Streaming RPCs (client, server, bidirectional)
- Metadata and error handling
- Integration with Python gRPC services via IPC

**JSON-RPC 2.0**:

Custom implementation over HTTP:
- POST endpoint accepting JSON-RPC requests
- Method routing to Python handlers
- Batch request support
- Error response standardization
- Notification (one-way) support

**Server-Sent Events (SSE)**:

HTTP-based event streaming:
- `text/event-stream` content type
- Event ID, event type, and data fields
- Automatic reconnection support
- Integration with Python async generators

**h2c (HTTP/2 Cleartext)**:

HTTP/2 without TLS:
- Upgrade from HTTP/1.1 via `Upgrade: h2c` header
- Direct h2c connection with prior knowledge
- Useful for internal services and development

---

## Feature Requirements

### 1. Protocol Support

**HTTP/1.1** (Priority: P0)
- Full HTTP/1.1 spec compliance
- Keep-alive and pipelining
- Chunked transfer encoding
- Range requests for static files
- Compression (gzip, brotli, deflate)

**HTTP/2** (Priority: P0)
- ALPN negotiation over TLS
- h2c upgrade from HTTP/1.1
- Stream multiplexing
- Header compression (HPACK)
- Flow control and prioritization
- Server push (P1 - future)

**HTTP/3** (Priority: P2)
- Pending Bun native support
- QUIC transport
- 0-RTT connection establishment
- Improved performance on lossy networks

**WebSocket** (Priority: P0)
- RFC 6455 compliance
- Binary and text frames
- Ping/pong heartbeat
- Compression extensions (permessage-deflate)
- Subprotocol negotiation
- Maximum message size limits

**gRPC** (Priority: P1)
- Unary RPCs
- Server streaming
- Client streaming
- Bidirectional streaming
- Metadata handling
- Status codes and error details
- Server reflection API
- Health checking protocol

**JSON-RPC 2.0** (Priority: P1)
- Request/response format
- Notification support
- Batch requests
- Error handling
- Method routing

**Server-Sent Events** (Priority: P1)
- Event stream format
- Event ID and types
- Automatic reconnection
- CORS support

**h2c (HTTP/2 Cleartext)** (Priority: P2)
- Upgrade mechanism
- Prior knowledge mode
- Development and testing use cases

### 2. Static File Serving

**Requirements** (Priority: P0):
- Efficient static file serving via Bun
- Configurable root directory
- MIME type detection
- ETag generation and validation
- Last-Modified headers
- Cache-Control headers
- Range request support (partial content)
- Compression (gzip, brotli)
- Directory listing (optional, disabled by default)
- Index file serving (index.html)

**Performance Targets**:
- Near-zero overhead (Bun native)
- 100,000+ static req/s for small files
- Streaming for large files (> 10MB)

### 3. Type-Safe Client Generation

**Requirements** (Priority: P1):
- Introspect Litestar routes at runtime
- Generate TypeScript client with type-safe methods
- Support for request/response types
- Path parameter and query string types
- Header and cookie types
- WebSocket client generation
- In-memory generation (no file I/O during development)
- Optional file output for production builds

**Example**:

Python route:
```python
@get("/users/{user_id:int}")
async def get_user(user_id: int) -> User:
    ...
```

Generated TypeScript client:
```typescript
const client = createClient<typeof app>();

// Type-safe method with IntelliSense
const user: User = await client.users.get({ user_id: 123 });
```

### 4. Configuration System

**Configuration Sources** (Priority: P0):
1. Configuration file (vertebra.config.ts)
2. CLI arguments
3. Environment variables

**Priority**: CLI args > env vars > config file > defaults

**Configuration Schema**:

```typescript
interface VertebraConfig {
  server: {
    host: string;           // default: "127.0.0.1"
    port: number;           // default: 8000
    workers: number | "auto"; // default: "auto" (CPU count)
    tls?: {
      cert: string;         // path to certificate
      key: string;          // path to private key
      ca?: string;          // path to CA bundle
    };
  };

  python: {
    app: string;            // required: "module:app"
    workers: number;        // default: same as server.workers
    timeout: number;        // default: 30 (seconds)
    venv?: string;          // path to virtual environment
  };

  protocols: {
    http2: boolean;         // default: true
    http3: boolean;         // default: false
    websocket: boolean;     // default: true
    grpc: boolean;          // default: false
    jsonrpc: boolean;       // default: false
  };

  static?: {
    dir: string;            // path to static files directory
    maxAge: number;         // cache max-age in seconds
    compression: boolean;   // enable gzip/brotli
  };

  ipc: {
    socketPath?: string;    // default: /tmp/vertebra-{pid}.sock
    protocol: "protobuf" | "msgpack"; // default: "protobuf"
    poolSize: number;       // default: 2
    poolMax: number;        // default: 10
    sharedMemoryThreshold?: number; // bytes, default: 1048576 (1MB)
  };

  monitoring: {
    metrics: boolean;       // default: true
    metricsPath: string;    // default: "/metrics"
    health: boolean;        // default: true
    healthPath: string;     // default: "/health"
    tracing: boolean;       // default: false
  };

  limits: {
    maxHeaderSize: number;      // default: 16384 (16KB)
    maxBodySize: number;        // default: 10485760 (10MB)
    maxConnections: number;     // default: 10000
    requestTimeout: number;     // default: 30 (seconds)
    keepAliveTimeout: number;   // default: 5 (seconds)
  };
}
```

### 5. CLI Interface

**Basic Usage** (Priority: P0):
```bash
vertebra [OPTIONS] APP
```

**Arguments**:
- `APP`: Python ASGI application in "module:app" format (required)

**Options**:
```
Server Options:
  --host TEXT              Bind socket to this host [default: 127.0.0.1]
  --port INTEGER           Bind socket to this port [default: 8000]
  --workers INTEGER        Number of worker processes [default: auto]
  --reload                 Enable auto-reload on code changes

SSL/TLS Options:
  --ssl-cert PATH          Path to SSL certificate
  --ssl-key PATH           Path to SSL key
  --ssl-ca PATH            Path to CA bundle

Protocol Options:
  --http2 / --no-http2     Enable HTTP/2 [default: enabled]
  --grpc / --no-grpc       Enable gRPC [default: disabled]
  --ws / --no-ws           Enable WebSocket [default: enabled]

Static Files:
  --static-dir PATH        Serve static files from directory
  --static-max-age INTEGER Cache max-age in seconds [default: 3600]

Python Options:
  --python-workers INTEGER Number of Python workers [default: auto]
  --python-timeout INTEGER Request timeout in seconds [default: 30]
  --venv PATH              Python virtual environment path

Development:
  --log-level TEXT         Log level [default: info]
  --access-log             Enable access logging
  --debug                  Enable debug mode

Configuration:
  --config PATH            Load configuration from file

Other:
  --version                Show version and exit
  --help                   Show this message and exit
```

**Examples**:

Development server:
```bash
vertebra myapp:app --reload --debug
```

Production server with TLS:
```bash
vertebra myapp:app \
  --host 0.0.0.0 \
  --port 443 \
  --workers 4 \
  --ssl-cert /etc/ssl/cert.pem \
  --ssl-key /etc/ssl/key.pem \
  --http2
```

With static files and gRPC:
```bash
vertebra myapp:app \
  --static-dir ./static \
  --grpc \
  --workers auto
```

Using configuration file:
```bash
vertebra myapp:app --config vertebra.config.ts
```

### 6. Monitoring & Observability

**Metrics Endpoint** (Priority: P1):
- Prometheus-compatible metrics at `/metrics`
- Request count, duration histograms
- Active connections
- Worker status and health
- IPC bridge metrics (latency, throughput)
- Error rates

**Health Check Endpoint** (Priority: P0):
- Liveness probe at `/health/live`
- Readiness probe at `/health/ready`
- Worker pool status
- IPC bridge connectivity

**Logging** (Priority: P0):
- Structured JSON logging
- Configurable log levels
- Access logs (optional)
- Error logs with stack traces
- Worker process logs

**Tracing** (Priority: P2):
- OpenTelemetry integration
- Distributed tracing across IPC boundary
- Span attributes for HTTP requests
- Trace context propagation

---

## Performance Requirements

### Throughput Targets

| Workload Type | Target (req/s) | Baseline (Granian) |
|---------------|----------------|-------------------|
| Hello World (GET) | 60,000+ | 50,000 |
| JSON API (small payload) | 55,000+ | 45,000 |
| Static files (< 1KB) | 100,000+ | N/A |
| WebSocket messages | 500,000+ | N/A |

### Latency Targets

| Metric | Target | Baseline (Granian) |
|--------|--------|-------------------|
| p50 latency | < 2ms | 2-3ms |
| p95 latency | < 10ms | 12-15ms |
| p99 latency | < 25ms | 30-35ms |
| IPC overhead (added) | < 1ms | N/A |

### Concurrency Targets

- **Concurrent connections**: 10,000+ sustained
- **WebSocket connections**: 5,000+ per worker
- **Requests in flight**: 1,000+ per worker

### Resource Efficiency

| Metric | Target | Baseline (Granian) |
|--------|--------|-------------------|
| Memory per worker | ≤ 20MB | 15MB |
| CPU usage (idle) | < 1% | < 1% |
| File descriptors | < 1024 per worker | < 1024 |

### Scalability

- **Horizontal scaling**: Linear scaling up to 16 workers
- **Worker efficiency**: > 90% of single-worker performance per worker
- **IPC scaling**: No degradation up to 8 workers

### Benchmark Suite

**Test Scenarios** (Priority: P0):
1. **Hello World**: Simple GET request returning static text
2. **JSON API**: GET request with JSON parsing and serialization
3. **Database Query**: GET request with database lookup (PostgreSQL)
4. **POST with Body**: POST request with JSON body (10KB)
5. **Static File**: GET request for static file (1KB, 100KB, 1MB)
6. **WebSocket Echo**: Send and receive WebSocket messages
7. **Concurrent Connections**: Sustained load with 10k connections

**Tools**:
- wrk2 for HTTP benchmarking
- autocannon for Node.js-style benchmarking
- k6 for complex scenarios
- Custom WebSocket benchmark tool

**Comparison Baseline**:
- Granian (primary target)
- Uvicorn (secondary)
- Hypercorn (tertiary)

---

## Implementation Plan

### Overview

The implementation is divided into 10 phases spanning approximately 16-20 weeks. Each phase has clear deliverables, success criteria, and dependencies.

### Phase 1: Foundation & IPC Protocol Design (2 weeks)

**Objectives**:
- Define Protocol Buffer schemas for ASGI messages
- Design Unix Domain Socket protocol
- Create TypeScript/Python code generation pipeline

**Deliverables**:
1. `asgi.proto` with ASGI request/response messages
2. `config.proto` with configuration messages
3. `metrics.proto` with monitoring messages
4. TypeScript code generator (using ts-proto or Protobuf-ES)
5. Python code generator (using protoc)
6. Build scripts for proto compilation
7. Unit tests for serialization/deserialization

**Success Criteria**:
- Protocol Buffers compile without errors
- Round-trip serialization test passes (TS → Python → TS)
- Type safety verified in TypeScript
- Documentation for protocol schema

**Dependencies**: None

### Phase 2: Bun HTTP Server Core (2 weeks)

**Objectives**:
- Implement basic HTTP server with routing
- Add static file serving
- Create configuration system

**Deliverables**:
1. HTTP/1.1 server implementation
2. HTTP/2 server implementation
3. Request router (static vs API routing)
4. Static file handler with caching
5. Configuration loader (file, env, CLI)
6. Logging infrastructure
7. Basic error handling

**Success Criteria**:
- HTTP/1.1 "Hello World" benchmark: > 80,000 req/s
- HTTP/2 support verified with h2load
- Static file serving benchmark: > 100,000 req/s
- Configuration loads from all sources

**Dependencies**: None

### Phase 3: IPC Bridge - TypeScript Side (2 weeks)

**Objectives**:
- Implement Unix Domain Socket client
- Add request serialization and connection pooling

**Deliverables**:
1. UDS connection manager
2. Protocol Buffer request serializer
3. Response deserializer
4. Connection pool with load balancing
5. Timeout and retry logic
6. IPC metrics collection
7. Unit tests for IPC client

**Success Criteria**:
- UDS connection established successfully
- Protocol Buffer serialization < 100μs (p95)
- Connection pooling works (reuse verified)
- Timeout handling tested
- IPC overhead measured and documented

**Dependencies**: Phase 1

### Phase 4: IPC Bridge - Python Side (2 weeks)

**Objectives**:
- Implement Unix Domain Socket server
- Add ASGI application interface
- Create worker process infrastructure

**Deliverables**:
1. UDS server implementation (asyncio)
2. Protocol Buffer deserializer to ASGI dict
3. ASGI application invoker
4. Response serializer to Protocol Buffers
5. Worker lifecycle management
6. Graceful shutdown handler
7. Unit tests for Python bridge

**Success Criteria**:
- ASGI request successfully invoked
- Response serialized and sent back
- Worker process starts and stops gracefully
- End-to-end test: TypeScript → Python → TypeScript
- IPC overhead < 1ms measured

**Dependencies**: Phase 1, Phase 3

### Phase 5: Worker Pool Management (2 weeks)

**Objectives**:
- Implement worker process spawning
- Add health checks and auto-restart
- Create load balancing

**Deliverables**:
1. Worker pool manager (Bun side)
2. Process spawning and lifecycle
3. Health check system (ping/pong)
4. Auto-restart on crash
5. Graceful shutdown coordination
6. Load balancing algorithm (round-robin, least-connections)
7. Worker metrics and status API

**Success Criteria**:
- Multiple workers spawn successfully
- Crashed worker auto-restarts
- Load balancing distributes requests evenly
- Graceful shutdown tested (no dropped requests)
- Health checks detect unhealthy workers

**Dependencies**: Phase 4

### Phase 6: WebSocket Support (2 weeks)

**Objectives**:
- Implement WebSocket upgrade handling
- Add bidirectional IPC for WebSocket frames
- Create connection state management

**Deliverables**:
1. WebSocket upgrade handler (Bun)
2. WebSocket frame serialization to Protocol Buffers
3. Bidirectional IPC channel
4. Connection state manager
5. Ping/pong heartbeat
6. Connection close handling
7. WebSocket client for testing

**Success Criteria**:
- WebSocket upgrade successful
- Bidirectional messages flow correctly
- Echo server benchmark: > 500,000 msg/s
- Connection state tracked accurately
- Graceful close on client or server side

**Dependencies**: Phase 5

### Phase 7: Protocol Extensions (3 weeks)

**Objectives**:
- Implement gRPC, JSON-RPC, SSE, h2c support

**Deliverables**:
1. gRPC handler using @grpc/grpc-js
2. gRPC server reflection
3. JSON-RPC 2.0 handler
4. Server-Sent Events handler
5. h2c (HTTP/2 cleartext) support
6. Protocol-specific routing
7. Integration tests for each protocol

**Success Criteria**:
- gRPC unary RPC works
- gRPC streaming (client, server, bidirectional) works
- JSON-RPC requests handled correctly
- SSE events stream to client
- h2c upgrade successful

**Dependencies**: Phase 5

### Phase 8: Performance Optimization (2 weeks)

**Objectives**:
- Optimize IPC overhead
- Add shared memory for large payloads
- Tune for Granian parity

**Deliverables**:
1. Shared memory implementation
2. Request/response object pooling
3. Protocol Buffer optimization (arena allocation)
4. Zero-copy static file serving
5. Comprehensive benchmark suite
6. Profiling and flame graphs
7. Performance tuning guide

**Success Criteria**:
- IPC overhead < 500μs (p95) with optimizations
- Throughput > 60,000 req/s (Hello World)
- Shared memory works for bodies > 1MB
- No memory leaks detected (24hr load test)
- Granian performance exceeded

**Dependencies**: Phase 5, Phase 7

### Phase 9: Type-Safe Client Generation (2 weeks)

**Objectives**:
- Introspect Litestar routes
- Generate TypeScript client with types

**Deliverables**:
1. Route introspection system
2. TypeScript type emitter
3. Client method generator
4. In-memory type generation
5. File output mode for builds
6. CLI tool for codegen
7. Integration with Litestar

**Success Criteria**:
- Routes introspected successfully
- TypeScript types generated accurately
- Client methods type-safe (IntelliSense works)
- Round-trip type safety (TS → Python → TS)
- Documentation and examples

**Dependencies**: Phase 5

### Phase 10: Production Readiness (3 weeks)

**Objectives**:
- Harden error handling
- Add monitoring and metrics
- Create deployment guides

**Deliverables**:
1. Comprehensive error handling
2. Circuit breaker for failing workers
3. Prometheus metrics endpoint
4. Health check endpoints
5. OpenTelemetry integration (optional)
6. Systemd service file
7. Docker image and Dockerfile
8. Kubernetes deployment manifests
9. Production deployment guide
10. Migration guide from Granian/Uvicorn

**Success Criteria**:
- 24-hour stability test passes
- Graceful degradation on worker failure
- Metrics verified with Prometheus
- Health checks work with Kubernetes
- Production deployment successful (test environment)
- Documentation complete

**Dependencies**: Phase 8, Phase 9

### Timeline Summary

| Phase | Duration | Dependencies | Cumulative Time |
|-------|----------|--------------|-----------------|
| Phase 1 | 2 weeks | None | 2 weeks |
| Phase 2 | 2 weeks | None | 2 weeks (parallel) |
| Phase 3 | 2 weeks | Phase 1 | 4 weeks |
| Phase 4 | 2 weeks | Phase 1, 3 | 6 weeks |
| Phase 5 | 2 weeks | Phase 4 | 8 weeks |
| Phase 6 | 2 weeks | Phase 5 | 10 weeks |
| Phase 7 | 3 weeks | Phase 5 | 13 weeks |
| Phase 8 | 2 weeks | Phase 5, 7 | 15 weeks |
| Phase 9 | 2 weeks | Phase 5 | 15 weeks (parallel) |
| Phase 10 | 3 weeks | Phase 8, 9 | 18 weeks |

**Total Estimated Time**: 18-20 weeks (4.5-5 months)

**Critical Path**: Phase 1 → Phase 3 → Phase 4 → Phase 5 → Phase 7 → Phase 8 → Phase 10

### Relationship to PRD-1 (Rust Core IPC Layer)

**Parallel Development**:
- PRD-3 (Bun) can proceed independently of PRD-1 (Rust)
- Both target the same IPC protocol (Protocol Buffers over UDS)
- Learnings from PRD-3 can inform PRD-1 design

**Migration Path**:
- Phase 1-10 complete Bun implementation
- PRD-1 can replace Phase 3-4 with Rust IPC bridge
- Benchmark comparison: Bun vs Rust IPC performance
- Choose optimal implementation based on results

**Expected Outcome**:
- Rust may provide 10-20% performance gain over TypeScript for IPC
- Bun may be simpler to develop and maintain
- Hybrid approach possible: Bun server + Rust IPC bridge

---

## Testing & Validation

### Unit Testing

**TypeScript Tests** (using Bun test):
- Protocol Buffer serialization/deserialization
- Request routing logic
- Configuration loading
- Connection pooling
- Error handling

**Python Tests** (using pytest):
- ASGI dict conversion
- Worker lifecycle
- Graceful shutdown
- Protocol Buffer deserialization
- Error handling

**Coverage Target**: > 80% line coverage

### Integration Testing

**IPC Bridge Tests**:
- End-to-end request/response flow
- Timeout handling
- Worker crash recovery
- Connection pooling behavior
- Shared memory (future)

**Protocol Tests**:
- HTTP/1.1 compliance
- HTTP/2 stream multiplexing
- WebSocket bidirectional messaging
- gRPC unary and streaming
- JSON-RPC request handling
- Server-Sent Events streaming

**Coverage Target**: All major code paths tested

### End-to-End Testing

**Real Application Tests**:
- Litestar Hello World app
- Litestar with database (PostgreSQL)
- Litestar with authentication
- Litestar with WebSocket
- Litestar with static files

**Success Criteria**:
- All tests pass without modification to Litestar app
- Performance meets or exceeds Granian
- No memory leaks detected

### Performance Testing

**Benchmark Suite**:

1. **Hello World Benchmark**:
   ```bash
   wrk -t4 -c100 -d30s http://localhost:8000/
   ```
   Target: > 60,000 req/s

2. **JSON API Benchmark**:
   ```bash
   wrk -t4 -c100 -d30s http://localhost:8000/api/users
   ```
   Target: > 55,000 req/s

3. **Static File Benchmark**:
   ```bash
   wrk -t4 -c100 -d30s http://localhost:8000/static/1kb.txt
   ```
   Target: > 100,000 req/s

4. **WebSocket Benchmark**:
   ```bash
   python websocket_bench.py --connections 1000 --messages 100000
   ```
   Target: > 500,000 msg/s

5. **Concurrent Connections Test**:
   ```bash
   k6 run --vus 10000 --duration 60s concurrent_test.js
   ```
   Target: 10,000 sustained connections

6. **Load Test** (24 hours):
   ```bash
   k6 run --vus 1000 --duration 24h load_test.js
   ```
   Target: No crashes, < 1% error rate, stable memory

**Comparison Testing**:
- Run identical benchmarks against Granian, Uvicorn, Hypercorn
- Document performance comparison
- Identify areas for optimization

### Stress Testing

**Scenarios**:
1. Worker crash during request
2. Slow Python response (timeout)
3. Malformed ASGI response
4. Resource exhaustion (memory, file descriptors)
5. Network issues (connection drops)

**Success Criteria**:
- Graceful error handling (no crashes)
- Appropriate HTTP status codes
- Worker recovery successful
- No memory leaks

### Compatibility Testing

**ASGI Compliance**:
- Test with Litestar, FastAPI, Starlette, Django
- Verify ASGI 3.0 spec compliance
- Test lifecycle hooks (startup, shutdown)
- Test middleware compatibility

**Success Criteria**:
- All frameworks work without modification
- No ASGI spec violations
- Middleware functions correctly

---

## Security & Reliability

### Security Considerations

**Unix Domain Socket Security**:
- Socket file permissions: 0600 (owner only)
- Socket location: `/tmp/vertebra-{pid}.sock` (unique per instance)
- Verify socket ownership before connection
- Clean up socket file on shutdown

**Request Validation**:
- Header size limits (default: 16KB)
- Body size limits (default: 10MB)
- URL length limits (default: 8KB)
- Request timeout (default: 30s)
- Reject malformed requests before IPC

**Resource Limits**:
- Max concurrent connections (default: 10,000)
- Max connections per worker (default: 1,000)
- Memory limit per worker (default: 512MB)
- File descriptor limit (default: 1024)

**TLS Termination**:
- TLS handled by Bun (before IPC)
- Certificate validation
- Cipher suite configuration
- ALPN for protocol negotiation

**Rate Limiting**:
- Per-IP request limits
- Global request limits
- Backpressure when workers saturated

**Input Sanitization**:
- Validate all headers
- Validate all path parameters
- Prevent path traversal attacks
- Prevent header injection

### Error Handling

**Worker Crashes**:
1. Detect via health check failure or process exit
2. Log crash details (exit code, signal)
3. Restart worker with exponential backoff
4. Return 503 Service Unavailable for in-flight requests
5. Alert if crash rate exceeds threshold

**IPC Failures**:
1. Connection timeout: retry with different worker
2. Serialization error: return 500 Internal Server Error
3. Deserialization error: return 502 Bad Gateway
4. Protocol version mismatch: log and restart worker

**Protocol Errors**:
1. Malformed HTTP: return 400 Bad Request
2. Invalid WebSocket upgrade: return 400 Bad Request
3. gRPC protocol error: return appropriate gRPC status
4. JSON-RPC parse error: return JSON-RPC error response

**Resource Exhaustion**:
1. Out of memory: gracefully refuse new connections (503)
2. File descriptor limit: close idle connections
3. Shared memory full: fall back to UDS serialization
4. Worker pool saturated: return 503 Service Unavailable

**Error Response Format**:
```json
{
  "error": {
    "type": "ServiceUnavailable",
    "message": "All workers are busy, please retry",
    "code": 503,
    "request_id": "req-abc123"
  }
}
```

### Reliability Features

**Graceful Shutdown**:
1. Receive SIGTERM
2. Stop accepting new connections
3. Wait for in-flight requests (timeout: 30s)
4. Send SIGTERM to workers
5. Wait for workers to exit (timeout: 30s)
6. Send SIGKILL to unresponsive workers
7. Clean up socket files
8. Exit

**Health Checks**:
- **Liveness**: Is the server running?
  - Endpoint: `/health/live`
  - Response: 200 OK if server is alive

- **Readiness**: Is the server ready to handle requests?
  - Endpoint: `/health/ready`
  - Response: 200 OK if workers are healthy, 503 otherwise
  - Checks: Worker pool status, IPC connectivity

**Worker Recovery**:
- Auto-restart crashed workers
- Exponential backoff (1s, 2s, 4s, 8s, max 60s)
- Circuit breaker: disable worker after 5 crashes in 60s
- Manual recovery: restart all workers via admin endpoint

**Request Retry**:
- Retry failed requests with different worker (max 2 retries)
- Don't retry non-idempotent requests (POST, PUT, etc.)
- Exponential backoff for retries

**Circuit Breaker**:
- Monitor worker error rate
- Open circuit if error rate > 50% for 10s
- Half-open after 30s (allow test requests)
- Close circuit if error rate drops below 10%

**Logging**:
- Structured JSON logs
- Request ID for tracing
- Error logs with stack traces
- Worker logs aggregated
- Configurable log levels

**Monitoring**:
- Prometheus metrics for alerting
- Worker status dashboard
- IPC bridge metrics (latency, throughput, errors)
- Request rate and error rate
- Memory and CPU usage

---

## Developer Experience

### Configuration

**Zero-Config Defaults**:
- Host: 127.0.0.1
- Port: 8000
- Workers: auto (CPU count)
- HTTP/2: enabled
- WebSocket: enabled
- Static files: disabled
- gRPC: disabled

**Configuration File** (vertebra.config.ts):
```typescript
import { defineConfig } from "vertebra";

export default defineConfig({
  server: {
    host: "0.0.0.0",
    port: 8000,
    workers: "auto",
  },
  python: {
    app: "myapp:app",
    venv: ".venv",
  },
  protocols: {
    http2: true,
    grpc: true,
  },
  static: {
    dir: "./static",
    maxAge: 3600,
  },
});
```

**Environment Variables**:
```bash
VERTEBRA_HOST=0.0.0.0
VERTEBRA_PORT=8000
VERTEBRA_WORKERS=4
VERTEBRA_PYTHON_APP=myapp:app
```

**CLI Arguments** (highest priority):
```bash
vertebra myapp:app --host 0.0.0.0 --port 8000 --workers 4
```

### Development Workflow

**Hot Reload**:
```bash
vertebra myapp:app --reload
```
- Watch Python files for changes
- Restart workers on change
- Preserve WebSocket connections (if possible)

**Debug Mode**:
```bash
vertebra myapp:app --debug
```
- Verbose logging
- Pretty-print errors
- Source maps for TypeScript
- Disable response caching

**Interactive Shell** (future):
```bash
vertebra shell myapp:app
```
- Python REPL with app context
- Test routes interactively
- Inspect worker state

### Migration from Granian/Uvicorn

**CLI Compatibility**:
Vertebra CLI is designed to be a drop-in replacement:

```bash
# Granian
granian myapp:app --host 0.0.0.0 --port 8000 --workers 4

# Vertebra (same command)
vertebra myapp:app --host 0.0.0.0 --port 8000 --workers 4
```

**Environment Variables**:
Compatible with Uvicorn/Granian environment variables:
- `HOST`, `PORT`, `WORKERS`
- `WEB_CONCURRENCY` (alternative to `WORKERS`)

**Feature Parity**:
All Granian/Uvicorn features supported:
- HTTP/1.1, HTTP/2
- WebSocket
- SSL/TLS
- Multi-worker
- Graceful shutdown

**Performance Comparison Tool**:
```bash
vertebra benchmark --target granian,uvicorn,vertebra
```
- Run identical benchmarks
- Generate comparison report
- Identify performance differences

### Error Messages

**Clear and Actionable**:

Bad:
```
Error: Connection failed
```

Good:
```
Error: Failed to connect to Python worker

Possible causes:
  1. Worker crashed (check logs)
  2. Socket file missing (/tmp/vertebra-1234.sock)
  3. Permission denied

Suggested actions:
  - Check worker logs: tail -f /var/log/vertebra/worker-1.log
  - Verify socket file: ls -la /tmp/vertebra-1234.sock
  - Restart server: vertebra myapp:app --reload
```

**Type Errors**:
```
Error: Type mismatch in route /users/{user_id}

Expected: int
Received: "abc"

File: myapp.py:42
Route: @get("/users/{user_id:int}")

Suggestion: Ensure user_id is an integer in the request URL
```

### Documentation

**Getting Started Guide**:
1. Installation
2. Hello World example
3. Configuration basics
4. Deployment to production

**API Reference**:
- Configuration schema
- CLI options
- Monitoring endpoints
- Error codes

**Protocol Guides**:
- HTTP/2 setup
- WebSocket integration
- gRPC services
- JSON-RPC handlers

**Performance Tuning**:
- Worker count optimization
- IPC tuning
- Shared memory configuration
- Benchmark analysis

**Migration Guides**:
- From Granian
- From Uvicorn
- From Gunicorn

---

## Risks & Mitigation

### High Risk: IPC Overhead Exceeds Target

**Risk**: IPC overhead > 1ms negates Bun's performance advantage

**Probability**: Medium
**Impact**: High (project goal not achieved)

**Mitigation**:
1. Use Unix Domain Sockets (not TCP)
2. Use Protocol Buffers (faster than JSON/MessagePack)
3. Implement connection pooling (reuse connections)
4. Add shared memory for large payloads
5. Benchmark early and often (Phase 3-4)
6. Profile and optimize hot paths
7. Consider Rust IPC bridge if TypeScript insufficient (PRD-1)

**Contingency**:
- If overhead > 2ms: Fall back to Granian recommendation
- If overhead 1-2ms: Continue optimization, document trade-offs
- If overhead < 1ms: Proceed as planned

### Medium Risk: Worker Pool Management Complexity

**Risk**: Worker lifecycle bugs cause crashes or hangs

**Probability**: Medium
**Impact**: Medium (reliability issues)

**Mitigation**:
1. Study Granian and Uvicorn implementations
2. Use proven patterns (process spawning, health checks)
3. Comprehensive integration testing (Phase 5)
4. Stress testing (worker crashes, timeouts)
5. Extensive logging and monitoring
6. Circuit breaker for failing workers

**Contingency**:
- Use existing worker pool library (e.g., pm2, supervisor)
- Simplify worker management (single worker mode for debugging)

### Medium Risk: WebSocket Bidirectional IPC Complexity

**Risk**: WebSocket frame forwarding introduces latency or bugs

**Probability**: Medium
**Impact**: Medium (WebSocket performance suffers)

**Mitigation**:
1. Design bidirectional IPC protocol carefully (Phase 6)
2. Use separate connections for WebSocket vs HTTP
3. Benchmark WebSocket performance early
4. Test with real-time applications (chat, live updates)
5. Implement connection state tracking
6. Add reconnection logic

**Contingency**:
- Simplify WebSocket handling (direct Python handling)
- Use existing WebSocket proxy (nginx, HAProxy)

### Low Risk: TypeScript/Python Type Mismatches

**Risk**: Protobuf types don't align with ASGI spec

**Probability**: Low
**Impact**: Low (fixable with schema updates)

**Mitigation**:
1. Protobuf schema as single source of truth (Phase 1)
2. ASGI spec compliance tests
3. Round-trip serialization tests
4. Type generation from schema
5. Code review for type safety

**Contingency**:
- Manual type mapping if automated generation fails
- Fallback to JSON serialization (slower but flexible)

### Low Risk: Bun Runtime Bugs

**Risk**: Bun has bugs affecting stability or performance

**Probability**: Low
**Impact**: Medium (blocked on Bun fixes)

**Mitigation**:
1. Use stable Bun version (v1.0.13+)
2. Monitor Bun issue tracker
3. Contribute bug reports and fixes
4. Maintain compatibility with multiple Bun versions
5. Test with Bun canary builds

**Contingency**:
- Pin to known-good Bun version
- Consider Node.js fallback (slower but more stable)

---

## Open Questions

1. **HTTP/3 Support**: Should we wait for Bun native HTTP/3 or integrate a third-party library?
   - **Recommendation**: Wait for Bun native support (low priority)

2. **Shared Memory Implementation**: Use POSIX shared memory, mmap, or Bun-specific API?
   - **Recommendation**: POSIX shared memory for compatibility

3. **Metrics Format**: Prometheus only, or support OpenTelemetry as well?
   - **Recommendation**: Prometheus (P0), OpenTelemetry (P2)

4. **Type Generation**: Runtime or build-time?
   - **Recommendation**: Both (runtime for dev, build-time for prod)

5. **Worker Language**: TypeScript workers instead of Python?
   - **Recommendation**: Python (goal is ASGI compatibility)

6. **Rust Integration**: Replace TypeScript IPC bridge with Rust (PRD-1)?
   - **Recommendation**: Develop both, benchmark, choose best

---

## Success Metrics

### Performance Metrics

- [ ] Throughput > 60,000 req/s (Hello World)
- [ ] Throughput > 55,000 req/s (JSON API)
- [ ] Static file throughput > 100,000 req/s
- [ ] IPC overhead < 1ms (p95)
- [ ] WebSocket throughput > 500,000 msg/s
- [ ] 10,000+ concurrent connections sustained
- [ ] Memory per worker ≤ 20MB

### Reliability Metrics

- [ ] 24-hour load test with < 1% error rate
- [ ] No memory leaks detected
- [ ] Worker crash recovery < 1 second
- [ ] Graceful shutdown with 0 dropped requests
- [ ] Health checks detect failures within 5 seconds

### Compatibility Metrics

- [ ] Litestar apps work without modification
- [ ] FastAPI apps work without modification
- [ ] Starlette apps work without modification
- [ ] ASGI 3.0 spec compliance verified

### Developer Experience Metrics

- [ ] Getting started guide < 5 minutes
- [ ] Type-safe client generation works
- [ ] CLI drop-in replacement for Granian
- [ ] Zero-config defaults work for 90% of use cases
- [ ] Error messages actionable and clear

### Adoption Metrics (Post-Launch)

- [ ] 100+ GitHub stars (3 months)
- [ ] 10+ production deployments (6 months)
- [ ] 5+ community contributions (6 months)
- [ ] PyPI downloads > 1,000/month (6 months)

---

## Appendix A: Research Sources

### Bun Performance
- [Strapi: Bun vs Node.js 2025 Performance Guide](https://strapi.io/blog/bun-vs-nodejs-performance-comparison-guide)
- [Medium: Why Choose Bun Over Node.js in Late 2026](https://medium.com/@lalatenduswain/why-choose-bun-over-node-js-deno-and-other-javascript-runtimes-in-late-2026-121f25f208eb)
- [Özkan Pakdil: Bun Microservice Framework Benchmark](https://ozkanpakdil.github.io/posts/my_collections/2026/2026-01-10-bun-microservice-framework-benchmark/)
- [Fenil Sonani: Bun vs Node.js Performance Comparison](https://fenilsonani.com/articles/bun-vs-nodejs-performance-comparison)

### Bun WebSocket Performance
- [GitHub: WebSocket Benchmark](https://github.com/ntsd/websocket-benchmark)
- [Daniel Lemire: WebSocket Benchmark in JavaScript](https://lemire.me/blog/2023/11/25/a-simple-websocket-benchmark-in-javascript-node-js-versus-bun/)
- [Bun Documentation: WebSockets](https://bun.sh/docs/api/websockets)

### Granian Performance
- [GitHub: Granian - Rust HTTP Server for Python](https://github.com/emmett-framework/granian)
- [DeployHQ: Python Application Servers in 2025](https://www.deployhq.com/blog/python-application-servers-in-2025-from-wsgi-to-modern-asgi-solutions)
- [Granian Benchmarks vs Other Servers](https://github.com/emmett-framework/granian/blob/master/benchmarks/vs.md)

### IPC Performance
- [Baeldung: IPC Performance Comparison](https://www.baeldung.com/linux/ipc-performance-comparison)
- [CopyProgramming: TCP Loopback vs Unix Domain Socket](https://copyprogramming.com/howto/tcp-loopback-connection-vs-unix-domain-socket-performance)
- [REVSYS: Unix Sockets Performance](https://www.revsys.com/12days/unix-sockets/)
- [Medium: Unix Domain Sockets for High-Performance Microservices](https://medium.com/@sanathshetty444/beyond-http-unleashing-the-power-of-unix-domain-sockets-for-high-performance-microservices-252eee7b96ad)

### Zero-Copy IPC
- [InfoQ: Zero-Copy In-Memory Sharing](https://www.infoq.com/news/2023/03/zero-copy-v6d/)
- [metafunctor: ZeroIPC](https://metafunctor.com/post/2025-01-zeroipc/)
- [GitHub: Eclipse iceoryx](https://github.com/eclipse-iceoryx/iceoryx)

### Serialization Performance
- [Medium: Message Serialization Experiments](https://medium.com/@hugovs/the-need-for-speed-experimenting-with-message-serialization-93d7562b16e4)
- [DEV: Protocol Buffers vs MessagePack Performance](https://dev.to/nate10/performance-analysis-of-json-buffer-custom-binary-protocol-protobuf-and-messagepack-for-websockets-2apn)
- [peerdh: Comparing MessagePack vs Protocol Buffers](https://peerdh.com/blogs/programming-insights/comparing-data-serialization-techniques-messagepack-vs-protocol-buffers-in-python)
- [CloudThat: Optimizing API Performance with Protocol Buffers](https://www.cloudthat.com/resources/blog/optimizing-api-performance-with-protocol-buffers-flatbuffers-messagepack-and-cbor)

### gRPC with TypeScript/Bun
- [OneUpTime: gRPC in Node.js with TypeScript](https://oneuptime.com/blog/post/2026-01-08-grpc-nodejs-typescript/view)
- [Bun Blog: gRPC.js Support in Bun v1.0.13](https://bun.sh/blog/bun-v1.0.13)

### ASGI and HTTP/2
- [ASGI 3.0 Documentation](https://asgi.readthedocs.io/en/latest/specs/www.html)
- [Encode: Working with ASGI and HTTP](https://www.encode.io/articles/asgi-http)

### Litestar Framework
- [GitHub: Litestar](https://github.com/litestar-org/litestar)
- [Litestar Documentation: Lifecycle Hooks](https://docs.litestar.dev/main/usage/lifecycle-hooks.html)
- [Medium: Introducing Litestar Framework](https://medium.com/django-unleashed/introducing-litestar-production-ready-light-flexible-extensible-asgi-api-framework-9c1ee642df17)

### Python Concurrency
- [Medium: AsyncIO vs Multiprocessing vs ThreadPools](https://medium.com/@yogeshkrishnanseeniraj/high-performance-python-asyncio-vs-multiprocessing-vs-threadpools-2026-guide-ad49d40452fc)
- [Real Python: Speed Up Python with Concurrency](https://realpython.com/python-concurrency/)
- [TestDriven: Concurrency, Parallelism, and asyncio](https://testdriven.io/blog/concurrency-parallelism-asyncio/)

### Protocol Buffers with TypeScript
- [GitHub: ts-proto](https://github.com/stephenh/ts-proto)
- [Buf: Protobuf-ES](https://buf.build/blog/protobuf-es-the-protocol-buffers-typescript-javascript-runtime-we-all-deserve)
- [Escape Tech: Using Protobuf with TypeScript](https://escape.tech/blog/using-protobuf-typescript/)

---

## Appendix B: TypeScript Module Structure

```
src/js/
├── package.json
├── tsconfig.json
├── bun.config.ts
├── src/
│   ├── server/
│   │   ├── index.ts              # Main entry point
│   │   ├── config.ts             # Configuration loader
│   │   ├── router.ts             # Request routing
│   │   ├── worker-pool.ts        # Python worker pool
│   │   ├── protocols/
│   │   │   ├── http.ts           # HTTP/1.1 & HTTP/2
│   │   │   ├── http3.ts          # HTTP/3 (future)
│   │   │   ├── websocket.ts      # WebSocket
│   │   │   ├── grpc.ts           # gRPC
│   │   │   ├── jsonrpc.ts        # JSON-RPC 2.0
│   │   │   ├── sse.ts            # Server-Sent Events
│   │   │   └── h2c.ts            # HTTP/2 cleartext
│   │   ├── bridge/
│   │   │   ├── index.ts          # IPC bridge interface
│   │   │   ├── uds.ts            # Unix Domain Socket
│   │   │   ├── serializer.ts     # Protobuf serialization
│   │   │   ├── pool.ts           # Connection pooling
│   │   │   └── shm.ts            # Shared memory (future)
│   │   └── monitoring/
│   │       ├── metrics.ts        # Prometheus metrics
│   │       ├── health.ts         # Health checks
│   │       └── tracing.ts        # Request tracing
│   ├── client/
│   │   ├── index.ts              # Client factory
│   │   ├── generator.ts          # Type generation
│   │   └── types.ts              # Base types
│   ├── codegen/
│   │   ├── index.ts              # CLI entry
│   │   ├── typescript.ts         # TS emitter
│   │   ├── python.ts             # Python emitter
│   │   └── protobuf.ts           # Proto generator
│   └── proto/
│       ├── asgi.proto            # ASGI messages
│       ├── config.proto          # Configuration
│       └── metrics.proto         # Metrics
└── tests/
    ├── server/
    ├── bridge/
    └── protocols/
```

---

## Appendix C: Python Module Structure

```
src/py/vertebra/
├── __init__.py
├── server/
│   ├── __init__.py
│   ├── bridge.py             # IPC bridge (UDS server)
│   ├── worker.py             # Worker process main
│   ├── asgi.py               # ASGI adapter
│   └── lifecycle.py          # Startup/shutdown
├── proto/
│   ├── __init__.py
│   └── generated/
│       ├── __init__.py
│       ├── asgi_pb2.py       # Generated from asgi.proto
│       └── config_pb2.py     # Generated from config.proto
├── cli.py                    # CLI interface
└── config.py                 # Configuration
```

---

## Appendix D: Example Configuration

```typescript
// vertebra.config.ts
import { defineConfig } from "vertebra";

export default defineConfig({
  server: {
    host: "0.0.0.0",
    port: 8000,
    workers: 4,
    tls: {
      cert: "./certs/cert.pem",
      key: "./certs/key.pem",
    },
  },

  python: {
    app: "myapp:app",
    workers: 4,
    timeout: 30,
    venv: ".venv",
  },

  protocols: {
    http2: true,
    http3: false,
    websocket: true,
    grpc: true,
    jsonrpc: true,
  },

  static: {
    dir: "./static",
    maxAge: 3600,
    compression: true,
  },

  ipc: {
    socketPath: "/tmp/vertebra.sock",
    protocol: "protobuf",
    poolSize: 2,
    poolMax: 10,
    sharedMemoryThreshold: 1048576, // 1MB
  },

  monitoring: {
    metrics: true,
    metricsPath: "/metrics",
    health: true,
    healthPath: "/health",
    tracing: false,
  },

  limits: {
    maxHeaderSize: 16384,
    maxBodySize: 10485760,
    maxConnections: 10000,
    requestTimeout: 30,
    keepAliveTimeout: 5,
  },
});
```

---

## Appendix E: Example Python Application

```python
# myapp.py
from litestar import Litestar, get, post, websocket
from litestar.datastructures import State
from dataclasses import dataclass

@dataclass
class User:
    id: int
    name: str
    email: str

@get("/")
async def index() -> dict[str, str]:
    return {"message": "Hello, Vertebra!"}

@get("/users/{user_id:int}")
async def get_user(user_id: int) -> User:
    return User(id=user_id, name="Alice", email="alice@example.com")

@post("/users")
async def create_user(data: User) -> User:
    return data

@websocket("/ws")
async def websocket_handler(socket: "WebSocket") -> None:
    await socket.accept()
    while True:
        message = await socket.receive_text()
        await socket.send_text(f"Echo: {message}")

app = Litestar(
    route_handlers=[index, get_user, create_user, websocket_handler]
)
```

**Running with Vertebra**:

```bash
# Development
vertebra myapp:app --reload --debug

# Production
vertebra myapp:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 4 \
  --http2 \
  --grpc \
  --static-dir ./static
```

---

**End of PRD-3**
