# Tasks: Elite-Performance HTTP Layer (Rust Hyper) & Protocol Support

## Performance Targets (Tiered World-Class)

**Multi-Model Consensus: Gemini Pro (9/10) + Gemini Flash (6/10)**

These targets MUST be met to advance between phases. Failure to hit targets requires investigation and discussion.

**Target Tiers:**
- **BENCHMARK**: Achievable in isolated benchmarks, bare metal, ideal conditions
- **PRODUCTION**: Realistic for Cloud Run gen2 with CPU always-allocated

---

### IPC Layer (Ring Buffer) - BENCHMARK TARGETS
| Metric | Target | Gate | Consensus |
|--------|--------|------|-----------|
| IPC latency p50 | < 5µs | G2 | 9/10 achievable |
| IPC latency p99 | < 10µs | G2 | 9/10 achievable |
| IPC latency p999 | < 50µs | G2 | AT RISK (virtualization) |
| Throughput | > 1M msg/sec | G3 | Conservative (SPSC can do 10-20M) |
| Header serialization | < 100ns | G1.5 | Guaranteed by zerocopy |

### HTTP Layer - BENCHMARK TARGETS
| Metric | Target | Gate | Consensus |
|--------|--------|------|-----------|
| HTTP overhead (Hyper parse + ring write) | < 100µs | G3 | 9/10 achievable |
| End-to-end p99 (HTTP → Python ACK) | < 200µs | G3 | AT RISK (Python GIL) |
| HTTP throughput | > 500k req/s (4-core) | G4 | Aggressive but achievable |
| H2C handshake | < 1ms | G2 | Achievable |

### HTTP Layer - PRODUCTION TARGETS (Cloud Run gen2)
| Metric | Target | Gate | Notes |
|--------|--------|------|-------|
| End-to-end p99 | < 2ms | G4-PROD | 10x relaxed for GIL + virtualization |
| End-to-end p999 | < 5ms | G4-PROD | Cloud Run scheduling noise |
| HTTP throughput | > 100k req/s | G4-PROD | With low latency (tradeoff) |

### Elite DX - BENCHMARK TARGETS
| Metric | Target | Gate | Consensus |
|--------|--------|------|-----------|
| RPC call overhead | < 50µs | G8 | Achievable |
| State patch latency (Python → JS) | < 100µs | G9 | Ignores network physics |
| Subscription setup | < 1ms | G8 | Achievable |
| Type HMR push | < 50ms | G10 | Delivery only (not reload) |

### Elite DX - PRODUCTION TARGETS
| Metric | Target | Gate | Notes |
|--------|--------|------|-------|
| RPC call overhead | < 500µs | G8-PROD | Includes TCP/browser event loop |
| State patch latency | < 1ms | G9-PROD | Realistic for WebSocket |

### Protocol-Specific - BENCHMARK
| Metric | Target | Gate | Consensus |
|--------|--------|------|-----------|
| WebSocket frame latency | < 50µs | G5 | Achievable |
| gRPC unary overhead | < 100µs | G6 | Achievable |
| gRPC streaming frame | < 50µs | G6 | Achievable |

---

### Critical Mitigations (Required to Hit Targets)

1. **Multi-Process Workers**: Use 1 process per core to avoid Python GIL contention
2. **Cloud Run Config**: Enable "CPU allocation: always allocated" in gen2
3. **Batch Ring Drains**: Drain queue in bulk to amortize Python wake cost
4. **GIL Release**: Rust code must release GIL while waiting on ring buffer
5. **Pre-Allocation**: All ring buffer slots pre-allocated (no dynamic allocation in hot path)
6. **Connection Reuse**: HTTP/2 multiplexing required for throughput targets

---

## Gating Strategy

Each gate's tests MUST pass before advancing. **Performance targets are mandatory pass criteria.**

| Gate | Focus | Performance Criteria | Blocks |
|------|-------|---------------------|--------|
| **G1** | Type bridge preservation + health | Functional only | Phase 1.5 |
| **G1.5** | IPC header serialization | Header ser < 100ns | Phase 2 |
| **G2** | H2C + backpressure | IPC p99 < 10µs | Phase 3 |
| **G3** | Zero-copy verification | HTTP overhead < 100µs, > 1M msg/sec | Phase 4 |
| **G4** | Graceful shutdown + metrics | HTTP throughput > 500k req/s | Phase 5 |
| **G5** | RFC 8441 WebSocket | WS frame < 50µs | Phase 6 |
| **G6** | gRPC transport | gRPC overhead < 100µs | Phase 7 |
| **G7** | Python bindings + CLI | End-to-end functional | Phase 8 |
| **G8** | Streaming RPC | RPC overhead < 50µs | Phase 9 |
| **G9** | NativeState | State patch < 100µs | Phase 10 |
| **G10** | Vite Integration | Type HMR < 50ms | Complete |

---

## Phase 1: Foundation + ASGI Proxy (Priority: CRITICAL, Gate: G1)

### 1.1 Dependencies & Module Structure

- [ ] **1.1.1** Add Hyper dependencies to `vertebra-core/Cargo.toml`
  ```toml
  hyper = { version = "1.8", features = ["http1", "http2", "server"] }
  hyper-util = { version = "0.1.19", features = ["tokio"] }
  http-body-util = "0.1.3"
  zerocopy = { version = "0.8", features = ["derive"] }
  ```

- [ ] **1.1.2** Create module structure
  ```
  src/rs/vertebra-core/src/server/
  ├── mod.rs           # VertebraServer, ServerConfig
  ├── listener.rs      # TCP binding, accept loop
  └── health.rs        # /health, /__ready__ endpoints
  ```

- [ ] **1.1.3** Update `lib.rs` to export server module
  - Add `pub mod server;`
  - Re-export `VertebraServer`

### 1.2 Core Server Implementation

- [ ] **1.2.1** Implement `VertebraServer` struct
  - Fields: `TcpListener`, `RingPool`, `ServerConfig`
  - Constructor with host, port, worker count
  - `local_addr()` method for test binding

- [ ] **1.2.2** Implement TCP accept loop
  - Async accept in `listener.rs`
  - Connection counting for metrics
  - Graceful shutdown flag check

- [ ] **1.2.3** Implement H2C connection handler
  ```rust
  http2::Builder::new(TokioExecutor::new())
      .http2_only(true)  // H2C mode
      .serve_connection(io, service)
  ```

- [ ] **1.2.4** Implement HTTP/1.1 fallback
  - Auto-detect protocol from first bytes
  - Support upgrade-based h2c

### 1.3 Health Endpoints

- [ ] **1.3.1** Implement `/health` endpoint
  - Returns 200 OK with JSON body
  - Includes server version, uptime

- [ ] **1.3.2** Implement `/__ready__` endpoint
  - Returns 200 when workers connected
  - Returns 503 during startup

- [ ] **1.3.3** Cloud Run health check support
  - Works over H2C (not just HTTP/1.1)
  - Responds within 1s

### 1.4 Ring Buffer Integration (Basic)

- [ ] **1.4.1** Create request → ring buffer writer
  - Acquire ring from pool
  - Write request envelope (basic format)
  - Release ring on response

- [ ] **1.4.2** Create ring buffer → response reader
  - Wait for worker response
  - Parse response envelope
  - Send HTTP response

### 1.5 Testing (Gate G1)

- [ ] **1.5.1** Unit test: H2C configuration
  ```rust
  #[tokio::test]
  async fn test_h2c_accepts_prior_knowledge() {
      // Client uses http2_only(true)
  }
  ```

- [ ] **1.5.2** Unit test: Health endpoints
  - Test /health returns 200
  - Test /__ready__ returns 503 before workers ready

- [ ] **1.5.3** Integration test: Type bridge preservation
  - `/__vertebra/types.d.ts` proxied correctly
  - ETag → If-None-Match → 304 flow
  - X-Vertebra-Watch: true → 30s long-poll

- [ ] **1.5.4** Integration test: Header pass-through
  - ALL request headers forwarded
  - ALL response headers preserved
  - Hop-by-hop headers stripped

### GATE G1 CHECKLIST (Functional Only)
- [ ] `curl --http2-prior-knowledge http://localhost:8080/health` returns 200
- [ ] `/__vertebra/types.d.ts` returns correct TypeScript
- [ ] ETag/If-None-Match → 304 works
- [ ] 30s long-poll with X-Vertebra-Watch works
- [ ] All headers preserved (ETag, X-TypeScript-Version, etc.)

---

## Phase 1.5: IPC Protocol Layout (Priority: CRITICAL, Gate: G1.5)

### 1.5.1 Binary Header Format

- [ ] **1.5.1.1** Define `IpcRequestHeader` struct
  ```rust
  #[derive(AsBytes, FromBytes, FromZeroes)]
  #[repr(C, packed)]
  pub struct IpcRequestHeader {
      pub method: u8,        // GET=0, POST=1, PUT=2, DELETE=3, PATCH=4
      pub path_len: u16,
      pub header_count: u8,
      pub body_len: u32,
      pub request_id: u64,
  }
  ```

- [ ] **1.5.1.2** Define header enum mappings
  ```rust
  pub enum CommonHeader {
      ContentType = 0,
      Accept = 1,
      Authorization = 2,
      ContentLength = 3,
      // ... etc
  }
  ```

- [ ] **1.5.1.3** Implement custom header encoding
  - Length-prefixed name for unknown headers
  - Intern common headers to enum IDs

### 1.5.2 Dispatcher Implementation

- [ ] **1.5.2.1** Create `Dispatcher` trait
  ```rust
  pub trait Dispatcher: Send + Sync {
      fn select_ring(&self) -> &SpscRing;
  }
  ```

- [ ] **1.5.2.2** Implement `RoundRobinDispatcher`
  - Atomic counter for fair distribution
  - Simple, predictable, baseline strategy

- [ ] **1.5.2.3** Implement `LeastLoadedDispatcher`
  - Track queue depth per ring
  - Select ring with most free slots
  - Update depth on send/recv

- [ ] **1.5.2.4** Add CLI flag `--dispatch-strategy`
  - Values: `round-robin` (default), `least-loaded`
  - Environment variable: `VERTEBRA_DISPATCH_STRATEGY`

### 1.5.3 Serialization Performance

- [ ] **1.5.3.1** Pre-allocate scratch buffers
  - Thread-local scratch for header serialization
  - Avoid allocations in hot path

- [ ] **1.5.3.2** Benchmark header serialization
  - **TARGET: < 100ns per request**
  - Use `criterion` for micro-benchmarks

### 1.5.4 Testing (Gate G1.5)

- [ ] **1.5.4.1** Unit test: Header roundtrip
  - Serialize → deserialize → identical

- [ ] **1.5.4.2** Unit test: Dispatcher selection
  - Round-robin distributes evenly
  - Least-loaded prefers emptier rings

- [ ] **1.5.4.3** Benchmark: Header serialization
  - **MUST achieve < 100ns**
  - If fails, profile and optimize

### GATE G1.5 CHECKLIST
- [ ] Header serialization benchmark: **< 100ns** ✗/✓
- [ ] Round-robin dispatcher functional
- [ ] Least-loaded dispatcher functional
- [ ] Zero allocations in header serialization path

---

## Phase 2: Zero-Copy Body Handling (Priority: CRITICAL, Gate: G2)

### 2.1 Zero-Copy Bytes Transfer

- [ ] **2.1.1** Implement `Bytes` → ring buffer write
  - Move `Bytes`, never clone
  - Use `Bytes::split_off` for chunking if needed

- [ ] **2.1.2** Audit for memcpy calls
  - Use `perf` or `cargo-flamegraph`
  - Identify any hidden copies

- [ ] **2.1.3** Body follows header in slot layout
  ```
  [IpcRequestHeader][path bytes][headers][body pointer]
  ```

### 2.2 Body Streaming

- [ ] **2.2.1** Create `src/rs/vertebra-core/src/server/bridge/body.rs`

- [ ] **2.2.2** Implement chunked body streaming
  - Don't buffer entire request
  - Stream chunks as they arrive

- [ ] **2.2.3** Handle Content-Length vs Transfer-Encoding
  - Known length: single write
  - Chunked: progressive writes

### 2.3 Backpressure Handling

- [ ] **2.3.1** Ring full → HTTP backpressure
  - `poll_ready` returns Pending when ring full
  - HTTP/2 flow control window shrinks

- [ ] **2.3.2** Configure H2 window sizes
  ```rust
  http2::Builder::new()
      .initial_stream_window_size(65535)
      .initial_connection_window_size(1048576)
  ```

- [ ] **2.3.3** Implement timeout for blocked writes
  - Configurable timeout (default 30s)
  - Return 503 if worker permanently blocked

### 2.4 Response Assembly

- [ ] **2.4.1** Create `src/rs/vertebra-core/src/server/bridge/response.rs`

- [ ] **2.4.2** Parse response envelope from ring
  - Status code
  - Headers
  - Body pointer/length

- [ ] **2.4.3** Handle 304 Not Modified
  - Empty body (critical for Vite)
  - Preserve ETag headers

- [ ] **2.4.4** Preserve all response headers
  - ETag, Cache-Control, Content-Encoding
  - X-TypeScript-Version (type bridge)

### 2.5 Testing (Gate G2)

- [ ] **2.5.1** Benchmark: IPC latency
  - **p50: < 5µs**
  - **p99: < 10µs**
  - **p999: < 50µs**

- [ ] **2.5.2** Verify zero-copy with pointer tracking
  - Instrument `Bytes` data pointer
  - Verify same pointer in ring

- [ ] **2.5.3** Test backpressure behavior
  - Slow worker → client sees delays
  - Very slow worker → 503 after timeout

- [ ] **2.5.4** Test 304 response flow
  - ETag in, 304 + ETag out

### GATE G2 CHECKLIST
- [ ] IPC latency p50: **< 5µs** ✗/✓
- [ ] IPC latency p99: **< 10µs** ✗/✓
- [ ] IPC latency p999: **< 50µs** ✗/✓
- [ ] No memcpy in hot path (verified)
- [ ] Backpressure prevents memory explosion
- [ ] 304 responses work correctly

---

## Phase 3: HTTP Performance & Throughput (Priority: CRITICAL, Gate: G3)

### 3.1 HTTP Overhead Optimization

- [ ] **3.1.1** Profile Hyper parsing overhead
  - Target: < 50µs for header parsing
  - Use `criterion` benchmarks

- [ ] **3.1.2** Optimize ring buffer write path
  - Target: < 50µs for ring write
  - Combined: < 100µs total overhead

- [ ] **3.1.3** Implement connection pooling
  - Reuse connections (HTTP/2 multiplexing)
  - Track connection count in metrics

### 3.2 Throughput Optimization

- [ ] **3.2.1** Benchmark sustained throughput
  - Target: **> 1M msg/sec** through IPC
  - Use `oha` or custom harness

- [ ] **3.2.2** Optimize Tokio task spawning
  - Batch multiple requests per task where possible
  - Minimize context switches

- [ ] **3.2.3** Verify CPU utilization
  - Target: < 50% CPU at 100k req/s
  - Profile hotspots if exceeded

### 3.3 Memory Efficiency

- [ ] **3.3.1** Profile memory usage
  - Target: < 10MB server overhead
  - No memory leaks under load

- [ ] **3.3.2** Verify no GC (Rust guarantee)
  - Document deterministic allocation patterns

### 3.4 Testing (Gate G3)

- [ ] **3.4.1** Benchmark: HTTP overhead
  - **< 100µs** (Hyper + ring write)

- [ ] **3.4.2** Benchmark: End-to-end p99
  - **< 200µs** (HTTP receive to Python ACK)

- [ ] **3.4.3** Benchmark: Throughput
  - **> 1M msg/sec** sustained

- [ ] **3.4.4** Benchmark: HTTP requests/sec
  - **> 500k req/s** on 4-core

### GATE G3 CHECKLIST
- [ ] HTTP overhead: **< 100µs** ✗/✓
- [ ] End-to-end p99: **< 200µs** ✗/✓
- [ ] IPC throughput: **> 1M msg/sec** ✗/✓
- [ ] Memory overhead: **< 10MB** ✗/✓
- [ ] CPU at 100k req/s: **< 50%** ✗/✓

---

## Phase 4: Graceful Shutdown & Metrics (Priority: HIGH, Gate: G4)

### 4.1 Graceful Shutdown

- [ ] **4.1.1** Implement connection draining
  - Stop accepting new connections
  - Wait for in-flight requests (configurable timeout)
  - Force-close after drain timeout

- [ ] **4.1.2** Signal handling
  - SIGTERM: initiate graceful shutdown
  - SIGINT: same as SIGTERM
  - SIGQUIT: immediate shutdown (if needed)

- [ ] **4.1.3** Worker coordination
  - Notify workers of shutdown
  - Wait for workers to finish current requests

### 4.2 Metrics

- [ ] **4.2.1** Request metrics
  - Total requests, requests/sec
  - Latency histogram (p50, p99, p999)
  - Status code distribution

- [ ] **4.2.2** Connection metrics
  - Active connections (HTTP/1.1 and HTTP/2)
  - Total connections accepted
  - Connection errors

- [ ] **4.2.3** Ring buffer metrics
  - Queue depth per worker
  - Full/empty events
  - Backpressure events

- [ ] **4.2.4** Prometheus endpoint
  - `/__metrics__` compatible with existing format
  - Efficient scraping

### 4.3 Testing (Gate G4)

- [ ] **4.3.1** Test graceful shutdown
  - In-flight requests complete
  - No dropped connections

- [ ] **4.3.2** Test metrics accuracy
  - Latency percentiles match actual

- [ ] **4.3.3** Benchmark: HTTP throughput
  - **> 500k req/s** on 4-core machine

### GATE G4 CHECKLIST
- [ ] HTTP throughput: **> 500k req/s** (4-core) ✗/✓
- [ ] Graceful shutdown completes in-flight requests
- [ ] Metrics endpoint works
- [ ] Signal handlers work correctly

---

## Phase 5: WebSocket over HTTP/2 (Priority: HIGH, Gate: G5)

### 5.1 RFC 8441 Implementation

- [ ] **5.1.1** Enable extended CONNECT protocol
  ```rust
  http2::Builder::new()
      .enable_connect_protocol()
  ```

- [ ] **5.1.2** Handle CONNECT with `:protocol = websocket`
  - Detect WebSocket upgrade request
  - Validate Sec-WebSocket-* headers

- [ ] **5.1.3** Create `src/rs/vertebra-core/src/server/protocol/websocket.rs`

### 5.2 WebSocket Handler

- [ ] **5.2.1** Add `tokio-tungstenite = "0.28"` dependency

- [ ] **5.2.2** Implement upgrade handler
  ```rust
  async fn handle_websocket_upgrade(req) -> Response {
      // Spawn WebSocket handler
      // Return 200 OK for CONNECT
  }
  ```

- [ ] **5.2.3** Implement frame → IPC bridge
  - WebSocket frame → ring buffer
  - Ring buffer → WebSocket frame

### 5.3 Concurrent Streams

- [ ] **5.3.1** Support multiple WebSocket streams per H2 connection
  - Each stream gets unique ID
  - Route frames by stream ID

- [ ] **5.3.2** Graceful close handling
  - Close frames sent/received
  - Ring buffer cleanup

### 5.4 Testing (Gate G5)

- [ ] **5.4.1** Test WebSocket over HTTP/2
  - Connect with h2-compatible client
  - Send/receive messages

- [ ] **5.4.2** Test concurrent streams
  - Multiple WebSockets on one connection

- [ ] **5.4.3** Benchmark: Frame latency
  - **< 50µs** per frame

### GATE G5 CHECKLIST
- [ ] WebSocket frame latency: **< 50µs** ✗/✓
- [ ] RFC 8441 CONNECT works
- [ ] Concurrent streams on single connection
- [ ] Graceful close handling

---

## Phase 6: gRPC Transport (Priority: HIGH, Gate: G6)

### 6.1 gRPC Detection & Parsing

- [ ] **6.1.1** Detect gRPC requests
  ```rust
  fn is_grpc(req: &Request) -> bool {
      req.headers().get(CONTENT_TYPE)
          .map(|v| v.as_bytes().starts_with(b"application/grpc"))
          .unwrap_or(false)
  }
  ```

- [ ] **6.1.2** Create `src/rs/vertebra-core/src/server/protocol/grpc.rs`

- [ ] **6.1.3** Parse gRPC frame header
  - 1 byte: compressed flag
  - 4 bytes: message length (big-endian)
  - N bytes: protobuf message

### 6.2 gRPC Routing

- [ ] **6.2.1** Route gRPC bodies through ring buffers
  - Same zero-copy path as HTTP

- [ ] **6.2.2** Frame responses as gRPC
  - Add 5-byte header to response
  - Set grpc-status trailer

- [ ] **6.2.3** Handle trailers
  - grpc-status (required)
  - grpc-message (optional)

### 6.3 Python gRPC Support

- [ ] **6.3.1** Add betterproto2 dependency
  ```toml
  [project.optional-dependencies]
  grpc = ["betterproto2[compiler,grpclib]>=0.1.0"]
  ```

- [ ] **6.3.2** Create example proto
  ```protobuf
  // src/schema/services/example.proto
  service Greeter {
      rpc SayHello(HelloRequest) returns (HelloResponse);
  }
  ```

- [ ] **6.3.3** Add `make proto` target
  - Generate Python stubs
  - Generate TypeScript types

- [ ] **6.3.4** Create gRPC server wrapper
  - `src/py/vertebra/rpc/server.py`
  - Service registration API

### 6.4 Testing (Gate G6)

- [ ] **6.4.1** Test with grpcurl
  ```bash
  grpcurl -plaintext localhost:8080 vertebra.example.Greeter/SayHello
  ```

- [ ] **6.4.2** Test unary requests end-to-end

- [ ] **6.4.3** Benchmark: gRPC overhead
  - **< 100µs** unary call overhead

### GATE G6 CHECKLIST
- [ ] gRPC unary overhead: **< 100µs** ✗/✓
- [ ] grpcurl works
- [ ] Python stubs generate
- [ ] End-to-end unary works

---

## Phase 7: Python Bindings & CLI (Priority: HIGH, Gate: G7)

### 7.1 PyO3 Bindings

- [ ] **7.1.1** Create `src/rs/vertebra-py/src/server.rs`

- [ ] **7.1.2** Implement `VertebraServer` Python class
  ```rust
  #[pyclass]
  pub struct VertebraServer {
      inner: Arc<vertebra_core::server::VertebraServer>,
      runtime: tokio::runtime::Runtime,
  }
  ```

- [ ] **7.1.3** Expose methods
  - `__new__(host, port, workers)`
  - `serve()` (blocking, releases GIL)
  - `shutdown()`
  - `local_addr()`

### 7.2 Python Wrapper

- [ ] **7.2.1** Create `src/py/vertebra/server/rust.py`
  - Import from `vertebra._vertebra`
  - Pythonic API wrapper

- [ ] **7.2.2** Configuration translation
  - Python config → Rust config
  - Environment variable support

### 7.3 CLI Integration

- [ ] **7.3.1** Add `--server [rust|bun]` flag
  - Default: `rust` for production
  - Default: `bun` for `--dev`

- [ ] **7.3.2** Add `--h2c` flag
  - Explicit h2c mode
  - Auto-enabled for Cloud Run detection

- [ ] **7.3.3** Update `vertebra serve` command
  - Backward compatible
  - Clear migration path

### 7.4 Testing (Gate G7)

- [ ] **7.4.1** Test `vertebra serve --server rust`
  - Server starts
  - Handles requests

- [ ] **7.4.2** Test graceful shutdown from Python
  - Signal handlers work
  - Clean exit

- [ ] **7.4.3** Test configuration options
  - Host, port, workers all configurable

### GATE G7 CHECKLIST
- [ ] `vertebra serve --server rust` works
- [ ] `vertebra serve --server bun` still works
- [ ] Graceful shutdown from CLI
- [ ] All configuration options work

---

## Phase 8: Elite DX - Streaming RPC (Priority: HIGH, Gate: G8)

### 8.1 Type Bridge Extensions

- [ ] **8.1.1** Add `SubscriptionType` to IR
  ```python
  @dataclass
  class SubscriptionType(TypeNode):
      yield_type: TypeNode
  ```

- [ ] **8.1.2** Add `StreamType` to IR
  ```python
  @dataclass
  class StreamType(TypeNode):
      item_type: TypeNode
  ```

- [ ] **8.1.3** Update extractor for `AsyncGenerator[T, None]`
  - Detect return type annotation
  - Extract yield type

### 8.2 Procedure Decorator Updates

- [ ] **8.2.1** Detect `AsyncGenerator` return type
  ```python
  if is_async_generator(return_type):
      metadata.type = "subscription"
      metadata.yield_type = extract_yield_type(return_type)
  ```

- [ ] **8.2.2** Register subscription in ProcedureRegistry
  - Store yield type for TypeScript generation

### 8.3 Streaming Serializer

- [ ] **8.3.1** Create `src/py/vertebra/procedures/serialization.py`

- [ ] **8.3.2** Implement NDJSON framing
  - Newline-delimited JSON
  - Simple, efficient

- [ ] **8.3.3** Implement SSE alternative
  - `data:` prefix format
  - For HTTP/1.1 compatibility

### 8.4 TypeScript Generator Updates

- [ ] **8.4.1** Generate `Subscription<T>` interface
  ```typescript
  interface Subscription<T> {
      subscribe(callback: (value: T) => void): () => void;
      [Symbol.asyncIterator](): AsyncIterator<T>;
  }
  ```

- [ ] **8.4.2** Generate subscription method types
  - `client.chat.messages.subscribe(callback)`

### 8.5 TypeScript Client Package

- [ ] **8.5.1** Create `src/ts/packages/vertebra-client/`
  - package.json
  - tsconfig.json
  - src/index.ts

- [ ] **8.5.2** Implement `createClient<AppRouter>()`
  - Type-safe client factory
  - Inferred from generated types

- [ ] **8.5.3** WebSocket transport
  - Auto-reconnect
  - Heartbeat

- [ ] **8.5.4** Fetch transport
  - Standard RPC calls
  - Timeout handling

### 8.6 Subscription Handling

- [ ] **8.6.1** Implement `subscribe()` method
  - Returns unsubscribe function
  - Manages WebSocket lifecycle

- [ ] **8.6.2** Implement async iteration
  - `for await...of` support
  - Clean cleanup on break

- [ ] **8.6.3** Auto-reconnection
  - Detect disconnect
  - Resubscribe automatically

### 8.7 Testing (Gate G8)

- [ ] **8.7.1** Test AsyncGenerator → Subscription flow
  - Python generator yields
  - JS client receives

- [ ] **8.7.2** Test type generation accuracy
  - Generated types compile
  - Types match runtime behavior

- [ ] **8.7.3** Benchmark: RPC overhead
  - **< 50µs** call overhead

### GATE G8 CHECKLIST
- [ ] RPC call overhead: **< 50µs** ✗/✓
- [ ] AsyncGenerator → Subscription works
- [ ] Type generation correct
- [ ] WebSocket reconnection works

---

## Phase 9: Elite DX - NativeState (Priority: HIGH, Gate: G9)

### 9.1 NativeState Base Class

- [ ] **9.1.1** Create `src/py/vertebra/state.py`

- [ ] **9.1.2** Implement `NativeState` class
  ```python
  class NativeState(msgspec.Struct):
      _subscribers: list[Callable] = []

      def __setattr__(self, name, value):
          super().__setattr__(name, value)
          if not name.startswith("_"):
              self._push_patch()
  ```

- [ ] **9.1.3** Track mutations via `__setattr__`
  - Capture field changes
  - Trigger patch generation

### 9.2 JSON Patch Generation

- [ ] **9.2.1** Implement patch diff
  - Compare old state to new state
  - Generate RFC 6902 JSON Patch

- [ ] **9.2.2** Debounce rapid mutations
  - Batch patches within window
  - Configurable debounce time

- [ ] **9.2.3** Support nested object patches
  - Deep diff for nested structures
  - Efficient path notation

### 9.3 Auto-Generated Subscriptions

- [ ] **9.3.1** Create subscription endpoint factory
  ```python
  @classmethod
  def create_subscription(cls, key: str):
      @procedure
      async def subscribe() -> AsyncGenerator[dict, None]:
          # Initial state
          yield {"type": "init", "state": ...}
          # Patches
          while True:
              patch = await queue.get()
              yield {"type": "patch", "ops": patch}
      return subscribe
  ```

- [ ] **9.3.2** Send `{ type: 'init', state: ... }` on connect

- [ ] **9.3.3** Send `{ type: 'patch', ops: [...] }` on mutation

### 9.4 State Registry

- [ ] **9.4.1** Implement global registry
  - Key-based instance lookup
  - Thread-safe access

- [ ] **9.4.2** Lifecycle management
  - Create on first access
  - Destroy on explicit call

### 9.5 Framework Adapters

- [ ] **9.5.1** React: `useVertebraState<T>(key)`
  ```typescript
  function useVertebraState<T>(key: string): T {
      // Subscribe to state
      // Apply patches on update
      // Trigger re-render
  }
  ```

- [ ] **9.5.2** Svelte: Vertebra store
  - Implement Svelte store contract
  - `$` syntax compatibility

- [ ] **9.5.3** Vue: Composable
  - Return reactive refs
  - Auto-patch application

- [ ] **9.5.4** Solid: Signal adapter
  - createSignal integration
  - Fine-grained reactivity

### 9.6 Testing (Gate G9)

- [ ] **9.6.1** Test Python mutation → frontend update
  - Change state in Python
  - Verify JS receives update

- [ ] **9.6.2** Test JSON Patch accuracy
  - Patches apply correctly
  - State stays in sync

- [ ] **9.6.3** Benchmark: State patch latency
  - **< 100µs** (Python mutation to JS callback)

- [ ] **9.6.4** Test reconnection with rehydration
  - Disconnect/reconnect
  - State resyncs correctly

### GATE G9 CHECKLIST
- [ ] State patch latency: **< 100µs** ✗/✓
- [ ] Python → JS state sync works
- [ ] JSON Patches apply correctly
- [ ] Reconnection rehydrates state
- [ ] React hook works
- [ ] Svelte store works
- [ ] Vue composable works

---

## Phase 10: Elite DX - Vite Integration (Priority: MEDIUM, Gate: G10)

### 10.1 Type Endpoint in Rust

- [ ] **10.1.1** Implement `/__vertebra/types.d.ts` in Rust
  - Serve generated TypeScript types
  - Proxied from Python (ASGI)

- [ ] **10.1.2** ETag caching support
  - Generate ETag from content hash
  - Return 304 on If-None-Match match

- [ ] **10.1.3** Gzip compression
  - Compress if Accept-Encoding: gzip
  - Pre-compress for efficiency

### 10.2 WebSocket Type HMR

- [ ] **10.2.1** Replace HTTP polling with WebSocket push
  - Single persistent connection
  - Instant updates on change

- [ ] **10.2.2** File watcher integration
  - Watch Python source files
  - Trigger type regeneration
  - Push to connected clients

- [ ] **10.2.3** Benchmark: Type HMR latency
  - **< 50ms** from save to IDE update

### 10.3 Vite HMR Proxy

- [ ] **10.3.1** Proxy Vite dev server
  - Forward requests to Vite
  - Handle WebSocket upgrade

- [ ] **10.3.2** Forward HMR WebSocket messages
  - Vite HMR protocol
  - Asset hot updates

- [ ] **10.3.3** Serve Vite assets
  - Development: proxy to Vite
  - Production: serve from manifest

### 10.4 Production Asset Serving

- [ ] **10.4.1** Parse Vite `manifest.json`
  - Map routes to hashed assets
  - Handle entry points

- [ ] **10.4.2** Serve static assets
  - `/assets/*` path
  - Cache headers (immutable)

- [ ] **10.4.3** Handle `/static` path
  - Static file serving
  - MIME type detection

### 10.5 Testing (Gate G10)

- [ ] **10.5.1** Test type HMR flow
  - Change Python type
  - Verify IDE updates

- [ ] **10.5.2** Benchmark: Type HMR latency
  - **< 50ms** save to IDE update

- [ ] **10.5.3** Test asset serving
  - Development mode works
  - Production mode works

- [ ] **10.5.4** Test dev/prod mode switching
  - Seamless transition

### GATE G10 CHECKLIST
- [ ] Type HMR latency: **< 50ms** ✗/✓
- [ ] Types.d.ts endpoint works
- [ ] 304 caching works
- [ ] Vite assets serve correctly
- [ ] Dev mode proxies Vite
- [ ] Production serves from manifest

---

## Final Validation Checklist

### Functional - HTTP Layer
- [ ] `vertebra serve --server rust` starts
- [ ] `curl --http2-prior-knowledge` works
- [ ] `curl --http1.1` falls back correctly
- [ ] WebSocket over HTTP/2 works (RFC 8441)
- [ ] gRPC service responds
- [ ] Cloud Run deployment succeeds
- [ ] `vertebra serve --server bun` still works

### Functional - Elite DX
- [ ] AsyncGenerator procedures generate Subscription<T> types
- [ ] JS client can subscribe to Python streams
- [ ] NativeState mutations push to frontend
- [ ] useVertebraState hook triggers React re-renders
- [ ] Svelte store reacts to state changes
- [ ] Vue composable works
- [ ] Type changes trigger instant HMR (not polling)
- [ ] Vite assets serve correctly (dev and prod)

### Performance - BENCHMARK Targets (bare metal, ideal)
- [ ] IPC latency p50: **< 5µs**
- [ ] IPC latency p99: **< 10µs**
- [ ] IPC latency p999: **< 50µs**
- [ ] HTTP overhead: **< 100µs**
- [ ] End-to-end p99: **< 200µs**
- [ ] Throughput: **> 1M msg/sec**
- [ ] HTTP throughput: **> 500k req/s** (4-core)
- [ ] RPC overhead: **< 50µs**
- [ ] State patch latency: **< 100µs**
- [ ] WebSocket frame: **< 50µs**
- [ ] gRPC overhead: **< 100µs**
- [ ] Type HMR: **< 50ms**

### Performance - PRODUCTION Targets (Cloud Run gen2)
- [ ] End-to-end p99: **< 2ms**
- [ ] End-to-end p999: **< 5ms**
- [ ] HTTP throughput: **> 100k req/s** with low latency
- [ ] RPC overhead: **< 500µs** (includes network)
- [ ] State patch latency: **< 1ms** (includes WebSocket)

### Performance - Universal
- [ ] No `memcpy` in hot path
- [ ] Memory: **< 10MB** server overhead
- [ ] CPU: **< 50%** at 100k req/s
- [ ] Pre-allocated ring buffer slots
- [ ] GIL released during ring wait

### Documentation
- [ ] Cloud Run deployment guide
- [ ] gRPC quick-start guide
- [ ] Elite DX guide (Streaming RPC, NativeState)
- [ ] Framework adapter guides (React, Svelte, Vue)
- [ ] All examples work as documented
- [ ] CLAUDE.md updated

### Feature Parity
- [ ] Integration tests pass with `--server rust`
- [ ] Integration tests pass with `--server bun`
- [ ] No behavioral differences
- [ ] Vite types.d.ts endpoint identical behavior
- [ ] ETag headers preserved
- [ ] X-Vertebra-Watch header forwarded

---

## Performance Target Summary

If any target cannot be achieved, document the following and escalate for discussion:

1. **Measured value** vs target
2. **Root cause** of gap
3. **Architectural constraints** preventing target
4. **Proposed alternatives** or relaxed targets
5. **Trade-off analysis** (what we gain/lose)

Example escalation format:
```
## Performance Gap: IPC p99

**Target:** < 10µs
**Measured:** 15µs

**Root Cause:** Python GIL contention during msgspec decode

**Constraints:** Cannot bypass msgspec without major refactor

**Alternatives:**
1. Accept 15µs p99 (50% over target)
2. Move decode to Rust (adds complexity)
3. Use process-per-worker (increases memory)

**Recommendation:** Accept 15µs with documented trade-off
```
