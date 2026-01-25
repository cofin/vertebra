# Task Checklist Report: pluggable-runtime-layer

- Generated: 2026-01-24
- Source: `specs/active/pluggable-runtime-layer/tasks.md`
- Total tasks: 259
- Done: 0
- Pending: 259

## Checklist

# Tasks: Elite-Performance HTTP Layer (Rust Hyper) & Protocol Support

## Performance Targets (Tiered World-Class)





### IPC Layer (Ring Buffer) - BENCHMARK TARGETS

### HTTP Layer - BENCHMARK TARGETS

### HTTP Layer - PRODUCTION TARGETS (Cloud Run gen2)

### Elite DX - BENCHMARK TARGETS

### Elite DX - PRODUCTION TARGETS

### Protocol-Specific - BENCHMARK


### Critical Mitigations (Required to Hit Targets)



## Gating Strategy




## Phase 1: Foundation + ASGI Proxy (Priority: CRITICAL, Gate: G1)

### 1.1 Dependencies & Module Structure

- [ ] **1.1.1** Add Hyper dependencies to `vertebra-core/Cargo.toml`

- [ ] **1.1.2** Create module structure

- [ ] **1.1.3** Update `lib.rs` to export server module

### 1.2 Core Server Implementation

- [ ] **1.2.1** Implement `VertebraServer` struct

- [ ] **1.2.2** Implement TCP accept loop

- [ ] **1.2.3** Implement H2C connection handler

- [ ] **1.2.4** Implement HTTP/1.1 fallback

### 1.3 Health Endpoints

- [ ] **1.3.1** Implement `/health` endpoint

- [ ] **1.3.2** Implement `/__ready__` endpoint

- [ ] **1.3.3** Cloud Run health check support

### 1.4 Ring Buffer Integration (Basic)

- [ ] **1.4.1** Create request → ring buffer writer

- [ ] **1.4.2** Create ring buffer → response reader

### 1.5 Testing (Gate G1)

- [ ] **1.5.1** Unit test: H2C configuration

- [ ] **1.5.2** Unit test: Health endpoints

- [ ] **1.5.3** Integration test: Type bridge preservation

- [ ] **1.5.4** Integration test: Header pass-through

### GATE G1 CHECKLIST (Functional Only)
- [ ] `curl --http2-prior-knowledge http://localhost:8080/health` returns 200
- [ ] `/__vertebra/types.d.ts` returns correct TypeScript
- [ ] ETag/If-None-Match → 304 works
- [ ] 30s long-poll with X-Vertebra-Watch works
- [ ] All headers preserved (ETag, X-TypeScript-Version, etc.)


## Phase 1.5: IPC Protocol Layout (Priority: CRITICAL, Gate: G1.5)

### 1.5.1 Binary Header Format

- [ ] **1.5.1.1** Define `IpcRequestHeader` struct

- [ ] **1.5.1.2** Define header enum mappings

- [ ] **1.5.1.3** Implement custom header encoding

### 1.5.2 Dispatcher Implementation

- [ ] **1.5.2.1** Create `Dispatcher` trait

- [ ] **1.5.2.2** Implement `RoundRobinDispatcher`

- [ ] **1.5.2.3** Implement `LeastLoadedDispatcher`

- [ ] **1.5.2.4** Add CLI flag `--dispatch-strategy`

### 1.5.3 Serialization Performance

- [ ] **1.5.3.1** Pre-allocate scratch buffers

- [ ] **1.5.3.2** Benchmark header serialization

### 1.5.4 Testing (Gate G1.5)

- [ ] **1.5.4.1** Unit test: Header roundtrip

- [ ] **1.5.4.2** Unit test: Dispatcher selection

- [ ] **1.5.4.3** Benchmark: Header serialization

### GATE G1.5 CHECKLIST
- [ ] Header serialization benchmark: **< 100ns** ✗/✓
- [ ] Round-robin dispatcher functional
- [ ] Least-loaded dispatcher functional
- [ ] Zero allocations in header serialization path


## Phase 2: Zero-Copy Body Handling (Priority: CRITICAL, Gate: G2)

### 2.1 Zero-Copy Bytes Transfer

- [ ] **2.1.1** Implement `Bytes` → ring buffer write

- [ ] **2.1.2** Audit for memcpy calls

- [ ] **2.1.3** Body follows header in slot layout

### 2.2 Body Streaming

- [ ] **2.2.1** Create `src/rs/vertebra-core/src/server/bridge/body.rs`

- [ ] **2.2.2** Implement chunked body streaming

- [ ] **2.2.3** Handle Content-Length vs Transfer-Encoding

### 2.3 Backpressure Handling

- [ ] **2.3.1** Ring full → HTTP backpressure

- [ ] **2.3.2** Configure H2 window sizes

- [ ] **2.3.3** Implement timeout for blocked writes

### 2.4 Response Assembly

- [ ] **2.4.1** Create `src/rs/vertebra-core/src/server/bridge/response.rs`

- [ ] **2.4.2** Parse response envelope from ring

- [ ] **2.4.3** Handle 304 Not Modified

- [ ] **2.4.4** Preserve all response headers

### 2.5 Testing (Gate G2)

- [ ] **2.5.1** Benchmark: IPC latency

- [ ] **2.5.2** Verify zero-copy with pointer tracking

- [ ] **2.5.3** Test backpressure behavior

- [ ] **2.5.4** Test 304 response flow

### GATE G2 CHECKLIST
- [ ] IPC latency p50: **< 5µs** ✗/✓
- [ ] IPC latency p99: **< 10µs** ✗/✓
- [ ] IPC latency p999: **< 50µs** ✗/✓
- [ ] No memcpy in hot path (verified)
- [ ] Backpressure prevents memory explosion
- [ ] 304 responses work correctly


## Phase 3: HTTP Performance & Throughput (Priority: CRITICAL, Gate: G3)

### 3.1 HTTP Overhead Optimization

- [ ] **3.1.1** Profile Hyper parsing overhead

- [ ] **3.1.2** Optimize ring buffer write path

- [ ] **3.1.3** Implement connection pooling

### 3.2 Throughput Optimization

- [ ] **3.2.1** Benchmark sustained throughput

- [ ] **3.2.2** Optimize Tokio task spawning

- [ ] **3.2.3** Verify CPU utilization

### 3.3 Memory Efficiency

- [ ] **3.3.1** Profile memory usage

- [ ] **3.3.2** Verify no GC (Rust guarantee)

### 3.4 Testing (Gate G3)

- [ ] **3.4.1** Benchmark: HTTP overhead

- [ ] **3.4.2** Benchmark: End-to-end p99

- [ ] **3.4.3** Benchmark: Throughput

- [ ] **3.4.4** Benchmark: HTTP requests/sec

### GATE G3 CHECKLIST
- [ ] HTTP overhead: **< 100µs** ✗/✓
- [ ] End-to-end p99: **< 200µs** ✗/✓
- [ ] IPC throughput: **> 1M msg/sec** ✗/✓
- [ ] Memory overhead: **< 10MB** ✗/✓
- [ ] CPU at 100k req/s: **< 50%** ✗/✓


## Phase 4: Graceful Shutdown & Metrics (Priority: HIGH, Gate: G4)

### 4.1 Graceful Shutdown

- [ ] **4.1.1** Implement connection draining

- [ ] **4.1.2** Signal handling

- [ ] **4.1.3** Worker coordination

### 4.2 Metrics

- [ ] **4.2.1** Request metrics

- [ ] **4.2.2** Connection metrics

- [ ] **4.2.3** Ring buffer metrics

- [ ] **4.2.4** Prometheus endpoint

### 4.3 Testing (Gate G4)

- [ ] **4.3.1** Test graceful shutdown

- [ ] **4.3.2** Test metrics accuracy

- [ ] **4.3.3** Benchmark: HTTP throughput

### GATE G4 CHECKLIST
- [ ] HTTP throughput: **> 500k req/s** (4-core) ✗/✓
- [ ] Graceful shutdown completes in-flight requests
- [ ] Metrics endpoint works
- [ ] Signal handlers work correctly


## Phase 5: WebSocket over HTTP/2 (Priority: HIGH, Gate: G5)

### 5.1 RFC 8441 Implementation

- [ ] **5.1.1** Enable extended CONNECT protocol

- [ ] **5.1.2** Handle CONNECT with `:protocol = websocket`

- [ ] **5.1.3** Create `src/rs/vertebra-core/src/server/protocol/websocket.rs`

### 5.2 WebSocket Handler

- [ ] **5.2.1** Add `tokio-tungstenite = "0.28"` dependency

- [ ] **5.2.2** Implement upgrade handler

- [ ] **5.2.3** Implement frame → IPC bridge

### 5.3 Concurrent Streams

- [ ] **5.3.1** Support multiple WebSocket streams per H2 connection

- [ ] **5.3.2** Graceful close handling

### 5.4 Testing (Gate G5)

- [ ] **5.4.1** Test WebSocket over HTTP/2

- [ ] **5.4.2** Test concurrent streams

- [ ] **5.4.3** Benchmark: Frame latency

### GATE G5 CHECKLIST
- [ ] WebSocket frame latency: **< 50µs** ✗/✓
- [ ] RFC 8441 CONNECT works
- [ ] Concurrent streams on single connection
- [ ] Graceful close handling


## Phase 6: gRPC Transport (Priority: HIGH, Gate: G6)

### 6.1 gRPC Detection & Parsing

- [ ] **6.1.1** Detect gRPC requests

- [ ] **6.1.2** Create `src/rs/vertebra-core/src/server/protocol/grpc.rs`

- [ ] **6.1.3** Parse gRPC frame header

### 6.2 gRPC Routing

- [ ] **6.2.1** Route gRPC bodies through ring buffers

- [ ] **6.2.2** Frame responses as gRPC

- [ ] **6.2.3** Handle trailers

### 6.3 Python gRPC Support

- [ ] **6.3.1** Add betterproto2 dependency

- [ ] **6.3.2** Create example proto

- [ ] **6.3.3** Add `make proto` target

- [ ] **6.3.4** Create gRPC server wrapper

### 6.4 Testing (Gate G6)

- [ ] **6.4.1** Test with grpcurl

- [ ] **6.4.2** Test unary requests end-to-end

- [ ] **6.4.3** Benchmark: gRPC overhead

### GATE G6 CHECKLIST
- [ ] gRPC unary overhead: **< 100µs** ✗/✓
- [ ] grpcurl works
- [ ] Python stubs generate
- [ ] End-to-end unary works


## Phase 7: Python Bindings & CLI (Priority: HIGH, Gate: G7)

### 7.1 PyO3 Bindings

- [ ] **7.1.1** Create `src/rs/vertebra-py/src/server.rs`

- [ ] **7.1.2** Implement `VertebraServer` Python class

- [ ] **7.1.3** Expose methods

### 7.2 Python Wrapper

- [ ] **7.2.1** Create `src/py/vertebra/server/rust.py`

- [ ] **7.2.2** Configuration translation

### 7.3 CLI Integration

- [ ] **7.3.1** Add `--server [rust|bun]` flag

- [ ] **7.3.2** Add `--h2c` flag

- [ ] **7.3.3** Update `vertebra serve` command

### 7.4 Testing (Gate G7)

- [ ] **7.4.1** Test `vertebra serve --server rust`

- [ ] **7.4.2** Test graceful shutdown from Python

- [ ] **7.4.3** Test configuration options

### GATE G7 CHECKLIST
- [ ] `vertebra serve --server rust` works
- [ ] `vertebra serve --server bun` still works
- [ ] Graceful shutdown from CLI
- [ ] All configuration options work


## Phase 8: Elite DX - Streaming RPC (Priority: HIGH, Gate: G8)

### 8.1 Type Bridge Extensions

- [ ] **8.1.1** Add `SubscriptionType` to IR

- [ ] **8.1.2** Add `StreamType` to IR

- [ ] **8.1.3** Update extractor for `AsyncGenerator[T, None]`

### 8.2 Procedure Decorator Updates

- [ ] **8.2.1** Detect `AsyncGenerator` return type

- [ ] **8.2.2** Register subscription in ProcedureRegistry

### 8.3 Streaming Serializer

- [ ] **8.3.1** Create `src/py/vertebra/procedures/serialization.py`

- [ ] **8.3.2** Implement NDJSON framing

- [ ] **8.3.3** Implement SSE alternative

### 8.4 TypeScript Generator Updates

- [ ] **8.4.1** Generate `Subscription<T>` interface

- [ ] **8.4.2** Generate subscription method types

### 8.5 TypeScript Client Package

- [ ] **8.5.1** Create `src/ts/packages/vertebra-client/`

- [ ] **8.5.2** Implement `createClient<AppRouter>()`

- [ ] **8.5.3** WebSocket transport

- [ ] **8.5.4** Fetch transport

### 8.6 Subscription Handling

- [ ] **8.6.1** Implement `subscribe()` method

- [ ] **8.6.2** Implement async iteration

- [ ] **8.6.3** Auto-reconnection

### 8.7 Testing (Gate G8)

- [ ] **8.7.1** Test AsyncGenerator → Subscription flow

- [ ] **8.7.2** Test type generation accuracy

- [ ] **8.7.3** Benchmark: RPC overhead

### GATE G8 CHECKLIST
- [ ] RPC call overhead: **< 50µs** ✗/✓
- [ ] AsyncGenerator → Subscription works
- [ ] Type generation correct
- [ ] WebSocket reconnection works


## Phase 9: Elite DX - NativeState (Priority: HIGH, Gate: G9)

### 9.1 NativeState Base Class

- [ ] **9.1.1** Create `src/py/vertebra/state.py`

- [ ] **9.1.2** Implement `NativeState` class


- [ ] **9.1.3** Track mutations via `__setattr__`

### 9.2 JSON Patch Generation

- [ ] **9.2.1** Implement patch diff

- [ ] **9.2.2** Debounce rapid mutations

- [ ] **9.2.3** Support nested object patches

### 9.3 Auto-Generated Subscriptions

- [ ] **9.3.1** Create subscription endpoint factory

- [ ] **9.3.2** Send `{ type: 'init', state: ... }` on connect

- [ ] **9.3.3** Send `{ type: 'patch', ops: [...] }` on mutation

### 9.4 State Registry

- [ ] **9.4.1** Implement global registry

- [ ] **9.4.2** Lifecycle management

### 9.5 Framework Adapters

- [ ] **9.5.1** React: `useVertebraState<T>(key)`

- [ ] **9.5.2** Svelte: Vertebra store

- [ ] **9.5.3** Vue: Composable

- [ ] **9.5.4** Solid: Signal adapter

### 9.6 Testing (Gate G9)

- [ ] **9.6.1** Test Python mutation → frontend update

- [ ] **9.6.2** Test JSON Patch accuracy

- [ ] **9.6.3** Benchmark: State patch latency

- [ ] **9.6.4** Test reconnection with rehydration

### GATE G9 CHECKLIST
- [ ] State patch latency: **< 100µs** ✗/✓
- [ ] Python → JS state sync works
- [ ] JSON Patches apply correctly
- [ ] Reconnection rehydrates state
- [ ] React hook works
- [ ] Svelte store works
- [ ] Vue composable works


## Phase 10: Elite DX - Vite Integration (Priority: MEDIUM, Gate: G10)

### 10.1 Type Endpoint in Rust

- [ ] **10.1.1** Implement `/__vertebra/types.d.ts` in Rust

- [ ] **10.1.2** ETag caching support

- [ ] **10.1.3** Gzip compression

### 10.2 WebSocket Type HMR

- [ ] **10.2.1** Replace HTTP polling with WebSocket push

- [ ] **10.2.2** File watcher integration

- [ ] **10.2.3** Benchmark: Type HMR latency

### 10.3 Vite HMR Proxy

- [ ] **10.3.1** Proxy Vite dev server

- [ ] **10.3.2** Forward HMR WebSocket messages

- [ ] **10.3.3** Serve Vite assets

### 10.4 Production Asset Serving

- [ ] **10.4.1** Parse Vite `manifest.json`

- [ ] **10.4.2** Serve static assets

- [ ] **10.4.3** Handle `/static` path

### 10.5 Testing (Gate G10)

- [ ] **10.5.1** Test type HMR flow

- [ ] **10.5.2** Benchmark: Type HMR latency

- [ ] **10.5.3** Test asset serving

- [ ] **10.5.4** Test dev/prod mode switching

### GATE G10 CHECKLIST
- [ ] Type HMR latency: **< 50ms** ✗/✓
- [ ] Types.d.ts endpoint works
- [ ] 304 caching works
- [ ] Vite assets serve correctly
- [ ] Dev mode proxies Vite
- [ ] Production serves from manifest


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


## Performance Target Summary



## Performance Gap: IPC p99
