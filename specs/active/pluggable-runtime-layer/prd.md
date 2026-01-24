# PRD: Elite-Performance HTTP Layer (Rust Hyper) & Protocol Support

## Intelligence Context

### Complexity Assessment

- **Classification**: High (Complex)
- **Estimated Phases**: 5
- **Risk Level**: Medium (new Rust HTTP layer, but builds on existing primitives)
- **Breaking Changes**: None (additive, Bun remains for development)

### Multi-Model Consensus

**Models Consulted:** Gemini 3 Pro (FOR), Gemini 3 Flash (AGAINST/Devil's Advocate)
**Result:** UNANIMOUS - Both 9/10 confidence for custom Rust HTTP in vertebra-core
**Key Quote:** "The only architecture that satisfies the 'Elite Performance' and zero-copy mandates"

### Similar Features Referenced

1. `src/rs/vertebra-core/src/channel.rs` - Existing SPSC ring buffers
2. `src/rs/vertebra-core/src/async_ring.rs` - Async ring buffer wrappers
3. `src/rs/vertebra-core/src/notify.rs` - eventfd/wake primitives
4. `src/ts/src/server/index.ts` - Current Bun server (to be supplemented)
5. `src/ts/src/ipc/asgi.ts` - ASGI bridge patterns

### Dependencies on Other Specs

| Spec | Dependency | Status |
|------|------------|--------|
| `rust-fast-path` | IPC p99 < 10µs for sync, < 20µs for async | **MET** ✅ |

**Achieved IPC Latencies (Jan 2026):**
| Scenario | Result | Target |
|----------|--------|--------|
| Sync Immediate | 1.15µs | < 10µs ✅ |
| Pure Wakeup (futex) | 0.8µs | < 15µs ✅ |
| AsyncRing Immediate | 1.64µs | < 10µs ✅ |

**Fix Applied:** Added `recv_hybrid()` to `SpscRing` using futex-based wait, plus `has_data()` fast atomic check. Eliminated poll() syscalls from spin loop.

### Patterns to Follow

- **Zero-Copy First**: Hyper `Bytes` → ring buffer without clone
- **Tokio Runtime**: Async HTTP handling with existing sync ring primitives
- **RFC 8441**: WebSocket over HTTP/2 via extended CONNECT
- **Industry Precedent**: Cloudflare Pingora, high-frequency trading systems

### Scope & Non-Goals (per Codex Review)

**In Scope:**
- HTTP/1.1 and HTTP/2 (h2c) server using Hyper
- WebSocket over HTTP/2 (RFC 8441)
- gRPC transport layer
- Zero-copy ring buffer integration
- ASGI proxy for Python workers
- Elite DX (Streaming RPC, NativeState, Framework Adapters)

**Non-Goals (MVP):**
- HTTP/3 support (future roadmap)
- Data-plane sockets (shared memory only)
- Replacement of Bun broker/pools (Bun continues for DB/S3/Redis/crypto)
- tokio-uring integration (0.x API, NOT production ready)

### Compatibility Contracts

The following integrations MUST continue working when Rust replaces Bun for HTTP:

| Integration | Requirement |
|-------------|-------------|
| **Type Bridge** | `/__vertebra/types.d.ts` proxied to ASGI unchanged |
| **Vite HMR** | Long-poll with `X-Vertebra-Watch: true` up to 30s |
| **ETag Caching** | `If-None-Match`/`ETag` headers preserved for 304 |
| **Litestar Plugin** | All ASGI semantics preserved |

### Gating Strategy (per Codex Review)

Implementation follows strict gates—each gate's tests must pass before advancing:

| Gate | Focus | Must Pass Before |
|------|-------|------------------|
| **G1** | Type bridge + health endpoints | Phase 2 (H2C) |
| **G2** | H2C + backpressure | Phase 3 (Zero-Copy) |
| **G3** | Zero-copy verification | Phase 4 (Shutdown) |
| **G4** | Graceful shutdown + metrics | Phase 5 (WebSocket) |
| **G5** | RFC 8441 WebSocket | Phase 6 (gRPC) |
| **G6** | gRPC transport | Phase 7 (PyO3/CLI) |

### Detected Stack and Commands

| Component | Technology |
|-----------|------------|
| HTTP Server | Rust Hyper (new) |
| Async Runtime | Tokio |
| WebSocket | tokio-tungstenite |
| Ring Buffers | vertebra-core (existing) |
| Python Bindings | PyO3 (existing) |
| gRPC Stubs | betterproto2 |

---

## Problem Statement

### User Problem Being Solved

Vertebra's product guidelines mandate **"Elite Performance & Design"** with **"Zero-Copy First"** architecture. Beyond the HTTP layer limitations, the current developer experience falls short of the "Elite DX" vision:

**HTTP Layer Limitations (Solved by Rust Hyper):**

1. **No H2C Support**: Google Cloud Run requires HTTP/2 cleartext (h2c) for end-to-end HTTP/2. Bun's h2c support is undocumented/unreliable.

2. **V8 GC Pauses**: Bun's JavaScript runtime introduces garbage collection pauses that violate sub-millisecond latency targets.

3. **FFI Overhead**: Current flow requires: `Bun HTTP → Serialization → bun:ffi → Ring Buffer`. Each boundary adds latency.

4. **No Native gRPC**: Users building microservices must bolt on gRPC separately, losing shared-memory benefits.

**Developer Experience Limitations (Solved by Elite DX Bridge):**

5. **Polling-Based Type Updates**: Current `/__vertebra/types.d.ts` uses HTTP long-polling. Should be instant WebSocket/SSE push.

6. **No Streaming RPC**: `@procedure` only supports request/response. No `AsyncGenerator` → `Subscription<T>` support for real-time data.

7. **No Native State Sync**: No way to have Python objects automatically push state changes to frontend stores (JSON Patch).

8. **No Framework Adapters**: Missing `useVertebraState()` for React, Svelte stores, Vue signals—the "Inertia Killer" pattern.

### Multi-Model Consensus Conclusion

After consulting Gemini 3 Pro and Gemini 3 Flash (devil's advocate), both models concluded with 9/10 confidence:

> "Building a custom Rust HTTP/2 server layer directly within vertebra-core using Hyper is the only architecture that satisfies the 'Elite Performance' and zero-copy mandates while natively solving the Google Cloud Run h2c requirement."

**Discarded alternatives:**
- **Hypercorn**: Python GIL incompatible with sub-millisecond targets
- **Granian**: ASGI/RSGI bottleneck, translation cost to ring buffers
- **Sidecar Proxy (Envoy)**: Adds network hop, violates zero-copy

### Business Value

1. **Cloud Run Excellence**: Native h2c enables elite Cloud Run deployments without sidecars
2. **True Zero-Copy**: HTTP request bodies stream directly into ring buffers
3. **Sub-Millisecond Latency**: No GC pauses, no FFI boundary, no serialization
4. **gRPC Native**: Same transport layer handles HTTP and gRPC
5. **Industry Leadership**: Architecture matches Cloudflare Pingora, HFT systems

### Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| H2C works with Cloud Run | Pass | `curl --http2-prior-knowledge` |
| WebSocket over HTTP/2 | Pass | RFC 8441 compliance test |
| IPC latency p50 | < 5µs | Ring buffer round-trip benchmark |
| IPC latency p99 | < 10µs | Ring buffer round-trip benchmark |
| IPC latency p999 | < 50µs | Ring buffer round-trip benchmark |
| HTTP request overhead | < 100µs | End-to-end request benchmark |
| Throughput | > 1M msg/sec | Sustained throughput test |
| Zero-copy verification | Pass | No `memcpy` in hot path |
| gRPC unary latency | < 100µs | grpcurl benchmark (excluding network) |
| Bun still works for dev | Pass | `vertebra serve --dev` |

**Performance Philosophy (from Product Guidelines):**
- "Elite Performance & Design" — extreme performance is mandatory, not optional
- "Zero-Copy First" — eliminate serialization overhead at every boundary
- "Efficiency over Simplicity" — complex zero-copy implementations beat simple slow ones

---

## Acceptance Criteria

### Functional Requirements

#### FR-1: Rust HTTP Server (Hyper)

- [ ] Create `src/rs/vertebra-core/src/server/` module
- [ ] Implement HTTP/1.1 server using Hyper
- [ ] Implement HTTP/2 cleartext (h2c) via `http2_only(true)`
- [ ] Support both prior-knowledge h2c and upgrade headers
- [ ] Integrate with existing Tokio runtime

#### FR-2: Zero-Copy Ring Buffer Integration

- [ ] Stream request bodies directly into SPSC ring buffers
- [ ] Use Hyper's `Bytes` (reference-counted) without cloning
- [ ] Response bodies read from ring buffers without copy
- [ ] Backpressure handling when workers are slow

#### FR-2b: IPC Protocol Layout (Gap from Gemini Review)

- [ ] Define binary header format using `zerocopy` derives
- [ ] Implement compact encoding for common headers (Content-Type, Accept, etc.)
- [ ] Use integer enums for HTTP methods and common header names
- [ ] Pre-allocate scratch space for header serialization
- [ ] Headers must precede body pointer in ring buffer slot

```rust
// src/rs/vertebra-core/src/server/bridge/header.rs
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(AsBytes, FromBytes, FromZeroes)]
#[repr(C, packed)]
pub struct IpcRequestHeader {
    pub method: u8,           // GET=0, POST=1, PUT=2, etc.
    pub path_len: u16,        // Length of path string
    pub header_count: u8,     // Number of headers
    pub body_len: u32,        // Body length (0 for no body)
    pub request_id: u64,      // Correlation ID
    // Followed by: path bytes, then header entries, then body pointer
}
```

#### FR-2c: Worker Load Balancing (Gap from Gemini Review)

- [ ] Implement dispatcher abstraction for ring buffer selection
- [ ] Support Round-Robin dispatch (baseline)
- [ ] Support Least-Loaded dispatch (prefer ring with most free slots)
- [ ] Track ring buffer queue depth per worker
- [ ] Configuration option: `--dispatch-strategy [round-robin|least-loaded]`

#### FR-3: WebSocket over HTTP/2 (RFC 8441)

- [ ] Enable `extended_connect_protocol` on HTTP/2 builder
- [ ] Handle CONNECT method with `:protocol = websocket`
- [ ] Bridge WebSocket frames to IPC ring buffers
- [ ] Support concurrent WebSocket streams on single H2 connection

#### FR-4: gRPC Transport

- [ ] Detect gRPC requests via `Content-Type: application/grpc`
- [ ] Route gRPC bodies through same ring buffer mechanism
- [ ] Support unary, server-streaming, client-streaming, bidirectional
- [ ] Add betterproto2 for Python stub generation

#### FR-5: Python Server Bindings (PyO3)

- [ ] Expose `VertebraServer` class to Python via PyO3
- [ ] CLI: `vertebra serve --server rust` flag
- [ ] Configuration: host, port, workers, ring buffer sizes
- [ ] Graceful shutdown with connection draining

#### FR-6: Cloud Run Compatibility

- [ ] Health check endpoint works over h2c
- [ ] Support Cloud Run's port name `h2c` convention
- [ ] Handle Cloud Run's load balancer behavior
- [ ] Documentation with Terraform/YAML examples

#### FR-7: Bun Compatibility Mode (ADDITIVE, NOT REPLACEMENT)

**Architecture:** Rust HTTP server is an ADDITION to the existing Bun path, not a replacement. Components are mix-and-match:

| Component | Options | Default |
|-----------|---------|---------|
| HTTP Server | `rust` / `bun` | `rust` (prod), `bun` (dev) |
| Database Pools | Bun only | Bun |
| Redis Client | Bun only | Bun |
| S3/Storage | Bun only | Bun |
| Crypto (password) | Bun only | Bun |
| Static Assets | Both | Bun (dev), Rust (prod) |

**Mix-and-Match Supported:**
- `--server rust` + Bun database drivers = **SUPPORTED** (production recommended)
- `--server bun` + all Bun services = **SUPPORTED** (development default)
- `--server rust` + Bun S3/crypto = **SUPPORTED**

**Compatibility Requirements:**
- [ ] Bun remains default for `--dev` mode
- [ ] Bun continues to handle: database drivers, S3, crypto
- [ ] Clear migration path for existing Bun-based deployments
- [ ] Runtime selection via `--server [rust|bun]` or `VERTEBRA_SERVER` env var
- [ ] **Warning emitted** when mixing components with known limitations
- [ ] Existing Bun code paths are NOT modified, only new Rust paths added

#### FR-8: Elite DX - Streaming RPC (The "tRPC-Style" Client)

- [ ] Extend Type Bridge IR to support `SubscriptionType` and `StreamType`
- [ ] Update `@procedure` decorator to detect `AsyncGenerator[T, None]` return types
- [ ] Generate `Subscription<T>` types in TypeScript
- [ ] Implement `StreamingResponse` serializer (NDJSON or SSE-style framing)
- [ ] WebSocket transport for subscriptions (via Rust server or shared memory)

```python
# Python: Streaming procedure
@procedure
async def chat_messages(room_id: str) -> AsyncGenerator[Message, None]:
    async for msg in message_stream(room_id):
        yield msg
```

```typescript
// TypeScript: Generated subscription type
client.chat.messages.subscribe("room-1", (msg) => console.log(msg));
```

#### FR-9: Elite DX - NativeState (Real-Time Sync)

- [ ] Implement `NativeState` base class (extends `msgspec.Struct`)
- [ ] Auto-generate subscription endpoints for NativeState instances
- [ ] Implement JSON Patch diff generation on state mutation
- [ ] Stream format: `{ type: 'init', state: ... }` then `{ type: 'patch', ops: [...] }`
- [ ] Auto-reconnecting subscriptions with state rehydration

```python
# Python: NativeState primitive
class Counter(NativeState):
    count: int = 0

    def increment(self):
        self.count += 1  # Auto-pushes patch to subscribers
```

#### FR-10: Elite DX - Framework Adapters

- [ ] React: `useVertebraState<T>(key)` hook
- [ ] Svelte: Vertebra store adapter
- [ ] Vue: Composable with reactive refs
- [ ] Solid: Signal-based adapter
- [ ] All adapters apply JSON Patches and trigger reactivity

```typescript
// React: Zero-latency state sync
function Counter() {
    const counter = useVertebraState<Counter>("counter");
    return <div>{counter.count}</div>;  // Auto-updates on Python mutation
}
```

#### FR-11: Elite DX - Vite/Litestar Integration Preservation

- [ ] Rust server exposes `/__vertebra/types.d.ts` endpoint (replacing Bun)
- [ ] WebSocket channel for instant type updates (not polling)
- [ ] Vite HMR proxy support in development mode
- [ ] Asset serving from Vite manifest (`manifest.json`)
- [ ] Static file serving for `/static`, `/assets` paths
- [ ] Preserve Litestar native pipeline compatibility matrix

### Non-Functional Requirements

#### NFR-1: Performance (Tiered World-Class Targets)

**Multi-Model Consensus:** Gemini Pro (9/10) + Gemini Flash (6/10) validated these targets with caveats for Cloud Run production.

**IPC Layer (Ring Buffer) - BENCHMARK TARGETS (achievable):**
- [ ] IPC latency p50 < 5µs
- [ ] IPC latency p99 < 10µs
- [ ] IPC latency p999 < 50µs
- [ ] Throughput > 1M msg/sec sustained
- [ ] Header serialization < 100ns (zerocopy guarantees this)

**HTTP Layer (Hyper + IPC) - BENCHMARK TARGETS:**
- [ ] HTTP overhead < 100µs (Hyper parsing + ring buffer write)
- [ ] End-to-end p99 < 200µs (HTTP receive to Python worker ACK)
- [ ] Throughput > 500k req/s on 4-core machine

**PRODUCTION TARGETS (Cloud Run gen2 with CPU always-allocated):**
- [ ] End-to-end p99 < 2ms (10x relaxed for GIL + virtualization)
- [ ] End-to-end p999 < 5ms (Cloud Run scheduling noise)
- [ ] Throughput > 100k req/s with low latency (latency vs throughput tradeoff)

**Elite DX (Streaming/State) - BENCHMARK:**
- [ ] RPC call overhead < 50µs
- [ ] State patch latency < 100µs (Python mutation to JS callback)
- [ ] Subscription setup < 1ms

**Elite DX - PRODUCTION (network physics included):**
- [ ] RPC call overhead < 500µs (includes TCP/browser event loop)
- [ ] State patch latency < 1ms (realistic for WebSocket delivery)
- [ ] Type HMR push < 50ms (delivery only, not Python reload)

**Resource Efficiency:**
- [ ] Memory overhead < 10MB for server component
- [ ] No GC pauses (Rust has no GC)
- [ ] CPU utilization < 50% at 100k req/s

**Critical Mitigations (from Consensus):**
1. Use multi-process workers (1 per core) to avoid Python GIL contention
2. Cloud Run: Enable "CPU allocation: always allocated" in gen2
3. Implement batch ring buffer drains to amortize Python wake cost
4. Release GIL in Rust while waiting on ring buffer
5. Pre-allocate all ring buffer slots (no dynamic allocation in hot path)

#### NFR-2: Zero-Copy Verification

- [ ] Audit hot path for `memcpy` calls
- [ ] Hyper `Bytes` moved into ring buffer, not cloned
- [ ] Response assembly uses scatter-gather where possible

#### NFR-3: Platform Support

- [ ] Linux: Full support (primary target for Cloud Run)
- [ ] macOS: Full support (development)
- [ ] Windows: Basic support (may use fallback paths)

#### NFR-4: Feature Parity Testing (Gap from Gemini Review)

- [ ] Integration test suite runs against BOTH Rust and Bun servers
- [ ] CI pipeline validates feature parity on every PR
- [ ] Document any intentional behavioral differences
- [ ] Warn developers when using dev-only features
- [ ] Vite types.d.ts endpoint works identically (304 caching, ETag passthrough)

---

## Technical Approach

### Architecture Overview (Additive Design)

**Key Point:** Rust HTTP server ADDS to the existing Bun infrastructure. Bun continues running for database/S3/crypto. Components communicate via shared memory ring buffers.

```
                    ┌─────────────────────────────────────────┐
                    │           vertebra serve                 │
                    │         --server rust (prod)             │
                    │         --server bun (dev)               │
                    └─────────────────┬───────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         │                            │                            │
         ▼                            ▼                            ▼
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│    Rust Server      │    │     Bun Broker      │    │   Python Workers    │
│    (NEW - HTTP)     │    │   (EXISTING - DB)   │    │      (ASGI)         │
│                     │    │                     │    │                     │
│  • Hyper HTTP/1+2   │    │  • PostgreSQL pool  │    │  • Litestar app     │
│  • H2C (Cloud Run)  │    │  • MySQL pool       │    │  • Business logic   │
│  • WebSocket/H2     │    │  • Redis client     │    │  • gRPC stubs       │
│  • gRPC transport   │    │  • S3/GCS storage   │    │  • Procedure calls  │
│  • Zero-copy IPC    │    │  • Password hashing │    │                     │
└──────────┬──────────┘    │  • CSRF tokens      │    └──────────┬──────────┘
           │               │  • Dev hot reload   │               │
           │               └──────────┬──────────┘               │
           │                          │                          │
           └──────────────────────────┼──────────────────────────┘
                                      │
                           ┌──────────▼──────────┐
                           │   Shared Memory     │
                           │   Ring Buffers      │
                           │   (SPSC/MPMC)       │
                           │                     │
                           │  Already exists in  │
                           │  vertebra-core!     │
                           └─────────────────────┘

Mix-and-Match Example:
  --server rust  →  Rust handles HTTP  →  Ring Buffer  →  Python worker
       ↓                                                        ↓
  Bun Broker ←──────────── Ring Buffer ←─────────── DB query via Broker
```

### ASGI Proxy Architecture (per Codex Review)

The Rust server acts as a **transparent ASGI proxy**—it does NOT special-case any endpoints:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ASGI PROXY FLOW                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  HTTP Request (including /__vertebra/types.d.ts)                            │
│     └─→ Rust Server receives via Hyper                                      │
│         └─→ Encode to ring envelope:                                        │
│             - request_id: u64                                               │
│             - method: u8 (GET=0, POST=1, etc.)                              │
│             - path: length-prefixed bytes                                   │
│             - headers: Vec<(id, len, value)>  ← preserves ALL headers       │
│             - body: Stream<Bytes>                                           │
│                                                                             │
│  Ring Buffer (SPSC)                                                         │
│     └─→ Python worker reads envelope                                        │
│         └─→ ASGI app processes (TypeBridgeApp, Litestar, etc.)             │
│             └─→ May block 30s for long-poll (X-Vertebra-Watch)             │
│             └─→ Writes response envelope:                                   │
│                 - request_id: u64                                           │
│                 - status: u16 (200, 304, etc.)                              │
│                 - headers: Vec<(name, value)>  ← preserves ETag, etc.       │
│                 - body: Vec<u8> (empty for 304)                             │
│                                                                             │
│  HTTP Response                                                              │
│     └─→ Rust Server decodes envelope                                        │
│         └─→ Sends via Hyper (preserving status + ALL headers)               │
│                                                                             │
│  CRITICAL: Type bridge logic stays in Python, Rust is transparent proxy    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Header Pass-Through Requirements

| Header | Direction | Purpose | MUST Preserve |
|--------|-----------|---------|---------------|
| `If-None-Match` | Request | Cache validation | ✓ |
| `ETag` | Response | Cache identifier | ✓ |
| `X-Vertebra-Watch` | Request | HMR long-poll trigger | ✓ |
| `X-TypeScript-Version` | Response | Type version tracking | ✓ |
| `Accept-Encoding` | Request | Compression negotiation | ✓ |
| `Content-Encoding` | Response | Compression indicator | ✓ |
| `Cache-Control` | Response | Caching policy | ✓ |

**Strip only hop-by-hop headers:** `Connection`, `Keep-Alive`, `Transfer-Encoding`

### Data Flow: Zero-Copy Path

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ZERO-COPY REQUEST PATH                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. TCP Socket                                                          │
│     └─→ Hyper reads into Bytes (reference-counted buffer)               │
│                                                                         │
│  2. HTTP/2 Frame                                                        │
│     └─→ Hyper parses headers, yields body as Stream<Bytes>              │
│                                                                         │
│  3. Ring Buffer Write                                                   │
│     └─→ Bytes moved (not copied) into ring buffer slot                  │
│     └─→ Only pointer + length written to shared memory                  │
│                                                                         │
│  4. Python Worker                                                       │
│     └─→ PyO3 exposes buffer as memoryview (no copy)                     │
│     └─→ ASGI receive() yields chunks                                    │
│                                                                         │
│  5. Response                                                            │
│     └─→ Python writes to ring buffer                                    │
│     └─→ Rust reads and sends via Hyper                                  │
│                                                                         │
│  TOTAL COPIES: 0 (in hot path)                                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Hyper Server Implementation

```rust
// src/rs/vertebra-core/src/server/mod.rs

use hyper::{server::conn::http2, service::service_fn, Request, Response};
use hyper::body::{Bytes, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

pub struct VertebraServer {
    listener: TcpListener,
    ring_pool: RingPool,
}

impl VertebraServer {
    pub async fn serve_h2c(&self) -> Result<(), Error> {
        loop {
            let (stream, _addr) = self.listener.accept().await?;
            let io = TokioIo::new(stream);
            let ring = self.ring_pool.acquire();

            tokio::spawn(async move {
                // HTTP/2 cleartext (no TLS)
                let builder = http2::Builder::new(TokioExecutor::new())
                    .enable_connect_protocol();  // RFC 8441 WebSocket

                let service = service_fn(move |req| {
                    handle_request(req, ring.clone())
                });

                if let Err(e) = builder.serve_connection(io, service).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_request(
    req: Request<Incoming>,
    ring: SpscRing,
) -> Result<Response<Full<Bytes>>, Error> {
    // Check for WebSocket upgrade (RFC 8441)
    if is_websocket_upgrade(&req) {
        return handle_websocket(req, ring).await;
    }

    // Check for gRPC
    if is_grpc_request(&req) {
        return handle_grpc(req, ring).await;
    }

    // Standard HTTP: stream body to ring buffer
    let body = req.into_body();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame?.into_data() {
            // ZERO COPY: Bytes moved into ring buffer
            ring.send_bytes(data)?;
        }
    }

    // Wait for response from Python worker
    let response_data = ring.recv_async().await?;
    Ok(Response::new(Full::new(response_data)))
}
```

### WebSocket over HTTP/2 (RFC 8441)

```rust
// src/rs/vertebra-core/src/server/websocket.rs

use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

async fn handle_websocket(
    req: Request<Incoming>,
    ring: SpscRing,
) -> Result<Response<Empty<Bytes>>, Error> {
    // Verify it's a valid WebSocket upgrade
    let protocol = req.extensions().get::<hyper::ext::Protocol>();
    if protocol != Some(&"websocket".parse().unwrap()) {
        return Err(Error::InvalidProtocol);
    }

    // Spawn handler for upgraded connection
    tokio::spawn(async move {
        match hyper::upgrade::on(req).await {
            Ok(upgraded) => {
                let ws = WebSocketStream::from_raw_socket(
                    upgraded,
                    Role::Server,
                    None,
                ).await;

                // Bridge WebSocket frames to IPC
                websocket_ipc_bridge(ws, ring).await;
            }
            Err(e) => eprintln!("WebSocket upgrade failed: {}", e),
        }
    });

    // Return 200 OK for CONNECT (RFC 8441)
    Ok(Response::new(Empty::new()))
}

async fn websocket_ipc_bridge(
    mut ws: WebSocketStream<impl AsyncRead + AsyncWrite>,
    ring: SpscRing,
) {
    loop {
        tokio::select! {
            // Client → Worker
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        ring.send(&data).ok();
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            // Worker → Client
            data = ring.recv_async() => {
                if let Ok(data) = data {
                    ws.send(Message::Binary(data.to_vec())).await.ok();
                }
            }
        }
    }
}
```

### gRPC Transport

```rust
// src/rs/vertebra-core/src/server/grpc.rs

fn is_grpc_request(req: &Request<Incoming>) -> bool {
    req.headers()
        .get(CONTENT_TYPE)
        .map(|v| v.as_bytes().starts_with(b"application/grpc"))
        .unwrap_or(false)
}

async fn handle_grpc(
    req: Request<Incoming>,
    ring: SpscRing,
) -> Result<Response<Full<Bytes>>, Error> {
    // gRPC uses HTTP/2 with specific framing:
    // - 1 byte: compressed flag
    // - 4 bytes: message length (big-endian)
    // - N bytes: message data

    let body = req.into_body();

    // Stream gRPC frames to ring buffer
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame?.into_data() {
            // Parse gRPC frame header
            if data.len() >= 5 {
                let _compressed = data[0];
                let length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
                let message = &data[5..5 + length as usize];

                // Send to Python worker
                ring.send(message)?;
            }
        }
    }

    // Receive response from Python
    let response_data = ring.recv_async().await?;

    // Frame as gRPC response
    let mut grpc_response = Vec::with_capacity(5 + response_data.len());
    grpc_response.push(0); // not compressed
    grpc_response.extend_from_slice(&(response_data.len() as u32).to_be_bytes());
    grpc_response.extend_from_slice(&response_data);

    Ok(Response::builder()
        .header(CONTENT_TYPE, "application/grpc")
        .header("grpc-status", "0")
        .body(Full::new(Bytes::from(grpc_response)))?)
}
```

### Python Bindings (PyO3)

```rust
// src/rs/vertebra-py/src/server.rs

use pyo3::prelude::*;

#[pyclass]
pub struct VertebraServer {
    inner: Arc<vertebra_core::server::VertebraServer>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl VertebraServer {
    #[new]
    fn new(host: &str, port: u16, workers: usize) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let inner = runtime.block_on(async {
            vertebra_core::server::VertebraServer::bind(host, port, workers).await
        })?;

        Ok(Self { inner: Arc::new(inner), runtime })
    }

    fn serve(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            self.runtime.block_on(self.inner.serve_h2c())
        })?;
        Ok(())
    }

    fn shutdown(&self) -> PyResult<()> {
        self.runtime.block_on(self.inner.shutdown())?;
        Ok(())
    }
}
```

### Elite DX Architecture: Real-Time Bridge

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        ELITE DX: REAL-TIME BRIDGE                            │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PYTHON BACKEND                           FRONTEND (React/Vue/Svelte)        │
│  ══════════════                           ═══════════════════════════        │
│                                                                              │
│  ┌─────────────────┐                      ┌─────────────────────────┐       │
│  │  @procedure     │───RPC Call──────────▶│  client.todos.add()     │       │
│  │  def add_todo() │                      │                         │       │
│  └─────────────────┘                      │  // Type-safe, instant  │       │
│                                           └─────────────────────────┘       │
│  ┌─────────────────┐                      ┌─────────────────────────┐       │
│  │  @procedure     │                      │  client.chat.subscribe( │       │
│  │  async def chat │───Subscription──────▶│    msg => render(msg)   │       │
│  │  () -> AsyncGen │    (WebSocket)       │  )                      │       │
│  └─────────────────┘                      └─────────────────────────┘       │
│                                                                              │
│  ┌─────────────────┐                      ┌─────────────────────────┐       │
│  │  class Counter  │                      │  useVertebraState(      │       │
│  │  (NativeState): │───JSON Patch────────▶│    "counter"            │       │
│  │    count: int   │    (Auto-Push)       │  ) // Auto-reactive     │       │
│  └─────────────────┘                      └─────────────────────────┘       │
│                                                                              │
│  TRANSPORT: WebSocket over HTTP/2 (RFC 8441) or Shared Memory Channel       │
│  LATENCY TARGET: < 50µs for RPC overhead, < 100µs for state patches         │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### NativeState Implementation

```python
# src/py/vertebra/state.py

from msgspec import Struct
from vertebra.procedures import procedure
import json_patch

class NativeState(Struct):
    """Base class for auto-syncing state objects."""

    _subscribers: list[Callable] = []
    _previous_state: bytes = b""

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if not name.startswith("_"):
            self._push_patch()

    def _push_patch(self):
        """Generate and push JSON Patch to all subscribers."""
        current = msgspec.json.encode(self)
        if self._previous_state:
            patch = json_patch.make_patch(
                msgspec.json.decode(self._previous_state),
                msgspec.json.decode(current)
            )
            for subscriber in self._subscribers:
                subscriber({"type": "patch", "ops": patch.to_string()})
        self._previous_state = current

    @classmethod
    def create_subscription(cls, instance_key: str):
        """Auto-generate subscription endpoint for this state."""
        @procedure
        async def subscribe() -> AsyncGenerator[dict, None]:
            instance = get_state_instance(instance_key)
            # Initial state
            yield {"type": "init", "state": msgspec.json.decode(msgspec.json.encode(instance))}
            # Patches
            queue = asyncio.Queue()
            instance._subscribers.append(queue.put_nowait)
            try:
                while True:
                    patch = await queue.get()
                    yield patch
            finally:
                instance._subscribers.remove(queue.put_nowait)
        return subscribe
```

### Type Bridge Extensions for Streams

```python
# src/py/vertebra/types/ir.py additions

@dataclass
class SubscriptionType(TypeNode):
    """Represents AsyncGenerator[T, None] → Subscription<T>"""
    yield_type: TypeNode

@dataclass
class StreamType(TypeNode):
    """Represents Iterator[T] → Stream<T>"""
    item_type: TypeNode
```

```typescript
// Generated TypeScript
interface Subscription<T> {
    subscribe(callback: (value: T) => void): () => void;  // Returns unsubscribe
    [Symbol.asyncIterator](): AsyncIterator<T>;
}

// Usage
const unsub = client.chat.messages.subscribe(msg => console.log(msg));
// Or async iteration
for await (const msg of client.chat.messages) {
    console.log(msg);
}
```

### File Structure (Updated per Gemini Review)

```
src/rs/vertebra-core/
├── Cargo.toml                    # Add: hyper, http-body-util, tokio-tungstenite 0.28
├── src/
│   ├── lib.rs                    # Add: pub mod server;
│   ├── server/
│   │   ├── mod.rs               # NEW: VertebraServer, ServerConfig
│   │   ├── listener.rs          # NEW: TCP/Unix socket binding and accept loop
│   │   ├── dispatcher.rs        # NEW: Load balancing for ring selection
│   │   │
│   │   ├── protocol/            # Protocol implementations
│   │   │   ├── mod.rs
│   │   │   ├── http1.rs         # NEW: HTTP/1.1 Hyper service
│   │   │   ├── h2c.rs           # NEW: HTTP/2 cleartext service
│   │   │   ├── websocket.rs     # NEW: RFC 8441 WebSocket upgrade
│   │   │   └── grpc.rs          # NEW: gRPC frame detection
│   │   │
│   │   └── bridge/              # IPC Integration
│   │       ├── mod.rs
│   │       ├── header.rs        # NEW: Zerocopy header serialization
│   │       ├── body.rs          # NEW: Zero-copy Bytes handling
│   │       └── response.rs      # NEW: Response assembly from ring buffer
│   │
│   ├── channel.rs               # EXISTING: SPSC ring buffers
│   ├── async_ring.rs            # EXISTING: Async wrappers
│   └── ...

src/rs/vertebra-py/
├── src/
│   ├── lib.rs                   # Add: server module
│   └── server.rs                # NEW: PyO3 VertebraServer bindings

src/py/vertebra/
├── server/
│   ├── __init__.py              # Add: VertebraServer re-export
│   └── rust.py                  # NEW: Python wrapper for Rust server
├── cli/
│   └── serve.py                 # MODIFY: Add --server rust flag
├── state.py                     # NEW: NativeState base class
├── types/
│   ├── ir.py                    # MODIFY: Add SubscriptionType, StreamType
│   ├── extractor.py             # MODIFY: Handle AsyncGenerator
│   └── typescript.py            # MODIFY: Generate Subscription<T>
├── procedures/
│   ├── decorator.py             # MODIFY: Detect AsyncGenerator
│   └── serialization.py         # NEW: StreamingResponse serializer

src/ts/
├── packages/
│   └── vertebra-client/         # NEW: TypeScript RPC client package
│       ├── src/
│       │   ├── index.ts         # Client factory
│       │   ├── transport.ts     # WebSocket/fetch transport
│       │   ├── subscription.ts  # Subscription<T> implementation
│       │   └── state.ts         # State sync utilities
│       └── package.json
├── react/                       # NEW: React adapter
│   └── useVertebraState.ts
├── svelte/                      # NEW: Svelte adapter
│   └── vertebraStore.ts
├── vue/                         # NEW: Vue adapter
│   └── useVertebraState.ts

src/schema/                       # NEW: Proto definitions
├── services/
│   └── example.proto            # NEW: Example gRPC service
```

---

## Testing Strategy

### Unit Tests (Rust)

```rust
// src/rs/vertebra-core/src/server/tests.rs

#[tokio::test]
async fn test_h2c_request() {
    let server = VertebraServer::bind("127.0.0.1", 0, 1).await.unwrap();
    let addr = server.local_addr();

    tokio::spawn(server.serve_h2c());

    // Test with h2c client
    let client = hyper::Client::builder()
        .http2_only(true)
        .build_http();

    let resp = client.get(format!("http://{}/health", addr).parse().unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_zero_copy_body() {
    // Verify Bytes are moved, not copied
    let data = Bytes::from(vec![0u8; 1024]);
    let ptr_before = data.as_ptr();

    let ring = SpscRing::new(4096, 16);
    ring.send_bytes(data).unwrap();

    let received = ring.recv().unwrap();
    // In true zero-copy, pointer would be same region
    // (This is a simplified test; actual verification requires more instrumentation)
}
```

### Integration Tests

```bash
# H2C with Cloud Run simulation
curl --http2-prior-knowledge -v http://localhost:8080/health

# WebSocket over HTTP/2
wscat --http2 -c ws://localhost:8080/ws

# gRPC
grpcurl -plaintext localhost:8080 vertebra.example.Greeter/SayHello
```

### Benchmark Tests

```bash
# Throughput benchmark
oha -z 10s -c 256 --http2 http://localhost:8080/echo

# Latency benchmark
oha -z 10s -c 1 --http2 --latency-correction http://localhost:8080/echo

# Compare with Bun baseline
vertebra serve --server bun &
oha -z 10s -c 256 http://localhost:8080/echo
```

---

## Implementation Phases

### Phase 1: HTTP/1.1 + H2C Server (Priority: CRITICAL)

**Goal:** Basic Hyper server with h2c support.

- [ ] Add Hyper dependencies to vertebra-core Cargo.toml
- [ ] Create `src/rs/vertebra-core/src/server/mod.rs`
- [ ] Implement basic request/response handling
- [ ] Integrate with existing ring buffers
- [ ] Add health check endpoint
- [ ] Test with `curl --http2-prior-knowledge`

**Validation:** Cloud Run deployment accepts h2c traffic.

### Phase 2: Zero-Copy Integration (Priority: CRITICAL)

**Goal:** Verify and optimize zero-copy path.

- [ ] Audit Bytes flow through ring buffers
- [ ] Remove any unnecessary copies
- [ ] Implement backpressure handling
- [ ] Add memory instrumentation for verification
- [ ] Benchmark latency improvements

**Validation:** No `memcpy` in hot path, IPC p99 < 10µs, HTTP overhead < 100µs.

### Phase 3: WebSocket over HTTP/2 (Priority: HIGH)

**Goal:** RFC 8441 compliance for WebSocket/H2.

- [ ] Enable `extended_connect_protocol`
- [ ] Implement CONNECT handler
- [ ] Bridge WebSocket frames to IPC
- [ ] Handle multiple concurrent streams
- [ ] Test with real WebSocket clients

**Validation:** WebSocket works over HTTP/2 connection.

### Phase 4: gRPC Transport (Priority: HIGH)

**Goal:** Native gRPC support.

- [ ] Add gRPC frame parsing
- [ ] Route gRPC to same ring buffers
- [ ] Add betterproto2 for Python stubs
- [ ] Create example service
- [ ] Test with grpcurl

**Validation:** gRPC services work end-to-end.

### Phase 5: Production Hardening (Priority: HIGH)

**Goal:** Production-ready deployment.

- [ ] PyO3 bindings for Python CLI
- [ ] `--server rust` CLI flag
- [ ] Graceful shutdown with draining
- [ ] Cloud Run deployment examples
- [ ] Performance benchmarks vs Bun baseline
- [ ] Documentation

**Validation:** Full production deployment succeeds.

### Phase 6: Elite DX - Streaming RPC (Priority: HIGH)

**Goal:** tRPC-style subscriptions with AsyncGenerator support.

- [ ] Extend Type Bridge IR with `SubscriptionType`, `StreamType`
- [ ] Update `@procedure` to detect `AsyncGenerator` return types
- [ ] Implement NDJSON/SSE streaming serializer
- [ ] Add WebSocket subscription transport to Rust server
- [ ] Create `vertebra-client` TypeScript package
- [ ] Generate `Subscription<T>` types from Python

**Validation:** JS client receives stream from Python AsyncGenerator.

### Phase 7: Elite DX - NativeState (Priority: HIGH)

**Goal:** Real-time Python → Frontend state sync.

- [ ] Implement `NativeState` base class
- [ ] Auto-generate subscription endpoints for state instances
- [ ] Implement JSON Patch generation on mutation
- [ ] Create React `useVertebraState<T>()` hook
- [ ] Create Svelte store adapter
- [ ] Create Vue composable
- [ ] Auto-reconnect with state rehydration

**Validation:** Python state mutation triggers React component re-render.

### Phase 8: Elite DX - Vite Integration (Priority: MEDIUM)

**Goal:** Seamless frontend tooling integration.

- [ ] Rust server serves `/__vertebra/types.d.ts`
- [ ] WebSocket channel for instant type HMR (replace polling)
- [ ] Vite HMR proxy in development mode
- [ ] Asset serving from Vite manifest
- [ ] Preserve Litestar native pipeline compatibility

**Validation:** Type changes trigger instant HMR without polling.

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Tokio/ring buffer boundary complexity | Medium | Medium | Existing async_ring.rs provides patterns |
| RFC 8441 implementation bugs | Low | Medium | Extensive testing with real clients |
| Performance regression vs Bun | Low | High | Continuous benchmarking, keep Bun as fallback |
| Cloud Run edge cases | Medium | Medium | Test with actual Cloud Run, not just local |
| gRPC streaming complexity | Medium | Medium | Start with unary, add streaming later |

---

## Zero-Copy Library Recommendations

### Analysis: Pingora vs Direct Hyper

**Decision: Build directly on Hyper, NOT Pingora.**

| Aspect | Pingora | Direct Hyper |
|--------|---------|--------------|
| **Primary Use** | L7 HTTP proxy | Application server |
| **Architecture** | Receives → forwards to upstream | Receives → handles directly |
| **Zero-copy path** | Optimized for proxy forwarding | Direct ring buffer writes |
| **Complexity** | Proxy abstractions we don't need | Minimal, exactly what we need |
| **Internal** | Uses Hyper internally anyway | We get the same primitives |

Pingora (35M+ req/sec, 70% less CPU than NGINX) is impressive but designed to sit *in front of* application servers. Vertebra needs HTTP to write *directly* into shared-memory ring buffers—no upstream forwarding. Building on Hyper (which powers Pingora) gives us direct control.

**workerd** (Cloudflare's JS/WASM runtime) is unrelated—different product for different purpose.

### Recommended Zero-Copy Crates

#### 1. `zerocopy` (Google) - HIGH VALUE

```rust
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(AsBytes, FromBytes, FromZeroes)]
#[repr(C)]
struct IpcHeader {
    msg_type: u32,
    payload_len: u32,
    request_id: u64,
}

// Zero-copy parse from ring buffer
fn parse_header(buf: &[u8]) -> Option<&IpcHeader> {
    IpcHeader::ref_from_prefix(buf)
}
```

- Compile-time safety for memory layout
- Zero-copy parsing of structured data
- Production-proven at Google scale

#### 2. `bytes` (already via Hyper) - ESSENTIAL

Hyper's `Bytes` type is reference-counted, enabling zero-copy transfer:

```rust
let data: Bytes = body.frame().await?.into_data()?;
ring.send_bytes(data)?;  // Moved, not copied
```

#### 3. `ipmpsc` - WORTH EVALUATING

Shared memory ring buffer with zero-copy deserialization. Claims 10x better than `ipc_channel`:

```rust
use ipmpsc::{ZeroCopyReceiver, SharedRingBuffer};

let receiver = ZeroCopyReceiver::new(ring_buffer)?;
let msg: &MyStruct = receiver.recv_zero_copy()?;  // No deserialization!
```

Consider for future ring buffer enhancements.

#### 4. `tokio-uring` - FUTURE OPTIMIZATION (Linux 5.1+)

True kernel-bypass async I/O for ultimate performance:

```rust
// Future phase: io_uring for direct DMA to userspace
use tokio_uring::net::TcpStream;

let (result, buf) = stream.read(vec![0u8; 4096]).await;
// Kernel reads directly into our buffer, no copies
```

Roadmap item for Linux-specific elite mode.

---

## Dependencies

### New Rust Crates (Updated per 2026 API Review)

```toml
# src/rs/vertebra-core/Cargo.toml
[dependencies]
hyper = { version = "1.8", features = ["http1", "http2", "server"] }
hyper-util = { version = "0.1.19", features = ["tokio"] }
http-body-util = "0.1.3"
tokio-tungstenite = "0.28"  # Significant perf improvements since 0.26
zerocopy = { version = "0.8", features = ["derive"] }  # Formally verified

# Optional: experimental only (NOT production ready)
# tokio-uring = { version = "0.5", optional = true }  # 0.x API, Linux only
```

### New Python Dependencies

```toml
# pyproject.toml
[project.optional-dependencies]
grpc = ["betterproto2[compiler,grpclib]>=0.1.0"]
```

---

## References

- [Hyper HTTP/2 Guide](https://hyper.rs/guides/1/server/hello-world/)
- [RFC 8441: Bootstrapping WebSockets with HTTP/2](https://datatracker.ietf.org/doc/html/rfc8441)
- [Cloud Run HTTP/2 Configuration](https://cloud.google.com/run/docs/configuring/http2)
- [Cloudflare Pingora Architecture](https://blog.cloudflare.com/how-we-built-pingora-the-proxy-that-connects-cloudflare-to-the-internet/)
- [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite)
- [Betterproto2](https://betterproto.github.io/python-betterproto2/getting-started/)
