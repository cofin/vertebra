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

### Patterns to Follow

- **Zero-Copy First**: Hyper `Bytes` → ring buffer without clone
- **Tokio Runtime**: Async HTTP handling with existing sync ring primitives
- **RFC 8441**: WebSocket over HTTP/2 via extended CONNECT
- **Industry Precedent**: Cloudflare Pingora, high-frequency trading systems

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

Vertebra's product guidelines mandate **"Elite Performance & Design"** with **"Zero-Copy First"** architecture. The current Bun-based HTTP layer has limitations:

1. **No H2C Support**: Google Cloud Run requires HTTP/2 cleartext (h2c) for end-to-end HTTP/2. Bun's h2c support is undocumented/unreliable.

2. **V8 GC Pauses**: Bun's JavaScript runtime introduces garbage collection pauses that violate sub-millisecond latency targets.

3. **FFI Overhead**: Current flow requires: `Bun HTTP → Serialization → bun:ffi → Ring Buffer`. Each boundary adds latency.

4. **No Native gRPC**: Users building microservices must bolt on gRPC separately, losing shared-memory benefits.

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
| Request latency p99 | < 1ms | Benchmark suite |
| Zero-copy verification | Pass | No `memcpy` in hot path |
| gRPC unary latency | < 2ms | grpcurl benchmark |
| Bun still works for dev | Pass | `vertebra serve --dev` |

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

#### FR-7: Bun Compatibility Mode

- [ ] Bun remains default for `--dev` mode
- [ ] Bun continues to handle: database drivers, S3, crypto, static assets
- [ ] Clear migration path for existing Bun-based deployments
- [ ] Runtime selection via environment variable or CLI flag

### Non-Functional Requirements

#### NFR-1: Performance

- [ ] Request latency p50 < 500µs, p99 < 1ms
- [ ] Throughput > 100k req/s on 4-core machine
- [ ] Memory overhead < 10MB for server component
- [ ] No GC pauses (Rust has no GC)

#### NFR-2: Zero-Copy Verification

- [ ] Audit hot path for `memcpy` calls
- [ ] Hyper `Bytes` moved into ring buffer, not cloned
- [ ] Response assembly uses scatter-gather where possible

#### NFR-3: Platform Support

- [ ] Linux: Full support (primary target for Cloud Run)
- [ ] macOS: Full support (development)
- [ ] Windows: Basic support (may use fallback paths)

---

## Technical Approach

### Architecture Overview

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
│    (Production)     │    │   (Dev + Services)  │    │      (ASGI)         │
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
```

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

### File Structure

```
src/rs/vertebra-core/
├── Cargo.toml                    # Add: hyper, http-body-util, tokio-tungstenite
├── src/
│   ├── lib.rs                    # Add: pub mod server;
│   ├── server/
│   │   ├── mod.rs               # NEW: VertebraServer, serve_h2c()
│   │   ├── handler.rs           # NEW: Request handling, routing
│   │   ├── websocket.rs         # NEW: RFC 8441 WebSocket/H2
│   │   ├── grpc.rs              # NEW: gRPC frame handling
│   │   └── health.rs            # NEW: Health check endpoint
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

**Validation:** No `memcpy` in hot path, p99 < 1ms.

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

### New Rust Crates

```toml
# src/rs/vertebra-core/Cargo.toml
[dependencies]
hyper = { version = "1.4", features = ["http1", "http2", "server"] }
hyper-util = { version = "0.1", features = ["tokio"] }
http-body-util = "0.1"
tokio-tungstenite = "0.24"
zerocopy = { version = "0.8", features = ["derive"] }  # Zero-copy struct parsing

# Optional: evaluate for future
# ipmpsc = "0.2"  # Zero-copy shared memory IPC
# tokio-uring = "0.5"  # Linux io_uring (feature-gated)
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
