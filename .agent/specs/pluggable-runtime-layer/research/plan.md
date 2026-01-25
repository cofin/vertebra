# Research Plan: Elite-Performance HTTP Layer & Protocol Support

## Executive Summary

After multi-model consensus analysis (Gemini Pro + Gemini Flash, both 9/10 confidence), the optimal architecture for Vertebra's elite performance requirements is clear:

**Build a custom Rust HTTP server using Hyper directly in `vertebra-core` that writes to existing ring buffers.**

This is the only architecture that satisfies:
- Zero-copy mandate (Hyper `Bytes` → ring buffer without clone)
- H2C requirement for Cloud Run (`http2_only(true)`)
- WebSocket over HTTP/2 (RFC 8441 `extended_connect_protocol`)
- Sub-millisecond latency targets

## Part 1: Multi-Model Consensus Results

### Models Consulted
1. **Gemini 3 Pro** (stance: FOR custom Rust) - 9/10 confidence
2. **Gemini 3 Flash** (stance: AGAINST/Devil's advocate) - 9/10 confidence

### Unanimous Conclusion
Both models independently concluded that extending `vertebra-core` with Hyper-based HTTP is "the only architecture" that meets requirements. Key quotes:

> "Building a custom Rust HTTP/2 server layer directly within vertebra-core using Hyper is the only architecture that satisfies the 'Elite Performance' and zero-copy mandates." - Gemini Pro

> "Option C (extending vertebra-core with a custom Hyper-based implementation) is the only architecture that satisfies the 'Zero-Copy First' mandate while providing the granular control required for h2c and RFC 8441 WebSockets." - Gemini Flash (Devil's Advocate)

### Options Evaluated and Discarded

| Option | Verdict | Reason |
|--------|---------|--------|
| **Hypercorn** | REJECTED | Python GIL incompatible with "sub-millisecond, zero-copy" mandates |
| **Granian** | REJECTED | ASGI/RSGI bottleneck, translation cost to ring buffers |
| **Bun (current)** | REJECTED for h2c | Lacks production h2c, V8 GC pauses |
| **Sidecar Proxy (Envoy)** | REJECTED | Adds network hop, violates "Efficiency over Simplicity" |
| **Custom Hyper in vertebra-core** | APPROVED | Direct IPC injection, zero-copy, h2c native |

---

## Part 2: Technical Architecture

### 2.1 Data Flow Comparison

**Current (Bun-based):**
```
HTTP Request → Bun HTTP (V8) → Serialization → bun:ffi → Ring Buffer → Python Worker
                    ↑
              GC pauses, no h2c
```

**Proposed (Rust Hyper):**
```
HTTP Request → Hyper (Rust/Tokio) → Direct Write → Ring Buffer → Python Worker
                    ↑
              Zero-copy, native h2c, no GC
```

### 2.2 Hyper Configuration for H2C

```rust
// vertebra-core/src/server/mod.rs
use hyper::server::conn::http2;
use hyper::service::service_fn;

async fn serve_h2c(listener: TcpListener) {
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            // h2c: HTTP/2 over cleartext (no TLS)
            http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, service_fn(handle_request))
                .await
        });
    }
}
```

### 2.3 Zero-Copy Request Body Handling

```rust
// Stream request body directly into ring buffer
async fn handle_request(req: Request<Incoming>) -> Result<Response<Full<Bytes>>> {
    let ring = get_worker_ring();

    // Hyper's Bytes are reference-counted, no copy needed
    while let Some(chunk) = req.body().frame().await {
        if let Ok(data) = chunk?.into_data() {
            // Write directly to shared memory ring buffer
            ring.send_bytes(data)?;  // Bytes moved, not copied
        }
    }

    // Wait for response from Python worker
    let response_bytes = ring.recv().await?;
    Ok(Response::new(Full::new(response_bytes)))
}
```

### 2.4 WebSocket over HTTP/2 (RFC 8441)

```rust
use hyper::upgrade::Upgraded;
use tokio_tungstenite::WebSocketStream;

// Enable extended CONNECT for WebSocket over H2
let builder = http2::Builder::new(executor)
    .enable_connect_protocol();  // RFC 8441

async fn handle_websocket_upgrade(req: Request<Incoming>) -> Result<Response<Empty<Bytes>>> {
    if req.method() == Method::CONNECT &&
       req.extensions().get::<hyper::ext::Protocol>() == Some(&"websocket".into()) {

        tokio::spawn(async move {
            let upgraded = hyper::upgrade::on(req).await?;
            let ws_stream = WebSocketStream::from_raw_socket(upgraded, Role::Server, None).await;

            // Bridge WebSocket frames to IPC
            handle_websocket_ipc(ws_stream).await
        });

        Ok(Response::new(Empty::new()))
    }
}
```

### 2.5 Cloud Run Compatibility

Cloud Run requires:
1. **Port name `h2c`** in container spec
2. **Prior-knowledge h2c** (client assumes HTTP/2 without upgrade)
3. **Health checks over h2c**

```yaml
# Cloud Run service.yaml
spec:
  template:
    spec:
      containers:
        - image: gcr.io/project/vertebra-app
          ports:
            - name: h2c  # Critical: enables HTTP/2 cleartext
              containerPort: 8080
```

---

## Part 3: Bun's Remaining Role

### What Bun Keeps
With Rust handling HTTP ingress, Bun's role becomes:

1. **Database Drivers** - Bun.SQL is genuinely fast for PostgreSQL/MySQL
2. **S3 Client** - Bun.S3Client is optimized
3. **Password Hashing** - Bun.password uses native crypto
4. **Development Server** - Hot reload, static assets for local dev
5. **Build Tooling** - Asset bundling, TypeScript compilation

### Architecture After Change

```
                    ┌─────────────────────────────────────────┐
                    │           vertebra serve                 │
                    └─────────────────┬───────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
              ▼                       ▼                       ▼
    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
    │   Rust Server   │     │   Bun Broker    │     │  Python Workers │
    │   (Hyper h2c)   │     │  (DB/Redis/S3)  │     │    (ASGI)       │
    │                 │     │                 │     │                 │
    │  • HTTP/1.1     │     │  • PostgreSQL   │     │  • Litestar     │
    │  • HTTP/2 (h2c) │     │  • MySQL        │     │  • Business     │
    │  • WebSocket    │     │  • SQLite       │     │    Logic        │
    │  • gRPC         │     │  • Redis        │     │  • gRPC Stubs   │
    └────────┬────────┘     │  • S3/Storage   │     └────────┬────────┘
             │              │  • Crypto Ops   │              │
             │              └────────┬────────┘              │
             │                       │                       │
             └───────────────────────┴───────────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │   Shared Memory     │
                          │   Ring Buffers      │
                          │   (SPSC/MPMC)       │
                          └─────────────────────┘
```

---

## Part 4: gRPC Integration

### Betterproto for Python Stubs
The consensus supports adding gRPC via betterproto2 for clean Python stubs:

```protobuf
// src/schema/services/greeter.proto
syntax = "proto3";
package vertebra.example;

service Greeter {
    rpc SayHello(HelloRequest) returns (HelloResponse);
}
```

### Rust Server Handles gRPC Transport

Since we're building the HTTP layer in Rust, gRPC transport comes naturally:
- Hyper handles HTTP/2 framing
- `tonic` or manual implementation for gRPC semantics
- Request bodies route through same ring buffers to Python

```rust
// gRPC detection: Content-Type: application/grpc
fn is_grpc_request(req: &Request<Incoming>) -> bool {
    req.headers()
        .get(CONTENT_TYPE)
        .map(|v| v.as_bytes().starts_with(b"application/grpc"))
        .unwrap_or(false)
}
```

---

## Part 5: Implementation Difficulty Assessment

### Overall Difficulty: 7/10

| Component | Effort | Risk | Notes |
|-----------|--------|------|-------|
| Hyper HTTP/1.1 server | 3 days | LOW | Well-documented, many examples |
| H2C (HTTP/2 cleartext) | 2 days | LOW | `http2_only(true)` is simple |
| WebSocket over HTTP/2 | 4 days | MEDIUM | RFC 8441 requires manual CONNECT handling |
| Ring buffer integration | 3 days | LOW | Already have ring buffers, just glue code |
| gRPC transport | 5 days | MEDIUM | Can use tonic or manual impl |
| Python bindings (PyO3) | 2 days | LOW | Already have PyO3 in project |
| Cloud Run testing | 2 days | MEDIUM | Need to test with actual Cloud Run |
| **Total** | **21 days** | **MEDIUM** | |

### Key Technical Challenges

1. **Tokio/Ring Buffer Boundary**: Hyper is async (tokio), ring buffers may be sync/spin-wait. Need careful boundary management.

2. **Backpressure**: When Python workers are slow, need to handle backpressure from ring buffers back to HTTP clients.

3. **Connection Lifecycle**: HTTP/2 connections are long-lived. Need to handle connection draining on shutdown.

4. **Memory Layout**: For true zero-copy, may need ring buffer slots sized to match HTTP/2 frame sizes.

---

## Part 6: Alternative Considerations

### Why Not Just Fork/Extend Granian?

Granian uses Hyper internally, so why not extend it?

1. **RSGI Bottleneck**: Granian speaks RSGI/ASGI to Python, adding a translation layer
2. **No Ring Buffer Access**: Can't inject directly into our shared memory
3. **Maintenance Burden**: Forking means diverging from upstream
4. **Architecture Mismatch**: Granian is "server for Python apps", we need "server that coordinates workers"

### Why Not Pingora?

Cloudflare's Pingora is impressive but:
1. **Proxy-Focused**: Designed for proxying, not application serving
2. **Heavy**: More features than we need
3. **Learning Curve**: Different abstractions than Hyper ecosystem

### Future: io_uring

Once the Hyper server is in place, io_uring integration becomes possible:
- `tokio-uring` for Linux
- Direct kernel bypass for even lower latency
- Read directly into shared memory pages

---

## Part 7: Recommendations

### Immediate Actions

1. **Create `vertebra-core/src/server/` module** with Hyper-based HTTP server
2. **Add dependencies**: `hyper`, `http-body-util`, `tokio-tungstenite`
3. **Implement h2c endpoint** with direct ring buffer writes
4. **Test with Cloud Run** using `curl --http2-prior-knowledge`

### Phased Rollout

1. **Phase 1**: HTTP/1.1 + H2C server with ring buffer integration
2. **Phase 2**: WebSocket over HTTP/2 (RFC 8441)
3. **Phase 3**: gRPC transport
4. **Phase 4**: Bun becomes optional (development mode only)

### Keep Bun For

- Local development (hot reload, static serving)
- Database drivers (genuinely fast)
- Crypto operations (password hashing)
- Build tooling

---

## Part 8: Zero-Copy Rust Libraries Analysis

### Pingora Evaluation

**Decision: Build directly on Hyper, NOT Pingora.**

Cloudflare Pingora is impressive (35M+ req/sec, 70% less CPU than NGINX) but is designed as an HTTP **proxy**—it sits *in front of* application servers and forwards requests to upstream services.

Vertebra needs the HTTP layer to write *directly* into shared-memory ring buffers. Using Pingora would add an unnecessary layer:

```
HTTP → Pingora → ??? → Ring Buffers  ❌ Extra hop
HTTP → Hyper → Ring Buffers          ✅ Direct path
```

Since Pingora uses Hyper internally, we get the same primitives by building directly on Hyper with more control.

**Note:** workerd is Cloudflare's JS/WASM runtime (different product, not relevant here).

### Recommended Zero-Copy Crates

| Crate | Value | Use Case |
|-------|-------|----------|
| **zerocopy** (Google) | HIGH | Zero-copy struct parsing for IPC headers |
| **bytes** (via Hyper) | ESSENTIAL | Reference-counted buffers, already included |
| **ipmpsc** | EVALUATE | Shared memory ring buffer, claims 10x vs ipc_channel |
| **tokio-uring** | FUTURE | Linux io_uring for kernel-bypass I/O |

### zerocopy Integration Example

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

### io_uring Roadmap (Linux 5.1+)

Future optimization for true kernel-bypass I/O:

```rust
// tokio-uring: DMA directly into ring buffer pages
let (result, buf) = stream.read(vec![0u8; 4096]).await;
```

This would be a feature-gated Linux-specific elite mode.

---

## References

- [Hyper HTTP/2 Documentation](https://hyper.rs/guides/1/server/hello-world/)
- [RFC 8441: Bootstrapping WebSockets with HTTP/2](https://datatracker.ietf.org/doc/html/rfc8441)
- [Cloud Run HTTP/2 Configuration](https://cloud.google.com/run/docs/configuring/http2)
- [Cloudflare Pingora Blog](https://blog.cloudflare.com/how-we-built-pingora-the-proxy-that-connects-cloudflare-to-the-internet/)
- [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite)
- [Betterproto2 Documentation](https://betterproto.github.io/python-betterproto2/getting-started/)
- [Google zerocopy crate](https://crates.io/crates/zerocopy)
- [ipmpsc - Shared Memory IPC](https://crates.io/crates/ipmpsc)
- [tokio-uring](https://github.com/tokio-rs/tokio-uring)
