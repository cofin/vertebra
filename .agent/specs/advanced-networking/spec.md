# Spec: Advanced Networking

## Code Analysis Summary
**Current State:**
- `vertebra-server`: (Planned) Basic Hyper server.
- `litestar-vite`: Provides a reference for what we want to exceed. It handles dev server integration, asset serving, and HMR proxying.
- **Goal:** Native Rust implementation of advanced networking features to support modern web apps and microservices.

**Target Architecture:**
- **HTTP/2 & HTTP/3:** Enabled in `vertebra-server` using `hyper` and `h3` (or `quinn`).
- **gRPC:** Native support. Since we control the HTTP layer, we can route gRPC requests efficiently.
- **SSE:** Server-Sent Events for real-time updates (alternative to WebSockets).
- **Vite/Rspack Integration (Advanced):**
    - The Rust server should be aware of the frontend build manifest.
    - It should handle `ETag` generation and caching intelligently.
    - It should support "Early Hints" (103) for asset preloading.

## Requirements

### 1. HTTP/2 & HTTP/3
- **Hyper Configuration:** Enable `http2` feature.
- **H2C:** Support cleartext HTTP/2 (required for gRPC and some internal deployments).
- **QUIC/HTTP3:** Use `h3` crate to provide experimental HTTP/3 support.

### 2. gRPC Support
- **Transport:** The server should identify `application/grpc` content type.
- **Routing:** Route gRPC requests to specific Python handlers (via IPC).
- **Python Integration:** Use `betterproto` or `grpclib` in Python to handle the actual RPC logic, but the Rust server handles the framing and transport.

### 3. Server-Sent Events (SSE)
- **Endpoint:** Support `text/event-stream`.
- **Channel:** Allow Python workers to push events to a channel that the Rust server broadcasts to clients.

### 4. Advanced Frontend Integration
- **Manifest Parsing:** Read `manifest.json` from Vite/Rspack to serve correct assets.
- **Preloading:** Send `Link` headers or 103 Early Hints for critical CSS/JS.

## API Changes

### Rust (`vertebra-server`)
```rust
pub struct ServerConfig {
    pub http2: bool,
    pub http3: bool,
    pub grpc: bool,
}
```
