# Tasks: Elite-Performance HTTP Layer (Rust Hyper) & Protocol Support

## Phase 1: Rust HTTP Server with H2C (Priority: CRITICAL)

### Backend Core

- [ ] 1. Add Hyper dependencies to vertebra-core Cargo.toml
  - Add `hyper = { version = "1.4", features = ["http1", "http2", "server"] }`
  - Add `hyper-util = { version = "0.1", features = ["tokio"] }`
  - Add `http-body-util = "0.1"`
  - Add `zerocopy = { version = "0.8", features = ["derive"] }`

- [ ] 2. Create `src/rs/vertebra-core/src/server/mod.rs` module
  - Implement `VertebraServer` struct with TcpListener + RingPool
  - Configure h2c mode via `http2::Builder::new().http2_only(true)`
  - Support both HTTP/1.1 and HTTP/2 cleartext
  - Integrate with existing Tokio runtime

- [ ] 3. Implement request handler with ring buffer integration
  - Create `src/rs/vertebra-core/src/server/handler.rs`
  - Stream request bodies as `Bytes` into ring buffers
  - Read response bodies from ring buffers
  - Handle backpressure when workers are slow

- [ ] 4. Add health check endpoint
  - Create `src/rs/vertebra-core/src/server/health.rs`
  - Implement `/health` and `/__ready__` endpoints
  - Support h2c health checks for Cloud Run

### Testing

- [ ] 5. Create unit tests for Rust HTTP server
  - Test h2c configuration with `hyper::Client::builder().http2_only(true)`
  - Test request/response flow through ring buffers
  - Test graceful shutdown

- [ ] 6. Create integration tests for H2C
  - Test with `curl --http2-prior-knowledge`
  - Test concurrent H2 streams
  - Test Cloud Run compatibility

---

## Phase 2: Zero-Copy Verification (Priority: CRITICAL)

### Backend Core

- [ ] 7. Implement zero-copy Bytes transfer
  - Ensure `Bytes` are moved, not cloned, into ring buffers
  - Use `zerocopy` crate for IPC header parsing
  - Audit hot path for any `memcpy` calls

- [ ] 8. Add IpcHeader with zerocopy derives
  - Create `#[derive(AsBytes, FromBytes)]` struct for message framing
  - Implement zero-copy header parsing from ring buffer

- [ ] 9. Implement backpressure handling
  - Ring buffer full → apply backpressure to HTTP connection
  - Use `http2::server::Builder::initial_stream_window_size()` for flow control

### Testing

- [ ] 10. Create zero-copy verification tests
  - Instrument `Bytes` pointer tracking
  - Verify no allocations in hot path
  - Benchmark latency: target p99 < 1ms

---

## Phase 3: WebSocket over HTTP/2 (Priority: HIGH)

### Backend Core

- [ ] 11. Enable RFC 8441 extended CONNECT protocol
  - Add `.enable_connect_protocol()` to http2::Builder
  - Handle CONNECT method with `:protocol = websocket`

- [ ] 12. Create `src/rs/vertebra-core/src/server/websocket.rs`
  - Add `tokio-tungstenite = "0.24"` dependency
  - Implement WebSocket upgrade handler
  - Bridge WebSocket frames to IPC ring buffers

- [ ] 13. Implement WebSocket-IPC bridge
  - Bidirectional frame forwarding (client ↔ worker)
  - Handle multiple concurrent WebSocket streams
  - Graceful close handling

### Testing

- [ ] 14. Create WebSocket over HTTP/2 tests
  - Test with h2-compatible WebSocket client
  - Test concurrent streams on single connection
  - Test message ordering and delivery

---

## Phase 4: gRPC Transport (Priority: HIGH)

### Backend Core

- [ ] 15. Add gRPC request detection
  - Detect `Content-Type: application/grpc`
  - Create `src/rs/vertebra-core/src/server/grpc.rs`

- [ ] 16. Implement gRPC frame parsing
  - Parse 5-byte gRPC header (compressed flag + length)
  - Route message bodies through ring buffers
  - Frame responses as gRPC

- [ ] 17. Add betterproto2 for Python stubs
  - Add `grpc = ["betterproto2[compiler,grpclib]>=0.1.0"]` to pyproject.toml
  - Create `src/schema/services/example.proto` with Greeter service
  - Add `make proto` target for code generation

- [ ] 18. Create gRPC server wrapper for Python
  - Implement `src/py/vertebra/rpc/server.py`
  - Support service registration
  - Coordinate with Rust server lifecycle

### Testing

- [ ] 19. Create gRPC integration tests
  - Test with `grpcurl -plaintext`
  - Test unary requests end-to-end
  - Test concurrent gRPC calls

---

## Phase 5: Python Bindings & CLI (Priority: HIGH)

### Backend Core

- [ ] 20. Create PyO3 bindings for VertebraServer
  - Create `src/rs/vertebra-py/src/server.rs`
  - Expose `VertebraServer` class with `serve()` and `shutdown()`
  - Handle Tokio runtime lifecycle

- [ ] 21. Create Python wrapper module
  - Create `src/py/vertebra/server/rust.py`
  - Wrap PyO3 bindings with Pythonic API
  - Handle configuration translation

- [ ] 22. Update CLI serve command
  - Add `--server [rust|bun]` option (default: rust for prod, bun for dev)
  - Add `--h2c` flag for explicit h2c mode
  - Maintain backward compatibility

- [ ] 23. Implement graceful shutdown
  - Connection draining with configurable timeout
  - Signal handling (SIGTERM, SIGINT)
  - Worker coordination

### Testing

- [ ] 24. Create CLI integration tests
  - Test `vertebra serve --server rust`
  - Test graceful shutdown behavior
  - Test configuration options

---

## Phase 6: Cloud Run Excellence (Priority: HIGH)

### Deployment Support

- [ ] 25. Create Cloud Run deployment examples
  - Create `examples/cloud-run/` directory
  - Add Dockerfile with h2c configuration
  - Add service.yaml for Knative

- [ ] 26. Create Terraform module
  - Create `examples/cloud-run/terraform/`
  - Configure `google_cloud_run_v2_service`
  - Set port name to `h2c`
  - Configure autoscaling

- [ ] 27. Optimize startup and health checks
  - Minimize cold start latency
  - Ensure health checks work over h2c
  - Document health check configuration

- [ ] 28. Performance benchmarking
  - Benchmark Rust server vs Bun baseline
  - Profile h2c overhead
  - Document performance characteristics

### Documentation

- [ ] 29. Create Cloud Run deployment guide
  - Full tutorial with step-by-step instructions
  - Terraform and YAML examples
  - Troubleshooting section

- [ ] 30. Create gRPC quick-start guide
  - Proto file creation
  - Code generation with betterproto2
  - Service implementation example

- [ ] 31. Update CLAUDE.md
  - Document new `--server rust` flag
  - Add Rust server architecture notes
  - Update development commands

---

## Phase 7: Runtime Adapter Interface (Priority: MEDIUM)

### Backend Core

- [ ] 32. Define RuntimeAdapter TypeScript interface
  - Create `src/ts/src/runtime/types.ts`
  - Define ServerHandle, ProcessHandle interfaces
  - Define crypto and filesystem abstractions

- [ ] 33. Implement BunRuntimeAdapter
  - Create `src/ts/src/runtime/bun.ts`
  - Wrap Bun-specific APIs
  - Keep Bun for: database drivers, S3, crypto, dev server

- [ ] 34. Create runtime detection module
  - Create `src/ts/src/runtime/detect.ts`
  - Check `VERTEBRA_RUNTIME` env var
  - Auto-detect based on available APIs

---

## Validation Checklist

### Functional
- [ ] `vertebra serve --server rust` starts
- [ ] `curl --http2-prior-knowledge` works
- [ ] WebSocket over HTTP/2 works (RFC 8441)
- [ ] gRPC service responds
- [ ] Cloud Run deployment succeeds
- [ ] `vertebra serve --server bun` still works (dev mode)

### Performance
- [ ] Request latency p50 < 500µs
- [ ] Request latency p99 < 1ms
- [ ] Throughput > 100k req/s on 4-core
- [ ] No `memcpy` in hot path (zero-copy verified)
- [ ] gRPC unary p99 < 2ms

### Documentation
- [ ] Cloud Run guide complete
- [ ] gRPC quick-start complete
- [ ] Examples work as documented
- [ ] CLAUDE.md updated
