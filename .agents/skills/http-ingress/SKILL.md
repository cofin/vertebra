---
name: http-ingress
description: Use when implementing or debugging HTTP/1.1, HTTP/2, HTTP/3, or gRPC ingress pipelines, dispatchers, radix routing, security middleware, or WebSocket/SSE/PubSub streams in vertebra-http.
---

<!-- truth: start -->
- **Crate**: `src/rs/vertebra-http` (`main.rs`, `server.rs`, `response.rs`, `routing.rs`, `batcher.rs`, `dispatcher/`, `http3.rs`, `grpc.rs`, `sse.rs`, `pubsub.rs`, `ws_mount.rs`, `cors.rs`, `compression.rs`, `assets.rs`, `payload.rs`, `multipart.rs`).
- **Payload Bounds**: Enforce `payload_limits`, `multipart_limits`, and `max_grpc_message_size` via `collect_bounded_body` before buffering into memory or SHM (`HTTP-01`).
- **Backpressure**: `BackpressureController` sheds excess load with `503 Service Unavailable` and `Retry-After`; priority routes bypass admission shedding (`HTTP-02`).
- **Protocol Framing**: Prior-knowledge h2c matches the exact 24-byte preface; gRPC validates the 5-byte length-prefixed header and `application/grpc` content type (`HTTP-04`, `HTTP-05`).
- **Router Parity**: `normalize_typed_path` and FNV-1a `handler_id` match Python route table compilation across all 7 HTTP methods (`HTTP-10`).
- **Verify**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,compression`
<!-- truth: end -->

# HTTP Ingress & Protocol Dispatch (`http-ingress`)

Implementation guide for the Rust Hyper/Tokio/Quinn ingress engine, `response::handle` pipeline, `RouteTable`, `RequestBatcher`, `ShmDispatcher`, offload modules, and realtime streams in `src/rs/vertebra-http`.

## Architecture & Domain References

- [Ingress and routing](../../bundles/knowledge/architecture/http/ingress-and-routing.md)
- [Batching dispatch and backpressure](../../bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md)
- [Protocol and offload modules](../../bundles/knowledge/architecture/http/protocol-and-offload-modules.md)
- [Server and dispatch](../../bundles/knowledge/domains/http/server-and-dispatch.md)
- [Logging, tracing, and GenAI telemetry](../../bundles/knowledge/domains/telemetry/logging-tracing-and-genai.md)
- Ingress protocol reference: [`references/ingress-and-protocols.md`](references/ingress-and-protocols.md)
- Review checklist & suppressions: [`review-http-ingress`](../review-http-ingress/SKILL.md)

## Core Implementation Rules

1. **Bounded Ingress & Adaptive Batching (`HTTP-01`, `HTTP-02`)**:
   - Reject oversized bodies (`413 Payload Too Large`) via `collect_bounded_body` (`payload.rs`) before allocating SHM slots, and use the zero-length fast path when `body.size_hint().exact() == Some(0)`.
   - Maintain 1:1 batcher-to-worker shard affinity in `RequestBatcher` (`batcher.rs`) and `ShmDispatcher` (`dispatcher/shm.rs`), releasing shard mutexes before `oneshot::send`.

2. **Header Sanitization & Security (`HTTP-06`, `HTTP-07`)**:
   - Strip untrusted inbound identity headers (`x-client-cert-*`, `x-forwarded-client-cert`) before injecting verified mTLS (`tls.rs` `WebPkiClientVerifier` + `extract_der_common_name`) or JWT claims.
   - Validate JWT algorithm headers against configured allowlists and reject `alg: none` or algorithm family mismatches.

3. **Radix Router & Offload Paths (`HTTP-08`, `HTTP-10`)**:
   - Keep `RouteTable` (`routing.rs`) path normalization (`normalize_typed_path` in `vertebra-core/src/router.rs`) and FNV-1a handler hashes identical between Rust and Python (`vertebra.framework._router`).
   - Register mounted sub-apps (`MOUNT_HANDLER_BASE | mount_idx`) across `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, and `HEAD`.
   - Canonicalize static asset offload paths (`assets.rs`) and verify the resolved path remains inside the configured root directory.

4. **Graceful Connection Draining (`HTTP-03`, `HTTP-09`)**:
   - Propagate `CancellationToken` shutdown across HTTP/1.1 keep-alive loops, HTTP/2 `GOAWAY` frames, HTTP/3 Quinn endpoints (`http3.rs`), and active WebSocket/SSE registries (`ws_registry.close_all`).

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,compression
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-http --all-targets --features tls,compression -- -D warnings
```
