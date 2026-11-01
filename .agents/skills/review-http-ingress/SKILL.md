---
name: review-http-ingress
description: Use when reviewing HTTP/1.1, HTTP/2, HTTP/3, or gRPC ingress pipelines, dispatchers, connection lifecycles, and realtime transports in vertebra-http.
---

<!-- truth: start -->
- **Limits**: Request body, multipart, and streaming limits enforced before buffering (`HTTP-01`).
- **Queues**: Ingress rings shed with 503 + Retry-After on saturation; no unbounded channels (`HTTP-02`).
- **Protocols**: Exact connection preface matching for h2c and gRPC 5-byte framing (`HTTP-04`, `HTTP-05`).
- **Security**: Inbound client identity headers stripped before mTLS injection (`HTTP-06`, `HTTP-07`).
- **Router**: Normalization and FNV-1a handler IDs match Python router (`HTTP-10`).
- **Suppressions**: Respect `FP-HTTP-01` in the shared catalog.
<!-- truth: end -->

# HTTP Ingress Review (`review-http-ingress`)

## Scope

- Paths: `[areas.http-ingress]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `HTTP-01`..`HTTP-12` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [Ingress and routing](../../bundles/knowledge/architecture/http/ingress-and-routing.md)
- [Batching dispatch and backpressure](../../bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md)
- [Protocol and offload modules](../../bundles/knowledge/architecture/http/protocol-and-offload-modules.md)
- [Server and dispatch](../../bundles/knowledge/domains/http/server-and-dispatch.md)
- Protocol limits reference: [`references/protocol-limits.md`](references/protocol-limits.md)

## Review Process

1. **Payload and backpressure**: Verify all streams are bounded and full rings shed cleanly (`HTTP-01`, `HTTP-02`).
2. **Resource reclamation**: Confirm in-flight correlation slots are reclaimed on disconnect or crash (`HTTP-03`).
3. **Framing contracts**: Inspect h2c prior knowledge detection and gRPC flag/length trailers (`HTTP-04`, `HTTP-05`).
4. **Security and tokens**: Ensure constant-time secret comparison and untrusted header stripping (`HTTP-06`, `HTTP-07`, `HTTP-08`).
5. **Realtime channels**: Verify bounded buffers and heartbeats on SSE and WebSockets (`HTTP-11`).
6. **Feature gates**: Confirm compilation under isolated feature combinations (`HTTP-12`).

## Suppressions Index

- `FP-HTTP-01`: Matchit route path syntax

## Focused Commands

```bash
cargo test -p vertebra-http
cargo check -p vertebra-http --no-default-features --features tls,compression
```
