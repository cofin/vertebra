# HTTP/1.1, HTTP/2, HTTP/3 & gRPC Ingress

`vertebra-http` is the Rust HTTP server built on Tokio, Hyper, `rustls`, and `quinn`. It terminates connections, handles static files and offloaded edge middleware in Rust, and batches dynamic requests for Python workers.

## Request Batching

Crossing into Python or waking a worker once per HTTP request adds overhead under concurrency. `RequestBatcher` collects incoming requests over a few microseconds, or up to a configured batch limit, into one batch so a worker handles many requests per wake.

## Multi-Protocol Support

| Protocol | Implementation Details |
| --- | --- |
| **HTTP/1.1 & HTTP/2** | Hyper connection handlers with keep-alive, pipelining, and ALPN TLS negotiation via `rustls`. |
| **HTTP/3 (QUIC)** | Optional UDP/QUIC listener enabled with TLS, `VERTEBRA_HTTP3=1`, and the `http3` crate feature. Advertises `Alt-Svc` alongside TCP HTTP/2. |
| **gRPC Framing** | 5-byte length-prefixed frame parser and `grpc-status` trailer encoder wired to the Python gRPC service registry. |
| **WebSocket & SSE** | RFC 6455 WebSocket upgrade and frame handling plus Server-Sent Events streaming. |

## Rust-Direct Offload

Before forwarding a request to Python, `vertebra-http` handles common edge work in Rust without touching the Python GIL:

- **Health & Agent Discovery**: Serves `/health`, `/.well-known/agent-card.json`, and `/.well-known/mcp.json` directly from Rust memory.
- **CORS & Security Headers**: Answers `OPTIONS` preflight requests and attaches `Access-Control-Allow-*` headers in Rust.
- **Response Compression**: Compresses outbound bodies with `zstd`, Brotli, or `gzip` based on `Accept-Encoding`.
- **Static Files & Multipart**: Serves static assets with `ETag`, `Cache-Control`, and path-traversal checks, and parses `multipart/form-data` uploads in Rust.

## Backpressure & Circuit Breaking

`BackpressureController` in `vertebra-http` sheds excess load with `503 Service Unavailable` or `429 Too Many Requests` responses instead of letting queues grow without bound.

### Rejection Reasons

| Reason | HTTP Status | Condition |
| --- | --- | --- |
| `Saturated` | `503` | Active request slots are full and the waiting queue has reached `max_queue_depth`. |
| `QueueTimeout` | `503` | Request waited longer than `max_queue_time` for an active slot. |
| `CircuitOpen` | `429` | Recent error rate reached `failure_rate_threshold` after at least `min_samples` responses. |

### Circuit Breaker Lifecycle

![Circuit breaker lifecycle](../assets/diagrams/circuit-breaker-light.svg#only-light)
![Circuit breaker lifecycle](../assets/diagrams/circuit-breaker-dark.svg#only-dark)

- **`Closed`**: Normal operation. Errors are counted over a sliding window configured by `VERTEBRA_BP_CB_WINDOW_MS`.
- **`Open`**: Requests fail immediately with `429` without calling Python workers until the window elapses.
- **`HalfOpen`**: Allows probe requests through. Reaching `VERTEBRA_BP_CB_HALF_OPEN_PROBES` consecutive successes closes the circuit; any `5xx` error reopens it.

### Metrics Snapshot

Backpressure metrics expose atomic counters for accepted and rejected requests (`saturated`, `queue`, `circuit`, and `timeout`) alongside live gauges for in-flight requests, queue depth, and circuit state.
