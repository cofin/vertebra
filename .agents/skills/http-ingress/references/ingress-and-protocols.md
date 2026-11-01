# Ingress Pipelines, Protocol Framing, and Realtime Streams

## Protocol Multiplexing

- **HTTP/1.1 & h2c (`server.rs`, `h2_handler.rs`)**:
  - Inspect initial connection bytes against the 24-byte HTTP/2 connection preface (`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`) before falling back to HTTP/1.1 parsing.
  - Support HTTP/1.1 `Upgrade: h2c` and `Upgrade: websocket` handshakes with explicit header validation.
- **HTTP/3 (`h3_handler.rs`)**:
  - Quinn QUIC endpoint shares the same `Dispatcher` and `Router` pipeline as TCP ingress and advertises `Alt-Svc` headers when enabled.
- **gRPC (`grpc.rs`)**:
  - Frame format: 1-byte compression flag (`0` or `1`) + 4-byte big-endian message length.
  - Emit HTTP/2 trailing headers (`grpc-status`, `grpc-message`) using standard status codes (`0` OK, `8` RESOURCE_EXHAUSTED, `12` UNIMPLEMENTED, `13` INTERNAL).

## Realtime Pub/Sub (`ws.rs`, `sse.rs`, `webtransport.rs`)

- **WebSockets**: Enforce `max_frame_size` and `max_message_size`, respond to ping frames, and clean up topic subscriptions in `Drop`/disconnect paths.
- **SSE**: Emit `id:`, `event:`, `data:`, and `retry:` fields with `\n\n` framing and periodic keep-alive comments (`: keepalive\n\n`).
