# Ingress Pipelines, Protocol Framing, and Realtime Streams

## Protocol Multiplexing

- **HTTP/1.1 & h2c (`server.rs`, `h2c.rs`)**:
  - Inspect initial connection bytes against the 24-byte HTTP/2 connection preface (`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`) before falling back to HTTP/1.1 parsing.
  - Support HTTP/1.1 `Upgrade: h2c` and `Upgrade: websocket` handshakes with explicit header validation.
- **HTTP/3 (`http3.rs`, `--features http3`)**:
  - Quinn QUIC endpoint shares the same `Dispatcher` and `Router` pipeline as TCP ingress and advertises `Alt-Svc` headers when enabled.
- **gRPC (`grpc.rs`)**:
  - Frame format: 1-byte compression flag (`0` or `1`) + 4-byte big-endian message length.
  - Emit HTTP/2 trailing headers (`grpc-status`, `grpc-message`) using standard status codes (`0` OK, `8` RESOURCE_EXHAUSTED, `12` UNIMPLEMENTED, `13` INTERNAL).

## Realtime Pub/Sub & MCP Bridge (`ws_mount.rs`, `sse.rs`, `pubsub.rs`, `mcp_bridge.rs`)

- **WebSockets (`ws_mount.rs`)**: Enforce `max_frame_size` and `max_message_size`, respond to ping frames, and clean up topic subscriptions in `Drop`/disconnect paths.
- **SSE & PubSub (`sse.rs`, `pubsub.rs`)**: Emit `id:`, `event:`, `data:`, and `retry:` fields with `\n\n` framing, bounded per-subscriber buffers, and periodic keep-alive comments (`: keepalive\n\n`).
- **MCP Bridge (`mcp_bridge.rs`)**: Serve `/.well-known/mcp` discovery metadata and stream MCP JSON-RPC frames over bounded SSE/HTTP ingress channels.
