# HTTP Ingress Protocol and Resource Limits

This reference documents required operational limits across HTTP protocols in `vertebra-http`.

---

## Wire Limits

1. **Request Body Limits**:
   - Fixed bodies exceeding `max_body_size` reject immediately with `413 Payload Too Large`.
   - Streaming chunked bodies enforce per-chunk bounds and running cumulative byte counters.

2. **gRPC Message Framing**:
   - The 5-byte header encodes 1 compression flag byte and 4 length bytes (big-endian `u32`).
   - Length is validated against `max_grpc_message_size` before memory allocation.
   - Every termination returns `grpc-status` and `grpc-message` in trailers.

3. **Realtime Buffer Limits**:
   - Server-Sent Events (SSE) and WebSocket channels enforce max pending subscriber buffers.
   - When subscriber buffers overflow, policy dictates eviction or dropped connection with close frame `1008 Policy Violation`.
