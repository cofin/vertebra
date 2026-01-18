# Vertebra Driver Protocol

This document defines the Bun-backed driver IPC protocol used between Python drivers and the Bun server.

## Message Transport

- Framing: 4-byte big-endian length prefix + MessagePack payload.
- Encoding: MessagePack via msgspec (Python) and @msgpack/msgpack (TypeScript).
- Transport: UDS on Unix, TCP/named pipe on Windows (configurable).

## Messages

### DriverRequest

Fields:
- `id`: Request identifier.
- `driver`: Driver name (postgres, mysql, sqlite, etc).
- `method`: Operation (connect, execute, fetch, close, etc).
- `params`: Operation-specific payload.

### DriverResponse

Fields:
- `id`: Request identifier.
- `ok`: Success flag.
- `result`: Operation result (rows, counts, etc).
- `error`: Optional error payload.

### StreamChunk

Fields:
- `id`: Request identifier.
- `index`: Chunk index (monotonic).
- `done`: Indicates end of stream.
- `data`: Optional binary payload.

### DriverError

Fields:
- `code`: Error code string (DB-API compatible).
- `message`: Human-readable message.
- `details`: Optional structured metadata.

## Payload Conventions

- Binary payloads are always `bytes` (Python) or `Uint8Array` (TypeScript).
- JSON payloads use UTF-8 encoded text with JSON semantics.
- UUIDs are serialized as canonical strings.

## Versioning

- Protocol versions are embedded by the client in driver `params` and validated by the server.
- Backward-incompatible changes require a new major protocol version.
