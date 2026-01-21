# Vertebra Driver Protocol (Shared Memory)

This document defines the **shared-memory broker protocol** used between clients
(Python/Bun) and the central Bun broker. There are **no sockets or TCP** in the data plane.

## Message Transport

- Transport: shared memory ring buffers (SPSC per client).
- Encoding: MessagePack for control + small results (msgspec in Python).
- Large results: Arrow IPC stream buffers (zero-copy).

## Messages

### Request

Fields:
- `id`: Request identifier (u64).
- `kind`: Operation (acquire, execute, release, stats, register, begin, commit, rollback).
- `flags`: Bits for Arrow usage, error handling, etc.
- `payload_len`: Inline payload size.
- `payload_ref`: Inline offset or buffer index (for Arrow payloads).

### Response

Fields:
- `id`: Request identifier (u64).
- `status`: ok/error enum.
- `flags`: Arrow/error bits.
- `payload_len`: Inline payload size.
- `payload_ref`: Inline offset or buffer index.

## Payload Conventions

- Small payloads: MessagePack with msgspec buffer protocol.
- Large payloads: Arrow IPC stream in shared buffer pool.
- Binary payloads: `bytes` / `Uint8Array` when inline.

## Transaction Commands

The protocol supports explicit transaction management:

- `BEGIN`: Start a transaction on the current connection token.
- `COMMIT`: Commit the current transaction.
- `ROLLBACK`: Rollback the current transaction.
- `SAVEPOINT`: Create a savepoint (nested transactions).

## MCP Tool Invocation

The Model Context Protocol (MCP) uses the shared memory transport for zero-overhead tool calls.

### Tool Call Request
```json
{
  "kind": "mcp_call",
  "tool": "tool_name",
  "arguments": { "arg1": "val1" },
  "id": 123
}
```

### Tool Call Response
```json
{
  "id": 123,
  "status": "ok",
  "content": [ ... ]
}
```

## Versioning

- ABI version is embedded in shared-memory header.
- Backward-incompatible changes require a new ABI version.
