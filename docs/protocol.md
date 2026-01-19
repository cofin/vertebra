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
- `kind`: Operation (acquire, execute, release, stats, register).
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

## Versioning

- ABI version is embedded in shared-memory header.
- Backward-incompatible changes require a new ABI version.
