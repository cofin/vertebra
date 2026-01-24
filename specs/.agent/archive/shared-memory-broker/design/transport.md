# Shared Memory Transport Design

## Goals

- Zero-socket data plane between broker and clients.
- Predictable latency with bounded queues.
- Zero-copy for large result sets (Arrow IPC).

## Topology

Each client (or provider) owns a **dedicated pair** of ring buffers:

- `req_ring`: client -> broker (SPSC)
- `resp_ring`: broker -> client (SPSC)

The broker maintains a registry of active ring pairs and polls them for work.
This avoids MPMC complexity while preserving lock-free behavior.

## Shared Memory Layout

Each ring uses the `vertebra-core` layout:

- `ShmHeader` at the start of the mapping.
- Ring buffer slots (power-of-two count) after the header.
- Fixed slot sizes: 256 / 1024 / 4096 bytes.

Large payloads (Arrow buffers) are stored in **dedicated shared-memory regions**.
The ring payload carries a lightweight Arrow descriptor (`name`, `size`) so
clients can map the region directly and release it when done.

## Signaling

- `wait_on_address` / futex-based waits (via `vertebra-core` sync helpers).
- Sender wakes receiver on enqueue; receiver blocks on empty.

## Message Types

Control plane is MessagePack (msgspec in Python). Payload:

```
struct Request {
  u64 id;
  u32 kind;       // acquire, execute, release, stats, register
  u32 flags;      // bits: arrow, error, etc.
  u64 payload_len;
  u64 payload_ref; // inline offset or buffer index
}

struct Response {
  u64 id;
  u32 status;     // ok, error
  u32 flags;      // arrow, error
  u64 payload_len;
  u64 payload_ref;
}
```

## Result Encoding Rules

- **Small results**: MessagePack payloads (rows + metadata).
- **Large results**: Arrow IPC stream stored in a shared-memory region.

Threshold (initial):
- Arrow if payload >= 1MB OR rows >= 1,000.

## Arrow Lifecycle

- Broker allocates Arrow regions and tracks TTL.
- Clients call `arrow_release` once done; broker or provider cleans up.

## Provider Registration

Providers register by sending a `register_provider` request with:

- provider name
- driver capabilities
- ring pair handles

Broker routes requests by provider name and uses the provider's ring
pair for execution.

## Compatibility

- Python uses CFFI to read/write ring buffers.
- Bun uses `bun:ffi` to call the same C ABI.
- No sockets or TCP in the data plane.
