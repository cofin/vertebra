---
name: ipc-tiered-rings-and-batch-bridge
description: RingPair request/response channels, 4-tier TieredRingSet with VRES wire envelopes, and in-process BatchBridge slab transport.
sources:
  - src/rs/vertebra-core/src/transport/mod.rs
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/rs/vertebra-core/src/tiered_ring.rs
  - src/rs/vertebra-core/src/bridge.rs
  - src/rs/vertebra-core/src/buffer.rs
---

# Tiered Rings & In-Process Batch Bridge

Vertebra provides three transport abstractions atop its ring primitives: per-worker [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs), size-segregated [`TieredRingSet`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs), and the in-process [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs).

## 1. Per-Worker `RingPair` (`transport/ring_pair.rs`)

A [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) combines a request `MpmcRing` (Tokio ingress $\to$ Python worker) with a 4-tier response [`TieredRingSet`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs) (Python worker $\to$ Tokio ingress) plus two OS [`Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs) instances:

- Requests and responses use the compact `VREQ` / `VRES` binary wire framing in [`transport/mod.rs`](../../../../../src/rs/vertebra-core/src/transport/mod.rs) and [`tiered_ring.rs`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs).
- `RingPair::from_env` reconstructs the request ring, all 4 response rings, and both `Notifier` descriptors inside child processes using the `VERTEBRA_*_FD` environment variables.

## 2. 4-Tier Shared-Memory Response Ring Set (`tiered_ring.rs`)

[`TieredRingSet`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs) eliminates internal slot fragmentation across heterogeneous payloads by provisioning four [`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs) response tiers:

| Tier | Slot Size | Default Capacity | Typical Payload |
|---|---|---|---|
| Tier 0 (`tiny`) | `512 B` | `256` slots | Health checks, status codes, small JSON pings |
| Tier 1 (`small`) | `4 KiB` | `128` slots | Standard REST JSON responses |
| Tier 2 (`medium`) | `64 KiB` | `32` slots | Large JSON lists, SSR HTML fragments |
| Tier 3 (`large`) | `1 MiB` | `8` slots | Full-page SSR payloads, bulk exports |

- **Spillover Promotion**: `TieredRingSet::send_response` selects the smallest tier fitting the 20-byte `VRES` header plus headers and body. If that tier's ring is full, it promotes the write to the next larger tier and records a spillover counter.
- **20-Byte `VRES` Response Envelope**: Responses carry a 20-byte `VRES` header (`magic: "VRES"`, `correlation_id: u64`, `status: u16`, `flags: u16`, `headers_len: u32`) followed by compact headers and body bytes, written directly into the SHM slot via `SpscRing::try_send_with`.

## 3. In-Process `BatchBridge` (`bridge.rs`, `buffer.rs`)

In `Topology::Embedded`, [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs) bypasses POSIX shared memory using in-process arenas:

1. **`TransportArena` & `DataSlab`**: Tokio ingress threads push [`RequestSlot`](../../../../../src/rs/vertebra-core/src/bridge.rs) descriptors into a [`TransportArena`](../../../../../src/rs/vertebra-core/src/bridge.rs) whose strings and body bytes reside in a contiguous [`DataSlab`](../../../../../src/rs/vertebra-core/src/buffer.rs).
2. **`BatchSnapshot`**: When the Python worker calls `get_batch(timeout_ms)` in [`api.rs`](../../../../../src/rs/vertebra-py/src/api.rs), `BatchBridge::poll_batch` returns a [`BatchSnapshot`](../../../../../src/rs/vertebra-core/src/bridge.rs) (`Arc<DataSlab>` + `Vec<RequestSlot>`) wrapped by PyO3 [`RequestBatch`](../../../../../src/rs/vertebra-py/src/batch.rs).
3. **`BufferPool` Recycling**: Dropped `DataSlab` buffers return to [`BufferPool`](../../../../../src/rs/vertebra-core/src/buffer.rs) so steady-state request batches avoid heap allocation churn.
