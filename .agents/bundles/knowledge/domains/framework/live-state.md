---
type: Concept
title: "Live State Synchronization"
description: "Server-authoritative LiveState rooms with 64-bit HybridLogicalClock LWW conflict resolution and MessagePack delta frames across Rust, Python, and TypeScript."
status: stable
tags:
  - domains
  - framework
  - live-state
  - realtime
  - msgpack
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-vertebra-core-src-protocol-mod-rs
    resource: src/rs/vertebra-core/src/protocol/mod.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/protocol/mod.rs)"
  - id: src-framework-live-py
    resource: src/py/vertebra/framework/live.py
    title: "Python implementation module (src/py/vertebra/framework/live.py)"
  - id: src-src-live-index-ts
    resource: src/ts/vertebra-client/src/live/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/live/index.ts)"
  - id: src-tests-live-test-ts
    resource: src/ts/vertebra-client/tests/live.test.ts
    title: "TypeScript client module (src/ts/vertebra-client/tests/live.test.ts)"
---

# Live State Synchronization

<!-- truth: start -->
- `LiveState` and `LiveStateRoom` synchronize server-authoritative state across Rust, Python, and `@vertebra/client/live` using 64-bit `HybridLogicalClock` (`48b wall_ms << 16 | 16b logical`) last-write-wins registers.
- State mutations emit compact positional MessagePack `StateDeltaFrame` (`array_like=True`) payloads keyed by deterministic FNV-1a state and field hashes.
<!-- truth: end -->

Vertebra's Live Event State protocol synchronizes reactive state objects across Rust (`vertebra_core::protocol`), Python (`vertebra.framework.live`), and TypeScript (`@vertebra/client/live`) using compact MessagePack array frames and 64-bit Hybrid Logical Clock (HLC) Last-Write-Wins (LWW) conflict resolution.

## 1. Cross-Language Wire Protocol (`protocol/mod.rs`, `framework/live.py`, `live/index.ts`)

| Concept | Rust (`vertebra-core`) | Python (`vertebra.framework.live`) | TypeScript (`@vertebra/client/live`) |
|---|---|---|---|
| Opcodes (`0x01..0x08`) | [`StateOpcode`](../../../../../src/rs/vertebra-core/src/protocol/mod.rs)[^src-vertebra-core-src-protocol-mod-rs] | [`StateOpcode`](../../../../../src/py/vertebra/framework/live.py)[^src-framework-live-py] | [`StateOpcode`](../../../../../src/ts/vertebra-client/src/live/index.ts)[^src-src-live-index-ts] (`FieldSet` .. `BatchPatch`) |
| 64-bit HLC (`48b wall_ms << 16 \| 16b logical`) | [`HybridLogicalClock`](../../../../../src/rs/vertebra-core/src/protocol/mod.rs) | [`HybridLogicalClock`](../../../../../src/py/vertebra/framework/live.py) | [`HybridLogicalClock`](../../../../../src/ts/vertebra-client/src/live/index.ts) |
| State & Field Hashing | `state_id` (FNV-1a 64), `field_id` (FNV-1a 32) | `LiveState.compute_state_id`, `LiveState.compute_field_id` | `fnv1aStateId` (`bigint`), `fnv1aFieldId` (`number`) |
| Delta Frame / Register | `StateDelta`, `LwwStateRegister`, `encode_delta`, `decode_delta` | [`StateDeltaFrame`](../../../../../src/py/vertebra/framework/live.py) (`array_like=True`) | `StateDeltaMessage<T>`, `OfflineDeltaQueue` |

## 2. Python `LiveState` & `LiveStateRoom` (`framework/live.py`)

- **[`LiveState`](../../../../../src/py/vertebra/framework/live.py)**: Subclasses compute `__vertebra_state_id__` (FNV-1a 64 of class name), `__vertebra_field_map__` (FNV-1a 32 of annotated attributes), and `__vertebra_actions__` (methods decorated with `@live_action`) in `__init_subclass__`. Setting or deleting a public attribute advances `_hlc.tick()` and appends a `FIELD_SET` or `FIELD_DELETE` `StateDeltaFrame` to `_pending_deltas`.
- **LWW Resolution & Actions**: `apply_delta(frame)` compares `frame.timestamp_ns` against `_field_timestamps[field_id]`, rejecting stale updates (`frame.timestamp_ns < current_ts`) and merging newer timestamps via `_hlc.receive()`. `dispatch_action(frame)` executes `@live_action` methods (sync or async) for `ACTION_CALL` (`0x06`) and returns an `ACTION_RESULT` (`0x07`) frame.
- **[`LiveStateRoom`](../../../../../src/py/vertebra/framework/live.py)**: Binds a `LiveState` instance to a pub/sub `channel`, draining encoded MessagePack frames (`encode_msgpack_bytes` / `decode_msgpack` via `StateDeltaFrame.encode` and `StateDeltaFrame.decode`) through `broadcast_pending()` and handling client `SYNC_REQUEST`, `ACTION_CALL`, and mutation frames via `handle_client_frame(raw_bytes)`.

## 3. TypeScript `LiveStateClient` (`@vertebra/client/live`)

- **[`LiveStateClient<TState>`](../../../../../src/ts/vertebra-client/src/live/index.ts)**: Tracks per-field HLC timestamps (`Map<number, bigint>`), applies optimistic local mutations via `mutate(key, value)` (queuing into [`OfflineDeltaQueue`](../../../../../src/ts/vertebra-client/src/live/index.ts) when disconnected and flushing on `setConnected(true)`), reconciles inbound server deltas in `applyServerDelta(delta)`, and notifies subscribers with immutable state snapshots.
- **Invariant**: FNV-1a 64-bit state IDs and 32-bit field IDs must remain byte-for-byte identical across Rust, Python, and TypeScript.

Provenance sources: [^src-tests-live-test-ts].

[^src-vertebra-core-src-protocol-mod-rs]: `src/rs/vertebra-core/src/protocol/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/mod.rs).
[^src-framework-live-py]: `src/py/vertebra/framework/live.py` — Python implementation module (src/py/vertebra/framework/live.py).
[^src-src-live-index-ts]: `src/ts/vertebra-client/src/live/index.ts` — TypeScript client module (src/ts/vertebra-client/src/live/index.ts).
[^src-tests-live-test-ts]: `src/ts/vertebra-client/tests/live.test.ts` — TypeScript client module (src/ts/vertebra-client/tests/live.test.ts).
