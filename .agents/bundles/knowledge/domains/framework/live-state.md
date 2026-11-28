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
  - by: agent:flow-executor
    at: "2026-11-17T05:50:00Z"
    commit: 5a833832
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
  - id: src-live-counter-example
    resource: docs/examples/live_state_and_ssr.py
    title: "Native HTTP live-state counter example"
  - id: src-tests-live-test-ts
    resource: src/ts/vertebra-client/tests/live.test.ts
    title: "TypeScript client module (src/ts/vertebra-client/tests/live.test.ts)"
---

# Live State Synchronization

<!-- truth: start -->
- `LiveState` and `LiveStateRoom` synchronize server-authoritative state across Rust, Python, and `@vertebra/client/live` using 64-bit `HybridLogicalClock` (`48b wall_ms << 16 | 16b logical`) last-write-wins registers.
- `LiveState.__init_subclass__` walks `reversed(cls.__mro__)` so subclasses inherit parent `__vertebra_field_map__` and `__vertebra_actions__` definitions.
- `StateDeltaFrame` encodes compact positional MessagePack payloads keyed by deterministic FNV-1a hashes, and `StateDeltaFrame.decode` validates per-`StateOpcode` arity (`_OPCODE_ARITY`) and integer fields, raising `ValueError` on malformed frames.
- Applications supply the transport and decoding. `LiveStateRoom.broadcast_pending()` invokes its optional async publisher with encoded frames, and `LiveStateClient.applyServerDelta()` accepts decoded messages with 64-bit fields represented as `bigint`.
<!-- truth: end -->

Vertebra's Live Event State protocol synchronizes reactive state objects across Rust (`vertebra_core::protocol`), Python (`vertebra.framework.live`), and TypeScript (`@vertebra/client/live`) using compact MessagePack array frames and 64-bit Hybrid Logical Clock (HLC) Last-Write-Wins (LWW) conflict resolution.

## 1. Cross-Language Wire Protocol (`protocol/mod.rs`, `framework/live.py`, `live/index.ts`)

| Concept | Rust (`vertebra-core`) | Python (`vertebra.framework.live`) | TypeScript (`@vertebra/client/live`) |
|---|---|---|---|
| Opcodes (`0x01..0x08`) | [`StateOpcode`](../../../../../src/rs/vertebra-core/src/protocol/mod.rs)[^src-vertebra-core-src-protocol-mod-rs] | [`StateOpcode`](../../../../../src/py/vertebra/framework/live.py)[^src-framework-live-py] | [`StateOpcode`](../../../../../src/ts/vertebra-client/src/live/index.ts)[^src-src-live-index-ts] (`FieldSet` .. `BatchPatch`) |
| 64-bit HLC (`48b wall_ms << 16 \| 16b logical`) | [`HybridLogicalClock`](../../../../../src/rs/vertebra-core/src/protocol/mod.rs) | [`HybridLogicalClock`](../../../../../src/py/vertebra/framework/live.py) | [`HybridLogicalClock`](../../../../../src/ts/vertebra-client/src/live/index.ts) |
| State & Field Hashing | `state_id` (FNV-1a 64), `field_id` (FNV-1a 32) | `LiveState.compute_state_id`, `LiveState.compute_field_id` | `fnv1aStateId` (`bigint`), `fnv1aFieldId` (`number`) |
| Delta Frame / Register | `StateDelta`, `LwwStateRegister`, `encode_delta`, `decode_delta` | [`StateDeltaFrame`](../../../../../src/py/vertebra/framework/live.py) (`array_like=True`, `_OPCODE_ARITY` checked on `decode`) | `StateDeltaMessage<T>`, `OfflineDeltaQueue` |

## 2. Python `LiveState` & `LiveStateRoom` (`framework/live.py`)

- **[`LiveState`](../../../../../src/py/vertebra/framework/live.py)**: `__init_subclass__` computes `__vertebra_state_id__` (FNV-1a 64 of class name) and walks `reversed(cls.__mro__)` (skipping `LiveState` and non-subclasses) to accumulate inherited and subclass `__vertebra_field_map__` (FNV-1a 32 of public annotated attributes) and `__vertebra_actions__` (`@live_action` methods). Setting or deleting a public attribute advances `_hlc.tick()` and appends a `FIELD_SET` or `FIELD_DELETE` `StateDeltaFrame` to `_pending_deltas`.
- **Wire Arity & LWW Resolution**: `StateDeltaFrame.decode(data)` validates MessagePack decoding, checks exact per-`StateOpcode` element counts against `_OPCODE_ARITY`, and enforces integer types via `_require_wire_int`, raising `ValueError` on truncated or mismatched frames. `apply_delta(frame)` compares `frame.timestamp_ns` against `_field_timestamps[field_id]`, rejecting stale updates (`frame.timestamp_ns < current_ts`) and merging newer timestamps via `_hlc.receive()`. `dispatch_action(frame)` executes `@live_action` methods (sync or async) for `ACTION_CALL` (`0x06`) and returns an `ACTION_RESULT` (`0x07`) frame.
- **[`LiveStateRoom`](../../../../../src/py/vertebra/framework/live.py)**: Binds a `LiveState` instance to a pub/sub `channel`, draining encoded MessagePack frames (`encode_msgpack_bytes` / `decode_msgpack` via `StateDeltaFrame.encode` and `StateDeltaFrame.decode`) through `broadcast_pending()` and handling client `SYNC_REQUEST`, `ACTION_CALL`, and mutation frames via `handle_client_frame(raw_bytes)`.

## 3. TypeScript `LiveStateClient` (`@vertebra/client/live`)

- **[`LiveStateClient<TState>`](../../../../../src/ts/vertebra-client/src/live/index.ts)**: Tracks per-field HLC timestamps (`Map<number, bigint>`), applies optimistic local mutations via `mutate(key, value)` (queuing into [`OfflineDeltaQueue`](../../../../../src/ts/vertebra-client/src/live/index.ts) when disconnected and flushing on `setConnected(true)`), reconciles inbound server deltas in `applyServerDelta(delta)`, and notifies subscribers with immutable state snapshots.
- **Invariant**: FNV-1a 64-bit state IDs and 32-bit field IDs must remain byte-for-byte identical across Rust, Python, and TypeScript.

## 4. Native HTTP Counter

The [counter example](../../../../../docs/examples/live_state_and_ssr.py)[^src-live-counter-example] serves an HTML page and a native SSE response. Each subscription owns a counter and a room. The stream sends the initial state, drains each mutation with `broadcast_pending()`, and projects each change into a JSON snapshot. The page closes its `EventSource` when it receives the completion event.

Provenance sources: [^src-tests-live-test-ts].

[^src-vertebra-core-src-protocol-mod-rs]: `src/rs/vertebra-core/src/protocol/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/mod.rs).
[^src-framework-live-py]: `src/py/vertebra/framework/live.py` — Python implementation module (src/py/vertebra/framework/live.py).
[^src-src-live-index-ts]: `src/ts/vertebra-client/src/live/index.ts` — TypeScript client module (src/ts/vertebra-client/src/live/index.ts).
[^src-live-counter-example]: `docs/examples/live_state_and_ssr.py` serves the native HTTP live-state example.
[^src-tests-live-test-ts]: `src/ts/vertebra-client/tests/live.test.ts` — TypeScript client module (src/ts/vertebra-client/tests/live.test.ts).
