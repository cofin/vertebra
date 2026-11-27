---
type: Task
id: "runtime-perf-remediation:ch02:t02"
chapter_id: ch02-embedded-bridge-and-batching
title: Zero-copy PathAndQuery in InProcessRequest and BatchResponseSink on BatchBridge
description: Store http::uri::PathAndQuery directly in InProcessRequest without Bytes::copy_from_slice while preserving InProcessRequest::new and BatchSnapshot APIs, and add BatchResponseSink to BatchBridge so submit_responses invokes the batch sink once.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md
  - src/rs/vertebra-core/src/transport/mod.rs
  - src/rs/vertebra-core/src/bridge.rs
  - src/rs/vertebra-core/Cargo.toml
files:
  - src/rs/vertebra-core/src/transport/mod.rs
  - src/rs/vertebra-core/src/bridge.rs
  - src/rs/vertebra-core/Cargo.toml
tests:
  - src/rs/vertebra-core/src/transport/mod.rs
  - src/rs/vertebra-core/src/bridge.rs
estimated_loc: 160
plan_revision: 2
plan_commit: "86527cf1"
commit: "3024aee2"
sources:
  - id: src-core-transport
    resource: src/rs/vertebra-core/src/transport/mod.rs::InProcessRequest
    title: Zero-copy in-process request container and BatchSnapshot accessors
  - id: src-core-bridge
    resource: src/rs/vertebra-core/src/bridge.rs::BatchBridge
    title: In-process BatchBridge queue and direct response sink dispatch
tags:
  - vertebra-core
  - batch-bridge
  - zero-copy
  - transport
---

# Zero-copy PathAndQuery in InProcessRequest and BatchResponseSink on BatchBridge

## Objective & Scope Boundary

Eliminate per-request `path` and `query` `Bytes::copy_from_slice` allocations in `InProcessRequest` and add a batch-level `BatchResponseSink` callback on `BatchBridge` [^src-core-transport] [^src-core-bridge].

Stay strictly inside `vertebra-core` (`transport/mod.rs`, `bridge.rs`, and `Cargo.toml` only if `http` dependency is not yet declared).

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md` (Section 2.2 contracts)
- `src/rs/vertebra-core/src/transport/mod.rs::InProcessRequest` and `BatchSnapshot`
- `src/rs/vertebra-core/src/bridge.rs::BatchBridge`, `set_response_sink`, `try_dispatch_to_sink_batch`, and `submit_responses`
- `src/rs/vertebra-core/Cargo.toml` (`[dependencies]` section for `http`)

## Contracts & Edge Cases

Implement the `InProcessRequest` and `BatchBridge` contracts defined in `chapters/ch02-embedded-bridge-and-batching/spec.md`:

- Ensure `http` is in `src/rs/vertebra-core/Cargo.toml` (`http.workspace = true` if missing).
- Back `InProcessRequest` path and query with `InProcessPathQuery::{Uri(http::uri::PathAndQuery), Bytes { path, query }}`:
  - Keep `pub node: TransportNode`, `pub headers_raw`, `pub body`, `pub path_params_raw`, and exact signature of `InProcessRequest::new(...)` so all existing callers and tests compile unchanged.
  - Add `InProcessRequest::from_path_and_query(...)` storing `PathAndQuery` directly and populating `node.path_len` and `node.query_len` (`0` when `query()` is `None` or `Some("")`).
  - Update `BatchSnapshot::{path_bytes, path, query_bytes, query, serialized_size, serialize_into}` to read through `InProcessRequest::{path_bytes, path_str, query_bytes, query_str}` (returning `None` for empty query `/?`, skipping UTF-8 validation on `Uri` variant).
- Add `pub type BatchResponseSink = Arc<dyn Fn(Vec<BridgeResponse>) + Send + Sync>;` to `bridge.rs` with `fast_batch_sink: OnceLock<BatchResponseSink>`, `has_override_batch_sink: AtomicBool`, and `override_batch_sink: RwLock<Option<BatchResponseSink>>`.
- Add `set_batch_response_sink` and `clear_batch_response_sink` mirroring `set_response_sink` / `clear_response_sink`.
- Update `try_dispatch_to_sink_batch`:
  - Dispatch the entire `Vec<BridgeResponse>` to the active `BatchResponseSink` in a single call.
  - On the override `RwLock` path, clone the `Arc<BatchResponseSink>` and drop the read guard before calling the sink so re-entrant replacement cannot deadlock.
  - When no batch sink is active, fall back to the existing `ResponseSink` loop, and if neither is registered, return `Err(responses)` so `submit_responses` sends via `resp_tx`.
- Update `make_bridge_with_codec` and `batch_writer_auto_seals_on_slab_or_arena_full` test constructors in `bridge.rs`.

## Test Matrix

| Case | Scenario | Expected Result |
| :--- | :--- | :--- |
| `from_path_and_query` with path + query | `"/api/items?page=2&limit=10".parse()` | `snap.path(0) == "/api/items"`, `snap.query(0) == Some("page=2&limit=10")`, wire roundtrip matches |
| `from_path_and_query` with empty query | `"/?".parse()` | `snap.path(0) == "/"`, `snap.query(0) == None`, `snap.query_bytes(0) == None` |
| Backward-compatible `InProcessRequest::new` | Existing `Bytes` path/query caller | Identical behavior across accessors and `serialize_into` |
| `BatchResponseSink` single invocation | `submit_responses` with 4 items | Batch sink called once with len 4; single `ResponseSink` not called |
| `submit_response` single item | Both sinks registered | `submit_response` invokes `ResponseSink`, not `BatchResponseSink` |
| Override & clear fallback | Replace batch sink, re-enter inside callback, then `clear_batch_response_sink()` | New sink receives batch without deadlock; after clear, falls back to `ResponseSink` then `resp_tx` |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -- bridge transport
```

## Acceptance Criteria

- `InProcessRequest::from_path_and_query` moves `http::uri::PathAndQuery` with zero slice copies and preserves `BatchSnapshot` accessors and wire serialization.
- Existing `InProcessRequest::new` callers and tests pass unchanged.
- `BatchBridge::submit_responses` invokes `BatchResponseSink` once per batch on the lock-free `OnceLock` fast path and handles override/clear fallback cleanly.
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -- bridge transport` passes.

[^src-core-transport]: `src/rs/vertebra-core/src/transport/mod.rs::InProcessRequest` — Zero-copy in-process request container and BatchSnapshot accessors.
[^src-core-bridge]: `src/rs/vertebra-core/src/bridge.rs::BatchBridge` — In-process BatchBridge queue and direct response sink dispatch.
