---
type: Spec
flow_id: http-ingress-consolidation
title: "Chapter 3: HTTP ingress, dispatch, and realtime consolidation"
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T03:30:00Z
updated_at: 2026-10-22T03:30:00Z
description: "Consolidate vertebra-http into deep ingress, batcher, dispatcher, backpressure, server, realtime, and worker modules while fixing correlation truncation, dead SHM streaming, and unwired ingress interceptors."
tags: [refactor, rust, http, correctness]
parent_prd: vertebra-design-quality
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 3: HTTP ingress, dispatch, and realtime consolidation

**Flow ID:** `http-ingress-consolidation`

## Specification

### Parent Context

This chapter is Chapter 3 of the [`vertebra-design-quality`](../vertebra-design-quality/spec.md) master roadmap. It executes on branch `refactor/http-ingress-consolidation` branched from the tip of Chapter 2 (`core-ipc-consolidation`, which delivers `vertebra_core::protocol::flags::{FrameFlags, RequestFlags}`, `vertebra_core::transport::headers::RawHeaders`, `ResponseSlotView`, and `vertebra_core::util::crypto`). It owns the Rust HTTP server crate (`src/rs/vertebra-http/**`) for roadmap requirements:

- **R4 (Correctness defects fixed test-first):** `CorrelationId` 32-bit generation preservation (`HTTP-19`, `OKF-01`) and live SHM multi-frame streaming through `InFlightRequests` (`HTTP-06`, `HTTP-10`).
- **R5 (Dead code and duplicate removal):** Delete `mcp_bridge.rs` (`HTTP-01`, `MCP-03`), `ResponseCollector` and `PendingResponseBody` (`HTTP-39`), `PendingRequests` (`HTTP-06`), `DirectRequest`/`DirectBatch` (`HTTP-17`), four duplicate body stream wrappers (`HTTP-08`), duplicate `FLAG_*` constants (`HTTP-07`), dead `ServerConfig` fields (`HTTP-24`), and unused Cargo dependencies (`HTTP-38`).
- **R6 (Honest capabilities and wired modules):** Wire the orphan ingress modules into `ingress::Pipeline` (`HTTP-02`, `HTTP-03`, `HTTP-04`, `HTTP-05`, `HTTP-13`, `HTTP-33`, `HTTP-34`, `HTTP-42`, `HTTP-43`, `MCP-07`), wire worker crash recovery and supervision (`HTTP-23`, `OKF-07`), and start the HTTP/3 endpoint when enabled (`HTTP-28`).
- **R7 (Deep module reorganization):** Reorganize the 39 flat files in `src/rs/vertebra-http/src/` into `body.rs`, `batcher.rs`, `routing.rs`, `server/`, `ingress/`, `dispatcher/`, `backpressure/`, `realtime/`, and `workers/`.
- **R9 (Hot-path performance):** Method-indexed router (`HTTP-29`), zero-copy `PathAndQuery` (`HTTP-30`), per-connection batcher shard (`HTTP-31`), shard-grouped correlation locking (`HTTP-20`) with reusable `ShmBatchScratch` (`HTTP-21`), single-parse `HeaderMap` construction (`HTTP-10`), and lock-free bucketed circuit breaker counters (`HTTP-11`, `HTTP-12`).
- **R10 (MCP/A2A alignment):** Delete the Rust `mcp_bridge.rs` stub (`HTTP-01`, `MCP-03`), source the MCP protocol version header from configuration (`MCP-02`), and evaluate `WellKnownDiscoveryCache` against the Python route benchmark under Decision D3 (`HTTP-42`, `MCP-07`).

### Code Analysis Summary

All 39 source files and 18 integration test files under `src/rs/vertebra-http/` (10,025 lines total at commit `edceea6`) were inspected:

- **`src/rs/vertebra-http/src/response.rs` (317 lines):** Mixes top-level `handle` (`:75-206`), `ReceiverFrameStream` (`:33-50`), `ResponseBody` (`:57`), header encoders (`:255-271`, `:315-317`), `RejectionReason` status mapping (`:208-223`), and health JSON (`:225-253`). Constructs `HealthEndpoint::default()` per request (`:92-97`), copies `path.to_owned()` and `query.map(str::to_owned)` (`:163-164`), formats `Alt-Svc` per response (`:187-193`), and never invokes `access_log`, `assets`, `compression`, `cors`, `discovery`, `grpc`, `multipart`, `payload`, `profile`, `pubsub`, or `sse` (`HTTP-02`, `HTTP-05`, `HTTP-08`, `HTTP-13`, `HTTP-28`, `HTTP-30`, `HTTP-40`).
- **`src/rs/vertebra-http/src/collector.rs` (288 lines) & `policy.rs` (174 lines):** `PendingResponseBody` (`:20-41`) duplicates `ReceiverFrameStream`; `ResponseCollector` (`:45-128`) has no production callers (`HTTP-08`, `HTTP-39`). `BatchPolicy::Adaptive` (`policy.rs:16-105`) updates `ema_arrival_rate` (`:98`) that nothing reads, and `RequestBatcher::run` (`collector.rs:255-277`) drains immediately after `try_recv` without lingering (`HTTP-15`).
- **`src/rs/vertebra-http/src/router.rs` (107 lines) & `route_table.rs` (86 lines):** Hashes uppercased HTTP method strings in `HashMap<String, matchit::Router>` on every lookup (`router.rs:35-37,72-106`) (`HTTP-29`). `route_table.rs:70-85` `handler_id_reference_values` only asserts `!= 0` instead of checking shared FNV-1a constants (`HTTP-16`).
- **`src/rs/vertebra-http/src/dispatcher/{mod,types,direct,bridge,shm,tests}.rs` (1,007 lines), `correlation.rs` (332 lines), & `response_reader.rs` (295 lines):**
  - `shm.rs:176` packs `(internal_id << 8) | (shard_idx as u64)`, shifting off the top 8 bits of the 32-bit slab generation returned by `InFlightRequests::insert` (`correlation.rs:64`). When `generations[index]` wraps past `2^24`, `InFlightRequests::complete` (`correlation.rs:78`) permanently rejects completions for that slot (`HTTP-19`, `OKF-01`).
  - `Dispatcher::handle_raw_response_slot` (`dispatcher/mod.rs:229`, `shm.rs:248-343`) has zero production callers, and `PendingRequests` (`response_reader.rs:45-112`) is a dead `DashMap` used only by `tests/streaming_body_tests.rs` (`HTTP-06`).
  - `shm.rs:256-298` parses response slots three times and converts headers through three intermediate allocations (`HTTP-10`).
  - `Dispatcher` wraps `Inner` with `Option` fields and a `DashMap` allocated even in SHM mode (`dispatcher/mod.rs:47-129`, `HTTP-18`). `dispatch_shm` locks shard mutexes per request (`shm.rs:157`, `HTTP-20`) and allocates a `HashMap<(usize, u32), ShmDispatchBatch>` per batch (`shm.rs:86,179-181`, `HTTP-21`).
  - `ShmDispatcher::new` spawns an uncancelled sweeper task holding `Arc<Self>` (`shm.rs:58-68`), `main.rs:261` never cancels its `CancellationToken`, and `response_reader.rs:204-230` checks `is_cancelled()` without `tokio::select!` (`HTTP-22`).
- **`src/rs/vertebra-http/src/ring_factory.rs` (487 lines):** `WorkerLifecycle`, `cleanup_stale_shm`, `install_panic_cleanup_hook`, `RingPair::reclaim`, and `InFlightRequests::sweep_for_worker` (`correlation.rs:131-157`) have zero production callers (`HTTP-23`, `OKF-07`).
- **`src/rs/vertebra-http/src/backpressure.rs` (553 lines):** `record_success`/`record_failure` (`:397-439`) lock a global `std::sync::Mutex` and push to an unbounded `VecDeque<(Instant, bool)>` on every response (`HTTP-11`). Open-to-HalfOpen transition logic is duplicated (`:289-321`, `HTTP-12`), and stream watermarks (`:181-276`) are unwired (`HTTP-14`).
- **Orphan ingress modules (`cors.rs` 108, `compression.rs` 270, `discovery.rs` 134, `profile.rs` 101, `assets.rs` 511, `access_log.rs` 200, `payload.rs` 107, `multipart.rs` 154):** Unwired from `handle`; `compression.rs` has two parallel APIs (`HTTP-04`); `cors.rs` does not deserialize the Python offload spec or regex (`HTTP-03`); `discovery.rs` uses a `DashMap` and hardcoded MCP version (`HTTP-42`, `MCP-02`, `MCP-07`); `profile.rs` returns fake JSON (`HTTP-33`); `assets.rs` reads whole files into memory and stats sidecars per request (`HTTP-43`); `access_log.rs` buffers unbounded `Vec<String>` under a `Mutex` and emits invalid JSON via Rust `{:?}` (`HTTP-34`).
- **Connection protocols (`server.rs` 197, `h2c.rs` 223, `tls.rs` 408, `http3.rs` 307):** `server.rs:41-47` hits a global atomic per request (`HTTP-31`), calls `detect_cleartext_protocol` `peek` per connection and discards the result (`server.rs:148`, `HTTP-25`), duplicates plain and TLS connection serving (`server.rs:141-197`, `HTTP-26`), never starts `serve_http3_endpoint` (`http3.rs:125`, `HTTP-28`), and uses a hand-rolled SHA-256 and short-length DER byte scanner in `tls.rs:191-312` (`HTTP-32`).
- **Realtime (`sse.rs` 408, `pubsub.rs` 435, `grpc.rs` 392, `ws_mount.rs` 438, `hmr.rs` 296):** `PubSubBroker` and `SseHub` duplicate replay rings and fan out three ways (`HTTP-35`), `grpc.rs:337-371` spawns a task to iterate an already-materialized `Vec<GrpcFrame>` (`HTTP-36`), `sse.rs` allocates per line and header sanitize (`HTTP-44`), and `hmr.rs:153-184` re-implements the WebSocket 101 handshake instead of using `ws_mount` (`HTTP-45`).
- **Config, main, Cargo, and MCP bridge (`config.rs` 260, `main.rs` 334, `mcp_bridge.rs` 78, `Cargo.toml`):** `mcp_bridge.rs` is a blocking stdio stub with an unread `endpoint_url` (`HTTP-01`, `MCP-03`), `config.rs` has 10 unread fields (`HTTP-24`), `main.rs` duplicates `AppSpec` loading and initializes `BatchBridge` in SHM mode (`HTTP-37`), and `Cargo.toml` pulls unused dependencies and un-gated compression crates (`HTTP-38`).

#### Findings from `http.md` not carried in this chapter

- **`HTTP-09` (Request header encoder copies core codec and wraps lengths):** Assigned to and resolved in Chapter 2 (`core-ipc-consolidation`) Task 2.5 alongside `CORE-42` (`RawHeaders` in `vertebra-core/src/transport/headers.rs`).
- **`HTTP-27` (HTTP knowledge docs disagree with code):** Assigned to Chapter 10 (`knowledge-resync`) Task 10.1 so documentation is updated once after the final module layout lands.
- **`HTTP-41` (Criterion benchmark target and new HTTP benchmark cases):** The `make rs-bench` target is assigned to Chapter 1 (`review-infrastructure`) Task 1.6, and the new `shm_dispatch/roundtrip_1000`, `response_decode/slot_20_headers`, and `compression` Criterion benchmarks are assigned to Chapter 8 (`hot-path-performance`) Task 8.1. (Task 3.2 deduplicates the existing `bridge_server` helper in `benches/server_throughput.rs` when moving `direct_worker.rs` and updating `BatchLimit`; Task 3.5 adds `backpressure/record_contended_8_threads`; Task 3.9 adds `realtime/publish_1k_subscribers`.)

### Relevant Patterns

- [Product and Engineering Guidelines](../../product/product-guidelines.md) — Clean-break domain naming without `Vertebra*` prefixes or aliases, zero-copy hot paths, single FNV-1a route identity across Rust and Python.
- [Lock-Free Shared Memory & Sharded Concurrency](../../knowledge/patterns/rust/lock-free-shm-and-sharding.md) — Shard-grouped locking, atomic state machines, and bounded ring/slab invariants.
- [HTTP/1.1 & HTTP/2 Ingress & Native Routing](../../knowledge/architecture/http/ingress-and-routing.md) and [Request Batching, Dispatch & Backpressure](../../knowledge/architecture/http/batching-dispatch-and-backpressure.md) — Target architecture for ingress, batching, correlation, and backpressure.
- [SHM Worker Supervision & Crash Recovery](../../knowledge/architecture/runtime/shm-supervision-and-recovery.md) — `RingFactory`, `WorkerLifecycle`, and `/dev/shm` cleanup lifecycle.
- [Project Workflow](../../knowledge/workflow.md) — Verification strategies, `make rs-test`, `make py-test`, and Criterion performance gates.

### Requirements

| ID | Requirement |
| --- | --- |
| C3-R1 | Unify HTTP response bodies on `body::FrameReceiver` and `body::ResponseBody`, deduplicate `FrameFlags` imports from `vertebra-core`, and preserve all 32 bits of the slab generation counter in `CorrelationId(u64)`. |
| C3-R2 | Replace string-hashed method routing with a method-tag-indexed `Router` in `routing.rs`, enforce exact cross-language FNV-1a `handler_id` reference values, replace `collector.rs` and `policy.rs` with `batcher.rs` (`BatchLimit`, zero-copy `PathAndQuery`), and move `direct_worker.rs` to `benches/support/`. |
| C3-R3 | Refactor `Dispatcher` to `DispatchBackend`, lock correlation shards once per shard per batch/drain with reusable `ShmBatchScratch`, fix sweeper and reader shutdown lifecycles with `Weak` references and `CancellationToken` `select!`, and wire `workers::Supervisor` (`WorkerLifecycle`, `sweep_for_worker`, `RingPair::reclaim`, `cleanup_stale_shm`, panic cleanup hook). |
| C3-R4 | Route SHM streaming frames (`FrameFlags::STREAMING`, `FINAL`, `ERROR`, `TRAILERS`) through `InFlightRequests` (`Slot::Unary` / `Slot::Stream`), delete `PendingRequests`, and build `HeaderMap` once from `ResponseSlotView`. |
| C3-R5 | Split `backpressure.rs` into `backpressure/{mod,circuit,streams}.rs`, replace the global `Mutex<VecDeque>` circuit window with lock-free bucketed atomic counters, deduplicate state transitions with `#[repr(u8)] CircuitState`, and wire `StreamLease` into `FrameReceiver` producers. |
| C3-R6 | Split `response.rs` into `ingress/{mod,pipeline,health}.rs` and `body.rs`, hold one `Arc<HealthEndpoint>` in `ServerState`, assign collector shards per connection, unify `ResponseCompressor`, and wire `CorsPolicy` consuming the JSON offload spec from Python. |
| C3-R7 | Wire ingress endpoints (`discovery.rs`, `profile.rs`, `assets.rs`, `access_log.rs`) into `ingress::Pipeline`: benchmark `WellKnownDiscoveryCache` against the Python route per Decision D3 (keeping only if faster), make `/debug/pprof` explicit or feature-backed per the stub policy, stream static assets with cached startup metadata, and bound `access_log` with valid JSON escaping over `AsyncLogCore`. |
| C3-R8 | Consolidate connection protocols under `server/{mod,tls,tls_identity,h2c,http3}.rs`: share one `AutoBuilder` and generic `serve_connection`, remove the redundant cleartext `peek`, start `serve_http3_endpoint` when HTTP/3 is enabled, and replace hand-rolled SHA-256 and short-length DER scanning with `ring::digest` and TLV parsing. |
| C3-R9 | Consolidate realtime modules under `realtime/{mod,topic_hub,sse,pubsub,ws,grpc,hmr}.rs`: merge `SseHub` and `PubSubBroker` onto a single `TopicHub` and `BroadcastRing`, remove per-event allocations in `sse.rs`, stream gRPC frames without spawning a task, and route HMR upgrades through `ws` handshake and `WsConnectionRegistry`. |
| C3-R10 | Delete `mcp_bridge.rs` (`HTTP-01`, `MCP-03`), wire or remove unread `ServerConfig` fields (`HTTP-24`), deduplicate `AppSpec` loading and conditional `BatchBridge` init in `main.rs` (`HTTP-37`), and trim `vertebra-http/Cargo.toml` dependencies and features (`HTTP-38`). |

### Target Layout

```text
src/rs/vertebra-http/
  Cargo.toml                 trimmed dependencies and explicit hyper/compression/server features (HTTP-38)
  benches/
    server_throughput.rs     deduplicated bridge_server helper + backpressure and realtime bench groups (HTTP-11, HTTP-17, HTTP-35)
    support/
      mod.rs                 DirectWorkerPool moved from src/direct_worker.rs for benchmark/test use (HTTP-17)
  src/
    lib.rs                   clean module exports
    main.rs                  ServerConfig::from_env -> AppSpec -> workers::Supervisor / Server::run (HTTP-22, HTTP-23, HTTP-37)
    config.rs                live ServerConfig fields + env/JSON parsing for CORS, compression, discovery, mTLS (HTTP-24, MCP-02)
    body.rs                  ResponseBody, FrameReceiver (with optional StreamLease), PendingResponse, build_response (HTTP-08, HTTP-10, HTTP-14, HTTP-40)
    batcher.rs               RequestBatcher, PendingRequest (PathAndQuery), Batch, BatchLimit, RequestTags (HTTP-15, HTTP-30, HTTP-39)
    routing.rs               method-indexed Router ([Option<matchit::Router<HandlerId>>; 9] + ext), normalize_typed_path, RouteTable, handler_id (HTTP-16, HTTP-29)
    server/
      mod.rs                 accept loop, shared AutoBuilder, generic serve_connection, shard-per-connection (HTTP-26, HTTP-31)
      tls.rs                 TlsAcceptor (HTTP-26, HTTP-32)
      tls_identity.rs        TLV DER walker + ring SHA-256 + trusted proxy normalization wired into serve_connection (HTTP-32)
      h2c.rs                 h2c preface constants + Upgrade: h2c gated by enable_h2c_upgrade (HTTP-25)
      http3.rs               QUIC/HTTP/3 endpoint spawned when config.http3.enabled, precomputed Alt-Svc HeaderValue (HTTP-28)
    ingress/
      mod.rs                 handle(): pipeline.intercept -> route -> batcher dispatch -> pipeline.finish (HTTP-02, HTTP-40)
      pipeline.rs            ordered request interceptors and response filters built from ServerConfig (HTTP-02)
      health.rs              single HealthEndpoint in ServerState + backpressure snapshot JSON (HTTP-05, HTTP-40)
      cors.rs                CorsPolicy deserialized from offload JSON with precomputed HeaderValues and Regex (HTTP-03)
      compression.rs         single ResponseCompressor with q-value negotiation and buffer reuse (HTTP-04)
      discovery.rs           WellKnownDocs (fixed slots, config-driven paths & version header; kept only if faster per D3) (HTTP-42, MCP-02, MCP-07)
      profile.rs             ProfileEndpoint with real pprof/mimalloc or explicit 501 per stub policy, using vertebra_core::util::crypto::constant_time_eq (HTTP-33)
      assets.rs              startup metadata/sidecar index, ReaderStream into FrameReceiver for large files, Bytes cache for fingerprinted assets (HTTP-43)
      access_log.rs          valid JSON/CLF formatting into AsyncLogCore sink with %t UTC timestamp token (HTTP-34)
      payload.rs             bounded request body decompression wired into ingress body read (HTTP-02)
      multipart.rs           multipart parser wired with payload limits (HTTP-02)
    dispatcher/
      mod.rs                 Dispatcher { backend: DispatchBackend, backpressure } (HTTP-18)
      direct.rs              Direct backend sending Batch directly without DirectRequest copy (HTTP-17, HTTP-18)
      bridge.rs              Bridge backend (HTTP-18)
      shm.rs                 ShmDispatcher with ShmBatchScratch, shard-grouped locking, Weak sweeper, and ResponseSlotView handling (HTTP-06, HTTP-10, HTTP-20, HTTP-21, HTTP-22)
      shm_reader.rs          AsyncFd reader with FrameFlags peek, streaming frame routing, and CancellationToken select! (HTTP-06, HTTP-22, HTTP-39)
      correlation.rs         CorrelationId(u64) (32-bit gen, 24-bit slot, 8-bit shard) + InFlightRequests { Unary, Stream } (HTTP-06, HTTP-19, OKF-01)
      tests.rs               dispatcher unit tests
    backpressure/
      mod.rs                 BackpressureController, admission semaphore, RejectionReason::status/reason_phrase (HTTP-13)
      circuit.rs             #[repr(u8)] CircuitState, lock-free bucketed window [AtomicU64; 2 * BUCKETS] (HTTP-11, HTTP-12)
      streams.rs             StreamLease wired into FrameReceiver (HTTP-14)
    realtime/
      mod.rs                 realtime exports
      topic_hub.rs           TopicHub + BroadcastRing shared by SSE and pub/sub (HTTP-35)
      sse.rs                 SseEvent, SseHub over TopicHub, zero-alloc line iterator and Cow sanitize (HTTP-35, HTTP-44)
      pubsub.rs              PubSubBroker over TopicHub (HTTP-35)
      ws.rs                  WsConnectionRegistry, handshake helpers (HTTP-45)
      grpc.rs                LPM framing and iterator-backed stream body without spawned task (HTTP-36)
      hmr.rs                 dev HMR endpoint reusing ws handshake and WsConnectionRegistry (HTTP-45)
    workers/
      mod.rs                 worker factory and supervisor exports
      factory.rs             RingFactory, cleanup_stale_shm, install_panic_cleanup_hook (HTTP-23, OKF-07)
      supervisor.rs          Supervisor polling WorkerLifecycle, calling sweep_for_worker + RingPair::reclaim, respawning with backoff (HTTP-23, OKF-07)
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Wire correlation ID bit layout change (`CorrelationId`) could break in-flight request matching if encode/decode sites drift. | Encapsulate all bit manipulation inside `CorrelationId::{encode, decode}` in `dispatcher/correlation.rs`, delete all raw `<< 8` / `>> 8` shifts across `vertebra-http`, and verify with a generation-wrap unit test (`2^24 + 1` reuses) plus full SHM integration tests. |
| Routing SHM streaming frames through `InFlightRequests` (`HTTP-06`) touches the hot response drain path. | Parse headers once via borrowed `ResponseSlotView`, group completions by shard to lock once per shard per wakeup, and verify with both `tests/streaming_body_tests.rs` and the `server_throughput` benchmark gate. |
| Wiring `ingress::Pipeline` into `handle` (`HTTP-02`) could add per-request overhead when optional features are disabled. | Build `Pipeline` once from `ServerConfig` with `Option` fast-skip branches so disabled interceptors/filters cost a single branch; gate on `server_throughput` (`direct_dispatch` and `bridge_dispatch`). |
| Decision D3 (`WellKnownDiscoveryCache`) requires benchmarking Rust discovery interception against the Python route. | Task 3.7 states the exact benchmark comparison rule and both concrete outcomes (keep `ingress/discovery.rs` if at least 5% faster on `/.well-known/agent-card.json`; delete `discovery.rs` and its config hook in the same task if not). |

## Implementation Plan

### Phase 1: Core Primitives, Routing, Dispatch & Streaming
- [x] 3.1 Shared body, correlation, and flag primitives ([worksheet](tasks/3.1.md))
- [x] 3.2 Routing and batcher ([worksheet](tasks/3.2.md))
- [x] 3.3 Dispatcher backend, lifecycle, and worker supervision ([worksheet](tasks/3.3.md))
- [x] 3.4 SHM streaming responses ([worksheet](tasks/3.4.md))
- [x] 3.5 Lock-free backpressure ([worksheet](tasks/3.5.md))
- [x] Checkpoint: `make lint-all && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,compression`

### Phase 2: Ingress Pipeline, Protocols, Realtime & Config Cleanup
- [x] 3.6 Ingress pipeline core ([worksheet](tasks/3.6.md))
- [x] 3.7 Ingress endpoints ([worksheet](tasks/3.7.md))
- [x] 3.8 Connection protocols ([worksheet](tasks/3.8.md))
- [x] 3.9 Realtime topic hub ([worksheet](tasks/3.9.md))
- [x] 3.10 Config, main, dependencies, and MCP bridge removal ([worksheet](tasks/3.10.md))
- [x] Chapter gate: `make lint-all && make rs-test && make py-test && make ts-test` (+ `make bench-check BASELINE=pre-refactor`), then area review per Global Constraint 12.

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C3-R1 | 3.1 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http correlation streaming` |
| C3-R2 | 3.2 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http router batcher && uv run pytest src/py/tests/unit/server/test_routing.py -q` |
| C3-R3 | 3.3 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test test_dispatcher --test test_server` |
| C3-R4 | 3.4 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test streaming_body_tests` |
| C3-R5 | 3.5 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test backpressure_streams_tests && cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput -- backpressure` |
| C3-R6 | 3.6 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test ingress_fastpath_tests --test test_server` |
| C3-R7 | 3.7 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test assets_and_observability_tests --test mcp_discovery_bridge_tests` |
| C3-R8 | 3.8 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,http3 --test h2c_and_mtls_tests --test test_tls --test http3_quic_tests` |
| C3-R9 | 3.9 | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test sse_hub_tests --test pubsub_broker_tests --test grpc_wire_tests --test ws_mount_tests --test test_hmr` |
| C3-R10 | 3.10 | `make lint-all && make rs-test && make py-test` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| HTTP-01 | 3.10 |
| HTTP-02 | 3.6 |
| HTTP-03 | 3.6 |
| HTTP-04 | 3.6 |
| HTTP-05 | 3.6 |
| HTTP-06 | 3.4 |
| HTTP-07 | 3.1 |
| HTTP-08 | 3.1 |
| HTTP-10 | 3.4 |
| HTTP-11 | 3.5 |
| HTTP-12 | 3.5 |
| HTTP-13 | 3.6 |
| HTTP-14 | 3.5 |
| HTTP-15 | 3.2 |
| HTTP-16 | 3.2 |
| HTTP-17 | 3.2 |
| HTTP-18 | 3.3 |
| HTTP-19 | 3.1 |
| HTTP-20 | 3.3 |
| HTTP-21 | 3.3 |
| HTTP-22 | 3.3 |
| HTTP-23 | 3.3 |
| HTTP-24 | 3.10 |
| HTTP-25 | 3.8 |
| HTTP-26 | 3.8 |
| HTTP-28 | 3.8 |
| HTTP-29 | 3.2 |
| HTTP-30 | 3.2 |
| HTTP-31 | 3.6 |
| HTTP-32 | 3.8 |
| HTTP-33 | 3.7 |
| HTTP-34 | 3.7 |
| HTTP-35 | 3.9 |
| HTTP-36 | 3.9 |
| HTTP-37 | 3.10 |
| HTTP-38 | 3.10 |
| HTTP-39 | 3.2 |
| HTTP-40 | 3.6 |
| HTTP-42 | 3.7 |
| HTTP-43 | 3.7 |
| HTTP-44 | 3.9 |
| HTTP-45 | 3.9 |
| MCP-02 | 3.10 |
| MCP-03 | 3.10 |
| MCP-07 | 3.7 |
| OKF-01 | 3.1 |
| OKF-07 | 3.3 |

## Continuity Snapshot

- Active flow: `http-ingress-consolidation`, state `planned`. Current task: none. Next step: `/flow:implement http-ingress-consolidation` after roadmap approval and completion of `core-ipc-consolidation`.
- Plan identity: plan_revision 2, plan_commit "". State identity: state_revision 0.
- Blockers: none. Decisions: inherited D1–D4 from the roadmap (D2 governs `mcp_bridge.rs` removal in 3.10; D3 governs `WellKnownDiscoveryCache` benchmark evaluation in 3.7; stub policy from Global Constraint 6 governs `profile.rs` in 3.7).
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/workflow.md`, `.agents/bundles/knowledge/architecture/http/ingress-and-routing.md`, `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md`, `.agents/bundles/knowledge/architecture/runtime/shm-supervision-and-recovery.md`, `.agents/bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md`.
