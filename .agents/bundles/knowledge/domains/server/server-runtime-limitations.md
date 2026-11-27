---
type: Gotcha
title: Server Runtime Limitations
description: Residual runtime boundaries around mid-stream error signaling, per-stream backpressure, client disconnect visibility, fail-loud OS entropy checks, shared libpython requirements, SHM pub/sub limits, diagnostic profiling, and WebTransport scope.
status: stable
tags:
- server
- worker
- streaming
- limitations
generated:
  by: flow/0.28.0
  at: "2026-11-12T01:15:00Z"
verified:
- by: agent:flow-setup
  at: "2026-11-27T22:10:00Z"
  commit: f3b624a2
- by: agent:flow-executor
  at: "2026-11-17T04:20:00Z"
  commit: 5a833832
- by: agent:flow-executor
  at: "2026-11-13T06:06:00Z"
  commit: 05d88d00
sources:
- id: asgi-protocol-rs
  resource: src/rs/vertebra-py/src/asgi.rs::AsgiProtocol
  title: Rust ASGI progressive stream flush and mid-stream FLAG_ERROR emission
- id: rsgi-http-rs
  resource: src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol
  title: Rust RSGI HTTP progressive response_stream flush
- id: bridge-http-rs
  resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
  title: Embedded bridge correlation completion
- id: ring-pair-rs
  resource: src/rs/vertebra-core/src/transport/ring_pair.rs::RingPair
  title: SHM unary VRES and ordered VRSF streaming frames
- id: shm-reader-rs
  resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker
  title: Per-worker reader StreamPumpTable isolation and shutdown cancellation
- id: streams-http-rs
  resource: src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry
  title: Stream frame receiver Infallible contract and disconnect notification
- id: core-id-rs
  resource: src/rs/vertebra-core/src/id.rs
  title: Fail-loud OS entropy pool and Unix fork reseed hook
- id: supervisor-py
  resource: src/py/vertebra/supervisor/_supervisor.py::EmbeddedPythonRuntime
  title: Shared libpython runtime verification for packaged vertebra-server
- id: queue-base-py
  resource: src/py/vertebra/framework/queues/base.py::QueueBackendSettings
  title: Platform-aware queue backend default (memory on win32, shm on POSIX)
- id: shm-queue-py
  resource: src/py/vertebra/framework/queues/backends.py::SHMQueueBackend
  title: Multi-process POSIX ShmQueueRing queue backend
- id: queue-ring-rs
  resource: src/rs/vertebra-py/src/queue_ring.rs::ShmQueueRing
  title: Rust ShmQueueRing fixed-capacity POSIX SHM ring and CAS store
- id: transport-py-rs
  resource: src/rs/vertebra-py/src/transport.rs::init_transport
  title: Unix-only SHM transport and Windows notify-FD fallback
- id: channel-bus-py
  resource: src/py/vertebra/server/_channels.py::ChannelBus
  title: Bounded subscriber backpressure and notification bridge
- id: pg-notify-py
  resource: src/py/vertebra/db/adapters/postgres/notify.py::AsyncPostgresNotificationSource
  title: PostgreSQL NOTIFY 8000-byte frame limit and spill envelopes
- id: ora-notify-py
  resource: src/py/vertebra/db/adapters/oracle/notify.py::AsyncOracleNotificationSource
  title: Oracle TxEventQ and DBMS_AQ notification transport
- id: wt-bridge-py
  resource: src/rs/vertebra-py/src/webtransport.rs::register_webtransport_handler
  title: Embedded-only WebTransport BatchBridge session dispatch and Shm 501 guard
- id: broker-py
  resource: src/rs/vertebra-py/src/broker.rs::pubsub_publish
  title: Worker pubsub_publish Embedded and SHM bridge routing
- id: core-bridge-rs
  resource: src/rs/vertebra-core/src/bridge.rs::encode_shm_pubsub_frame
  title: SHM pub/sub 256-byte CHAN_MAX_PAYLOAD_BYTES frame encoding
- id: http-profile-rs
  resource: src/rs/vertebra-http/src/profile.rs::ProfileEndpoint
  title: Diagnostic /debug/pprof/* authorization and 404 guards
- id: server-loops-py
  resource: src/py/vertebra/server/_loops.py::EventLoopPool
  title: Inline EventLoopPool exit hook and fast-loop eager task factory
- id: server-platform-py
  resource: src/py/vertebra/server/platform.py::ZPagesRegistry
  title: Diagnostic /taskz cross-shard call-graph timeout and fallback boundaries
- id: http-zpages-rs
  resource: src/rs/vertebra-http/src/zpages.rs::ZPagesEndpoint
  title: Opt-in HTTP Z-Pages authorization, timeout, and SHM fan-out aggregation
---

# Server Runtime Limitations

<!-- truth: start -->
- Progressive ASGI, RSGI, and native `App` streaming flushes headers on the first non-final chunk; mid-stream application exceptions emit `STREAM_FLAG_ERROR` and terminate the body early because `FrameReceiver` yields `Result<Frame<Bytes>, Infallible>` and HTTP status/headers are already on the wire (`AsgiRunner.__call__` remains collecting by contract) [^asgi-protocol-rs] [^rsgi-http-rs] [^bridge-http-rs] [^ring-pair-rs].
- Each SHM response stream isolates continuation frames inside a per-stream `StreamPumpTable` `mpsc` queue (`DEFAULT_STREAM_QUEUE_DEPTH = 64`) so a slow client cannot stall later responses on the same worker; if that staging queue saturates (`TrySendError::Full`), `StreamPumpTable` aborts the stalled stream pump and emits a `500` `FLAG_STREAMING | FLAG_ERROR` termination frame [^shm-reader-rs] [^streams-http-rs].
- `BatchBridge::submit_stream_frame` detects client closure via `StreamDelivery::Closed` (`ConnectionResetError` in Python), whereas SHM worker streams learn of closed receivers asynchronously when dispatcher sweeps fire `notify_disconnect` or reader shutdown cancels delivery [^streams-http-rs] [^shm-reader-rs].
- ID generation in `vertebra-core` requires a functioning OS entropy source (`getrandom::fill`) and panics on failure rather than falling back to time-seeded PRNG state; Unix worker forks automatically advance `FORK_GENERATION` via `pthread_atfork` to reseed child entropy pools [^core-id-rs].
- Packaged POSIX `vertebra-server` wheels use `$ORIGIN`/`@loader_path` relative rpaths and require a dynamically linked shared `libpython` runtime (`ENABLE_SHARED=1`, checked by `EmbeddedPythonRuntime.require_shared_library` unless `VERTEBRA_SERVER_BINARY` is set) [^supervisor-py].
- `SHMQueueBackend` (`vertebra.framework.queues.backends` and `vertebra.extensions.litestar.queues.shm_backend`) persists tasks and CAS records across single-host Unix processes via Rust `ShmQueueRing` with futex and ulock wakeups (fixed ring capacity `4096`, `slot_bytes` `8192`, `256`-byte record keys, 22-byte `/vtbq_<16-hex>` POSIX `shm_open` names). On Windows (`win32`), `ShmQueueRing` raises `NotImplementedError`, `QueueBackendSettings` defaults to `"memory"` (`_default_queue_backend_kind`), `init_transport(mode="shm")` raises `RuntimeError`, and `get_request_notify_fd()` returns `-1` (using `get_batch` polling). `ChannelBus` stays single-host in memory; cross-host queue wakeups and channels require Valkey, Redis, PostgreSQL (`NOTIFY` capped at `8,000` bytes with `ChangeFeed` spill envelopes), or Oracle (`TxEventQ` / `DBMS_AQ` with adaptive polling fallback) [^queue-base-py] [^shm-queue-py] [^queue-ring-rs] [^transport-py-rs] [^channel-bus-py] [^pg-notify-py] [^ora-notify-py].
- HTTP/3 WebTransport (`WebTransportSession`, `WebTransportStream`, `register_webtransport_handler`) runs only in single-process `Topology::Embedded` over `BatchBridge`; `Topology::Shm` and unattached embedded endpoints log a warning and reject extended `CONNECT` with `501 Not Implemented`, and session dispatch forwards bidirectional streams and datagrams only (no unidirectional stream forwarding) [^wt-bridge-py].
- SHM worker `pubsub_publish` frames `[topic_len][key_len][topic][key][payload]` into `ShmLeaseTable` channel `__vtb_pubsub__` and raises `ValueError` whenever the framed payload exceeds `CHAN_MAX_PAYLOAD_BYTES` (`256` bytes); payloads above `256` bytes are accepted only in `Topology::Embedded` via `BatchBridge::publish_pubsub` [^broker-py] [^core-bridge-rs].
- `ProfileEndpoint` (`/debug/pprof/*`) returns `404 Not Found` (`{"error":"heap profiling not supported"}`) for `/debug/pprof/heap` even when enabled and bearer-authorized, returns `404 Not Found` (`{"error":"profiling feature disabled"}`) for `/debug/pprof/profile` unless built with `all(unix, feature = "profiling")`, and clamps CPU capture duration to `10 ms..5000 ms` (default `1000 ms`) [^http-profile-rs].
- Inline single-loop `EventLoopPool` instances wrap `loop.run_forever` and `loop.run_until_complete` at construction so `stop()` called from a running loop closes the loop on exit after `_drain_loop()`; custom C or Rust loop types with read-only method descriptors suppress wrapper assignment and still drain pending tasks and async generators before `loop.stop()`, leaving final `loop.close()` to the caller after `run_forever()` returns [^server-loops-py].
- `capture_loop_task_snapshots` and `/taskz` (`ZPagesRegistry.get_tasks_payload` and `get_tasks_payload_async`) use Python 3.14+ `asyncio.capture_call_graph` and `asyncio.format_call_graph` when supported and fall back to `Task.get_stack(limit=8)` on Python 3.12–3.13 or non-stdlib task types; cross-thread background shard inspections bound each shard probe to a `1.0 s` timeout and omit unresponsive, stopped, or closed loops [^server-loops-py] [^server-platform-py].
- Opt-in HTTP Z-Pages on `vtb run` require `--zpages` (`VERTEBRA_ZPAGES_ENABLED=1`) and enforce loopback-only or constant-time Bearer auth, failing closed with `401` on non-loopback binds without a token. Diagnostic worker probes time out after `2.0 s`, Embedded threads share one `ZPagesRegistry` per process, and SHM supports `?worker=<id>` or parallel fan-out merging. Loop lag sampling runs when `VERTEBRA_LOOP_TELEMETRY` is `1` or `true` (or a recorder is attached) and exports under `"loop_telemetry"` in `/varz` on both request-serving workers and `vtb run --worker-only` `HealthProbeServer`, while sync-only workers still record no scheduling lag [^http-zpages-rs] [^server-loops-py] [^server-platform-py].
<!-- truth: end -->

## Operational Context

Current server architecture and streaming transport details live in [ASGI, RSGI & WSGI Runners](../../architecture/worker/asgi-and-wsgi-runners.md), [Python Server, Worker Loop & Protocol Adapters Domain](worker-and-protocols.md), and [Multi-Lane Worker Runner](worker-lanes.md).

[^asgi-protocol-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — Rust ASGI progressive stream flush and mid-stream `FLAG_ERROR` emission.
[^rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — Rust RSGI HTTP progressive `response_stream` flush.
[^bridge-http-rs]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded bridge correlation completion.
[^ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs::RingPair` — SHM unary `VRES` and ordered `VRSF` streaming frames.
[^shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker` — Per-worker reader `StreamPumpTable` isolation and shutdown cancellation.
[^streams-http-rs]: `src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry` — `FrameReceiver` `Infallible` contract and `notify_disconnect`.
[^core-id-rs]: `src/rs/vertebra-core/src/id.rs` — Fail-loud OS entropy pool and Unix `pthread_atfork` reseed hook.
[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::EmbeddedPythonRuntime` — Shared `libpython` validation for packaged `vertebra-server`.
[^queue-base-py]: `src/py/vertebra/framework/queues/base.py::QueueBackendSettings` — Platform-aware queue backend default (`memory` on `win32`, `shm` on POSIX).
[^shm-queue-py]: `src/py/vertebra/framework/queues/backends.py::SHMQueueBackend` — Multi-process POSIX `ShmQueueRing` queue backend.
[^queue-ring-rs]: `src/rs/vertebra-py/src/queue_ring.rs::ShmQueueRing` — Rust `ShmQueueRing` fixed-capacity POSIX SHM ring and CAS store.
[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::init_transport` — Unix-only SHM transport and Windows notify-FD fallback.
[^channel-bus-py]: `src/py/vertebra/server/_channels.py::ChannelBus` — Bounded subscriber backpressure and notification bridge.
[^pg-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py::AsyncPostgresNotificationSource` — PostgreSQL `NOTIFY` 8000-byte frame limit and spill envelopes.
[^ora-notify-py]: `src/py/vertebra/db/adapters/oracle/notify.py::AsyncOracleNotificationSource` — Oracle `TxEventQ` and `DBMS_AQ` notification transport.
[^wt-bridge-py]: `src/rs/vertebra-py/src/webtransport.rs::register_webtransport_handler` — Embedded-only WebTransport `BatchBridge` session dispatch and `Shm` `501` guard.
[^broker-py]: `src/rs/vertebra-py/src/broker.rs::pubsub_publish` — Worker `pubsub_publish` Embedded and SHM bridge routing.
[^core-bridge-rs]: `src/rs/vertebra-core/src/bridge.rs::encode_shm_pubsub_frame` — SHM pub/sub 256-byte `CHAN_MAX_PAYLOAD_BYTES` frame encoding.
[^http-profile-rs]: `src/rs/vertebra-http/src/profile.rs::ProfileEndpoint` — Diagnostic `/debug/pprof/*` authorization and `404` guards.
[^server-loops-py]: `src/py/vertebra/server/_loops.py::EventLoopPool` — Inline `EventLoopPool` exit hook and fast-loop eager task factory.
[^server-platform-py]: `src/py/vertebra/server/platform.py::ZPagesRegistry` — Diagnostic `/taskz` cross-shard call-graph timeout and fallback boundaries.
[^http-zpages-rs]: `src/rs/vertebra-http/src/zpages.rs::ZPagesEndpoint` — Opt-in HTTP Z-Pages authorization, `2.0 s` diagnostic timeout, and SHM fan-out aggregation.
