---
type: Gotcha
title: Server Runtime Limitations
description: Residual runtime boundaries around mid-stream error signaling, SHM reader backpressure, client disconnect visibility, fail-loud OS entropy checks, and shared libpython requirements.
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
  title: Per-worker reader backpressure and shutdown cancellation
- id: streams-http-rs
  resource: src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry
  title: Stream frame receiver Infallible contract and disconnect notification
- id: core-id-rs
  resource: src/rs/vertebra-core/src/id.rs
  title: Fail-loud OS entropy pool and Unix fork reseed hook
- id: supervisor-py
  resource: src/py/vertebra/supervisor/_supervisor.py::EmbeddedPythonRuntime
  title: Shared libpython runtime verification for packaged vertebra-server
- id: shm-queue-py
  resource: src/py/vertebra/extensions/litestar/queues/shm_backend.py::SHMQueueBackend
  title: Single-host POSIX SHM queue backend
- id: channel-bus-py
  resource: src/py/vertebra/server/_channels.py::ChannelBus
  title: Bounded subscriber backpressure and notification bridge
- id: pg-notify-py
  resource: src/py/vertebra/db/adapters/postgres/notify.py::AsyncPostgresNotificationSource
  title: PostgreSQL NOTIFY 8000-byte frame limit and spill envelopes
- id: ora-notify-py
  resource: src/py/vertebra/db/adapters/oracle/notify.py::AsyncOracleNotificationSource
  title: Oracle TxEventQ and DBMS_AQ notification transport
---

# Server Runtime Limitations

<!-- truth: start -->
- Progressive ASGI, RSGI, and native `App` streaming flushes headers on the first non-final chunk; mid-stream application exceptions emit `STREAM_FLAG_ERROR` and terminate the body early because `FrameReceiver` yields `Result<Frame<Bytes>, Infallible>` and HTTP status/headers are already on the wire (`AsgiRunner.__call__` remains collecting by contract) [^asgi-protocol-rs] [^rsgi-http-rs] [^bridge-http-rs] [^ring-pair-rs].
- A slow streaming client stalls delivery of later responses from the same worker while its SHM reader awaits bounded stream-channel capacity until the client drains or disconnects; other workers continue independently [^shm-reader-rs].
- `BatchBridge::submit_stream_frame` detects client closure via `StreamDelivery::Closed` (`ConnectionResetError` in Python), whereas SHM worker streams learn of closed receivers asynchronously when dispatcher sweeps fire `notify_disconnect` or reader shutdown cancels delivery [^streams-http-rs] [^shm-reader-rs].
- ID generation in `vertebra-core` requires a functioning OS entropy source (`getrandom::fill`) and panics on failure rather than falling back to time-seeded PRNG state; Unix worker forks automatically advance `FORK_GENERATION` via `pthread_atfork` to reseed child entropy pools [^core-id-rs].
- Packaged POSIX `vertebra-server` wheels use `$ORIGIN`/`@loader_path` relative rpaths and require a dynamically linked shared `libpython` runtime (`ENABLE_SHARED=1`, checked by `EmbeddedPythonRuntime.require_shared_library` unless `VERTEBRA_SERVER_BINARY` is set) [^supervisor-py].
- `SHMQueueBackend` and memory `ChannelBus` operate across single-host processes only; cross-host queue wakeups and channels require Valkey, Redis, PostgreSQL (`NOTIFY` capped at `8,000` bytes — send wakeup IDs or `ChangeFeed` spill envelopes), or Oracle (`TxEventQ` / `DBMS_AQ`, falling back to adaptive polling when AQ privileges are absent) [^shm-queue-py] [^channel-bus-py] [^pg-notify-py] [^ora-notify-py].
<!-- truth: end -->

## Operational Context

Current server architecture and streaming transport details live in [ASGI, RSGI & WSGI Runners](../../architecture/worker/asgi-and-wsgi-runners.md), [Python Server, Worker Loop & Protocol Adapters Domain](worker-and-protocols.md), and [Multi-Lane Worker Runner](worker-lanes.md).

[^asgi-protocol-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — Rust ASGI progressive stream flush and mid-stream `FLAG_ERROR` emission.
[^rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — Rust RSGI HTTP progressive `response_stream` flush.
[^bridge-http-rs]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded bridge correlation completion.
[^ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs::RingPair` — SHM unary `VRES` and ordered `VRSF` streaming frames.
[^shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker` — Per-worker reader backpressure and shutdown cancellation.
[^streams-http-rs]: `src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry` — `FrameReceiver` `Infallible` contract and `notify_disconnect`.
[^core-id-rs]: `src/rs/vertebra-core/src/id.rs` — Fail-loud OS entropy pool and Unix `pthread_atfork` reseed hook.
[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::EmbeddedPythonRuntime` — Shared `libpython` validation for packaged `vertebra-server`.
[^shm-queue-py]: `src/py/vertebra/extensions/litestar/queues/shm_backend.py::SHMQueueBackend` — Single-host POSIX SHM queue backend.
[^channel-bus-py]: `src/py/vertebra/server/_channels.py::ChannelBus` — Bounded subscriber backpressure and notification bridge.
[^pg-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py::AsyncPostgresNotificationSource` — PostgreSQL `NOTIFY` 8000-byte frame limit and spill envelopes.
[^ora-notify-py]: `src/py/vertebra/db/adapters/oracle/notify.py::AsyncOracleNotificationSource` — Oracle `TxEventQ` and `DBMS_AQ` notification transport.
