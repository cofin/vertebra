---
type: Gotcha
title: Server Runtime Limitations
description: Verified gaps in mounted response streaming, startup failure handling, the supervisor package surface, worker lanes, and wheel-shipped server binaries that callers and maintainers must account for.
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
- by: agent:flow-sync
  at: "2026-11-12T01:15:00Z"
  commit: c8722c76
sources:
- id: asgi-protocol-rs
  resource: src/rs/vertebra-py/src/asgi.rs::AsgiProtocol
  title: Rust ASGI protocol send path and body buffer
- id: rsgi-http-rs
  resource: src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol
  title: Rust RSGI HTTP protocol stream accumulation
- id: bridge-http-rs
  resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
  title: Embedded bridge correlation completion
- id: ring-pair-rs
  resource: src/rs/vertebra-core/src/transport/ring_pair.rs::RingPair
  title: SHM response ring VRES framing
- id: worker-py
  resource: src/py/vertebra/server/worker.py::Worker
  title: Worker lifespan entry and mount runner instantiation
- id: supervisor-init-py
  resource: src/py/vertebra/supervisor/__init__.py
  title: Supervisor package docstring
- id: run-cli-py
  resource: src/py/vertebra/cli/run.py
  title: vtb run command and worker-only flag
- id: workers-cli-py
  resource: src/py/vertebra/cli/workers.py::execute_worker_cli
  title: vtb workers run entry point
- id: supervisor-py
  resource: src/py/vertebra/supervisor/_supervisor.py::find_server_binary
  title: Server binary resolution
---

# Server Runtime Limitations

<!-- truth: start -->
- Mounted ASGI responses do not stream: `AsgiProtocol::send` appends every `more_body=True` chunk to `AsgiProtocolState.body_buf` and submits one unary response when the final chunk arrives, so SSE and MCP streamable HTTP deliver nothing until the handler returns [^asgi-protocol-rs].
- RSGI `response_stream` appends into `RsgiHttpProtocol.stream_chunks` and flushes once at return [^rsgi-http-rs].
- The embedded bridge completes every correlation with `stream_rx: None`, and SHM response rings carry only unflagged `VRES` frames (`RingPair::send_response`), so neither transport carries a progressive response today [^bridge-http-rs] [^ring-pair-rs].
- `Worker.run` logs and swallows ASGI lifespan startup failures (`"ASGI lifespan startup failed for mount %r"`), and `_instantiate_mount_runners` logs and swallows RSGI `__rsgi_init__` failures; the worker then serves traffic without the resources startup was meant to create [^worker-py].
- `vertebra.supervisor` (`supervisor/__init__.py`) contains only a docstring; it imports nothing and defines no `__all__`, so launcher symbols must be imported from `vertebra.supervisor._supervisor` [^supervisor-init-py].
- `vtb run --worker-only` only sets `VERTEBRA_WORKER_ONLY`, which no module reads, and still launches the HTTP server [^run-cli-py].
- `vtb workers run` calls `execute_worker_cli` without `run_until_stopped=True`, so it enters the app lifespan, stops the runner, and exits; `WorkerRunner` is built without a `QueueService`, so `process_once` has nothing to dequeue [^workers-cli-py].
- `find_server_binary` checks for a packaged `vertebra/bin/vertebra-server[.exe]`, but the `vertebra-core` wheel does not ship one; installs resolve `VERTEBRA_SERVER_BINARY` or a locally built binary [^supervisor-py].
<!-- truth: end -->

## Planned Fixes

The `server-runtime-correctness` flow owns incremental mounted streaming over both transports ([#81](https://github.com/cofin/vertebra/issues/81)), fail-loud startup ([#84](https://github.com/cofin/vertebra/issues/84)), the supervisor package exports ([#83](https://github.com/cofin/vertebra/issues/83)), worker-only and worker-lane runtime behavior ([#85](https://github.com/cofin/vertebra/issues/85)), and the wheel-shipped relocatable server binary ([#82](https://github.com/cofin/vertebra/issues/82)).

Current runtime behavior is described in [ASGI, RSGI & WSGI Runners](../../architecture/worker/asgi-and-wsgi-runners.md), [Python Server, Worker Loop & Protocol Adapters Domain](worker-and-protocols.md), and [Multi-Lane Worker Runner](worker-lanes.md).

[^asgi-protocol-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — Rust ASGI protocol send path and body buffer.
[^rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — Rust RSGI HTTP protocol stream accumulation.
[^bridge-http-rs]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded bridge correlation completion.
[^ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs::RingPair` — SHM response ring `VRES` framing.
[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — Worker lifespan entry and mount runner instantiation.
[^supervisor-init-py]: `src/py/vertebra/supervisor/__init__.py` — Supervisor package docstring.
[^run-cli-py]: `src/py/vertebra/cli/run.py` — `vtb run` command and worker-only flag.
[^workers-cli-py]: `src/py/vertebra/cli/workers.py::execute_worker_cli` — `vtb workers run` entry point.
[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::find_server_binary` — Server binary resolution.
