# Phase 1: Server Completion

**Goal**: Feature parity with standard ASGI servers and robust production behavior.
**Status**: Complete.

## Tasks

### Litestar Integration (Plugin)
- [x] **P1.0**: Create `src/py/vertebra/extensions/litestar.py` (optional dependency).
    - Implement `VertebraPlugin` class.
    - Implement `InitPluginProtocol` (logging config).
    - Implement `CLIPluginProtocol` (add `run` command).
    - Ensure parity with `litestar-granian` (arguments, behavior).

### Streaming & Protocols
- [x] **P1.1**: Implement `http.request` frame in `src/ts/src/ipc/asgi.ts`.
- [x] **P1.2**: Update `AsgiBridge` (TS) to stream request body chunks.
- [x] **P1.3**: Update `AsgiWorker` (Py) to yield chunks in `receive()`.
- [x] **P1.4**: Implement `http.disconnect` handling (AbortController -> IPC).
- [x] **P1.5**: Implement WebSocket frame types (`websocket.accept`, `send`, `receive`, `close`).
- [x] **P1.6**: Wire WebSocket upgrade in Bun router to IPC bridge.
    - **Note**: Protobuf IPC remains post-MVP; keep MessagePack as the MVP codec.

### Metrics & Observability
- [x] **P1.7**: Create `src/ts/src/server/metrics.ts`.
- [x] **P1.8**: Implement `UnifiedMetricsRegistry` (Aggregator).
- [x] **P1.9**: Wire Broker IPC stats fetch (`broker.stats`) into registry.
- [x] **P1.10**: Expose `GET /__metrics__` endpoint (Prometheus format).
- [x] **P1.11**: Implement structured JSON logging (TS side).
- [x] **P1.12**: Implement trace ID propagation (HTTP -> IPC).

### Lifecycle & CLI ("Go Fast")
- [x] **P1.13**: Implement Graceful Shutdown (drain timeout).
- [x] **P1.14**: Create `vertebra.cli` module.
- [x] **P1.15**: Implement "Go Fast" Startup Banner (ASCII art, stats summary).
- [x] **P1.16**: Implement process manager (Supervisor) in Python/Rust.

## Verification
- Run `bun test`.
- Run `uv run pytest src/py/tests -k asgi`.
- Verify `vertebra serve app:app` shows the "Go Fast" banner.
- Verify `curl http://localhost:8000/__metrics__` returns metrics output.
- Verify logs are JSON formatted in prod mode.
