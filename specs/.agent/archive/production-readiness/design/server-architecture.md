# Design: Bun Server Architecture (Phase 1)

**Focus**: Closing gaps in PRD-3 (Streaming, WebSockets, Metrics) and Litestar Integration.

## 1. Litestar Plugin Integration (`litestar-granian` parity)

**Goal**: Seamless integration into Litestar's CLI and lifecycle, matching `litestar-granian` patterns.

**Component**: `VertebraPlugin`
**Location**: `src/py/vertebra/plugin.py`
**Implements**: `InitPluginProtocol`, `CLIPluginProtocol` (from `litestar.plugins`)

**Capabilities**:
1.  **CLI Integration (`on_cli_init`)**:
    -   Registers a `run` command group (or overrides default).
    -   Adds command: `litestar run` (launches Vertebra instead of Uvicorn).
    -   Exposes flags: `--host`, `--port`, `--reload`, `--workers`.
2.  **App Initialization (`on_app_init`)**:
    -   Configures logging (intercepts standard logs and routes to structlog/json if configured).
    -   Injects `VertebraPlugin` state into `app.state`.
    -   (Future) Registers Type Bridge watcher if in dev mode.

**Runtime Behavior**:
-   The plugin detects if it's running via `litestar` CLI.
-   If `run` command is invoked, it delegates to `vertebra.server.manager.run_server()`.
-   This ensures users can use `litestar run` *or* `vertebra serve` interchangeably.

## 2. Request Body Streaming

**Current Limitation**: `AsgiBridge` buffers full body.
**Solution**:
1.  **TS Side**:
    -   Use `request.body.getReader()` in `src/ts/src/server/router.ts`.
    -   Read chunks loop.
    -   Send `http.request` frame for each chunk.
    -   Send `http.request` with `more_body: false` on stream end.
2.  **Py Side**:
    -   `AsgiWorker` receive loop needs to handle interleaved frames.
    -   If `http.request` frame received, push to `input_queue`.
    -   ASGI app `receive()` pulls from `input_queue`.

**Backpressure**:
-   If IPC channel `send()` blocks (ring buffer full), TS reader pauses.
-   This naturally propagates TCP backpressure to client.

## 3. WebSocket Bridging

**Flow**:
1.  **Upgrade**: Router detects `Upgrade: websocket`.
2.  **Accept**: TS upgrades connection to `ServerWebSocket`.
3.  **Handshake**: TS sends `websocket.connect` IPC frame.
4.  **Accept (Py)**: App sends `websocket.accept`. TS sets state to `OPEN`.
5.  **Traffic**:
    -   TS `message` event -> IPC `websocket.receive`.
    -   IPC `websocket.send` -> TS `ws.send()`.
6.  **Close**:
    -   TS `close` event -> IPC `websocket.disconnect`.
    -   IPC `websocket.close` -> TS `ws.close()`.

## 4. Graceful Shutdown

**Signal Handling**:
1.  **TS (Master)**:
    -   Listen for `SIGINT`/`SIGTERM`.
    -   `server.stop()` (stops accepting new conns).
    -   Set `isShuttingDown = true`.
    -   Notify Workers: Send `lifespan.shutdown`.
    -   Wait `drainTimeout` (e.g. 5s).
    -   If active requests > 0, wait.
    -   Kill workers `SIGKILL` if timeout exceeded.
    -   Exit `0`.

**Py (Worker)**:
1.  **Lifespan**: Receive `lifespan.shutdown`.
2.  **App**: Trigger shutdown events.
3.  **Exit**: Process exits.

## 5. Metrics & Observability

**Goal**: Unified observability endpoint without multi-port scraping.

**Architecture**:
-   **Bun Server** acts as the aggregator.
-   **Broker** exposes stats via IPC `broker.stats`.
-   **Python** exposes stats via IPC `worker.stats` (or shared memory counters).

**Components**:
1.  **Unified Endpoint**: `GET /__metrics__` (Prometheus text format).
    -   Served by Bun.
    -   Exported via `prom-client` on the Bun side (aligned with Python `prometheus-client` naming).
    -   Aggregates:
        -   **HTTP**: Request rate, latency buckets, active connections (Bun internal).
        -   **System**: CPU/RAM usage (Bun internal).
        -   **Pools**: DB connection stats (Fetched from Broker via IPC).
        -   **Workers**: Active/Idle counts (Tracked by WorkerManager).
2.  **Structured Logging**:
    -   All components (Bun, Rust, Python) emit JSON logs to stdout/stderr.
    -   **Fields**: `timestamp`, `level`, `component` (bun/broker/worker), `trace_id`, `message`.
    -   **Trace Context**: `X-Request-ID` or `traceparent` passed via IPC frames to correlate logs.
    -   **SQLSpec Correlation**: Map SQLSpec correlation IDs into `trace_id` when present.

## 6. Startup Experience ("Go Fast" CLI)

**Goal**: A polished, informative startup screen similar to Sanic/Astro.

**Banner Content**:
-   **Header**: ASCII Art "Vertebra" (Cyan).
-   **Version**: `v0.1.0` (Green).
-   **Server**: `http://0.0.0.0:8000` (Clickable).
-   **Workers**: `4` (Python 3.12).
-   **Pools**: `Postgres (20)`, `Redis (5)`.
-   **Mode**: `Development` (with HMR) or `Production`.

**Implementation**:
-   The **Rust Supervisor** (or Python CLI wrapper) gathers this info *after* successful bind/connect.
-   Prints the formatted block to stdout before streaming logs.
