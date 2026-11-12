---
type: Spec
flow_id: server-runtime-correctness
title: Server Runtime Correctness — Incremental Streaming, Fail-Loud Startup, Worker Modes, and Wheel-Shipped Server Binary
description: Stream mounted ASGI and RSGI responses incrementally across the in-process bridge and shared-memory transports, abort worker startup on lifespan failure, export the supervisor API, make worker-only and worker lanes real, and ship a relocatable vertebra-server binary inside every core wheel with a smoke test that runs vtb run, and remove FastPath aliases, predictable UUID entropy, native-helper fallbacks, and inert runtime parameters.
status: draft
state: planned
has_chapters: true
chapters:
  - ch01-bridge-stream-frames
  - ch02-python-stream-submission
  - ch03-asgi-rsgi-incremental-flush
  - ch04-lifespan-fail-loud
  - ch05-supervisor-exports-and-worker-lanes
  - ch06-relocatable-server-binary
  - ch07-wheel-packaging-and-smoke
  - ch08-fastpath-cleanup
  - ch09-runtime-native-cleanup
  - ch10-docs-and-okf-sync
  - ch11-product-okf-and-release-gate
current_chapter: ch01-bridge-stream-frames
current_task: "server-runtime-correctness:ch01:t01"
parent_prd: integration-remediation
depends_on_flows: []
plan_revision: 2
plan_commit: null
state_revision: 1
last_verified_checkpoint: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T02:02:40Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: asgi-protocol-rs
    resource: src/rs/vertebra-py/src/asgi.rs::AsgiProtocol
    title: Rust ASGI protocol that buffers more_body chunks into body_buf until the final chunk
  - id: bridge-core-rs
    resource: src/rs/vertebra-core/src/bridge.rs::BatchBridge
    title: In-process batch bridge with unary-only BridgeResponse and ResponseSink
  - id: bridge-http-rs
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
    title: Embedded-topology correlation table that completes every response with stream_rx None
  - id: shm-reader-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs::PendingRequests
    title: Unreachable ResponseSlot stream responder map that drops frames with try_send
  - id: transport-py-rs
    resource: src/rs/vertebra-py/src/transport.rs::submit_raw_response
    title: Python-callable raw response submission whose SHM branch writes streaming chunks as unary frames
  - id: worker-py
    resource: src/py/vertebra/server/worker.py::Worker
    title: Worker run loop that logs and swallows ASGI lifespan startup failures
  - id: supervisor-py
    resource: src/py/vertebra/supervisor/_supervisor.py::find_server_binary
    title: Server binary resolution that expects vertebra/bin/vertebra-server inside the installed package
  - id: run-cli-py
    resource: src/py/vertebra/cli/run.py::run
    title: vtb run command that sets VERTEBRA_WORKER_ONLY without any reader
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: Release wheel matrix (Linux x86_64 and aarch64, macOS, Windows) without a server binary in vertebra-core
  - id: fastpath-attach-py
    resource: src/py/vertebra/server/_fastpath.py::attach_framework_fastpath
    title: FastPath resolver that probes four alias locations
  - id: id-rs
    resource: src/rs/vertebra-core/src/id.rs::fill_os_random
    title: UUID entropy source with a time-seeded fallback and no fork detection
tags:
  - server
  - streaming
  - asgi
  - rsgi
  - lifespan
  - supervisor
  - workers
  - packaging
  - wheel
---

# Specification: Server Runtime Correctness

<!-- truth: start -->
- **Incremental Mounted Streaming**: Mounted ASGI and RSGI responses flush the response start and every non-final body chunk as ordered stream frames through both the embedded in-process bridge and the shared-memory rings, with lossless backpressure and client-disconnect signalling ([#81](https://github.com/cofin/vertebra/issues/81)) [^asgi-protocol-rs] [^bridge-core-rs] [^bridge-http-rs] [^shm-reader-rs] [^transport-py-rs].
- **Fail-Loud Startup**: ASGI lifespan startup failures, startup timeouts, and RSGI `__rsgi_init__` failures abort worker startup with `WorkerStartupError`; SHM workers exit with code `78`, the Rust supervisor stops respawning, and `vertebra-server` exits non-zero ([#84](https://github.com/cofin/vertebra/issues/84)) [^worker-py].
- **Supervisor API and Worker Modes**: `vertebra.supervisor` exports its launcher API ([#83](https://github.com/cofin/vertebra/issues/83)); worker lanes bind a resolved `QueueService`, `vtb workers run` keeps running until signalled, and `vtb run --worker-only` runs worker lanes behind a health-probe listener instead of a no-op environment flag ([#85](https://github.com/cofin/vertebra/issues/85)) [^run-cli-py].
- **Wheel-Shipped Server Binary**: Every `vertebra-core` wheel (Linux x86_64 and aarch64, macOS x86_64 and arm64, Windows AMD64, CPython `3.12`–`3.15t`) carries a relocatable `vertebra/bin/vertebra-server` linked against the shared libpython of its ABI, and a smoke test installs the wheel and runs `vtb run` locally, in CI, and in the release matrix ([#82](https://github.com/cofin/vertebra/issues/82)) [^supervisor-py] [^release-yaml].
- **Runtime Cleanup**: Framework FastPath objects resolve only through `__vertebra_fastpath__`, one `has_custom_exception_handlers` lives in `_fastpath_core.py`, Starlette and FastAPI responses serialize by exact type without copying, UUID entropy comes from `getrandom` with a panic on failure and fork-safe reseeding, `_zero_copy.py` binds native helpers directly, `import_optional` relies on `sys.modules`, Spanner `batch_write` no longer accepts the inert `max_commit_delay_ms`, and `EmbeddingBroker.submit_batch` has no fixed per-chunk sleep [^fastpath-attach-py] [^id-rs].
- **Roadmap**: `11` dependency-ordered chapters and `33` vertical-slice tasks; `mcp-protocol-completeness` consumes the streaming contract delivered by `ch01`–`ch03`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement

Applications that mount a Litestar, Starlette, FastAPI, or MCP ASGI app under Vertebra cannot stream. `AsgiProtocol::send` in `src/rs/vertebra-py/src/asgi.rs` appends every `more_body=True` chunk of `http.response.body`, `http.response.zerocopysend`, and `http.response.sse` to `AsgiProtocolState.body_buf` and submits one unary response only when the final chunk arrives [^asgi-protocol-rs]. The embedded transport cannot carry a stream even if the protocol flushed: `BridgeResponse` has no flags, and `BridgeCorrelation::ensure_sink_registered` completes every correlation with `PendingResponse { stream_rx: None, .. }` [^bridge-core-rs] [^bridge-http-rs]. The shared-memory rings carry only unflagged `VRES` frames, and `shm_reader::drain_worker` completes every drained frame as a unary response; `ShmDispatcher::handle_raw_response_slot` and `PendingRequests` decode a separate 16-byte `ResponseSlot` format with streaming flags, but no production path calls them and they drop frames with `try_send` when a stream queue holds `64` frames. `submit_raw_response(..., flags=FLAG_STREAMING)` writes each chunk through `send_shm_response_detached` as a unary `VRES` frame, discarding the flags [^shm-reader-rs] [^transport-py-rs]. RSGI `response_stream` accumulates into `RsgiHttpProtocol.stream_chunks` with the same effect. SSE and MCP streamable HTTP therefore deliver nothing until the handler returns ([#81](https://github.com/cofin/vertebra/issues/81)).

Startup is not fail-loud. `Worker.run` wraps `enter_litestar_server_lifespan` and `AsgiLifespan` in `except Exception: logger.exception("ASGI lifespan startup failed for mount %r", ...)` and serves traffic without the resources startup was meant to create; `_instantiate_mount_runners` does the same for RSGI `__rsgi_init__` [^worker-py]. A failing SHM worker would also be respawned by `Supervisor::poll_workers` with exponential backoff instead of stopping the server ([#84](https://github.com/cofin/vertebra/issues/84)).

`src/py/vertebra/supervisor/__init__.py` holds only a docstring that names imports it never performs ([#83](https://github.com/cofin/vertebra/issues/83)). `vtb run --worker-only` sets `VERTEBRA_WORKER_ONLY`, which no module reads, and still launches the HTTP server [^run-cli-py]. `vtb workers run` calls `execute_worker_cli` without `run_until_stopped=True`, so it enters and exits the lifespan immediately, and every `WorkerRunner` (CLI, colocated `--with-workers`) is built without a `QueueService`, so `WorkerRunner.process_once` returns `0` forever ([#85](https://github.com/cofin/vertebra/issues/85)).

`find_server_binary` looks for `vertebra/bin/vertebra-server` inside the installed package first, but `[tool.maturin]` in `pyproject.toml` ships only the `_vertebra` extension, `release.yaml` builds no server binary for the core wheel, `release-binaries.yml` publishes standalone binaries without a linux-arm64 entry, and `src/rs/vertebra-http/build.rs` bakes the build machine's absolute libpython directory into the binary's rpath [^supervisor-py] [^release-yaml]. A wheel-only install of `vertebra-core` cannot run `vtb run` ([#82](https://github.com/cofin/vertebra/issues/82)).

### 1.2 Non-Goals

- Changing the buffered batch path (`AsgiRunner.__call__`, `_AsgiMountRunner.__call__`, and the result-returning branch of `dispatch_batch`): it returns complete `AsgiResponse` objects by contract and stays collecting; streaming applies to every submit path (`drive_asgi_batch`, `drive_rsgi_batch`, `spawn_single`, `spawn_batch`).
- Aborting an HTTP body with a transport-level error after streaming began: `FrameReceiver` carries `Result<Frame<Bytes>, Infallible>`, so an error frame ends the body early; the limitation is recorded in `ch10:t04`.
- Gating the accept loop on worker lifespan readiness: requests queue in the bridge or rings until the worker loop starts after startup, and a failed startup shuts the server down.
- Defining how applications register queue tasks and backends (owned by `queues-tasks-and-channels`); this flow only resolves an already-built `QueueService` for worker lanes.
- Removing the standalone `release-binaries.yml` channel.

### 1.3 Code Analysis Summary

| Existing symbol | Verified consequence |
| --- | --- |
| `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol::send` [^asgi-protocol-rs] | Buffers non-final chunks; submits via `ResponseSubmitter::submit_raw_inner` only when `!more_body` and `direct_submit && immediate_submit`. |
| `src/rs/vertebra-core/src/bridge.rs::{BridgeResponse,ResponseSink,BatchBridge::submit_response}` [^bridge-core-rs] | Unary frames only; the sink returns `()` so producers cannot observe backpressure or disconnect. |
| `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation::{ensure_sink_registered,complete}` [^bridge-http-rs] | Completes with `stream_rx: None`; `sweep_expired` answers `504` only for correlations not yet completed, so a stream completed at its first frame is never swept. |
| `src/rs/vertebra-http/src/body.rs::PendingResponse::streaming` | Existing progressive response constructor over `mpsc::Receiver<Result<Frame<Bytes>, Infallible>>`; `response::build_response` already takes the streaming branch. |
| `src/rs/vertebra-core/src/transport/ring_pair.rs::{RingPair::send_response,RingPair::drain_with}` | SHM response rings carry only unflagged `VRES` frames (`magic, correlation_id, status, headers_len, body_len`); `shm_reader::drain_worker` completes every drained frame as unary. |
| `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::PendingRequests` [^shm-reader-rs], `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher::handle_raw_response_slot` | Decode the 16-byte `ResponseSlot` format and drop frames with `try_send`; no production path calls them. |
| `src/rs/vertebra-py/src/transport.rs::{submit_raw_response,ResponseSubmitter::submit_raw_inner}` [^transport-py-rs] | SHM streaming branch sends unary `VRES` frames; the bridge branch has no stream path. |
| `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` | `response_stream` sets `streaming` and appends into `stream_chunks`; flushes once at return. |
| `src/py/vertebra/server/_asgi/_queues.py::AsgiSend` | Python fallback collects chunks; `on_chunk` callback exists but no production caller sets it. |
| `src/py/vertebra/server/worker.py::{Worker.run,_instantiate_mount_runners}` [^worker-py] | Swallows lifespan and RSGI init failures. |
| `src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor::poll_workers` | Respawns every exited worker until the circuit breaker trips; the exit status is discarded by `WorkerLifecycle::check_alive`. |
| `src/py/vertebra/supervisor/__init__.py` | Docstring only; `__all__` absent. |
| `src/py/vertebra/cli/run.py::run`, `src/py/vertebra/cli/workers.py::{execute_worker_cli,run_workers}` [^run-cli-py] | `--worker-only` unread; `vtb workers run` exits after entering the lifespan; no `QueueService` binding. |
| `src/py/vertebra/supervisor/_supervisor.py::find_server_binary` [^supervisor-py], `pyproject.toml` `[tool.maturin]`, `src/rs/vertebra-http/build.rs` | Packaged binary path expected but never shipped; absolute rpath is not relocatable. |

### 1.4 Relevant OKF Knowledge & Glossary

Read [tiered-rings-and-batch-bridge.md](../../knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md), [asgi-and-wsgi-runners.md](../../knowledge/architecture/worker/asgi-and-wsgi-runners.md), [server-and-dispatch.md](../../knowledge/domains/http/server-and-dispatch.md), [worker-and-protocols.md](../../knowledge/domains/server/worker-and-protocols.md), [worker-lanes.md](../../knowledge/domains/server/worker-lanes.md), [packaging-and-pgo.md](../../knowledge/domains/frontend/packaging-and-pgo.md), and [pyo3-and-python-runtime.md](../../knowledge/gotchas/pyo3-and-python-runtime.md).

- **Stream frame**: one ordered response unit carrying `request_id`, `status`, `flags` (`vertebra_core::ipc::{FLAG_STREAMING, FLAG_HEADERS_SENT, FLAG_FINAL, FLAG_TRAILERS, FLAG_ERROR}`), raw headers, and a body slice. The first frame carries `FLAG_STREAMING | FLAG_HEADERS_SENT` and the status and headers; continuation frames carry `FLAG_STREAMING`; the last carries `FLAG_STREAMING | FLAG_FINAL` (plus `FLAG_TRAILERS` when it carries trailers) or `FLAG_STREAMING | FLAG_ERROR`.
- **Unary response**: a single complete response; responses whose first body message is final keep the existing unary path and zero-copy fast paths.
- **Startup failure**: any exception raised while entering a mount's lifespan or RSGI init, including `lifespan.startup.failed` and startup timeout.
- **Packaged binary**: `vertebra/bin/vertebra-server[.exe]` inside an installed `vertebra-core` wheel.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1.** The in-process bridge carries ordered `ResponseStreamFrame`s through a dedicated stream sink that blocks until the stream accepts each frame and reports a closed client.
- **R2.** The embedded dispatcher turns the first stream frame into `PendingResponse::streaming`, forwards continuation, trailer, final, and error frames in order, and reports closed clients.
- **R3.** The SHM response rings carry flagged `VRSF` stream frames, the SHM reader routes them through the same registry with awaited (lossless) delivery, and the unreachable 16-byte `ResponseSlot` routing path (`handle_raw_response_slot`, `PendingRequests`) is removed.
- **R4.** `_vertebra.submit_stream_frame` submits one stream frame on either transport, blocks with the GIL detached while the stream is backpressured, and raises `ConnectionResetError` when the client is gone.
- **R5.** `AsgiProtocol` in direct-submit mode flushes the response start and every non-final `http.response.body`, `http.response.zerocopysend`, and `http.response.sse` chunk as stream frames; single-chunk responses stay unary; trailers ride the final frame; an exception after streaming began emits an error frame; `send` after disconnect raises `ConnectionResetError`.
- **R6.** RSGI `response_stream` transports flush each `send_bytes` and `send_str` call as a stream frame in direct-submit mode.
- **R7.** The Python fallback path (`AsgiSend` used by `AsgiRunner.spawn_single` and `_AsgiMountRunner._execute_and_submit`) streams through an injected stream submitter; the buffered batch path keeps collecting.
- **R8.** Streaming is proven end to end through the `vertebra-server` binary in embedded and SHM topologies for ASGI body chunks, ASGI SSE, and RSGI streams.
- **R9.** Lifespan startup failure, startup timeout, and RSGI init failure raise `WorkerStartupError` from `Worker.run`, unwinding already-started mounts; the embedded topology exits non-zero through the existing worker-failure path.
- **R10.** SHM workers exit with `EXIT_STARTUP_FAILURE = 78` on `WorkerStartupError`; the Rust supervisor marks such workers dead without respawn and stops the server, which exits non-zero.
- **R11.** `vertebra.supervisor` lazily exports `run_supervisor`, `build_server_command`, `find_server_binary`, `find_repo_root`, and `worker_main`.
- **R12.** Worker lanes bind a `QueueService` resolved from the CLI target or `app.state["vertebra_queue_service"]` and fail loud when none exists; `vtb workers run` runs until signalled; colocated `--with-workers` lanes bind the same service.
- **R13.** `vtb run --worker-only` runs worker lanes inside the app lifespan behind a health-probe listener on `--host`/`--port` and never launches `vertebra-server`; `--worker-only` is mutually exclusive with `--with-workers` and `--oneshot-job`.
- **R14.** `tools/scripts/build_server_binary.py` builds a relocatable `vertebra-server` for the running CPython ABI against a shared libpython and stages it at `src/py/vertebra/bin/`.
- **R15.** `build_server_command` fails loud when a packaged binary meets an interpreter without a shared libpython, and `run_supervisor` exports `PYTHONHOME` plus the platform library search path for the embedded interpreter.
- **R16.** Every core wheel ships the binary; Linux and macOS repairs exclude libpython; `release-binaries.yml` adds linux-arm64.
- **R17.** A smoke test installs the built core wheel into a clean environment and runs `vtb run` against a native route and a streaming ASGI mount, locally (`make core-wheel-check`), in CI, and across the release wheel matrix including linux-aarch64.
- **R18.** Public docs, OKF chapters, product docs, and a limitations chapter describe the delivered behaviour.
- **R19.** Framework FastPath objects are published and resolved only under `__vertebra_fastpath__` (attribute, or `state["__vertebra_fastpath__"]` for slotted apps); one `has_custom_exception_handlers` lives in `_fastpath_core.py`; Starlette and FastAPI result serialization dispatches on exact response type and returns the body without copying.
- **R20.** `fill_os_random` uses `getrandom::fill` and panics on failure, and every thread entropy pool refills after `fork()`.
- **R21.** `_zero_copy.py` binds its five native helpers directly from `vertebra._vertebra`, and `import_optional` relies on `sys.modules` with `_module_cache` removed.
- **R22.** No Spanner `batch_write` (driver or SQLSpec, sync or async) accepts `max_commit_delay_ms`.
- **R23.** `EmbeddingBroker.submit_batch` applies no fixed per-chunk sleep.

### 2.2 Concrete Contracts

```rust
// src/rs/vertebra-core/src/bridge.rs
#[derive(Debug, Clone)]
pub struct ResponseStreamFrame {
    pub request_id: u64,
    pub status: u16,
    pub flags: u16,
    pub headers_raw: bytes::Bytes,
    pub body: bytes::Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDelivery {
    Accepted,
    Closed,
}

pub type StreamSink = Arc<dyn Fn(ResponseStreamFrame) -> StreamDelivery + Send + Sync>;

impl BatchBridge {
    pub fn set_stream_sink(&self, sink: StreamSink);
    pub fn submit_stream_frame(&self, frame: ResponseStreamFrame) -> Result<StreamDelivery, TransportError>;
}

// src/rs/vertebra-core/src/transport/ring_pair.rs
pub const RESPONSE_STREAM_MAGIC: u32 = 0x56_52_53_46; // "VRSF"
impl RingPair {
    pub fn send_stream_frame(&self, correlation_id: u64, status: u16, flags: u16, headers_raw: &[u8], body: &[u8]) -> Result<(), RingPairError>;
    pub fn send_stream_frame_blocking(&self, correlation_id: u64, status: u16, flags: u16, headers_raw: &[u8], body: &[u8]) -> Result<(), RingPairError>;
    pub fn max_stream_frame_payload(&self) -> usize;
    pub fn drain_frames_with<F: FnMut(ResponseFrameView<'_>)>(&self, max_items: usize, f: F) -> Result<usize, RingPairError>;
}

// src/rs/vertebra-http/src/dispatcher/streams.rs
pub struct ResponseStreamRegistry { /* DashMap<u64, mpsc::Sender<Result<Frame<Bytes>, Infallible>>> */ }
impl ResponseStreamRegistry {
    pub fn deliver_blocking(&self, frame: ResponseStreamFrame, complete: impl FnOnce(u64, PendingResponse) -> bool) -> StreamDelivery;
    pub async fn deliver(&self, frame: ResponseStreamFrame, complete: impl FnOnce(u64, PendingResponse) -> bool) -> StreamDelivery;
    pub fn active_streams(&self) -> usize;
}

// src/rs/vertebra-py/src/transport.rs
#[pyfunction]
#[pyo3(name = "submit_stream_frame", signature = (request_id, status, flags, headers, body))]
pub fn submit_stream_frame(py: Python<'_>, request_id: u64, status: u16, flags: u16,
    headers: Option<&Bound<'_, PyAny>>, body: &Bound<'_, PyAny>) -> PyResult<()>;
impl ResponseSubmitter {
    pub fn submit_stream_frame_inner(py: Python<'_>, request_id: u64, status: u16, flags: u16,
        headers_raw: &[u8], body: &[u8]) -> PyResult<()>;
}
```

```python
# src/py/vertebra/_vertebra.pyi
STREAM_FLAG_STREAMING: Final[int]
STREAM_FLAG_FINAL: Final[int]
STREAM_FLAG_ERROR: Final[int]
STREAM_FLAG_HEADERS_SENT: Final[int]
STREAM_FLAG_TRAILERS: Final[int]
def submit_stream_frame(
    request_id: int, status: int, flags: int,
    headers: Sequence[tuple[bytes, bytes]] | None, body: Buffer, slot_capacity: int = 16384,
) -> None: ...

# src/py/vertebra/server/_asgi/_queues.py
type StreamSubmitter = Callable[[int, int, int, list[tuple[bytes, bytes]], bytes], None]
class AsgiSend:
    def __init__(self, request_id: int = 0, on_chunk: ChunkCallback | None = None,
                 stream_submitter: StreamSubmitter | None = None) -> None: ...
    @property
    def streamed(self) -> bool: ...

# src/py/vertebra/server/worker.py
class WorkerStartupError(VertebraError, RuntimeError):
    """Raised when a mount's lifespan startup or RSGI init fails before serving."""

# src/py/vertebra/supervisor/_worker.py
EXIT_STARTUP_FAILURE: Final = 78

# src/py/vertebra/server/lanes.py
class WorkerRunner:
    @staticmethod
    def resolve_queue_service(target: Any) -> QueueService: ...

# src/py/vertebra/server/platform.py
class HealthProbeServer:
    def __init__(self, zpages: ZPagesRegistry, *, host: str, port: int) -> None: ...
    @property
    def bound_port(self) -> int: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...

# src/py/vertebra/supervisor/_supervisor.py
class EmbeddedPythonRuntime(msgspec.Struct, frozen=True, gc=False):
    home: str
    library_dir: str
    library_file: str | None
    @classmethod
    def detect(cls) -> Self: ...
    def require_shared_library(self, binary: Path) -> None: ...
    def apply(self, env: dict[str, str]) -> None: ...

# tools/scripts/build_server_binary.py
class ServerBinaryTarget(msgspec.Struct, frozen=True, gc=False):
    python_version: str
    free_threaded: bool
    rust_target: str | None
    binary_name: str
class ServerBinaryBuilder: ...
```

### 2.3 Caller → Seam → Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| ASGI app `await send(...)` under `drive_asgi_batch` (embedded) | `AsgiProtocol::send` | `ResponseSubmitter::submit_stream_frame_inner` → `BatchBridge::submit_stream_frame` → `ResponseStreamRegistry::deliver_blocking` → hyper `FrameReceiver` |
| Same, SHM topology | `ResponseSubmitter::submit_stream_frame_inner` | `Transport::send_stream_frame` (`VRSF`) → `shm_reader::drain_worker` → `Dispatcher::deliver_stream_frame` → `ResponseStreamRegistry::deliver` |
| RSGI `transport.send_bytes` | `RsgiTransport::send_bytes` | `ResponseSubmitter::submit_stream_frame_inner` |
| Python fallback `AsgiRunner._execute_and_submit` | `AsgiSend.__call__` | `_vertebra.submit_stream_frame` |
| `Worker.run` | lifespan `ExitStack` | `AsgiLifespan.__enter__`, `enter_litestar_server_lifespan`, `__rsgi_init__` → `WorkerStartupError` |
| `vertebra.supervisor._worker.main` | `WorkerStartupError` handler | `sys.exit(78)` → `WorkerLifecycle::check_alive` exit code → `Supervisor::poll_workers` → shutdown token |
| `vtb run --worker-only` | `execute_worker_cli(..., health_probe=...)` | `WorkerRunner.resolve_queue_service`, `HealthProbeServer`, `WorkerRunner.run` |
| `vtb run`/`vtb dev`/`litestar run` | `build_server_command`, `run_supervisor` | `EmbeddedPythonRuntime.detect/require_shared_library/apply` |
| cibuildwheel `before-build` | `tools/scripts/build_server_binary.py` | `cargo build --bin vertebra-server`, `install_name_tool` (macOS), maturin `include` |

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Required behaviour |
| --- | --- |
| First body message has `more_body=False` | Unary path unchanged (zero-copy, `single_body_py`, content-length). |
| `more_body=True` with empty `body` | Flushes the headers frame once; empty continuation chunks are not sent. |
| Trailers expected (`trailers=True` in start) | Final frame carries `FLAG_FINAL | FLAG_TRAILERS` after `http.response.trailers`; body completion alone does not finalize. |
| App raises after first stream frame | Error frame (`FLAG_STREAMING | FLAG_ERROR`) is submitted once; body ends early; exception is logged with request id. |
| Client disconnects mid-stream | Next submit returns `Closed`; `send` raises `ConnectionResetError`; `receive` yields `http.disconnect`. |
| Per-stream queue full | Producer blocks with GIL detached until capacity (bridge) or the SHM reader awaits capacity; no frame is dropped. |
| Stream frame for unknown request id | `StreamDelivery::Closed`; logged at debug; never panics. |
| No stream sink registered on the bridge | `submit_stream_frame` returns `TransportError::Disconnected` → `RuntimeError("stream sink not registered")`. |
| Lifespan `startup.failed`, raises, or exceeds `30s` timeout | `WorkerStartupError` with the mount prefix; lifespan task cancelled; earlier mounts shut down in reverse order. |
| SHM worker exits `78` | No respawn; server cancels and exits non-zero; other exit codes keep backoff respawn. |
| `--worker-only` with `--with-workers` or `--oneshot-job` | `CLIError` exit code `2`. |
| Worker lanes with no resolvable `QueueService` | `ImproperConfigurationError` naming the target and the `vertebra_queue_service` state key. |
| Packaged binary with static-only interpreter (no `LDLIBRARY` file in `LIBDIR`) | `SystemExit` message → `CLIError` exit `2` naming the interpreter and the shared-library requirement. |
| Developer binary under `CARGO_TARGET_DIR` or `src/rs/target` | No shared-library requirement; environment still exported when a shared library exists. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Stream transport | Transport-neutral `ResponseStreamFrame` + bridge `StreamSink`, sibling `VRSF` ring frames, one `ResponseStreamRegistry` | Adding `flags` to `BridgeResponse` or `VRES` (every unary constructor and parser changes; the unary sink cannot report disconnects); reviving the unreachable 16-byte `ResponseSlot` ring path (rings, overflow regions, and `drain_with` all parse `VRES`). |
| Backpressure on a full stream | Block the producing worker thread with the GIL detached (`blocking_send`), matching the SHM `RingFull` precedent in `send_shm_response_inline_or_detached` | Unbounded channels (memory growth under slow clients); dropping frames (corrupts the body); returning a pending awaitable (requires a new waker bridge in `AsgiProtocol`). |
| SHM full-queue handling | The SHM reader awaits `ResponseStreamRegistry::deliver`, which awaits per-stream capacity | `try_send` that drops frames (silent data loss). |
| Startup failure signalling | Typed `WorkerStartupError` + exit code `78` (`EX_CONFIG`) | Re-raising the raw exception (supervisor cannot tell config failures from crashes). |
| Server binary in wheels | Per-ABI binary linked to a shared libpython, launcher exports `PYTHONHOME` and library path | Static libpython (manylinux images ship neither a shared nor a static libpython, and static embedding breaks extension symbol resolution without `--export-dynamic`); serving from inside `_vertebra` (contradicts the documented embedded `vertebra-server` topology). |
| Worker-only health probes | Minimal asyncio listener serving `ZPagesRegistry` pages | Launching `vertebra-server` with routes disabled (still embeds an HTTP app and Python worker threads). |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/18 files documented]`.

| File | Owning task | Change |
| --- | --- | --- |
| `docs/usage/server-and-workers.md` | `ch10:t01` | Streaming semantics, buffered path, lifespan fail-loud, worker-only, `QueueService` binding. |
| `docs/reference/cli.md` | `ch10:t01` | `--worker-only` behaviour and exclusivity; `vtb workers run` lifetime. |
| `docs/reference/build-and-wasm.md` | `ch10:t01` | `vertebra.supervisor` exports and `EmbeddedPythonRuntime`. |
| `docs/usage/packaging-and-deployment.md` | `ch10:t02` | Wheel-shipped binary, shared-libpython requirement, platforms. |
| `docs/getting-started/installation.md` | `ch10:t02` | Platform matrix including linux-aarch64 and interpreter requirement. |
| `docs/getting-started/cli.md` | `ch10:t02` | `vtb run` from a wheel install and `--worker-only`. |
| `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` | `ch10:t03` | Stream frames, stream sink, SHM lossless delivery. |
| `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` | `ch10:t03` | Progressive flush state machine for ASGI/RSGI/Python fallback; single `__vertebra_fastpath__` resolution. |
| `.agents/bundles/knowledge/domains/http/server-and-dispatch.md` | `ch10:t03` | `ResponseStreamRegistry`, SHM stream routing, supervisor startup-failure stop. |
| `.agents/bundles/knowledge/domains/server/worker-and-protocols.md` | `ch10:t04` | `WorkerStartupError`, exit code `78`, supervisor exports, binary runtime. |
| `.agents/bundles/knowledge/domains/server/worker-lanes.md` | `ch10:t04` | `resolve_queue_service`, worker-only, `HealthProbeServer`. |
| `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` (new) | `ch10:t04` | Caveats record, including the UUID entropy panic. |
| `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` | `ch11:t01` | Build script, maturin include, repair exclusions, smoke gate. |
| `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` | `ch11:t01` | Relocatable rpath and shared-libpython gotchas. |
| `.agents/bundles/knowledge/domains/index.md` | `ch11:t01` | List the limitations chapter. |
| `.agents/bundles/product/tech-stack.md` | `ch11:t02` | Maturin include, wheel binary, release matrix. |
| `.agents/bundles/product/product.md` | `ch11:t02` | Streaming capability and wheel-run topology statement. |
| `.agents/bundles/knowledge/index.md` | `ch11:t02` | List the limitations chapter. |

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning task(s) | Verification strategy | Canonical test / gate |
| --- | --- | --- | --- |
| R1 | `ch01:t01` | `behavior_tdd` | `src/rs/vertebra-core/src/bridge.rs` `#[cfg(test)] mod tests` |
| R2 | `ch01:t03` | `behavior_tdd` | `src/rs/vertebra-http/tests/it/streaming_body.rs` |
| R3 | `ch01:t02`, `ch01:t04`, `ch02:t01` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/shm.rs`; `streaming_body.rs`; `src/rs/vertebra-py/src/transport.rs` tests |
| R4 | `ch02:t01` | `behavior_tdd` | `transport.rs` tests; `mypy.stubtest vertebra._vertebra` |
| R5 | `ch03:t01` | `behavior_tdd` | `src/rs/vertebra-py/src/asgi.rs` tests; `src/py/tests/unit/server/test_asgi.py` |
| R6 | `ch03:t02` | `behavior_tdd` | `src/py/tests/unit/server/test_rsgi_protocol.py` |
| R7 | `ch03:t03` | `behavior_tdd` | `src/py/tests/unit/server/test_asgi.py` |
| R8 | `ch03:t04` | `integration` | `src/py/tests/integration/server/test_vertebra_server.py` |
| R9 | `ch04:t01` | `behavior_tdd` | `src/py/tests/unit/server/test_runner.py`, `test_asgi.py` |
| R10 | `ch04:t02`, `ch04:t03` | `behavior_tdd` | `src/py/tests/unit/cli/test_cli_commands.py`; `supervisor.rs` tests |
| R11 | `ch05:t01` | `behavior_tdd` | `test_cli_commands.py` |
| R12 | `ch05:t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_worker_lanes.py`, `src/py/tests/unit/cli/test_run_with_workers.py` |
| R13 | `ch05:t03` | `behavior_tdd` | `test_run_with_workers.py` |
| R14 | `ch06:t01` | `behavior_tdd` | `src/py/tests/unit/build/test_packaging_and_pgo.py` |
| R15 | `ch06:t02` | `behavior_tdd` | `test_cli_commands.py` |
| R16 | `ch07:t01`, `ch07:t03` | `static_validation` | `make core-wheel-check`; release matrix |
| R17 | `ch07:t02`, `ch07:t03` | `integration` | `tools/scripts/smoke_vtb_run.py` via `make core-wheel-check` and CI |
| R19 | `ch08:t01`, `ch08:t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_litestar_plugin.py`, `test_fastapi_plugin.py`, `test_starlette_plugin.py` |
| R20 | `ch09:t01` | `behavior_tdd` | `src/rs/vertebra-core/src/id.rs` `#[cfg(test)] mod tests` |
| R21 | `ch09:t02`, `ch09:t03` | `behavior_tdd` | `src/py/tests/unit/db/test_vector_wire_codecs.py`, `src/py/tests/unit/framework/test_typing.py`; `mypy.stubtest vertebra._vertebra` |
| R22 | `ch09:t04` | `behavior_tdd` | `src/py/tests/unit/db/test_spanner.py`, `src/py/tests/unit/extensions/test_sqlspec_spanner.py` |
| R23 | `ch09:t02` | `behavior_tdd` | `src/py/tests/unit/embeddings/test_embedding_broker.py` |
| R18 | `ch10:t01`–`ch10:t04`, `ch11:t01`, `ch11:t02` | `documentation_validation` | `make docs-check`, `make review-check` |
| Release gate | `ch11:t03` | `static_validation` | Flow recovery checkpoint below |

## 5. Implementation Plan

- [ ] ch01-bridge-stream-frames — Bridge stream frames, VRSF ring frames, shared stream registry, and lossless SHM routing ([spec](chapters/ch01-bridge-stream-frames/spec.md))
- [ ] ch02-python-stream-submission — `submit_stream_frame` binding for both transports ([spec](chapters/ch02-python-stream-submission/spec.md))
- [ ] ch03-asgi-rsgi-incremental-flush — Progressive ASGI/RSGI/Python fallback flush and server-binary streaming acceptance ([spec](chapters/ch03-asgi-rsgi-incremental-flush/spec.md))
- [ ] ch04-lifespan-fail-loud — `WorkerStartupError`, exit code `78`, and supervisor stop ([spec](chapters/ch04-lifespan-fail-loud/spec.md))
- [ ] ch05-supervisor-exports-and-worker-lanes — Supervisor exports, `QueueService` binding, worker-only mode ([spec](chapters/ch05-supervisor-exports-and-worker-lanes/spec.md))
- [ ] ch06-relocatable-server-binary — Per-ABI build script and launcher runtime resolution ([spec](chapters/ch06-relocatable-server-binary/spec.md))
- [ ] ch07-wheel-packaging-and-smoke — Core wheel binary, `vtb run` smoke, CI and release matrix ([spec](chapters/ch07-wheel-packaging-and-smoke/spec.md))
- [ ] ch08-fastpath-cleanup — Single FastPath attribute, shared handler check, and exact-type response serialization ([spec](chapters/ch08-fastpath-cleanup/spec.md))
- [ ] ch09-runtime-native-cleanup — Fail-loud UUID entropy, direct native bindings, module cache removal, Spanner `batch_write` signature, and broker latency ([spec](chapters/ch09-runtime-native-cleanup/spec.md))
- [ ] ch10-docs-and-okf-sync — User docs and runtime OKF chapters with limitations ([spec](chapters/ch10-docs-and-okf-sync/spec.md))
- [ ] ch11-product-okf-and-release-gate — Packaging OKF, product docs, indexes, and final gates ([spec](chapters/ch11-product-okf-and-release-gate/spec.md))

**Flow Recovery Checkpoint:** `make lint-all && make rs-test && make py-test && make core-wheel-check && make docs-check && make review-check && make review-route BASE=c8722c76` exits `0`, followed by a correctness review and a fresh quality review over `c8722c76..HEAD` before `/flow:finish server-runtime-correctness`.

## 6. Current State & Remaining Plan Snapshot

- **Plan Baseline**: `state: planned`, `plan_revision: 2`, `plan_commit: null` (stamped on approval), authored against `main @ c8722c76`.
- **Upstream Dependencies**: none. `mcp-protocol-completeness` depends on `ch01`–`ch03` of this flow for incremental mounted-ASGI streaming.
- **Next Task**: `server-runtime-correctness:ch01:t01` once approved; `0/33` tasks completed.
- **Documentation & OKF Status**: `[0/18 files documented]` across `ch10` and `ch11`.
- **Next Command**: `/flow:implement server-runtime-correctness ch01` after approval.

[^asgi-protocol-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — Rust ASGI protocol that buffers `more_body` chunks into `body_buf` until the final chunk.
[^bridge-core-rs]: `src/rs/vertebra-core/src/bridge.rs::BatchBridge` — In-process batch bridge with unary-only `BridgeResponse` and `ResponseSink`.
[^bridge-http-rs]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded-topology correlation table that completes every response with `stream_rx: None`.
[^shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::PendingRequests` — Unreachable `ResponseSlot` stream responder map that drops frames with `try_send`.
[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::submit_raw_response` — Raw response submission whose SHM branch writes streaming chunks as unary frames.
[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — Worker run loop that logs and swallows ASGI lifespan startup failures.
[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::find_server_binary` — Server binary resolution expecting `vertebra/bin/vertebra-server` in the installed package.
[^run-cli-py]: `src/py/vertebra/cli/run.py::run` — `vtb run` command that sets `VERTEBRA_WORKER_ONLY` without any reader.
[^release-yaml]: `.github/workflows/release.yaml` — Release wheel matrix without a server binary in `vertebra-core`.
[^fastpath-attach-py]: `src/py/vertebra/server/_fastpath.py::attach_framework_fastpath` — FastPath resolver that probes four alias locations.
[^id-rs]: `src/rs/vertebra-core/src/id.rs::fill_os_random` — UUID entropy source with a time-seeded fallback and no fork detection.
