---
type: Learnings
flow_id: runtime-throughput-and-315
created_at: 2026-10-24T17:10:25Z
updated_at: 2026-10-24T22:13:00Z
---

# Learnings — Runtime Throughput, Multi-Core Scaling, and Python 3.15 Optimizations

## Phase 1 (`c68065a`)

- Custom subagents under `.agents/agents/*.md` validate tool lists against the live subagent tool registry; keep lazy MCP multiplexer tools out of `areas.toml` and agent frontmatter (`df36a30`).
- `vertebra.cli.run` must preserve inherited `VERTEBRA_WORKERS` / `VERTEBRA_THREADS` environment variables when CLI options are omitted, and `supervisor._worker` must skip `App().mount("/", target)` when `--asgi` loads an existing `App` instance (`c68065a`).

## Phase 2 (`1ac8b83`)

- `BatchBridge` bounded `crossbeam::bounded(4)` / `tokio::sync::mpsc::channel(4)` queues deadlock whenever more than 5 batches are in flight (`c >= 16` on 1 core, `c >= 32` on 8 cores, or async `io10`); unbounded queues paired with ingress admission control, `OnceLock<ResponseSink>`, `InProcessRequest`-backed `BatchSnapshot`s, and a Dekker `SeqCst` request `Notifier` (`consumer_sleeping`, `consumer_count <= 1` set from `spawn_python_workers` `worker_threads` rather than `config.thread_count`) eliminate the deadlock, fixed 1 MiB `DataSlab` cap, and second Python thread hop for Embedded `ASGI`/`RSGI`/`async` workers (`b74ded5`).
- `ShmHeader` `ABI_VERSION = 3` adds cache-line-isolated `consumer_sleeping` and `active_flags` atomics enabling `SeqCst` Dekker `eventfd` coalescing (`0` `write()` syscalls while peers are polling), generation-synchronized SPSC cached heads, `CacheLine<AtomicU64>`-padded MPMC sequence slots, and zero-copy `MpmcSlotView`/`SlotView`-backed `BatchSnapshot`s (`1ac8b83`).

## Phase 3 (`3ce98ae`)

- Single-word `AtomicU64` circuit-breaker state + sharded atomic sliding window + opportunistic non-lingering `RequestBatcher` + raw compact binary header pass-through eliminate per-request mutexes, `Instant::now()` syscalls, discarded `MSG_PEEK` calls, and MessagePack transcoding in `vertebra-http` (`dbc19f5`).
- Inside the Jetski terminal sandbox (`uname().nodename == "sandbox"`), `setsockopt(SO_REUSEPORT, 1)` and dual `bind()` succeed while gVisor's user-mode netstack rejects a second `listen()` on the same loopback port with `EADDRINUSE`; live multi-worker `SO_REUSEPORT` benchmarks must run outside the sandbox (`dbc19f5`).
- Replacing `#[tokio::main]` with an explicit `current_thread` / `multi_thread` Tokio builder clamped to `available_parallelism()`, adding `EmbeddedSupervisor` `SO_REUSEPORT` multi-process supervision, gating `BatchBridge::init` to `Topology::Embedded`, encoding `MOUNT_HANDLER_BASE | mount_idx` across all verbs including `OPTIONS`/`HEAD`, and removing `Mutex<ShmBatchScratch>` from `ShmDispatcher` remove multi-core scaling bottlenecks (`3ce98ae`).

## Phase 4 (`00d0e7a`)

- CPython 3.12+ synthesizes PEP 688 `__buffer__` on any `#[pyclass]` implementing `__getbuffer__`; declaring `def __buffer__(self, flags: int, /) -> memoryview: ...` in `.pyi` stubs satisfies both static `mypy` `Buffer` checks and `mypy.stubtest` while `PyBuffer_FillInfo(view, slf.as_ptr(), ...)` anchors zero-copy `Request.body_view` directly to `Arc<BatchSnapshot>` (`7742f8b`).
- Both `asyncio.BaseEventLoop.create_task` and `uvloop.Loop.create_task` accept a PyO3 `_SteppedCoroutine` implementing `__await__`, `__iter__`, `__next__`, `send`, `throw`, and `close`, letting `drive_asgi_batch` and `drive_rsgi_batch` finish non-suspending unary handlers in one `PyIter_Send` call (`0` `asyncio.Task` allocations) and promote suspending coroutines without a Python wrapper frame (`00d0e7a`).

## Phase 5 (`5f4f421`)

- Executing default sync native routes inline in the vectorized batch loop (`submit_batch_raw`) while reserving thread-pool offload for explicit `sync_to_thread=True`, releasing `Request` (`Arc<BatchSnapshot>`) before `await` in `_await_async_item`, and running single-loop SHM workers inline via `loop.add_reader(notify_fd, ...)` with Dekker `prepare_request_sleep` / `cancel_request_sleep` eliminate GIL thrashing, `BufferPool` starvation, and cross-thread wakeups (`e4ae09d`).
- `LitestarFastPathBypass.dispatch` compiles pure-signature sync/async Litestar handlers into `_DirectSignaturePlan` entries for zero-`Request` execution and delegates DI/DTO/`File`/SSE routes to `dispatch_litestar_handler_direct`, while `LitestarPlugin` guards `app_config.compression_config = None` behind `VERTEBRA_SERVER_ACTIVE=1` / `VERTEBRA_RUST_OFFLOAD=1` and exports `app.state["vertebra_offload_config_json"]` for `vertebra-http` (`5f4f421`).
- `Makefile` sets `CARGO_TARGET_DIR ?= /tmp/vtb-cargo-target`, so `vertebra.supervisor._supervisor.find_server_binary()` must honor `CARGO_TARGET_DIR` and compare `st_mtime_ns` across workspace target directories (`CARGO_TARGET_DIR`, `target`, `src/rs/target`, `/tmp/vtb-cargo-target`) to avoid launching stale `src/rs/target/release/vertebra-server` binaries (`5f4f421`).

## [2026-10-24] Code Review (`cf5979e..a115466`)

- **Worker batch fastpath dispatch (`src/py/vertebra/server/worker.py`)**: `Worker._process_batch` must pass `single_root_asgi.dispatch_app` (not `single_root_asgi._app`) to `drive_asgi_batch` so root-mounted ASGI apps carrying `__vertebra_fastpath__` execute `LitestarFastPathBypass`.
- **Wheel build matrix (`.github/workflows/release.yaml`)**: `pypa/cibuildwheel@v4.3.4` builds CPython `3.15` (`cp315-*`) and free-threaded `3.14t`/`3.15t` (`cp314t-*`, `cp315t-*`) wheels natively via `CIBW_BUILD` without deprecated `CIBW_ENABLE`.
- **SHM response tier active-flag ordering (`src/rs/vertebra-core/src/transport/ring_pair.rs`)**: `TieredRingSet` must set `mark_tier_active(bit)` before publishing `write_head` (clearing if empty on send error) and check `ring.has_data_acquire()` when `(mask & bit) == 0` so consumers never spin between `write_head` publication and `active_flags` update.
- **Admission wakeup fence & Vary/media-type offloads (`src/rs/vertebra-http/src/{backpressure,cors,compression,response,batcher}.rs`)**: `AdmissionPermit::drop` and `BackpressureController::try_acquire` require a `SeqCst` fence between `in_flight` and `queue_depth` / `notified.enable()` to close Store-Load lost-wakeup windows; CORS and compression offloads must append `Vary` tokens without clobbering existing `Vary` headers, skip incompressible `Content-Type` responses in `compress_if_eligible`, and align `batcher::http_version_tag` with `response::version_tag`.
- **Litestar plugin lifecycle & fastpath parity (`src/py/vertebra/extensions/litestar/{plugin,fastpath,offload}.py`, `src/rs/vertebra-http/src/main.rs`)**: Real Litestar `AppConfig` has no `on_app_init` list attribute; `LitestarPlugin` must expose an idempotent `ensure_finalized(app)` invoked from `server_lifespan`, `AsgiRunner`, `LitestarFastPathBypass.dispatch`, and `vertebra-http`'s `extract_offload_json_from_app`. `LitestarFastPathBypass` must pre-seed `ScopeState.body` (guarding Litestar's `Empty` sentinel) and replay consumed `receive()` chunks on fallback, serialize `HTTPException`-like status/detail responses (`4xx`/`5xx`) and disable bypass when `after_exception`, `csrf_config`, `allowed_hosts`, or active Python `compression_config`/`cors_config` are configured, emit `http.response.start` before `http.response.pathsend` and stream SSE via `http.response.body` chunks, close `UploadFile` instances in `finally`, ignore Litestar's built-in default `HTTPException` map when checking custom exception handlers, avoid v3 `@deprecated` `resolve_*` warnings on finalized handlers, and delegate handlers with `response_headers`/`cookies`/`etag`/`background` or exceeded `request_max_body_size` accurately.

## Task 5.3 Closure (`071e5b6`)

- All review findings from `cf5979e..a115466` are remediated and covered by regression tests across `shm.rs`, `ingress_fastpath.rs`, `test_litestar_plugin.py`, `test_litestar_direct_fastpath.py`, `test_litestar_rust_offload.py`, `test_runner.py`, and `test_packaging_and_pgo.py` (`make rs-test`, `make py-test` 858 passed, `make ts-test` 52 passed, `make coverage` 81%, `make lint-all`, and `make review-check` all exit `0`).
- Live benchmarks on `071e5b6` confirm `0` non-2xx errors and `0` deadlocks across all topologies (`c=1..256+`, including `io10 c=256`), with Vertebra #1 across every single-core (`taskset -c 0`) and multi-core (`taskset -c 0-7`, `1w`/`4w`/`8w`) category against Granian `2.8.4` (`390,564 req/s` on Python 3.12 and `391,789 req/s` on CPython `3.15.0rc3` `PYTHON_JIT=1` at `8w hello c=256`, plus `204,331 req/s` on free-threaded `3.15.0rc3+freethreaded` single-process `4t`).
