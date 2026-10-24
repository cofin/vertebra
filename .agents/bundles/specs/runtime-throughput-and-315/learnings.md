---
type: Learnings
flow_id: runtime-throughput-and-315
created_at: 2026-10-24T17:10:25Z
updated_at: 2026-10-24T19:29:00Z
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
