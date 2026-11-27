---
type: Guide
title: "Server Platform & Process Lifecycle"
description: "Python-side runtime around vertebra-server: the launcher, binary discovery, worker loop and uvloop inline polling, free-threaded loop sharding, OTel loop telemetry, sync pools, and /taskz platform diagnostics."
status: stable
tags:
  - architecture
  - runtime
  - free-threaded
  - platform
  - process-lifecycle
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-27T22:10:00Z"
    commit: f3b624a2
  - by: agent:flow-executor
    at: "2026-11-19T00:15:00Z"
    commit: 6f76f8a7
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-supervisor-supervisor-py
    resource: src/py/vertebra/supervisor/_supervisor.py
    title: "Python implementation module (src/py/vertebra/supervisor/_supervisor.py)"
  - id: src-cli-run-py
    resource: src/py/vertebra/cli/run.py
    title: "Python implementation module (src/py/vertebra/cli/run.py)"
  - id: src-cli-launch-py
    resource: src/py/vertebra/cli/_launch.py
    title: "Python implementation module (src/py/vertebra/cli/_launch.py)"
  - id: src-cli-dev-py
    resource: src/py/vertebra/cli/dev.py
    title: "Python implementation module (src/py/vertebra/cli/dev.py)"
  - id: src-server-worker-py
    resource: src/py/vertebra/server/worker.py
    title: "Python implementation module (src/py/vertebra/server/worker.py)"
  - id: src-server-dispatch-py
    resource: src/py/vertebra/server/_dispatch.py
    title: "Python implementation module (src/py/vertebra/server/_dispatch.py)"
  - id: src-server-mounts-py
    resource: src/py/vertebra/server/_mounts.py
    title: "Lazy import-string mount specification and resolution (src/py/vertebra/server/_mounts.py)"
  - id: src-server-loops-py
    resource: src/py/vertebra/server/_loops.py
    title: "Python implementation module (src/py/vertebra/server/_loops.py)"
  - id: src-server-lanes-py
    resource: src/py/vertebra/server/lanes.py
    title: "Python implementation module (src/py/vertebra/server/lanes.py)"
  - id: src-server-gil-audit-py
    resource: src/py/vertebra/server/_gil_audit.py
    title: "Python implementation module (src/py/vertebra/server/_gil_audit.py)"
  - id: src-server-platform-py
    resource: src/py/vertebra/server/platform.py
    title: "Python implementation module (src/py/vertebra/server/platform.py)"
  - id: src-telemetry-otel-py
    resource: src/py/vertebra/telemetry/otel.py
    title: "Python implementation module (src/py/vertebra/telemetry/otel.py)"
  - id: src-utils-sync-tools-py
    resource: src/py/vertebra/utils/sync_tools.py
    title: "Python implementation module (src/py/vertebra/utils/sync_tools.py)"
  - id: src-vertebra-http-src-health-rs
    resource: src/rs/vertebra-http/src/health.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/health.rs)"
  - id: src-vertebra-http-src-response-rs
    resource: src/rs/vertebra-http/src/response.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/response.rs)"
---

# Server Platform & Process Lifecycle

<!-- truth: start -->
- `run_supervisor` launches `vertebra-server` resolved by `find_server_binary`, applies `EmbeddedPythonRuntime` metadata, forwards `SIGTERM` to the child process, exports `VERTEBRA_*` env config, and enforces graceful drain and `SIGTERM` timeouts.
- `create_event_loop()` attaches `_eager_task_factory` (eager `asyncio.Task` start that defers `anyio` `_run_coro` wrappers) across every resolved `LoopBackend` (`uvloop`, `winloop`, `zuvloop`, `rloop`, `asyncio`); `sync_tools.run_()` delegates blocking loop creation to it.
- `EventLoopPool` manages 1 inline/background shard on GIL builds and `N` shards on free-threaded Python, names shard tasks `vertebra.loop.shard.{idx}.task.{seq}`, and drains pending tasks plus async generators on `stop()`.
- `EventLoopMetricSample` and `EventLoopRecorder` (`vertebra.telemetry`) record per-shard scheduling lag, active-task counts, Python 3.14+ `zuvloop` callbacks, and optional OpenTelemetry metrics.
- `WorkerRunner` names background tasks `vertebra.worker.runner` and `vertebra.worker.lane.{lane}.{seq}`; `ZPagesRegistry` and `HealthProbeServer` serve `/taskz` and `/debug/zpages/tasks` via Python 3.14+ `asyncio.capture_call_graph` (with Python 3.12–3.13 stack fallbacks).
<!-- truth: end -->

This chapter covers the Python runtime that launches `vertebra-server` and executes inside workers. See [SHM Worker Spawn & Shutdown](shm-supervision-and-recovery.md) and [Runtime Topologies](topologies-and-embedding.md) for Rust supervision and topology selection.

## Launcher (`vertebra.supervisor._supervisor`)

[`run_supervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py)[^src-supervisor-supervisor-py] starts `vertebra-server` via `subprocess.Popen` and returns its exit code (multi-process `SO_REUSEPORT` and SHM worker supervision happen inside `vertebra-server`).

- **Environment, Mounts & TUI**: `vtb run` and `vtb dev` populate `VERTEBRA_*` variables via `ServerLaunchEnv` (preserving pre-existing env vars when flags are omitted, including repeatable `--mount PREFIX=MODULE:ATTR[@PROTOCOL]` and `[tool.vertebra.mounts]` exported as `VERTEBRA_MOUNTS` / `VERTEBRA_BASE_APP`) and invoke `run_supervisor`. When stderr is a TTY, `vtb dev` passes `tui=True` to pipe output into `_run_with_tui`.
- **Binary Discovery**: `find_server_binary` checks `VERTEBRA_SERVER_BINARY`, then `CARGO_TARGET_DIR/{release,debug}/vertebra-server[.exe]`, then packaged `vertebra/bin/vertebra-server[.exe]`, and finally workspace `target/` and `src/rs/target/` directories (preferring `release` over `debug` and choosing the newest binary by `st_mtime_ns` within the profile).
- **Runtime & Shutdown Timeouts**: Applies `EmbeddedPythonRuntime.detect()` (`PYTHONHOME`, platform dynamic loader path, and POSIX shared `libpython` validation), exports `VERTEBRA_DRAIN_TIMEOUT_MS` and `VERTEBRA_SHUTDOWN_TIMEOUT_MS`, installs a scoped `SIGTERM` forwarder on the main thread, and on `KeyboardInterrupt` waits `shutdown_timeout_ms`, sends `SIGTERM`, and waits 5 s more.

## Worker Loop Lifecycle (`worker.py`)

[`run_worker`](../../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] executes inside each embedded Python worker thread or SHM subprocess:

1. Validates `SHMAllocationPolicy`, detects `CgroupQuota`, runs `GilAuditReport.audit()`, and applies optional Linux CPU affinity (`VERTEBRA_PIN_AFFINITY=1`/`true`) via `configure_worker_affinity` (`os.sched_setaffinity`). `Worker.handle_sigterm` enters lame-duck draining mode (`_lame_duck.enter_lame_duck()`).
2. Initializes transport (`init_transport(mode="shm")` when `VERTEBRA_TRANSPORT_MODE=shm`, else `init_transport()`).
3. Compiles handler specs (`sync_inline`, `sync_thread`, or `async_native`), resolves lazy import-string mounts via [`resolve_mount_target`](../../../../../src/py/vertebra/server/_mounts.py)[^src-server-mounts-py] (raising `WorkerStartupError(prefix, "import")` on failure and running `ensure_vertebra_integration`), enters `enter_litestar_server_lifespan` and `AsgiLifespan` for ASGI mounts (`WorkerStartupError(prefix, "lifespan")`), and instantiates mount runners (`WorkerStartupError(prefix, "rsgi_init")` on `__rsgi_init__` failure).
4. Picks the loop mode:
   - Single-shard async/ASGI/RSGI/co-located-worker mode with a request `notify_fd >= 0`: runs `_run_inline_event_loop` splitting `_on_notify_readable` on `loop.add_reader(notify_fd, ...)` and `_drain_batches` (up to `_INLINE_DRAIN_BUDGET = 16` batches per turn with a 1-turn `call_soon` grace check before Dekker `prepare_request_sleep` / `cancel_request_sleep`).
   - Multi-loop free-threaded pools or platforms without a `notify_fd` (such as Windows): polls `get_batch` and dispatches async work via `EventLoopPool` (`spawn_batch` / `drive_*_batch`).
   - Pure-sync (`all_sync_native`) and mixed sync/async (`native_only`) native routes share a single-pass dispatch path flushing sync replies via `submit_batch_raw` and stepping async replies via `drive_native_batch`; single-root ASGI/RSGI/WSGI mounts call `drive_asgi_batch`, `drive_rsgi_batch`, or `drive_wsgi_batch` directly.
5. On exit, stops co-located `WorkerRunner`, closes `lifespan_stack` (`AsgiLifespan` shutdown), shuts down the per-worker sync `executor` and cached `shutdown_worker_executor(wait=True)`, calls `shutdown_wsgi_executor()`, and calls `loop_pool.stop()`.

## Event Loops, Telemetry & Free-Threaded Sharding

- **Loop Resolution (`_loops.py`)**: [`resolve_loop_backend`](../../../../../src/py/vertebra/server/_loops.py)[^src-server-loops-py] evaluates `_LOOP_BACKEND_SPECS` (`winloop` on `win32`, `uvloop` on POSIX, `zuvloop` on all platforms, `rloop` on POSIX, fallback `asyncio`), honoring `--loop`/`VERTEBRA_LOOP` and skipping `quarantined_modules` (extensions that re-enabled the GIL under free-threaded Python). `create_event_loop()` calls `new_event_loop()` directly and attaches `_eager_task_factory`.
- **GIL Auditing (`_gil_audit.py`)**: [`is_free_threaded()` and `GilAuditReport`](../../../../../src/py/vertebra/server/_gil_audit.py)[^src-server-gil-audit-py] compare GIL state across imports, flag newly imported native extensions as suspects if the GIL re-enabled, and raise `RuntimeError` under `VERTEBRA_STRICT_FREE_THREADING=1`.
- **`EventLoopPool` & Call-Graph Snapshots**: Runs inline on the caller thread when `size == 1` (`inline_single_loop=True`), or spawns daemon threads `vertebra-event-loop-{i}` coalescing batch wakeups via one `call_soon_threadsafe` per target loop. `stop()` cancels pending tasks and calls `_drain_loop()` (`shutdown_asyncgens`). `capture_loop_task_snapshots()` and `format_task_call_graphs()` use Python 3.14+ `asyncio.capture_call_graph` with `Task.get_stack(limit=8)` fallback on 3.12–3.13. [`WorkerRunner`](../../../../../src/py/vertebra/server/lanes.py)[^src-server-lanes-py] names lane tasks `vertebra.worker.lane.{lane}.{seq}`.
- **Loop Telemetry (`telemetry/otel.py`)**: [`EventLoopMetricSample` and `EventLoopRecorder`](../../../../../src/py/vertebra/telemetry/otel.py)[^src-telemetry-otel-py] track lag histograms, active-task snapshots, `zuvloop` telemetry callbacks, and optional `opentelemetry.metrics` instruments (`vertebra.loop.lag_ms`, `vertebra.loop.active_tasks`).
- **Sharded ASGI Mounts**: When `EventLoopPool` has multiple shards, `_instantiate_mount_runners` creates `_AsgiMountRunner`, wrapping one `AsgiRunner` (bound to `pool.primary_loop`) and dispatching across shards via `pool.schedule_batch` / `pool.next_loop()`. Buffered batch calls wait up to `VERTEBRA_ASGI_BATCH_TIMEOUT` (default 30 s, `0`/`none` disables), returning `504` and cancelling timed-out tasks.

## Sync Pools & Loop-Affine Resources (`utils/sync_tools.py`)

- **`run_()`**: Delegates to `PortalProvider.default().call(...)` when a loop is already running on the thread; otherwise calls `asyncio.run(..., loop_factory=create_event_loop)` [^src-utils-sync-tools-py].
- **`SyncPool`**: Lazy `ThreadPoolExecutor` (`cpu_count * 2` default) with per-loop `_AsyncCompletionQueue` wakeups coalesced via `loop.call_soon_threadsafe(self.drain)`, `contextvars` propagation, and post-`fork` recreation. `async_()` uses `SyncPool.default()` instead of `asyncio.to_thread` when the GIL is disabled.
- **`SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]`**: Caches one resource per event loop in a `WeakKeyDictionary`.

## Platform Helpers (`platform.py`)

- **`CgroupQuota.detect()`**: Reads cgroup v2 `cpu.max` and `memory.max` (falling back to `os.cpu_count()`) to compute `effective_workers` and `memory_max_bytes`; `CgroupQuota.effective_cpu_count()` combines `os.sched_getaffinity(0)` with the cgroup quota [^src-server-platform-py].
- **`LameDuckController`**: `enter_lame_duck()` flips `readiness_status()` from `(200, {"status": "ok"})` to `(503, {"status": "lame_duck", ...})`.
- **`ZPagesRegistry` & `HealthProbeServer`**: Serves `/healthz`, `/statusz`, `/varz`, `/rpcz`, `/brokerz`, and `/taskz` (`/debug/zpages/tasks`) over async HTTP/1.1, and discovers `vertebra.platform_adapters` entry points (`PlatformAdapter`). The Rust `HealthEndpoint` ([`response.rs`](../../../../../src/rs/vertebra-http/src/response.rs)[^src-vertebra-http-src-response-rs], [`health.rs`](../../../../../src/rs/vertebra-http/src/health.rs)[^src-vertebra-http-src-health-rs]) intercepts `/healthz`, `/livez`, `/__vertebra/health`, and `/readyz` separately in Rust.
- **`SHMAllocationPolicy`**: Allocates anonymous SHM FDs via `os.memfd_create` (or duplicated tmpfile fallback) and passes FDs across `AF_UNIX` sockets via `SCM_RIGHTS`.

Provenance sources: [^src-cli-run-py], [^src-cli-launch-py], [^src-cli-dev-py], [^src-server-dispatch-py], [^src-utils-sync-tools-py].

[^src-supervisor-supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py` — Python implementation module (src/py/vertebra/supervisor/_supervisor.py).
[^src-cli-run-py]: `src/py/vertebra/cli/run.py` — Python implementation module (src/py/vertebra/cli/run.py).
[^src-cli-launch-py]: `src/py/vertebra/cli/_launch.py` — Python implementation module (src/py/vertebra/cli/_launch.py).
[^src-cli-dev-py]: `src/py/vertebra/cli/dev.py` — Python implementation module (src/py/vertebra/cli/dev.py).
[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Python implementation module (src/py/vertebra/server/worker.py).
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Python implementation module (src/py/vertebra/server/_dispatch.py).
[^src-server-mounts-py]: `src/py/vertebra/server/_mounts.py` — Lazy import-string mount specification and resolution (src/py/vertebra/server/_mounts.py).
[^src-server-loops-py]: `src/py/vertebra/server/_loops.py` — Python implementation module (src/py/vertebra/server/_loops.py).
[^src-server-lanes-py]: `src/py/vertebra/server/lanes.py` — Python implementation module (src/py/vertebra/server/lanes.py).
[^src-server-gil-audit-py]: `src/py/vertebra/server/_gil_audit.py` — Python implementation module (src/py/vertebra/server/_gil_audit.py).
[^src-server-platform-py]: `src/py/vertebra/server/platform.py` — Python implementation module (src/py/vertebra/server/platform.py).
[^src-telemetry-otel-py]: `src/py/vertebra/telemetry/otel.py` — Python implementation module (src/py/vertebra/telemetry/otel.py).
[^src-utils-sync-tools-py]: `src/py/vertebra/utils/sync_tools.py` — Python implementation module (src/py/vertebra/utils/sync_tools.py).
[^src-vertebra-http-src-health-rs]: `src/rs/vertebra-http/src/health.rs` — Rust implementation module (src/rs/vertebra-http/src/health.rs).
[^src-vertebra-http-src-response-rs]: `src/rs/vertebra-http/src/response.rs` — Rust implementation module (src/rs/vertebra-http/src/response.rs).
