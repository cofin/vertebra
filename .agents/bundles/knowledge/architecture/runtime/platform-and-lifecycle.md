---
type: Guide
title: "Server Platform & Process Lifecycle"
description: "Python-side runtime around vertebra-server: the launcher, binary discovery, worker loop and uvloop inline polling, 3.14t/3.15t loop sharding, sync pools, and platform helpers."
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
  - id: src-server-loops-py
    resource: src/py/vertebra/server/_loops.py
    title: "Python implementation module (src/py/vertebra/server/_loops.py)"
  - id: src-server-gil-audit-py
    resource: src/py/vertebra/server/_gil_audit.py
    title: "Python implementation module (src/py/vertebra/server/_gil_audit.py)"
  - id: src-server-platform-py
    resource: src/py/vertebra/server/platform.py
    title: "Python implementation module (src/py/vertebra/server/platform.py)"
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
- `run_supervisor` selects the newest `vertebra-server` binary (`st_mtime_ns` across Cargo target directories), exports `VERTEBRA_*` launch configuration, and enforces graceful drain and `SIGTERM` shutdown timeouts.
- `EventLoopPool` manages `uvloop`/`winloop`/`zuvloop`/`rloop`/`asyncio` loop shards (`1` on GIL builds, `N` on `3.14t`/`3.15t`) and drains all tasks and async generators on `stop()`.
<!-- truth: end -->

This chapter covers the Python code that launches the Rust binary and runs inside its workers. Rust-side spawning of SHM workers is in [SHM Worker Spawn & Shutdown](shm-supervision-and-recovery.md); topology selection is in [Runtime Topologies](topologies-and-embedding.md).

## Launcher (`vertebra.supervisor._supervisor`)

[`run_supervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py)[^src-supervisor-supervisor-py] starts the `vertebra-server` binary with `subprocess.Popen` and returns its exit code. Multi-process `SO_REUSEPORT` embedded supervision and SHM worker supervision are performed inside `vertebra-server` itself.

- `vtb run` and `vtb dev` build the environment (`VERTEBRA_APP`, `VERTEBRA_HOST`, `VERTEBRA_PORT`, `VERTEBRA_WORKERS`, `VERTEBRA_THREADS`, `VERTEBRA_TOPOLOGY`, `VERTEBRA_INTERFACE`, `VERTEBRA_NATIVE`, `VERTEBRA_SERVER_ACTIVE=1`, optional `PYTHON_JIT=1`, TLS paths, `VERTEBRA_STATIC_DIR`), preserving pre-existing `VERTEBRA_*` env vars when CLI flags are omitted, and call `run_supervisor(server_cmd=build_server_command(), env=...)`. `vtb dev` passes `tui=True` when stderr is a TTY, which pipes stderr into a Rich live display (`_run_with_tui`).
- `find_server_binary` resolves `VERTEBRA_SERVER_BINARY`, then the packaged `vertebra/bin/vertebra-server[.exe]`, and then selects the freshest built binary by `st_mtime_ns` across `CARGO_TARGET_DIR`, `target/{release,debug}`, `src/rs/target/{release,debug}`, and `/tmp/vtb-cargo-target` so `vtb run` never launches a stale workspace binary after `make build`.
- On `KeyboardInterrupt` it waits `shutdown_timeout_ms` for the child, then sends `SIGTERM` and waits 5 s more.
- `run_supervisor` exports `VERTEBRA_DRAIN_TIMEOUT_MS` and `VERTEBRA_SHUTDOWN_TIMEOUT_MS` (`--drain-timeout`, `--shutdown-timeout`).

## Worker Loop Lifecycle (`worker.py`)

[`run_worker`](../../../../../src/py/vertebra/server/worker.py)[^src-server-worker-py] runs in each embedded Python worker thread or SHM subprocess:

1. Installs `SIGTERM`/`SIGINT` handlers (main thread only) that stop the `Worker` cleanly at the next batch boundary.
2. Initializes the transport (`init_transport(mode="shm")` when `VERTEBRA_TRANSPORT_MODE=shm`; otherwise `init_transport()`, a no-op when the bridge already exists).
3. Compiles handler dispatch specs (`sync_inline` by default, `sync_thread` when `sync_to_thread=True`, or `async_native`) and `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`) mount bindings, entering any Litestar CLI-plugin `server_lifespan` contexts (`VertebraPlugin.ensure_finalized`) and starting `AsgiLifespan` (`lifespan.startup`) for mounted ASGI apps (`scope["state"]`).
4. Calls `configure_worker_affinity(VERTEBRA_WORKER_ID, enable_pinning=VERTEBRA_CPU_AFFINITY in {1,true,TRUE})`, which pins to `worker_index % os.cpu_count()` via `os.sched_setaffinity` on Linux.
5. Calls `GilAuditReport.audit()`.
6. Selects the execution loop: when `scheduler.size == 1` and a request `notify_fd` is available (`RingPair` in `shm` or single-consumer `BatchBridge` in `embedded` with async/ASGI/RSGI routes), `_run_inline_event_loop` creates a single `uvloop` (or `asyncio`) loop on the worker thread and registers `loop.add_reader(notify_fd, ...)` with Dekker `prepare_request_sleep()` / `cancel_request_sleep()`. Multi-loop free-threaded pools use `EventLoopPool(size=pool_size)` with single-wakeup `spawn_batch`; pure-sync embedded native and single-root WSGI (`single_root_wsgi` -> `drive_wsgi_batch`) workers poll `get_batch` in a tight vector loop and flush via `submit_batch_raw`.
7. On exit, runs `AsgiLifespan.shutdown()` (`lifespan.shutdown`) and stops the event loop(s) and optional thread pools (`sync_to_thread` / WSGI).

## Event Loops & Free-Threaded Support

- **`create_event_loop()` & `resolve_loop_backend()`** ([`_loops.py`](../../../../../src/py/vertebra/server/_loops.py)[^src-server-loops-py]): exposes `LoopBackend` (`auto`, `uvloop`, `winloop`, `rloop`, `zuvloop`, `asyncio`) and `LOOP_BACKEND_CHOICES` backed by declarative `_LOOP_BACKEND_SPECS` (`winloop` on `win32`, `uvloop` then `zuvloop` (Python 3.14+) and `rloop` on `posix`, falling back to `asyncio`). `resolve_loop_backend` honors explicit `--loop` and `VERTEBRA_LOOP` overrides, skips `quarantined_modules` (populated by `GilAuditReport` or `VERTEBRA_QUARANTINED_LOOPS` when an extension re-enabled the GIL under `3.14t`/`3.15t`), instantiates `new_event_loop()` directly without deprecated `AbstractEventLoopPolicy` APIs, and enables `asyncio.eager_task_factory` when supported.
- **`is_free_threaded()`** ([`_gil_audit.py`](../../../../../src/py/vertebra/server/_gil_audit.py)[^src-server-gil-audit-py]): `not sys._is_gil_enabled()` when available, else the `Py_GIL_DISABLED` build flag.
- **`GilAuditReport`** ([`_gil_audit.py`](../../../../../src/py/vertebra/server/_gil_audit.py)): a frozen `msgspec.Struct` that compares GIL state before and after import, lists newly loaded native extensions as suspects when the GIL was re-enabled, logs a warning, and raises `RuntimeError` when `VERTEBRA_STRICT_FREE_THREADING` is `1`/`true`.
- **`EventLoopPool`**: manages `size` persistent event loops (`inline_single_loop=True` when `size == 1` attaches an inline loop on the caller thread without spawning a background thread; `size > 1` spawns daemon threads `vertebra-event-loop-{i}` and coalesces batch submissions into one `call_soon_threadsafe` wakeup per target loop in `spawn_batch`). `stop()` cancels and awaits every pending task, runs `shutdown_asyncgens`, stops each loop, and closes a loop only after its thread exits; `__init__` raises `RuntimeError` when a shard does not signal readiness within 5 s.
- **Per-shard ASGI mounts**: `_instantiate_mount_runners` builds an `_AsgiMountRunner` when `EventLoopPool` has more than one loop, holding one `AsgiRunner(app, loop=shard_loop)` per shard and spreading `spawn_batch` batches across them. The worker dispatches mounts through `spawn_batch`; `_AsgiMountRunner.__call__` and `dispatch_batch(async_mode=False)`'s `async_native` branch are the buffered paths. Both wait up to `VERTEBRA_ASGI_BATCH_TIMEOUT` (default 30 s, `0`/`none` disables); timed-out requests get `504` and their tasks are cancelled through `schedule_batch` future cancellation.

## Sync Pools & Loop-Affine Resources (`utils/sync_tools.py`)

- **`SyncPool`**: a lazily created `ThreadPoolExecutor` (`max(1, max_workers if max_workers is not None else cpu_count * 2)` workers) paired with per-loop `_AsyncCompletionQueue` (`add_reader` socketpair wakeup) that copies `contextvars` into each call and recreates itself after `fork` (PID check). `SyncPool.default()` is process-wide. `async_()` wrappers (`_AsyncWrapper`) use it instead of `asyncio.to_thread` when the GIL is disabled; the SQLAlchemy dialect adapter (`extensions/sqlalchemy/dialects.py`) owns its own instance.
- **`SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]`**: caches one resource per event loop in a `WeakKeyDictionary`, built synchronously by `factory` or awaited asynchronously via `async def get` (`clear`).

## Platform Helpers (`platform.py`)

- **`CgroupQuota.detect()`** ([`platform.py`](../../../../../src/py/vertebra/server/platform.py)[^src-server-platform-py]): reads cgroup v2 `cpu.max` and `memory.max` (falling back to Linux `os.sched_getaffinity(0)` CPU mask width when `cpu.max` is missing or `max`); `cpu_quota_cores = quota / period`, `effective_workers = max(1, ceil(cpu_quota_cores))`, `memory_max_bytes` or `None`.
- **`LameDuckController`**: `enter_lame_duck()` records a timestamp; `readiness_status()` returns `(503, {"status": "lame_duck", ...})` afterwards, else `(200, {"status": "ok"})`.
- **`ZPagesRegistry`**: `handle_zpage(path)` renders JSON for `/healthz` (delegates to its `LameDuckController`), `/statusz`, `/varz` (`set_var`), and `/rpcz` (`record_rpc`); other paths return 404. `discover_adapters()` loads entry points in the `vertebra.platform_adapters` group.
- **`PlatformAdapter`**: a `Protocol` with `name` and `configure_server(zpages, quota)`.
- **`ShmAllocationPolicy`** ([`platform.py`](../../../../../src/py/vertebra/server/platform.py)): `allocate_anonymous_shm_fd(name, size)` returns `(fd, "memfd")` from `os.memfd_create(f"vertebra-{name}", MFD_CLOEXEC)` or `(fd, "tmpfile")` from a duplicated temporary file, then `ftruncate`s it. `send_fd`/`recv_fd` pass a descriptor over an `AF_UNIX` socket with `SCM_RIGHTS`.

The Rust binary caches a `HealthEndpoint` on `ServerState`: `response::handle` calls `state.health_endpoint.try_intercept(req_path)` ([`response.rs`](../../../../../src/rs/vertebra-http/src/response.rs)[^src-vertebra-http-src-response-rs], [`health.rs`](../../../../../src/rs/vertebra-http/src/health.rs)[^src-vertebra-http-src-health-rs]) for `/healthz`, `/livez`, `/__vertebra/health`, and `/readyz`. It is separate from `ZPagesRegistry`.

Provenance sources: [^src-cli-run-py], [^src-cli-launch-py], [^src-cli-dev-py], [^src-server-dispatch-py], [^src-utils-sync-tools-py].

[^src-supervisor-supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py` — Python implementation module (src/py/vertebra/supervisor/_supervisor.py).
[^src-cli-run-py]: `src/py/vertebra/cli/run.py` — Python implementation module (src/py/vertebra/cli/run.py).
[^src-cli-launch-py]: `src/py/vertebra/cli/_launch.py` — Python implementation module (src/py/vertebra/cli/_launch.py).
[^src-cli-dev-py]: `src/py/vertebra/cli/dev.py` — Python implementation module (src/py/vertebra/cli/dev.py).
[^src-server-worker-py]: `src/py/vertebra/server/worker.py` — Python implementation module (src/py/vertebra/server/worker.py).
[^src-server-dispatch-py]: `src/py/vertebra/server/_dispatch.py` — Python implementation module (src/py/vertebra/server/_dispatch.py).
[^src-server-loops-py]: `src/py/vertebra/server/_loops.py` — Python implementation module (src/py/vertebra/server/_loops.py).
[^src-server-gil-audit-py]: `src/py/vertebra/server/_gil_audit.py` — Python implementation module (src/py/vertebra/server/_gil_audit.py).
[^src-server-platform-py]: `src/py/vertebra/server/platform.py` — Python implementation module (src/py/vertebra/server/platform.py).
[^src-utils-sync-tools-py]: `src/py/vertebra/utils/sync_tools.py` — Python implementation module (src/py/vertebra/utils/sync_tools.py).
[^src-vertebra-http-src-health-rs]: `src/rs/vertebra-http/src/health.rs` — Rust implementation module (src/rs/vertebra-http/src/health.rs).
[^src-vertebra-http-src-response-rs]: `src/rs/vertebra-http/src/response.rs` — Rust implementation module (src/rs/vertebra-http/src/response.rs).
