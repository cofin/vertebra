---
type: Guide
title: "Server Platform & Process Lifecycle"
description: "Python-side runtime around vertebra-server: the launcher, worker signal handling, 3.14t loop sharding, sync pools, and library-only platform helpers."
status: stable
tags: [architecture, runtime, free-threaded, platform, process-lifecycle]
sources:
  - resource: src/py/vertebra/supervisor/_supervisor.py
  - resource: src/py/vertebra/cli/run.py
  - resource: src/py/vertebra/cli/dev.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/server/_loops.py
  - resource: src/py/vertebra/server/_gil_audit.py
  - resource: src/py/vertebra/server/platform.py
  - resource: src/py/vertebra/utils/sync_tools.py
  - resource: src/rs/vertebra-http/src/health.rs
---

# Server Platform & Process Lifecycle

This chapter covers the Python code that launches the Rust binary and runs inside its workers. Rust-side spawning of SHM workers is in [SHM Worker Spawn & Shutdown](shm-supervision-and-recovery.md); topology selection is in [Runtime Topologies](topologies-and-embedding.md).

## Launcher (`vertebra.supervisor._supervisor`)

[`run_supervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py) starts the `vertebra-server` binary with `subprocess.Popen` and returns its exit code. It does not supervise Python workers.

- `vtb run` and `vtb dev` build the environment (`VERTEBRA_APP`, `VERTEBRA_HOST`, `VERTEBRA_PORT`, `VERTEBRA_WORKERS`, `VERTEBRA_THREADS`, `VERTEBRA_TOPOLOGY`, `VERTEBRA_INTERFACE`, `VERTEBRA_NATIVE`, TLS paths, `VERTEBRA_STATIC_DIR`) and call `run_supervisor(server_cmd=build_server_command(), env=...)`. `vtb dev` passes `tui=True` when stderr is a TTY, which pipes stderr into a Rich live display (`_run_with_tui`).
- `find_server_binary` resolves `VERTEBRA_SERVER_BINARY`, then the packaged `vertebra/bin/vertebra-server[.exe]`, then `target/{release,debug}` and `src/rs/target/{release,debug}` under the repository root.
- On `KeyboardInterrupt` it waits `shutdown_timeout_ms` for the child, then sends `SIGTERM` and waits 5 s more.
- `run_supervisor` exports `VERTEBRA_DRAIN_TIMEOUT_MS` and `VERTEBRA_SHUTDOWN_TIMEOUT_MS` (`--drain-timeout`, `--shutdown-timeout`).

## Worker Loop Lifecycle (`run_worker`)

[`run_worker`](../../../../../src/py/vertebra/server/_dispatch.py) runs in the embedded Python thread or in each SHM subprocess:

1. Installs `SIGTERM`/`SIGINT` handlers (main thread only) that clear `_RUNNING`, so the loop exits at the next batch boundary.
2. Initializes the transport (`init_transport(mode="shm")` when `VERTEBRA_TRANSPORT_MODE=shm`; otherwise `init_transport()`, a no-op when the bridge already exists).
3. Warms `_handler_dispatch_spec` for every handler.
4. Calls `configure_worker_affinity(VERTEBRA_WORKER_ID, enable_pinning=VERTEBRA_CPU_AFFINITY in {1,true,TRUE})`, which pins to `worker_index % os.cpu_count()` via `os.sched_setaffinity` on Linux.
5. Calls `GilAuditReport.audit()`.
6. Creates the scheduler: `EventLoopPool(size=pool_size)` (controlled via `VERTEBRA_LOOP_SHARDS` or `pool_size`) when `pool_size > 1`, `is_free_threaded()`, or running in embedded mode (`EventLoopPool(size=1)`), or no scheduler when running the SHM worker loop directly.
7. On exit, stops the scheduler and the WSGI executor.

## Free-Threaded Support

- **`is_free_threaded()`** ([`_loops.py`](../../../../../src/py/vertebra/server/_loops.py)): `not sys._is_gil_enabled()` when available, else the `Py_GIL_DISABLED` build flag.
- **`GilAuditReport`** ([`_gil_audit.py`](../../../../../src/py/vertebra/server/_gil_audit.py)): a frozen `msgspec.Struct` that compares GIL state before and after import, lists newly loaded native extensions as suspects when the GIL was re-enabled, logs a warning, and raises `RuntimeError` when `VERTEBRA_STRICT_FREE_THREADING` is `1`/`true`. `quarantine_unsafe_modules` records `uvloop <= 0.22.1` on free-threaded runs and logs a warning.
- **`EventLoopPool`**: one daemon thread per shard (`vertebra-event-loop-{i}`), each running a persistent asyncio loop. `get_loop(i)` maps `i % size`; `next_loop()`/`loop` round-robin; `spawn`, `spawn_batch`, `schedule`, and `schedule_batch` place coroutines on shards; `stop()` waits up to 2 s for tasks, stops loops, and joins threads. `size < 1` raises `ValueError`.
- **Per-shard ASGI mounts**: `_instantiate_mount_runners` builds an `_AsgiMountRunner` when the pool has more than one loop. It holds one `AsgiRunner(app, loop=shard_loop)` per shard and spreads `spawn_batch` requests across them round-robin. RSGI mounts start on the primary loop only.

## Sync Pools & Loop-Affine Resources (`utils/sync_tools.py`)

- **`SyncPool`**: a lazily created `ThreadPoolExecutor` (default `2 × cpu_count` workers) that copies `contextvars` into each call and recreates itself after `fork` (PID check). `SyncPool.default()` is process-wide. `async_()` wrappers (`_AsyncWrapper`) use it instead of `asyncio.to_thread` when the GIL is disabled; the SQLAlchemy dialect adapter (`extensions/sqlalchemy/dialects.py`) owns its own instance.
- **`LoopShardedResource[T]`**: caches one resource per event loop in a `WeakKeyDictionary`, built by `factory` or awaited from `async_factory` (`get` / `get_async` / `clear`).

## Platform Helpers (`platform.py`)

- **`CgroupQuota.detect()`** ([`platform.py`](../../../../../src/py/vertebra/server/platform.py)): reads cgroup v2 `cpu.max` and `memory.max`; `cpu_quota_cores = quota / period` (host CPU count when the file is missing or `max`), `effective_workers = max(1, ceil(cpu_quota_cores))`, `memory_max_bytes` or `None`.
- **`LameDuckController`**: `enter_lame_duck()` records a timestamp; `readiness_status()` returns `(503, {"status": "lame_duck", ...})` afterwards, else `(200, {"status": "ok"})`.
- **`ZPagesRegistry`**: `handle_zpage(path)` renders JSON for `/healthz` (delegates to its `LameDuckController`), `/statusz`, `/varz` (`set_var`), and `/rpcz` (`record_rpc`); other paths return 404. `discover_adapters()` loads entry points in the `vertebra.platform_adapters` group.
- **`PlatformAdapter`**: a `Protocol` with `name` and `configure_server(zpages, quota)`.
- **`ShmAllocationPolicy`** ([`platform.py`](../../../../../src/py/vertebra/server/platform.py)): `allocate_anonymous_shm_fd(name, size)` returns `(fd, "memfd")` from `os.memfd_create(f"vertebra-{name}", MFD_CLOEXEC)` or `(fd, "tmpfile")` from a duplicated temporary file, then `ftruncate`s it. `send_fd`/`recv_fd` pass a descriptor over an `AF_UNIX` socket with `SCM_RIGHTS`.

The Rust binary has its own health interception: `response::handle` calls `HealthEndpoint::default().try_intercept(path)` ([`health.rs`](../../../../../src/rs/vertebra-http/src/health.rs)) for `/healthz`, `/livez`, `/__vertebra/health`, and `/readyz`. It is separate from `ZPagesRegistry`.
