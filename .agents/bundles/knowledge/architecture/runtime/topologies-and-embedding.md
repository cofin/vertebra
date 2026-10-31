---
type: Guide
title: "Runtime Topologies & Python Embedding"
description: "How the vertebra-server binary starts, selects Auto, Embedded, or Shm topologies, supervises SO_REUSEPORT embedded workers or SHM subprocesses, and runs free-threaded Python pools."
status: stable
tags: [architecture, runtime, topologies, pyo3, free-threaded]
sources:
  - resource: src/rs/vertebra-http/src/main.rs
  - resource: src/rs/vertebra-http/src/config.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/workers/factory.rs
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/rs/vertebra-py/src/transport.rs
  - resource: src/py/vertebra/server/worker.py
  - resource: src/py/vertebra/server/_dispatch.py
  - resource: src/py/vertebra/supervisor/_worker.py
  - resource: src/py/vertebra/cli/_launch.py
  - resource: src/py/vertebra/cli/run.py
---

# Runtime Topologies & Python Embedding

The `vertebra-server` binary ([`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs)) builds an explicit Tokio runtime (`Builder::new_current_thread` when `worker_threads <= 1`, `Builder::new_multi_thread` clamped to `available_parallelism()` when `> 1`) and hosts Python in one of three configured [`Topology`](../../../../../src/rs/vertebra-http/src/config.rs) modes (`Topology::from_str` accepts `"auto"`, `"embedded"`, and `"shm"`): `Embedded`, `Shm`, or `Auto` (which automatically resolves to `Shm` on POSIX non-free-threaded Python when `workers > 1`, and `Embedded` for single-worker or free-threaded `3.14t`/`3.15t` runtimes).

```mermaid
flowchart TD
    Ingress["Tokio ingress (server::run_with_collector)"]
    Ingress -->|"Topology::Embedded (single-worker, 3.14t/3.15t, or explicit)"| Bridge["BatchBridge (InProcessRequest + ResponseSink)"]
    Ingress -->|"Topology::Shm (auto when workers > 1 on GIL Unix, or explicit)"| Shm["RingPair per worker (memfd / shm_open)"]
    Bridge --> PyThread["Embedded Python worker(s) (worker.run_worker)"]
    Shm --> PySub["Python subprocesses (python -m vertebra.supervisor._worker)"]
```

## Topology Selection

- `vtb run` ([`run.py`](../../../../../src/py/vertebra/cli/run.py)) and `ServerLaunchEnv.to_environ()` ([`_launch.py`](../../../../../src/py/vertebra/cli/_launch.py)) preserve inherited `VERTEBRA_TOPOLOGY`, `VERTEBRA_WORKERS`, `VERTEBRA_THREADS`, and `VERTEBRA_JIT` environment variables whenever the corresponding CLI options (`--topology auto|embedded|shm`, `--workers`, `--threads`, `--jit/--no-jit`) are omitted, automatically resolving `auto`/omitted topology to `"shm"` when `workers > 1` on POSIX standard GIL builds (`not is_free_threaded_runtime()`) and `"embedded"` otherwise before launching the binary through `run_supervisor` (see [Platform & Lifecycle](platform-and-lifecycle.md)).
- When `Topology::Embedded` runs with `config.workers.count > 1` (either on free-threaded Python or via explicit `--topology embedded`) and `VERTEBRA_EMBEDDED_WORKER != "1"`, `EmbeddedSupervisor` pre-initializes shared SHM broker FDs (`ShmLeaseTable` / `ShmStatementCache`) and spawns `W` child `vertebra-server` worker processes (`VERTEBRA_EMBEDDED_WORKER=1`) binding the same port via `SO_REUSEPORT`, computing per-worker `(tokio_threads, py_threads)` against `std::thread::available_parallelism()` to prevent core oversubscription.
- `Topology::Shm` (`VERTEBRA_TOPOLOGY=shm`, selected automatically for `workers > 1` on standard GIL Unix or explicitly via `--topology shm`) runs one central Rust HTTP ingress process dispatching across shared-memory `RingPair`s to `W` Python worker subprocesses, sizing `config.worker_threads` (`available_cpus.saturating_sub(W).clamp(1, 4)`) when `VERTEBRA_TOKIO_THREADS` is unset so the central ingress and `W` Python workers share the CPU budget without oversubscription. On non-Unix targets `Shm` falls back to the in-process bridge dispatcher.

## Startup Sequence (`main.rs`)

1. `ServerConfig::from_env()` and `EmbeddedSupervisor` check (`workers > 1` under `Embedded` / Unix `Auto` spawns `SO_REUSEPORT` child workers and supervises signals/respawns).
2. Explicit `tokio::runtime::Builder` construction (`new_current_thread` for 1 Tokio thread, `new_multi_thread` otherwise).
3. `BatchBridge::init` runs **only** when the resolved topology is `Topology::Embedded` (never allocated in `Topology::Shm`).
4. With the `python` feature, `prepare_embedded_python_env` prepends `.venv/bin` / `VIRTUAL_ENV/bin` to `PATH` before CPython initialization (so relocatable `uv` `python-build-standalone` `libpython` resolves `pyvenv.cfg`), and `append_to_inittab!(pymodule_init)` registers the statically linked `_vertebra` module before the first `Python::attach`.
5. `Python::attach` populates `sys.path` via `ensure_embedded_python_path` (including `.venv`/`VIRTUAL_ENV` `site-packages`), imports `VERTEBRA_APP` (default `app:app`), invokes `extract_offload_json_from_app` (calling `LitestarPlugin.ensure_finalized(app)` and reading `app.state["vertebra_offload_config_json"]` when `VERTEBRA_OFFLOAD_CONFIG` is unset), and `build_route_table_from_app` reads `handler_index` and registers each mount prefix across `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, and `HEAD` encoded with `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`).
6. `Router::from_entries` builds the router.
7. Dispatcher selection (next sections).
8. `RequestBatcher::spawn_pool(dispatcher, BatchLimit::default(), shard_count, 16384)` starts the double-buffered collector tasks (`config.workers.count.clamp(1, 8)` in `Topology::Shm` for 1:1 batcher-to-worker affinity, and `config.worker_threads.clamp(1, 8)` in `Topology::Embedded`).
9. `server::run_with_collector` serves until `ctrl_c` (`SIGINT`) or `CancellationToken` drain resolves.
10. In `Shm`, `RingFactory::shutdown_workers(children, 5 s)` stops the subprocesses; in `Embedded`, Python worker `JoinHandle`s are joined cleanly.

## Embedded Mode (`Topology::Embedded`)

Each `vertebra-server` worker process hosts its own Tokio ingress and embedded Python interpreter.

- **Single `_vertebra` copy**: `vertebra-server` links `vertebra-py` directly. `spawn_python_workers` imports the inittab module `_vertebra` and sets `sys.modules["vertebra._vertebra"]` to it, so Python never `dlopen`s the wheel's `_vertebra.cpython-*.so` with a second `static BRIDGE: OnceLock<BatchBridge>`.
- **Dispatcher**: `Dispatcher::new()` (`DispatchBackend::Bridge(Arc<BridgeCorrelation>)`) uses an 8-shard `InFlightRequests` correlation slab, submits zero-copy `Vec<InProcessRequest>` batches via `BatchBridge::submit_inprocess_batch`, and registers a direct `ResponseSink` callback on `BatchBridge` that completes Hyper `oneshot::Sender`s directly on submission without a `DashMap` or intermediate `spawn_response_reader` task, alongside a periodic `sweep_expired` background timer.
- **Python worker thread(s)**: `spawn_python_workers` ([`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs)) checks `_vertebra.FREE_THREADED`, sets `BatchBridge::set_consumer_count(worker_threads)`, calls `init_transport()`, and invokes `run_worker` in [`worker.py`](../../../../../src/py/vertebra/server/worker.py). If an embedded worker thread errors or exits unexpectedly before server shutdown, it cancels `server_shutdown_token` and causes `run_server` to exit non-zero. When `consumer_count == 1` and async/ASGI/RSGI handlers are mounted, the worker runs inline on a single `uvloop` thread via `loop.add_reader(request_notify_fd, ...)` with Dekker `prepare_request_sleep` / `cancel_request_sleep`; pure-sync native embedded workers poll `get_batch` directly with `py.detach`.

## Shared-Memory Mode (`Topology::Shm`, Unix)

- `RingFactory::with_broker_config(worker_count.max(1), RingPairConfig::for_environment(worker_count.max(1)), ...)` creates one [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) per worker: a request `MpmcRing` plus a right-sized `TieredRingSet` of response `SpscRing`s (`ShmHeader` `ABI_VERSION = 3`, `46 MiB/worker` default or `12 MiB/worker` `compact` profile), backed by anonymous `memfd_create` on Linux and `shm_open` elsewhere (with `/tmp/vtb-*` fallback for `VQOV`/`VROV` overflow regions when `/dev/shm` is constrained).
- `Dispatcher::from_shm(Arc::new(ShmDispatcher::new(ring_pairs, 65536)))` routes whole batches with 1:1 batcher-to-worker shard affinity (`home = shard_idx % worker_count`, sticking to the warm MRU worker at `prev_active == 0` and spilling to the least-loaded healthy worker when `home` is unhealthy, full, or `in_flight >= 64`) and signals `notify_request_if_sleeping()` via Dekker `consumer_sleeping`; `shm_reader::spawn_response_readers` drains response slots zero-copy via `drain_responses_with`.
- `RingFactory::spawn_all` starts `python -m vertebra.supervisor._worker` once per worker. Spawn, FD inheritance, and shutdown are covered in [SHM Worker Spawn & Shutdown](shm-supervision-and-recovery.md).
- Inside each child, [`_worker.main`](../../../../../src/py/vertebra/supervisor/_worker.py) imports the app (registering native `App` routes only when `VERTEBRA_NATIVE` is truthy so `--asgi` / `VERTEBRA_NATIVE=0` mounts `App` once without double-mounting) and calls `run_worker` (`VERTEBRA_TRANSPORT_MODE=shm` -> `init_transport(mode="shm")`).

## Worker Loop Modes (`worker.py`)

- When `scheduler.size == 1` and a request `notify_fd` is active (`RingPair` in `shm` or single-consumer `BatchBridge` in `embedded` with async/ASGI/RSGI routes), `Worker._run_inline_event_loop` drives `try_get_batch()` directly on the `uvloop` / `asyncio` loop thread via `loop.add_reader(notify_fd, ...)` with zero cross-thread `call_soon_threadsafe` hops.
- When `pool_size > 1` on free-threaded builds without multi-thread `spawn_python_workers`, async work shards across `EventLoopPool(size=pool_size)`.

## Free-Threaded Python (`3.14t` & `3.15t`)

- `_vertebra`, `_bundler`, and `_storage` declare `#[pymodule(gil_used = false)]` ([`vertebra-py/src/lib.rs`](../../../../../src/rs/vertebra-py/src/lib.rs), `vertebra-bundler/src/lib.rs`, `vertebra-storage/src/lib.rs`), omit PyO3 global-mutex `freelist`s under `#[cfg(Py_GIL_DISABLED)]`, and set `immutable_type` on non-subclassed `#[pyclass]` types.
- `spawn_python_workers` in `vertebra-http` queries `_vertebra.FREE_THREADED` and spawns `N` symmetric embedded Python worker threads sharing `BatchBridge`'s lock-free `crossbeam` receiver when `py_threads > 1`, keeping object creation, execution, and `Drop` on the owning thread (`ob_tid` biased refcount locality).
- The `vertebra-server` binary installs `mimalloc::MiMalloc` as `#[global_allocator]` behind the default `mimalloc` feature (`default-features = false`, without `override`, coexisting cleanly with `libpython3.14t`/`libpython3.15t`'s internal `mimalloc`). PyO3 extension wheels use the host allocator.
- Loop sharding, GIL auditing, and CPU affinity are described in [Platform & Lifecycle](platform-and-lifecycle.md).
