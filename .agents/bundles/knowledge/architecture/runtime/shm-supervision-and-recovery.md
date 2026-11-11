---
type: Guide
title: "SHM Worker Spawn, Shutdown & Recovery Hooks"
description: "How RingFactory hands shared-memory rings to Python worker subprocesses, stops them, and which crash-recovery helpers exist but are not wired into the server."
status: stable
tags:
  - architecture
  - runtime
  - shared-memory
  - process-lifecycle
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 9af810f5
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-http-src-workers-factory-rs
    resource: src/rs/vertebra-http/src/workers/factory.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/workers/factory.rs)"
  - id: src-vertebra-http-src-workers-supervisor-rs
    resource: src/rs/vertebra-http/src/workers/supervisor.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/workers/supervisor.rs)"
  - id: src-vertebra-http-src-main-rs
    resource: src/rs/vertebra-http/src/main.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/main.rs)"
  - id: src-vertebra-http-src-dispatcher-shm-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs)"
  - id: src-vertebra-core-src-process-rs
    resource: src/rs/vertebra-core/src/process.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/process.rs)"
  - id: src-vertebra-core-src-cleanup-rs
    resource: src/rs/vertebra-core/src/cleanup.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/cleanup.rs)"
  - id: src-vertebra-core-src-transport-ring-pair-rs
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs)"
  - id: src-vertebra-core-src-channel-rs
    resource: src/rs/vertebra-core/src/channel.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/channel.rs)"
---

# SHM Worker Spawn, Shutdown & Recovery Hooks

<!-- truth: start -->
- `RingFactory` pre-allocates per-worker `RingPair` shared-memory regions in the parent process, clears `FD_CLOEXEC` on inherited descriptors, and spawns `python -m vertebra.supervisor._worker` subprocesses.
- Shutdown drains active worker subprocesses within the configured timeout and reclaims shared-memory segments and broker slot leases on worker exit.
<!-- truth: end -->

In `Topology::Shm` the Rust binary creates every ring before any worker exists, spawns each Python worker once, and stops them at shutdown. [`RingFactory`](../../../../../src/rs/vertebra-http/src/workers/factory.rs)[^src-vertebra-http-src-workers-factory-rs] owns that path.

## Ring Allocation & FD Inheritance

`RingFactory::new(worker_count, config)` calls `RingPair::create(config)` per worker, then clears `FD_CLOEXEC` on every descriptor with `vertebra_core::process::make_inheritable(&pair.all_fds())` ([`process.rs`](../../../../../src/rs/vertebra-core/src/process.rs)[^src-vertebra-core-src-process-rs]).

`RingFactory::spawn_worker` builds `python -m vertebra.supervisor._worker` with `config.workers.python_bin`, passes the variables from `RingPairFds::to_env_vars(worker_id)`, sets `VERTEBRA_APP`, propagates `VERTEBRA_THREADS`, and on Linux calls `process::set_parent_death_signal` (`PR_SET_PDEATHSIG = SIGKILL`). The child otherwise inherits the parent environment.

| Variable | Value |
|---|---|
| `VERTEBRA_TRANSPORT_MODE` | `shm` |
| `VERTEBRA_WORKER_ID` | 0-based worker index |
| `VERTEBRA_REQ_RING_FD` / `VERTEBRA_REQ_RING_SIZE` | request `MpmcRing` region FD and mapping size |
| `VERTEBRA_RESP_RING_{SMALL,MEDIUM,LARGE,XLARGE}_FD` / `_SIZE` | response `SpscRing` tier FDs and mapping sizes |
| `VERTEBRA_REQ_NOTIFY_FD` / `VERTEBRA_RESP_NOTIFY_FD` | Linux `eventfd`, or the read end of a `pipe(2)` elsewhere |
| `VERTEBRA_REQ_NOTIFY_WRITE_FD` / `VERTEBRA_RESP_NOTIFY_WRITE_FD` | pipe write ends; emitted only on non-Linux Unix |
| `VERTEBRA_BROKER_SHM_FD` / `VERTEBRA_BROKER_SHM_SIZE` | shared-memory database broker (`ShmLeaseTable` + co-located `ShmStatementCache` + `ShmChannelSlot` ring) FD and mapping size when configured via `with_broker()` |

`RingPair::from_env` parses these variables, dynamically reconstructs `request_slots` and each response tier's `slot_count` from `VERTEBRA_REQ_RING_SIZE` and `VERTEBRA_RESP_RING_{SMALL,MEDIUM,LARGE,XLARGE}_SIZE` via `RingLayout::slots_from_total_size` (matching both `RingPairConfig::default()` and `RingPairConfig::compact()`), and attaches the inherited shared-memory descriptors.

## Spawn & Shutdown in `main.rs`

- `factory.spawn_all(&app_path, &config.workers.python_bin)` spawns all workers once. If one spawn fails, already-started children are killed and startup aborts.
- After `server::run_with_collector` returns, `RingFactory::shutdown_workers(children, 5 s)` sends `SIGTERM` to every child, polls `try_wait` until the deadline, then `kill()`s and reaps the rest, calling `reclaim_worker(pid)` on the attached `ShmLeaseTable` so any connection leases held by exiting workers are immediately returned to `SLOT_STATE_FREE`.
- Inside the child, `run_worker` installs `SIGTERM`/`SIGINT` handlers that stop the batch loop at the next batch boundary.

## Correlation Expiry

`ShmDispatcher::new` ([`dispatcher/shm.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs)[^src-vertebra-http-src-dispatcher-shm-rs]) spawns a sweep task that runs every second and expires in-flight correlations older than 30 s, so a dead worker's requests eventually complete instead of hanging forever.

## Recovery & Supervision (`workers/supervisor.rs`, `workers/factory.rs`)

- **`Supervisor`** ([`workers/supervisor.rs`](../../../../../src/rs/vertebra-http/src/workers/supervisor.rs)[^src-vertebra-http-src-workers-supervisor-rs]): polls worker liveness every `500 ms` (`Supervisor::spawn`). When a worker crashes (`process_worker_exit`), `Supervisor` immediately marks the worker unhealthy in `ShmDispatcher` (`set_worker_healthy(worker_id, false)` so `worker_ring_has_capacity` routes new batches away from its ring), fails any in-flight correlations for that worker with `502 Bad Gateway` (`sweep_for_worker`), reclaims its `RingPair` cursors (`RingPair::reclaim`), and schedules an exponential-backoff respawn (`set_worker_healthy(worker_id, true)` once `lifecycle.respawn()` succeeds).
- **`RingPair::reclaim`** (`unsafe`, [`ring_pair.rs`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs)[^src-vertebra-core-src-transport-ring-pair-rs]): calls `reset()` on the request ring and each response tier, then clears both notifiers. `SpscRing::reset` zeroes `read_head`, `write_head`, `wait_read`, `wait_write`, `send_waiters`, `recv_waiters`, clears the borrow flag, and bumps `ShmHeader::generation` (`Release`). `MpmcRing::reset` zeroes the cursors, re-seeds the slot sequence array, and bumps `generation`.
- **`WorkerLifecycle`** (`workers/factory.rs`): tracks a `Child`, opens a Linux pidfd via `platform::linux::pidfd_open` (flags `0`), and checks liveness with `Child::try_wait()`. When `is_alive()` detects worker exit and a broker `ShmLeaseTable` is attached (`with_broker`), `reclaim_worker_leases()` calls `ShmLeaseTable::reclaim_worker(pid)` to free any leaked database connection slots. `respawn_delay()` is `1 s << min(crash_count - 1, 5)` (1 s to 32 s). `record_crash` resets the count after 60 s of uptime. `circuit_breaker_tripped()` is true after more than 10 crashes within 300 s.
- **`cleanup_stale_shm`** (`workers/factory.rs`): unlinks `/dev/shm` entries starting with `vertebra-`, `vertebra_`, or `vtb-`.
- **`install_panic_cleanup_hook`** (`workers/factory.rs`): chains a panic hook that calls `cleanup::cleanup_all()`.
- **`cleanup.rs` registry** ([`cleanup.rs`](../../../../../src/rs/vertebra-core/src/cleanup.rs)[^src-vertebra-core-src-cleanup-rs]): an in-process list of named regions (`register_name`, `deregister_name`, `registered_names`, `cleanup_all` → `shm_unlink_named`).

See [SHM gotchas](../../gotchas/shm-and-cross-platform-ipc.md) for leftover-segment cleanup in tests.

Provenance sources: [^src-vertebra-http-src-main-rs], [^src-vertebra-core-src-channel-rs].

[^src-vertebra-http-src-workers-factory-rs]: `src/rs/vertebra-http/src/workers/factory.rs` — Rust implementation module (src/rs/vertebra-http/src/workers/factory.rs).
[^src-vertebra-http-src-workers-supervisor-rs]: `src/rs/vertebra-http/src/workers/supervisor.rs` — Rust implementation module (src/rs/vertebra-http/src/workers/supervisor.rs).
[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Rust implementation module (src/rs/vertebra-http/src/main.rs).
[^src-vertebra-http-src-dispatcher-shm-rs]: `src/rs/vertebra-http/src/dispatcher/shm.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs).
[^src-vertebra-core-src-process-rs]: `src/rs/vertebra-core/src/process.rs` — Rust implementation module (src/rs/vertebra-core/src/process.rs).
[^src-vertebra-core-src-cleanup-rs]: `src/rs/vertebra-core/src/cleanup.rs` — Rust implementation module (src/rs/vertebra-core/src/cleanup.rs).
[^src-vertebra-core-src-transport-ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs).
[^src-vertebra-core-src-channel-rs]: `src/rs/vertebra-core/src/channel.rs` — Rust implementation module (src/rs/vertebra-core/src/channel.rs).
