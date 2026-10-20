---
type: Guide
title: "SHM Worker Supervision & Crash Recovery"
description: "Shared-memory FD inheritance, WorkerSupervisor process lifecycle, pidfd crash detection, ring reclamation, and /dev/shm sweeping."
status: stable
tags: [architecture, runtime, supervisor, shared-memory, crash-recovery]
sources:
  - resource: src/rs/vertebra-http/src/ring_factory.rs
  - resource: src/rs/vertebra-core/src/process.rs
  - resource: src/rs/vertebra-core/src/cleanup.rs
  - resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  - resource: src/py/vertebra/supervisor/_supervisor.py
---

# SHM Worker Supervision & Crash Recovery

In `Topology::SharedMemory`, [`RingFactory`](../../../../../src/rs/vertebra-http/src/ring_factory.rs), [`process.rs`](../../../../../src/rs/vertebra-core/src/process.rs), and [`WorkerSupervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py) manage the full lifecycle of shared-memory rings and Python worker subprocesses without reallocating shared memory on worker restarts.

## 1. File Descriptor Inheritance (`RingFactory` & `RingPairFds`)

[`RingFactory`](../../../../../src/rs/vertebra-http/src/ring_factory.rs) allocates a [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) (request `MpmcRing` + 4-tier response `TieredRingSet`) and clears `FD_CLOEXEC` via `RingPair::make_inheritable()` so child processes inherit the open descriptors across `fork`/`exec`:

| Environment Variable | Purpose | Platform Notes |
|---|---|---|
| `VERTEBRA_REQ_RING_FD` | Request `MpmcRing` shared memory FD | Linux `memfd` or macOS `shm_open` FD |
| `VERTEBRA_RESP_TINY_FD` .. `LARGE_FD` | 4-tier response `SpscRing` shared memory FDs | `512 B`, `4 KiB`, `64 KiB`, `1 MiB` tiers |
| `VERTEBRA_REQ_NOTIFY_FD` | Request wakeup read/event FD | Linux `eventfd` or macOS pipe `read_fd` |
| `VERTEBRA_RESP_NOTIFY_FD` | Response wakeup read/event FD | Linux `eventfd` or macOS pipe `read_fd` |
| `VERTEBRA_REQ_NOTIFY_WRITE_FD` | Request wakeup pipe `write_fd` | Set on non-Linux Unix (`pipe(2)` pair); `-1` on Linux |
| `VERTEBRA_RESP_NOTIFY_WRITE_FD` | Response wakeup pipe `write_fd` | Set on non-Linux Unix (`pipe(2)` pair); `-1` on Linux |
| `VERTEBRA_WORKER_ID` | 0-based worker index | Used for logging and worker isolation |

## 2. Subprocess Crash Detection (`process.rs`)

[`vertebra-core/src/process.rs`](../../../../../src/rs/vertebra-core/src/process.rs) monitors spawned worker PIDs:

- **Linux (`target_os = "linux"`)**: Uses `pidfd_open(pid, PIDFD_NONBLOCK)` and `waitid(P_PID, pid, WEXITED | WNOHANG)` for zero-polling readiness notifications when a child exits, extracting exit code or signal (`CLD_KILLED` / `CLD_DUMPED`).
- **macOS / Non-Linux Unix**: Polls `waitpid(pid, WNOHANG)` and translates `WIFEXITED` / `WIFSIGNALED`.

## 3. Ring Reclamation & Exponential Backoff Circuit Breaker

When a worker crashes mid-request, its SHM rings may contain partially written frames or non-zero waiter flags:

1. **In-Place Ring Reclamation**: Before respawning the worker, [`RingPair::reclaim()`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) resets `write_idx`, `read_idx`, `send_waiters`, `recv_waiters`, `wait_write_ptr`, and `wait_read_ptr` to `0` and increments `epoch` (`Ordering::Release`). The existing `memfd` / POSIX SHM FDs are reused directly by the replacement process.
2. **Backoff & Circuit Breaker**: [`WorkerLifecycle`](../../../../../src/rs/vertebra-core/src/process.rs) and [`WorkerSupervisor`](../../../../../src/py/vertebra/supervisor/_supervisor.py) apply exponential backoff (`min(100ms * 2^attempt, 30s)`) and trip the circuit breaker if a worker exceeds `max_restarts` within the sliding window.

## 4. Orphaned Shared-Memory Sweeping (`cleanup.rs`)

On startup, [`cleanup.rs`](../../../../../src/rs/vertebra-core/src/cleanup.rs) scans `/dev/shm` for `vertebra-*` and `vtb-*` segments, inspects `ShmHeader::creator_pid` (when magic `0x5645_5254_4542_5241` matches), checks `kill(pid, 0) == ESRCH`, and unlinks segments left behind by `SIGKILL`ed processes.
