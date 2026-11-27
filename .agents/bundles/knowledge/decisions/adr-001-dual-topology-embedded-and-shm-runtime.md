---
type: Decision
title: "ADR-001: Dual-Topology Embedded and Shared-Memory Runtime"
description: "Architectural decision to support both in-process Embedded and multi-process POSIX Shared-Memory worker topologies with automatic selection."
status: stable
tags:
  - decisions
  - adr
  - runtime
  - ipc
  - shm
  - free-threaded
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-27T22:10:00Z"
    commit: f3b624a2
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-http-src-main-rs
    resource: src/rs/vertebra-http/src/main.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/main.rs)"
  - id: src-vertebra-http-src-config-rs
    resource: src/rs/vertebra-http/src/config.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/config.rs)"
  - id: src-vertebra-core-src-bridge-rs
    resource: src/rs/vertebra-core/src/bridge.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/bridge.rs)"
  - id: src-vertebra-core-src-transport-ring-pair-rs
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs)"
  - id: src-cli-launch-py
    resource: src/py/vertebra/cli/_launch.py
    title: "Python implementation module (src/py/vertebra/cli/_launch.py)"
---

# ADR-001: Dual-Topology Embedded and Shared-Memory Runtime

<!-- truth: start -->
- **Decision**: Support two first-class execution topologies — `Topology::Embedded` (in-process `BatchBridge` with `SO_REUSEPORT` multi-process supervision) and `Topology::Shm` (central Rust ingress + lock-free `MpmcRing`/`TieredRingSet` worker processes) — with `auto` (`--topology auto` / `VERTEBRA_TOPOLOGY=auto`) selecting `Shm` when `workers > 1` on POSIX standard-GIL Python and `Embedded` for single-worker or free-threaded (`3.14t`/`3.15t`) runtimes.
- **Status**: Accepted (`v0.2.0`).
<!-- truth: end -->

## Context

Python web servers face distinct concurrency constraints across single-worker deployments, multi-worker standard-GIL deployments (`3.12`–`3.15`), and free-threaded `Py_GIL_DISABLED` deployments (`3.14t`/`3.15t`):

- A single in-process embedded interpreter eliminates IPC serialization and performs best for single-worker or free-threaded multi-thread pools, or when `SO_REUSEPORT` distributes independent connections across symmetric processes.
- Standard-GIL multi-worker deployments that share cross-worker state (centralized backpressure, shared connection leases, WebSocket/SSE pub/sub broadcast, or single-writer DuckDB analytical queries) benefit from a unified Rust ingress process dispatching over lock-free POSIX shared-memory rings (`MpmcRing` request ring + tiered `SpscRing` response rings).

## Alternatives Considered

| Option | Trade-offs | Outcome |
| :--- | :--- | :--- |
| **Embedded-only (`SO_REUSEPORT` processes)** | Lowest per-request overhead for stateless routes, but duplicates TLS/HTTP/3 state per worker and cannot share a single ingress backpressure queue without cross-process IPC. | Rejected as sole mode; retained as `Topology::Embedded`. |
| **Shared-memory-only (`Topology::Shm`)** | Uniform multi-process isolation and centralized ingress offload, but incurs an extra ring hop and wakeup on single-worker or free-threaded `3.14t`/`3.15t` deployments. | Rejected as sole mode; retained as `Topology::Shm`. |
| **Dual `Embedded` + `Shm` with `Auto` selection** | Keeps one unified `RequestBatch` / `BatchSnapshot` FFI surface in `vertebra-py` while choosing the optimal transport for `--workers` and `is_free_threaded_runtime()`. | **Selected**. |

## Consequences

- `vertebra-py` exposes identical `Request`, `Response`, `RequestBatch`, and `drive_{asgi,rsgi,wsgi}_batch` primitives regardless of whether batches arrive from `BatchBridge` (`Embedded`) or `RingPair` (`Shm`).
- `ServerLaunchEnv.to_environ()` (`src/py/vertebra/cli/_launch.py`) and `ServerConfig::from_env()` (`src/rs/vertebra-http/src/config.rs`) resolve `"auto"` deterministically into `Topology::Embedded` or `Topology::Shm` and honor explicit `--topology embedded|shm` overrides.

Provenance sources: [^src-vertebra-http-src-main-rs], [^src-vertebra-http-src-config-rs], [^src-vertebra-core-src-bridge-rs], [^src-vertebra-core-src-transport-ring-pair-rs], [^src-cli-launch-py].

[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Rust implementation module (src/rs/vertebra-http/src/main.rs).
[^src-vertebra-http-src-config-rs]: `src/rs/vertebra-http/src/config.rs` — Rust implementation module (src/rs/vertebra-http/src/config.rs).
[^src-vertebra-core-src-bridge-rs]: `src/rs/vertebra-core/src/bridge.rs` — Rust implementation module (src/rs/vertebra-core/src/bridge.rs).
[^src-vertebra-core-src-transport-ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs).
[^src-cli-launch-py]: `src/py/vertebra/cli/_launch.py` — Python implementation module (src/py/vertebra/cli/_launch.py).
