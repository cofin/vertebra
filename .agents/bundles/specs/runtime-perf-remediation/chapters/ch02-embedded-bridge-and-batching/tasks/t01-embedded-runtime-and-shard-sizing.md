---
type: Task
id: "runtime-perf-remediation:ch02:t01"
chapter_id: ch02-embedded-bridge-and-batching
title: Right-size Embedded Tokio threads, RequestBatcher shards, and cgroup CPU detection
description: Default non-free-threaded Embedded Tokio worker_threads to 1 when VERTEBRA_TOKIO_THREADS is unset, align RequestBatcher shard_count to active Python consumers, and use cgroup-aware config::num_cpus() in EmbeddedSupervisor and embedded worker startup.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-http/src/main.rs
  - src/rs/vertebra-http/src/workers/factory.rs
files:
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-http/src/main.rs
  - src/rs/vertebra-http/src/workers/factory.rs
tests:
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-http/src/workers/factory.rs
estimated_loc: 110
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-http-config
    resource: src/rs/vertebra-http/src/config.rs::ServerConfig::from_env
    title: Server configuration, Tokio thread defaults, and cgroup CPU detection
  - id: src-http-main
    resource: src/rs/vertebra-http/src/main.rs::run_server
    title: Tokio runtime startup and RequestBatcher shard sizing
  - id: src-http-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::EmbeddedSupervisor::spawn_one
    title: Multi-process SO_REUSEPORT embedded worker CPU budgeting
tags:
  - embedded
  - tokio
  - config
  - sharding
  - cgroup
---

# Right-size Embedded Tokio threads, RequestBatcher shards, and cgroup CPU detection

## Objective & Scope Boundary

Eliminate Tokio thread oversubscription and redundant `RequestBatcher` shards in 1-worker Embedded mode, and make embedded CPU budgeting cgroup-aware [^src-http-config] [^src-http-main] [^src-http-factory].

Stay strictly inside `config.rs`, `main.rs`, and `workers/factory.rs`. Do not modify `server.rs` or `batcher.rs`.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md` (Section 2.2 contracts)
- `src/rs/vertebra-http/src/config.rs::ServerConfig::from_env` and `num_cpus`
- `src/rs/vertebra-http/src/main.rs::build_tokio_runtime`, `spawn_python_workers`, and `run_server`
- `src/rs/vertebra-http/src/workers/factory.rs::EmbeddedSupervisor::spawn_one`

## Contracts & Edge Cases

Implement the `config.rs` signatures defined in `chapters/ch02-embedded-bridge-and-batching/spec.md`:

- Expose `pub fn num_cpus() -> usize` in `config.rs` and replace `std::thread::available_parallelism()` in `EmbeddedSupervisor::spawn_one` (`workers/factory.rs`) and `spawn_python_workers` (`main.rs`).
- Extract `ServerConfig::apply_runtime_thread_env(available_cpus, free_threaded, get_env)` called by `from_env()` with `cfg!(Py_GIL_DISABLED)` so tests never mutate process env vars:
  - On `Topology::Embedded` with `!free_threaded` and unset `VERTEBRA_TOKIO_THREADS`, set `self.worker_threads = 1` (selecting `Builder::new_current_thread()` in `build_tokio_runtime`).
  - When `VERTEBRA_TOKIO_THREADS` is explicitly set (`> 0`), honor it on all builds and topologies.
  - Preserve `Topology::Shm` multi-worker clamping (`available_cpus.saturating_sub(workers).clamp(1, 4).min(available_cpus)`) and free-threaded `VERTEBRA_THREADS` scaling.
- Add `pub fn compute_batcher_shard_count(topology: Topology, shm_workers: usize, embedded_consumers: usize) -> usize`:
  - `Topology::Shm` returns `shm_workers.clamp(1, 8)`.
  - `Topology::Embedded` returns `embedded_consumers.clamp(1, 8)`.
- Expose `EmbeddedPythonWorkers::consumer_count(&self) -> usize` in `main.rs` (returning `self.handles.len().max(1)`) and pass it to `compute_batcher_shard_count` in `run_server` (`1` for GIL Embedded even if `VERTEBRA_TOKIO_THREADS > 1`, `embedded_py_threads.clamp(1, 8)` for FT Embedded, fallback `config.worker_threads.clamp(1, 8)` under `not(feature = "python")`).

## Test Matrix

| Case | Input | Expected Output |
| :--- | :--- | :--- |
| GIL Embedded default | `Topology::Embedded`, `free_threaded = false`, no env | `worker_threads == 1` |
| GIL Embedded with `VERTEBRA_THREADS=8` | `Topology::Embedded`, `free_threaded = false`, `VERTEBRA_THREADS=8` | `worker_threads == 1`, `thread_count == 8` |
| Explicit `VERTEBRA_TOKIO_THREADS=2` on GIL Embedded | `Topology::Embedded`, `free_threaded = false`, `VERTEBRA_TOKIO_THREADS=2` | `worker_threads == 2` |
| Free-threaded Embedded default | `Topology::Embedded`, `free_threaded = true`, `available_cpus = 12` | `worker_threads == 12` (or `VERTEBRA_THREADS.min(12)`) |
| SHM 4w default | `Topology::Shm`, `VERTEBRA_WORKERS=4`, `available_cpus = 12` | `worker_threads == 4` |
| Shard count sizing | `(Embedded, 1, 1)`, `(Embedded, 1, 12)`, `(Shm, 4, 0)`, `(Shm, 0, 0)` | `1`, `8`, `4`, `1` |
| Cgroup-aware `num_cpus()` | Existing `compute_embedded_worker_budgets` + `num_cpus() >= 1` | Non-zero and bounded by `available_parallelism` |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http -- config factory
```

## Acceptance Criteria

- `ServerConfig::from_env()` defaults `worker_threads = 1` on non-free-threaded `Topology::Embedded` when `VERTEBRA_TOKIO_THREADS` is unset.
- Explicit `VERTEBRA_TOKIO_THREADS` overrides and `Py_GIL_DISABLED` multi-thread scaling work unchanged.
- `RequestBatcher` `shard_count` in `main.rs` matches active Python consumers (`1` for GIL Embedded, `1..=8` for FT Embedded and SHM).
- `EmbeddedSupervisor::spawn_one` and `spawn_python_workers` call `config::num_cpus()`.
- All unit tests in `config.rs` and `workers/factory.rs` pass.

[^src-http-config]: `src/rs/vertebra-http/src/config.rs::ServerConfig::from_env` — Server configuration, Tokio thread defaults, and cgroup CPU detection.
[^src-http-main]: `src/rs/vertebra-http/src/main.rs::run_server` — Tokio runtime startup and RequestBatcher shard sizing.
[^src-http-factory]: `src/rs/vertebra-http/src/workers/factory.rs::EmbeddedSupervisor::spawn_one` — Multi-process SO_REUSEPORT embedded worker CPU budgeting.
