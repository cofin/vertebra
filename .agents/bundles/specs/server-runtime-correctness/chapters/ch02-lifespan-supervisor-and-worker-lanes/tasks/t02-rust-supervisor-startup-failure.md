---
type: Task
id: "server-runtime-correctness:ch02:t02"
flow_id: server-runtime-correctness
chapter_id: ch02-lifespan-supervisor-and-worker-lanes
title: Stop Rust Supervisor Respawn on Exit Code 78 and Exit Server Non-Zero
description: "Track WorkerLifecycle::last_exit_code in vertebra-http and abort Supervisor::poll_workers and run_server when any SHM worker exits with EXIT_STARTUP_FAILURE = 78."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch02:t01"
files:
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/rs/vertebra-http/src/workers/supervisor.rs
  - src/rs/vertebra-http/src/main.rs
tests:
  - src/rs/vertebra-http/src/workers/supervisor.rs
  - src/py/tests/integration/server/test_vertebra_server.py
read_set:
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/rs/vertebra-http/src/workers/supervisor.rs
  - src/rs/vertebra-http/src/main.rs
  - src/py/tests/integration/server/test_vertebra_server.py
estimated_loc: 150
tags:
  - rust
  - supervisor
  - shm
  - lifespan
sources:
  - id: supervisor-rs
    resource: "src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor"
    title: SHM worker poll and respawn supervisor
  - id: main-rs
    resource: "src/rs/vertebra-http/src/main.rs::run_server"
    title: Server shutdown token and exit result handling
---

# Task `t02`: Stop Rust Supervisor Respawn on Exit Code 78 and Exit Server Non-Zero

## Objective & Scope Boundary

Teach the Rust SHM supervisor to treat worker exit code `78` as a fatal startup error without backoff respawn ([#84](https://github.com/cofin/vertebra/issues/84)) per [`../spec.md` §2](../spec.md) [^supervisor-rs] [^main-rs].

## Bounded Read Manifest

- `src/rs/vertebra-http/src/workers/factory.rs` — `WorkerLifecycle::check_alive` exit status capture.
- `src/rs/vertebra-http/src/workers/supervisor.rs` — `Supervisor::spawn` and `poll_workers`.
- `src/rs/vertebra-http/src/main.rs` — `run_server` shutdown token and non-zero return.

## Contracts & Edge Cases

- Define `pub const EXIT_STARTUP_FAILURE: i32 = 78;` and `WorkerLifecycle::last_exit_code(&self) -> Option<i32>`.
- When a worker exits with `Some(78)`, `Supervisor::poll_workers` marks it `Dead`, stores `startup_failed = true`, cancels `shutdown`, and never respawns; non-`78` runtime crashes retain exponential backoff respawn.
- `run_server` returns `Err` when `shm_startup_failed` is set so `vertebra-server` exits non-zero in both embedded and SHM topologies.

## Test Matrix

- Rust unit tests in `supervisor.rs` (`78` halts immediately vs exit `1` respawns) plus integration test in `test_vertebra_server.py` verifying non-zero exit on failing lifespan in embedded and SHM modes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib workers && cargo build --manifest-path src/rs/Cargo.toml -p vertebra-http --bin vertebra-server && uv run --no-sync pytest src/py/tests/integration/server/test_vertebra_server.py -q
```

## Acceptance Criteria

- [ ] SHM worker exit `78` stops `vertebra-server` non-zero with zero respawn attempts.
- [ ] Transient worker exit `1` preserves backoff respawn.

[^supervisor-rs]: `src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor` — SHM worker poll and respawn supervisor.
[^main-rs]: `src/rs/vertebra-http/src/main.rs::run_server` — Server shutdown token and exit result handling.
