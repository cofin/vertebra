---
type: Task
id: "server-runtime-correctness:ch04:t03"
chapter_id: ch04-lifespan-fail-loud
title: Stop the Rust supervisor and server on worker startup failure
description: Record worker exit codes in WorkerLifecycle, treat exit code 78 as a fatal startup failure in Supervisor::poll_workers (no respawn, startup_failed flag, shutdown cancel), and make run_server return an error so vertebra-server exits non-zero in SHM topology.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch04:t02"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch04-lifespan-fail-loud/spec.md
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/rs/vertebra-http/src/workers/supervisor.rs
  - src/rs/vertebra-http/src/main.rs
  - src/py/tests/integration/server/test_vertebra_server.py
files:
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/rs/vertebra-http/src/workers/supervisor.rs
  - src/rs/vertebra-http/src/main.rs
tests:
  - src/rs/vertebra-http/src/workers/supervisor.rs
  - src/py/tests/integration/server/test_vertebra_server.py
estimated_loc: 150
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: factory-rs
    resource: src/rs/vertebra-http/src/workers/factory.rs::WorkerLifecycle
    title: WorkerLifecycle check_alive, record_crash, respawn_delay, circuit_breaker_tripped
  - id: rust-supervisor-rs
    resource: src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor
    title: Supervisor spawn, poll_workers, process_worker_exit, and tests
  - id: main-rs
    resource: src/rs/vertebra-http/src/main.rs::run_server
    title: SHM topology Supervisor::spawn call and run_server error return
tags:
  - rust
  - supervisor
  - fail-loud
---

# Task t03: Stop the Rust Supervisor and Server on Worker Startup Failure

## Objective & Scope Boundary

Capture the exit status in `WorkerLifecycle::check_alive`, add `EXIT_STARTUP_FAILURE = 78`, and change `Supervisor::{spawn,poll_workers,process_worker_exit}` so a worker exiting `78` is marked `WorkerHealth::Dead` without respawn, sets a shared `startup_failed` flag, and cancels the server shutdown token. `run_server` in `main.rs` passes the flag and returns `Err("python worker startup failed")` when set. Out of scope: embedded topology (already exits non-zero through `worker_failed`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-http/src/workers/factory.rs::{WorkerLifecycle,WorkerLifecycle::check_alive,WorkerLifecycle::record_crash,WorkerHealth}` [^factory-rs].
- `src/rs/vertebra-http/src/workers/supervisor.rs::{Supervisor::spawn,Supervisor::poll_workers,Supervisor::handle_worker_exit,Supervisor::process_worker_exit}` and `mod tests` [^rust-supervisor-rs].
- `src/rs/vertebra-http/src/main.rs::run_server` — `Supervisor::spawn(Arc::clone(&factory), Arc::clone(&shm), server_shutdown_token.clone())` call and the final `embedded_worker_failed` check [^main-rs].
- `src/py/tests/integration/server/test_vertebra_server.py` — SHM startup-failure case added here.

## Symbol & Signature Contracts

- `src/rs/vertebra-http/src/workers/factory.rs`:
  - `pub const EXIT_STARTUP_FAILURE: i32 = 78;` re-exported from `workers/mod.rs` only if the module already re-exports factory items (otherwise reachable as `workers::factory::EXIT_STARTUP_FAILURE`).
  - `WorkerLifecycle` gains `last_exit_code: Option<i32>`; `check_alive` stores `status.code()` on exit; `pub fn last_exit_code(&self) -> Option<i32>`; `pub fn exited_with_startup_failure(&self) -> bool`.
- `src/rs/vertebra-http/src/workers/supervisor.rs`:
  - `pub fn spawn(factory, shm, shutdown: CancellationToken, startup_failed: Arc<AtomicBool>) -> JoinHandle<()>`.
  - `pub async fn poll_workers(factory, shm, shutdown: &CancellationToken, startup_failed: &AtomicBool)`.
  - `process_worker_exit` (still sweeps in-flight requests with `502` and reclaims rings and leases) then: `exited_with_startup_failure()` → `state = WorkerHealth::Dead`, `error!(worker_id, "worker startup failed; stopping server")`, `startup_failed.store(true, Release)`, `shutdown.cancel()`; otherwise the existing crash/backoff/circuit-breaker logic.
  - `handle_worker_exit` keeps its signature and passes a local token and flag (test helper path).
- `src/rs/vertebra-http/src/main.rs::run_server`: create `let shm_startup_failed = Arc::new(AtomicBool::new(false));`, pass it to `Supervisor::spawn`, and after shutdown return `Err("python worker startup failed".into())` when it is set (checked before the embedded failure check).

## Edge Cases & Error Semantics

- A worker exiting `78` after running more than `60s` is still fatal (startup code is only used for startup failures).
- Signal-terminated workers (`status.code() == None`) keep backoff respawn.
- Multiple workers failing concurrently cancel the token once; the flag is idempotent.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-http/src/workers/supervisor.rs` (`mod tests`, children spawned with `Command::new("sh").args(["-c", "exit 78"])`), `src/py/tests/integration/server/test_vertebra_server.py` (binary).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Startup failure stops | Lifecycle child `sh -c "exit 78"` (waited); `poll_workers(&factory, &shm, &token, &flag)` | `flag == true`, `token.is_cancelled()`, lifecycle `state == WorkerHealth::Dead`, `last_exit_code() == Some(78)` |
| Crash respawns | Child `sh -c "exit 1"` | `flag == false`, token not cancelled, `state == WorkerHealth::Respawning` |
| Existing lease reclaim | Existing `supervisor_poll_workers_reclaims_broker_leases_on_exit` updated to the new signature | Still reclaims `1` lease |
| SHM binary exit | `launch_server(app_source=<lifespan.startup.failed app>, topology="shm", workers=2, wait_ready=False)` | Process exits non-zero within `20s`; log contains `"worker startup failed; stopping server"` and no `"respawning worker"` line |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib workers::supervisor`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib workers
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-http --all-targets --features tls,compression -- -D warnings
  make dev
  uv run --no-sync pytest src/py/tests/integration/server/test_vertebra_server.py -q -k startup
  ```
- Planned local commit: `fix(http): stop supervisor and server on worker startup failure`

## Acceptance Criteria

- [ ] Exit code `78` stops respawning, cancels shutdown, and makes `vertebra-server` exit non-zero (R10).
- [ ] Other exit codes keep crash backoff and circuit-breaker behaviour (R10).

## Notes & Discoveries

[^factory-rs]: `src/rs/vertebra-http/src/workers/factory.rs::WorkerLifecycle` — `WorkerLifecycle` `check_alive`, `record_crash`, `respawn_delay`, `circuit_breaker_tripped`.
[^rust-supervisor-rs]: `src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor` — `Supervisor` `spawn`, `poll_workers`, `process_worker_exit`, and tests.
[^main-rs]: `src/rs/vertebra-http/src/main.rs::run_server` — SHM topology `Supervisor::spawn` call and `run_server` error return.
