---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch07-async-bridge-and-client-sessions
title: "Chapter 07: Async Completion Setup-Abort Guard and Physical Native PostgreSQL Clients"
description: Add armed RAII worker-abort ownership across fallible Python completion setup in vertebra-db and expose SyncPostgresClient and AsyncPostgresClient physical sessions in vertebra._postgres with GIL-detached sync I/O and cancellation-safe async publication.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch06-alloydb-keepalive-unix-and-direct-iam
replan_trigger: null
current_task: null
plan_revision: 5
plan_commit: 0147d80c
state_revision: 0
last_verified_checkpoint: adf34226
last_reviewed_commit: adf34226
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: 0147d80c
sources:
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared Tokio database worker spawner and AsyncCompletionQueue drainer
  - id: core-notify
    resource: src/rs/vertebra-core/src/notify.rs::AsyncCompletionQueue
    title: Lock-free completion queue and eventfd/pipe Notifier wakeup
  - id: spanner-py
    resource: src/rs/vertebra-spanner/src/lib.rs::PySyncSpannerClient
    title: Reference optional-wheel PyO3 client freelist and immutable_type patterns
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Complete 25-parameter native SyncPostgresClient and AsyncPostgresClient constructor contract
tags:
  - postgres
  - pyo3
  - async
  - client
  - rust
---

# Chapter 07: Async Completion Setup-Abort Guard and Physical Native PostgreSQL Clients

<!-- truth: start -->
- **Armed Setup-Abort Worker Ownership**: `spawn_rust_db_task_with_attr` in `src/rs/vertebra-db/src/py_async.rs` wraps every spawned `JoinHandle` in a single RAII abort-on-drop guard immediately after `db_runtime_handle().spawn(fut)` and only disarms inside the completion monitor after worker join, guaranteeing zero leaked workers or `PendingSession` sockets if `Py::new(DbTaskCanceller)` or `add_done_callback` raises [^py-async-rs] [^core-notify].
- **Detached Synchronous Client (`SyncPostgresClient`)**: `PySyncPostgresClient` (`frozen`, `immutable_type`, conditional `freelist = 128` under `not(Py_GIL_DISABLED)`) exposes the exact 25-parameter keyword-only constructor from `contracts.md`, copies `Mapping[str, str]` inputs while attached, drives `connect()` and `close()` inside `py.detach(|| block_on_db(...))`, and maps errors to canonical `vertebra._vertebra` exceptions [^spanner-py] [^pg-contracts].
- **Cancellation-Safe Asynchronous Client (`AsyncPostgresClient`)**: `PyAsyncPostgresClient` uses dedicated loop attribute `_vertebra_postgres_completion_drainer`, reserves `begin_connect` synchronously, commits `PendingSession` exclusively inside `map_ok` on the Python event loop thread (rolling back to `Closed` if cancelled before or while queued in `DbCompletionRing`), and never calls `Python::attach` on Tokio workers [^py-async-rs] [^pg-contracts].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` currently calls `abort.abort()` in two separate manual error branches before spawning its completion monitor task, rather than owning the `JoinHandle` through a unified RAII abort-on-drop guard that covers every fallible Python setup point until the monitor completes [^py-async-rs]. Building on that hardened async bridge and the complete PostgreSQL transport/authentication/cloud/Unix engine from Chapters 01–06, Chapter 07 exposes the public `SyncPostgresClient` and `AsyncPostgresClient` PyO3 classes and `src/py/vertebra/_postgres.pyi` type stubs with full 25-parameter constructors, GIL-detached synchronous session lifecycle, and cancellation-safe asynchronous session publication [^pg-contracts].

### 1.2 Non-Goals

- Exhaustive Python constructor parity characterization and isolated free-threaded wheel/GIL CI verification (owned by Chapter 08).
- Query execution (`query`, `query_multi`), transactions, physical connection pools, portals, pipelines, `COPY`, Arrow, notifications, and logical replication (owned by Chapters 09–24).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), and [pyo3-and-python-runtime.md](../../../../knowledge/gotchas/pyo3-and-python-runtime.md). Note that `errors.rs::postgres_error_to_py` (`PyOnceLock` cache over `vertebra._vertebra` exception classes) was introduced in `ch05:t02` and is exercised end-to-end across the Python FFI seam in `ch07:t02` and `ch07:t03`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R7 (`ch07:t01`)**: Replace manual `abort.abort()` error branches in `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` with one private armed abort-on-drop worker guard wrapping `JoinHandle<Result<T, E>>` immediately after `spawn`, moving the armed guard into the monitor task and disarming only after `worker_handle.await` finishes while preserving original `PyErr` values.
- **R6 (`ch07:t02`)**: Implement `PySyncPostgresClient` (`SyncPostgresClient` in `vertebra._postgres`) in `src/rs/vertebra-postgres/src/sync_client.rs`, register it in `lib.rs`, and declare `@final` `SyncPostgresClient` in `src/py/vertebra/_postgres.pyi` with the complete 25-parameter keyword-only `def __new__(cls, *, ...) -> SyncPostgresClient`, `connect() -> None`, `close() -> None`, `closed: bool`, and `parameters: dict[str, str]` (preserving `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool`, without exposing `BackendKeyData`).
- **R7 (`ch07:t03`)**: Implement `PyAsyncPostgresClient` (`AsyncPostgresClient` in `vertebra._postgres`) in `src/rs/vertebra-postgres/src/async_client.rs`, register it alongside `DbCompletionDrainer` (`_vertebra_postgres_completion_drainer`) in `lib.rs`, and declare `@final` `AsyncPostgresClient` in `src/py/vertebra/_postgres.pyi` (`def __new__(cls, *, ...) -> AsyncPostgresClient`, `connect() -> Awaitable[None]`, `close() -> Awaitable[None]`, `closed: bool`, `parameters: dict[str, str]`, preserving `GIL_USED: bool` alongside `FREE_THREADED: bool`).

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator Map**:
  - Python `SyncPostgresClient(...)` -> `PostgresConfig::validate_and_normalize` (pure, attached `Mapping[str, str]` copy) -> `connect()` -> `SessionHandle::begin_connect` -> `py.detach(|| block_on_db(attempt.run()))` -> `PendingSession::commit` / `postgres_error_to_py`.
  - Python `await AsyncPostgresClient(...).connect()` -> `SessionHandle::begin_connect` -> `spawn_rust_db_task_with_attr(..., "_vertebra_postgres_completion_drainer", attempt.run(), |py, pending| pending.commit()...)` -> `RustDbTaskAwaitable`.
- **Rust (`src/rs/vertebra-db/src/py_async.rs`)**:
  - `struct ArmedWorkerGuard<T>(Option<JoinHandle<T>>)` (`impl Drop` calls `handle.abort()`; `async fn join_and_disarm(mut self) -> Result<T, tokio::task::JoinError>`).
- **Python (`src/py/vertebra/_postgres.pyi` & `vertebra._postgres`)**:
  - Preserved module-level constants `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool` (`__all__ = ["FREE_THREADED", "GIL_USED", "AsyncPostgresClient", "SyncPostgresClient", "__version__"]`).
  - `@final class SyncPostgresClient:` and `@final class AsyncPostgresClient:` sharing the exact 25 keyword-only `def __new__(cls, *, ...)` parameters (`user`, `database`, `host="localhost"`, `port=5432`, `password=None`, `unix_socket=None`, `sslmode="verify-full"`, `ca_pem=None`, `client_cert_pem=None`, `client_key_pem=None`, `server_name=None`, `channel_binding="prefer"`, `connect_timeout=10.0`, `application_name="vertebra"`, `session_gucs=None`, `keepalives=True`, `keepalives_idle=None`, `keepalives_interval=None`, `keepalives_count=None`, `cloud_sql_instance=None`, `cloud_sql_ip_type="PRIVATE"`, `alloydb_instance_uri=None`, `alloydb_ip_type="PRIVATE"`, `enable_iam_auth=False`, `cloud_auth_options=None`, `iam_auth_options=None`) returning `SyncPostgresClient` / `AsyncPostgresClient` for `#[pyclass(frozen, immutable_type)]` `mypy.stubtest` compatibility.
  - Properties on both classes: `@property def closed(self) -> bool: ...` and `@property def parameters(self) -> dict[str, str]: ...` (returning a fresh dict copy from `session.parameters()` on every read; never exposing process ID or cancel secret key).

### 2.3 Edge-Case & Failure-Mode Matrix

| Condition | Behavior & Error Mapping |
| --- | --- |
| `Future.add_done_callback` raises `RuntimeError` during `spawn_rust_db_task_with_attr` | Original `RuntimeError` returned to Python; `ArmedWorkerGuard::drop` aborts worker; captured `PendingSession` / `DropFlag` drops without running `map_ok`; subsequent retry sees `Closed` (not stuck `Connecting`). |
| Calling `connect()` on an already `Ready` client | `begin_connect` returns `BeginConnect::AlreadyReady`; sync returns `None`, async returns a ready `Awaitable[None]` owning zero socket/guard so cancelling that awaitable preserves the live `Ready` session. |
| Overlapping `connect()` while `Connecting`, or `connect()` after `Closed` | Raises `vertebra._vertebra.InterfaceError` (`sqlstate = None`). |
| Async `connect()` completes into `DbCompletionRing` at the exact instant Python cancels the `Future` | `DbCompletionDrainer.__call__` sees `fut.cancelled() == True`, skips `produce` (`map_ok`), drops `CompletedDbTask` -> drops `PendingSession` -> rolls back session state to `Closed` and closes socket cleanly. |
| Calling `close()` twice or cancelling `await client.close()` | State transitions to `Closed` and takes `TransportStream` *before* detached/async network shutdown, so repeated `close()` is idempotent and cancelling `close()` never restores `Ready`. |

### 2.4 Alternatives Comparison

| Option | Decision & Rationale |
| --- | --- |
| Unique `_vertebra_postgres_completion_drainer` loop attribute vs reusing `_vertebra_db_completion_drainer` | **Chosen: `_vertebra_postgres_completion_drainer`**. Separate `cdylib` wheels have distinct `DbCompletionDrainer` PyO3 type objects; sharing one loop attribute name would cause `cast::<DbCompletionDrainer>()` collisions when multiple extensions attach to the same event loop. |
| Commit `Ready` inside Tokio worker vs returning `PendingSession` into `map_ok` | **Chosen: Commit inside `map_ok` on event-loop thread**. Prevents races where a worker publishes `Ready` onto `SessionHandle` milliseconds after the caller's `asyncio.Task` was cancelled. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 file stubbed in ch07]` (`src/py/vertebra/_postgres.pyi` updated with `SyncPostgresClient` and `AsyncPostgresClient` signatures in `ch07:t02` and `ch07:t03`; user guides and OKF data-domain chapters update in `ch30:t01` and `ch30:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R7 | [t01](tasks/t01-async-bridge-setup-abort.md) | `regression_tdd` | `src/rs/vertebra-db/src/py_async.rs` |
| R6 | [t02](tasks/t02-sync-postgres-client.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_driver.py` |
| R7 | [t03](tasks/t03-async-postgres-client.md) | `regression_tdd` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/rs/vertebra-db/src/py_async.rs` |

## 5. Implementation Plan

- [x] [adf34226] native-postgres-driver:ch07:t01 — Abort shared database workers when Python completion setup fails (tasks/t01-async-bridge-setup-abort.md)
- [x] [adf34226] native-postgres-driver:ch07:t02 — Expose physical synchronous native PostgreSQL sessions (tasks/t02-sync-postgres-client.md)
- [x] [adf34226] native-postgres-driver:ch07:t03 — Expose async physical sessions with cancellation-safe publication (tasks/t03-async-postgres-client.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests && make dev && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_ -q && uv run --no-sync python -m mypy.stubtest vertebra._postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: 0147d80c`; chapter `state: completed` at `adf34226`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: None (`3/3` completed at `adf34226`).
- **Downstream Dependents**: Unblocks `ch08-constructor-parity-and-wheel-gil`.

[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared Tokio database worker spawner and event-loop completion drainer.
[^core-notify]: `src/rs/vertebra-core/src/notify.rs::AsyncCompletionQueue` — Lock-free completion queue and non-blocking notifier.
[^spanner-py]: `src/rs/vertebra-spanner/src/lib.rs::PySyncSpannerClient` — Reference optional-wheel PyO3 client attributes and GIL-detached execution.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL constructor and lifecycle contracts.
