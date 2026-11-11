---
type: Task
id: "native-postgres-driver:ch23:t03"
chapter_id: ch23-replication-feedback-resume-and-bridge
title: Expose cancellation-safe native replication handles
description: Expose AsyncPostgresClient.replication_identity, AsyncPostgresClient.open_replication, and cancellation-safe AsyncPostgresReplication PyO3 handles and type stubs.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch23:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/vertebra/db/adapters/postgres/types.py
  - .agents/skills/pyo3-bridge/SKILL.md
files:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_change_source.py
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 240
plan_revision: 6
plan_commit: bf53faf3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T14:45:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: bf53faf3
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Dedicated event ownership, closed property, and replication handle signatures
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Independent AsyncCompletionQueue drainer and cancellation-safe outcome producer
  - id: ffi-discipline
    resource: .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
    title: PyO3 gil_used=false and worker-thread detachment invariants
tags:
  - postgres
  - replication
  - pyo3
  - cancellation
---

# Task t03: Expose Cancellation-Safe Native Replication Handles

## Objective & Scope Boundary

Expose `AsyncPostgresClient.replication_identity`, `AsyncPostgresClient.open_replication`, and the PyO3 `AsyncPostgresReplication` handle (`next`, `acknowledge`, `aclose`, `__aenter__`, `__aexit__`, read-only `closed`) across `src/rs/vertebra-postgres/src/replication/mod.rs`, `src/rs/vertebra-postgres/src/async_client.rs`, and `src/py/vertebra/_postgres.pyi` with cancellation-safe completion guards so cancelled or closed futures never mark `Commit` boundaries as published or leak background sockets [^npd-contracts] [^py-async-rs] [^ffi-discipline]. High-level Python `PostgresChangeSource` and `PostgresCheckpointStore` belong to Chapter 24.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — `AsyncPostgresReplication.closed`, `replication_identity`, and `open_replication` contracts [^npd-contracts].
- `src/rs/vertebra-postgres/src/replication/mod.rs` — `PostgresReplicationConnection::probe_identity` and `ReplicationSupervisor` from `ch22:t01` and `ch23:t02`.
- `src/rs/vertebra-postgres/src/async_client.rs` — `AsyncPostgresClient` PyO3 methods and `vertebra._postgres` completion drainer wiring.
- `src/py/vertebra/_postgres.pyi` — Public Python stub declarations for `vertebra._postgres`.
- `src/py/vertebra/db/adapters/postgres/types.py` — Pure-Python `PostgresReplicationIdentity`, `PostgresReplicationRecord`, and `PG_UNCHANGED_TOAST` from `ch22:t02`.
- [SKILL.md](../../../../../../skills/pyo3-bridge/SKILL.md) — `gil_used = false`, immutable PyO3 types, and event-loop completion conversion rules.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/replication/mod.rs`:
  - `#[pyclass(name = "AsyncPostgresReplication", module = "vertebra._postgres", frozen, immutable_type)] pub struct PyAsyncPostgresReplication { ... }` registered via `replication::register_pymodule(m)`.
  - `@property def closed(self) -> bool`: atomic `Ordering::Acquire` state read performing zero I/O; `False` while active or during recoverable raw `reconnect=True` backoff, `True` immediately once normal EOF, unrecoverable error, or `aclose()` finishes ownership termination.
  - `def next(self) -> Awaitable[PostgresReplicationRecord | None]`:
    - Rejects concurrent overlapping `next()` calls with `InterfaceError` and schedules onto Tokio via `spawn_rust_db_task_with_attr` (`RustDbTaskAwaitable`).
    - Wraps each popped `ReplicationStreamEvent` in an RAII `PendingDeliveryGuard` transferred into the event-loop `map_ok` (`DbOutcomeProducer`: `FnOnce(Python<'_>) -> PyResult<Py<PyAny>>`) closure without touching any nonexistent `Future` argument:
      - When `map_ok` runs on the Python event loop while `closed == false` and the supervisor generation still matches, it disarms the guard, invokes `ReplicationFeedback::record_published_commit(commit_end_lsn)` for `Commit` records, and constructs `PostgresReplicationRecord` (hydrating `PG_UNCHANGED_TOAST` and lossless `PgInterval`/`PgArray`/`PgComposite`/`PgTemporal`/`PgJson`/`PgBitString`/`PgVector`).
      - If a Tokio `next()` waiter is cancelled OR `DbCompletionDrainer` drops `map_ok` without invoking it because the caller cancelled the Python `asyncio.Future` while keeping `AsyncPostgresReplication` open, `PendingDeliveryGuard::drop` restores the unconsumed event via generation-fenced `supervisor.push_front_unconsumed(event)` and clears the single-waiter flag without calling `record_published_commit`, so the next `await handle.next()` receives that exact record in wire order.
  - `def acknowledge(self, commit_end_lsn: int) -> Awaitable[None]`: validates `0 <= commit_end_lsn < 2**64` (rejecting `bool`), forwards to `ReplicationSupervisor::acknowledge`, and schedules `StandbyStatusUpdate` flush without claiming Python checkpoint durability.
  - `def aclose(self) -> Awaitable[None]`, `def __aenter__(self) -> Awaitable[AsyncPostgresReplication]`, `def __aexit__(self, exc_type: Any = None, exc_val: Any = None, exc_tb: Any = None) -> Awaitable[None]`: idempotent; cancels pending read/write/backoff waiters, clears the `1`-slot unconsumed buffer and bounded queue, sends final current feedback + `CopyDone` under `connect_timeout` budget, joins worker tasks, and sets `closed = True`.
- `src/rs/vertebra-postgres/src/async_client.rs` & `src/py/vertebra/_postgres.pyi` (all `7` stub methods declared as `def ... -> Awaitable[T]`, never `async def`, matching `RustDbTaskAwaitable` and `mypy.stubtest vertebra._postgres`, importing `PostgresReplicationIdentity` and `PostgresReplicationRecord` cleanly in `_postgres.pyi`):
  - Both `AsyncPostgresClient` and `PostgresReplicationConnection` / `ReplicationSupervisor` check `PostgresConfig.pooler_mode` and reject `PoolerMode::Transaction`, `PoolerMode::Statement`, and `PoolerMode::Auto` (requiring explicit `PoolerMode::Direct | PoolerMode::Session`) with `InterfaceError` (`PostgresError::Interface`) before dialing any dedicated standalone preflight or `replication=database` socket (matching Python `PostgreSQLConfig.derive_direct_config()`). (Because `AsyncPostgresClient.__new__` defaults to `PoolerMode::Direct`, Python `pooler_mode` rejection tests acquire a client from an `AsyncPostgresPool` configured with `pooler_mode` in `("transaction", "statement", "auto")` and assert `0` standalone replication socket accepts when `replication_identity` or `open_replication` fails with `InterfaceError`.)
  - `def replication_identity(self, slot: str, publication: str) -> Awaitable[PostgresReplicationIdentity]` — runs `PostgresReplicationConnection::probe_identity` using this client's `crate::config::PostgresConfig` without `START_REPLICATION`, closes probe sockets on success, error, or cancellation, and returns `PostgresReplicationIdentity`.
  - `def open_replication(self, slot: str, publication: str, *, start_lsn: int | None = None, queue_capacity: int = 1024, reconnect: bool = True) -> Awaitable[AsyncPostgresReplication]` — validates `queue_capacity >= 1` and `u64` `start_lsn` (rejecting `bool`), opens `ReplicationSupervisor` with a pending-publication guard inside `map_ok` (closing the supervisor immediately if the `open_replication` `Future` is cancelled while queued on the drainer), and returns `AsyncPostgresReplication`.

## Edge Cases & Error Semantics

- Cancelling a `next()` wait on Tokio OR cancelling its Python `asyncio.Future` after the completion is queued on `DbCompletionDrainer` while keeping `handle` open MUST NOT call `record_published_commit` and MUST restore the popped `ReplicationStreamEvent` to `push_front_unconsumed` so `acknowledge(commit_lsn)` fails while unconsumed, the subsequent `await handle.next()` returns that exact record in order (enabling `acknowledge(commit_lsn)` afterward), and `await handle.aclose()` clears any unconsumed record with zero late wire acknowledgement.
- `replication_identity(slot, publication)` or `open_replication(...)` cancelled mid-flight or while queued on the drainer aborts/closes dedicated sockets to physical EOF without leaking background sessions.
- Calling `replication_identity` or `open_replication` on a pool-acquired client configured with `pooler_mode` in `("transaction", "statement", "auto")`, or calling `next()` / `acknowledge()` after `closed == True`, raises canonical `InterfaceError`; calling `aclose()` repeatedly returns `None` idempotently.
- Never call `Python::attach` inside Tokio tasks; all Python object construction (`PostgresReplicationIdentity`, `PostgresReplicationRecord`, `PG_UNCHANGED_TOAST`) occurs inside the event-loop `DbOutcomeProducer`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_postgres_change_source.py` (`-k native_replication`) and `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Identity preflight & `PoolerMode` rejection | Acquire clients from `AsyncPostgresPool` configured with `pooler_mode` in `("transaction", "statement", "auto")` and call `replication_identity` / `open_replication`; call `ident = await client.replication_identity("vertebra_slot", "vertebra_pub")` on a direct/session client against scripted peer | `("transaction", "statement", "auto")` pool-acquired clients raise `InterfaceError` with `0` standalone replication socket accepts; valid direct/session client observes catalog query + `IDENTIFY_SYSTEM` + clean EOF (zero `START_REPLICATION` frames) and returns matching `ident.identity_key`. |
| Lossless `next()` & `control` record ordering | `open_replication("vertebra_slot", "vertebra_pub", start_lsn=0x1_0000_0100, reconnect=False)` receiving `Begin`, `Update` (with TOAST `'u'` + `Decimal("9.50")`), `Commit(0x1_0000_0220)` | First `next()` returns `PostgresReplicationRecord(kind="control", generation=0, ...)`; subsequent calls return `"begin"`, `"update"` (with `PG_UNCHANGED_TOAST` and `Decimal("9.50")`), and `"commit"` (`commit_end_lsn=0x1_0000_0220`); `handle.closed is False`. |
| Cancelled `next()` retention (`PendingDeliveryGuard`) & `aclose()` fence | Peer delivers `Commit(0x1_0000_0300)` then `Begin(xid=9)`; cancel caller task after `Commit` completion is queued on drainer while keeping `handle` open; test `acknowledge(0x1_0000_0300)`, then call `await handle.next()` twice and `acknowledge`; separately cancel queued `Commit` followed directly by `await handle.aclose()` | Cancelled `Commit` is NOT marked published (`acknowledge(0x1_0000_0300)` rejected while unconsumed); next two `await handle.next()` calls return exact retained `Commit(0x1_0000_0300)` then `Begin(xid=9)` in order, after which `await handle.acknowledge(0x1_0000_0300)` succeeds; cancel-then-`aclose()` sets `closed is True` idempotently with zero late wire ack. |
| Idle keepalive + full queue responsiveness | Fill `queue_capacity=2` without calling `next()`, send peer `'k'` (`reply_requested=1`) while Python event loop runs | Event loop stays responsive, peer receives `'r'` status frame promptly with unchanged `flush_lsn`, and `handle.closed` stays `False`. |
| `closed` transitions across raw reconnect vs terminal error | Drop peer socket under `reconnect=True` (recoverable) vs `reconnect=False` (terminal) | Under `reconnect=True`, `handle.closed` stays `False` across reconnect and yields `kind="control", generation=1`; under `reconnect=False`, `next()` raises `ConnectionFailure` and `handle.closed` becomes `True`. |

## Verification

- Initial Red baseline: `make dev && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k native_replication -q`
- Final Green + Lint + Typecheck + Stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_change_source.py
  uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_change_source.py
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k native_replication -q
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
  ```

## Acceptance Criteria

- [ ] `AsyncPostgresClient.replication_identity`, `AsyncPostgresClient.open_replication`, and `AsyncPostgresReplication` (`closed`, `next`, `acknowledge`, `aclose`, `__aenter__`, `__aexit__`) reject `PoolerMode::Transaction | Statement | Auto` before dialing (verified via `AsyncPostgresPool`-acquired clients with `0` standalone replication socket accepts), use `def ... -> Awaitable[T]` stubs, and pass `ruff`, `mypy`, and `mypy.stubtest vertebra._postgres` (R22, R23).
- [ ] RAII `PendingDeliveryGuard` + generation-fenced `push_front_unconsumed` retain unconsumed cancelled `next()` events for ordered next-call delivery without prematurely advancing `record_published_commit`, Tokio workers never call `Python::attach`, and `closed` accurately reflects active, reconnecting, and terminal states without I/O (R22, R23).
- [ ] Commit message follows `feat(postgres): expose explicit replication identity and handles` once verified.

## Notes & Discoveries

- `AsyncPostgresReplication.acknowledge` updates protocol `StandbyStatusUpdate` positions only; durable checkpoint compare-and-swap before protocol feedback is enforced by `PostgresChangeSource` in Chapter 24 [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Dedicated event ownership, closed property, and replication handle signatures.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Independent AsyncCompletionQueue drainer and cancellation-safe outcome producer.
[^ffi-discipline]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 gil_used=false and worker-thread detachment invariants.
