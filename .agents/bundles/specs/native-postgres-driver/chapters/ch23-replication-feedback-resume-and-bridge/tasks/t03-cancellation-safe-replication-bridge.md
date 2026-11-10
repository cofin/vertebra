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
plan_revision: 5
plan_commit: fd571e3a
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
updated_at: "2026-11-08T02:05:22Z"
commit: null
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
  - `def next(self) -> Awaitable[PostgresReplicationRecord | None]`: rejects concurrent overlapping `next()` calls with `InterfaceError`; schedules onto Tokio via `spawn_rust_db_task_with_attr`; inside the event-loop `DbOutcomeProducer` callback (never on a Tokio worker thread), checks `future.cancelled()` and handle `closed` BEFORE committing publication of `Commit` LSN to `ReplicationFeedback::record_published_commit` and converting owned Rust fields into `PostgresReplicationRecord` (hydrating `PG_UNCHANGED_TOAST` and lossless `PgInterval`/`PgArray`/`PgComposite`/`PgTemporal`/`PgJson`/`PgBitString`/`PgVector`).
  - `def acknowledge(self, commit_end_lsn: int) -> Awaitable[None]`: validates `0 <= commit_end_lsn < 2**64` (rejecting `bool`), forwards to `ReplicationSupervisor::acknowledge`, and schedules `StandbyStatusUpdate` flush without claiming Python checkpoint durability.
  - `def aclose(self) -> Awaitable[None]`, `__aenter__`, `__aexit__`: idempotent; cancels pending read/write/backoff waiters, sends final current feedback + `CopyDone` under `connect_timeout` budget, drains/drops bounded queue, joins worker tasks, and sets `closed = True`.
- `src/rs/vertebra-postgres/src/async_client.rs` & `src/py/vertebra/_postgres.pyi`:
  - Because raw `crate::config::PostgresConfig` does not carry `pooler_mode` (`PoolerMode` lives in `crate::session::statements::PoolerMode` / `SessionHandle` and Python `PostgreSQLConfig.derive_direct_config()`), `AsyncPostgresClient` rejects non-direct/session `PoolerMode` (`Transaction` / `Statement`) with `InterfaceError` / `ImproperConfigurationError` before dialing dedicated non-pooled `replication=database` connections.
  - `async def replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity` — runs `PostgresReplicationConnection::probe_identity` using this client's validated `crate::config::PostgresConfig` without `START_REPLICATION`, closes probe sockets on success, error, or cancellation, and returns `PostgresReplicationIdentity`.
  - `async def open_replication(self, slot: str, publication: str, *, start_lsn: int | None = None, queue_capacity: int = 1024, reconnect: bool = True) -> AsyncPostgresReplication` — validates `queue_capacity >= 1` and `u64` `start_lsn` (rejecting `bool`), opens `ReplicationSupervisor` with `crate::config::PostgresConfig`, and returns `AsyncPostgresReplication`.

## Edge Cases & Error Semantics

- Cancelling a `next()` `Future` while its completion is already queued onto the Python event loop MUST NOT mark a `Commit` record as published in `ReplicationFeedback` and MUST retain or cleanly close stream state without silently advancing ackability; if `aclose()` follows, no late ack or post-close publication can occur.
- `replication_identity(slot, publication)` cancelled mid-flight aborts the probe task and closes its short-lived sockets to physical EOF without sending `START_REPLICATION`.
- Calling `next()` or `acknowledge()` after `closed == True` raises canonical `InterfaceError`; calling `aclose()` repeatedly returns `None` idempotently.
- Never call `Python::attach` inside Tokio tasks; all Python object construction (`PostgresReplicationIdentity`, `PostgresReplicationRecord`, `PG_UNCHANGED_TOAST`) occurs inside the event-loop `DbOutcomeProducer`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_postgres_change_source.py` (`-k native_replication`) and `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Identity preflight without `START_REPLICATION` | `ident = await client.replication_identity("vertebra_slot", "vertebra_pub")` against scripted peer | Peer observes catalog query + `IDENTIFY_SYSTEM` + clean EOF (zero `START_REPLICATION` frames); `ident.identity_key == ("7399001122334455667", "appdb", "vertebra_slot", ident.publication_fingerprint)`. |
| Lossless `next()` & `control` record ordering | `open_replication("vertebra_slot", "vertebra_pub", start_lsn=0x1_0000_0100, reconnect=False)` receiving `Begin`, `Update` (with TOAST `'u'` + `Decimal("9.50")`), `Commit(0x1_0000_0220)` | First `next()` returns `PostgresReplicationRecord(kind="control", generation=0, ...)`; subsequent calls return `"begin"`, `"update"` (with `PG_UNCHANGED_TOAST` and `Decimal("9.50")`), and `"commit"` (`commit_end_lsn=0x1_0000_0220`); `handle.closed is False`. |
| Queued `next()` cancellation fence | Peer delivers `Commit(0x1_0000_0300)`; cancel caller task when completion is queued before loop turn; then inspect `acknowledge(0x1_0000_0300)` / `aclose()` | Cancelled `Commit` is NOT marked published (`acknowledge(0x1_0000_0300)` rejected); `await handle.aclose()` sets `handle.closed is True` (and second `await handle.aclose()` is idempotent) with zero late wire ack for `0x1_0000_0300`. |
| Idle keepalive + full queue responsiveness | Fill `queue_capacity=2` without calling `next()`, send peer `'k'` (`reply_requested=1`) while Python event loop runs | Event loop stays responsive, peer receives `'r'` status frame promptly with unchanged `flush_lsn`, and `handle.closed` stays `False`. |
| `closed` transitions across raw reconnect vs terminal error | Drop peer socket under `reconnect=True` (recoverable) vs `reconnect=False` (terminal) | Under `reconnect=True`, `handle.closed` stays `False` across reconnect and yields `kind="control", generation=1`; under `reconnect=False`, `next()` raises `ConnectionFailure` and `handle.closed` becomes `True`. |

## Verification

- Initial Red baseline: `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_change_source.py -k native_replication -q`
- Final Green + Stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k native_replication -q
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
  ```

## Acceptance Criteria

- [ ] `AsyncPostgresClient.replication_identity` and `AsyncPostgresClient.open_replication` plus `AsyncPostgresReplication` (`closed`, `next`, `acknowledge`, `aclose`, async context manager) are exposed in Rust and verified by `mypy.stubtest vertebra._postgres` (R22, R23).
- [ ] Queued completion cancellation never marks unseen `Commit` boundaries published, Tokio workers never call `Python::attach`, and `closed` accurately reflects active, reconnecting, and terminal states without I/O (R22, R23).
- [ ] Commit message follows `feat(postgres): expose explicit replication identity and handles` once verified.

## Notes & Discoveries

- `AsyncPostgresReplication.acknowledge` updates protocol `StandbyStatusUpdate` positions only; durable checkpoint compare-and-swap before protocol feedback is enforced by `PostgresChangeSource` in Chapter 24 [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Dedicated event ownership, closed property, and replication handle signatures.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Independent AsyncCompletionQueue drainer and cancellation-safe outcome producer.
[^ffi-discipline]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 gil_used=false and worker-thread detachment invariants.
