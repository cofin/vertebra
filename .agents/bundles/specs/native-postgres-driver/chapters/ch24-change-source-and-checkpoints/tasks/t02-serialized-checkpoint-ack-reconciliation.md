---
type: Task
id: "native-postgres-driver:ch24:t02"
chapter_id: ch24-change-source-and-checkpoints
title: Serialize checkpoint acknowledgements and uncertain outcomes
description: Implement shielded single-flight PostgresChangeSource.acknowledge, yield-gate generation barriers, PostgresReplicationGapError, and KnownClosed store-reload reconciliation.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch24:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/py/vertebra/db/adapters/postgres/change_stream.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/tests/unit/db/test_postgres_change_source.py
files:
  - src/py/vertebra/db/adapters/postgres/change_stream.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
tests:
  - src/py/tests/unit/db/test_postgres_change_source.py
estimated_loc: 235
plan_revision: 8
plan_commit: d90d4625
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
updated_at: "2026-11-12T04:25:15Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Shielded acknowledgement task, yield gate, and PostgresReplicationGapError contracts
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Uncertain checkpoint save reconciliation and generation barrier design
  - id: pg-init
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: Lazy package exports for PostgresReplicationGapError
tags:
  - postgres
  - cdc
  - checkpoints
  - cancellation
---

# Task t02: Serialize Checkpoint Acknowledgements and Uncertain Outcomes

## Objective & Scope Boundary

Add `PostgresReplicationGapError(OperationalError)` and `PostgresChangeSource.acknowledge(commit_end_lsn)` in `src/py/vertebra/db/adapters/postgres/change_stream.py` (exporting `PostgresReplicationGapError` in `__init__.py`) so durable checkpoint compare-and-swap always completes before protocol `StandbyStatusUpdate` feedback, caller cancellation via `asyncio.shield` never retracts a possibly committed save or leaves unhandled tasks, and any ambiguous store/native failure closes the active generation and reloads durable `(commit_end_lsn, next_event_id)` into a `KnownClosed` state [^npd-contracts] [^npd-proto-research] [^pg-init]. Config-controlled reconnect/reopen loops and multi-generation replay tests belong to `ch24:t03`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Shielded ack barrier, `PostgresReplicationGapError` reasons, and CAS conflict termination rules [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Delivery barrier and save-success/native-ack-failure race closure [^npd-proto-research].
- `src/py/vertebra/db/adapters/postgres/change_stream.py` — `PostgresChangeSource`, `PostgresCheckpointStore`, and `MemoryPostgresCheckpointStore` from `ch24:t01`.
- `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package export barrel [^pg-init].
- `src/py/tests/unit/db/test_postgres_change_source.py` — Unit test suite and `NativeReplicationDriverPort`.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/change_stream.py` & `__init__.py`:
  - `type ReplicationGapReason = Literal["identity_mismatch", "wal_unavailable", "checkpoint_uncertain", "retries_exhausted"]`
  - `class PostgresReplicationGapError(OperationalError):`
    - Attributes: `reason: ReplicationGapReason`, optional `sqlstate: str | None` preserved from underlying `DatabaseError` / `__cause__`.
  - `PostgresChangeSource.acknowledge(self, commit_end_lsn: int) -> None` (async):
    1. Under a short non-I/O state guard (never held across `store.save`, `store.load`, `handle.acknowledge`, or `handle.next`):
       - Reject `bool`, negative/`>= 2**64` integers, concurrent in-flight `_ack_task` (`InterfaceError`), closed/inactive stream, non-commit LSNs, future LSNs, and LSNs `< known_checkpoint.commit_end_lsn` (`ValueError` / `InterfaceError`).
       - Return `None` idempotently without I/O when `commit_end_lsn == known_checkpoint.commit_end_lsn` and no ack is uncertain/in-flight.
       - Capture exact `(commit_end_lsn, snapshot_next_event_id)` recorded when that `COMMIT` event yielded (NEVER `self._next_event_id` after subsequent `B`/`C` yields), close the yield gate (`_yield_gate.clear()`), and spawn ONE source-owned tracked `_ack_task`.
    2. Caller awaits `asyncio.shield(self._ack_task)`: if caller is cancelled, `CancelledError` propagates to caller while `_ack_task` continues running under source ownership with a done-callback / stored outcome so Python's event loop never logs an unhandled task exception and the next `stream()` turn or `aclose()` surfaces any stored failure.
    3. Inside `_ack_task`:
       - First `await asyncio.wait_for(self._checkpoint_store.save(target_cp, expected=expected_cp), timeout=self._config.checkpoint_timeout)`.
       - Only after `save` returns cleanly, `await asyncio.wait_for(handle.acknowledge(commit_end_lsn), timeout=self._config.checkpoint_timeout)`.
       - On both succeeding: update `known_checkpoint = target_cp`, prune emitted commit snapshots `<= commit_end_lsn`, reopen `_yield_gate`, and preserve at most `1` pending completed `handle.next()` result in exact stream order.
       - On ANY `save` error, `handle.acknowledge` error, or internal cancellation: invalidate the generation, close `handle`, run bounded `store.load(identity_key)` (never assuming a failed/cancelled `save` did not commit), and install `(reloaded_checkpoint, reloaded_next_event_id)` atomically in `KnownClosed` state with zero further old-generation yields. `PostgresCheckpointConflictError` also closes/reloads and terminates stream ownership permanently without adopting another writer's unseen events. Chain any reload/close failure onto the primary exception.
  - Basic `async def aclose(self) -> None`: joins or cancels+reloads `_ack_task` within `checkpoint_timeout`, closes `handle`, and transitions to `Closed` with zero detached background tasks.

## Edge Cases & Error Semantics

- If transaction `A` (`next_event_id = 10` after `A.COMMIT`) and transaction `B` (`next_event_id = 15` after `B.COMMIT`) have both yielded, `await source.acknowledge(A.commit_end_lsn)` MUST persist `next_event_id = 10` (the snapshot captured at `A.COMMIT`), NOT `15`.
- If `store.save` succeeds for `A` and `handle.acknowledge` fails, the old native handle (still at pre-`A` feedback LSN) is closed immediately and NEVER reused in the old generation.
- While `_yield_gate` is closed during `_ack_task`, `stream()` does not yield any new `ChangeEvent`; if `handle.next()` completes during a successful ack barrier, that single buffered record is yielded in order once `_yield_gate` reopens, whereas if `_ack_task` reconciles to `KnownClosed`, that stale-generation record is discarded.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_change_source.py` (`-k 'ack_ or checkpoint_save'`, using `asyncio.Event` barriers and scripted peers — zero `asyncio.sleep` timing races).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Strict save-before-feedback ordering & snapshot counter accuracy | Yield `TxA` (`commit_end_lsn=0x1_0000_0200`, post-commit ID `5`) and `TxB` (`commit_end_lsn=0x1_0000_0300`, post-commit ID `9`); block `store.save` on `Event`; call `acknowledge(0x1_0000_0200)` | While `store.save` is blocked on `Event`, scripted wire peer observes ZERO `'r'` feedback advancement; releasing `Event` saves `(lsn=0x1_0000_0200, next_event_id=5)` (NOT `9`) and then advances wire `flush_lsn` to `0x1_0000_0200`. |
| Caller cancellation during in-flight `acknowledge` (`asyncio.shield`) | Block `store.save` on `Event`, cancel caller task awaiting `source.acknowledge(0x1_0000_0200)`, then release `Event` | Caller receives `CancelledError`; shielded `_ack_task` finishes `save` + wire `acknowledge`, updates `known_checkpoint`, logs zero unhandled loop exceptions, and keeps stream valid (or surfaces stored error if `save` failed). |
| Save succeeds then native `acknowledge` fails | `store.save` commits `(0x1_0000_0200, 5)`, scripted `handle.acknowledge` raises `ConnectionFailure` | Generation invalidated, `handle.closed` becomes `True`, `store.load` reloads `(0x1_0000_0200, 5)` into `KnownClosed`, zero post-error old-generation events yield, and `ConnectionFailure` propagates. |
| Ambiguous `store.save` commits then raises `CancelledError` / `OSError` | Custom `PostgresCheckpointStore.save` mutates durable state to `(0x1_0000_0200, 5)` then raises `OperationalError` | Wire `handle.acknowledge` is NOT called; reconciliation reloads `(0x1_0000_0200, 5)` from `store.load` rather than assuming `save` rolled back. |
| Concurrent `acknowledge` & CAS conflict termination | Call second `acknowledge` while first `_ack_task` is active; separately inject external CAS writer causing `PostgresCheckpointConflictError` | Concurrent `acknowledge` immediately raises `InterfaceError`; CAS conflict closes stream, reloads store, and raises `PostgresCheckpointConflictError` without continuing stream. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k 'ack_ or checkpoint_save' -q`
- Final Green + Lint + Stubtest + Typecheck gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/change_stream.py src/py/vertebra/db/adapters/postgres/__init__.py src/py/tests/unit/db/test_postgres_change_source.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/change_stream.py src/py/vertebra/db/adapters/postgres/__init__.py src/py/tests/unit/db/test_postgres_change_source.py
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k 'ack_ or checkpoint_save' -q
  ```

## Acceptance Criteria

- [ ] `PostgresChangeSource.acknowledge` enforces single-flight CAS `store.save` strictly before `handle.acknowledge` using the exact `COMMIT`-time `next_event_id` snapshot (R22, R23).
- [ ] Shielded `_ack_task`, yield-gate barrier, and `KnownClosed` store reload prevent old-LSN/new-ID drift, unhandled background tasks, and post-conflict ownership continuation (R22, R23).
- [ ] Commit message follows `fix(postgres): serialize durable checkpoint acknowledgement ownership` once verified.

## Notes & Discoveries

- Transitioning ambiguous ack failures to `KnownClosed` makes `ch24:t02` completely self-contained before `ch24:t03` adds config-controlled reconnect and replay over `KnownClosed` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Shielded acknowledgement task, yield gate, and PostgresReplicationGapError contracts.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Uncertain checkpoint save reconciliation and generation barrier design.
[^pg-init]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package exports for PostgresReplicationGapError.
