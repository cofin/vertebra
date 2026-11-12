---
type: Task
id: "native-postgres-driver:ch24:t03"
chapter_id: ch24-change-source-and-checkpoints
title: Preserve change-source replay identity and shutdown semantics
description: Implement source-owned reconnect over reconnect=False native streams, deterministic replay event_id restoration, and absolute-deadline fail-closed aclose().
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch24:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/py/vertebra/db/adapters/postgres/change_stream.py
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/tests/unit/db/test_postgres_change_source.py
files:
  - src/py/vertebra/db/adapters/postgres/change_stream.py
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
    title: Source-owned retry, replay event_id invariance, and checkpoint_timeout aclose contract
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Replay deduplication coordinates and fail-closed uncertain checkpoint termination
  - id: cdc-py
    resource: src/py/vertebra/db/cdc.py::ChangeEvent
    title: ChangeEvent replay ID stability across reconnects
tags:
  - postgres
  - cdc
  - recovery
  - shutdown
---

# Task t03: Preserve Change-Source Replay Identity and Shutdown Semantics

## Objective & Scope Boundary

Complete `PostgresChangeSource` in `src/py/vertebra/db/adapters/postgres/change_stream.py` with a source-owned reconnect loop (`max 8` recoverable attempts, `0.1s..5.0s` jitter when `config.reconnect=True`) around `reconnect=False` native handles, exact `(commit_end_lsn, next_event_id)` replay restoration after transport drops or `ch24:t02` uncertain ack reconciliation, concurrent `stream()` refusal, and single-deadline `aclose()` failing closed with `PostgresReplicationGapError(reason="checkpoint_uncertain")` when checkpoint state remains unresolved [^npd-contracts] [^npd-proto-research] [^cdc-py]. Driver-level `_open_replication_stream` / `_replication_identity` hooks on `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` belong to `ch26:t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Reconnect retry limits, `PostgresReplicationGapError` reasons, and `aclose()` deadline rules [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Generation invalidation and replay counter restoration [^npd-proto-research].
- `src/py/vertebra/db/adapters/postgres/change_stream.py` — `PostgresChangeSource`, `_ack_task`, and `KnownClosed` reconciliation from `ch24:t01`–`t02`.
- `src/py/vertebra/db/adapters/postgres/events.py` — `PostgresReplicationDriver(Protocol)` (kept strictly protocol-typed with zero concrete legacy driver imports).
- `src/py/tests/unit/db/test_postgres_change_source.py` — `PostgresChangeSource` unit test suite.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/change_stream.py`:
  - Keep `self._driver: PostgresReplicationDriver` strictly typed against `events.PostgresReplicationDriver` throughout reconnect and shutdown.
  - `PostgresChangeSource.stream(self) -> AsyncIterator[ChangeEvent]`:
    - Reject concurrent `stream()` calls on the same `PostgresChangeSource` with `InterfaceError`.
    - Always call `self._driver._open_replication_stream(self._config, start_lsn=..., reconnect=False)`.
    - On recoverable transport failure (`ConnectionFailure` / transient `OperationalError`) or `KnownClosed` uncertain-ack reconciliation:
      - Close `_yield_gate`, increment source generation, cancel/join pending `handle.next()`, and `await handle.aclose()` under timeout.
      - Reload `loaded = await asyncio.wait_for(self._checkpoint_store.load(identity_key), timeout=self._config.checkpoint_timeout)` and install `_next_event_id = loaded.next_event_id if loaded is not None else 1` together with `resume_lsn = loaded.commit_end_lsn if loaded is not None else self._config.start_lsn`.
      - When `self._config.reconnect is False`: remain closed and surface the original failure (chaining any reload error) without opening a second generation.
      - When `self._config.reconnect is True`: retry up to `8` recoverable attempts (`0.1s..5.0s` exponential jitter, cancellable promptly on `aclose()`), re-verifying `await self._driver._replication_identity(slot, publication)` matches `identity_key` (mismatch raises terminal `PostgresReplicationGapError(reason="identity_mismatch")`), reopening with `reconnect=False`, validating the new generation `control` record, and resuming yields from the reloaded `(resume_lsn, _next_event_id)`. Exhausting `8` attempts raises `PostgresReplicationGapError(reason="retries_exhausted")` with the last transport error as `__cause__`.
  - `PostgresChangeSource.aclose(self) -> None` (and `async def __aenter__` / `async def __aexit__`):
    - Compute one absolute `deadline = loop.time() + self._config.checkpoint_timeout`, mark closing, gate all yields, and initiate zero new acknowledgements.
    - If `_ack_task` is in flight, shield-wait up to a bounded slice of remaining budget while reserving budget for cancellation + `store.load(identity_key)` + `handle.aclose()`.
    - On timeout waiting for `_ack_task`, cancel and join `_ack_task`, then perform bounded `store.load(identity_key)` within remaining deadline; if durable checkpoint state still cannot be established, mark the source permanently fail-closed (`_terminal_error` set, zero future yields/reopens/background tasks) and raise `PostgresReplicationGapError("Checkpoint state remained uncertain during shutdown", reason="checkpoint_uncertain")`.
    - Repeated `await source.aclose()` calls after clean shutdown are idempotent no-ops; repeated calls after `checkpoint_uncertain` re-raise or preserve terminal closed state without leaking tasks.

## Edge Cases & Error Semantics

- Terminal failures NEVER retry and NEVER reset or overwrite the checkpoint store: `PostgresCheckpointConflictError`, `PostgresReplicationGapError(reason="identity_mismatch")`, server WAL-removed / invalidated-slot errors mapped to `PostgresReplicationGapError(reason="wal_unavailable")` (preserving SQLSTATE), and auth/permission `DatabaseError`s.
- Short state lock discipline: never hold the internal state lock across `handle.next()`, `yield event`, `store.load`/`store.save`, or `handle.acknowledge`/`handle.aclose`; after `handle.next()` resolves, synchronously check generation + `_yield_gate` and assign `event_id` without an intervening `await`.
- Never discard a completed `handle.next()` result and continue yielding later events from that same generation: only a full generation close + checkpoint reload may discard buffered records so every unseen/unacknowledged event replays in order with its exact `event_id`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_change_source.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Deterministic `A` / `B` / `C` replay ID invariance | Yield `TxA` (events `1..3`), `TxB` (`4..6`), `TxC` (`7..9`); `acknowledge(TxA.lsn)` saves `(TxA.lsn, 4)` then `handle.acknowledge` fails; `config.reconnect=True` reopens against scripted peer | Every `_open_replication_stream` call receives `reconnect=False`; generation `0` yields zero further records; generation `1` starts at `TxA.lsn` and replays `TxB` as exact `event_id = 4..6` and `TxC` as `7..9` with identical dedup coordinates; a fresh `PostgresChangeSource` instance over the same store yields the exact same IDs. |
| Save commits then raises before reconnect | Store commits `(TxA.lsn, 4)` then raises `OperationalError` during `acknowledge`; `config.reconnect=True` | Reconnect reloads `(TxA.lsn, 4)` from store (not pre-`TxA` LSN `None`/`1`) and resumes at `TxA.lsn` with `event_id = 4`. |
| Terminal `identity_mismatch`, `wal_unavailable`, & `retries_exhausted` | Reconnect encounters changed `system_identifier` (`identity_mismatch`), server WAL-removed error (`wal_unavailable`), or `9` consecutive socket resets (`retries_exhausted`) | Raises `PostgresReplicationGapError` with exact `.reason` and preserved `__cause__`/SQLSTATE; store checkpoint remains untouched. |
| `config.reconnect=False` single-generation reconciliation | Socket drops after `TxA` under `config.reconnect=False` | Closes handle, reloads checkpoint into `KnownClosed`, opens zero replacement streams, and raises original `ConnectionFailure`. |
| `aclose()` deadline & `checkpoint_uncertain` fail-closed | Hang custom `store.save` and `store.load` past `checkpoint_timeout = 0.05`, invoke `await source.aclose()` | Cancels hung store ops within budget, closes native handle, leaves zero background tasks, marks source permanently closed, and raises `PostgresReplicationGapError(reason="checkpoint_uncertain")`. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -q`
- Final Green + Lint + Mypy + Rust gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/change_stream.py src/py/tests/unit/db/test_postgres_change_source.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/change_stream.py src/py/tests/unit/db/test_postgres_change_source.py
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -q
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k notification_source -q
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
  ```

## Acceptance Criteria

- [ ] `PostgresChangeSource` owns reconnect over `reconnect=False` native handles, restores exact `(commit_end_lsn, next_event_id)` replay IDs across `A`/`B`/`C` partial-ack failures, and rejects concurrent `stream()` calls (R22, R23).
- [ ] `aclose()` enforces one absolute `checkpoint_timeout` budget across in-flight ack join/cancel, store reload, and handle closure, raising `PostgresReplicationGapError(reason="checkpoint_uncertain")` fail-closed when durability remains unresolved (R22, R23).
- [ ] Commit message follows `fix(postgres): reconcile durable checkpoints before source reconnect` once verified.

## Notes & Discoveries

- Non-transactional logical messages (`pg_logical_emit_message(false, ...)`) are yielded immediately as `MESSAGE` events, while slot WAL retention advances only at acknowledged `COMMIT` boundaries [^npd-proto-research].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Source-owned retry, replay event_id invariance, and checkpoint_timeout aclose contract.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Replay deduplication coordinates and fail-closed uncertain checkpoint termination.
[^cdc-py]: `src/py/vertebra/db/cdc.py::ChangeEvent` — ChangeEvent replay ID stability across reconnects.
