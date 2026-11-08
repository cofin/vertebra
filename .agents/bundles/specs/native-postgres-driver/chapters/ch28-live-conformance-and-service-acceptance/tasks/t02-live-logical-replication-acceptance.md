---
type: Task
id: "native-postgres-driver:ch28:t02"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Verify live PG14-18 pgoutput logical replication, durable checkpoints, and slot cleanup
description: Add session-scoped wal_level=logical PostgreSQL 14-18 Docker fixtures in integration/conftest.py and prove live PostgresChangeSource streaming, durable checkpoint CAS before LSN feedback, reconnect replay, backpressure, and slot teardown.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch28:t01"
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/cdc.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/tests/integration/conftest.py
tests:
  - src/py/tests/integration/db/test_postgres_replication.py
estimated_loc: 240
plan_revision: 9
plan_commit: b0de4dcb
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
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Integration Docker harness fixtures in conftest.py
  - id: db-cdc
    resource: src/py/vertebra/db/cdc.py
    title: Engine-neutral change-feed envelopes and source abstractions
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Logical replication identity, checkpoint CAS, and durable feedback contracts
tags:
  - postgres
  - replication
  - cdc
  - checkpoints
  - integration
---

# Task t02: Verify Live PG14–18 `pgoutput` Logical Replication, Durable Checkpoints, and Slot Cleanup

## Objective & Scope Boundary

Prove end-to-end `PostgresChangeSource` (`vertebra.db.adapters.postgres.change_stream`) behavior against live `wal_level=logical` PostgreSQL 14–18 Docker containers in `src/py/tests/integration/conftest.py` and `src/py/tests/integration/db/test_postgres_replication.py` [^int-conftest] [^db-cdc] [^pg-contracts]:
1. Add a session-scoped logical PostgreSQL 14–18 container fixture in `integration/conftest.py` via `docker_service.run(image=f"postgres:{version}", container_port=5432, command=["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"], ..., transient=True)` with `sslmode="disable"`, bounded readiness polling, and `SHOW wal_level` verification.
2. Verify `INSERT`/`UPDATE`/`DELETE`, `BEGIN`/`COMMIT` ordering, unchanged TOAST / key-only tuples, rolled-back transactions, durable `PostgresCheckpointStore` CAS persistence before `confirmed_flush_lsn` advances, reconnect replay from durable `(commit_end_lsn, next_event_id)`, consumer backpressure, cancellation, and deterministic slot/publication teardown.

Out of scope: Modifying the Rust `pgoutput` protocol decoder from Chapters 22–24 or provisioning external cloud replication instances.

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — Existing `pytest-databases` `DockerService` and PostgreSQL config fixtures [^int-conftest].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig`, `direct_dsn`, and `derive_direct_config`.
- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — `PostgresNotifyConfig` and `build_emit_message_sql`.
- [src/py/vertebra/db/cdc.py](../../../../../../../src/py/vertebra/db/cdc.py) — Change event envelopes and `AsyncChangeSource` contract [^db-cdc].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — `PostgresReplicationIdentity`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` invariants [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py`:
  - `vtb_postgres_logical_config(docker_service: DockerService, request: pytest.FixtureRequest) -> Iterator[tuple[int, PostgreSQLConfig]]`: session-scoped container fixture parameterized across `(14, 15, 16, 17, 18)` invoking `docker_service.run(image=f"postgres:{version}", container_port=5432, command=["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"], env=..., check=..., transient=True)`, constructing `PostgreSQLConfig(..., sslmode="disable")`, and asserting `SHOW wal_level == "logical"` and major `server_version_num == version`.
- `src/py/tests/integration/db/test_postgres_replication.py`:
  - Import `PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` from `vertebra.db.adapters.postgres` (`vertebra.db.adapters.postgres.change_stream`).
  - Test helper creates unique per-test schema, table (`REPLICA IDENTITY DEFAULT` and `FULL` cases), `CREATE PUBLICATION <pub> FOR TABLE ...`, and `SELECT pg_create_logical_replication_slot(<slot>, 'pgoutput')` on a separate normal native session (`PostgresChangeSource` never creates or drops slots/publications implicitly).
  - Exercises `PostgresChangeSource` + durable `PostgresCheckpointStore` across normal tailing, explicit commit-boundary `acknowledge(...)`, `pg_replication_slots.confirmed_flush_lsn` inspection, forced disconnect/resume, bounded queue backpressure (`queue_capacity` paused via `asyncio.Event`), `aclose()` / `Task.cancel()`, and `finally` cleanup asserting `0` remaining test-named rows in `pg_replication_slots`.

## Edge Cases & Error Semantics

- Rolled-back transactions (`BEGIN; INSERT ...; ROLLBACK;`) emit zero committed change events to `PostgresChangeSource` consumers.
- Out-of-order or non-contiguous commit boundary acknowledgment is rejected; `PostgresCheckpointStore.save` failure raises before native standby status feedback is sent, leaving `pg_replication_slots.confirmed_flush_lsn` unadvanced on the server.
- Mismatched `identity_key` (`(system_identifier, database, slot, publication_fingerprint)`) or missing/invalid slot or publication raises `PostgresReplicationGapError` (`OperationalError` subclass) or canonical `DatabaseError` without hanging workers.
- Cancelling or closing `PostgresChangeSource` while idle or while downstream is backpressured releases the replication WAL sender (`active = false` in `pg_replication_slots`) within bounded timeout so `pg_drop_replication_slot` succeeds immediately in `finally`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_postgres_replication.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Live PG14–18 `INSERT`/`UPDATE`/`DELETE` + rollback | Execute committed `INSERT`/`UPDATE`/`DELETE` + rolled-back `INSERT` across PG14–18 logical containers | Ordered `BEGIN` -> relation/row changes -> `COMMIT` envelopes for committed tx only; monotonically increasing `int64` event IDs. |
| Durable checkpoint before `confirmed_flush_lsn` + reconnect replay | Emit tx1 and tx2, ack tx1 only, inspect `pg_replication_slots`, close & reopen `PostgresChangeSource` from saved checkpoint | `confirmed_flush_lsn` advances to tx1 `commit_end_lsn`; reopened source replays tx2 (not tx1) with matching `next_event_id`. |
| Checkpoint store failure & conflict | Inject `save()` error or competing `expected` CAS mismatch during ack | `confirmed_flush_lsn` does not advance on store error; CAS mismatch raises `PostgresCheckpointConflictError`. |
| Backpressure, keepalive, & slot cleanup | Pause consumer with `queue_capacity=4`, insert burst rows, then `aclose()` and query `pg_replication_slots` | Bounded memory without lost acked records; WAL sender exits cleanly and `pg_drop_replication_slot` leaves `0` test slots. |

## Verification

- Initial Red baseline: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_replication.py -q -rs`
- Final Green + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_replication.py -q -rs && uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_postgres_replication.py`

## Acceptance Criteria

- [ ] `vtb_postgres_logical_config` runs live `wal_level=logical` PostgreSQL 14–18 containers and passes full `PostgresChangeSource` streaming, ack, reconnect replay, backpressure, and cancellation tests with zero skips (R22, R23, R25).
- [ ] Every test-created replication slot, publication, table, and native session is cleaned up in `finally` with zero orphan `pg_replication_slots` entries (R22, R25).

## Notes & Discoveries

- [2026-11-07 16:45] Reuse the adapter-owned `PostgresCheckpointStore` (`vertebra.db.adapters.postgres.change_stream`) contract directly rather than introducing a second `pgoutput` parser in integration tests.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Integration Docker harness fixtures in `conftest.py`.
[^db-cdc]: `src/py/vertebra/db/cdc.py` — Engine-neutral change-feed envelopes and source abstractions.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Logical replication identity, checkpoint CAS, and durable feedback contracts.
