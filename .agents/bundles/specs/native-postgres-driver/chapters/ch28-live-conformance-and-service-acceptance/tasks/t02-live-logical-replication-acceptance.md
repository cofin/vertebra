---
type: Task
id: "native-postgres-driver:ch28:t02"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Verify live PG14-18 pgoutput logical replication, durable checkpoints, and slot cleanup
description: Add VERTEBRA_CONFORMANCE_DOCKER-gated wal_level=logical PostgreSQL 14-18 Docker fixtures in integration/conftest.py and prove parameterized live PostgresChangeSource streaming, durable checkpoint CAS before LSN feedback, reconnect replay, backpressure, and slot teardown.
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
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Integration Docker harness fixtures and require_live_postgres_docker gate in conftest.py
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
1. Add a `VERTEBRA_CONFORMANCE_DOCKER == "1"`-gated session-scoped logical PostgreSQL 14–18 container fixture (`@pytest.fixture(params=(14, 15, 16, 17, 18), scope="session")`) in `integration/conftest.py` calling `require_live_postgres_docker()` (`ensure_module("vertebra._postgres", ...)` checked first) before lazily resolving `docker_service` and running `postgres:{version}` (`wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`, `sslmode="disable"`).
2. Verify parameterized `INSERT`/`UPDATE`/`DELETE`, `BEGIN`/`COMMIT` ordering, unchanged TOAST / key-only tuples, rolled-back transactions, durable `PostgresCheckpointStore` CAS persistence before `confirmed_flush_lsn` advances, reconnect replay from durable `(commit_end_lsn, next_event_id)`, consumer backpressure, cancellation, and deterministic slot/publication teardown with zero `for` loops over test cases or versions inside pytest functions.

Out of scope: Modifying the Rust `pgoutput` protocol decoder or provisioning external cloud replication instances.

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `require_live_postgres_docker`, zero-skip session hook, and `DockerService` usage from `ch28:t01` [^int-conftest].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig`, `direct_dsn`, and `derive_direct_config`.
- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — `PostgresNotifyConfig` and logical message coordination.
- [src/py/vertebra/db/cdc.py](../../../../../../../src/py/vertebra/db/cdc.py) — Change event envelopes and `AsyncChangeSource` contract [^db-cdc].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — `PostgresReplicationIdentity`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` invariants [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py`:
  - `@pytest.fixture(params=(14, 15, 16, 17, 18), scope="session") def vtb_postgres_logical_config(request: pytest.FixtureRequest) -> Iterator[tuple[int, PostgreSQLConfig]]`: calls `require_live_postgres_docker()` first (so offline `make py-test` skips cleanly before pulling/starting containers), then obtains `docker_service = request.getfixturevalue("docker_service")` and runs `docker_service.run(image=f"postgres:{version}", container_port=5432, command=["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"], env=..., check=..., transient=True)`, asserting `SHOW wal_level == "logical"` and major `server_version_num == version` over a native `sslmode="disable"` connection before yielding.
- `src/py/tests/integration/db/test_postgres_replication.py`:
  - Import `PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` from `vertebra.db.adapters.postgres`.
  - Parameterize replica-identity modes (`DEFAULT`, `FULL`), failure injections, and PG14–18 server versions via `@pytest.mark.parametrize` and `vtb_postgres_logical_config` (zero test-body `for` loops iterating over case specs or backends).
  - Provision unique per-test schema, table, `CREATE PUBLICATION`, and `pg_create_logical_replication_slot(..., 'pgoutput')` on a separate normal native session, exercising tailing, commit-boundary `acknowledge(...)`, `pg_replication_slots.confirmed_flush_lsn` verification, forced disconnect/resume, bounded `queue_capacity` backpressure, `aclose()` / `Task.cancel()`, and `finally` teardown asserting `0` remaining test-named rows in `pg_replication_slots`.

## Edge Cases & Error Semantics

- Rolled-back transactions (`BEGIN; INSERT ...; ROLLBACK;`) emit zero committed change events to `PostgresChangeSource` consumers.
- Out-of-order or non-contiguous commit boundary acknowledgment fails closed; `PostgresCheckpointStore.save` failure raises before native standby status feedback runs, leaving `pg_replication_slots.confirmed_flush_lsn` unadvanced.
- Mismatched `identity_key` (`(system_identifier, database, slot, publication_fingerprint)`) or dropped/missing slot raises `PostgresReplicationGapError` or canonical `DatabaseError` without hanging tasks.
- Cancelling or closing `PostgresChangeSource` releases the WAL sender (`active = false` in `pg_replication_slots`) within bounded timeout so `pg_drop_replication_slot` succeeds cleanly in `finally`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_postgres_replication.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Parameterized PG14–18 `INSERT`/`UPDATE`/`DELETE`, TOAST, & rollback | Committed `INSERT`/TOAST-unchanged `UPDATE`/`DELETE` (`REPLICA IDENTITY DEFAULT` & `FULL`) + rolled-back `INSERT` across `vtb_postgres_logical_config` | Ordered `BEGIN` -> row changes (`PG_UNCHANGED_TOAST` preserved on untouched TOAST column) -> `COMMIT` for committed tx only; monotonic `int64` event IDs. |
| Durable checkpoint before `confirmed_flush_lsn` + reconnect replay | Emit tx1 and tx2, ack tx1 only, inspect `pg_replication_slots`, close & reopen `PostgresChangeSource` from saved checkpoint | `confirmed_flush_lsn` equals tx1 `commit_end_lsn`; reopened source replays tx2 (never tx1) with continuous `next_event_id`. |
| Parameterized checkpoint failure, CAS conflict, & identity mismatch | `save()` error, competing `expected` CAS conflict, and mismatched database/publication `identity_key` | `confirmed_flush_lsn` stays unadvanced on store error; CAS mismatch raises `PostgresCheckpointConflictError`; bad identity raises `PostgresReplicationGapError`. |
| Backpressure, cancellation, & zero-leak slot teardown | Pause consumer with `queue_capacity=4`, insert burst rows, then `aclose()` / `task.cancel()` and query `pg_replication_slots` | Bounded queue holds without losing acked events; WAL sender goes inactive cleanly and `finally` leaves `0` test slots. |

## Verification

- Offline safety check: `env -u VERTEBRA_CONFORMANCE_DOCKER -u VERTEBRA_CONFORMANCE_POSTGRES_DSN -u VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS uv run --no-sync pytest src/py/tests/integration/db/test_postgres_replication.py -q`
- Final strict live Green + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_replication.py -q -rs && uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_postgres_replication.py`

## Acceptance Criteria

- [ ] `vtb_postgres_logical_config` skips cleanly offline and runs live `wal_level=logical` PG14–18 containers under `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` passing parameterized `PostgresChangeSource` streaming, ack, reconnect replay, backpressure, and cancellation tests with `0` skips/xfails (R22, R23, R25).
- [ ] Every test-created replication slot, publication, table, and native session is cleaned up in `finally` with `0` orphan `pg_replication_slots` entries (R22, R25).

## Notes & Discoveries

- Reuse the adapter-owned `PostgresCheckpointStore` (`vertebra.db.adapters.postgres.change_stream`) contract directly rather than introducing test-local `pgoutput` parsers.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Integration Docker harness fixtures and `require_live_postgres_docker` gate in `conftest.py`.
[^db-cdc]: `src/py/vertebra/db/cdc.py` — Engine-neutral change-feed envelopes and source abstractions.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Logical replication identity, checkpoint CAS, and durable feedback contracts.
