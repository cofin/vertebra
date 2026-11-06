---
type: Task
id: native-postgres-driver:t80
title: Implement PostgreSQL change sources and checkpoint stores
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t79
files:
- src/py/vertebra/db/adapters/postgres/events.py
- src/py/vertebra/db/adapters/postgres/change_stream.py
- src/py/vertebra/db/adapters/postgres/types.py
- src/py/vertebra/db/adapters/postgres/__init__.py
- src/py/tests/unit/db/test_postgres_change_source.py
tests:
- src/py/tests/unit/db/test_postgres_change_source.py::test_change_source_event_envelopes
- src/py/tests/unit/db/test_postgres_change_source.py::test_checkpoint_store_atomic_compare_and_swap
- src/py/tests/unit/db/test_postgres_change_source.py::test_checkpoint_store_identity_and_monotonicity
- src/py/tests/unit/db/test_postgres_change_source.py::test_change_source_structural_port
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
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- native
- data-events
requirements:
- R22
- R23
---

# Implement PostgreSQL change sources and checkpoint stores

## Objective

Implement PostgreSQL change sources and checkpoint stores; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 400 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Extend permanent adapter events.py with `PostgresReplicationDriver(Protocol)` now that t79 native replication/identity types exist. Declare readonly config:PostgreSQLConfig, async `_replication_identity(slot:str,publication:str)->PostgresReplicationIdentity` and async `_open_replication_stream(config:PostgresChangeConfig,*,start_lsn:int|None=None,reconnect:bool=False)->AsyncPostgresReplication`. Future annotations and TYPE_CHECKING import of .change_stream.PostgresChangeConfig avoid runtime cycle; native handle/identity/config imports remain TYPE_CHECKING to preserve optional dependency loading. Do not import absent replication types or change_stream in t73. Source constructor/_driver uses PostgresReplicationDriver, not concrete legacy driver types. Define adapter-owned PostgresChangeConfig(slot,publication,channel=vertebra_events,start_lsn=None,queue_capacity=1024,reconnect=True,checkpoint_timeout=10.0), PostgresReplicationIdentity from t79, and frozen PostgresCheckpoint(system_identifier,database,slot,publication_fingerprint,commit_end_lsn,next_event_id). Validate identifiers, positive finite timeout/bounds, u64 LSN and counter1..2^63-1. identity_key is the exact four-string tuple, never a delimiter-joined ambiguous string.
2. Freeze async PostgresCheckpointStore.load(identity_key:tuple[str,str,str,str])->PostgresCheckpoint|None and save(checkpoint,*,expected:PostgresCheckpoint|None)->None. save is atomic durable compare-and-swap for that identity: current must equal expected, checkpoint identity must match key/expected, commit_end_lsn and next_event_id never decrease, equal LSN requires equal counter. A mismatch raises adapter PostgresCheckpointConflictError(OperationalError) without mutation. Return means durable according to caller store; an exception/cancel may follow a committed write, so caller MUST reload. Store operations cooperate with cancellation and complete under source checkpoint_timeout. MemoryPostgresCheckpointStore uses an asyncio.Lock for CAS and is explicitly nondurable; it returns immutable records.
3. Source(driver,config,checkpoint_store=None).stream() gets driver._replication_identity before store.load(identity_key), then calls driver._open_replication_stream(config,start_lsn=loaded.commit_end_lsn if loaded else config.start_lsn,reconnect=False). Hook unpacks config for native slot/publication/queue_capacity; checkpoint_timeout is never a native kwarg. Verify generation control identity matches exactly before emitting. Operation BEGIN/INSERT/UPDATE/DELETE/TRUNCATE/MESSAGE/COMMIT maps to ChangeEvent; preserve payload/LSN/key/old/new/TOAST; signed64 counter seeded loaded next_event_id, checked before yield. No ChangeFeed republish/reassignment or auto-ack. Collect emitted COMMIT boundary->next counter snapshots for later t81 acknowledgement, bounded1024.
4. Canonical typed NativeReplicationDriverPort implements protocol by real AsyncPostgresClient identity preflight/open_replication against scripted peers; a typed protocol assignment and PostgresChangeSource public constructor prove normal structural use. No getattr/Any cast, runtime protocol type check, AST assertion or fake old-driver hook bypasses typing. Tests cover typed envelopes, NULL/TOAST, stable IDs within source, empty/standalone-message transactions, signed64/Parquet compatibility, store key isolation, monotonic/equal-LSN validation and two concurrent CAS writers with exactly one winner/no partial mutation. No acknowledge method is exposed until complete orchestration t81; do not add a placeholder. Source basic finally closes handle; reconnect and close races are t82. Commit `feat(postgres): define atomic checkpoint stores and change envelopes`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k 'change_source or checkpoint_' -q
uv run --no-sync python -m pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py -q
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/change_stream.py src/py/tests/unit/db/test_postgres_change_source.py
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R22, R23: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
