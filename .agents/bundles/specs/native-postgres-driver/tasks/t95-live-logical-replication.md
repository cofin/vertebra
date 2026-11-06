---
type: Task
id: native-postgres-driver:t95
title: Accept live PostgreSQL logical replication and durable source recovery
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t94
files:
- src/py/tests/integration/conftest.py
- src/py/tests/integration/db/test_postgres_replication.py
tests:
- src/py/tests/integration/db/test_postgres_replication.py
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
- replication
- integration
requirements:
- R22
- R23
- R25
---

# Accept live PostgreSQL logical replication and durable source recovery

## Objective

Prove complete PostgresChangeSource behavior against test-owned logical PostgreSQL14–18 servers, including durable acknowledgment and resource cleanup.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, completed pgoutput/replication/source worksheets, `.agents/skills/python-framework/SKILL.md` and installed pytest-databases skill. Existing predecessor tests are scripted wire and Python unit coverage, not a live logical server fixture. This worksheet supplies that missing native adapter/server acceptance. One invocation/one signed local conventional commit; No external/live cloud service writes and no implementation while plan_commit is null.

## Steps

Every test-owned vanilla PostgreSQL, pgvector, PgBouncer, administrative, cancellation observer and installed-wheel native config in this worksheet explicitly sets `sslmode="disable"`; these local images do not provide TLS. This is scoped to PostgreSQL fixtures and never changes other engine defaults or dedicated t05 TLS authentication fixtures.

1. Add session-scoped test-owned logical PostgreSQL service fixture parameterized versions14–18 in integration/conftest.py using actual `docker_service.run(image="postgres:<version>",container_port=5432,command=["postgres","-c","wal_level=logical","-c","max_replication_slots=10","-c","max_wal_senders=10"],env=...,check=...,transient=True)`. Installed DockerService.run exposes command, env, check, dynamic ports and lifecycle; provide_postgres_service does not expose command. Reuse clientless psql readiness helpers and native clients. Do not add unsupported pytest CLI flags or hand-delete package-owned containers. Assert SHOW wal_level/version and bound setup timeout before publishing fixture.
2. Use unique test-owned schema/table/publication/slot names and separate native normal/replication sessions through direct_dsn. Create only local-container resources; test function owns slots/publications, service owns container. RED acceptance asserts real insert/update/delete, begin/commit event ordering, correct transaction LSNs/identity, relation changes and unchanged TOAST/key-only behavior defined by predecessor pgoutput contract, plus rollback producing no committed changes. Reuse source's durable checkpoint store whose identity, acknowledged commit-end LSN and signed-int64 next event ID persist atomically; only contiguous emitted commit boundaries can be acknowledged. Slots/publications are explicitly pre-created by this local fixture; PostgresChangeSource never provisions/drops them or promises an initial snapshot. Test source identity mismatch, out-of-order acknowledgment and durable-store save failure preventing feedback advancement. Reuse source's durable checkpoint test store; no second pgoutput decoder in Python tests.
3. Advance acknowledgment only after downstream durable acceptance, inspect confirmed_flush_lsn with a separate native client, then disconnect/reconnect and prove replay from last durable checkpoint and no claim of exactly-once delivery. Exercise keepalive reply-request/status feedback, test-owned slot already exists, invalid slot/publication and server disconnect. Bound consumer queue, pause downstream with an event, assert backpressure reaches producer/source according to predecessor resource contract without losing acknowledged data or unbounded memory. Service interruption tests may use only this fixture's owned container.
4. Cancel/close source while waiting and while downstream is backpressured; assert native replication socket/worker terminates, stream lease released and cleanup can drop its slot. Finally close every source, drop owned slots/publications/tables and query pg_replication_slots to prove no named slot remains; do not drop unrelated slots. Test fresh connection and no retained WAL resources. Strict configured service lane must fail on unavailable Docker/permissions rather than skip or xfail.
5. Record all five server versions and real event/ack/recovery assertions. Protocol decoder implementation remains completed predecessor work; defect fixes need bounded revise worksheet then rerun. Commit test(postgres): accept live logical replication recovery.

## Verification

Estimated changed lines including fixture/tests330–400 by sharing version/setup and predecessor checkpoint helpers. If more than one fixture/acceptance commit is needed, revise the worksheet before execution. Planned commands:

```bash
make dev
VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_replication.py -q -rs
uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_postgres_replication.py
```

## Acceptance Criteria

- [ ] R22, R23, R25: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] PG14–18 live replication, acknowledgment, recovery, backpressure and cancellation pass through PostgresChangeSource without skips.
- [ ] Every created slot/publication/table/socket is test-owned and cleaned; no external/cloud mutation occurs.
- [ ] One local signed conventional commit and task-first state/evidence synchronization complete this bounded acceptance.

## Notes & Discoveries

None.
