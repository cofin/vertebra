---
type: Task
id: native-postgres-driver:t90
title: Open and close dedicated native PostgreSQL event sessions from drivers
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t89
files:
- src/py/vertebra/db/adapters/postgres/_sync.py
- src/py/vertebra/db/adapters/postgres/_async.py
- src/py/vertebra/db/adapters/postgres/config.py
- src/py/vertebra/db/adapters/postgres/events.py
- src/py/tests/unit/db/test_postgres_event_hooks.py
tests:
- src/py/tests/unit/db/test_postgres_event_hooks.py
- src/py/tests/unit/db/test_postgres_event_hooks.py::test_native_notification_publish_modes
requirements:
- R21
- R22
- R23
- R24
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
created_at: '2026-11-05T00:00:00Z'
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- events
- lifecycle
---

# Open and close dedicated native PostgreSQL event sessions from drivers

## Objective

Complete the production hooks on the permanent native classes before t91 activation, consumed by notification and change sources on both driver facades, with independently owned direct sessions and bounded shutdown.

## Context

Read `../spec.md`, native listener/replication/identity worksheets and source wrappers. This task follows t89 and precedes t91. Public driver.py exports and legacy class bodies remain untouched; existing legacy async construction remains valid. The new tests use final defining modules now and switch to canonical imports in t91. No temporary class/module is introduced. One executor invocation creates one signed conventional local commit on the complete feature branch; shared files change only after predecessors. No production execution with null plan_commit, no hosted action and no unrelated edits. Estimated implementation plus behavioral tests300–380 lines. Source notification/CDC modules remain owned by their existing worksheets; implement hooks and resource ownership here, not duplicate protocol loops.

## Steps

1. Add RED facade tests importing SyncPostgreSQLDriver from final `_sync.py` and AsyncPostgreSQLDriver from final `_async.py` until t91 publishes both together over an actual scripted native PostgreSQL wire listener, using test configs with explicit sslmode="disable". Assert direct session options, startup/authentication, LISTEN emission, logical IDENTIFY_SYSTEM/catalog preflight, no START_REPLICATION during identity lookup and real socket closure. Do not accept a fake hook returning success without opening/observing a native session. Include open failure, cancellation during open, closed driver, transaction-mode pooler without explicit direct_dsn and wrong source identity.
2. Implement PostgreSQLConfig.derive_direct_config rejection for transaction/statement pooler without explicit direct_dsn; direct/session config may use its dsn. A derived config uses direct/session endpoint, disables broker/query-pool sharing and preserves complete TLS/GUC/cloud/IAM primitive kwargs and designated credentials. Each event open creates dedicated native AsyncPostgresClient even on the sync driver; never acquires a query-pool lease or extracts cross-extension Rust caches. Validate caller configuration before network work.
3. Both permanent implementation classes (never class bodies in driver.py) expose async `_open_notification_listener(channels,*,queue_capacity=1024,reconnect=False)->AsyncPostgresListener`, async `_open_replication_stream(config:PostgresChangeConfig,*,start_lsn=None,reconnect=False)->AsyncPostgresReplication` and async `_replication_identity(slot,publication)->PostgresReplicationIdentity`. The replication hook extracts config.slot/config.publication/config.queue_capacity and calls native `open_replication(slot,publication,start_lsn=start_lsn,queue_capacity=config.queue_capacity,reconnect=reconnect)`. Identity lookup awaits native `replication_identity(slot,publication)` on a fresh direct client and always closes it in finally. Native identity preflight performs no START. Source wrapper resolves identity before checkpoint_store.load(identity_key), then opens with reconnect=False; source owns retries. No checkpoint_timeout argument enters native open APIs.
4. Adapter-owned events.py owns a guarded registry pairing each successful raw native listener/replication handle with its dedicated client. Register only after successful open; failure/cancellation closes any partially created handle/client in finally. A handle may close normally while driver remains alive; use native listener/replication read-only `.closed:bool` (true after terminal EOF/error/aclose), prune closed registrations on every next open and driver close, and close the corresponding owned AsyncPostgresClient in finally. Do not assume weakref support or add callbacks. Repeated short-lived sources retain no inactive sockets/clients. Shutdown marks registry closed before await, prevents racing opens/publication, closes handles then clients boundedly, preserves first error while still closing the rest and is idempotent. Track actual source/listener shutdown rather than merely clearing a Python collection.
5. Extend `_async.py` driver.close to await registry cleanup cancellation-shielded before pool closure. Extend `_sync.py` driver.close to bridge the same coroutine through existing vertebra.utils.portal.PortalProvider.default().call with bounded timeout, rather than asyncio.run on a possibly active loop. Awaited native cleanup executes in its independent runtime with GIL detached; waiting bridge releases the GIL and tests prove an unrelated Python thread progresses. Registry owns native handles only, not original-loop Python consumer tasks. Add actual-wire tests for normal handle close then reopen, multiple active handles, failed close cleanup and sync close while caller has a running event loop; no leaked backend sessions or query-pool capacity changes.

6. Add REAL async `_publish_notification(channel:str,payload:str)->None` to BOTH permanent classes, structurally satisfying events.py::PostgresNotificationDriver created in t73 (readonly config, async publish and listener hook). PostgresNotificationSource wraps payloads and uniformly awaits this hook; neither source nor driver inspects an int/awaitable union or driver mode. Async native hook awaits own `execute("SELECT pg_notify($1, $2)",(channel,payload),timeout=10.0)`, preserving same-task ambient Async transaction participation. Sync native hook first checks this driver's active Sync transaction/scope and raises exact InterfaceError BEFORE offload or I/O when one is active; a Sync transaction cannot migrate through copied ContextVars to a worker thread. Outside such a scope it awaits `asyncio.to_thread(self.execute,"SELECT pg_notify($1, $2)",(channel,payload),timeout=10.0)`, with normal worker-finally lease cleanup and the completed t57 explicit 10-second native command I/O budget plus bounded cancellation/retirement cleanup. The 10 seconds cover reserved command I/O only; acquisition/reset have their own deadlines, CPU conversion/projection is not preemptible, and source/hook signatures are unchanged. Direct synchronous `driver.execute(pg_notify...)` remains the route for joining a Sync transaction; no new cross-mode view/bridge is introduced.
7. Add public PostgresNotificationSource positive tests using both permanent native driver classes through the uniform protocol, plus typed assignments to PostgresNotificationDriver checked by Mypy without casts/ignores or runtime Protocol checks. Observe numeric bound publication, exact timeout=10.0 forwarding on BOTH modes, payload wrapping/order and lease cleanup through actual native wire/config. A stalled command peer proves native client-expiry OperationalError(sqlstate=None), fenced cancellation/retirement and no query replay; actual server57014 retains QueryCancelledError. Do not replace this bound with asyncio.wait_for around a running Sync worker. Within an active Sync scope, source.publish raises InterfaceError before asyncio.to_thread or any SQL; direct Sync execute still participates in that transaction. Within an Async scope, awaited source.publish joins its transaction and rollback suppresses delivery. Outside Sync scope, offload remains responsive and a cancelled await cannot guarantee withdrawal of already-running Sync SQL; test a barrier-controlled worker finishes/cleans its lease under its native bound and do not claim exactly-once delivery. Preserve this mode boundary in public hook docstrings, for later consumer documentation. t73's real legacy publishers retain their established pyformat-sync/numeric-async behavior until t91 and disappear in t92/t93; do not add dummy compatibility hooks. Commit feat(postgres): own dedicated event driver sessions.

## Verification

Record actual RED then passing public wire assertions. Commands are planned, not executed evidence:

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/db/test_postgres_change_source.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/tests/unit/db/test_postgres_event_hooks.py
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/tests/unit/db/test_postgres_event_hooks.py
```

## Acceptance Criteria

- [ ] R21/R22/R23/R24: both driver facades supply real numeric publishers, dedicated native event/identity hooks, typed source contracts and source-owned reconnect coordination.
- [ ] Failed opens, active-handle shutdown and sync/async close release observed sockets without borrowing query pool or changing unrelated transaction scope.
- [ ] One local signed conventional commit and task-first state/evidence synchronization complete the hook boundary.

## Notes & Discoveries

None.
