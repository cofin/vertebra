---
type: Task
id: native-postgres-driver:t73
title: Integrate live notification sources with active subscriptions
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t72
files:
- src/py/vertebra/db/adapters/postgres/events.py
- src/py/vertebra/db/adapters/postgres/driver.py
- src/py/vertebra/db/adapters/postgres/notify.py
- src/py/vertebra/db/adapters/postgres/config.py
- src/py/tests/unit/db/test_postgres_native_notifications.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
tests:
- src/py/tests/unit/db/test_postgres_native_notifications.py::test_notification_source_updates_live_channels
- src/py/tests/unit/db/test_postgres_native_notifications.py::test_notification_source_payload_and_close
- src/py/tests/unit/db/test_postgres_native_notifications.py::test_notification_source_structural_port
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::test_postgres_notify_publish
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
- R21
---

# Integrate live notification sources with active subscriptions

## Objective

Integrate live notification sources with active subscriptions; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 400 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Add permanent adapter-owned `events.py::PostgresNotificationDriver(Protocol)`. Its readonly property config:PostgreSQLConfig and async `_open_notification_listener(channels:Sequence[str],*,queue_capacity:int=1024,reconnect:bool=False)->AsyncPostgresListener` express consumed event capabilities. Declare async `_publish_notification(channel:str,payload:str)->None` as the uniform publication seam; source never inspects driver kind or awaits an int/coroutine union. events.py uses future annotations and TYPE_CHECKING imports for native listener/config types, avoiding eager optional native imports. The permanent module is extended with replication protocol in t80 after those native types exist, and with real handle registry in t90; none is a temporary shim/global vendor contract.
2. Annotate PostgresNotificationSource constructor/_driver with this narrow protocol imported under TYPE_CHECKING, replacing concrete Sync/AsyncPostgreSQLDriver annotations. Source.publish preserves bytes/base64/ChangeFeed envelope wrapping then awaits `_driver._publish_notification(channel,wire_payload)`; remove execute/isinstance/asyncio dispatch from source. Add REAL async `_publish_notification` methods to the two current legacy classes in driver.py (only these symbols and asyncio import): sync awaits asyncio.to_thread(self.execute,"SELECT pg_notify(%s, %s)",(channel,payload)); async awaits self.execute("SELECT pg_notify($1, $2)",(channel,payload)). These relocate existing behavior into its owning adapter, with no dummy event hook/fallback or ordinary query changes. t90 implements numeric publishers on permanent native classes; old publisher bodies disappear with legacy classes in t92/t93, no final compatibility shim. Extend source options queue_capacity=1024,reconnect=False and one active native listener. subscribe/unsubscribe validates entire set before I/O, updates active listener awaiting command completion and preserves future subscription snapshot; concurrent listen fails InterfaceError.
3. Source listen awaits `_open_notification_listener` then yields NotificationEvent(channel,payload:bytes,sender_id=str(pid),metadata={reconnected:gap,delivery:at_most_once}). Preserve raw/b64/ChangeFeed hydration and spill errors, order/duplicates; finally closes handle, aclose and async contexts are idempotent. Runtime unsupported provider without hook retains clear NotSupportedError on iteration, without adding a fake hook to legacy driver classes. Reject transaction/statement poolers without direct_dsn; valid direct/session configs preserve established derive_direct_config behavior. No generic registry engine branch.
4. Canonical typed fixture class `NativeNotificationDriverPort` structurally satisfies PostgresNotificationDriver using real native SyncPostgresClient query execution in async publish hook (via asyncio.to_thread) plus dedicated AsyncPostgresClient listener operations against scripted server. A typed protocol assignment and ordinary public source construction execute behavior, not runtime Protocol checks/AST mirrors. Assert subscription ordering, bytes/ChangeFeed spills, no pool starvation, close on break/error/cancel and reuse. Remove old mock driver.listen_notifications delegation test in test_pg_dataplane_and_datafusion.py after moving its subscription/idempotence assertions into native public-source tests; preserve publish and unsupported-provider behavior. Existing legacy publish test uses the REAL bound legacy publisher method as mocked hook side_effect so source wrapper and owning method execute; verify actual sync execute runs off-loop with pyformat and async execute uses numeric binds, rather than letting an AsyncMock hook swallow the behavior. No dummy old driver hook is added. Full public Sync/Async drivers satisfy protocol after t90 hooks and are exercised after t91 activation/legacy cleanup. Commit `feat(postgres): wire typed active notification sources`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k notification_source -q
uv run --no-sync python -m pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -k notif -q
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/notify.py src/py/tests/unit/db/test_postgres_native_notifications.py
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R21: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
