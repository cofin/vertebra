---
type: Task
id: sqlspec-adapter-parity:ch04-sqlspec-cutover-storage-events-docs:t02
flow_id: sqlspec-adapter-parity
chapter_id: ch04-sqlspec-cutover-storage-events-docs
title: Harden ChangeFeedSyncEventBackend and ChangeFeedAsyncEventBackend with Checkpoints & Object Spill
description: Add durable checkpoint persistence and ArtifactReference large-payload spill to extensions/sqlspec/events.py.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- sqlspec-adapter-parity:ch04-sqlspec-cutover-storage-events-docs:t01
files:
- src/py/vertebra/extensions/sqlspec/events.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
read_set:
- src/py/vertebra/extensions/sqlspec/events.py
- src/py/vertebra/db/cdc.py
- src/py/vertebra/db/adapters/postgres/notify.py
- src/py/vertebra/db/adapters/oracle/notify.py
estimated_loc: 195
tags:
- sqlspec
- events
- cdc
sources:
- id: sqlspec-events
  resource: src/py/vertebra/extensions/sqlspec/events.py
  title: ChangeFeedSyncEventBackend and ChangeFeedAsyncEventBackend
- id: db-cdc
  resource: src/py/vertebra/db/cdc.py
  title: SyncChangeSource and AsyncChangeSource CDC primitives
---

# Task `t02`: Harden ChangeFeedSyncEventBackend and ChangeFeedAsyncEventBackend with Checkpoints & Object Spill

## Objective & Scope Boundary

Harden `src/py/vertebra/extensions/sqlspec/events.py` (`ChangeFeedSyncEventBackend`, `ChangeFeedAsyncEventBackend`) per [`../spec.md`](../spec.md) [^sqlspec-events] [^db-cdc].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/events.py` — generic CDC event backend bridge (zero engine-specific SQL inside generic module).
- `src/py/vertebra/db/cdc.py` — `SyncChangeSource` and `AsyncChangeSource` resume-token contracts.

## Contracts & Edge Cases

- Persist resume tokens across restarts via the configured `BaseEventQueueStore` checkpoint seam.
- Spill event payloads approaching PostgreSQL's `8000`-byte `NOTIFY` frame ceiling into the durable `EventQueueStore` table and emit compact wakeup IDs over `SyncNotificationSource` / `AsyncNotificationSource`.
- Keep `extensions/sqlspec/events.py` free of vendor dispatch or `_compat.py` monkeypatching.

## Test Matrix

- Unit tests verifying resume-token recovery after consumer restart and large (`> 8000` byte) payload spilling + wakeup delivery.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] `ChangeFeedSyncEventBackend` and `ChangeFeedAsyncEventBackend` persist checkpoints and spill oversized notify frames cleanly.
- [ ] Zero regressions in the fast verification gate.

[^sqlspec-events]: `src/py/vertebra/extensions/sqlspec/events.py` — ChangeFeedSyncEventBackend and ChangeFeedAsyncEventBackend.
[^db-cdc]: `src/py/vertebra/db/cdc.py` — SyncChangeSource and AsyncChangeSource CDC primitives.
