---
type: Task
id: "queues-tasks-and-channels:ch07-queues-channels-docs-and-okf:t01"
flow_id: queues-tasks-and-channels
chapter_id: ch07-queues-channels-docs-and-okf
title: "Update Queues, Realtime Channels & Database Capabilities Documentation"
description: "Update docs/usage/queues-and-realtime.md, docs/reference/queues.md, and docs/reference/database-capabilities.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "ch04-litestar-queues-integration:t03"
  - "ch05-django-tasks-and-cache-integration:t03"
  - "ch06-distributed-channels-and-valkey-stores:t03"
files:
  - docs/usage/queues-and-realtime.md
  - docs/reference/queues.md
  - docs/reference/database-capabilities.md
tests:
  - src/py/tests/unit/extensions/test_vite_and_docs_portal.py
read_set:
  - docs/usage/queues-and-realtime.md
  - docs/reference/queues.md
  - docs/reference/database-capabilities.md
  - src/py/vertebra/db/driver/capabilities.py
estimated_loc: 180
tags:
  - documentation
  - queues
  - channels
sources:
  - id: doc-qr
    resource: docs/usage/queues-and-realtime.md
    title: "Queues and realtime channels usage guide"
  - id: ref-q
    resource: docs/reference/queues.md
    title: "Queues API reference"
  - id: ref-cap
    resource: docs/reference/database-capabilities.md
    title: "Database driver capabilities reference"
---

# Task `t01`: Update Queues, Realtime Channels & Database Capabilities Documentation

## Objective & Scope Boundary

Update `docs/usage/queues-and-realtime.md` and `docs/reference/{queues,database-capabilities}.md` per [`../spec.md`](../spec.md) [^doc-qr] [^ref-q] [^ref-cap].

## Bounded Read Manifest

- `docs/usage/queues-and-realtime.md` — `QueueBackendSettings`, native PostgreSQL/Oracle/Redis/Valkey/SHM queues, and distributed channels.
- `docs/reference/{queues,database-capabilities}.md` — `SyncQueueStore`/`AsyncQueueStore` and `SyncNotificationSource`/`AsyncNotificationSource`.

## Contracts & Edge Cases

- Document `SyncQueueStore`/`AsyncQueueStore`, `SyncPostgresQueueStore`/`AsyncPostgresQueueStore`, `SyncOracleQueueStore`/`AsyncOracleQueueStore`, `QueueBackendSettings`, and distributed `ChannelsBackend` across Memory, Redis, Valkey, PostgreSQL, and Oracle.
- Use `pymdownx.tabbed` install tabs and 4–6 node conceptual Mermaid diagrams.

## Test Matrix

- Run `pytest test_vite_and_docs_portal.py` and verify zero unprefixed `QueueStore` or `NotificationSource` references remain in `docs/`.

## Verification

```bash
uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] `docs/usage/queues-and-realtime.md` and `docs/reference/{queues,database-capabilities}.md` match the implemented `Sync*`/`Async*` contracts.
- [ ] Zero regressions in the chapter fast verification gate.

[^doc-qr]: `docs/usage/queues-and-realtime.md` — Queues and realtime channels usage guide.
[^ref-q]: `docs/reference/queues.md` — Queues API reference.
[^ref-cap]: `docs/reference/database-capabilities.md` — Database driver capabilities reference.
