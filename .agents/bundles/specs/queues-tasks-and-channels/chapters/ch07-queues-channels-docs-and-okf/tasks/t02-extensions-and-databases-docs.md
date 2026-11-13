---
type: Task
id: "queues-tasks-and-channels:ch07-queues-channels-docs-and-okf:t02"
flow_id: queues-tasks-and-channels
chapter_id: ch07-queues-channels-docs-and-okf
title: "Update Litestar Queues, Django Tasks, ValkeyStore & Notification Listener Docs"
description: "Update docs/usage/extensions.md, docs/usage/databases.md, and docs/usage/analytics-and-cdc.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/usage/analytics-and-cdc.md
tests:
  - src/py/tests/unit/extensions/test_vite_and_docs_portal.py
read_set:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/usage/analytics-and-cdc.md
estimated_loc: 165
tags:
  - documentation
  - litestar-queues
  - django
  - valkey
sources:
  - id: doc-ext
    resource: docs/usage/extensions.md
    title: "Litestar and Django extension usage guide"
  - id: doc-db
    resource: docs/usage/databases.md
    title: "Database usage guide"
  - id: doc-cdc
    resource: docs/usage/analytics-and-cdc.md
    title: "Analytics, CDC, and notification listener guide"
---

# Task `t02`: Update Litestar Queues, Django Tasks, ValkeyStore & Notification Listener Docs

## Objective & Scope Boundary

Update `docs/usage/{extensions,databases,analytics-and-cdc}.md` per [`../spec.md`](../spec.md) [^doc-ext] [^doc-db] [^doc-cdc].

## Bounded Read Manifest

- `docs/usage/extensions.md` — `litestar-queues>=0.14.0` (`StoreQueueBackend`, `SHMQueueBackend`, `ContainerTaskDependencyProvider`), Django `6.0+` `TaskBackend` + `vertebra_worker`, Litestar `ChannelsPlugin`, and `ValkeyStore`.
- `docs/usage/{databases,analytics-and-cdc}.md` — synchronous `SyncNotificationSource` and asynchronous `AsyncNotificationSource` listener examples.

## Contracts & Edge Cases

- Provide concise code examples (`msgspec.Struct` without boilerplate flags, inline config dicts) for `litestar-queues`, Django `TASKS`, `ValkeyStore` RESP3 client-side tracking, and synchronous `Sync*NotificationSource.listen()` CLI/worker usage.

## Test Matrix

- Run `pytest test_vite_and_docs_portal.py` and verify zero `swap_channels_backend` or `django.queues` references remain.

## Verification

```bash
uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

## Acceptance Criteria

- [ ] `docs/usage/{extensions,databases,analytics-and-cdc}.md` document `litestar-queues>=0.14.0`, Django `6.0+` `TaskBackend`, `ValkeyStore`, and `Sync*`/`Async*` notification listeners.
- [ ] Zero regressions in the chapter fast verification gate.

[^doc-ext]: `docs/usage/extensions.md` — Litestar and Django extension usage guide.
[^doc-db]: `docs/usage/databases.md` — Database usage guide.
[^doc-cdc]: `docs/usage/analytics-and-cdc.md` — Analytics, CDC, and notification listener guide.
