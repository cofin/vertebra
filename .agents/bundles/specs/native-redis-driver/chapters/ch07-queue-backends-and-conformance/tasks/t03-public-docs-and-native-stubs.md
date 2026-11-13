---
type: Task
id: "native-redis-driver:ch07:t03"
flow_id: native-redis-driver
chapter_id: ch07-queue-backends-and-conformance
title: "Update Database, Queues/Realtime, and Native Stubs Documentation"
description: "Document vertebra-core[redis]/[valkey], Sync*/Async* notification sources, queue URLs, and vertebra._redis stubs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch06:t04"
  - "native-redis-driver:ch07:t01"
files:
  - docs/usage/databases.md
  - docs/usage/queues-and-realtime.md
  - docs/reference/native-stubs.md
tests: []
read_set:
  - docs/usage/databases.md
  - docs/usage/queues-and-realtime.md
  - docs/reference/native-stubs.md
estimated_loc: 155
tags:
  - docs
  - redis
  - valkey
  - queues
sources:
  - id: docs-db
    resource: docs/usage/databases.md
    title: "Database and cache drivers usage guide"
  - id: docs-queues
    resource: docs/usage/queues-and-realtime.md
    title: "Queues and realtime channels usage guide"
  - id: docs-stubs
    resource: docs/reference/native-stubs.md
    title: "Native extension stub API reference"
---

# Task `t03`: Update Database, Queues/Realtime, and Native Stubs Documentation

## Objective & Scope Boundary

Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, and `docs/reference/native-stubs.md` per [`../spec.md` §2](../spec.md) [^docs-db] [^docs-queues] [^docs-stubs].

## Bounded Read Manifest

- `docs/usage/databases.md` — Redis and Valkey configuration, tracking, and topology docs.
- `docs/usage/queues-and-realtime.md` — Queue install tabs (`redis` PyPI removal) and notification sources.
- `docs/reference/native-stubs.md` — TOC budget (`<= 14`) and `heading_level: 4` stub blocks.

## Contracts & Edge Cases

- Replace third-party `redis` PyPI install instructions with tabbed `vertebra-core[redis]` and `vertebra-core[valkey]` blocks (`uv`, `pip`, `pdm`, `poetry`).
- Document `SyncNotificationSource` / `AsyncNotificationSource` and keep `docs/reference/native-stubs.md` at `heading_level: 4` (`<= 14` TOC items, `< 120_000` bytes).
- **Edge Case**: Ensure zero stale references to `NotificationSource`, `PostgresNotificationSource`, or `OracleNotificationSource` remain in touched docs.

## Test Matrix

- `make docs` builds cleanly and passes `verify_docs_artifacts.py`.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] Public docs and `native-stubs.md` render cleanly via `make docs`.
- [ ] Zero regressions in the chapter fast verification gate.

[^docs-db]: `docs/usage/databases.md` — Database and cache drivers usage guide.
[^docs-queues]: `docs/usage/queues-and-realtime.md` — Queues and realtime channels usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
