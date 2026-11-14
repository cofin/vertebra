---
type: Task
id: native-redis-driver:ch04-queues-conformance-and-docs:t03
flow_id: native-redis-driver
chapter_id: ch04-queues-conformance-and-docs
title: Update Public Databases/Installation Docs, Native Stubs Test, Changelog, and Tech Stack
description: Update docs/usage/databases.md, docs/getting-started/installation.md, test_native_stubs.py, docs/changelog/index.md, AGENTS.md, and product/tech-stack.md.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 104fd6fa
depends_on:
- native-redis-driver:ch04-queues-conformance-and-docs:t02
files:
- docs/usage/databases.md
- docs/usage/queues-and-realtime.md
- docs/reference/native-stubs.md
- docs/changelog/index.md
- AGENTS.md
- .agents/bundles/product/product.md
tests: []
read_set:
- docs/usage/databases.md
- docs/usage/queues-and-realtime.md
- docs/reference/native-stubs.md
- docs/changelog/index.md
- AGENTS.md
- .agents/bundles/product/product.md
estimated_loc: 220
tags:
- docs
- redis
- valkey
- queues
- changelog
- product
sources:
- id: docs-db
  resource: docs/usage/databases.md
  title: Database and cache drivers usage guide
- id: docs-queues
  resource: docs/usage/queues-and-realtime.md
  title: Queues and realtime channels usage guide
- id: docs-stubs
  resource: docs/reference/native-stubs.md
  title: Native extension stub API reference
- id: docs-changelog
  resource: docs/changelog/index.md
  title: Release changelog
- id: agents-md
  resource: AGENTS.md
  title: Repository agent instructions and Makefile commands
- id: product-md
  resource: .agents/bundles/product/product.md
  title: OKF product definition and crate capability table
---

# Task `t03`: Update Public Databases/Installation Docs, Native Stubs Test, Changelog, and Tech Stack

## Objective & Scope Boundary

Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, and `docs/reference/native-stubs.md` per [`../spec.md` §2](../spec.md) [^docs-db] [^docs-queues] [^docs-stubs].

Update `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` per [`../spec.md` §2](../spec.md) [^docs-changelog] [^agents-md] [^product-md].

## Bounded Read Manifest

- `docs/usage/databases.md` — Redis and Valkey configuration, tracking, and topology docs.
- `docs/usage/queues-and-realtime.md` — Queue install tabs (`redis` PyPI removal) and notification sources.
- `docs/reference/native-stubs.md` — TOC budget (`<= 14`) and `heading_level: 4` stub blocks.

- `docs/changelog/index.md` — `Unreleased` section.
- `AGENTS.md` — Crate list and Quick Verification Commands (`make redis-test`, `make redis-wheel-check`).
- `.agents/bundles/product/product.md` — `vertebra-db` row referencing `parse_resp3_frame`/`crc16_xmodem`/`redis_hash_slot`.

## Contracts & Edge Cases

- Replace third-party `redis` PyPI install instructions with tabbed `vertebra-core[redis]` and `vertebra-core[valkey]` blocks (`uv`, `pip`, `pdm`, `poetry`).
- Document `SyncNotificationSource` / `AsyncNotificationSource` and keep `docs/reference/native-stubs.md` at `heading_level: 4` (`<= 14` TOC items, `< 120_000` bytes).
- **Edge Case**: Ensure zero stale references to `NotificationSource`, `PostgresNotificationSource`, or `OracleNotificationSource` remain in touched docs.

- Remove `parse_resp3_frame`, `crc16_xmodem`, and `redis_hash_slot` from the `vertebra-db` row in `product.md` and add `vertebra-redis`.
- Add `vertebra-redis`, `make redis-test`, and `make redis-wheel-check` to `AGENTS.md`.
- **Edge Case**: Keep `product.md` `<= 200` lines and verify `verified[].commit` is a valid ancestor SHA.

## Test Matrix

- `make docs` builds cleanly and passes `verify_docs_artifacts.py`.

- `make review-check && make docs` exits `0`.

## Verification

```bash
make review-check && make docs
```

## Acceptance Criteria

- [ ] Public docs and `native-stubs.md` render cleanly via `make docs`.
- [ ] `changelog`, `AGENTS.md`, and `product.md` reflect shipped `vertebra-redis` and `Sync*`/`Async*` notification contracts.
- [ ] Zero regressions in the fast verification gate.

[^docs-db]: `docs/usage/databases.md` — Database and cache drivers usage guide.
[^docs-queues]: `docs/usage/queues-and-realtime.md` — Queues and realtime channels usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^agents-md]: `AGENTS.md` — Repository agent instructions and Makefile commands.
[^product-md]: `.agents/bundles/product/product.md` — OKF product definition and crate capability table.
