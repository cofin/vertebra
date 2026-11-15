---
type: Task
id: native-valkey-driver:ch04-conformance-and-docs:t03
flow_id: native-valkey-driver
chapter_id: ch04-conformance-and-docs
title: Update Public Docs, Native Stubs, Changelog, AGENTS.md, and Product Definition
description: Update docs/usage/databases.md, docs/usage/queues-and-realtime.md, docs/reference/native-stubs.md, docs/changelog/index.md, AGENTS.md, and .agents/bundles/product/product.md.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch04-conformance-and-docs:t02
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
verification_strategy: documentation_validation
---

# Task `t03`: Update Public Docs, Native Stubs, Changelog, AGENTS.md, and Product Definition

## Objective & Scope Boundary

Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md` (`Unreleased`), `AGENTS.md`, and `.agents/bundles/product/product.md` per [`../spec.md` §2](../spec.md) [^docs-db] [^docs-queues] [^docs-stubs] [^docs-changelog] [^agents-md] [^product-md].

## Bounded Read Manifest

- `docs/usage/databases.md` — Valkey configuration, hash-field expiration (`HEXPIRE`/`HPEXPIRE`), tracking, and topology docs.
- `docs/usage/queues-and-realtime.md` — Queue install tabs (`vertebra-core[valkey]`) and notification sources (`SyncValkeyNotificationSource`, `AsyncValkeyNotificationSource`).
- `docs/reference/native-stubs.md` — TOC budget (`<= 14`) and `heading_level: 4` stub blocks.
- `docs/changelog/index.md` — `Unreleased` section.
- `AGENTS.md` — Crate list and Quick Verification Commands (`make valkey-test`, `make valkey-wheel-check`).
- `.agents/bundles/product/product.md` — `vertebra-db` row and `vertebra-valkey` row.

## Contracts & Edge Cases

- Document `vertebra-core[valkey]` (`vertebra-valkey` wheel) with tabbed install blocks (`uv`, `pip`, `pdm`, `poetry`) and Valkey 9.0 hash-field expiration examples.
- Add `vertebra._valkey` to `docs/reference/native-stubs.md` at `heading_level: 4` (`<= 14` TOC items, `< 120_000` bytes).
- Remove `parse_resp3_frame`, `crc16_xmodem`, and `redis_hash_slot` from the `vertebra-db` row in `.agents/bundles/product/product.md`, add `vertebra-valkey`, and sync `make valkey-test` and `make valkey-wheel-check` in `AGENTS.md` (`DOC-08`).
- **Edge Case**: Keep `.agents/bundles/product/product.md` `<= 200` lines with a valid `verified[].commit` ancestor SHA.

## Test Matrix

- `uv run --no-sync python -m tools.docs.scan_public_docstrings` and Zensical artifact budget checks pass cleanly.
- `make review-check` exits `0`.

## Verification

```bash
uv run --no-sync python -m tools.docs.scan_public_docstrings && make review-check
```

## Acceptance Criteria

- [ ] Public docs, `native-stubs.md`, and `Unreleased` changelog render cleanly.
- [ ] `AGENTS.md` and `.agents/bundles/product/product.md` reflect shipped `vertebra-valkey` and removal of the `vertebra_db` `resp3` module.
- [ ] Zero regressions in the fast verification gate.

[^docs-db]: `docs/usage/databases.md` — Database and cache drivers usage guide.
[^docs-queues]: `docs/usage/queues-and-realtime.md` — Queues and realtime channels usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^agents-md]: `AGENTS.md` — Repository agent instructions and Makefile commands.
[^product-md]: `.agents/bundles/product/product.md` — OKF product definition and crate capability table.
