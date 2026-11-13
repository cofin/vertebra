---
type: Task
id: "native-redis-driver:ch08:t01"
flow_id: native-redis-driver
chapter_id: ch08-changelog-and-okf-sync
title: "Update Changelog, AGENTS.md, and OKF Product Definition"
description: "Record vertebra-redis, NotificationSource split, and resp3 removal in changelog, AGENTS.md, and product.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch07:t03"
files:
  - docs/changelog/index.md
  - AGENTS.md
  - .agents/bundles/product/product.md
tests: []
read_set:
  - docs/changelog/index.md
  - AGENTS.md
  - .agents/bundles/product/product.md
estimated_loc: 90
tags:
  - docs
  - changelog
  - product
sources:
  - id: docs-changelog
    resource: docs/changelog/index.md
    title: "Release changelog"
  - id: agents-md
    resource: AGENTS.md
    title: "Repository agent instructions and Makefile commands"
  - id: product-md
    resource: .agents/bundles/product/product.md
    title: "OKF product definition and crate capability table"
---

# Task `t01`: Update Changelog, AGENTS.md, and OKF Product Definition

## Objective & Scope Boundary

Update `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` per [`../spec.md` §2](../spec.md) [^docs-changelog] [^agents-md] [^product-md].

## Bounded Read Manifest

- `docs/changelog/index.md` — `Unreleased` section.
- `AGENTS.md` — Crate list and Quick Verification Commands (`make redis-test`, `make redis-wheel-check`).
- `.agents/bundles/product/product.md` — `vertebra-db` row referencing `parse_resp3_frame`/`crc16_xmodem`/`redis_hash_slot`.

## Contracts & Edge Cases

- Remove `parse_resp3_frame`, `crc16_xmodem`, and `redis_hash_slot` from the `vertebra-db` row in `product.md` and add `vertebra-redis`.
- Add `vertebra-redis`, `make redis-test`, and `make redis-wheel-check` to `AGENTS.md`.
- **Edge Case**: Keep `product.md` `<= 200` lines and verify `verified[].commit` is a valid ancestor SHA.

## Test Matrix

- `make review-check && make docs` exits `0`.

## Verification

```bash
make review-check && make docs
```

## Acceptance Criteria

- [ ] `changelog`, `AGENTS.md`, and `product.md` reflect shipped `vertebra-redis` and `Sync*`/`Async*` notification contracts.
- [ ] Zero regressions in the chapter fast verification gate.

[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^agents-md]: `AGENTS.md` — Repository agent instructions and Makefile commands.
[^product-md]: `.agents/bundles/product/product.md` — OKF product definition and crate capability table.
