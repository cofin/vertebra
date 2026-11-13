---
type: Task
id: "native-redis-driver:ch08:t02"
flow_id: native-redis-driver
chapter_id: ch08-changelog-and-okf-sync
title: "Author redis-limitations.md and Refresh Data and Queue OKF Chapters"
description: "Create redis-limitations.md (<= 200 lines) and update database-drivers-and-dialers.md and distributed-queues-and-email.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch08:t01"
files:
  - .agents/bundles/knowledge/domains/data/redis-limitations.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
estimated_loc: 170
tags:
  - okf
  - redis
  - valkey
  - limitations
sources:
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers and dialers OKF chapter"
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: "Distributed queues and email OKF chapter"
---

# Task `t02`: Author redis-limitations.md and Refresh Data and Queue OKF Chapters

## Objective & Scope Boundary

Create `redis-limitations.md` (`<= 200` lines) and update `database-drivers-and-dialers.md` and `distributed-queues-and-email.md` per [`../spec.md` §2](../spec.md) [^okf-drivers] [^okf-queues].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Driver wheel architecture (`<= 200` lines).
- `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Queue backend architecture (`<= 200` lines).
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Canonical OKF limitations layout.

## Contracts & Edge Cases

- Document dedicated pub/sub listener sockets, Cluster `{hash_tag}` `CROSSSLOT` rules, RESP3 `CLIENT TRACKING` requirements, and `unix://` standalone-only constraints in `redis-limitations.md`.
- **Edge Case**: Every OKF chapter stays `<= 200` lines with valid `sources:` and footnotes.

## Test Matrix

- `make review-check` exits `0`.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `redis-limitations.md`, `database-drivers-and-dialers.md`, and `distributed-queues-and-email.md` pass `make review-check`.
- [ ] Zero regressions in the chapter fast verification gate.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues and email OKF chapter.
