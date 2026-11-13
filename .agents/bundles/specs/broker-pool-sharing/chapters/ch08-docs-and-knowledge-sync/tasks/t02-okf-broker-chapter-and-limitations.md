---
type: Task
id: "broker-pool-sharing:ch08:t02"
flow_id: broker-pool-sharing
chapter_id: ch08-docs-and-knowledge-sync
title: "Synchronize OKF Broker Chapter and Author shm-broker-limitations.md"
description: "Update broker-and-shm-leases.md, create shm-broker-limitations.md (<= 200 lines each), and index in domains/index.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch08:t01"
files:
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/data/shm-broker-limitations.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
  - .agents/bundles/knowledge/domains/index.md
estimated_loc: 185
tags:
  - okf
  - knowledge
  - limitations
sources:
  - id: broker-okf
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: "Broker and SHM leases OKF chapter"
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: "OKF domains catalog index"
---

# Task `t02`: Synchronize OKF Broker Chapter and Author shm-broker-limitations.md

## Objective & Scope Boundary

Refresh `broker-and-shm-leases.md`, author `shm-broker-limitations.md` (`<= 200` lines each), and link `shm-broker-limitations.md` in `domains/index.md` per [`../spec.md` §2](../spec.md) [^broker-okf] [^domains-idx].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Current broker OKF chapter.
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Canonical OKF limitations chapter format.
- `.agents/bundles/knowledge/domains/index.md` — Domains index catalog.

## Contracts & Edge Cases

- Document `ShmOwnerHeader`, `BrokerEngineCapsule` ABI v1, `LeaseState::Quarantined`, UDS `SCM_RIGHTS` attach, `BrokerHost` crash `ROLLBACK`, `vertebra.db.dbapi`, and `BrokerPool` in `broker-and-shm-leases.md`.
- Record BigQuery/Spanner native broker exclusion, Windows named-SHM exclusion, `MAX_POOL_PARTITIONS = 64`, 256-slot signal cleanup cap, and macOS 31-byte `shm_open` name limit in `shm-broker-limitations.md`.
- **Edge Case**: Every `sources:` path and relative link resolves on disk; `domains/index.md` carries zero YAML frontmatter.

## Test Matrix

- `make review-check` exits `0` with zero OKF, link, truth-budget, or privacy errors.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] Both OKF chapters are `<= 200` lines, present-tense, and pass `make review-check`.
- [ ] Zero regressions in the chapter fast verification gate.

[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker and SHM leases OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
