---
type: Task
id: "broker-pool-sharing:ch09:t02"
chapter_id: ch09-docs-and-okf
title: Update the OKF broker chapter and add the limitations record
description: Rewrite the broker-and-shm-leases OKF chapter to the shipped architecture, add broker-pool-limitations.md covering BigQuery/Spanner exclusion, macOS ulock caveats, and Windows non-support, and link it from the domains index.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "broker-pool-sharing:ch08:t04"
read_set:
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/specs/broker-pool-sharing/spec.md
files:
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/data/broker-pool-limitations.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
estimated_loc: 200
plan_revision: 3
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
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: broker-okf
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: Broker and SHM leases knowledge chapter
  - id: domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: Domains knowledge index
tags:
  - okf
  - limitations
---

# Task t02: Update the OKF Broker Chapter and Add the Limitations Record

## Objective & Scope Boundary

1. Rewrite `broker-and-shm-leases.md` [^broker-okf] in present tense: segment ownership and cleanup, attach handshake, per-pool partitions and heartbeats, `dbrpc` frames, `BrokerEngineCapsule` / `BrokerEngineRegistry`, lease states including `Quarantined`, reclamation with `ROLLBACK`, `stable_hash64`, SQLSpec proxy and `BrokerPool`, metrics. Include the detailed implementation Mermaid diagram here (not in `docs/`). Use `path::Symbol` provenance footnotes.
2. Create `broker-pool-limitations.md` with OKF v0.2 frontmatter: BigQuery (stateless REST jobs, nothing to lease) and Spanner (sessions managed inside `vertebra-spanner`) excluded from native broker pools; SQLite owned by `native-sqlite-driver`; macOS `ulock` wait/wake caveats (private API surface, no cross-process robust-mutex equivalent, wake latency); Windows named segments unsupported; any caveat recorded in task Notes across `ch01`–`ch08`.
3. Add a link to the limitations record in `domains/index.md` [^domains-index].

## Bounded Read Manifest

- The two existing OKF files [^broker-okf] [^domains-index]; `database-drivers-and-dialers.md` for link style; root spec.

## Symbol & Signature Contracts

- Frontmatter matches sibling OKF chapters (`type`, `title`, `description`, `sources` with `[^id]` footnotes).
- Relative links resolve from `domains/data/`.

## Edge Cases & Error Semantics

- A caveat from a completed task Notes section that is not in the record is a defect; the executor scans `chapters/*/tasks/*.md` Notes before writing.
- No private paths, internal product names, or customer references.

## Public-Seam Test Blueprint

| Check | Command | Expected |
| --- | --- | --- |
| OKF validation | `make review-check` | exit `0` |
| Index link | `git grep -n "broker-pool-limitations.md" .agents/bundles/knowledge/domains/index.md` | one hit |
| Stale names | `git grep -n "InMemoryBrokerTransport\|use_shm" .agents/bundles/knowledge` | no output |

## Verification

- `make review-check`

## Acceptance Criteria

- Three OKF files updated or created; matrix checks pass.

## Notes & Discoveries

- None yet.
