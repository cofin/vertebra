---
type: Task
id: native-redis-driver:ch04-queues-conformance-and-docs:t04
flow_id: native-redis-driver
chapter_id: ch04-queues-conformance-and-docs
title: Author OKF redis-limitations.md and Synchronize Data/Framework Chapters and Review Skills
description: Create redis-limitations.md and sync database-drivers-and-dialers.md, distributed-queues-and-email.md, review-pyo3-bridge/SKILL.md, domains/index.md, and workflow.md.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 1b191388
depends_on:
- native-redis-driver:ch04-queues-conformance-and-docs:t03
files:
- .agents/bundles/knowledge/domains/data/redis-limitations.md
- .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
- .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
- .agents/skills/review-pyo3-bridge/SKILL.md
- .agents/bundles/knowledge/domains/index.md
- .agents/bundles/knowledge/workflow.md
tests: []
read_set:
- .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
- .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
- .agents/bundles/knowledge/domains/data/postgres-limitations.md
- .agents/skills/review-pyo3-bridge/SKILL.md
- .agents/bundles/knowledge/domains/index.md
- .agents/bundles/knowledge/workflow.md
estimated_loc: 220
tags:
- okf
- redis
- valkey
- limitations
- skills
- workflow
sources:
- id: okf-drivers
  resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  title: Database drivers and dialers OKF chapter
- id: okf-queues
  resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  title: Distributed queues and email OKF chapter
- id: review-pyo3-skill
  resource: .agents/skills/review-pyo3-bridge/SKILL.md
  title: PyO3 bridge review skill
- id: domains-idx
  resource: .agents/bundles/knowledge/domains/index.md
  title: OKF domains catalog index
- id: okf-workflow
  resource: .agents/bundles/knowledge/workflow.md
  title: Project Makefile and verification workflow chapter
---

# Task `t04`: Author OKF redis-limitations.md and Synchronize Data/Framework Chapters and Review Skills

## Objective & Scope Boundary

Create `redis-limitations.md` (`<= 200` lines), update `database-drivers-and-dialers.md` and `distributed-queues-and-email.md`, and synchronize `vertebra-redis`, `redis-limitations.md`, `make redis-test`, and `make redis-wheel-check` across `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` per [`../spec.md` §2](../spec.md) [^okf-drivers] [^okf-queues] [^review-pyo3-skill] [^domains-idx] [^okf-workflow].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Driver wheel architecture (`<= 200` lines).
- `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Queue backend architecture (`<= 200` lines).
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Canonical OKF limitations layout.
- `.agents/skills/review-pyo3-bridge/SKILL.md` — Review skill crate list (`<= 200` lines).
- `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
- `.agents/bundles/knowledge/workflow.md` — Canonical Makefile targets (`<= 200` lines).

## Contracts & Edge Cases

- Document dedicated pub/sub listener sockets, Cluster `{hash_tag}` `CROSSSLOT` rules, RESP3 `CLIENT TRACKING` requirements, and `unix://` standalone-only constraints in `redis-limitations.md`.
- Keep every OKF chapter and `review-pyo3-bridge/SKILL.md` `<= 200` lines with valid `sources:` and footnotes, and keep `domains/index.md` free of YAML frontmatter and standalone `---` rules.
- **Edge Case**: Preserve `vertebra-sqlite` entries if `native-sqlite-driver` landed first.

## Test Matrix

- `make review-check` exits `0` with zero line-cap, frontmatter, link, or privacy issues.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `redis-limitations.md`, `database-drivers-and-dialers.md`, and `distributed-queues-and-email.md` stay `<= 200` lines and pass `make review-check`.
- [ ] `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` pass `make review-check`.
- [ ] Zero regressions in the fast verification gate.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues and email OKF chapter.
[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
