---
type: Task
id: "native-sqlite-driver:ch05:t02"
flow_id: native-sqlite-driver
chapter_id: ch05-docs-and-knowledge-sync
title: "Author sqlite-limitations.md and Refresh Database Drivers OKF Chapter"
description: "Create sqlite-limitations.md (<= 200 lines), update database-drivers-and-dialers.md, and link in domains/index.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch05:t01"
files:
  - .agents/bundles/knowledge/domains/data/sqlite-limitations.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
  - .agents/bundles/knowledge/domains/index.md
estimated_loc: 155
tags:
  - okf
  - sqlite
  - limitations
sources:
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers and dialers OKF chapter"
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: "OKF domains catalog index"
---

# Task `t02`: Author sqlite-limitations.md and Refresh Database Drivers OKF Chapter

## Objective & Scope Boundary

Author `sqlite-limitations.md` (`<= 200` lines), update `database-drivers-and-dialers.md` (`<= 200` lines), and index in `domains/index.md` per [`../spec.md` §2](../spec.md) [^okf-drivers] [^domains-idx].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Driver wheel architecture chapter.
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Canonical OKF limitations format.
- `.agents/bundles/knowledge/domains/index.md` — Domains index catalog.

## Contracts & Edge Cases

- Record WAL single-writer serialization, `:memory:` single-worker affinity vs shared URI, dynamic SQLite expression affinity in `VTBARROW`, and disabled `load_extension` in `sqlite-limitations.md`.
- **Edge Case**: Keep `domains/index.md` free of YAML frontmatter and standalone `---` rules.

## Test Matrix

- `make review-check` validates frontmatter, truth blocks, links, and line caps.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] `sqlite-limitations.md` and `database-drivers-and-dialers.md` stay `<= 200` lines and pass `make review-check`.
- [ ] Zero regressions in the chapter fast verification gate.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
