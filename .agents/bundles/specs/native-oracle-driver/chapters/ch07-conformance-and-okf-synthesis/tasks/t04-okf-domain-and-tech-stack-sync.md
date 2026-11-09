---
type: Task
id: "native-oracle-driver:ch07:t04"
flow_id: native-oracle-driver
chapter_id: ch07-conformance-and-okf-synthesis
title: "Task 04: Synchronize OKF Data Domain Chapters and Tech Stack"
description: Update database-drivers-and-dialers.md, query-pipeline-cdc-and-tenancy.md, and tech-stack.md with the shipped vertebra-oracle architecture, crate-local Arrow 59 SHM boundary, delayed-commit pipelines, and VPD reset-or-evict tenancy.
status: stable
state: pending
priority: P1
owner: null
depends_on:
  - "native-oracle-driver:ch07:t03"
files:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/product/tech-stack.md
tests: []
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/product/tech-stack.md
verification_strategy: documentation_validation
estimated_loc: 140
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T01:28:30Z"
completed_at: null
commit_sha: null
commit: null
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public contracts for vertebra-oracle, Arrow 59 SHM, pipelines, and VPD tenancy
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: OKF data domain chapter for database drivers and dialers
  - id: okf-pipeline
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: OKF data domain chapter for query pipelines, notifications, and tenancy
  - id: okf-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product tech-stack inventory of Rust workspace crates and optional wheels
tags:
  - oracle
  - okf
  - knowledge
  - architecture
---

# Task 04: Synchronize OKF Data Domain Chapters and Tech Stack

## Objective & Scope Boundary

Synchronize `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`, and `.agents/bundles/product/tech-stack.md` in present tense with the shipped `vertebra-oracle` (`vertebra._oracle`, `oracledb = "=26.0.0-beta.5"`, crate-local `arrow 59`) architecture, worker-thread completion bridge, delayed-commit pipelines, VPD reset-or-evict tenancy, and `OracleNotificationSource` [^ora-contracts] [^okf-drivers] [^okf-pipeline] [^okf-tech-stack].

- **In Scope**:
  - `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`: Document `vertebra-oracle` (`vertebra._oracle`), `oracledb = "=26.0.0-beta.5"` (`features = ["arrow"]`), crate-local `arrow 59` `construct_py_arrow_cursor` / `ShmArrowStreamWriter` integration, per-connection worker threads + `connection.break_execution()` cancellation, Autonomous Wallet mTLS, and DRCP pooling (`<= 200` lines, `<= 40`-line truth block, verified `sources:` + `[^id]:` footnotes).
  - `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`: Document `SyncOraclePipeline` / `AsyncOraclePipeline` delayed-commit transaction/`SAVEPOINT` lifecycle, VPD trusted context package (`context_package_proc` / `context_clear_proc`) + `DBMS_SESSION.SET_IDENTIFIER` reset-or-evict pool contract in `vertebra.db.rls`, and `OracleNotificationSource` (`<= 200` lines, `<= 40`-line truth block, verified `sources:` + `[^id]:` footnotes).
  - `.agents/bundles/product/tech-stack.md`: Register `vertebra-oracle` (`vertebra._oracle`, `vertebra-core[oracle]`, `oracledb = "=26.0.0-beta.5"`, crate-local `arrow 59`) in the Rust workspace crates and optional wheel tables (`<= 200` lines).
- **Out of Scope**:
  - User-facing Zensical docs (`docs/usage/databases.md`, `README.md`, `AGENTS.md`, completed in `native-oracle-driver:ch07:t03`).

## Bounded Read Manifest

1. `.agents/bundles/specs/native-oracle-driver/contracts.md` — Authoritative contracts and symbol paths.
2. `.agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md` — Chapter 07 OKF synchronization plan.
3. `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Existing driver architecture chapter.
4. `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Existing pipeline, notification, and tenancy chapter.
5. `.agents/bundles/product/tech-stack.md` — Existing product technology stack chapter.

## Symbol & Signature Contracts

### OKF v0.2 Invariants

- Every updated OKF file must remain `<= 200` lines total, with a `<= 40`-line `<!-- truth: start -->` ... `<!-- truth: end -->` block (`3`–`5` bullets).
- Every `sources:` entry in YAML frontmatter must point to an existing `path/to/file.ext::Symbol` on disk and have a matching `[^id]:` footnote definition at the bottom of the file, cited inline in prose.
- Zero standalone `---` horizontal rules in the Markdown body and zero historical/changelog prose.

## Edge Cases & Error Semantics

- **Existing On-Disk `sources:` Verification**: By the time `ch07:t04` executes, `src/rs/vertebra-oracle/src/{lib,client,pool,arrow,notify}.rs` and `src/py/vertebra/db/adapters/oracle/{driver,core,notify,embeddings}.py` exist on disk; verify every `sources[].resource` resolves cleanly via `make review-check`.
- **Line-Budget Discipline**: Consolidate prose in place rather than appending long sections so all three OKF chapters stay strictly `<= 200` lines.

## Public-Seam Test Blueprint

- Run `make review-check` to validate OKF v0.2 frontmatter, truth-block line caps, `sources:` symbol existence, footnote parity, and privacy sanitization.
- Run `make lint-all` to confirm repository-wide checks remain green.

## Verification

```bash
make review-check
make lint-all
```

## Acceptance Criteria

- [ ] `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` documents `vertebra-oracle`, worker-thread completion, crate-local `arrow 59` SHM transfer, Wallet mTLS, and DRCP pooling within `<= 200` lines [^okf-drivers].
- [ ] `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` documents Oracle delayed-commit pipelines, VPD trusted-package + `SET_IDENTIFIER` reset-or-evict tenancy, and `OracleNotificationSource` within `<= 200` lines [^okf-pipeline].
- [ ] `.agents/bundles/product/tech-stack.md` registers `vertebra-oracle` (`vertebra._oracle`, `oracledb = "=26.0.0-beta.5"`, crate-local `arrow 59`) within `<= 200` lines, and `make review-check` exits `0` [^okf-tech-stack].

## Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public contracts for `vertebra-oracle`, Arrow 59 SHM, pipelines, and VPD tenancy.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — OKF data domain chapter for database drivers and dialers.
[^okf-pipeline]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — OKF data domain chapter for query pipelines, notifications, and tenancy.
[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product tech-stack inventory of Rust workspace crates and optional wheels.
