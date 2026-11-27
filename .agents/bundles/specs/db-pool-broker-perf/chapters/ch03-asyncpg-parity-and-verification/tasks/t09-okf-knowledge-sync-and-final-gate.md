---
type: Task
id: t09
chapter_id: ch03-asyncpg-parity-and-verification
title: Sync OKF knowledge chapters and run final repo verification gates
description: Update the four affected OKF knowledge chapters in present tense with ch01-ch03 architecture and run make lint-all, make rs-test, make review-check, and Flow validation.
status: stable
state: pending
priority: medium
verification_strategy: documentation_validation
worksheet_format: compact
depends_on:
  - t08
read_set:
  - .agents/bundles/knowledge/domains/data/sqlite-limitations.md
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
files:
  - .agents/bundles/knowledge/domains/data/sqlite-limitations.md
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
tests: []
estimated_loc: 55
plan_revision: 2
plan_commit: "16ccb56e"
commit: null
sources:
  - id: src-okf-sqlite-lim
    resource: .agents/bundles/knowledge/domains/data/sqlite-limitations.md
    title: OKF knowledge chapter for SQLite driver limitations and lazy_begin semantics
  - id: src-okf-broker
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: OKF knowledge chapter for SHM connection broker
  - id: src-okf-dialects
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: OKF knowledge chapter for SQLSpec, Advanced Alchemy, and SQLAlchemy dialects
  - id: src-okf-bench
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: OKF knowledge chapter for benchmarks and free-threaded verification
tags:
  - documentation
  - okf
  - verification
---

# t09: Sync OKF knowledge chapters and run final repo verification gates

## Objective & Scope Boundary

Synchronize the four affected OKF v0.2 knowledge chapters in present tense and verify full repository lint, Rust test, review-check, and Flow OKF validation gates.

In scope:
- `.agents/bundles/knowledge/domains/data/sqlite-limitations.md`: update `SyncSQLiteCursor` / `AsyncSQLiteCursor` `lazy_begin` documentation to note read-only statements skip implicit `BEGIN IMMEDIATE`.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`: document SQLite `lazy_begin` read-only skip semantics and `SyncSQLiteDialect` / `AsyncSQLiteDialect` `max_readers=0` default.
- `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`: document `wake_one_waiter_seqcst()`, `LeaseBroker::{wake_seq, wait_for_wake_async, partition_count}`, and `AsyncSHMBrokerTransport` per-loop local handoff + single-leader cross-process watcher.
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`: document `granian-rsgi-data-stdlib`, `NativePostgresWorldDriver` single-checkout parity, and `--group postgres-bench` 1:1 `asyncpg`/`psycopg` benchmarking.
- Run `make lint-all`, `make rs-test`, `make review-check`, and Flow OKF validation.

Out of scope:
- Touching unrelated pre-existing unstaged documentation edits outside the four target files.

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/sqlite-limitations.md` [^src-okf-sqlite-lim]
- `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` [^src-okf-broker]
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` [^src-okf-dialects]
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` [^src-okf-bench]

## Contracts & Edge Cases

- Maintain OKF v0.2 compliance: `<= 40`-line truth blocks, 1:1 `sources:` <-> `[^id]:` footnotes, present-tense prose only, no standalone `---` body rules, no historical notes or commit SHAs in prose.
- Note: `.agents/bundles/knowledge/domains/data/{sqlite-limitations,broker-and-shm-leases}.md` and `sqlspec-alchemy-and-dialects.md` currently have pre-existing unstaged edits in the working tree; inspect `git diff` on those files before editing so we preserve or stage only scoped hunks cleanly.

## Test Matrix

- `make lint-all` (`exit 0`)
- `make rs-test` (`exit 0`)
- `make review-check` (`exit 0`)
- Flow OKF validator (`exit 0`)

## Verification

```bash
make lint-all && make rs-test && make review-check
```

## Acceptance Criteria

- All 4 OKF knowledge chapters accurately reflect shipped code in present tense.
- `make lint-all`, `make rs-test`, `make review-check`, and Flow OKF validation all exit `0`.

[^src-okf-sqlite-lim]: `.agents/bundles/knowledge/domains/data/sqlite-limitations.md` — OKF knowledge chapter for SQLite driver limitations and lazy_begin semantics.
[^src-okf-broker]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — OKF knowledge chapter for SHM connection broker.
[^src-okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — OKF knowledge chapter for SQLSpec, Advanced Alchemy, and SQLAlchemy dialects.
[^src-okf-bench]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — OKF knowledge chapter for benchmarks and free-threaded verification.
