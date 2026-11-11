---
type: Task
id: "native-postgres-driver:ch31:t04"
chapter_id: ch31-data-and-testing-okf-sync
title: Synchronize product tech-stack, product definition, and distributed queues OKF chapters
description: Update product/tech-stack.md, product/product.md, and distributed-queues-and-email.md (all <= 200L) for 9 Rust crates, 5 optional companion PyO3 wheels including vertebra-postgres, native PostgreSQL capabilities, and PostgreSQLQueueBackend's vertebra-core[postgres] caller-injected PostgreSQLQueueExecutor contract.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch31:t03"
read_set:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  - pyproject.toml
  - src/rs/Cargo.toml
  - src/py/vertebra/framework/queues/postgres.py
files:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
tests: []
estimated_loc: 160
plan_revision: 6
plan_commit: bf53faf3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-10T14:45:00Z"
updated_at: "2026-11-10T14:45:00Z"
commit: null
sources:
  - id: prod-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product technology stack across Python, Rust crates, and companion wheels
  - id: prod-definition
    resource: .agents/bundles/product/product.md
    title: Product vision, runtime topologies, and database/queue capabilities
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: Distributed queues, PostgreSQLQueueBackend, and email domain chapter
  - id: root-pyproject
    resource: pyproject.toml
    title: Root optional dependencies, stubtest configuration, and postgres-bench group
  - id: ws-cargo
    resource: src/rs/Cargo.toml
    title: Nine-crate Rust workspace manifest including vertebra-postgres
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQLQueueBackend vertebra._postgres guard and caller-injected PostgreSQLQueueExecutor contract
tags:
  - postgres
  - okf
  - product
  - tech-stack
  - queues
---

# Task t04: Synchronize Product Tech-Stack, Product Definition, and Distributed Queues OKF Chapters

## Objective & Scope Boundary

Update `.agents/bundles/product/tech-stack.md` (`178L -> <= 200L`), `.agents/bundles/product/product.md` (`157L -> <= 200L`), and `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` (`119L -> <= 200L`) in present tense [^prod-tech-stack] [^prod-definition] [^okf-queues] [^root-pyproject] [^ws-cargo] [^pg-queue]:
1. `.agents/bundles/product/tech-stack.md` (`<= 200` lines):
   - Update the Rust workspace inventory to nine crates (adding `src/rs/vertebra-postgres`: `vertebra._postgres`, `postgres-protocol = "=0.6.12"`, `rustls` `ring`, `half = "2.7.1"`, `tokio-postgres = "=0.7.18"` dev-only test oracle), five optional standalone companion PyO3 wheels (`vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`, `vertebra-postgres` installed via `vertebra-core[postgres]`), six `#[pymodule(gil_used = false)]` `mypy.stubtest` targets, `[dependency-groups].postgres-bench` (`python_version == '3.12'` comparator markers) + `[tool.mypy]` `tools/benchmarks/postgres.py` overrides, and `postgres-test` / `postgres-wheel-check` / `postgres-bench` / `postgres-bench-check` verification commands [^prod-tech-stack] [^root-pyproject] [^ws-cargo].
2. `.agents/bundles/product/product.md` (`<= 200` lines):
   - Align product capabilities to six free-threaded `gil_used = false` PyO3 extensions including `vertebra._postgres`, standalone native PostgreSQL Protocol 3.0 pools/sessions, automatic Cloud SQL / AlloyDB mTLS 1.3 + IAM discovery, delayed-commit single-`Sync` `pipeline()`, binary `COPY` + `VTBARROW` `ArrowCursor`, dedicated `PostgresNotificationSource`, `PostgresChangeSource` (`pgoutput` v1 CAS checkpointing), and `PostgreSQLQueueBackend`'s `vertebra-core[postgres]` dependency guard + caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract (without claiming a built-in default queue executor prior to `queues-tasks-and-channels`), removing any residual third-party Python PostgreSQL runtime wording [^prod-definition].
3. `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` (`<= 200` lines):
   - Replace stale `ensure_module("psycopg", extra="postgres")` and legacy third-party package references in the truth block, `Queue Backend` table, and PostgreSQL section with `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` while preserving the truthful caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract (`executor is None` checks `vertebra._postgres` and raises `ImproperConfigurationError` instructing callers to pass `executor=` implementing `PostgreSQLQueueExecutor`, e.g., wrapping `AsyncPostgreSQLDriver` + `PostgresNotificationSource` over `CLAIM_POSTGRES_JOBS_SQL` `FOR UPDATE SKIP LOCKED`, `POSTGRES_QUEUE_DDL`, and dedicated `LISTEN`/`NOTIFY` zero-poll wakeups; built-in default queue executor delivery belongs to `queues-tasks-and-channels`) [^okf-queues] [^pg-queue].

Out of scope: Modifying `pyproject.toml`, `src/rs/Cargo.toml`, `src/py/vertebra/framework/queues/postgres.py`, or FFI/packaging/skill files (`ch32`).

## Bounded Read Manifest

- [.agents/bundles/product/tech-stack.md](../../../../../../../.agents/bundles/product/tech-stack.md) — Current `178`-line product technology stack [^prod-tech-stack].
- [.agents/bundles/product/product.md](../../../../../../../.agents/bundles/product/product.md) — Current `157`-line product vision and capability summary [^prod-definition].
- [.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md](../../../../../../../.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md) — Current `119`-line distributed queues and email OKF chapter [^okf-queues].
- [pyproject.toml](../../../../../../../pyproject.toml) — `[postgres]` optional extra and `[dependency-groups].postgres-bench` definitions [^root-pyproject].
- [src/rs/Cargo.toml](../../../../../../../src/rs/Cargo.toml) — Workspace member list and pinned Rust crate versions [^ws-cargo].
- [src/py/vertebra/framework/queues/postgres.py](../../../../../../../src/py/vertebra/framework/queues/postgres.py) — Final `PostgreSQLQueueBackend` native `vertebra._postgres` module check and caller-injected `PostgreSQLQueueExecutor` guard [^pg-queue].

## Symbol & Signature Contracts

- Add `src/rs/vertebra-postgres/pyproject.toml` and `src/rs/vertebra-postgres/Cargo.toml` (or `src/rs/vertebra-postgres/src/lib.rs`) to `sources:` in `tech-stack.md` with matching inline and footer citations.
- All three files pass `make review-check` (`<= 200` lines each, `<= 40`-line truth blocks, multi-line YAML lists, updated `verified[0].commit` ancestor SHAs, zero standalone `---` body rules).

## Edge Cases & Error Semantics

- `product/tech-stack.md` sits at `178` lines: compress multi-line table cells while adding `vertebra-postgres` and `[dependency-groups].postgres-bench` so it remains strictly `<= 200` lines.
- Preserve truthful caller-injected `PostgreSQLQueueExecutor` (`executor=`) documentation in `distributed-queues-and-email.md` (showing how native callers adapt `AsyncPostgreSQLDriver` + `PostgresNotificationSource`, while noting `PostgreSQLQueueBackend(executor=None)` raises `ImproperConfigurationError` after verifying `vertebra._postgres` and never claiming a built-in default executor or raw `AsyncPostgreSQLDriver` pass-through without a `PostgreSQLQueueExecutor` adapter).

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line budgets across product & queue OKF | `wc -l .agents/bundles/product/{tech-stack,product}.md .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` | Each file is strictly `<= 200` lines with truth blocks `<= 40` lines. |
| OKF validation & provenance parity | `make review-check` | Exits `0`; every `sources:` resource resolves on disk, 1:1 inline/footer footnotes match, and privacy checks pass across all `12` files updated in `ch31`. |
| Queue & stack consistency check | Inspect `tech-stack.md`, `product.md`, and `distributed-queues-and-email.md` | Lists `9` Rust crates, `6` `gil_used = false` extensions, `5` optional companion wheels, `[dependency-groups].postgres-bench`, and `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` with zero runtime `psycopg` claims. |

## Verification

- Line-budget check: `wc -l .agents/bundles/product/tech-stack.md .agents/bundles/product/product.md .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md`
- OKF & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `product/tech-stack.md`, `product/product.md`, and `distributed-queues-and-email.md` (`<= 200L` each) accurately document the nine-crate / five-companion-wheel stack, native PostgreSQL capabilities, and `PostgreSQLQueueBackend`'s `vertebra-core[postgres]` guard plus caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract (R1, R24, R27).
- [ ] `make review-check` exits `0` across all `12` OKF and index files owned by `ch31` (R27).

## Notes & Discoveries

- Keep `tokio-postgres = "=0.7.18"` explicitly labeled as a Rust `[dev-dependencies]` wire-test oracle in `product/tech-stack.md`, never a runtime `vertebra-postgres` client dependency.

[^prod-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product technology stack across Python, Rust crates, and companion wheels.
[^prod-definition]: `.agents/bundles/product/product.md` — Product vision, runtime topologies, and database/queue capabilities.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues, `PostgreSQLQueueBackend`, and email domain chapter.
[^root-pyproject]: `pyproject.toml` — Root optional dependencies, `stubtest` configuration, and `postgres-bench` group.
[^ws-cargo]: `src/rs/Cargo.toml` — Nine-crate Rust workspace manifest including `vertebra-postgres`.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — `PostgreSQLQueueBackend` `vertebra._postgres` guard and caller-injected `PostgreSQLQueueExecutor` contract.
