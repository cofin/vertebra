---
type: Task
id: "native-postgres-driver:ch30:t04"
chapter_id: ch30-user-and-domain-docs
title: Synchronize extensions, benchmarks, and product tech-stack OKF chapters
description: Update sqlspec-alchemy-and-dialects.md, benchmarks-and-free-threaded.md (<= 200 lines each), and product/tech-stack.md for unified $N SQLSpec/SQLAlchemy binds, 6 PyO3 extensions, [dependency-groups].bench + postgres-bench*, and the 9-crate / 5-companion-wheel stack.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch30:t03"
read_set:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/product/tech-stack.md
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - pyproject.toml
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/product/tech-stack.md
tests: []
estimated_loc: 195
plan_revision: 5
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: okf-extensions
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: OKF chapter for SQLSpec adapters, Advanced Alchemy, and SQLAlchemy +vtb dialects
  - id: okf-benchmarks
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: OKF chapter for benchmark governance and free-threaded Python verification
  - id: prod-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product tech-stack manifest across Python, Rust crates, and companion wheels
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver
    title: SQLSpec PostgreSQL sync and async adapters using ParameterStyle.NUMERIC
  - id: root-pyproject
    resource: pyproject.toml
    title: Workspace optional [postgres] extra, [dependency-groups].bench, and Mypy overrides
tags:
  - postgres
  - okf
  - extensions
  - benchmarks
  - tech-stack
---

# Task t04: Synchronize Extensions, Benchmarks, and Product Tech-Stack OKF Chapters

## Objective & Scope Boundary

Complete the `11`-file documentation and OKF synchronization across `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, and `.agents/bundles/product/tech-stack.md` [^okf-extensions] [^okf-benchmarks] [^prod-tech-stack] [^sqlspec-adapters] [^root-pyproject]:
1. `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`<= 200` lines per `DOC-08`):
   - Replace stale `sync %s and async $N` PostgreSQL claims with present-tense unified `ParameterStyle.NUMERIC` (`$N`) single-pass transport compilation across both `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` (preserving caller `StatementConfig` identity and `$1` / `$1::<cast>` tenant predicates) plus `paramstyle = "numeric_dollar"` across `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect`.
2. `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`<= 200` lines per `DOC-08`):
   - Update from `5` to `6` PyO3 `gil_used = false` extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres`), document `[dependency-groups].bench` (`psycopg[binary]==3.2.13`, `asyncpg==0.31.0`), `tools/benchmarks/postgres.py` (`schema_version=1`), `src/rs/vertebra-postgres/benches/postgres_native.rs`, `make postgres-bench` / `make postgres-bench-check` (`median <= 1.25x`, `p95 <= 1.50x`, `statements_s >= 0.80x`, `CV <= 0.15`), and isolated `3.14t`/`3.15t` `vertebra._postgres` disabled-GIL child-import and concurrent `SyncPostgresPool` verification.
3. `.agents/bundles/product/tech-stack.md`:
   - Update to `9` Rust workspace crates (adding `vertebra-postgres`), `5` standalone companion wheels (`vertebra-postgres` installed via `vertebra-core[postgres]`), `6` `mypy.stubtest` extension targets (`vertebra._postgres`), pinned `postgres-protocol = "=0.6.12"` (plus dev-only `tokio-postgres = "=0.7.18"`), and `[dependency-groups].bench` / `[tool.mypy]` `psycopg`/`asyncpg` isolation.

Out of scope: Modifying user guides (`ch30:t01`), `README.md`/`workflow.md`/`AGENTS.md` (`ch30:t02`), data OKF chapters (`ch30:t03`), or final branch review gates (`ch31:t01`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md](../../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md) — Current SQLSpec and SQLAlchemy `+vtb` dialect table (`151` lines) [^okf-extensions].
- [.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md](../../../../../knowledge/testing/benchmarks-and-free-threaded.md) — Current benchmark and `3.14t`/`3.15t` GIL testing chapter (`149` lines) [^okf-benchmarks].
- [.agents/bundles/product/tech-stack.md](../../../../../product/tech-stack.md) — Current `8`-crate / `4`-companion-wheel product tech stack (`171` lines) [^prod-tech-stack].
- [src/py/vertebra/extensions/sqlspec/adapters.py](../../../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) — `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` implementation [^sqlspec-adapters].
- [pyproject.toml](../../../../../../../pyproject.toml) — `[project.optional-dependencies].postgres`, `[dependency-groups].bench`, and `[tool.mypy]` overrides [^root-pyproject].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Public client, pool, and consumer compilation contracts.

## Symbol & Signature Contracts

- Both `knowledge/` chapters remain strictly `<= 200` lines (`DOC-08`), and all three OKF files retain multi-line YAML frontmatter (`generated:`, prepended `verified:`, `sources:`, `tags:` without state words), `<!-- truth: start --> ... <!-- truth: end -->` (`<= 40` lines immediately after `# H1`), 1:1 `sources:` <-> inline citation <-> bottom footnote parity, valid relative links, and zero standalone `---` body lines.

## Edge Cases & Error Semantics

- Keep MySQL (`%s` / `POSITIONAL_PYFORMAT`), SQLite (`?` / `QMARK`), Oracle (`:name`), BigQuery (`@name`), and Spanner (`@name`) SQLSpec/SQLAlchemy parameter documentation intact when updating PostgreSQL to `$N` (`ParameterStyle.NUMERIC` / `numeric_dollar`).
- Make clear in `benchmarks-and-free-threaded.md` and `tech-stack.md` that `psycopg` (`3.2.13`) and `asyncpg` (`0.31.0`) exist solely in `[dependency-groups].bench` as benchmark comparators and are never imported by `vertebra` runtime code.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check`, `make docs`, and `<= 200` line checks (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| OKF schema, line-budget, & provenance check | `make review-check` + `wc -l` on `sqlspec-alchemy-and-dialects.md` and `benchmarks-and-free-threaded.md` | Exits `0`; both `knowledge/` files are `<= 200` lines and all three OKF files pass frontmatter, truth-block, `sources:`, footnote parity, and privacy/cliché checks. |
| Tech-stack & extension parity audit | Inspect `sqlspec-alchemy-and-dialects.md`, `benchmarks-and-free-threaded.md`, and `tech-stack.md` + run `make docs` | `9` Rust crates, `5` companion wheels, `6` PyO3 extensions (`_postgres`), unified `$N` / `numeric_dollar` PostgreSQL compilation, and `postgres-bench*` gates match the live workspace; `make docs` exits `0`. |

## Verification

- Line-budget & OKF validator gate: `test "$(wc -l < .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)" -le 200 && test "$(wc -l < .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md)" -le 200 && make review-check`
- Documentation & link gate: `make docs`

## Acceptance Criteria

- [ ] `sqlspec-alchemy-and-dialects.md` (`<= 200` lines), `benchmarks-and-free-threaded.md` (`<= 200` lines), and `product/tech-stack.md` synchronize unified `$N` / `numeric_dollar` compilation, `6` PyO3 extensions (`_postgres`), `[dependency-groups].bench` + `postgres-bench*`, and the `9`-crate / `5`-companion-wheel stack in present tense (R27).
- [ ] `make review-check` and `make docs` exit `0` with strict 1:1 `sources:`-to-footnote parity across all `11` Chapter 30 files (R27).

## Notes & Discoveries

- [2026-11-07 16:45] Completing `ch30:t04` brings all `11/11` invalidated documentation and OKF files to parity and unblocks `ch31:t01` final review and release sign-off.

[^okf-extensions]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — OKF chapter for SQLSpec adapters, Advanced Alchemy, and SQLAlchemy `+vtb` dialects.
[^okf-benchmarks]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — OKF chapter for benchmark governance and free-threaded Python verification.
[^prod-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product tech-stack manifest across Python, Rust crates, and companion wheels.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQL sync and async adapters using `ParameterStyle.NUMERIC`.
[^root-pyproject]: `pyproject.toml` — Workspace optional `[postgres]` extra, `[dependency-groups].bench`, and Mypy overrides.
