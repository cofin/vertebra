---
type: Task
id: "native-postgres-driver:ch31:t03"
chapter_id: ch31-data-and-testing-okf-sync
title: Synchronize SQLSpec/SQLAlchemy dialects, benchmarks/free-threaded, and polyglot test matrix OKF chapters
description: Update sqlspec-alchemy-and-dialects.md, benchmarks-and-free-threaded.md, and trimmed polyglot-test-matrix.md (243L -> <= 200L) for unified $n/numeric_dollar compilation, _postgres in the gil_used = false extension enumeration, [dependency-groups].postgres-bench, and zero-skip make postgres-test gates.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch31:t02"
read_set:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/tests/unit/test_freethreaded_gil.py
files:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
tests: []
estimated_loc: 215
plan_revision: 8
plan_commit: d90d4625
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
updated_at: "2026-11-12T04:25:15Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: okf-dialects
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: SQLSpec, Advanced Alchemy, and +vtb SQLAlchemy dialects chapter
  - id: okf-bench-ft
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: Benchmarks and free-threaded 3.14t/3.15t verification chapter
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Polyglot Rust, Python, TypeScript, and database conformance test matrix
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver
    title: SQLSpec sync and async Vertebra driver adapters
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect
    title: SQLAlchemy +vtb PostgreSQL sync and async dialects
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: Free-threaded GIL state and FREE_THREADED flag verification suite
tags:
  - postgres
  - okf
  - sqlspec
  - benchmarks
  - testing
---

# Task t03: Synchronize SQLSpec/SQLAlchemy Dialects, Benchmarks/Free-Threaded, and Polyglot Test Matrix OKF Chapters

## Objective & Scope Boundary

Update `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`168L` at `c8722c76` -> `<= 200L`), `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`151L` -> `<= 200L`), and `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`243L` -> `<= 200L`) in present tense [^okf-dialects] [^okf-bench-ft] [^okf-test-matrix] [^sqlspec-adapters] [^sa-dialects] [^ft-gil-test]. Sibling driver flows edit the same enumerations, so extension lists are reconciled against the `Makefile` `lint` stubtest list and crate lists against `src/rs/*/Cargo.toml` at execution time, and no chapter states a fixed count:
1. `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`<= 200` lines):
   - Replace sync `pyformat` / `psycopg` and async `asyncpg` prose with unified `ParameterStyle.NUMERIC` (`$n`, copying caller `StatementConfig` so caller config identity is never mutated), `$n` PostgreSQL RLS tenant predicate binds across sync and async drivers, Liskov-compliant `select_to_arrow` / `ArrowResult` forwarding, and `default_paramstyle = paramstyle = "numeric_dollar"` across both `SyncPostgreSQLDialect` (`postgresql+vtb://`) and `AsyncPostgreSQLDialect` (`postgresql+vtb_async://`) [^okf-dialects] [^sqlspec-adapters] [^sa-dialects].
2. `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`<= 200` lines):
   - Add `_postgres` to the `#[pymodule(gil_used = false)]` extension enumeration (replacing any count word with the plain list from the `Makefile` `lint` stubtest list), document isolated subprocess `3.14t`/`3.15t` disabled-GIL probes alongside standard-GIL `FREE_THREADED is _is_free_threaded_runtime()` parity in `test_freethreaded_gil.py`, document `[dependency-groups].postgres-bench` (`psycopg[binary]==3.2.13` and `asyncpg==0.31.0` gated behind `python_version == '3.12'` environment markers strictly as reference comparators, separate from `[dependency-groups].bench`), and document `tools/benchmarks/postgres.py` (`schema_version=1`), `src/rs/vertebra-postgres/benches/postgres_native.rs`, and `make postgres-bench` / `make postgres-bench-check` [^okf-bench-ft] [^ft-gil-test].
3. `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`243` lines — over budget -> `<= 200` lines):
   - Trim repetitive test-directory enumerations, add `vertebra-postgres` / `_postgres` to the crate and extension enumerations (`mypy.stubtest` over the `Makefile` `lint` stubtest list in `make py-test` and `make lint-all`), and add `make postgres-wheel-check` (`tools/scripts/verify_postgres_wheel.py` outside-checkout wheel/sdist isolation) and `make postgres-test` (`VERTEBRA_CONFORMANCE_DOCKER=1` plus `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` pytest zero-skip hook across live PostgreSQL `14`–`18`, `wal_level=logical` `pgoutput` v1, multi-arch digest-pinned `pgvector` `0.8.1`, and `PgBouncer` `1.26.0` SCRAM `rolpassword`) [^okf-test-matrix].
   - Remove the optional psycopg converter acceptance paragraph and its `src-psycopg-bind-regressions` source/footnote when `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` no longer contains `test_postgres_sync_primitives_bind_with_real_psycopg_converter` after `ch27`; if the test still exists, keep the paragraph and record the remaining dependency under `## Notes & Discoveries`.

Out of scope: Modifying Python/Rust tests, `database-drivers-and-dialers.md` / `postgres-*.md` (`ch31:t01`), `query-pipeline-cdc-and-tenancy.md` (`ch31:t02`), or `product/` and queue OKF chapters (`ch31:t04`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md](../../../../../../../.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md) — Current `168`-line SQLSpec and SQLAlchemy dialects chapter [^okf-dialects].
- [.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md](../../../../../../../.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md) — Current `151`-line benchmarks and free-threaded chapter [^okf-bench-ft].
- [.agents/bundles/knowledge/testing/polyglot-test-matrix.md](../../../../../../../.agents/bundles/knowledge/testing/polyglot-test-matrix.md) — Current `243`-line polyglot test matrix to bring `<= 200` lines [^okf-test-matrix].
- [src/py/vertebra/extensions/sqlspec/adapters.py](../../../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) — Final SQLSpec PostgreSQL `ParameterStyle.NUMERIC` and Arrow/stack seams [^sqlspec-adapters].
- [src/py/vertebra/extensions/sqlalchemy/dialects.py](../../../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) — Final `default_paramstyle = paramstyle = "numeric_dollar"` dialect attributes [^sa-dialects].
- [src/py/tests/unit/test_freethreaded_gil.py](../../../../../../../src/py/tests/unit/test_freethreaded_gil.py) — `FREE_THREADED is _is_free_threaded_runtime()` and `3.14t`/`3.15t` subprocess checks [^ft-gil-test].

## Symbol & Signature Contracts

- All three chapters pass `make review-check` with `<= 200` lines each, `<= 40`-line truth blocks, multi-line YAML lists, updated `verified[0].commit` ancestor SHAs, zero standalone `---` body rules, and 1:1 `sources:`-to-footnote parity.
- Do not mention external SQL builder/ORM query libraries in any of the three chapters; mention `psycopg[binary]==3.2.13` and `asyncpg==0.31.0` solely in `benchmarks-and-free-threaded.md` as pinned `[dependency-groups].postgres-bench` (`python_version == '3.12'`) harness inputs (plus the converter paragraph in `polyglot-test-matrix.md` only while its test exists on disk).

## Edge Cases & Error Semantics

- `polyglot-test-matrix.md` starts `43` lines above the `200`-line limit: consolidate long file-by-file tables into compact suite-category rows before inserting `postgres-test` and `postgres-wheel-check` coverage so the total settles `<= 195`.
- `sqlspec-alchemy-and-dialects.md` names both `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` on SQLAlchemy `+vtb` PostgreSQL dialects because SQLAlchemy reads `default_paramstyle` when constructing its SQL compiler while DBAPI callers inspect `paramstyle`.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line budgets (including `243L` trim) | `wc -l .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md` | Every file is `<= 200` lines with truth blocks `<= 40` lines. |
| OKF v0.2 provenance & privacy check | `make review-check` | Exits `0`; all `sources:` paths resolve on disk, 1:1 inline/footer footnotes match, and zero forbidden terms or standalone `---` rules exist. |
| Stale count & driver audit | `git grep -n -i -E "(five|six|eight|nine) (pyo3 |rust |compiled )?(extensions|crates)" -- .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md` and `git grep -n -i -E "psycopg|asyncpg" -- .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` | Both commands print nothing. |
| Contract accuracy spot-check | Inspect all three files | Documents `ParameterStyle.NUMERIC`, `default_paramstyle = paramstyle = "numeric_dollar"`, `_postgres` in an extension enumeration matching the `Makefile` `lint` stubtest list, `[dependency-groups].postgres-bench` (`python_version == '3.12'`), `FREE_THREADED is _is_free_threaded_runtime()`, and `VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md`
- Converter test presence: `git grep -n "test_postgres_sync_primitives_bind_with_real_psycopg_converter" -- src/py/tests`
- OKF & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `sqlspec-alchemy-and-dialects.md`, `benchmarks-and-free-threaded.md`, and `polyglot-test-matrix.md` sit `<= 200` lines each and reflect unified `$n`/`numeric_dollar` compilation, `_postgres` in extension enumerations matching the `Makefile`, `[dependency-groups].postgres-bench`, and zero-skip `make postgres-test` / `make postgres-wheel-check` gates (R24, R25, R26, R27).
- [ ] `make review-check` exits `0` (R27).

## Notes & Discoveries

- Cite `tools/benchmarks/postgres.py` and `tools/scripts/verify_postgres_wheel.py` in `sources:` of `benchmarks-and-free-threaded.md` and `polyglot-test-matrix.md` once `ch29` has committed both scripts on disk.

[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec, Advanced Alchemy, and `+vtb` SQLAlchemy dialects chapter.
[^okf-bench-ft]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Benchmarks and free-threaded `3.14t`/`3.15t` verification chapter.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot Rust, Python, TypeScript, and database conformance test matrix.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec sync and async Vertebra driver adapters.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy `+vtb` PostgreSQL sync and async dialects.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded GIL state and `FREE_THREADED` flag verification suite.
