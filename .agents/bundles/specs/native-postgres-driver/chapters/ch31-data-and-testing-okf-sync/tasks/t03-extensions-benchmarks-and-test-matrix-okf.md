---
type: Task
id: "native-postgres-driver:ch31:t03"
chapter_id: ch31-data-and-testing-okf-sync
title: Synchronize SQLSpec/SQLAlchemy dialects, benchmarks/free-threaded, and polyglot test matrix OKF chapters
description: Update sqlspec-alchemy-and-dialects.md, benchmarks-and-free-threaded.md, and trimmed polyglot-test-matrix.md (245L -> <= 200L) for unified $n/numeric_dollar compilation, 6 gil_used = false extensions, [dependency-groups].postgres-bench, and zero-skip make postgres-test gates.
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

Update `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`168L -> <= 200L`), `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`152L -> <= 200L`), and `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`245L -> <= 200L`) in strict present tense [^okf-dialects] [^okf-bench-ft] [^okf-test-matrix] [^sqlspec-adapters] [^sa-dialects] [^ft-gil-test]:
1. `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`<= 200` lines):
   - Replace pre-cutover sync `pyformat` / `psycopg` and async `asyncpg` prose with unified `ParameterStyle.NUMERIC` (`$n`, copying caller `StatementConfig` cleanly so caller config identity is never mutated), `$n` PostgreSQL RLS tenant predicate binds across sync and async drivers, Liskov-compliant `select_to_arrow` / `ArrowResult` forwarding, and `default_paramstyle = paramstyle = "numeric_dollar"` across both `SyncPostgreSQLDialect` (`postgresql+vtb://`) and `AsyncPostgreSQLDialect` (`postgresql+vtb_async://`) [^okf-dialects] [^sqlspec-adapters] [^sa-dialects].
2. `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`<= 200` lines):
   - Update from five to all six `#[pymodule(gil_used = false)]` extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres`), document isolated subprocess `3.14t`/`3.15t` disabled-GIL probes alongside standard-GIL `FREE_THREADED is _is_free_threaded_runtime()` parity in `test_freethreaded_gil.py`, document `[dependency-groups].postgres-bench` (`psycopg[binary]==3.2.13` and `asyncpg==0.31.0` gated behind `python_version == '3.12'` environment markers strictly as reference comparators, separate from `[dependency-groups].bench`), and document `tools/benchmarks/postgres.py` (`schema_version=1`), `src/rs/vertebra-postgres/benches/postgres_native.rs`, and `make postgres-bench` / `make postgres-bench-check` [^okf-bench-ft] [^ft-gil-test].
3. `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`245` lines baseline — currently over budget -> trim and rewrite strictly `<= 200` lines):
   - Trim repetitive test-directory enumerations and replace stale `8 crates`, `5 extensions`, and `psycopg`/`asyncpg` runtime driver wording with `9` Rust workspace crates (`vertebra-postgres`), `6` `gil_used = false` extensions (`mypy.stubtest` over all six in `make py-test` and `make lint-all`), `make postgres-wheel-check` (`tools/scripts/verify_postgres_wheel.py` outside-checkout wheel/sdist isolation), and `make postgres-test` (`VERTEBRA_CONFORMANCE_DOCKER=1` plus `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` pytest zero-skip hook across live PostgreSQL `14`–`18`, `wal_level=logical` `pgoutput` v1, multi-arch digest-pinned `pgvector` `0.8.1`, and `PgBouncer` `1.26.0` SCRAM `rolpassword`) [^okf-test-matrix].

Out of scope: Modifying Python/Rust tests, `database-drivers-and-dialers.md` / `postgres-*.md` (`ch31:t01`), `query-pipeline-cdc-and-tenancy.md` (`ch31:t02`), or `product/` and queue OKF chapters (`ch31:t04`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md](../../../../../../../.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md) — Current `168`-line SQLSpec and SQLAlchemy dialects chapter [^okf-dialects].
- [.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md](../../../../../../../.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md) — Current `152`-line benchmarks and free-threaded chapter [^okf-bench-ft].
- [.agents/bundles/knowledge/testing/polyglot-test-matrix.md](../../../../../../../.agents/bundles/knowledge/testing/polyglot-test-matrix.md) — Current `245`-line polyglot test matrix to bring `<= 200` lines [^okf-test-matrix].
- [src/py/vertebra/extensions/sqlspec/adapters.py](../../../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) — Final SQLSpec PostgreSQL `ParameterStyle.NUMERIC` and Arrow/stack seams [^sqlspec-adapters].
- [src/py/vertebra/extensions/sqlalchemy/dialects.py](../../../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) — Final `default_paramstyle = paramstyle = "numeric_dollar"` dialect attributes [^sa-dialects].
- [src/py/tests/unit/test_freethreaded_gil.py](../../../../../../../src/py/tests/unit/test_freethreaded_gil.py) — Six-extension `FREE_THREADED is _is_free_threaded_runtime()` and `3.14t`/`3.15t` subprocess checks [^ft-gil-test].

## Symbol & Signature Contracts

- All three chapters pass `make review-check` with strictly `<= 200` lines each, `<= 40`-line truth blocks, multi-line YAML lists, updated `verified[0].commit` ancestor SHAs, zero standalone `---` body rules, and 1:1 `sources:`-to-footnote parity.
- Do not mention external competitor SQL builder/ORM query libraries in any of the three chapters; mention `psycopg[binary]==3.2.13` and `asyncpg==0.31.0` solely in `benchmarks-and-free-threaded.md` as pinned `[dependency-groups].postgres-bench` (`python_version == '3.12'`) harness inputs.

## Edge Cases & Error Semantics

- `polyglot-test-matrix.md` starts `45` lines above the `200`-line `DOC-08` limit: consolidate long file-by-file tables into compact suite-category rows before inserting `postgres-test` and `postgres-wheel-check` coverage so total line count settles comfortably `<= 195`.
- Ensure `sqlspec-alchemy-and-dialects.md` names both `default_paramstyle = "numeric_dollar"` AND `paramstyle = "numeric_dollar"` on SQLAlchemy `+vtb` PostgreSQL dialects because SQLAlchemy reads `default_paramstyle` when constructing its SQL compiler while DBAPI callers inspect `paramstyle`.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `DOC-08` line budgets (including `245L` trim) | `wc -l .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md .agents/bundles/knowledge/testing/{benchmarks-and-free-threaded,polyglot-test-matrix}.md` | Every file is strictly `<= 200` lines with truth blocks `<= 40` lines. |
| OKF v0.2 provenance & privacy check | `make review-check` | Exits `0`; all `sources:` paths resolve on disk, 1:1 inline/footer footnotes match, and zero forbidden terms or standalone `---` rules exist. |
| Contract accuracy spot-check | Inspect all three files | Documents `ParameterStyle.NUMERIC`, `default_paramstyle = paramstyle = "numeric_dollar"`, `6` extensions, `9` crates, `[dependency-groups].postgres-bench` (`python_version == '3.12'`), `FREE_THREADED is _is_free_threaded_runtime()`, and `VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md`
- OKF & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `sqlspec-alchemy-and-dialects.md`, `benchmarks-and-free-threaded.md`, and `polyglot-test-matrix.md` sit strictly `<= 200` lines each and accurately reflect unified `$n`/`numeric_dollar` compilation, six `gil_used = false` extensions, `[dependency-groups].postgres-bench`, and zero-skip `make postgres-test` / `make postgres-wheel-check` gates (R24, R25, R26, R27).
- [ ] `make review-check` exits `0` cleanly (R27).

## Notes & Discoveries

- When updating `benchmarks-and-free-threaded.md` and `polyglot-test-matrix.md`, feel free to cite `tools/benchmarks/postgres.py` and `tools/scripts/verify_postgres_wheel.py` in `sources:` once `ch29` has committed both scripts on disk.

[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec, Advanced Alchemy, and `+vtb` SQLAlchemy dialects chapter.
[^okf-bench-ft]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Benchmarks and free-threaded `3.14t`/`3.15t` verification chapter.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot Rust, Python, TypeScript, and database conformance test matrix.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec sync and async Vertebra driver adapters.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy `+vtb` PostgreSQL sync and async dialects.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded GIL state and `FREE_THREADED` flag verification suite.
