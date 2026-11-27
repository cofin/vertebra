---
type: Task
id: "runtime-perf-remediation:ch04:t04"
chapter_id: ch04-data-benchmarks-and-verification
title: Add standalone Advanced Alchemy benchmark comparator
description: Add capture_advanced_alchemy_report, register four SQLite/PostgreSQL COMPARATOR_PAIRS entries, and wire --engine advanced_alchemy --aa-engine sqlite|postgres CLI flags in tools/benchmarks/adapters/sql.py.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
  - tools/benchmarks/adapters/sql.py
  - tools/benchmarks/adapters/base.py
files:
  - tools/benchmarks/adapters/sql.py
tests:
  - src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
  - src/py/tests/unit/test_bench_infrastructure.py
estimated_loc: 190
plan_revision: 3
plan_commit: "9dfbd408"
commit: null
sources:
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 specification and comparator pairs contract
  - id: src-bench-sql
    resource: tools/benchmarks/adapters/sql.py::capture_sqlspec_report
    title: Standalone SQL, SQLAlchemy, and SQLSpec adapter comparator suite
tags:
  - benchmarks
  - advanced-alchemy
  - sqlalchemy
  - comparators
---

# Add Standalone Advanced Alchemy Benchmark Comparator

## Objective & Scope Boundary

Add `capture_advanced_alchemy_report` to `tools/benchmarks/adapters/sql.py` comparing Vertebra `+vtb`/`+vtb_async` Advanced Alchemy repository services against vanilla SQLAlchemy/Advanced Alchemy baselines on SQLite and PostgreSQL [^src-ch04-spec].

Register all four comparator pairs in `COMPARATOR_PAIRS` and wire `--engine advanced_alchemy --aa-engine sqlite|postgres` into `_build_parser` and `_dispatch_capture` [^src-bench-sql].

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md`
- `tools/benchmarks/adapters/sql.py`
- `tools/benchmarks/adapters/base.py`

## Contracts & Edge Cases

- Add `type AdvancedAlchemyEngine = Literal["sqlite", "postgres"]`, `_ADVANCED_ALCHEMY_ENGINES`, and `"advanced_alchemy"` in `SUPPORTED_SQL_ENGINES` and `__all__`.
- Implement `capture_advanced_alchemy_report` per chapter `spec.md` Section 2.2 [^src-ch04-spec], returning `AdapterBenchmarkReport(schema_version=ADAPTER_SCHEMA_VERSION, category="orm", engine=f"advanced_alchemy:{engine}", ...)`.
- Register exact `COMPARATOR_PAIRS` entries:
  - `"vertebra-aa-sqlite-sync": ("vanilla-aa-sqlite-sync",)`
  - `"vertebra-aa-sqlite-async": ("vanilla-aa-sqlite-async",)`
  - `"vertebra-aa-postgres-sync": ("vanilla-aa-postgres-sync",)`
  - `"vertebra-aa-postgres-async": ("vanilla-aa-postgres-async",)`
- Benchmark cases per driver:
  - `aa_point_select_1`: fetch single row by primary key (`id=1`) through `SQLAlchemySyncRepositoryService` / `SQLAlchemyAsyncRepositoryService`.
  - `aa_multi_row_select_10`: fetch 10 rows via repository `list()` with limit 10.
- Seed a lightweight 10-row `aa_bench_world` table idempotently before sampling so `:memory:` SQLite and fresh Postgres databases run without external setup. Keep default SQLAlchemy dialect pools (`StaticPool` / `SingletonThreadPool`) on SQLite `:memory:` rather than forcing `NullPool` so the seeded table persists across sessions.
- Rewrite input `dsn` cleanly:
  - SQLite: `sqlite+vtb:///` & `sqlite+vtb_async:///` for Vertebra; `sqlite:///` & `sqlite+aiosqlite:///` for vanilla (skipping vanilla async gracefully when `aiosqlite` is absent).
  - Postgres: `postgresql+vtb://` & `postgresql+vtb_async://` for Vertebra; `postgresql+psycopg://` & `postgresql+asyncpg://` for vanilla (skipping absent external drivers gracefully when `include_external=True`).
- Raise `ValueError` for unsupported `engine` values and `RuntimeError` if `sqlalchemy` or `advanced_alchemy` is not installed.
- Dispose all sync and async engines in `finally` blocks and check `assert_free_threaded_gil_disabled(strict=strict_free_threading)` around external comparator imports.

## Test Matrix

| Case | Setup | Expected Behavior |
| :--- | :--- | :--- |
| Offline SQLite capture | `capture_advanced_alchemy_report("sqlite", ":memory:", iterations=3, warmup=1)` | Emits `vertebra-aa-sqlite-sync`, `vertebra-aa-sqlite-async`, and vanilla cases |
| Comparator check | `check_report_against_comparators(report)` | Matches `vertebra-aa-sqlite-sync` against `vanilla-aa-sqlite-sync` without missing-pair errors |
| Invalid engine | `capture_advanced_alchemy_report("duckdb", ":memory:")` | Raises `ValueError` |
| CLI capture | `main(["capture", "--engine", "advanced_alchemy", "--aa-engine", "sqlite", "--dsn", ":memory:", "--iterations", "2", "--warmup", "1"])` | Exits `0` and emits valid `AdapterBenchmarkReport` JSON |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_adapter_benchmarks_sql.py src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/test_bench_infrastructure.py
uv run --no-sync ruff check tools/benchmarks/adapters/sql.py
uv run --no-sync mypy tools/benchmarks/adapters/sql.py
```

## Acceptance Criteria

- `capture_advanced_alchemy_report` runs offline on SQLite and supports PostgreSQL with `+vtb`/`+vtb_async` and vanilla comparators.
- All four `COMPARATOR_PAIRS` entries resolve cleanly in `check_report_against_comparators`.
- CLI `--engine advanced_alchemy --aa-engine sqlite|postgres` works and passes Ruff + Mypy.

[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 specification and comparator pairs contract.
[^src-bench-sql]: `tools/benchmarks/adapters/sql.py::capture_sqlspec_report` — Standalone SQL, SQLAlchemy, and SQLSpec adapter comparator suite.
