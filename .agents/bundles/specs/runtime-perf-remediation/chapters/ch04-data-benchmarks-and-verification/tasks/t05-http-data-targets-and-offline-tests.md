---
type: Task
id: "runtime-perf-remediation:ch04:t05"
chapter_id: ch04-data-benchmarks-and-verification
title: Add HTTP data targets and offline benchmark tests
description: Register SQLSpec, Advanced Alchemy, and stdlib HTTP data targets with Target.adapter tags in tools/benchmarks/targets.py and add comprehensive offline unit tests in test_e2e_benchmarks.py.
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch04:t02"
  - "runtime-perf-remediation:ch04:t03"
  - "runtime-perf-remediation:ch04:t04"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
  - tools/benchmarks/targets.py
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
  - src/py/tests/unit/test_bench_infrastructure.py
files:
  - tools/benchmarks/targets.py
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
tests:
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
  - src/py/tests/unit/test_bench_infrastructure.py
estimated_loc: 210
plan_revision: 3
plan_commit: "9dfbd408"
commit: "ff8c4272"
sources:
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 specification and target matrix
  - id: src-bench-targets
    resource: tools/benchmarks/targets.py::ALL_TARGETS
    title: Existing HTTP, data, and baseline target definitions
  - id: src-test-e2e-bench
    resource: src/py/tests/unit/tools/test_e2e_benchmarks.py
    title: Existing offline end-to-end benchmark tests
tags:
  - benchmarks
  - targets
  - tests
  - sqlspec
  - advanced-alchemy
---

# Add HTTP Data Targets and Offline Benchmark Tests

## Objective & Scope Boundary

Wire `adapter` and `extra_env` support into target factories in `tools/benchmarks/targets.py`, register the ten SQLSpec, Advanced Alchemy, and stdlib HTTP data targets, and add full offline unit test coverage in `src/py/tests/unit/tools/test_e2e_benchmarks.py` [^src-ch04-spec].

Preserve all existing target names, ports, and metadata invariants verified by `test_bench_infrastructure.py` [^src-bench-targets].

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md`
- `tools/benchmarks/targets.py`
- `src/py/tests/unit/tools/test_e2e_benchmarks.py`
- `src/py/tests/unit/test_bench_infrastructure.py`

## Contracts & Edge Cases

- Update `_vtb`, `_vtb_data`, `_granian_data`, `_uvicorn_data`, and `_shm_broker_env` in `tools/benchmarks/targets.py` to accept `adapter: str = "none"` and optional `extra_env: dict[str, str] | None = None`.
- Guard `apply_shm_broker_dsn` so `shm_broker=true` is only appended to PostgreSQL URLs (`postgres://`, `postgresql://`, `postgresql+...`), leaving SQLite paths untouched.
- Register the ten targets listed in chapter `spec.md` Section 2.3 on unique ports `8160`–`8169` [^src-ch04-spec]:

| Name | Port | App | Topology | Broker | Category | Adapter | Factory | `extra_env` |
| :--- | ---: | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `vtb-data-sqlspec-native-4w` | `8160` | `_DATA_NATIVE` | `SHM` | `False` | `data` | `sqlspec` | — | `VTB_BENCH_DB_ADAPTER=sqlspec`, `VTB_BENCH_DB_ADAPTER_FLAVOR=vertebra` |
| `vtb-data-sqlspec-shm-4w-broker` | `8161` | `_DATA_NATIVE` | `SHM` | `True` | `data` | `sqlspec` | — | `VTB_BENCH_DB_ADAPTER=sqlspec`, `VTB_BENCH_DB_ADAPTER_FLAVOR=vertebra` |
| `vtb-data-aa-native-4w` | `8162` | `_DATA_NATIVE` | `SHM` | `False` | `data` | `advanced_alchemy` | — | `VTB_BENCH_DB_ADAPTER=advanced_alchemy`, `VTB_BENCH_DB_ADAPTER_FLAVOR=vertebra` |
| `vtb-data-aa-shm-4w-broker` | `8163` | `_DATA_NATIVE` | `SHM` | `True` | `data` | `advanced_alchemy` | — | `VTB_BENCH_DB_ADAPTER=advanced_alchemy`, `VTB_BENCH_DB_ADAPTER_FLAVOR=vertebra` |
| `granian-asgi-data-stdlib` | `8164` | `tools.benchmarks.apps.data_asgi:asgi_app` | `BASELINE` | `False` | `data-baseline` | `stdlib` | `False` | `VTB_BENCH_DB_ADAPTER=stdlib`, `VTB_BENCH_DB_ADAPTER_FLAVOR=upstream` |
| `granian-asgi-data-sqlspec` | `8165` | `tools.benchmarks.apps.data_asgi:asgi_app` | `BASELINE` | `False` | `data-baseline` | `sqlspec-upstream` | `False` | `VTB_BENCH_DB_ADAPTER=sqlspec`, `VTB_BENCH_DB_ADAPTER_FLAVOR=upstream` |
| `granian-asgi-data-aa` | `8166` | `tools.benchmarks.apps.data_asgi:asgi_app` | `BASELINE` | `False` | `data-baseline` | `advanced_alchemy-vanilla` | `False` | `VTB_BENCH_DB_ADAPTER=advanced_alchemy`, `VTB_BENCH_DB_ADAPTER_FLAVOR=upstream` |
| `uvicorn-httptools-data-stdlib` | `8167` | `tools.benchmarks.apps.data_asgi:asgi_app` | `BASELINE` | `False` | `data-baseline` | `stdlib` | `False` | `VTB_BENCH_DB_ADAPTER=stdlib`, `VTB_BENCH_DB_ADAPTER_FLAVOR=upstream` |
| `uvicorn-httptools-data-sqlspec` | `8168` | `tools.benchmarks.apps.data_asgi:asgi_app` | `BASELINE` | `False` | `data-baseline` | `sqlspec-upstream` | `False` | `VTB_BENCH_DB_ADAPTER=sqlspec`, `VTB_BENCH_DB_ADAPTER_FLAVOR=upstream` |
| `uvicorn-httptools-data-aa` | `8169` | `tools.benchmarks.apps.data_asgi:asgi_app` | `BASELINE` | `False` | `data-baseline` | `advanced_alchemy-vanilla` | `False` | `VTB_BENCH_DB_ADAPTER=advanced_alchemy`, `VTB_BENCH_DB_ADAPTER_FLAVOR=upstream` |

- Each new target sets `Target.adapter` cleanly for DuckDB matrix filtering and exports `VTB_BENCH_DB_ADAPTER` plus `VTB_BENCH_DB_ADAPTER_FLAVOR`.
- Add offline tests in `test_e2e_benchmarks.py` [^src-test-e2e-bench] covering:
  - `DataBackend` SQLite sync and async operations across all four adapters (`native`, `sqlspec`, `advanced_alchemy`, `stdlib`), plus `DataBackendConfig` validation and Postgres missing-DSN error.
  - `capture_advanced_alchemy_report("sqlite", ...)` roundtrip, `COMPARATOR_PAIRS` check, and CLI `--engine advanced_alchemy --aa-engine sqlite`.
  - Unique ports, `Target.adapter` tags, env propagation, and `apply_shm_broker_dsn` Postgres-only guard across `ALL_TARGETS`.

## Test Matrix

| Case | Setup | Expected Behavior |
| :--- | :--- | :--- |
| Target port uniqueness | Inspect all entries in `ALL_TARGETS` | Zero duplicate ports; new targets appear in `DATA_TARGETS` or `DATA_BASELINE_TARGETS` |
| Adapter matrix tagging | Inspect new target `.adapter` and `.env` | Matches `sqlspec`, `advanced_alchemy`, `stdlib`, `sqlspec-upstream`, `advanced_alchemy-vanilla` |
| SHM broker DSN guard | `apply_shm_broker_dsn` on Postgres URL vs SQLite path | Appends `shm_broker=true` only on Postgres URLs |
| Offline 4-adapter SQLite backend | `tmp_path / "world.db"` across 4 adapters | Sync/async `get_world`, `get_worlds`, `update_worlds`, `get_cached_worlds` pass |
| Offline AA comparator | `capture_advanced_alchemy_report("sqlite", ":memory:", ...)` | Valid schema v1 report passing `check_report_against_comparators` |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/tools/test_matrix_targets_and_scenarios.py src/py/tests/unit/tools/test_cli_matrix_filters.py src/py/tests/unit/test_bench_infrastructure.py
uv run --no-sync ruff check tools/benchmarks/ src/py/tests/unit/tools/test_e2e_benchmarks.py
uv run --no-sync mypy tools/benchmarks/apps/_data_backend.py tools/benchmarks/apps/_db_drivers.py tools/benchmarks/apps/_db_orm_drivers.py tools/benchmarks/adapters/sql.py tools/benchmarks/targets.py
```

## Acceptance Criteria

- All ten new HTTP data targets resolve cleanly with unique ports and DuckDB-ready `Target.adapter` values.
- Offline unit tests exercise every new adapter path, comparator pair, and CLI option in `< 60s`.
- Fast Chapter Gate (`pytest`, `ruff`, `mypy`) passes with zero warnings or regressions.

[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 specification and target matrix.
[^src-bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS` — Existing HTTP, data, and baseline target definitions.
[^src-test-e2e-bench]: `src/py/tests/unit/tools/test_e2e_benchmarks.py` — Existing offline end-to-end benchmark tests.
