---
type: Task
id: "native-cursor-data-path:cleanup:t02"
chapter_id: cleanup-and-checks
title: "Benchmark comparison check and allow-list"
description: "Add a check subcommand to the adapter benchmark CLI that fails when a Vertebra case has a higher median than its comparator and is not on the allow-list."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "tools/benchmarks/adapters/sql.py:1-93"
  - "tools/benchmarks/adapters/sql.py:1310-1414"
  - "tools/benchmarks/adapters/base.py:51-76"
  - "tools/benchmarks/adapters/base.py:395-416"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py:1-60"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py:219-257"
files:
  - tools/benchmarks/adapters/sql.py
tests:
  - src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
estimated_loc: 240
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: bench-sql
    resource: tools/benchmarks/adapters/sql.py::main
    title: "Adapter benchmark CLI with capture and compare commands"
  - id: bench-base
    resource: tools/benchmarks/adapters/base.py::AdapterBenchmarkCase
    title: "Adapter benchmark case schema"
tags: [benchmarks, tooling, feature]
---

# t02 — Benchmark comparison check and allow-list

## Objective & Scope Boundary

One command tells whether every Vertebra case in a captured report is at least as fast as its comparator. The existing `compare` command only matches the same driver label across two reports.[^bench-sql]

In scope: `tools/benchmarks/adapters/sql.py` and its test file. Out of scope: `base.py`, `cache.py`, `storage.py`, the capture functions, and `Makefile` targets.

## Bounded Read Manifest

- `tools/benchmarks/adapters/sql.py:1-93` for imports, constants, and `__all__`.
- `tools/benchmarks/adapters/sql.py:1310-1414` for `_build_parser` and `main`.
- `tools/benchmarks/adapters/base.py:51-76` for the report and case fields.[^bench-base]
- `tools/benchmarks/adapters/base.py:395-416` for report validation.
- The test file's imports and its CLI test, as listed in `read_set`.

## Contracts & Edge Cases

Add to `tools/benchmarks/adapters/sql.py` and export each public name in `__all__`:

```python
type ComparatorStatus = Literal["pass", "fail", "allowed", "missing"]

COMPARATOR_PAIRS: Final[dict[str, tuple[str, ...]]]
COMPARATOR_ALLOW_LIST: Final[dict[str, str]]


class ComparatorCheckOutcome(msgspec.Struct, frozen=True, gc=False):
    engine: str
    case: str
    vertebra_driver: str
    comparator_driver: str | None
    vertebra_median_ms: float
    comparator_median_ms: float | None
    median_ratio: float | None
    status: ComparatorStatus
    reason: str | None = None


def check_report_against_comparators(
    report: AdapterBenchmarkReport,
    *,
    max_median_ratio: float = 1.0,
) -> tuple[ComparatorCheckOutcome, ...]: ...
```

`COMPARATOR_PAIRS` holds exactly these entries:

| Vertebra driver | Comparators |
| --- | --- |
| `vertebra-sqlite-sync` | `sqlite3` |
| `vertebra-sqlite-async` | `aiosqlite` |
| `vertebra-duckdb-sync` | `duckdb` |
| `vertebra-mysql-sync`, `vertebra-mariadb-sync` | `pymysql`, `MySQLdb` |
| `vertebra-mysql-async`, `vertebra-mariadb-async` | `aiomysql` |
| `vertebra-oracle-sync` | `oracledb-sync` |
| `vertebra-oracle-async` | `oracledb-async` |
| `vertebra-mssql-sync` | `pymssql`, `pyodbc` |
| `vertebra-spanner-sync` | `google-cloud-spanner-sync` |
| `vertebra-bigquery-sync` | `google-cloud-bigquery-sync` |
| `sqlalchemy-vtb-sync` | `sqlalchemy-vanilla-sync` |
| `vertebra-sqlspec-sqlite-sync` | `sqlspec-sqlite-sync` |
| `vertebra-sqlspec-duckdb-sync` | `sqlspec-duckdb-sync` |
| `vertebra-sqlspec-postgres-async` | `sqlspec-asyncpg` |
| `vertebra-sync` | `psycopg-sync` |
| `vertebra-async` | `asyncpg` |

`COMPARATOR_ALLOW_LIST` holds exactly these entries:

| Vertebra driver | Reason |
| --- | --- |
| `vertebra-bigquery-sync` | `"Emulator round trip dominates and drifts with job history."` |
| `vertebra-spanner-sync` | `"Emulator round trip dominates the measurement."` |
| `vertebra-mssql-sync` | `"Nearly all time is inside the engine round trip; results sit within run-to-run variation."` |
| `vertebra-duckdb-sync` | `"Bundled libduckdb-sys cc build omits DuckDB jemalloc until upstream ships it on crates.io."` |
| `vertebra-sqlspec-duckdb-sync` | `"Bundled libduckdb-sys cc build omits DuckDB jemalloc until upstream ships it on crates.io."` |

No other drivers are allow-listed; any additional entry needs the user's approval.

`check_report_against_comparators` rules:

1. Raise `ValueError` when `max_median_ratio` is not finite or is `<= 0`.
2. Walk `report.cases` in order. Skip a case whose `driver` is not a key of `COMPARATOR_PAIRS`.
3. For each comparator label of that driver, look up the case with the same `case` name and that label.
4. When at least one comparator case exists, emit one outcome per existing comparator. `median_ratio = vertebra.median_ms / comparator.median_ms`.
   - `median_ratio <= max_median_ratio` gives `status="pass"`, `reason=None`.
   - Otherwise, a driver in `COMPARATOR_ALLOW_LIST` gives `status="allowed"` with the allow-list reason.
   - Otherwise `status="fail"` with `reason=f"median_ratio {ratio:.3f} > {max_median_ratio:.3f}"`.
5. When no comparator case exists, emit one outcome with `comparator_driver=None`, `comparator_median_ms=None`, `median_ratio=None`, `status="missing"`, and `reason="no comparator case in report"`.
6. `engine` on every outcome is `report.engine`.

CLI: add a `check` subcommand in `_build_parser`.

| Argument | Meaning |
| --- | --- |
| `--report PATH` | Required, repeatable (`action="append"`, `type=Path`). Each path is a captured report. |
| `--max-median-ratio FLOAT` | Default `1.0`. |
| `--allow-missing` | `store_true`. A `missing` outcome no longer fails the command. |

In `main`, branch on `args.command == "check"` before the `compare` code. Parse each report with `parse_adapter_report_bytes`, collect outcomes, and write one line per outcome to stdout:

```text
[PASS] sqlite point_select_1 (vertebra-sqlite-sync vs sqlite3): median_ratio=0.769
[ALLOW] mssql point_select_1 (vertebra-mssql-sync vs pymssql): median_ratio=1.042 (Nearly all time is inside the engine round trip; results sit within run-to-run variation.)
[FAIL] oracle point_select_1 (vertebra-oracle-sync vs oracledb-sync): median_ratio=1.203 (median_ratio 1.203 > 1.000)
[MISSING] sqlite point_select_1 (vertebra-sqlite-sync): no comparator case in report
```

Exit codes: `1` when any outcome is `fail`, or any is `missing` and `--allow-missing` is absent. `2` when the reports yield no outcome at all, with `"No Vertebra case with a known comparator found\n"` on stderr. `0` otherwise.

Edge cases:

- An equal median passes.
- Vertebra drivers without a pair entry, such as `vertebra-duckdb-async`, produce no outcome.
- A report from `capture --no-external` yields only `missing` outcomes.
- The existing `compare` behavior and its exit codes do not change. Update the `main` docstring to name `check`.

## Test Matrix

Add to `src/py/tests/unit/tools/test_adapter_benchmarks_sql.py`. Build reports in memory with a small helper `_report(engine, cases)` that fills `AdapterBenchmarkCase` with `category="sql"`, `iterations=10`, `p99_ms=median_ms`, and `ops_per_sec=1000.0 / median_ms`.

| Test | Report | Assert |
| --- | --- | --- |
| `test_comparator_check_passes_when_vertebra_is_not_slower` | sqlite: `vertebra-sqlite-sync` 0.0010, `sqlite3` 0.0013; a second case with equal medians | Two outcomes, both `pass`; ratios `pytest.approx(0.769, abs=1e-3)` and `1.0` |
| `test_comparator_check_fails_slower_case_outside_allow_list` | oracle: `vertebra-oracle-sync` 0.089, `oracledb-sync` 0.074 | One outcome, `fail`, reason contains `"> 1.000"` |
| `test_comparator_check_allows_listed_driver_with_reason` | mssql: `vertebra-mssql-sync` 0.500, `pymssql` 0.480 | `allowed`; reason equals `COMPARATOR_ALLOW_LIST["vertebra-mssql-sync"]` |
| `test_comparator_check_allows_slower_duckdb_case` | duckdb: `vertebra-duckdb-sync` 0.426, `duckdb` 0.245 | One outcome, `allowed`, reason equals `COMPARATOR_ALLOW_LIST["vertebra-duckdb-sync"]` |
| `test_comparator_check_reports_each_present_comparator` | mysql: `vertebra-mysql-sync`, `pymysql` only | One outcome against `pymysql`; no `missing` outcome |
| `test_comparator_check_marks_missing_comparator` | sqlite: `vertebra-sqlite-sync` only, plus `vertebra-duckdb-async` | One `missing` outcome; nothing for the async DuckDB driver |
| `test_comparator_check_rejects_invalid_ratio` | parametrize `0.0`, `-1.0`, `float("nan")`, `float("inf")` | `ValueError` |
| `test_allow_list_entries_are_paired_and_explained` | none | Every allow-list key is a key of `COMPARATOR_PAIRS`; every reason is a non-empty single line |
| `test_sql_cli_check_exit_codes` | Write pass, fail, missing, and empty reports to `tmp_path` with `format_adapter_report_bytes` | `main(["check", "--report", pass])` is `0`; with the fail report added it is `1`; missing alone is `1`; missing with `--allow-missing` is `0`; a report with only unpaired drivers is `2`; `capsys` output has a `[FAIL]` line for the fail case |

Write the first test before the implementation and confirm it fails on import.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_adapter_benchmarks_sql.py -q
uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlite --dsn :memory: --iterations 50 --warmup 5 --output "$(mktemp -d)/sqlite.json"
uv run --no-sync ruff check tools/benchmarks/adapters/sql.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
uv run --no-sync mypy tools/benchmarks/adapters/sql.py
uv run --no-sync pyright tools/benchmarks/adapters/sql.py
```

## Acceptance Criteria

- [ ] The nine tests pass and the existing tests in the file still pass.
- [ ] `COMPARATOR_PAIRS` and `COMPARATOR_ALLOW_LIST` match the two tables above entry for entry. The allow-list has three entries and no DuckDB entry.
- [ ] `--max-median-ratio` defaults to `1.0`.
- [ ] `check` exits `0`, `1`, and `2` as specified.
- [ ] `ruff`, `mypy`, and `pyright` report nothing for the touched files.

[^bench-sql]: `tools/benchmarks/adapters/sql.py::main`
[^bench-base]: `tools/benchmarks/adapters/base.py::AdapterBenchmarkCase`
