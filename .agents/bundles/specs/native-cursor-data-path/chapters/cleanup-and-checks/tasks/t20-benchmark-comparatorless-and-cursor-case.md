---
type: Task
id: "native-cursor-data-path:cleanup:t20"
chapter_id: cleanup-and-checks
title: "Benchmark check: comparator-less cases and the SQLite cursor case"
description: "Report a listed Vertebra case that no comparator runs as [SKIP] without failing the check, keep an unlisted missing comparator a failure, and add a like-for-like SQLite case that reuses one cursor on both sides."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "tools/benchmarks/adapters/sql.py:1-147"
  - "tools/benchmarks/adapters/sql.py::capture_sqlite_report"
  - "tools/benchmarks/adapters/sql.py::check_report_against_comparators"
  - "tools/benchmarks/adapters/sql.py::_format_comparator_outcome"
  - "tools/benchmarks/adapters/sql.py::main"
  - "tools/benchmarks/postgres.py::_measure_async_vertebra_pipeline_batch"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py:1-37"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py::test_capture_sqlite_report_runs_offline_and_preserves_memory_isolation"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py::_report"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py::test_comparator_check_marks_missing_comparator"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py::test_allow_list_entries_are_paired_and_explained"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py::test_sql_cli_check_exit_codes"
files:
  - tools/benchmarks/adapters/sql.py
tests:
  - src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
estimated_loc: 190
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: bench-sql
    resource: tools/benchmarks/adapters/sql.py::check_report_against_comparators
    title: "Comparator check over one captured report"
  - id: bench-sqlite
    resource: tools/benchmarks/adapters/sql.py::capture_sqlite_report
    title: "SQLite capture with Vertebra and sqlite3 cases"
  - id: bench-pg-pipeline
    resource: tools/benchmarks/postgres.py::_measure_async_vertebra_pipeline_batch
    title: "The pipeline_select_16 case, which only the Vertebra async driver runs"
  - id: bench-tests
    resource: src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
    title: "Unit tests for the adapter benchmark CLI"
tags: [benchmarks, tooling, feature]
---

# t20 — Benchmark check: comparator-less cases and the SQLite cursor case

## Objective & Scope Boundary

`check` passes on a PostgreSQL report. Today it prints `[MISSING]` for the `vertebra-async` case `pipeline_select_16` and exits `1`, because no comparator driver runs a pipeline case.[^bench-sql][^bench-pg-pipeline]

The SQLite report also gains a case that compares like with like: one reused native cursor against one reused `sqlite3` cursor.[^bench-sqlite]

In scope: `tools/benchmarks/adapters/sql.py` and its test file. Out of scope: `base.py`, `tools/benchmarks/postgres.py`, the other capture functions, `COMPARATOR_PAIRS`, and `COMPARATOR_ALLOW_LIST`. No allow-list entry is added, removed, or reworded.

## Bounded Read Manifest

- `tools/benchmarks/adapters/sql.py:1-147` for imports, the type aliases, the pair table, the allow-list, `ComparatorCheckOutcome`, and `__all__`.
- `capture_sqlite_report`, `check_report_against_comparators`, `_format_comparator_outcome`, and the `check` branch of `main`.
- `_measure_async_vertebra_pipeline_batch` for the case and driver labels.
- The test file's imports, the SQLite capture test, the `_report` helper, and the three check tests named in `read_set`.[^bench-tests]

## Contracts & Edge Cases

### Comparator-less cases

Add to `tools/benchmarks/adapters/sql.py`:

```python
type ComparatorStatus = Literal["pass", "fail", "allowed", "missing", "skipped"]

COMPARATORLESS_CASES: Final[dict[tuple[str, str], str]] = {
    ("vertebra-async", "pipeline_select_16"): "No comparator driver runs a pipelined statement case.",
}
```

- The key is `(driver, case)`. The value is a one-line reason.
- Place the constant after `COMPARATOR_ALLOW_LIST`. Add `"COMPARATORLESS_CASES"` to `__all__` in sorted position.
- `ComparatorCheckOutcome` keeps its fields.

`check_report_against_comparators(report, *, max_median_ratio=1.0)` keeps its signature. Only the branch for a case with no comparator in the report changes:

| `(item.driver, item.case)` | `status` | `reason` |
| --- | --- | --- |
| A key of `COMPARATORLESS_CASES` | `"skipped"` | The value from `COMPARATORLESS_CASES` |
| Anything else | `"missing"` | `"no comparator case in report"` |

Both outcomes keep `comparator_driver=None`, `comparator_median_ms=None`, and `median_ratio=None`.

`_format_comparator_outcome` prints a skipped outcome as:

```text
[SKIP] <engine> <case> (<vertebra_driver>): <reason>
```

The `[MISSING]` line and the other three lines keep their format.

`main`, `check` branch: the exit rule stays `1` on any `fail`, or on any `missing` without `--allow-missing`. A `skipped` outcome never sets the failure flag. No flag is added.

Edge cases:

- A listed `(driver, case)` whose comparator case is present in the report is compared like any other case. The listing has no effect then.
- A report whose only outcomes are `skipped` exits `0`. Exit `2` stays reserved for a report with no outcome at all.
- A key whose driver is not in `COMPARATOR_PAIRS` can never be reached. A unit test rejects such a key.
- `--allow-missing` still turns an unlisted missing comparator into exit `0`.

### SQLite cursor case

In `capture_sqlite_report`, add the case `cursor_point_select_1` for two drivers. Labels, category, and mode match the existing sync cases: `category="sql"`, `driver_mode="sync_blocking"`.

`vertebra-sqlite-sync`, inside the existing `with SyncSQLiteDriver(cfg) as sync_drv:` block, after `sync_multi`:

```python
native_cursor = sync_drv.cursor()
try:
    sync_cursor_point = sample_sync(
        lambda: native_cursor.execute("SELECT ?", (1,)).fetchone(),
        iterations=iterations,
        warmup=warmup,
    )
finally:
    native_cursor.close()
```

`sqlite3`, inside the existing `if include_external:` block:

- Create `stdlib_cursor = conn.cursor()` on the line after `conn = sqlite3.connect(db_path)`.
- After `stdlib_multi`, sample `lambda: stdlib_cursor.execute("SELECT ?", (1,)).fetchone()` into `stdlib_cursor_point` with the same `iterations` and `warmup`.
- In the existing `finally`, call `stdlib_cursor.close()` before `conn.close()`.

Append one `compute_adapter_case_stats(case="cursor_point_select_1", ...)` entry per driver, directly after that driver's `multi_row_select_10` entry.

- The SQL text and the parameters are the same on both sides, and the same as `point_select_1`.
- The cursor is created once, outside the sampled callable, on both sides.
- With `include_external=False` the report holds the Vertebra case only. `check` then reports `[MISSING]` for it, as it does for the other SQLite cases.
- No async cursor case is added.
- The existing pair `"vertebra-sqlite-sync": ("sqlite3",)` covers the case. `COMPARATOR_PAIRS` does not change.
- Update the function's docstring to: `Measure Vertebra SQLite drivers and a reused native cursor against stdlib sqlite3 and optional aiosqlite.`

### Imports in the test file

`COMPARATOR_ALLOW_LIST` and `COMPARATOR_PAIRS` are imported and used by three tests, and `ruff` reports no unused import in the planning tree. Keep both. Add `COMPARATORLESS_CASES` to the same import, in sorted position. `ruff check` on the test file reports nothing when the task ends.

## Test Matrix

All tests go in `src/py/tests/unit/tools/test_adapter_benchmarks_sql.py` and build reports with the existing `_report` helper. Write each new test before the code it covers and see it fail first.

| Test | Input | Expected |
| --- | --- | --- |
| `test_capture_sqlite_report_runs_offline_and_preserves_memory_isolation` (extend `expected_modes`) | Real capture, `include_external=True` | Keys `("cursor_point_select_1", "vertebra-sqlite-sync")` and `("cursor_point_select_1", "sqlite3")` are present with mode `sync_blocking`, five iterations, and positive metrics |
| `test_comparator_check_skips_listed_comparatorless_case` | `postgres` report: `pipeline_select_16` for `vertebra-async`; `point_select_1` for `vertebra-async` at `0.07` and `asyncpg` at `0.08` | Two outcomes. The pipeline one has status `skipped`, the reason from `COMPARATORLESS_CASES`, and `None` for comparator driver, comparator median, and ratio. The other is `pass` |
| `test_comparator_check_compares_listed_case_when_comparator_present`, parametrized over `(vertebra_ms, expected_status)` = `(1.0, "pass")`, `(3.0, "fail")` | `postgres` report: `pipeline_select_16` for `vertebra-async` at `vertebra_ms` and for `asyncpg` at `2.0` | One outcome with `expected_status` and comparator driver `asyncpg` |
| `test_comparator_check_marks_missing_comparator` (existing, unchanged) | Unlisted case with no comparator | Status `missing`, reason `no comparator case in report` |
| `test_comparatorless_entries_are_paired_and_explained`, parametrized over `sorted(COMPARATORLESS_CASES.items())` | Each `((driver, case), reason)` | `driver` is a key of `COMPARATOR_PAIRS`; `case` is not empty; `reason` is not blank and has no newline |
| `test_sql_cli_check_skip_exit_codes`, parametrized over the four rows below | Reports written to `tmp_path` | Exit code and output line as listed |

Rows for `test_sql_cli_check_skip_exit_codes`, as `(cases, extra_args, expected_exit, expected_line_prefix)`:

| Cases in a `postgres` report | Extra arguments | Exit | Output contains |
| --- | --- | --- | --- |
| `pipeline_select_16` for `vertebra-async` only | none | `0` | `[SKIP] postgres pipeline_select_16 (vertebra-async): ` followed by the listed reason |
| `other_case` for `vertebra-async` only | none | `1` | `[MISSING] postgres other_case (vertebra-async): no comparator case in report` |
| `other_case` for `vertebra-async` only | `--allow-missing` | `0` | The same `[MISSING]` line |
| `pipeline_select_16` for `vertebra-async`; `point_select_1` for `vertebra-async` at `0.09` and `asyncpg` at `0.07` | none | `1` | Both a `[SKIP]` line and a `[FAIL] postgres point_select_1 (vertebra-async vs asyncpg):` line |

The existing tests for pass, fail, and allow-list (`test_comparator_check_passes_when_vertebra_is_not_slower`, `test_comparator_check_fails_slower_case_outside_allow_list`, `test_comparator_check_allows_listed_driver_with_reason`, `test_comparator_check_allows_slower_duckdb_case`) and `test_sql_cli_check_exit_codes` stay and pass unchanged.

New tests use `@pytest.mark.parametrize`, not loops. No inline comment in code or tests. New functions and tests have one-line docstrings.

## Verification

```bash
uv run --no-sync --frozen pytest src/py/tests/unit/tools/test_adapter_benchmarks_sql.py -p no:xdist -q
uv run --no-sync --frozen ruff check tools/benchmarks/adapters/sql.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
uv run --no-sync --frozen mypy | grep "tools/benchmarks/adapters/sql.py\|test_adapter_benchmarks_sql.py"
out="$(mktemp -d)"
uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlite --dsn :memory: --iterations 200 --warmup 20 --output "$out/sqlite.json"
uv run --no-sync python -m tools.benchmarks.adapters.sql check --report "$out/sqlite.json" --max-median-ratio 1000
git diff -- tools/benchmarks/adapters/sql.py | grep "^[-+].*vertebra-.*-sync\":"
make lint-all
```

`pytest` reports no failure. `ruff` reports nothing and the `mypy` `grep` prints nothing. The `check` output holds a line for `cursor_point_select_1`. The `git diff` `grep` prints nothing, which shows the pair table and the allow-list have no changed entry. The report file stays outside the repository.

## Acceptance Criteria

- [ ] A listed case with no comparator prints `[SKIP]` with its reason and does not change the exit code.
- [ ] An unlisted case with no comparator still exits `1`, and `0` with `--allow-missing`.
- [ ] The SQLite report holds `cursor_point_select_1` for `vertebra-sqlite-sync` and `sqlite3`, each sampled on one reused cursor.
- [ ] `COMPARATOR_PAIRS` and `COMPARATOR_ALLOW_LIST` have no diff.
- [ ] The test file passes, with the new tests parametrized as listed, and `ruff` and `mypy` report nothing for the two files.

[^bench-sql]: `tools/benchmarks/adapters/sql.py::check_report_against_comparators`
[^bench-sqlite]: `tools/benchmarks/adapters/sql.py::capture_sqlite_report`
[^bench-pg-pipeline]: `tools/benchmarks/postgres.py::_measure_async_vertebra_pipeline_batch`
[^bench-tests]: `src/py/tests/unit/tools/test_adapter_benchmarks_sql.py`
