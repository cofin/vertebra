---
type: Task
id: "native-cursor-data-path:wrapper:t09"
chapter_id: wrapper-layers
title: "Chapter close: removal check and SQLite and DuckDB adapter matrix"
description: "Confirm the names this chapter removed are gone, run the chapter checks, sample the adapter matrix for SQLite and DuckDB, and record the two SQLite wrapper acceptance rows."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: integration_acceptance
depends_on:
  - "native-cursor-data-path:wrapper:t03"
  - "native-cursor-data-path:wrapper:t08"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/wrapper-layers/spec.md"
  - "tools/benchmarks/adapters/sql.py:1011-1105"
  - "tools/benchmarks/adapters/sql.py:1190-1250"
  - "tools/benchmarks/adapters/sql.py:1312-1366"
  - "tools/benchmarks/adapters/base.py:1-40"
files:
  - .agents/bundles/specs/native-cursor-data-path/chapters/wrapper-layers/spec.md
tests: []
estimated_loc: 30
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
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/wrapper-layers/spec.md
    title: "Chapter spec with the measured-results table"
tags: [benchmarks, sqlite, duckdb, cleanup]
---

# t09 — Chapter close: removal check and SQLite and DuckDB adapter matrix

## Objective & Scope Boundary

After this task, the chapter spec holds measured numbers for the SQLSpec SQLite sync row and the SQLAlchemy `sqlite+vtb` row, and the removed names are confirmed gone. The result decides whether `network-engines` proceeds or is replanned.

In scope: running checks and benchmarks, and editing the measured-results table and the first bullet in section 6 of the chapter spec.

Out of scope:

- Any change to source, test, or benchmark code. A failed check or a missed target is recorded and the task is set to `blocked`.
- The benchmark comparison check with its allow-list. `cleanup-and-checks` adds it. This task uses the existing `compare` command.
- The driver limitations knowledge page. `cleanup-and-checks` writes it from the final run.

## Bounded Read Manifest

| Range | Why |
| --- | --- |
| Chapter `spec.md` | The removed-symbol table and the measured-results table |
| `tools/benchmarks/adapters/sql.py:1011-1105` | SQLAlchemy capture and its driver labels |
| `tools/benchmarks/adapters/sql.py:1190-1250` | SQLSpec capture and its driver labels |
| `tools/benchmarks/adapters/sql.py:1312-1366` | `capture` and `compare` arguments |
| `tools/benchmarks/adapters/base.py:1-40` | Default ratio thresholds |

## Contracts & Edge Cases

Driver labels in the reports:

| Case | Vertebra label | Comparator label |
| --- | --- | --- |
| SQLSpec SQLite sync | `vertebra-sqlspec-sqlite-sync` | `sqlspec-sqlite-sync` |
| SQLAlchemy `sqlite+vtb` | `sqlalchemy-vtb-sync` | `sqlalchemy-vanilla-sync` |
| SQLSpec DuckDB sync | `vertebra-sqlspec-duckdb-sync` | `sqlspec-duckdb-sync` |

Rules:

- Benchmarks run on a release build (`make build`), Python 3.12, with no other load on the machine.
- Each capture runs three times. The three runs of the different captures are interleaved, so the drivers take turns. The run with the median throughput ratio is the recorded one.
- A row meets its target when `compare` exits `0` with `--min-throughput-ratio 1.0` for the recorded run. The default p99 latency ratio limit of `compare` stays in force.
- The DuckDB row is measured on the jemalloc build from `embedded-cursors`, on 64-bit glibc Linux. Its target is at or below the comparator. A miss is recorded with both medians and the ratio, and it does not block this task or trigger the replan. `cleanup-and-checks` enforces DuckDB in its final run.
- Report files are written to a temporary directory outside the repository and are not committed.
- The spec table records median latency in microseconds for both drivers and the throughput ratio. It names no machine, path, or user.

| Case | Action |
| --- | --- |
| Both SQLite rows meet their targets | Fill the table, replace the first bullet of section 6 with "All tasks are complete. The SQLite wrapper rows meet their targets.", complete the task |
| Either SQLite row misses | Fill the table with the measured numbers, add one bullet naming the row and its ratio, set the task to `blocked`. The `replan_trigger` of `network-engines` applies |
| A removed name still matches | Set the task to `blocked` and name the file and line in section 6. Do not delete it here |
| `make lint-all` or a test file fails | Set the task to `blocked` and name the failing check in section 6 |
| A comparator package is not installed | The capture omits the comparator. Install the project's benchmark dependency group and rerun. Do not record a row without its comparator |

## Test Matrix

| Check | Expected |
| --- | --- |
| `git grep` for the SQLite adapter names removed in t07 | No match under `src`, `tools`, `docs`, apart from string literals in the removed-name test |
| `git grep` for the DuckDB adapter names removed in t08 | Same |
| `git grep -n "NATIVE_CURSOR_ENGINES" -- src/py/vertebra` | Matches in four files only: `db/driver/_common.py`, `db/dbapi/_sync.py`, `db/dbapi/_async.py`, and `extensions/sqlspec/_driver.py` |
| `make lint-all` | Exit `0` |
| The four chapter test files | All pass |
| `compare` for the two SQLite rows | Exit `0` |
| `compare` for the DuckDB row | Exit code recorded; a non-zero exit is a recorded miss, not a failure of this task |

`_driver.py` holds two reads, one per base class.

## Verification

```bash
git grep -n -E "normalize_sqlite_parameters|normalize_sqlite_many_parameters|_extract_named_colon_placeholders|_QUOTED_SQL_SEGMENT|normalize_duckdb_parameters|normalize_duckdb_many_parameters|normalize_duckdb_scalar" -- src tools docs
git grep -n "unwrap_typed_parameters" -- src/py/vertebra/extensions/sqlspec/adapters/sqlite src/py/vertebra/extensions/sqlspec/adapters/duckdb
git grep -n "NATIVE_CURSOR_ENGINES" -- src/py/vertebra
make lint-all
uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py src/py/tests/unit/extensions/test_sqlspec_sqlite_mysql_mssql_duckdb.py -q
make build
OUT="$(mktemp -d)"
for RUN in 1 2 3; do
  PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlspec --sqlspec-engine sqlite --dsn ":memory:" --iterations 20000 --warmup 2000 --output "$OUT/sqlspec-sqlite-$RUN.json"
  PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlalchemy --dsn "sqlite+vtb:///:memory:" --iterations 20000 --warmup 2000 --output "$OUT/sqlalchemy-sqlite-$RUN.json"
  PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlspec --sqlspec-engine duckdb --dsn ":memory:" --iterations 20000 --warmup 2000 --output "$OUT/sqlspec-duckdb-$RUN.json"
done
PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql compare --candidate "$OUT/sqlspec-sqlite-1.json" --baseline "$OUT/sqlspec-sqlite-1.json" --candidate-driver vertebra-sqlspec-sqlite-sync --baseline-driver sqlspec-sqlite-sync --min-throughput-ratio 1.0
PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql compare --candidate "$OUT/sqlalchemy-sqlite-1.json" --baseline "$OUT/sqlalchemy-sqlite-1.json" --candidate-driver sqlalchemy-vtb-sync --baseline-driver sqlalchemy-vanilla-sync --min-throughput-ratio 1.0
PYTHONPATH="src/py:." uv run --no-sync python -m tools.benchmarks.adapters.sql compare --candidate "$OUT/sqlspec-duckdb-1.json" --baseline "$OUT/sqlspec-duckdb-1.json" --candidate-driver vertebra-sqlspec-duckdb-sync --baseline-driver sqlspec-duckdb-sync --min-throughput-ratio 1.0
make review-check
```

Run each `compare` command for all three runs of its capture by changing the run number in both file arguments. Record the run whose throughput ratio is the middle of the three.

## Acceptance Criteria

- [ ] The two removed-name `git grep` commands print no source match.
- [ ] `NATIVE_CURSOR_ENGINES` is read in the four files named above and nowhere else under `src/py/vertebra`.
- [ ] `make lint-all` and the four chapter test files pass.
- [ ] The measured-results table in section 6 holds numbers for all three rows, and the DuckDB row states whether it is at or below its comparator.
- [ ] The SQLSpec SQLite sync row and the SQLAlchemy `sqlite+vtb` row each pass `compare` at ratio `1.0`, or the task is `blocked` with the miss recorded.
