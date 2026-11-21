---
type: Task
id: "native-cursor-data-path:cleanup:t07"
chapter_id: cleanup-and-checks
title: "Final acceptance run and measured results table"
description: "Capture the adapter matrix for every engine, enforce the acceptance rows with the comparison check, and replace the gap table in the driver limitations chapter with the measured results."
worksheet_format: compact
status: stable
state: completed
priority: P1
verification_strategy: integration_acceptance
depends_on:
  - "native-cursor-data-path:cleanup:t03"
  - "native-cursor-data-path:cleanup:t04"
  - "native-cursor-data-path:cleanup:t05"
  - "native-cursor-data-path:cleanup:t13"
  - "native-cursor-data-path:cleanup:t14"
  - "native-cursor-data-path:cleanup:t15"
  - "native-cursor-data-path:cleanup:t16"
  - "native-cursor-data-path:cleanup:t17"
  - "native-cursor-data-path:cleanup:t18"
  - "native-cursor-data-path:cleanup:t19"
  - "native-cursor-data-path:cleanup:t20"
  - "native-cursor-data-path:cleanup:t23"
  - "native-cursor-data-path:single:t27"
  - "native-cursor-data-path:wire:t02"
  - "native-cursor-data-path:wire:t03"
  - "native-cursor-data-path:wire:t04"
read_set:
  - ".agents/bundles/knowledge/domains/data/database-driver-limitations.md:1-45"
  - ".agents/bundles/knowledge/domains/data/database-driver-limitations.md:145-166"
  - "tools/benchmarks/adapters/sql.py:1-100"
  - "tools/benchmarks/fixtures.py::FIXTURE_SPECS"
files:
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
tests: []
estimated_loc: 60
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: knowledge-driver-limitations
    resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
    title: "Adapter throughput gap table"
  - id: bench-sql
    resource: tools/benchmarks/adapters/sql.py::main
    title: "Adapter benchmark CLI with capture and check commands"
  - id: bench-fixtures
    resource: tools/benchmarks/fixtures.py::FIXTURE_SPECS
    title: "Container fixtures for the network engines"
tags: [benchmarks, knowledge, acceptance]
---

# t07 — Final acceptance run and measured results table

## Objective & Scope Boundary

The driver limitations chapter shows what the adapters measure after this spec, in place of the gap table written before it. This is the one place the comparison check is enforced. The six blocking acceptance rows in the root spec are confirmed or the task stops. The two DuckDB rows are on the allow-list by user decision. They are measured and recorded, and they do not stop the task.[^knowledge-driver-limitations]

This task runs after `native-cursor-data-path:single:t27`, which closes `single-call-paths`, and after `native-cursor-data-path:wire:t02`, the last task of `wire-parity`. A `wire` task left `deferred` because its trigger did not fire counts as closed.

In scope: one knowledge chapter and the benchmark run that feeds it. Out of scope: any code change, including the benchmark allow-list. A missed target is not fixed here.

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/database-driver-limitations.md:1-45` for frontmatter and the truth block.
- Lines `145-166` of the same file for the section "Adapter Throughput Gaps Against Third-Party Drivers" and the paragraphs after its table.
- The comparator pair table, the allow-list, and `COMPARATORLESS_CASES` near the top of `tools/benchmarks/adapters/sql.py`.[^bench-sql]
- `tools/benchmarks/fixtures.py::FIXTURE_SPECS` for the container images and ports.[^bench-fixtures]

## Contracts & Edge Cases

The measured state before `single-call-paths` is in the root spec acceptance table and the hot-path research brief at `.agents/bundles/research/native-cursor-hot-paths_20261121.md`.

Run conditions: 64-bit glibc Linux, release build (`make build`), Python 3.12, loopback, containers from `FIXTURE_SPECS` on tmpfs, `--iterations 2000 --warmup 200` for embedded engines and `--iterations 500 --warmup 50` for network engines. Write every report to a directory created with `mktemp -d`. No report file is committed.

Captures, one report each:

| Report | Command arguments after `capture` |
| --- | --- |
| `sqlite.json` | `--engine sqlite --dsn :memory:` |
| `duckdb.json` | `--engine duckdb --dsn :memory:` |
| `sqlalchemy.json` | `--engine sqlalchemy --dsn sqlite+vtb:///:memory:` |
| `sqlspec-sqlite.json` | `--engine sqlspec --sqlspec-engine sqlite --dsn :memory:` |
| `sqlspec-duckdb.json` | `--engine sqlspec --sqlspec-engine duckdb --dsn :memory:` |
| `postgres.json` | `--engine postgres --dsn <postgres fixture URL>` |
| `sqlspec-postgres.json` | `--engine sqlspec --sqlspec-engine postgres --dsn <postgres fixture URL>` |
| `mysql.json` | `--engine mysql --dsn <mysql fixture URL>` |
| `mssql.json` | `--engine mssql --dsn <mssql fixture URL>` |
| `oracle.json` | `--engine oracle --dsn <oracle fixture URL>` |
| `oracle-relay-off.json` | `--engine oracle --oracle-reset-relay off --dsn <oracle fixture URL>` |
| `spanner.json` | `--engine spanner --dsn <spanner emulator target>` |
| `bigquery.json` | `--engine bigquery --dsn <bigquery emulator target>` |

Then run `check` once with every report passed through `--report`.

Blocking acceptance rows, all required:

| Root spec row | Report | Vertebra driver | Comparator |
| --- | --- | --- | --- |
| SQLSpec SQLite sync | `sqlspec-sqlite.json` | `vertebra-sqlspec-sqlite-sync` | `sqlspec-sqlite-sync` |
| SQLAlchemy `sqlite+vtb` | `sqlalchemy.json` | `sqlalchemy-vtb-sync` | `sqlalchemy-vanilla-sync` |
| Native SQLite `select_one` | `sqlite.json` | `vertebra-sqlite-sync` | `sqlite3` |
| PostgreSQL async pooled | `postgres.json` | `vertebra-async` | `asyncpg` |
| SQLSpec PostgreSQL async | `sqlspec-postgres.json` | `vertebra-sqlspec-postgres-async` | `sqlspec-asyncpg` |
| Oracle sync, relay off | `oracle-relay-off.json` | `vertebra-oracle-sync` | `oracledb-sync` |

Allow-listed rows. Both DuckDB drivers are on `COMPARATOR_ALLOW_LIST`, because the bundled build has no jemalloc. Both are measured and recorded in the table. Each prints `[ALLOW]` when slower and `[PASS]` when at or ahead.

| Row | Report | Vertebra driver | Comparator |
| --- | --- | --- | --- |
| Native DuckDB sync | `duckdb.json` | `vertebra-duckdb-sync` | `duckdb` |
| SQLSpec DuckDB sync | `sqlspec-duckdb.json` | `vertebra-sqlspec-duckdb-sync` | `sqlspec-duckdb-sync` |

Like-for-like row, recorded and not blocking. It reuses one cursor on both sides, so it shows the native statement cost without the driver's convenience layer.

| Row | Report | Case | Vertebra driver | Comparator |
| --- | --- | --- | --- | --- |
| SQLite reused cursor | `sqlite.json` | `cursor_point_select_1` | `vertebra-sqlite-sync` | `sqlite3` |

The PostgreSQL report holds `pipeline_select_16` for `vertebra-async`. It is listed in `COMPARATORLESS_CASES` and prints `[SKIP]`.

Stop rules:

- A `[FAIL]` or `[MISSING]` line on any of the six rows: repeat that capture twice more. If two of the three runs fail, mark the task blocked, record the three ratios in the worksheet, and route the spec through refine. Do not edit the knowledge chapter.
- The two DuckDB rows print `[ALLOW]` or `[PASS]`. Record the measured medians and the status in the table and continue.
- A `[FAIL]` on any other row outside the six, including `cursor_point_select_1`: record it in the table with its cause and continue. The allow-list is not edited.
- A `[MISSING]` line for a case outside the six that is not listed in `COMPARATORLESS_CASES`: record it and report it in the task result. Do not edit `COMPARATORLESS_CASES`.
- The Spanner or BigQuery emulator, the SQL Server container, or the MySQL container cannot start: record "not measured" with the reason for that row and continue. SQLite, PostgreSQL, and Oracle are required.

Chapter edit:

1. Rename the section heading to `## Adapter Results Against Third-Party Drivers`.
2. Replace the intro sentence with the run conditions and the date of the run.
3. Replace the table with columns `Adapter and case | Vertebra | Comparator | Status | Cause of any gap`. One row per `check` outcome on `point_select_1`, `sa_point_select_1`, and `cursor_point_select_1`, with medians in microseconds or milliseconds as the old table used. `Status` is `at or ahead`, `allowed`, or `behind`.
4. Keep the storage rows (`file://` get, `memory://` put) unchanged; this spec does not touch storage.
5. Keep a DuckDB row for each DuckDB driver with its measured status `allowed` or `at or ahead`. State the cause once: `vertebra-duckdb` keeps the `bundled` build, which has no jemalloc, because the published `libduckdb-sys` package ships no CMake tree.
6. Add an Oracle row for each relay setting.
7. Rewrite the paragraphs after the table so they describe current behavior. Remove sentences about layers this spec deleted.
8. Update any truth-block bullet that describes the removed layers. Keep the block at 40 lines or fewer.
9. Record two Oracle facts in the paragraphs after the table. First: on the Oracle driver, out-bind values and session-context tracking come back only from `execute` and the `execute_dml` method under it, not from the fetch methods; a statement that needs both rows and out-binds runs on the cursor. Second: a pooled Oracle release sends no rollback round trip when the connection has no open transaction, and still sends one when a transaction may be open, which includes every release after a query. Check both against `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/rs/vertebra-oracle/src/pool.rs` before writing them, and write what the code does.
10. Add a `verified` entry with the task commit. Add one `sources` entry with a matching footnote. Its resource is the path `tools/benchmarks/adapters/sql.py`, then `::`, then the function name `check_report_against_comparators`.

Text names only comparators present in the pair table from t02, no workstation path, and no internal tool.

## Test Matrix

| Check | Expected |
| --- | --- |
| `check` across all reports | Exit `0` |
| Six acceptance rows | `[PASS]` |
| DuckDB rows | `[ALLOW]` or `[PASS]`. Neither blocks the task |
| `cursor_point_select_1` | A `[PASS]` or `[FAIL]` line is present and recorded |
| `pipeline_select_16` | `[SKIP]` |
| `git diff --stat -- tools/benchmarks` | Empty |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |
| Old heading text `Adapter Throughput Gaps` in the bundle | No match outside `specs/` |

## Verification

```bash
make build
out="$(mktemp -d)"
uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlite --dsn :memory: --iterations 2000 --warmup 200 --output "$out/sqlite.json"
uv run --no-sync python -m tools.benchmarks.adapters.sql check $(for f in "$out"/*.json; do printf -- '--report %s ' "$f"; done)
make review-check
git grep -n "Adapter Throughput Gaps" -- .agents/bundles ':!.agents/bundles/specs'
```

Run one `capture` line per row of the capture table before `check`. The last command prints nothing.

## Acceptance Criteria

- [x] All thirteen captures ran or are recorded as not measured with a reason.
- [x] `check` exits `0` and the six blocking acceptance rows pass.
- [x] Each DuckDB row is recorded with its medians and its status, `allowed` or `at or ahead`.
- [x] The `cursor_point_select_1` row is recorded.
- [x] The allow-list has no diff from this task.
- [x] The chapter holds the measured table, a cause for every `allowed` or `behind` row, and the reason the bundled DuckDB build has no jemalloc.
- [x] `make review-check` passes and the prose lint reports no finding.
- [x] No report file is committed.

[^knowledge-driver-limitations]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md`
[^bench-sql]: `tools/benchmarks/adapters/sql.py::main`
[^bench-fixtures]: `tools/benchmarks/fixtures.py::FIXTURE_SPECS`
