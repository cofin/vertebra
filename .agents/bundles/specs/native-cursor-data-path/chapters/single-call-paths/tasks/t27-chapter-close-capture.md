---
type: Task
id: "native-cursor-data-path:single:t27"
chapter_id: single-call-paths
title: "Chapter close: adapter matrix capture for every acceptance row and the wire-parity trigger statement"
description: "Capture the adapter matrix three times for every acceptance row on a release build, record the medians in section 6 of the chapter spec, and state whether the wire-parity trigger fired."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: integration_acceptance
depends_on:
  - "native-cursor-data-path:single:t01"
  - "native-cursor-data-path:single:t02"
  - "native-cursor-data-path:single:t03"
  - "native-cursor-data-path:single:t04"
  - "native-cursor-data-path:single:t05"
  - "native-cursor-data-path:single:t06"
  - "native-cursor-data-path:single:t07"
  - "native-cursor-data-path:single:t08"
  - "native-cursor-data-path:single:t09"
  - "native-cursor-data-path:single:t10"
  - "native-cursor-data-path:single:t11"
  - "native-cursor-data-path:single:t12"
  - "native-cursor-data-path:single:t13"
  - "native-cursor-data-path:single:t14"
  - "native-cursor-data-path:single:t15"
  - "native-cursor-data-path:single:t16"
  - "native-cursor-data-path:single:t17"
  - "native-cursor-data-path:single:t18"
  - "native-cursor-data-path:single:t19"
  - "native-cursor-data-path:single:t20"
  - "native-cursor-data-path:single:t21"
  - "native-cursor-data-path:single:t22"
  - "native-cursor-data-path:single:t23"
  - "native-cursor-data-path:single:t24"
  - "native-cursor-data-path:single:t25"
  - "native-cursor-data-path:single:t26"
  - "native-cursor-data-path:single:t28"
  - "native-cursor-data-path:single:t29"
  - "native-cursor-data-path:single:t30"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:263-283"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t07-acceptance-run-and-results.md:65-103"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md"
  - "tools/benchmarks/adapters/sql.py:1459-1515"
  - "tools/benchmarks/fixtures.py::FIXTURE_SPECS"
files:
  - .agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md
tests: []
estimated_loc: 30
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:40:00Z
sources:
  - id: bench-sql
    resource: tools/benchmarks/adapters/sql.py::main
    title: "Adapter benchmark CLI with capture and check commands"
  - id: acceptance-run
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t07-acceptance-run-and-results.md
    title: "Capture commands and run conditions for the acceptance rows"
  - id: bench-fixtures
    resource: tools/benchmarks/fixtures.py::FIXTURE_SPECS
    title: "Container fixtures for the network engines"
tags: [benchmarks, acceptance]
---

# t27 — Chapter close: adapter matrix capture for every acceptance row and the wire-parity trigger statement

## Objective & Scope Boundary

Section 6 of the chapter spec shows what every acceptance row measures after this chapter, and says whether `wire-parity` has work to do.[^bench-sql][^acceptance-run]

In scope: the benchmark captures and section 6 of `chapters/single-call-paths/spec.md`.

Out of scope:

- Any other section of the chapter spec and any other repository file.
- Any code change, including the benchmark allow-list.
- A pass condition. No row has one in this task. The comparison check is enforced in `cleanup-and-checks`.

## Bounded Read Manifest

- Root `spec.md:263-283` for the acceptance table.
- `cleanup-and-checks/tasks/t07-acceptance-run-and-results.md:65-103` for the run conditions, the capture arguments, and the driver and comparator names per row.
- `chapters/single-call-paths/spec.md` for the current text of section 6.
- `tools/benchmarks/adapters/sql.py:1459-1515` for the `capture` and `check` arguments.
- `tools/benchmarks/fixtures.py::FIXTURE_SPECS` for the container images and ports.[^bench-fixtures]

## Contracts & Edge Cases

Run conditions, as in the acceptance run worksheet: 64-bit glibc Linux, release build from `make build`, Python 3.12, loopback, containers from `FIXTURE_SPECS` on tmpfs. Embedded engines use `--iterations 2000 --warmup 200`. Network engines use `--iterations 500 --warmup 50`. Drivers take turns within a capture. Every report is written under a directory created with `mktemp -d`. No report file is committed.

Captures. Each one runs three times, into `run1`, `run2`, and `run3` under the temporary directory:

| Root spec row | Report | Arguments after `capture` | Vertebra driver | Comparator | Case |
| --- | --- | --- | --- | --- | --- |
| SQLSpec SQLite sync | `sqlspec-sqlite.json` | `--engine sqlspec --sqlspec-engine sqlite --dsn :memory:` | `vertebra-sqlspec-sqlite-sync` | `sqlspec-sqlite-sync` | `point_select_1` |
| SQLAlchemy `sqlite+vtb` | `sqlalchemy.json` | `--engine sqlalchemy --dsn sqlite+vtb:///:memory:` | `sqlalchemy-vtb-sync` | `sqlalchemy-vanilla-sync` | `sa_point_select_1` |
| Native SQLite `select_one` | `sqlite.json` | `--engine sqlite --dsn :memory:` | `vertebra-sqlite-sync` | `sqlite3` | `point_select_1` |
| PostgreSQL async pooled | `postgres.json` | `--engine postgres --dsn <postgres fixture URL>` | `vertebra-async` | `asyncpg` | `point_select_1` |
| SQLSpec PostgreSQL async | `sqlspec-postgres.json` | `--engine sqlspec --sqlspec-engine postgres --dsn <postgres fixture URL>` | `vertebra-sqlspec-postgres-async` | `sqlspec-asyncpg` | `point_select_1` |
| Oracle sync, relay off | `oracle-relay-off.json` | `--engine oracle --oracle-reset-relay off --dsn <oracle fixture URL>` | `vertebra-oracle-sync` | `oracledb-sync` | `point_select_1` |
| Native DuckDB sync | `duckdb.json` | `--engine duckdb --dsn :memory:` | `vertebra-duckdb-sync` | `duckdb` | `point_select_1` |

Reading the results:

- After each run, call `check` with that run's seven reports and `--allow-missing`. Its exit status is ignored. Each printed line gives the ratio of medians and the two medians for one driver and case.
- For each row take the three ratios. The recorded median is the middle one. The recorded Vertebra and comparator medians are the ones from that middle run.

Section 6 edit. Replace the text that describes this task as pending with:

1. One sentence with the run conditions and the date of the run.
2. A table with the columns `Row | Vertebra driver | Comparator | Run 1 | Run 2 | Run 3 | Median ratio | Vertebra median | Comparator median`, one line per row above, in that order. Ratios have three decimals. Medians are in microseconds.
3. One sentence for the PostgreSQL async pooled row: its median ratio, and whether that ratio is above `1.0`.
4. One sentence for the Oracle sync, relay off row: its median ratio, and whether that ratio is above `1.0`.
5. One sentence that states the outcome: the `wire-parity` trigger fired when either ratio is above `1.0`, and did not fire when both are at or below `1.0`.

The other bullets of section 6 stay as they are. The text names only comparators already in `tools/benchmarks/adapters/sql.py`, no workstation path, and no internal tool.

Edge cases:

| Case | Behavior |
| --- | --- |
| A row's ratio is above `1.0` | Recorded. The task continues. No row blocks this task by its value |
| The three ratios of a row differ by more than `0.3` | Recorded as measured. Add the word `noisy` after the median ratio |
| The PostgreSQL or the Oracle container cannot start | Set the task to `blocked` with the reason. The trigger statement needs both rows |
| `check` prints `[MISSING]` for a case outside the seven rows | Ignored |
| `check` prints `[MISSING]` for one of the seven rows | Set the task to `blocked` and report the driver and case |
| The build is not a release build | Run `make build` first. A debug capture is not recorded |

## Test Matrix

| Check | Expected |
| --- | --- |
| Reports per run | Seven JSON files in each of `run1`, `run2`, `run3` |
| Section 6 table | Seven lines, each with three run ratios and a median |
| Trigger statement | Present for both rows, each naming its median ratio and whether it is above `1.0` |
| `git status --short -- tools/benchmarks src` | No change from this task |
| `git diff --stat` for the chapter spec | Changes only inside section 6 |
| Prose lint from the planning toolkit on the chapter spec | No finding |

## Verification

```bash
export PATH="$HOME/.cargo/bin:$PATH"
make build
out="$(mktemp -d)"
mkdir -p "$out/run1" "$out/run2" "$out/run3"
uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql capture --engine sqlite --dsn :memory: --iterations 2000 --warmup 200 --output "$out/run1/sqlite.json"
uv run --no-sync --frozen python -m tools.benchmarks.adapters.sql check --allow-missing $(for f in "$out"/run1/*.json; do printf -- '--report %s ' "$f"; done) || true
git status --short -- tools/benchmarks src
```

Run one `capture` line per row of the capture table and per run directory, with the iteration counts from the run conditions, then one `check` line per run directory.

## Acceptance Criteria

- [x] All seven captures ran three times on a release build.
- [x] Section 6 of the chapter spec holds the table with the three ratios and the median for each row.
- [x] Section 6 states, for the PostgreSQL async pooled row and the Oracle sync, relay off row, whether the median ratio is above `1.0`, and states whether the `wire-parity` trigger fired.
- [x] No file other than the chapter spec has a diff from this task, and the chapter spec diff is inside section 6.
- [x] No report file is committed.

[^bench-sql]: `tools/benchmarks/adapters/sql.py::main`
[^acceptance-run]: `.agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t07-acceptance-run-and-results.md`
[^bench-fixtures]: `tools/benchmarks/fixtures.py::FIXTURE_SPECS`
