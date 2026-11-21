---
type: Task
id: "native-cursor-data-path:network:t41"
chapter_id: network-engines
title: "Sample the adapter benchmark matrix for the six network engines"
description: "Run the adapter matrix for PostgreSQL, MySQL, SQL Server, Oracle, Spanner, and BigQuery and record each acceptance row with its comparator."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: integration_acceptance
depends_on:
  - "native-cursor-data-path:network:t40"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:205-221"
  - "tools/benchmarks/adapters/sql.py:1305-1360"
  - ".agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md:110-140"
files: []
tests: []
estimated_loc: 0
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: bench-adapters
    resource: "tools/benchmarks/adapters/sql.py"
    title: "Adapter benchmark matrix with capture and compare commands"
tags: [benchmark, performance, acceptance]
---

# Sample the adapter benchmark matrix for the six network engines

## Objective & Scope Boundary

The chapter ends with the adapter matrix capture for the six engines it touched. The task records the figures. It enforces no acceptance row. No repository file is edited. Reports go to a temporary directory outside the repository.

Out of scope:

- The comparison check and its allow-list. The cleanup chapter adds them and enforces the PostgreSQL and Oracle acceptance rows from its own final run.
- The Oracle capture with the relay off. The capture flag for it is added in the cleanup chapter, which owns `tools/benchmarks/adapters/`.
- Editing knowledge chapters with the numbers. The cleanup chapter does that from these reports.
- Tuning code to meet a target. A missed target is reported and replanned.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `tools/benchmarks/adapters/sql.py:1305-1360` defines the `capture` and `compare` commands and their arguments.
- The benchmarks knowledge chapter lists the fixtures each engine needs and how to start them.

## Contracts & Edge Cases

- Build release extensions with `make build` before sampling. Debug builds are not measured.
- Run `capture` once per engine: `postgres`, `mysql`, `mssql`, `oracle`, `spanner`, `bigquery`. Use the harness option that makes drivers take turns within one run, as the benchmarks knowledge chapter describes. Use Python 3.12.
- Oracle is captured once, with the default config, which keeps the relay on.
- Every capture passes `--output` with a path under a directory created by `mktemp -d`. Nothing is written under `tools/benchmarks/results/` and nothing is committed.
- The task result lists the Vertebra figure and the comparator figure for PostgreSQL async pooled, SQLSpec PostgreSQL async, and Oracle sync with the relay on. It states whether each Vertebra figure is ahead of or behind its comparator. No row has a pass condition here.
- MySQL, SQL Server, Spanner, and BigQuery figures are reported the same way.
- A figure behind its comparator is reported with the per-layer figures the harness prints, so the cleanup chapter starts its final run knowing where the gap is.
- An engine whose fixture cannot start on the host is reported as not measured, with the error text.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| PostgreSQL async pooled | `capture --engine postgres` | Figure and comparator recorded; ahead or behind stated |
| SQLSpec PostgreSQL async | Same report | Ratio recorded; ahead or behind stated |
| Oracle sync, relay on | `capture --engine oracle` with defaults | Figure and comparator recorded; ahead or behind stated |
| Other engines | `capture` for `mysql`, `mssql`, `spanner`, `bigquery` | Figures recorded or reported as not measured |

## Verification

- `make build`
- `OUT="$(mktemp -d)"`
- `uv run python -m tools.benchmarks.adapters.sql capture --engine postgres --output "$OUT/postgres.json"`
- `uv run python -m tools.benchmarks.adapters.sql capture --engine mysql --output "$OUT/mysql.json"`
- `uv run python -m tools.benchmarks.adapters.sql capture --engine mssql --output "$OUT/mssql.json"`
- `uv run python -m tools.benchmarks.adapters.sql capture --engine oracle --output "$OUT/oracle.json"`
- `uv run python -m tools.benchmarks.adapters.sql capture --engine spanner --output "$OUT/spanner.json"`
- `uv run python -m tools.benchmarks.adapters.sql capture --engine bigquery --output "$OUT/bigquery.json"`
- `git status --porcelain`

## Acceptance Criteria

- [ ] A report exists in the temporary directory for every engine that could be measured, and the task result carries the figures.
- [ ] The task result states both figures for the PostgreSQL rows and the default-config Oracle row.
- [ ] `git status --porcelain` shows no tracked file changed by this task.
