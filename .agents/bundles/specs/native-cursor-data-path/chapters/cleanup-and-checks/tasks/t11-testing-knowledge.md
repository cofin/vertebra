---
type: Task
id: "native-cursor-data-path:cleanup:t11"
chapter_id: cleanup-and-checks
title: "Testing knowledge chapters"
description: "Document the benchmark comparison check, its allow-list with the DuckDB entries, comparator-less cases, the SQLite cursor case, the Oracle relay capture option, and the scoped shared-memory test cleanup."
worksheet_format: compact
status: stable
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "native-cursor-data-path:cleanup:t01"
  - "native-cursor-data-path:cleanup:t03"
  - "native-cursor-data-path:cleanup:t07"
  - "native-cursor-data-path:cleanup:t20"
read_set:
  - ".agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md:109-151"
  - ".agents/bundles/knowledge/testing/polyglot-test-matrix.md:178-186"
  - ".agents/bundles/knowledge/testing/polyglot-test-matrix.md:222-227"
  - "tools/benchmarks/adapters/sql.py:1-100"
  - "tools/benchmarks/adapters/sql.py::capture_sqlite_report"
  - "src/py/tests/conftest.py::_cleanup_shm"
files:
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
tests: []
estimated_loc: 40
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: bench-chapter
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: "Benchmark suite and free-threaded chapter"
  - id: test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: "Polyglot test matrix chapter"
tags: [knowledge, testing, benchmarks, docs]
---

# t11 — Testing knowledge chapters

## Objective & Scope Boundary

Someone running benchmarks or parallel tests finds the new check and the cleanup rule in the testing chapters.[^bench-chapter][^test-matrix]

Out of scope: code, benchmark numbers (t07), and every other chapter.

## Bounded Read Manifest

- `benchmarks-and-free-threaded.md:109-151` for the truth block and section 3 "Standalone Adapter Comparators & Container Fixtures"; the `adapters/sql.py` bullet is at line 147.
- `polyglot-test-matrix.md:178-186` for the truth block (line 182 covers shared-memory cleanup) and `222-227` for section 4; the **SHM Hygiene** bullet is at line 226.
- The allow-list and `COMPARATORLESS_CASES` near the top of `tools/benchmarks/adapters/sql.py`, as they stand when this task runs, for the entries and reasons.
- `capture_sqlite_report` for the `cursor_point_select_1` case as t20 built it.
- `src/py/tests/conftest.py::_cleanup_shm` and `pytest_sessionfinish` as written by t01.

## Contracts & Edge Cases

`benchmarks-and-free-threaded.md`, section 3, `adapters/sql.py` bullet. Add:

1. The `check` subcommand: it reads one or more captured reports, pairs each Vertebra driver with its comparators through `COMPARATOR_PAIRS`, and exits non-zero when a Vertebra median is higher than a comparator median outside the allow-list.
2. The allow-list: name each allow-list key with its reason, as a nested list. Read the keys and reasons from the code. The list has `vertebra-bigquery-sync`, `vertebra-spanner-sync`, and `vertebra-mssql-sync`, where the engine round trip is the floor, and `vertebra-duckdb-sync` and `vertebra-sqlspec-duckdb-sync`, where the bundled DuckDB build has no jemalloc. State that the two DuckDB entries are there by user decision, that both rows are still measured, and that they print `[ALLOW]` or `[PASS]`.
3. Comparator-less cases: `COMPARATORLESS_CASES` maps `(driver, case)` to a reason. A listed Vertebra case with no comparator case in the report prints `[SKIP]` with the reason and does not fail the check. An unlisted one prints `[MISSING]` and fails. Name the entry `("vertebra-async", "pipeline_select_16")`.
4. The flags `--max-median-ratio` (default `1.0`) and `--allow-missing`.
5. The SQLite case `cursor_point_select_1`: `vertebra-sqlite-sync` and `sqlite3` each run `execute` plus `fetchone` on one cursor reused across iterations. State what it is for: it compares the native statement cost like for like, where `point_select_1` also includes each driver's convenience layer.
6. The capture flag `--oracle-reset-relay {on,off}`.
7. One example command:

```bash
uv run python -m tools.benchmarks.adapters.sql check --report sqlite.json --report oracle.json
```

Add one truth-block bullet for the check. Keep the block at 40 lines or fewer.

`polyglot-test-matrix.md`:

1. Rewrite the **SHM Hygiene** bullet at line 226. It states: `conftest.py` pins AnyIO to `asyncio` and generates `test-<uuid8>` names; an xdist worker removes only the names `shm_name` handed out in that test; a single-process run sweeps `/dev/shm/{test-*,vtb-*,*vertebra*}` after each test; the controller sweeps once at session end. Keep the existing advice about targeted `cargo test -p <crate>`.
2. Add the limitation: two separate `pytest` commands running at the same time each act as a controller and can remove each other's segments. Run them one after the other, or use one command with `-n`.
3. Update the truth-block bullet at line 182 to match.

Both files:

- Sources and footnotes match one to one. Add no new source unless a new footnote cites it.
- Add a `verified` entry with the task commit.
- Present tense. No workstation path and no internal tool name.

## Test Matrix

| Check | Expected |
| --- | --- |
| Allow-list keys in the benchmark chapter | Every key in the code is present, including the two DuckDB keys, and no key that is absent from the code |
| `COMPARATORLESS_CASES`, `[SKIP]`, and `cursor_point_select_1` in the benchmark chapter | Present |
| The phrase "uses jemalloc" in the benchmark chapter | No match |
| `--oracle-reset-relay` and `--allow-missing` in the benchmark chapter | Present |
| `PYTEST_XDIST_WORKER` or "xdist worker" in the test matrix chapter | Present |
| `make review-check` | Exit `0` |
| Prose lint from the planning toolkit on each edited file | No finding |

## Verification

```bash
for key in vertebra-bigquery-sync vertebra-spanner-sync vertebra-mssql-sync vertebra-duckdb-sync vertebra-sqlspec-duckdb-sync COMPARATORLESS_CASES cursor_point_select_1; do
  grep -c -- "$key" .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
done
grep -n "uses jemalloc" .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
grep -n -- "--oracle-reset-relay\|--allow-missing\|--max-median-ratio" .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
grep -n "xdist" .agents/bundles/knowledge/testing/polyglot-test-matrix.md
make review-check
```

Each `grep -c` prints `1` or more. The `uses jemalloc` search prints nothing. When the code holds a further user-approved key, run the same `grep -c` for that key.

## Acceptance Criteria

- [x] The benchmark chapter documents `check`, the allow-list with reasons including the two DuckDB entries, `COMPARATORLESS_CASES` with the `[SKIP]` outcome, the `cursor_point_select_1` case, and the three flags.
- [x] The test matrix chapter documents the cleanup rule per process role and the two-command limitation.
- [x] `make review-check` passes and the prose lint reports no finding.

[^bench-chapter]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`
[^test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md`
