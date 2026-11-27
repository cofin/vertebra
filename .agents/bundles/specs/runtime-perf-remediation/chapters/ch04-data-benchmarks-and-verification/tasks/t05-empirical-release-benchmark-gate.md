---
type: Task
id: "runtime-perf-remediation:ch04:t05"
chapter_id: ch04-data-benchmarks-and-verification
title: Run release build and live empirical benchmark gate
description: Build release artifacts and execute the live Granian HTTP/1.1 comparison, Embedded 1w Tokio A/B check, SQLite full-stack and standalone suites, and Docker PostgreSQL full-stack and standalone suites.
status: stable
state: pending
priority: P1
verification_strategy: integration_acceptance
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch04:t04"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/spec.md
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
  - tools/benchmarks/fixtures.py
  - tools/benchmarks/targets.py
files: []
tests: []
estimated_loc: 0
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-root-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/spec.md
    title: Root flow acceptance scenarios and baseline run 20261126-213834-f73052
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 empirical verification matrix and replan triggers
  - id: src-bench-fixtures
    resource: tools/benchmarks/fixtures.py::FixtureLauncher
    title: Local postgres:17-alpine Docker fixture launcher
tags:
  - benchmarks
  - verification
  - granian
  - sqlite
  - postgres
---

# Run Release Build and Live Empirical Benchmark Gate

## Objective & Scope Boundary

Build release artifacts via `make build`, run the full verification checks, and capture live benchmark evidence across HTTP/1.1, SQLite, and PostgreSQL matrices [^src-ch04-spec].

Validate every acceptance target in root `spec.md` Section 2.5 and evaluate the chapter `replan_trigger` before syncing OKF knowledge chapters [^src-root-spec].

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/spec.md`
- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md`
- `tools/benchmarks/fixtures.py`
- `tools/benchmarks/targets.py`

## Contracts & Edge Cases

- Execute release build (`make build`) before collecting any latency or RPS numbers; never benchmark debug wheels.
- Run Suite A (HTTP/1.1 vs Granian):
  - Targets: `granian-asgi,granian-rsgi,vtb-embedded-asgi,vtb-embedded-rsgi,vtb-embedded-native,vtb-embedded-wsgi,vtb-shm-asgi-1w,vtb-shm-native-1w,vtb-shm-wsgi-4w,vtb-embedded-native-4w,vtb-shm-native-4w` with `-s default`.
  - Run Embedded 1w A/B comparison with `VERTEBRA_TOKIO_THREADS=1` versus `VERTEBRA_TOKIO_THREADS=2` on `vtb-embedded-native`, `vtb-embedded-asgi`, and `vtb-embedded-rsgi`.
- Run Suite B (SQLite full-stack & standalone):
  - Full-stack HTTP with `VTB_BENCH_DB_BACKEND=sqlite` and a temporary WAL file path across `native`, `sqlspec`, and `advanced_alchemy` targets plus Granian/Uvicorn baselines.
  - Standalone comparators via `tools.benchmarks.adapters.sql capture`: `--engine sqlite`, `--engine sqlspec --sqlspec-engine sqlite`, and `--engine advanced_alchemy --aa-engine sqlite`, followed by `check`.
- Run Suite C (PostgreSQL full-stack & standalone):
  - Start `postgres:17-alpine` using `FixtureLauncher(get_fixture_spec("postgres"))` [^src-bench-fixtures] and poll `SELECT 1` until post-`initdb` startup completes.
  - Full-stack HTTP with `VTB_BENCH_DB_BACKEND=postgres` across `native` (per-worker vs SHM broker), `sqlspec`, and `advanced_alchemy` targets plus Granian/Uvicorn baselines.
  - Standalone comparators: `make postgres-bench`, `--engine sqlspec --sqlspec-engine postgres`, and `--engine advanced_alchemy --aa-engine postgres`, followed by `check`.
- If Docker is unavailable, report the exact blocker immediately rather than fabricating PostgreSQL numbers.
- Evaluate `replan_trigger`: if `VERTEBRA_TOKIO_THREADS=2` materially outperforms `1`, if `WsgiExecutor` misses `>= 10,000 RPS` on `io_10ms` or `>= 200k RPS` on `hello`, or if SQLSpec/Advanced Alchemy/Postgres show an unexpected regression, trigger a targeted spec revision before running `t06` and `t07`.

## Test Matrix

| Suite | Acceptance Bar | Evidence Artifact |
| :--- | :--- | :--- |
| WSGI concurrency | `>= 10,000 RPS` on `io_10ms` and `>= 200k RPS` on `hello` | `benchmarks.duckdb` run ID |
| Embedded 1w parity | Beats `granian-asgi` / `granian-rsgi` on `default` with `0` errors | `benchmarks.duckdb` + Tokio 1 vs 2 table |
| Multi-worker loopback | Improves peak 4w RPS and P99 over `20261126-213834-f73052` | `make bench-diff` output |
| SQLite & Postgres data | Zero HTTP errors across `native`, `sqlspec`, `advanced_alchemy` | `benchmarks.duckdb` + adapter JSON reports |
| Repo gates | `make lint-all` and `make rs-test` green | Command exit status `0` |

## Verification

```bash
make build
make lint-all
make rs-test
uv run --no-sync python -m tools.benchmarks.cli run -t granian-asgi,granian-rsgi,vtb-embedded-asgi,vtb-embedded-rsgi,vtb-embedded-native,vtb-embedded-wsgi,vtb-shm-asgi-1w,vtb-shm-native-1w,vtb-shm-wsgi-4w,vtb-embedded-native-4w,vtb-shm-native-4w -s default
uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlite --dsn :memory: --output /tmp/bench-sqlite.json
uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine sqlspec --sqlspec-engine sqlite --dsn :memory: --output /tmp/bench-sqlspec-sqlite.json
uv run --no-sync python -m tools.benchmarks.adapters.sql capture --engine advanced_alchemy --aa-engine sqlite --dsn :memory: --output /tmp/bench-aa-sqlite.json
uv run --no-sync python -m tools.benchmarks.adapters.sql check --report /tmp/bench-sqlite.json --report /tmp/bench-sqlspec-sqlite.json --report /tmp/bench-aa-sqlite.json
```

## Acceptance Criteria

- Release build, `make lint-all`, and `make rs-test` pass with zero failures.
- Suites A, B, and C complete with recorded run IDs and JSON comparator reports, and all root `spec.md` Section 2.5 acceptance scenarios pass.
- Replan trigger decisions (Tokio 1 vs 2, WSGI threshold, DB hot paths) are explicitly resolved with empirical measurements.

[^src-root-spec]: `.agents/bundles/specs/runtime-perf-remediation/spec.md` — Root flow acceptance scenarios and baseline run 20261126-213834-f73052.
[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 empirical verification matrix and replan triggers.
[^src-bench-fixtures]: `tools/benchmarks/fixtures.py::FixtureLauncher` — Local postgres:17-alpine Docker fixture launcher.
