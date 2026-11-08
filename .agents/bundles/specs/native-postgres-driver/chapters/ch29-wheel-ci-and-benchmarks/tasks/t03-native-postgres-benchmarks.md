---
type: Task
id: "native-postgres-driver:ch29:t03"
chapter_id: ch29-wheel-ci-and-benchmarks
title: Wire Criterion native benchmarks, live PG18 paired orchestrator, and Makefile targets
description: Add Criterion benches/postgres_native.rs in vertebra-postgres, implement the @pytest.mark.postgres_benchmark PG18 paired capture/compare orchestrator, and register postgres-test, postgres-wheel-check, postgres-bench, and postgres-bench-check in Makefile.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch29:t02"
read_set:
  - Makefile
  - src/py/tests/integration/conftest.py
  - src/rs/vertebra-db/src/connection.rs
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/benches/postgres_native.rs
  - Makefile
tests:
  - src/py/tests/integration/db/test_postgres_benchmarks.py
estimated_loc: 235
plan_revision: 8
plan_commit: 8c540809
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: root-makefile
    resource: Makefile
    title: Workspace test, wheel, and benchmark orchestration targets
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Docker PostgreSQL 18 service and clientless psql setup helpers
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Native Rust TCP transport underlying Criterion postgres_native benchmarks
  - id: bench-guide
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: Criterion and Python benchmark execution rules
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native client, pipeline, and VTBARROW cursor contracts
tags:
  - postgres
  - benchmarks
  - criterion
  - makefile
---

# Task t03: Wire Criterion Native Benchmarks, Live PG18 Paired Orchestrator, and `Makefile` Targets

## Objective & Scope Boundary

Connect Criterion Rust micro/session benchmarks, live PostgreSQL 18 paired reference/native benchmark orchestration, and canonical `Makefile` targets across `src/rs/vertebra-postgres/Cargo.toml`, `src/rs/vertebra-postgres/benches/postgres_native.rs`, `Makefile`, and `src/py/tests/integration/db/test_postgres_benchmarks.py` [^root-makefile] [^int-conftest] [^pg-conn-rs] [^bench-guide] [^pg-contracts]:
1. Register `[[bench]] name = "postgres_native" harness = false` (with workspace `criterion`) in `src/rs/vertebra-postgres/Cargo.toml` and implement `src/rs/vertebra-postgres/benches/postgres_native.rs` benchmarking real Rust cold authenticated connect+close, warm prepared `SELECT $1::bigint + 1`, and 64-statement single-`Sync` pipeline batches against `VERTEBRA_POSTGRES_BENCH_DSN` (compiling cleanly under `--no-run` when no live DSN is supplied and executing real database I/O when `VERTEBRA_POSTGRES_BENCH_DSN` is set).
2. Implement `src/py/tests/integration/db/test_postgres_benchmarks.py` (`@pytest.mark.postgres_benchmark`) owning a function-scoped `postgres_18_service` container (`sslmode="disable"`) and uniquely named schema (`bench_<hex>`):
   - Populate `bench_values(id BIGINT PRIMARY KEY, payload TEXT)` with `100_000` deterministic rows via clientless `psql`, compute and record the deterministic dataset checksum (`dataset_fingerprint`), and reset/re-verify before each capture.
   - Pass `VERTEBRA_POSTGRES_BENCH_DSN`, `VERTEBRA_POSTGRES_BENCH_RUN_ID`, and `VERTEBRA_POSTGRES_BENCH_SCHEMA` exclusively to child `python -m tools.benchmarks.postgres capture --implementation reference` and `--implementation native` subprocesses writing `artifacts/postgres/reference.json` and `artifacts/postgres/native.json` (retrying the whole paired capture at most once if `CV > 0.15`).
   - Run `python -m tools.benchmarks.postgres compare` producing `artifacts/postgres/comparison.json`, assert `passed is True` with zero missing gated scenarios, execute the diagnostic `100_000`-row native `select_to_arrow` read verifying typed schema/row count/checksum, and drop the owned schema and close all connections in `finally`.
3. Add four canonical `.PHONY` targets in `Makefile`:
   - `postgres-test`: runs `make dev` and strict `VERTEBRA_CONFORMANCE_DOCKER=1` pytest across `test_driver_conformance.py -k postgres`, `test_postgres_native.py`, `test_postgres_replication.py`, `test_postgres_services.py`, and `test_postgres_features.py`.
   - `postgres-wheel-check`: runs `uv run --no-sync python tools/scripts/verify_postgres_wheel.py`.
   - `postgres-bench`: builds release extensions (`make build` with Python 3.12 + `--group bench`) and runs `UV_PYTHON=3.12 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_benchmarks.py -q -s -m postgres_benchmark`.
   - `postgres-bench-check`: runs offline `uv run --no-sync python -m tools.benchmarks.postgres compare --reference artifacts/postgres/reference.json --native artifacts/postgres/native.json --output artifacts/postgres/comparison.json` without opening any network connection, keeping existing `rs-bench` and `bench-check` intact for core/HTTP coverage.

Out of scope: Modifying `tools/benchmarks/postgres.py` comparator thresholds (`ch29:t02`) or user/OKF Markdown guides (`ch30:t01`–`t04`).

## Bounded Read Manifest

- [Makefile](../../../../../../../Makefile) — Existing `dev`, `build`, `rs-bench`, `bench`, and `bench-check` target conventions [^root-makefile].
- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `postgres_18_service` and clientless `psql` fixture helpers [^int-conftest].
- [src/rs/vertebra-db/src/connection.rs](../../../../../../../src/rs/vertebra-db/src/connection.rs) — Transport options consumed by `vertebra-postgres` sessions [^pg-conn-rs].
- [.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md](../../../../../knowledge/testing/benchmarks-and-free-threaded.md) — Release build and benchmark execution discipline [^bench-guide].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Native query, pipeline, and `ArrowCursor` contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/Cargo.toml` & `src/rs/vertebra-postgres/benches/postgres_native.rs`:
  - `criterion_group!(benches, bench_cold_connect, bench_warm_prepared_single, bench_pipeline_64)` and `criterion_main!(benches)`.
- `src/py/tests/integration/db/test_postgres_benchmarks.py`:
  - `@pytest.mark.postgres_benchmark def test_postgres_paired_reference_vs_native_benchmarks(...) -> None`
- `Makefile`:
  - `.PHONY` additions: `postgres-test`, `postgres-wheel-check`, `postgres-bench`, `postgres-bench-check`.

## Edge Cases & Error Semantics

- `test_postgres_benchmarks.py` never compares against stale historical JSON reports or synthetic baselines; every `make postgres-bench` run generates fresh paired `reference.json` and `native.json` sharing one `paired_run_id`, `postgres_18_service`, and `dataset_fingerprint`.
- Write samples inside benchmarks execute strictly in rollback/savepoint scopes, and fixture teardown drops the generated schema and closes all raw clients and pools even on threshold failure.
- `make postgres-bench-check` executes with `VERTEBRA_POSTGRES_BENCH_DSN` unset and fails closed if `artifacts/postgres/{reference,native}.json` are missing, mismatched, noisy (`CV > 0.15`), or outside the `1.25x` / `1.50x` / `0.80x` bounds.

## Public-Seam Test Blueprint

- **Canonical test / bench files**: `src/py/tests/integration/db/test_postgres_benchmarks.py` and `src/rs/vertebra-postgres/benches/postgres_native.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Live PG18 paired capture + offline threshold check | `make postgres-bench && make postgres-bench-check` against `postgres_18_service` (`100_000`-row `bench_values`) | `reference.json`, `native.json`, and `comparison.json` written without credentials; `median <= 1.25x`, `p95 <= 1.50x`, `statements_s >= 0.80x`, `CV <= 0.15` (`passed=True`). |
| 100k-row `VTBARROW` diagnostic read | `SyncPostgreSQLDriver.select_to_arrow("SELECT id, payload FROM <schema>.bench_values ORDER BY id")` | `row_count == 100_000`, `used_shm is True`, typed schema `(Int64, Utf8)` matches deterministic checksum. |
| Criterion native Rust bench compilation & live execution | `cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --no-run` (plus live DSN smoke) | Compiles with zero warnings and executes real wire sessions when `VERTEBRA_POSTGRES_BENCH_DSN` is provided. |
| Default `pytest` exclusion | `uv run --no-sync pytest src/py/tests/integration/db/test_postgres_benchmarks.py -q` (without `-m postgres_benchmark`) | Deselected cleanly by default `addopts` so fast test suites do not run multi-sample benchmarks. |

## Verification

- Compile & unit/lint check: `cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --no-run && uv run --no-sync ruff check src/py/tests/integration/db/test_postgres_benchmarks.py`
- Full live benchmark & Makefile target gate: `make build && uv sync --python 3.12 --all-extras --dev --group bench && UV_PYTHON=3.12 UV_NO_SYNC=1 make build && make postgres-wheel-check && make postgres-test && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check`

## Acceptance Criteria

- [ ] `src/rs/vertebra-postgres/benches/postgres_native.rs` and `src/py/tests/integration/db/test_postgres_benchmarks.py` measure real release-built native PostgreSQL connect, prepared single-query, and 64-statement pipeline workloads against pinned `psycopg` 3.2.13 and `asyncpg` 0.31.0 references on live PG18 (R27).
- [ ] `Makefile` exposes `postgres-test`, `postgres-wheel-check`, `postgres-bench`, and `postgres-bench-check` alongside intact core/HTTP `rs-bench` and `bench-check` gates (R25, R26, R27).

## Notes & Discoveries

- [2026-11-07 16:45] `UV_NO_SYNC=1` preserves the installed `--group bench` environment across `make build` and `make postgres-bench` invocations without stripping optional wheels.

[^root-makefile]: `Makefile` — Workspace test, wheel, and benchmark orchestration targets.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Docker PostgreSQL 18 service and clientless `psql` setup helpers.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Native Rust TCP transport underlying Criterion `postgres_native` benchmarks.
[^bench-guide]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Criterion and Python benchmark execution rules.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native client, pipeline, and `VTBARROW` cursor contracts.
