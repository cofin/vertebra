---
type: Task
id: "native-postgres-driver:ch29:t02"
chapter_id: ch29-wheel-ci-and-benchmarks
title: Implement reproducible PostgreSQL benchmark capture and offline threshold comparator
description: Add bench-only psycopg 3.2.13 and asyncpg 0.31.0 dependencies with strict [tool.mypy] files and overrides, register postgres_benchmark pytest marker, and implement tools/benchmarks/postgres.py schema_version=1 reference/native capture and offline comparison CLI.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch29:t01"
read_set:
  - pyproject.toml
  - tools/benchmarks/__init__.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - tools/benchmarks/postgres.py
  - pyproject.toml
  - uv.lock
tests:
  - src/py/tests/unit/tools/test_postgres_benchmarks.py
estimated_loc: 245
plan_revision: 3
plan_commit: dd432f18
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: root-pyproject
    resource: pyproject.toml
    title: Workspace dependency groups, pytest markers, default addopts, and Mypy configuration
  - id: bench-pkg
    resource: tools/benchmarks/__init__.py
    title: Vertebra benchmark CLI and reporting package
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public PostgreSQL driver facade benchmarked in native capture mode
  - id: bench-guide
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: Benchmark governance, threshold, and free-threaded testing conventions
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native client, pool, and single-Sync pipeline contracts
tags:
  - postgres
  - benchmarks
  - harness
  - msgspec
---

# Task t02: Implement Reproducible PostgreSQL Benchmark Capture and Offline Threshold Comparator

## Objective & Scope Boundary

Create the deterministic `schema_version=1` PostgreSQL benchmark capture and offline comparison harness in `tools/benchmarks/postgres.py` plus bench-group dependency, strict Mypy target/override, and pytest marker configuration in `pyproject.toml` and `uv.lock` [^root-pyproject] [^bench-pkg] [^pg-driver] [^bench-guide] [^pg-contracts]:
1. Add `[dependency-groups] bench` entries `psycopg[binary]==3.2.13` and `asyncpg==0.31.0` in `pyproject.toml` (refreshing `uv.lock` without broad transitive upgrades), register `postgres_benchmark` in `[tool.pytest.ini_options]` with default `-m "not postgres_benchmark"` so standard `make py-test` runs never invoke live benchmark captures unless `-m postgres_benchmark` is explicitly requested, and update `[tool.mypy]` by adding `"tools/benchmarks/postgres.py"` to `[tool.mypy].files` and adding `"asyncpg"`, `"asyncpg.*"`, `"psycopg"`, `"psycopg.*"` to `[[tool.mypy.overrides]]` (`ignore_missing_imports = true`) so both `uv run --no-sync mypy tools/benchmarks/postgres.py` under `strict = true` and `make lint-all` without `--group bench` exit `0`.
2. Implement `python -m tools.benchmarks.postgres capture --implementation reference|native --dsn-env VERTEBRA_POSTGRES_BENCH_DSN --output <path>` and `python -m tools.benchmarks.postgres compare --reference <reference.json> --native <native.json> --output <comparison.json>` using frozen `msgspec.Struct` (`schema_version=1`) records serialized via `vertebra` `msgspec` helpers.
3. Implement symmetric `reference` and `native` scenario runners:
   - `10` unmeasured warmups per scenario.
   - Cold direct authenticated connect + query probe + close: `100` measured operations grouped as `5` samples of `20` (`psycopg.connect(..., autocommit=True, prepare_threshold=0)` sync, `asyncpg.connect(..., ssl=False)` async vs `SyncPostgresClient` / `AsyncPostgresClient` with `sslmode="disable"`).
   - Warm retained-lease prepared single query `SELECT $1::bigint + 1` (`SELECT %s::bigint + 1` with `prepare=True` on `psycopg`, `connection.prepare` + `fetchval` on `asyncpg`, and warm `SyncPostgresClient.query` / `AsyncPostgresClient.query` on native): `5` samples of `1000` fully consumed queries verifying exact scalar sums.
   - Pipeline `64` parameterized `SELECT` statements/batch × `100` batches/sample × `5` samples inside one explicit ambient transaction + per-batch nested savepoint (`psycopg` sync `conn.pipeline()` and `psycopg.AsyncConnection.pipeline()` vs native `pipeline()`), consuming all `64` results and releasing the savepoint inside the timed window while reporting outer commit/reset and transient pool `acquire + DISCARD ALL` as separate diagnostic scenarios.
4. Implement strict offline `compare`: enforce `native.median_ns <= 1.25 * reference.median_ns`, `native.p95_ns <= 1.50 * reference.p95_ns`, `native.statements_s >= 0.80 * reference.statements_s`, and `coefficient_of_variation <= 0.15` (computed as `stdev / mean` across the `5` per-sample mean per-operation latencies, allowing `retry_count <= 1` whole paired retry before failing closed).

Out of scope: Ephemeral PG18 container orchestration, Criterion Rust benches, and `Makefile` targets (owned by `ch29:t03`).

## Bounded Read Manifest

- [pyproject.toml](../../../../../../../pyproject.toml) — `[dependency-groups]`, `[tool.pytest.ini_options]`, `[tool.mypy].files`, and `[[tool.mypy.overrides]]` [^root-pyproject].
- [tools/benchmarks/__init__.py](../../../../../../../tools/benchmarks/__init__.py) — Existing benchmark module layout [^bench-pkg].
- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Final `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` exports [^pg-driver].
- [.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md](../../../../../knowledge/testing/benchmarks-and-free-threaded.md) — Benchmark reproducibility and threshold rules [^bench-guide].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Raw client, pool, and single-`Sync` pipeline contracts [^pg-contracts].

## Symbol & Signature Contracts

- `pyproject.toml`:
  - `[dependency-groups].bench`: add `"psycopg[binary]==3.2.13"` and `"asyncpg==0.31.0"`.
  - `[tool.mypy].files`: append `"tools/benchmarks/postgres.py"`.
  - `[[tool.mypy.overrides]]` (`ignore_missing_imports = true`): include `"asyncpg"`, `"asyncpg.*"`, `"psycopg"`, `"psycopg.*"`.
- `tools/benchmarks/postgres.py`:
  - Frozen `msgspec.Struct` (`frozen=True, gc=False`) schema (`SCHEMA_VERSION = 1`):
    - `ScenarioCapture`: `name: str`, `gated: bool`, `warmup_count: int`, `sample_groups: int`, `operations_per_sample: int`, `duration_ns: tuple[int, ...]`, `sample_mean_op_ns: tuple[float, ...]`, `median_ns: float`, `p95_ns: float`, `statements_s: float`, `coefficient_of_variation: float`, `verified_checksum: int`.
    - `PostgresBenchmarkReport`: `schema_version: int`, `capture_id: str`, `paired_run_id: str`, `implementation: Literal["reference", "native"]`, `python_version: str`, `platform: str`, `gil_enabled: bool`, `package_versions: dict[str, str]`, `git_sha: str`, `server_version_num: int`, `server_settings: dict[str, str]`, `hardware: dict[str, int | str]`, `transport_mode: str`, `tls_mode: str`, `dataset_fingerprint: str`, `preparation_policy: str`, `reset_policy: str`, `retry_count: int`, `scenarios: tuple[ScenarioCapture, ...]`.
    - `ScenarioComparison` & `PostgresComparisonReport`: per-scenario ratios, pass/fail flags, failure reasons, and overall `passed: bool`.
  - `def compare_reports(reference: PostgresBenchmarkReport, native: PostgresBenchmarkReport) -> PostgresComparisonReport`
  - `def main(argv: Sequence[str] | None = None) -> int`

## Edge Cases & Error Semantics

- `capture` reads the DSN strictly from `os.environ[dsn_env]`, validates CPython 3.12 standard GIL (`gil_enabled is True`), PostgreSQL 18 (`180000 <= server_version_num < 190000`), `sslmode=disable`, and `VERTEBRA_POSTGRES_BENCH_RUN_ID` / `VERTEBRA_POSTGRES_BENCH_SCHEMA`, and scrubs DSN/password/user substrings from every output field and exception string.
- Lazy/deferred imports of `psycopg` and `asyncpg` inside `reference` capture functions combined with `[[tool.mypy.overrides]]` ensure `python -m tools.benchmarks.postgres compare`, `--help`, unit tests, and `make lint-all` succeed cleanly without `--group bench` installed.
- `compare` opens zero network sockets, requires no environment variables, and fails (`passed=False`, exit code `1`) on: `schema_version != 1`, `paired_run_id` / `dataset_fingerprint` / `server_version_num` / `python_version` / `gil_enabled` mismatch, missing gated scenario, zero/negative counts, non-finite or negative durations, `retry_count > 1`, `coefficient_of_variation > 0.15`, secret-bearing metadata (`postgresql://`, `password=`), or any `median_ns > 1.25x`, `p95_ns > 1.50x`, or `statements_s < 0.80x` breach (exact boundary equality `1.25`, `1.50`, `0.80`, `0.15` passes).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/tools/test_postgres_benchmarks.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Exact threshold equality vs single-metric regression | Synthetic `schema_version=1` `reference` & `native` reports at `1.25x` median, `1.50x` p95, `0.80x` throughput, `0.15` CV vs `1.2501x` / `1.5001x` / `0.7999x` | Boundary equality exits `0` (`passed=True`); each individual metric breach exits `1` naming the failing scenario and metric. |
| Mismatched paired identity, server, GIL, or missing scenario | `paired_run_id`, `dataset_fingerprint`, `server_version_num`, or `gil_enabled` differs, or one gated scenario omitted | `compare` exits `1` with explicit comparability error; never ignores unmatched scenarios. |
| Noisy capture (`CV > 0.15`) and retry exhaustion | Sample-group means with `stdev / mean = 0.19` or `retry_count = 2` | Marked invalid (`passed=False`, exit `1`); individual slow latencies are never discarded. |
| Secret-bearing metadata, non-finite samples, & Mypy without `--group bench` | Report metadata containing `postgresql://bench:secret@` or `duration_ns=()` / `NaN`, plus `mypy` under default dev sync | Rejected immediately without echoing secrets; `mypy tools/benchmarks/postgres.py` and `make lint-all` exit `0` without `--group bench`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q`
- Final Green + lock + type/lint gate: `uv lock --check && uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q && uv run --no-sync python -m tools.benchmarks.postgres --help && uv run --no-sync ruff check tools/benchmarks/postgres.py src/py/tests/unit/tools/test_postgres_benchmarks.py && uv run --no-sync mypy tools/benchmarks/postgres.py && make lint-all`

## Acceptance Criteria

- [ ] `pyproject.toml` and `uv.lock` pin bench-only `psycopg[binary]==3.2.13` and `asyncpg==0.31.0`, register `"tools/benchmarks/postgres.py"` in `[tool.mypy].files` and `"asyncpg*"` / `"psycopg*"` in `[[tool.mypy.overrides]]`, and exclude `postgres_benchmark` from default `pytest` runs so `mypy tools/benchmarks/postgres.py` (`strict = true`) and `make lint-all` without `--group bench` exit `0` (R27).
- [ ] `tools/benchmarks/postgres.py` implements `schema_version=1` `capture` (`reference` and `native`) and offline `compare` with deterministic unit coverage of every threshold, noise, comparability, and secret-scrubbing rule (R27).

## Notes & Discoveries

- [2026-11-07 16:45] `psycopg` (`3.2.13`) provides both sync and `AsyncConnection.pipeline()` reference measurements because `asyncpg` (`0.31.0`) exposes no explicit single-`Sync` transactional pipeline context on its public API.

[^root-pyproject]: `pyproject.toml` — Workspace dependency groups, pytest markers, default `addopts`, and Mypy configuration.
[^bench-pkg]: `tools/benchmarks/__init__.py` — Vertebra benchmark CLI and reporting package.
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL driver facade benchmarked in native capture mode.
[^bench-guide]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Benchmark governance, threshold, and free-threaded testing conventions.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native client, pool, and single-`Sync` pipeline contracts.
