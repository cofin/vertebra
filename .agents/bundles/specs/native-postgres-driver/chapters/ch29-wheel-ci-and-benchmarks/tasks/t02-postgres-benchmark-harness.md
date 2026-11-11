---
type: Task
id: "native-postgres-driver:ch29:t02"
chapter_id: ch29-wheel-ci-and-benchmarks
title: Implement reproducible PostgreSQL benchmark capture and offline threshold comparator
description: Add Python 3.12-scoped [dependency-groups].postgres-bench comparators, bumpversion entries for vertebra-postgres, strict Mypy/pytest config, and clean msgspec schema_version=1 capture/compare CLI in tools/benchmarks/postgres.py.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch29:t01"
read_set:
  - pyproject.toml
  - tools/benchmarks/micro.py
  - src/py/vertebra/utils/serializers/__init__.py
  - src/py/vertebra/utils/module_loader.py
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - tools/benchmarks/postgres.py
  - pyproject.toml
  - uv.lock
tests:
  - src/py/tests/unit/tools/test_postgres_benchmarks.py
estimated_loc: 245
plan_revision: 6
plan_commit: 043d8444
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 043d8444
sources:
  - id: root-pyproject
    resource: pyproject.toml
    title: Dedicated postgres-bench dependency group, bumpversion rules, pytest markers, and Mypy settings
  - id: bench-micro
    resource: tools/benchmarks/micro.py
    title: Existing Vertebra benchmark module structure and msgspec sample patterns
  - id: vtb-serializers
    resource: src/py/vertebra/utils/serializers/__init__.py::encode_json
    title: Centralized decode_json, encode_json, and format_json serializer helpers
  - id: vtb-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Top-level optional module resolution without function-scoped nested imports
  - id: bench-guide
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: Benchmark reproducibility, free-threaded compatibility, and threshold governance
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native client, pool, and single-Sync pipeline benchmark contracts
tags:
  - postgres
  - benchmarks
  - harness
  - msgspec
---

# Task t02: Implement Reproducible PostgreSQL Benchmark Capture and Offline Threshold Comparator

## Objective & Scope Boundary

Create the deterministic `schema_version=1` PostgreSQL benchmark capture and offline comparison harness in `tools/benchmarks/postgres.py` alongside universal-lock-safe comparator dependency, `bumpversion`, pytest marker, and strict Mypy settings in `pyproject.toml` and `uv.lock` [^root-pyproject] [^bench-micro] [^vtb-serializers] [^vtb-loader] [^bench-guide] [^pg-contracts]:
1. Update `pyproject.toml` and refresh `uv.lock` (preserving `3.12`–`3.15t` universal lock compatibility without broad transitive upgrades):
   - Add dedicated `[dependency-groups].postgres-bench = ["psycopg[binary]==3.2.13; python_version == '3.12'", "asyncpg==0.31.0; python_version == '3.12'"]` (leaving `[dependency-groups].bench`, `[project.dependencies]`, and `[project.optional-dependencies]` free of `psycopg`/`asyncpg` so `make bench`, `make bench-ft` on `3.14t`/`3.15t`, and universal `uv lock` all succeed cleanly).
   - Register two `[[tool.bumpversion.files]]` entries syncing `vertebra-postgres` on `make release` / `make pre-release`: (a) `filename = "pyproject.toml"`, `search = 'vertebra-postgres=={current_version}'`, `replace = 'vertebra-postgres=={new_version}'` (updating both `[postgres]` and `[all]` extras), and (b) `filename = "src/rs/vertebra-postgres/pyproject.toml"`, `search = 'version = "{current_version}"'`, `replace = 'version = "{new_version}"'`.
   - Register `postgres_benchmark` in `[tool.pytest.ini_options].markers` and set `addopts = "-m 'not postgres_benchmark'"` so default `make py-test` and `make coverage` skip live multi-sample captures unless `-m postgres_benchmark` is selected.
   - Append `"tools/benchmarks/postgres.py"` to `[tool.mypy].files` and add `"asyncpg"`, `"asyncpg.*"`, `"psycopg"`, `"psycopg.*"` to `[[tool.mypy.overrides]]` (`ignore_missing_imports = true`) so `uv run --no-sync mypy tools/benchmarks/postgres.py` (`strict = true`) and `make lint-all` without `--group postgres-bench` exit `0`.
2. Implement `tools/benchmarks/postgres.py` (`capture --implementation reference|native --dsn-env VERTEBRA_POSTGRES_BENCH_DSN --output <path>` and offline `compare --reference <reference.json> --native <native.json> --output <comparison.json>`) adhering to strict Python organization rules:
   - Module docstring, grouped top-level imports, module-level `if TYPE_CHECKING:` imports for `asyncpg` and `psycopg`, explicit `__all__`, zero function-scoped nested `import` statements (loading `psycopg`, `asyncpg`, and `vertebra._postgres` dynamically via `vertebra.utils.module_loader.ensure_module` inside class methods), centralized JSON encoding/decoding strictly through `vertebra.utils.serializers.{decode_json, encode_json, format_json}` (zero direct `json.*` or `msgspec.json.*` calls), CLI terminal writes strictly through `sys.stdout.write` / `sys.stderr.write` (zero `print()`), structured domain classes (`PostgresBenchmarkRunner`, `PostgresBenchmarkComparator` — zero single-caller standalone module-level `_...` functions), and zero inline `#` comments.
   - Apply `gc=False` strictly on pure scalar/tuple leaf structs (`ScenarioCapture`, `ScenarioComparison`), and **omit `gc=False`** on `dict`/container-holding report envelopes (`PostgresBenchmarkReport` holding `package_versions: dict[str, str]`, `server_settings: dict[str, str]`, `hardware: dict[str, int | str]`, and `PostgresComparisonReport`).
3. Implement symmetric `reference` and `native` scenario measurements (`10` unmeasured warmups per scenario):
   - Cold direct authenticated connect + query probe + close: `5` samples of `20` operations (`psycopg.connect(..., autocommit=True, prepare_threshold=0)` sync and `asyncpg.connect(..., ssl=False)` async vs `SyncPostgresClient` and `AsyncPostgresClient` with `sslmode="disable"`).
   - Warm retained-lease prepared single query (`SELECT %s::bigint + 1` with `prepare=True` on `psycopg`, `connection.prepare("SELECT $1::bigint + 1")` + `fetchval` on `asyncpg`, and warm `SyncPostgresClient.query` / `AsyncPostgresClient.query` on native): `5` samples of `1000` fully consumed queries verifying exact scalar checksums.
   - Pipeline `64` parameterized `SELECT` statements/batch x `100` batches/sample x `5` samples inside one explicit ambient transaction plus per-batch nested savepoint (`psycopg` sync `conn.pipeline()` and `psycopg.AsyncConnection.pipeline()` vs native `SyncPostgreSQLDriver.pipeline()` and `AsyncPostgreSQLDriver.pipeline()`), consuming all `64` results and releasing the savepoint inside the timed window while recording outer commit/reset and transient pool `acquire + DISCARD ALL` as separate non-gated diagnostic scenarios.
4. Implement strict offline `compare_reports` enforcing `native.median_ns <= 1.25 * reference.median_ns`, `native.p95_ns <= 1.50 * reference.p95_ns`, `native.statements_s >= 0.80 * reference.statements_s`, and `coefficient_of_variation <= 0.15` (`stdev / mean` across the `5` per-sample mean per-op latencies, `retry_count <= 1`), tested thoroughly via `@pytest.mark.parametrize` in `src/py/tests/unit/tools/test_postgres_benchmarks.py`.

Out of scope: Ephemeral PG18 container orchestration, Criterion Rust benches, and `Makefile` targets (owned by `ch29:t03`).

## Bounded Read Manifest

- [pyproject.toml](../../../../../../../pyproject.toml) — `[dependency-groups]`, `[[tool.bumpversion.files]]`, `[tool.pytest.ini_options]`, and `[tool.mypy]` [^root-pyproject].
- [tools/benchmarks/micro.py](../../../../../../../tools/benchmarks/micro.py) — Existing Vertebra benchmark package conventions [^bench-micro].
- [src/py/vertebra/utils/serializers/__init__.py](../../../../../../../src/py/vertebra/utils/serializers/__init__.py) — `decode_json`, `encode_json`, and `format_json` serializer utilities [^vtb-serializers].
- [src/py/vertebra/utils/module_loader.py](../../../../../../../src/py/vertebra/utils/module_loader.py) — `ensure_module` optional import helper [^vtb-loader].
- [.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md](../../../../../knowledge/testing/benchmarks-and-free-threaded.md) — Benchmark reproducibility and free-threaded rules [^bench-guide].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Native client, pool, and single-`Sync` pipeline contracts [^pg-contracts].

## Symbol & Signature Contracts

- `pyproject.toml`:
  - `[dependency-groups].postgres-bench = ["psycopg[binary]==3.2.13; python_version == '3.12'", "asyncpg==0.31.0; python_version == '3.12'"]`.
  - `[[tool.bumpversion.files]]` entries for `pyproject.toml` (`vertebra-postgres=={current_version}`) and `src/rs/vertebra-postgres/pyproject.toml` (`version = "{current_version}"`).
  - `[tool.mypy].files`: append `"tools/benchmarks/postgres.py"`; `[[tool.mypy.overrides]]`: append `"asyncpg"`, `"asyncpg.*"`, `"psycopg"`, `"psycopg.*"`.
- `tools/benchmarks/postgres.py`:
  - `SCHEMA_VERSION: Final[int] = 1`
  - `class ScenarioCapture(msgspec.Struct, frozen=True, gc=False, kw_only=True):` (`name: str`, `gated: bool`, `warmup_count: int`, `sample_groups: int`, `operations_per_sample: int`, `duration_ns: tuple[int, ...]`, `sample_mean_op_ns: tuple[float, ...]`, `median_ns: float`, `p95_ns: float`, `statements_s: float`, `coefficient_of_variation: float`, `verified_checksum: int`)
  - `class PostgresBenchmarkReport(msgspec.Struct, frozen=True, kw_only=True):` (omits `gc=False`; fields: `schema_version: int`, `capture_id: str`, `paired_run_id: str`, `implementation: Literal["reference", "native"]`, `python_version: str`, `platform: str`, `gil_enabled: bool`, `package_versions: dict[str, str]`, `git_sha: str`, `server_version_num: int`, `server_settings: dict[str, str]`, `hardware: dict[str, int | str]`, `transport_mode: str`, `tls_mode: str`, `dataset_fingerprint: str`, `preparation_policy: str`, `reset_policy: str`, `retry_count: int`, `scenarios: tuple[ScenarioCapture, ...]`)
  - `class ScenarioComparison(msgspec.Struct, frozen=True, gc=False, kw_only=True):` (scalar/tuple per-scenario ratios, `passed: bool`, `failure_reasons: tuple[str, ...]`)
  - `class PostgresComparisonReport(msgspec.Struct, frozen=True, kw_only=True):` (omits `gc=False`; overall `schema_version: int`, `paired_run_id: str`, `passed: bool`, `failure_reasons: tuple[str, ...]`, `scenarios: tuple[ScenarioComparison, ...]`)
  - `def compare_reports(reference: PostgresBenchmarkReport, native: PostgresBenchmarkReport) -> PostgresComparisonReport`
  - `def main(argv: Sequence[str] | None = None) -> int`

## Edge Cases & Error Semantics

- `capture` reads the target DSN exclusively from `os.environ[dsn_env]`, enforces CPython 3.12 standard GIL (`gil_enabled is True`), PostgreSQL 18 (`180000 <= server_version_num < 190000`), `sslmode=disable`, and non-empty `VERTEBRA_POSTGRES_BENCH_RUN_ID` / `VERTEBRA_POSTGRES_BENCH_SCHEMA`, and strips DSN/password/user substrings from every report field and `sys.stderr.write` diagnostic.
- Running `capture --implementation reference` when `postgres-bench` is not installed raises a clean `MissingDependencyError` via `ensure_module("psycopg", package="psycopg[binary]")` / `ensure_module("asyncpg", package="asyncpg")` without module-import failures during `compare`, `--help`, unit tests, `make lint-all`, or `make bench-ft`.
- `compare` opens zero sockets, reads zero DSN env vars, decodes strictly via `decode_json(..., type_=PostgresBenchmarkReport)`, and fails (`passed=False`, exit `1`) whenever `schema_version` differs from `1`, `implementation` labels are wrong, `paired_run_id` / `dataset_fingerprint` / `server_version_num` / `python_version` / `gil_enabled` / `transport_mode` / `tls_mode` mismatch, any required gated scenario is absent, sample counts/durations are empty/negative/non-finite, `retry_count > 1`, `coefficient_of_variation > 0.15`, metadata leaks credential markers (`postgresql://`, `postgres://`, `password=`), or `median_ns > 1.25x`, `p95_ns > 1.50x`, or `statements_s < 0.80x` (exact boundary equality `1.25`, `1.50`, `0.80`, and `0.15` passes).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/tools/test_postgres_benchmarks.py` (using `@pytest.mark.parametrize` across every threshold, comparability, sample-validity, and redaction case — zero test-body `for` loops over case tables and zero nested imports).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Parameterized threshold boundaries vs single-metric breaches | `@pytest.mark.parametrize` over exact `1.25x` median / `1.50x` p95 / `0.80x` throughput / `0.15` CV vs `1.2501x`, `1.5001x`, `0.7999x`, and `0.1501` CV | Exact boundaries pass (`passed=True`, CLI exit `0`); each breach exits `1` naming the exact scenario and breached metric via `sys.stderr.write`. |
| Parameterized comparability & retry guards | `@pytest.mark.parametrize` varying `schema_version`, `paired_run_id`, `dataset_fingerprint`, `server_version_num`, `python_version`, `gil_enabled`, `retry_count=2`, and omitted gated scenario | Each exits `1` (`passed=False`) reporting the exact comparability mismatch without skipping unmatched gated scenarios. |
| Parameterized secret-scrubbing, invalid samples, & GC tracking | `@pytest.mark.parametrize` over `postgresql://bench:secret@host`, `password=secret`, `duration_ns=()`, `NaN`/`inf`, negative durations, plus `gc.is_tracked` check on `PostgresBenchmarkReport` | Credential substrings rejected without echoing secrets; invalid samples fail closed; `PostgresBenchmarkReport` participates cleanly in Python GC (`gc=False` omitted on container structs). |
| Universal lock, `bumpversion --dry-run`, & Mypy without `postgres-bench` | `uv lock --check`, `uv run --no-sync bump-my-version bump --dry-run patch`, and `uv run --no-sync mypy tools/benchmarks/postgres.py` | Lockfile resolves across `3.12`–`3.15t`; dry-run bump updates `vertebra-postgres` in root and crate `pyproject.toml`; strict Mypy passes without `--group postgres-bench`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q`
- Final Green + lock + bumpversion + lint/typecheck gate:
  ```bash
  uv lock --check
  uv run --no-sync bump-my-version bump --dry-run --allow-dirty patch
  uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q
  uv run --no-sync python -m tools.benchmarks.postgres --help
  uv run --no-sync ruff check tools/benchmarks/postgres.py src/py/tests/unit/tools/test_postgres_benchmarks.py
  uv run --no-sync ruff format --check tools/benchmarks/postgres.py src/py/tests/unit/tools/test_postgres_benchmarks.py
  uv run --no-sync mypy tools/benchmarks/postgres.py
  make lint-all
  ```

## Acceptance Criteria

- [ ] `pyproject.toml` and `uv.lock` isolate CPython 3.12 comparators in `[dependency-groups].postgres-bench` with `; python_version == '3.12'` markers, register `[[tool.bumpversion.files]]` for `vertebra-postgres` in both `pyproject.toml` and `src/rs/vertebra-postgres/pyproject.toml`, configure `[tool.mypy]` targets/overrides, and exclude `postgres_benchmark` from default `pytest` runs while keeping `3.12`–`3.15t` locking and `make bench-ft` intact (R27).
- [ ] `tools/benchmarks/postgres.py` implements clean class-structured `schema_version=1` `capture` and offline `compare` using `gc`-safe `msgspec.Struct` envelopes, top-level `if TYPE_CHECKING:` + `ensure_module`, `vertebra.utils.serializers`, and `sys.stdout.write`/`sys.stderr.write`, verified by `@pytest.mark.parametrize` tests in `src/py/tests/unit/tools/test_postgres_benchmarks.py` (R27).

## Notes & Discoveries

- `psycopg` (`3.2.13`) supplies both sync `conn.pipeline()` and async `AsyncConnection.pipeline()` single-`Sync` reference measurements alongside `asyncpg` (`0.31.0`) async cold-connect and single-prepared-query baselines because `asyncpg` does not expose an explicit transactional single-`Sync` pipeline context on its public Python API [^bench-guide] [^pg-contracts].

[^root-pyproject]: `pyproject.toml` — Dedicated postgres-bench dependency group, bumpversion rules, pytest markers, and Mypy settings.
[^bench-micro]: `tools/benchmarks/micro.py` — Existing Vertebra benchmark module structure and msgspec sample patterns.
[^vtb-serializers]: `src/py/vertebra/utils/serializers/__init__.py::encode_json` — Centralized `decode_json`, `encode_json`, and `format_json` serializer helpers.
[^vtb-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Top-level optional module resolution without function-scoped nested imports.
[^bench-guide]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Benchmark reproducibility, free-threaded compatibility, and threshold governance.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native client, pool, and single-`Sync` pipeline benchmark contracts.
