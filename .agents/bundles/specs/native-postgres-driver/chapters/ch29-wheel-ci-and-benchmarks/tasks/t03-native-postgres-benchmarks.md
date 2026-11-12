---
type: Task
id: "native-postgres-driver:ch29:t03"
chapter_id: ch29-wheel-ci-and-benchmarks
title: Wire Criterion native benchmarks, live PG18 paired orchestrator, and Makefile targets
description: Add Criterion benches/postgres_native.rs in vertebra-postgres, implement the @pytest.mark.postgres_benchmark PG18 paired orchestrator, update Makefile postgres-test/postgres-wheel-check/lint/lint-all in place, and add self-contained postgres-bench and postgres-bench-check.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch29:t02"
read_set:
  - Makefile
  - src/rs/vertebra-postgres/Cargo.toml
  - tools/benchmarks/postgres.py
  - src/py/tests/integration/conftest.py
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/benches/postgres_native.rs
  - Makefile
tests:
  - src/py/tests/integration/db/test_postgres_benchmarks.py
estimated_loc: 245
plan_revision: 8
plan_commit: d90d4625
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
updated_at: "2026-11-12T04:25:15Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: root-makefile
    resource: Makefile
    title: Existing postgres-test, postgres-wheel-check, lint, lint-all, and benchmark Makefile targets
  - id: pg-cargo
    resource: src/rs/vertebra-postgres/Cargo.toml
    title: Standalone vertebra-postgres crate manifest for Criterion bench registration
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Docker-gated PostgreSQL 18 service and clientless psql setup helpers
  - id: bench-guide
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: Criterion and Python release-build benchmark execution discipline
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

Wire Criterion Rust session benchmarks, live PostgreSQL 18 paired reference/native orchestration, and canonical in-place plus benchmark `Makefile` targets across `src/rs/vertebra-postgres/Cargo.toml`, `src/rs/vertebra-postgres/benches/postgres_native.rs`, `Makefile`, and `src/py/tests/integration/db/test_postgres_benchmarks.py` [^root-makefile] [^pg-cargo] [^int-conftest] [^bench-guide] [^pg-contracts]:
1. Add `criterion = { workspace = true }` under `[dev-dependencies]` and `[[bench]] name = "postgres_native" harness = false` in `src/rs/vertebra-postgres/Cargo.toml` [^pg-cargo], and implement `src/rs/vertebra-postgres/benches/postgres_native.rs` benchmarking live Rust cold authenticated connect+close, warm prepared `SELECT $1::bigint + 1`, and 64-statement single-`Sync` pipeline batches against `VERTEBRA_POSTGRES_BENCH_DSN` (compiling cleanly with zero warnings under `--no-run` and `--all-targets` Clippy when unset, and running real Protocol 3.0 wire sessions when `VERTEBRA_POSTGRES_BENCH_DSN` is set).
2. Implement `src/py/tests/integration/db/test_postgres_benchmarks.py` (`@pytest.mark.postgres_benchmark`, gated on `VERTEBRA_CONFORMANCE_DOCKER == "1"` matching `ch28`, using top-level imports, `vertebra.utils.serializers.decode_json`, zero nested imports, and zero inline `#` comments) owning an isolated `postgres_18_service` workflow (`sslmode="disable"`) and unique schema `bench_<hex>`:
   - Populate `bench_values(id BIGINT PRIMARY KEY, payload TEXT)` with `100_000` deterministic rows via clientless `psql`, compute the deterministic checksum (`dataset_fingerprint`), and verify initial state before each capture.
   - Spawn `python -m tools.benchmarks.postgres capture --implementation reference` and `--implementation native` child subprocesses with `VERTEBRA_POSTGRES_BENCH_DSN`, `VERTEBRA_POSTGRES_BENCH_RUN_ID`, and `VERTEBRA_POSTGRES_BENCH_SCHEMA` writing `artifacts/postgres/reference.json` and `artifacts/postgres/native.json` (performing at most one fresh whole paired retry with `retry_count == 1` if any scenario reports `coefficient_of_variation > 0.15`).
   - Run offline `python -m tools.benchmarks.postgres compare` writing `artifacts/postgres/comparison.json`, decode `PostgresComparisonReport` via `decode_json`, assert `passed is True` with every gated scenario present, run the diagnostic `100_000`-row `SyncPostgreSQLDriver.select_to_arrow` read checking `(Int64, Utf8)` schema, `row_count == 100_000`, `used_shm is True`, deterministic checksum, and zero lingering test-owned `/dev/shm` segments after `ArrowCursor.close()`, and drop the owned schema plus close all handles in `finally`.
3. Update `Makefile` cleanly with zero duplicate `.PHONY` declarations or duplicate target recipes [^root-makefile]:
   - Update **existing** `Makefile::postgres-test` **in place** (at `c8722c76` it runs only the crate tests and `_postgres` stubtest): run `$(MAKE) dev`, preserve `PYTHONPATH="$(CURDIR)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked` and `uv run --no-sync python -m mypy.stubtest vertebra._postgres`, and execute `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k postgres src/py/tests/integration/db/test_postgres_native.py src/py/tests/integration/db/test_postgres_replication.py src/py/tests/integration/db/test_postgres_services.py src/py/tests/integration/db/test_postgres_features.py -q -rs`.
   - Update **existing** `Makefile::postgres-wheel-check` **in place** (at `c8722c76` it runs an inline archive-only `python -c` check) to invoke `uv run --no-sync python tools/scripts/verify_postgres_wheel.py` (covering standalone sdist/wheel archive inspection plus both outside-checkout isolated venv probes from `ch29:t01`).
   - Add `tools/benchmarks/postgres.py` explicitly to the `ruff check`, `ruff format`, and `mypy` path lists of **both** `Makefile::lint` and `Makefile::lint-all` (which list `src/py/vertebra src/py/tests tools/scripts tools/pgo` at `c8722c76`).
   - Add self-contained `.PHONY: postgres-bench`: runs `uv sync --python 3.12 --all-extras --dev --group postgres-bench`, `$(MAKE) ts-build`, the release non-extension Rust build copied from `Makefile::build` (the same `cargo build --workspace --exclude ...` list, which excludes every PyO3 crate including `vertebra-postgres`, with `--release --features vertebra-http/python,vertebra-http/tls,vertebra-http/compression`), and one release `UV_PYTHON=3.12 UV_NO_SYNC=1 uv run --no-sync maturin develop --uv -r [-m src/rs/<crate>/Cargo.toml]` invocation for every PyO3 extension that `Makefile::build` builds (read the `maturin develop` lines of `Makefile::build` at execution time; never hardcode a count) so `--group postgres-bench` is never pruned by an implicit `uv` sync, followed by `VERTEBRA_CONFORMANCE_DOCKER=1 UV_PYTHON=3.12 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_benchmarks.py -q -s -m postgres_benchmark`.
   - Add `.PHONY: postgres-bench-check`: runs offline `uv run --no-sync python -m tools.benchmarks.postgres compare --reference artifacts/postgres/reference.json --native artifacts/postgres/native.json --output artifacts/postgres/comparison.json` without any network socket or DSN env var, leaving `bench`, `bench-all`, `bench-ft`, `rs-bench`, and `bench-check` untouched and leaving `src/py/vertebra/build/packaging/` completely untouched.

Out of scope: Modifying `tools/benchmarks/postgres.py` thresholds (`ch29:t02`), touching `src/py/vertebra/build/packaging/`, or editing user/OKF documentation (`ch30`–`ch32`).

## Bounded Read Manifest

- [Makefile](../../../../../../../Makefile) — Pre-existing `postgres-test`, `postgres-wheel-check`, `lint`, `lint-all`, `build` (exclude list and `maturin develop` lines), and benchmark targets [^root-makefile].
- [src/rs/vertebra-postgres/Cargo.toml](../../../../../../../src/rs/vertebra-postgres/Cargo.toml) — `vertebra-postgres` dependencies and dev-dependencies [^pg-cargo].
- `tools/benchmarks/postgres.py` — `capture`, `compare`, `PostgresBenchmarkReport`, and `PostgresComparisonReport` CLI/schema contract from `ch29:t02`.
- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `VERTEBRA_CONFORMANCE_DOCKER`-gated `postgres_18_service` and clientless `psql` helpers [^int-conftest].
- [.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md](../../../../../knowledge/testing/benchmarks-and-free-threaded.md) — Criterion and release-mode Python benchmark governance [^bench-guide].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Native session, single-`Sync` pipeline, and `ArrowCursor` contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/Cargo.toml` & `src/rs/vertebra-postgres/benches/postgres_native.rs`:
  - `[[bench]] name = "postgres_native" harness = false`
  - `criterion_group!(benches, bench_cold_connect, bench_warm_prepared_single, bench_pipeline_64)` and `criterion_main!(benches)`.
- `src/py/tests/integration/db/test_postgres_benchmarks.py`:
  - `@pytest.mark.postgres_benchmark def test_postgres_paired_reference_vs_native_benchmarks(postgres_18_service: PostgresServiceSpec, tmp_path: Path) -> None`
- `Makefile`:
  - Updated in place: `postgres-test`, `postgres-wheel-check`, `lint`, `lint-all`.
  - Added `.PHONY` targets: `postgres-bench`, `postgres-bench-check`.

## Edge Cases & Error Semantics

- `Makefile` emits zero `warning: overriding recipe for target` or `warning: ignoring old recipe for target` messages under `make -n postgres-test postgres-wheel-check postgres-bench postgres-bench-check`.
- Because plain `uv run maturin develop` re-syncs the default project environment and strips non-default dependency groups, `postgres-bench` explicitly sets `UV_PYTHON=3.12 UV_NO_SYNC=1` and invokes `uv run --no-sync maturin develop --uv -r` for every extension crate that `Makefile::build` builds after `uv sync --python 3.12 --all-extras --dev --group postgres-bench`.
- Every `make postgres-bench` run generates fresh paired `artifacts/postgres/{reference,native,comparison}.json` tied to one ephemeral `paired_run_id`, drops its `bench_<hex>` schema in `finally`, verifies zero residual test-owned `/dev/shm` segments after `ArrowCursor.close()`, and never writes DSN credentials to disk or stdout/stderr.

## Public-Seam Test Blueprint

- **Canonical test / bench files**: `src/py/tests/integration/db/test_postgres_benchmarks.py` and `src/rs/vertebra-postgres/benches/postgres_native.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Self-contained `make postgres-bench` + offline `make postgres-bench-check` | Run `make postgres-bench && env -u VERTEBRA_POSTGRES_BENCH_DSN make postgres-bench-check` against `postgres_18_service` (`100_000` rows) | `--group postgres-bench` stays intact across every release extension build that mirrors `Makefile::build`; `reference.json`, `native.json`, and `comparison.json` pass `median <= 1.25x`, `p95 <= 1.50x`, `statements_s >= 0.80x`, and `CV <= 0.15` with zero credential substrings. |
| `100_000`-row `VTBARROW` diagnostic & `/dev/shm` hygiene | `SyncPostgreSQLDriver.select_to_arrow("SELECT id, payload FROM <schema>.bench_values ORDER BY id")` then `cursor.close()` | `row_count == 100_000`, `used_shm is True`, typed `(Int64, Utf8)` checksum matches `dataset_fingerprint`, and zero test-owned `/dev/shm` segments remain after close. |
| In-place `postgres-test`, `postgres-wheel-check`, & `lint-all` | Run `make lint-all && make postgres-wheel-check && make postgres-test` | Zero Make recipe-override warnings; `lint-all` lints and typechecks `tools/benchmarks/postgres.py`; `postgres-test` runs Rust crate tests, `_postgres` stubtest, and zero-skip live pytest across all five PG suites. |
| Criterion compilation, Clippy, & default pytest deselection | `cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --no-run` and default `pytest src/py/tests/integration/db/test_postgres_benchmarks.py -q` | Bench target passes `cargo clippy --all-targets -D warnings` and compiles cleanly; default `pytest` deselects `postgres_benchmark` so `make py-test` stays offline and fast. |

## Verification

- Compile, Clippy, & Makefile recipe check:
  ```bash
  make -n postgres-test postgres-wheel-check postgres-bench postgres-bench-check >/dev/null
  cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --no-run
  make lint-all
  ```
- Full live wheel, conformance, & benchmark gate:
  ```bash
  make postgres-wheel-check
  make postgres-test
  make postgres-bench
  env -u VERTEBRA_POSTGRES_BENCH_DSN make postgres-bench-check
  make rs-bench
  ```

## Acceptance Criteria

- [ ] `src/rs/vertebra-postgres/Cargo.toml` and `src/rs/vertebra-postgres/benches/postgres_native.rs` compile cleanly under `--all-targets` Clippy and benchmark live native cold connect, warm prepared single-query, and 64-statement single-`Sync` pipeline batches when `VERTEBRA_POSTGRES_BENCH_DSN` is set (R27).
- [ ] `src/py/tests/integration/db/test_postgres_benchmarks.py` runs release-built paired `psycopg` 3.2.13 / `asyncpg` 0.31.0 vs native captures on live PG18 (`100_000` rows), enforces offline threshold parity, and verifies `VTBARROW` schema/checksum plus `/dev/shm` release (R27).
- [ ] `Makefile` updates `postgres-test`, `postgres-wheel-check`, `lint`, and `lint-all` in place with zero duplicate targets, adds self-contained `postgres-bench` (`--group postgres-bench` + `UV_NO_SYNC=1` release builds of every `Makefile::build` extension) and offline `postgres-bench-check`, preserves `bench`/`bench-ft`/`rs-bench`/`bench-check`, and leaves `src/py/vertebra/build/packaging/` and `tools/pgo/` untouched (R25, R26, R27).

## Notes & Discoveries

- Setting `UV_PYTHON=3.12 UV_NO_SYNC=1` on every `uv run --no-sync maturin develop --uv -r` invocation inside `postgres-bench` keeps `psycopg` and `asyncpg` installed from `--group postgres-bench` without adding them to `[dependency-groups].bench` or impacting free-threaded `make bench-ft` [^root-makefile] [^bench-guide].

[^root-makefile]: `Makefile` — Existing `postgres-test`, `postgres-wheel-check`, `lint`, `lint-all`, and benchmark Makefile targets.
[^pg-cargo]: `src/rs/vertebra-postgres/Cargo.toml` — Standalone `vertebra-postgres` crate manifest for Criterion bench registration.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Docker-gated PostgreSQL 18 service and clientless `psql` setup helpers.
[^bench-guide]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Criterion and Python release-build benchmark execution discipline.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native client, pipeline, and `VTBARROW` cursor contracts.
