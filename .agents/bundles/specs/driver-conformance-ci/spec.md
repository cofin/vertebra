---
type: Spec
flow_id: driver-conformance-ci
title: "Live Driver Conformance CI, DuckDB GCS Secret Rotation, and Oracle Notify Liveness"
description: "Ship GCS ADC bearer-token secret providers and DuckDB secret validation/invalidation, enforce Oracle AQ wait_timeout_seconds liveness, run versioned pytest-databases live conformance and native edge suites with zero-skip CI gates, and restrict PyPI duckdb to benchmarks."
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-duckdb-gcs-secrets-and-oracle-notify
  - ch02-conformance-harness-and-live-suites
  - ch03-make-lanes-bench-isolation-and-ci
  - ch04-docs-and-okf-sync
current_chapter: ch02-conformance-harness-and-live-suites
current_task: null
parent_prd: native-data-platform
depends_on_flows:
  - native-postgres-driver
plan_revision: 4
plan_commit: 37972f2a
last_verified_checkpoint: 38e02b16
last_reviewed_chapter: ch01-duckdb-gcs-secrets-and-oracle-notify
last_reviewed_commit: cae00a18
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-14T17:42:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-14T17:24:00Z"
    commit: 37972f2a
tags:
  - conformance
  - ci
  - duckdb
  - mysql
  - mariadb
  - mssql
  - oracle
  - gcs
sources:
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: "Cross-engine driver conformance matrix and _resolve_live_config"
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_mysql_config
    title: "pytest-databases service fixtures for MySQL, MariaDB, SQL Server, and Oracle"
  - id: pyproject-pd
    resource: pyproject.toml
    title: "pytest-databases>=0.20.0b1 and benchmark dependency-group declarations"
  - id: ci-workflow
    resource: .github/workflows/ci.yml
    title: "GitHub Actions CI jobs for Python, Rust, TypeScript, and docs"
  - id: makefile
    resource: Makefile
    title: "duckdb-test, mysql-test, mssql-test, oracle-test, PY_TEST_GROUPS, and install targets"
  - id: duckdb-secret
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
    title: "Cloud-agnostic DuckDB CREATE SECRET declaration with value_provider rotation"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: "DuckDB drivers with generation- and interval-driven dynamic secret refresh"
  - id: gcs-token-cache
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: "Native GCP token cache with generation, needs_refresh, invalidate, and get_token"
  - id: gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: "GCSConfig sync_token_cache and async_token_cache factories"
  - id: oracle-notify
    resource: src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker
    title: "Oracle AQ listener dequeue loop discarding wait_timeout_seconds"
  - id: oracle-native
    resource: src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config
    title: "Live Oracle 26ai native suite gated on VERTEBRA_CONFORMANCE_DOCKER"
  - id: driver-limits
    resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
    title: "Canonical database driver limitations and operational boundaries chapter"
---

# Specification: Live Driver Conformance CI, DuckDB GCS Secret Rotation, and Oracle Notify Liveness

<!-- truth: start -->
- **Baseline (`HEAD @ 38e02b16`, `ch01`–`ch02` Implemented)**: `ch01` shipped `vertebra.storage.{SyncGcsBearerSecretProvider,AsyncGcsBearerSecretProvider}`, `DuckDBSecretConfig` validation + `(HTTP 401)`/`(HTTP 403)` invalidation + startup `_client` cleanup, and Oracle AQ `wait_timeout_seconds` idle liveness (`a8ca7b24`, `b3b113d1`); `ch02` shipped `ConformanceGate`, `ServiceVersionMatrix`, `mariadb` `EngineConformanceSpec`, and live DuckDB, MySQL/MariaDB, SQL Server, and Oracle native suites (`38e02b16`); `Makefile` `db-conformance`, `bench`-only PyPI `duckdb` isolation, `.github/workflows/ci.yml` live lanes, and documentation sync remain in `ch03`–`ch04` [^conformance-suite] [^duckdb-secret] [^oracle-notify] [^makefile].
- **Target Architecture**: `ch02`–`ch04` wire `ConformanceGate` + `ServiceVersionMatrix` (`pytest-databases`) across DuckDB, MySQL, MariaDB, SQL Server, and Oracle via single-pass `make db-conformance` and `.github/workflows/ci.yml`, while delegating `VERTEBRA_CONFORMANCE_ENGINES=postgres` under `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` to `evaluate_postgres_zero_skip_gate` in `conftest.py` [^int-conftest] [^ci-workflow].
- **Scope & Boundaries**: Sequenced after archived `native-postgres-driver` ([`postgres-native-driver.md`](../../knowledge/domains/data/postgres-native-driver.md)) so `src/py/tests/integration/conftest.py`, `Makefile`, and `.github/workflows/ci.yml` merge cleanly; updates existing [`database-driver-limitations.md`](../../knowledge/domains/data/database-driver-limitations.md) in place (`db-conformance` still pending) [^int-conftest] [^driver-limits].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Current Baseline
Four merged native database wheels lack live server CI gates and carry three runtime/build gaps on `main @ 18baadcd`:
1. **Unshipped DuckDB GCS Bearer Rotation & Permissive Secret Config**: Documentation advertises GCS ADC `httpfs` rotation, yet no public provider wraps `SyncGcpTokenCache` / `AsyncGcpTokenCache` [^gcs-token-cache] [^gcs-config]; `DuckDBSecretConfig` allows `persistent=True` with `value_provider` and clamps negative/non-finite `refresh_interval_seconds` [^duckdb-secret]; and `SyncDuckDBDriver` / `AsyncDuckDBDriver` neither invalidate rejected `(HTTP 401)`/`(HTTP 403)` credentials nor close `self._client` if initial `required=True` secret refresh raises [^duckdb-driver].
2. **Ignored `OracleNotifyConfig.wait_timeout_seconds`**: `src/rs/vertebra-oracle/src/notify.rs` discards `let _ = wait_timeout_seconds.max(1);` without probing idle session liveness [^oracle-notify].
3. **Missing Live Conformance Lanes & PyPI `duckdb` Leakage**: `test_driver_conformance.py` omits `mariadb` and retains unused `_DUCKDB_SKIP_REASON` and `VERTEBRA_CONFORMANCE_DUCKDB_DSN` [^conformance-suite]; `test_oracle_native.py` lacks `@pytest.mark.driver_engine("oracle")` tagging and `ServiceVersionMatrix` hookup [^oracle-native]; `Makefile` pulls PyPI `--group duckdb` into `PY_TEST_GROUPS` and `make install` and falls back to `test_freethreaded_gil.py` in `oracle-test` [^makefile] [^pyproject-pd].

### 1.2 Non-Goals
- Modifying PostgreSQL live conformance or `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS` (owned by archived `native-postgres-driver` / [`postgres-native-driver.md`](../../knowledge/domains/data/postgres-native-driver.md)).
- Introducing GCP imports into `vertebra.db.adapters.duckdb` or `vertebra._duckdb`.
- Creating a duplicate limitations file (all caveats update [`database-driver-limitations.md`](../../knowledge/domains/data/database-driver-limitations.md) in place) [^driver-limits].

## 2. Requirements, Concrete Contracts & Architecture

- **`R1` Shipped GCS Bearer Secret Providers (`ch01:t01`)**: `src/py/vertebra/storage/_secrets.py` defines `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` (`from_config(config: GCSConfig, *, allow_ambient_adc=True, scopes=GCS_DEFAULT_SCOPES)`, `generation`, `token_cache`, `needs_refresh()`, `invalidate(failed_generation)`, `__call__() -> dict[str, str]` / `Awaitable[dict[str, str]]`), re-exported from `vertebra.storage`.
- **`R2` DuckDB Secret Validation, `401`/`403` Invalidation & Startup Cleanup (`ch01:t02`)**: `DuckDBSecretConfig.__post_init__` raises `ImproperConfigurationError` when `persistent=True` is combined with `value_provider` or when `refresh_interval_seconds` is `bool`, non-finite, or `<= 0` (when positive budget required; `0.0` rejected so callers use explicit positive intervals). `SyncDuckDBDriver` and `AsyncDuckDBDriver` match `\(HTTP (?:401|403)\)` on statement errors, call `provider.invalidate(applied_generation)` when present, re-raise unchanged, and close `self._client` if initial `required=True` secret refresh fails during initialization.
- **`R3` Oracle AQ `wait_timeout_seconds` Idle Liveness (`ch01:t03`)**: `vertebra-oracle/src/notify.rs` keeps `1s` `DBMS_AQ.DEQUEUE` slices so `close()` remains sub-second responsive while accumulating idle elapsed time and issuing `SELECT 1 FROM DUAL` whenever idle elapsed reaches `wait_timeout_seconds.max(1)`.
- **`R4` Zero-Skip `ConformanceGate`, `ServiceVersionMatrix` & Live Suites (`ch02:t01`–`t04`)**:
  - `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`, `@pytest.mark.driver_engine("<engine>")` / `@pytest.mark.conformance("<engine>")` or `param.engine`) deselects unrelated integration items and fails the session on any skip, xfail, or `0`-test engine; `ServiceVersionMatrix` maps `VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION` onto `pytest-databases` `0.20.0b1` fixtures (`mysql_8/84/96/97_service`, `mariadb_113/114/118/122/123/130_service`, `mssql_2022/2025_service`, `oracle_23ai_service`).
  - `test_driver_conformance.py` adds `mariadb` and removes dead conformance code; `test_duckdb_native.py` covers embedded `:memory:` `VTBARROW`/pipelines/savepoints, offline loopback HTTPS GCS bearer rotation, and opt-in `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1`; `test_mysql_native.py` and `test_mssql_native.py` verify live binary protocol, Arrow, pipeline, and cancel semantics; `test_oracle_native.py` adds engine tags and live AQ liveness verification.
- **`R5` Single-Pass `Makefile` Isolation & CI Matrix (`ch03:t01`–`t02`)**: `Makefile` adds `db-conformance`, cleans `oracle-test`/`mysql-test`/`mssql-test`/`duckdb-test`, and removes `--group duckdb` from `PY_TEST_GROUPS`, `make install`, and `make upgrade` (`pyproject.toml` keeps PyPI `duckdb` strictly in `bench`); `.github/workflows/ci.yml` adds `test-db-conformance` (DuckDB, MySQL, MariaDB, SQL Server with `VERTEBRA_CONFORMANCE_DOCKER=1`) and `conformance-oracle` (`continue-on-error: true` container lane).
- **`R6` User Docs & OKF Knowledge Synchronization (`ch04:t01`–`t04`)**: Synchronize `docs/usage/{storage,analytics-and-cdc,databases}.md`, `docs/reference/storage.md`, `docs/changelog/index.md`, `AGENTS.md` (`<= 100L`), `polyglot-test-matrix.md` (`<= 200L`), `benchmarks-and-free-threaded.md` (`<= 200L`), `workflow.md` (`<= 200L`), `database-drivers-and-dialers.md` (`<= 200L`), and `database-driver-limitations.md` (`<= 200L`).

## 3. Documentation & OKF Knowledge Plan

- **User & Reference Docs (`ch04:t01`–`t02`)**: `docs/usage/storage.md`, `docs/usage/analytics-and-cdc.md`, `docs/reference/storage.md` (`SyncGcsBearerSecretProvider`, `AsyncGcsBearerSecretProvider`), `docs/usage/databases.md` (native wheels summary, DuckDB GCS secret row, Oracle `wait_timeout_seconds` note), `docs/changelog/index.md`, and `AGENTS.md` (`<= 100L`).
- **OKF Chapters (`ch04:t03`–`t04`, `<= 200L` each)**: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (removing stale `PY_TEST_GROUPS` `--group duckdb` claim), `.agents/bundles/knowledge/workflow.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, and `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` (updated in place).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Tasks | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R3` GCS providers, DuckDB secret lifecycle & Oracle notify | `ch01` (`t01`–`t03`) | `storage/_secrets.py`, `duckdb/{config,driver}.py`, `vertebra-oracle/src/notify.rs` | `cargo test -p vertebra-oracle notify::` + `pytest test_gcs_token_refresh.py test_duckdb_driver.py` |
| `R4` Conformance gate, MariaDB matrix & live native suites | `ch02` (`t01`–`t04`) | `tests/integration/{conformance_gate,conftest}.py`, `tests/integration/db/test_*.py` | `pytest test_conformance_gate.py test_duckdb_native.py test_driver_conformance.py` |
| `R5` `Makefile` `db-conformance`, `duckdb` bench isolation & CI | `ch03` (`t01`–`t02`) | `Makefile`, `pyproject.toml`, `.github/workflows/ci.yml` | `pytest test_bench_infrastructure.py` + `make review-check` |
| `R6` User docs, `AGENTS.md` & OKF sync | `ch04` (`t01`–`t04`) | `docs/**`, `AGENTS.md`, `.agents/bundles/knowledge/**` | `make docs && make review-check` |

## 5. Implementation Plan

- [x] [cae00a18] [ch01-duckdb-gcs-secrets-and-oracle-notify](chapters/ch01-duckdb-gcs-secrets-and-oracle-notify/spec.md) — Ship sync/async GCS bearer secret providers, DuckDB secret validation/`401`-`403` invalidation/startup cleanup, and Oracle AQ `wait_timeout_seconds` liveness (`< 60s` offline Rust + unit gate).
- [~] [ch02-conformance-harness-and-live-suites](chapters/ch02-conformance-harness-and-live-suites/spec.md) — Implement `ConformanceGate`, `ServiceVersionMatrix`, `mariadb` matrix cleanup, and DuckDB, MySQL/MariaDB, SQL Server, and Oracle live suites (`< 60s` offline gate + embedded DuckDB/loopback GCS).
- [ ] [ch03-make-lanes-bench-isolation-and-ci](chapters/ch03-make-lanes-bench-isolation-and-ci/spec.md) — Single-pass `Makefile` `db-conformance` + `bench`-only PyPI `duckdb` isolation and `.github/workflows/ci.yml` conformance matrix (`< 60s` infrastructure unit gate).
- [ ] [ch04-docs-and-okf-sync](chapters/ch04-docs-and-okf-sync/spec.md) — Synchronize storage/analytics/database docs, changelog, `AGENTS.md`, testing/workflow OKF chapters, and `database-driver-limitations.md` (`< 60s` `make docs && make review-check` gate).

## 6. Current State & Remaining Plan Snapshot

- **State**: `active` (`1/4` chapters reviewed, `7/13` tasks completed through `ch02-conformance-harness-and-live-suites` at `38e02b16`; `6` tasks remain across `ch03`–`ch04`).
- **Full `/flow:finish` Verification**: `make lint-all && make rs-test && make py-test && make db-conformance ENGINE=duckdb && make db-conformance ENGINE=mysql VERSION=8.4 && make db-conformance ENGINE=mariadb VERSION=11.4 && make db-conformance ENGINE=mssql VERSION=2022 && make docs-check && make review-check`.

[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine driver conformance matrix and `_resolve_live_config`.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_mysql_config` — `pytest-databases` service fixtures for MySQL, MariaDB, SQL Server, and Oracle.
[^pyproject-pd]: `pyproject.toml` — `pytest-databases>=0.20.0b1` and benchmark dependency-group declarations.
[^ci-workflow]: `.github/workflows/ci.yml` — GitHub Actions CI jobs for Python, Rust, TypeScript, and docs.
[^makefile]: `Makefile` — `duckdb-test`, `mysql-test`, `mssql-test`, `oracle-test`, `PY_TEST_GROUPS`, and `install` targets.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Cloud-agnostic DuckDB `CREATE SECRET` declaration with `value_provider` rotation.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB drivers with generation- and interval-driven dynamic secret refresh.
[^gcs-token-cache]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — Native GCP token cache with `generation`, `needs_refresh`, `invalidate`, and `get_token`.
[^gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — `GCSConfig` `sync_token_cache` and `async_token_cache` factories.
[^oracle-notify]: `src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker` — Oracle AQ listener dequeue loop discarding `wait_timeout_seconds`.
[^oracle-native]: `src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config` — Live Oracle 26ai native suite gated on `VERTEBRA_CONFORMANCE_DOCKER`.
[^driver-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Canonical database driver limitations and operational boundaries chapter.
