---
type: Spec
flow_id: driver-conformance-ci
title: Live Driver Conformance CI and DuckDB GCS ADC Secret Rotation
description: Live pytest-databases conformance lanes in CI for the merged DuckDB, MySQL, MariaDB, SQL Server, and Oracle native wheels, engine-specific live edge suites, zero-skip enforcement, and shipped GCS ADC bearer-token secret rotation for DuckDB httpfs.
status: draft
state: planned
has_chapters: true
chapters:
  - ch01-live-conformance-harness
  - ch02-gcs-bearer-secret-rotation
  - ch03-engine-live-suites
  - ch04-make-and-ci-lanes
  - ch05-docs-and-okf
current_chapter: ch01-live-conformance-harness
current_task: "driver-conformance-ci:ch01:t01"
parent_prd: native-data-platform
depends_on_flows: []
research: []
plan_revision: 2
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_chapter: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T00:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T00:30:00Z"
    commit: c8722c76
sources:
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine driver conformance matrix and _resolve_live_config
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_mysql_config
    title: pytest-databases service fixtures for MySQL, MariaDB, SQL Server, and Oracle
  - id: ci-workflow
    resource: .github/workflows/ci.yml
    title: GitHub Actions CI jobs (lint, test-python, test-rust, test-typescript, build-docs)
  - id: makefile
    resource: Makefile
    title: duckdb-test, mysql-test, mssql-test, oracle-test, and py-test targets
  - id: duckdb-secret
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
    title: Cloud-agnostic DuckDB CREATE SECRET declaration with value_provider rotation
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB drivers with generation- and interval-driven dynamic secret refresh
  - id: gcs-token-cache
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: Native GCP token cache with generation, needs_refresh, invalidate, and get_token
  - id: gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: GCSConfig sync_token_cache and async_token_cache factories
  - id: gcs-rotation-tests
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_multi_session_token_generation_secret_rotation
    title: Rotation tests that define their own test-local bearer secret providers
  - id: oracle-native
    resource: src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config
    title: Live Oracle 26ai native suite gated on VERTEBRA_CONFORMANCE_DOCKER
  - id: test-matrix-okf
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Polyglot test matrix and database driver conformance knowledge chapter
  - id: oracle-notify
    resource: src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker
    title: Oracle AQ listener dequeue loop
  - id: pg-ch28
    resource: .agents/bundles/specs/native-postgres-driver/chapters/ch28-live-conformance-and-service-acceptance/spec.md
    title: PostgreSQL live conformance owned by native-postgres-driver
tags:
  - conformance
  - ci
  - pytest-databases
  - duckdb
  - mysql
  - mariadb
  - mssql
  - oracle
  - gcs
---

# Flow: Live Driver Conformance CI and DuckDB GCS ADC Secret Rotation

<!-- truth: start -->
- **Scope**: The merged native wheels `vertebra-duckdb`, `vertebra-mysql` (MySQL and MariaDB), `vertebra-mssql`, and `vertebra-oracle`. PostgreSQL live conformance stays in `native-postgres-driver` ch28 [^pg-ch28].
- **Real servers only through `pytest-databases`**: Every live lane starts its database with `pytest-databases` service fixtures (`mysql_8_service`, `mysql_84_service`, `mysql_97_service`, `mariadb_114_service`, `mariadb_118_service`, `mssql_2022_service`, `mssql_2025_service`, `oracle_23ai_service`); DuckDB runs embedded [^int-conftest].
- **Lane selection and zero-skip enforcement**: `VERTEBRA_CONFORMANCE_ENGINES=<engine>[,<engine>]` deselects integration items not tagged for those engines and fails the session when any selected item for a listed engine skips, xfails, or when a listed engine executes zero tests [^conformance-suite].
- **Engine tagging**: Items are tagged through the `driver_engine(name)` marker or a parametrized value carrying an `engine` attribute (`EngineConformanceSpec`, `CacheConformanceSpec`).
- **Versions**: `VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION` select the service fixture; unknown versions raise `pytest.UsageError` listing the allowed values.
- **CI**: `.github/workflows/ci.yml` gains a `test-db-conformance` job (one matrix row per engine/version, Python `3.12` plus `3.14t` rows for DuckDB and MySQL 8.4) running `make db-conformance ENGINE=<engine> VERSION=<version>` [^ci-workflow] [^makefile].
- **DuckDB GCS ADC rotation is a shipped feature**: `vertebra.storage.SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` wrap `SyncGcpTokenCache` / `AsyncGcpTokenCache` and return `{"bearer_token": <token>}` with `generation`, `needs_refresh()`, and `invalidate()` [^gcs-token-cache] [^gcs-config].
- **Cloud-agnostic engine drivers**: `vertebra.db.adapters.duckdb` never imports GCP code; it rotates any `value_provider` by generation, `needs_refresh()`, or interval, and invalidates providers exposing `invalidate(failed_generation)` when a statement fails with an `(HTTP 401)` or `(HTTP 403)` credential rejection, then re-raises [^duckdb-driver].
- **Fail loud**: `DuckDBSecretConfig` rejects `persistent=True` with a `value_provider` and negative or non-finite `refresh_interval_seconds` with `ImproperConfigurationError` [^duckdb-secret].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The four merged native driver wheels have never been verified against real servers in CI. `.github/workflows/ci.yml` runs `make py-test` only, which never sets `VERTEBRA_CONFORMANCE_DOCKER`, so `src/py/tests/integration/db/test_driver_conformance.py` skips SQL Server and Oracle, runs MySQL only against the in-process `vertebra._mysql.MySQLWireTestServer` loopback, and never exercises MariaDB even though `src/py/tests/integration/conftest.py::vtb_mariadb_config` exists [^conformance-suite] [^int-conftest] [^ci-workflow]. `src/py/tests/integration/db/test_oracle_native.py` is gated the same way and never runs in CI [^oracle-native]. No mechanism catches a live lane that silently skips, and no engine-specific live suites exist for MySQL, MariaDB, or SQL Server.

`Makefile::oracle-test` falls back to an unrelated free-threading test when `test_oracle_adapter.py` is missing and never runs the Oracle conformance or native suites [^makefile]. The conformance module carries dead surface: `_DUCKDB_SKIP_REASON`, the always-empty `_FAIL_LOUD_DRIVER_ENGINES` branch, a DuckDB `VERTEBRA_CONFORMANCE_DUCKDB_DSN` variable that `_resolve_live_config` never reads, and a duplicated SQL Server branch [^conformance-suite].

DuckDB GCS access through Google Cloud Application Default Credentials is advertised (`docs/index.md`, `docs/usage/databases.md`, the `0.3.0` changelog) but no shipped object produces a rotating DuckDB `gcs` secret from a GCP token cache. The only bearer providers live as private classes inside `src/py/tests/unit/storage/test_gcs_token_refresh.py`, use `secret_type="http"`, and assert on the private `driver._secret_generations` dict [^gcs-rotation-tests]. Revoked tokens are never invalidated, so a DuckDB session keeps sending a rejected token until the interval elapses. `DuckDBSecretConfig` silently clamps negative refresh intervals and allows short-lived rotated tokens to be written to DuckDB's persistent secret directory [^duckdb-secret] [^duckdb-driver].

This flow delivers: a lane-selection and zero-skip conformance gate with versioned `pytest-databases` fixtures; a MariaDB conformance engine and dead-code removal; live MySQL/MariaDB and SQL Server edge suites and Oracle suite tagging; `make db-conformance` and a CI matrix job; shipped GCS bearer secret providers with DuckDB lifecycle hardening and live `httpfs` acceptance; and synchronized user docs, OKF chapters, and a limitations record.

### 1.2 Non-Goals

- PostgreSQL live conformance, PG14–18 versioning, and the `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS` hook (owned by `native-postgres-driver` ch28) [^pg-ch28]. `VERTEBRA_CONFORMANCE_ENGINES=postgres` raises `pytest.UsageError` naming that flow.
- BigQuery, Spanner, Redis, and Valkey live lanes (no merged wheel in scope; their existing env gates stay unchanged).
- New driver features in the four wheels; this flow verifies existing behavior and records any live failure as a blocked task with evidence.
- GCP-specific code inside `vertebra.db.adapters.duckdb` or `vertebra._duckdb`.

### 1.3 Upstream Execution Baseline

- Branch `feat/cleanup-2` at `c8722c76` (`origin/main`), after the merged DuckDB (PR [#105](https://github.com/cofin/vertebra/pull/105)), MySQL (PR [#106](https://github.com/cofin/vertebra/pull/106)), SQL Server (PR [#107](https://github.com/cofin/vertebra/pull/107)), Oracle (PR [#103](https://github.com/cofin/vertebra/pull/103)), and PostgreSQL (PR [#104](https://github.com/cofin/vertebra/pull/104)) wheels.
- `parent_prd: native-data-platform`; no active flow is a prerequisite. `native-postgres-driver` ch28 also edits `src/py/tests/integration/conftest.py` (PostgreSQL fixtures only); this flow edits only the MySQL, MariaDB, SQL Server, and Oracle fixtures plus new hooks in that file.

### 1.4 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Conformance matrix | `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS`, `::_resolve_live_config`, `::_require_native_module`, `::sync_sql_driver` [^conformance-suite] | Test-owned matrix; remote-owned databases reached through `pytest-databases` containers (Docker) or env DSNs. |
| Service fixtures | `src/py/tests/integration/conftest.py::vtb_mysql_config`, `::vtb_mariadb_config`, `::vtb_mssql_config`, `::vtb_oracle_config` [^int-conftest] | `pytest-databases` 0.20.0b1 service fixtures resolved lazily via `request.getfixturevalue`. |
| CI and Make | `.github/workflows/ci.yml::test-python`, `Makefile::duckdb-test`, `::mysql-test`, `::mssql-test`, `::oracle-test` [^ci-workflow] [^makefile] | GitHub Actions `ubuntu-latest` runners with Docker; Make targets drive pytest. |
| Oracle live suite | `src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config` [^oracle-native] | Live suite gated on DSN or Docker. |
| DuckDB secrets | `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig`, `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver._refresh_dynamic_secrets`, `::AsyncDuckDBDriver._refresh_dynamic_secrets_async`, `::_probe_provider_generation`, `::_probe_provider_needs_refresh` [^duckdb-secret] [^duckdb-driver] | In-process engine; `value_provider` is a port (production GCS provider, test doubles). |
| GCP token cache | `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache`, `::AsyncGcpTokenCache`, `src/py/vertebra/storage/_store.py::GCSConfig.sync_token_cache`, `::GCS_DEFAULT_SCOPES` [^gcs-token-cache] [^gcs-config] | Remote-owned OAuth2 endpoint behind the native cache; tests use a loopback token server. |

### 1.5 Relevant OKF Knowledge Chapters & Skills

- [Polyglot Test Matrix](../../knowledge/testing/polyglot-test-matrix.md) [^test-matrix-okf], [Project Workflow](../../knowledge/workflow.md), [Database Drivers & Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [Object Storage Limitations](../../knowledge/domains/storage/object-storage-limitations.md) (format precedent for the new limitations record).
- Skills: `.agents/skills/python-framework/SKILL.md`, `.agents/skills/review-tests/SKILL.md`, `.agents/skills/review-docs-okf/SKILL.md`, and the `pytest-databases` skill.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: `VERTEBRA_CONFORMANCE_ENGINES` selects integration items by engine and enforces zero skips/xfails plus at least one executed test per listed engine.
- **R2**: Items carry engine identity through the `driver_engine(name)` marker or a parametrized `engine` attribute.
- **R3**: `VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION` select versioned `pytest-databases` service fixtures; unknown values fail loud.
- **R4**: The conformance matrix includes a `mariadb` engine; MariaDB, SQL Server, and Oracle skip offline with an explicit reason and never fall back to a sample URL.
- **R5**: Dead conformance surface (`_DUCKDB_SKIP_REASON`, `_FAIL_LOUD_DRIVER_ENGINES`, DuckDB `env_dsn_vars`, duplicated SQL Server branch) is removed.
- **R6**: A live MySQL/MariaDB suite covers `utf8mb4`, `DECIMAL(65,30)`, `DATETIME(6)`, `JSON`, `lastrowid`, multi-result `CALL`, `KILL QUERY` cancellation, Arrow load/stream, and duplicate-key `IntegrityError`.
- **R7**: A live SQL Server suite covers `DATETIMEOFFSET` offsets, `DECIMAL(38,10)`, `NVARCHAR(MAX)` and `VARBINARY(MAX)` LOBs, `WAITFOR` cancellation, savepoint rollback, Arrow load/stream, and duplicate-key `IntegrityError`.
- **R8**: The Oracle live suite is tagged `driver_engine("oracle")` and resolves its config only through the shared fixture or DSN.
- **R9**: `make db-conformance ENGINE=<engine> [VERSION=<version>]` runs one live lane; `make oracle-test` runs the Oracle unit, conformance, and native suites without a fallback branch.
- **R10**: CI runs a `test-db-conformance` matrix job covering every engine/version row.
- **R11**: `vertebra.storage` ships `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider`.
- **R12**: `DuckDBSecretConfig` rejects persistent dynamic secrets and invalid refresh intervals.
- **R13**: DuckDB drivers invalidate credential providers on `(HTTP 401)` / `(HTTP 403)` statement failures and re-raise.
- **R14**: A live DuckDB `httpfs` suite proves bearer-token rotation over a `gs://` read against a loopback GCS endpoint.
- **R15**: User docs, `AGENTS.md`, changelog, OKF chapters, and a limitations record describe the shipped behavior.
- **R16**: The Oracle AQ listener uses the configured `wait_timeout_seconds` as its idle dequeue wait budget, keeps 1-second `DBMS_AQ.DEQUEUE` slices so close stays responsive, pings the session when the budget elapses, and a test proves the configured value changes behavior [^oracle-notify].
- **R17**: `make install`, `upgrade`, `py-test`, and `coverage` never sync the PyPI `duckdb` group; only `--group bench` includes it, and the Python suite passes without PyPI `duckdb` [^makefile].

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# src/py/tests/integration/conformance_gate.py
class ConformanceGate:
    ENGINES_ENV: ClassVar[str] = "VERTEBRA_CONFORMANCE_ENGINES"
    ENGINE_MARKER: ClassVar[str] = "driver_engine"
    SUPPORTED_ENGINES: ClassVar[frozenset[str]] = frozenset({"duckdb", "mariadb", "mssql", "mysql", "oracle"})
    def __init__(self, engines: frozenset[str]) -> None: ...
    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> ConformanceGate: ...
    @property
    def engines(self) -> frozenset[str]: ...
    @property
    def active(self) -> bool: ...
    @staticmethod
    def item_engines(item: pytest.Item) -> frozenset[str]: ...
    def selects(self, item: pytest.Item, integration_root: Path) -> bool: ...
    def record(self, nodeid: str, engines: frozenset[str], report: pytest.TestReport) -> None: ...
    def failures(self) -> list[str]: ...

class ServiceVersionMatrix:
    FIXTURES: ClassVar[dict[str, dict[str, str]]]
    DEFAULTS: ClassVar[dict[str, str]] = {"mysql": "8.4", "mariadb": "11.4", "mssql": "2022", "oracle": "23ai"}
    @classmethod
    def fixture_name(cls, family: str, environ: Mapping[str, str]) -> str: ...

# src/py/vertebra/storage/_secrets.py
class SyncGcsBearerSecretProvider:
    def __init__(self, token_cache: SyncGcpTokenCache) -> None: ...
    @classmethod
    def from_config(cls, config: GCSConfig, *, allow_ambient_adc: bool = True, scopes: Sequence[str] = GCS_DEFAULT_SCOPES) -> Self: ...
    @property
    def generation(self) -> int: ...
    @property
    def token_cache(self) -> SyncGcpTokenCache: ...
    def needs_refresh(self) -> bool: ...
    def invalidate(self, failed_generation: int) -> None: ...
    def __call__(self) -> dict[str, str]: ...

class AsyncGcsBearerSecretProvider:
    def __init__(self, token_cache: AsyncGcpTokenCache) -> None: ...
    @classmethod
    def from_config(cls, config: GCSConfig, *, allow_ambient_adc: bool = True, scopes: Sequence[str] = GCS_DEFAULT_SCOPES) -> Self: ...
    @property
    def generation(self) -> int: ...
    @property
    def token_cache(self) -> AsyncGcpTokenCache: ...
    def needs_refresh(self) -> bool: ...
    def invalidate(self, failed_generation: int) -> None: ...
    async def __call__(self) -> dict[str, str]: ...

# src/py/vertebra/db/adapters/duckdb/config.py
class DuckDBSecretConfig(FrozenBaseStruct, frozen=True, gc=True):
    def __post_init__(self) -> None: ...  # persistent+value_provider and invalid refresh_interval_seconds raise ImproperConfigurationError

# src/py/vertebra/db/adapters/duckdb/driver.py
_CREDENTIAL_REJECTION_PATTERN: Final[re.Pattern[str]] = re.compile(r"\(HTTP (?:401|403)\)")
class SyncDuckDBDriver:
    def _invalidate_rejected_credentials(self, exc: BaseException) -> None: ...
class AsyncDuckDBDriver:
    def _invalidate_rejected_credentials(self, exc: BaseException) -> None: ...
```

`ServiceVersionMatrix.FIXTURES` maps `mysql` → `{"8.0": "mysql_8_service", "8.4": "mysql_84_service", "9.6": "mysql_96_service", "9.7": "mysql_97_service"}`, `mariadb` → `{"11.3": "mariadb_113_service", "11.4": "mariadb_114_service", "11.8": "mariadb_118_service", "12.2": "mariadb_122_service", "12.3": "mariadb_123_service", "13.0": "mariadb_130_service"}`, `mssql` → `{"2022": "mssql_2022_service", "2025": "mssql_2025_service"}`, `oracle` → `{"23ai": "oracle_23ai_service"}`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `pytest` session → `src/py/tests/integration/conftest.py::pytest_configure` → `ConformanceGate.from_environ(os.environ)` stored on `config.stash`; `pytest_collection_modifyitems` → `ConformanceGate.selects` deselects; hookwrapper `pytest_runtest_makereport` → `ConformanceGate.record`; `pytest_sessionfinish` → `ConformanceGate.failures` sets `session.exitstatus = pytest.ExitCode.TESTS_FAILED`.
- `vtb_mysql_config` / `vtb_mariadb_config` / `vtb_mssql_config` / `vtb_oracle_config` → `ServiceVersionMatrix.fixture_name` → `request.getfixturevalue(<pytest-databases service fixture>)`.
- `test_driver_conformance.py::_resolve_live_config` → env DSN or container fixture → `provide_driver`.
- `make db-conformance` → `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=$(ENGINE)` → `pytest src/py/tests/integration/db`; CI `test-db-conformance` → `make db-conformance`.
- Application → `DuckDBSecretConfig(value_provider=SyncGcsBearerSecretProvider.from_config(GCSConfig(...)))` → `SyncDuckDBDriver._refresh_dynamic_secrets` probes `generation` / `needs_refresh()` → `provider()` → `SyncGcpTokenCache.get_token()` → `DuckDBSecretConfig.to_create_secret_sql` → `CREATE OR REPLACE SECRET`.
- Statement failure → `SyncDuckDBDriver._invalidate_rejected_credentials(exc)` → `provider.invalidate(applied_generation)` → next statement refreshes.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Offline `make py-test` | No `VERTEBRA_CONFORMANCE_*` env | Gate inactive; no deselection; MariaDB/SQL Server/Oracle skip with explicit reasons; zero Docker calls | Offline lanes stay green and container-free |
| Unknown engine | `VERTEBRA_CONFORMANCE_ENGINES=sybase` | `pytest.UsageError` listing supported engines | No silent no-op lane |
| PostgreSQL requested | `VERTEBRA_CONFORMANCE_ENGINES=postgres` | `pytest.UsageError` naming `native-postgres-driver` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS` | Ownership boundary |
| Skipped item in lane | Container fixture skip or `pytest.skip` | Session exit code `1`; summary lists nodeids | Zero-skip live evidence |
| Empty lane | Engine listed, zero executed items | Session exit code `1`: `"mysql: 0 tests executed"` | Lane proves something |
| Unknown version | `VERTEBRA_CONFORMANCE_MSSQL_VERSION=2019` | `pytest.UsageError` listing `2022, 2025` | Explicit version matrix |
| Async provider on sync driver | `AsyncGcsBearerSecretProvider` on `SyncDuckDBDriver` | Existing `ImproperConfigurationError` ("requires a synchronous `value_provider`") | Sync/async split |
| Persistent rotated secret | `persistent=True` + `value_provider` | `ImproperConfigurationError` at construction | Short-lived tokens never hit disk |
| Negative / NaN interval | `refresh_interval_seconds=-1.0` or `float("nan")` | `ImproperConfigurationError` | No silent clamping |
| Revoked token | Statement fails with `(HTTP 401)` | Provider `invalidate(applied_generation)` called once per dynamic secret exposing it; original error re-raised; next statement re-mints | Fail loud, no silent retry of side-effecting SQL |
| Token endpoint down | `get_token()` raises `GoogleCloudAuthError` | Propagates from refresh when `required=True` | Fail loud |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=mariadb VERTEBRA_CONFORMANCE_MARIADB_VERSION=11.8`, when `pytest src/py/tests/integration/db` runs, then only MariaDB-tagged items run against `mariadb:11.8` and the session fails if any of them skip.
- Given a `GCSConfig` with an ADC file, when a `SyncDuckDBDriver` with `DuckDBSecretConfig(secret_type="gcs", value_provider=SyncGcsBearerSecretProvider.from_config(cfg))` reads `gs://vtb-bucket/data.csv` twice around `provider.invalidate(1)`, then the endpoint records `Bearer tok-1` then `Bearer tok-2`.
- Given a CI pull request touching `src/py/**`, when `test-db-conformance` runs, then every matrix row passes with zero skipped items.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Lane selection | `VERTEBRA_CONFORMANCE_ENGINES` gate deselecting by engine tag | `-k` expressions: `-k mysql` matches MariaDB items in `test_mysql_native.py` and starts unwanted containers; fragile on nodeid text. |
| Zero-skip enforcement | One engine-keyed gate in the integration conftest | Per-engine `*_REQUIRE_ZERO_SKIPS` variables: five near-identical hooks; ch28 keeps its PostgreSQL variable inside its own scope. |
| Version selection | Env var mapped to `pytest-databases` versioned fixtures, one container per CI row | Parametrizing every conformance test over versions: multiplies container starts per job and exceeds runner memory with SQL Server + Oracle. |
| GCS provider location | `vertebra.storage` (owner of `GCSConfig` and token caches) | Inside `vertebra.db.adapters.duckdb`: violates the cloud-agnostic engine driver rule; a sqlspec-only helper: excludes framework-free users. |
| Credential rejection | Invalidate provider and re-raise | Automatic retry: unsafe for side-effecting statements (`COPY ... TO 'gs://...'`, `INSERT ... SELECT`). Ignoring: keeps sending a revoked token until the interval elapses. |
| Live GCS verification | Loopback GCS endpoint plus real `httpfs` | Real GCS bucket: needs cloud credentials in CI; `pytest-databases` Floci GCP: its XML API coverage for `httpfs` is unverified. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/13 files documented]`
- `[ ]` `docs/usage/storage-auth-and-signing.md` — GCS bearer secret providers for DuckDB `httpfs` (`ch05 / t01`).
- `[ ]` `docs/usage/analytics-and-cdc.md` — DuckDB GCS ADC secret example, rotation and credential-rejection semantics (`ch05 / t01`).
- `[ ]` `docs/reference/storage.md` — `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` reference entries (`ch05 / t01`).
- `[ ]` `docs/usage/databases.md` — native-wheel engine statement, DuckDB ADC row, and Oracle listener wait budget (`ch05 / t02`).
- `[ ]` `docs/changelog/index.md` — `Unreleased` entries (`ch05 / t02`).
- `[ ]` `AGENTS.md` — `make db-conformance` in Quick Verification Commands (`ch05 / t02`).
- `[ ]` `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — live conformance gate, engines, versions, CI job, test targets without PyPI `duckdb` (`ch05 / t03`).
- `[ ]` `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — PyPI `duckdb` reachable only through `--group bench` (`ch04 / t03`).
- `[ ]` `.agents/bundles/knowledge/workflow.md` — `make db-conformance` and env variables (`ch05 / t03`).
- `[ ]` `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — DuckDB secret lifecycle and GCS provider seam (`ch05 / t03`).
- `[ ]` `.agents/bundles/knowledge/domains/data/driver-conformance-limitations.md` — new limitations record (`ch05 / t04`).
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — link to the limitations record (`ch05 / t04`).
- `[ ]` `.agents/bundles/knowledge/index.md` — link to the limitations record (`ch05 / t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R1, R2, R3 | `ch01 / t01` | `behavior_tdd` | `src/py/tests/integration/test_conformance_gate.py` |
| R4, R5 | `ch01 / t02` | `behavior_tdd` | `src/py/tests/integration/db/test_driver_conformance.py` |
| R11 | `ch02 / t01` | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py` |
| R12, R13 | `ch02 / t02` | `behavior_tdd` | `src/py/tests/unit/db/test_duckdb_driver.py` |
| R14 | `ch02 / t03` | `integration_acceptance` | `src/py/tests/integration/db/test_duckdb_native.py` |
| R6 | `ch03 / t01` | `integration_acceptance` | `src/py/tests/integration/db/test_mysql_native.py` |
| R7 | `ch03 / t02` | `integration_acceptance` | `src/py/tests/integration/db/test_mssql_native.py` |
| R8 | `ch03 / t03` | `regression_tdd` | `src/py/tests/integration/db/test_oracle_native.py` |
| R16 | `ch03 / t04` | `behavior_tdd` | `src/rs/vertebra-oracle/src/notify.rs` `mod tests` |
| R9 | `ch04 / t01` | `static_validation` | `make -n db-conformance ENGINE=mysql VERSION=8.0` |
| R10 | `ch04 / t02` | `static_validation` | YAML matrix assertion on `.github/workflows/ci.yml` |
| R17 | `ch04 / t03` | `static_validation` | `! grep -n -- '--group duckdb' Makefile`, `make py-test` without PyPI `duckdb` |
| R15 (user docs) | `ch05 / t01`, `ch05 / t02` | `documentation_validation` | `make docs` |
| R15 (OKF) | `ch05 / t03`, `ch05 / t04` | `documentation_validation` | `make review-check` |

## 5. Implementation Plan

- [ ] ch01-live-conformance-harness — Lane-selection and zero-skip conformance gate, versioned `pytest-databases` fixtures, MariaDB conformance engine, and dead-code removal (chapters/ch01-live-conformance-harness/spec.md)
- [ ] ch02-gcs-bearer-secret-rotation — Shipped GCS bearer secret providers, DuckDB secret lifecycle hardening, and live `httpfs` rotation acceptance (chapters/ch02-gcs-bearer-secret-rotation/spec.md)
- [ ] ch03-engine-live-suites — Live MySQL/MariaDB and SQL Server edge suites, Oracle suite tagging, and the Oracle AQ listener wait budget (chapters/ch03-engine-live-suites/spec.md)
- [ ] ch04-make-and-ci-lanes — `make db-conformance`, `oracle-test` cleanup, the `test-db-conformance` CI matrix job, and bench-only PyPI `duckdb` (chapters/ch04-make-and-ci-lanes/spec.md)
- [ ] ch05-docs-and-okf — User docs, `AGENTS.md`, changelog, OKF chapters, and the limitations record (chapters/ch05-docs-and-okf/spec.md)
- **Chapter / Phase Recovery Checkpoint**: `make dev && uv run --no-sync pytest src/py/tests/integration src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/db/test_duckdb_driver.py -q && make lint-all`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `c8722c76` — Initial plan authored; `0/5` chapters and `0/16` tasks completed.
- **Active Chapter**: `ch01-live-conformance-harness` (`0/2` tasks completed).
- **Remaining Plan**: `ch01-live-conformance-harness`, `ch02-gcs-bearer-secret-rotation`, `ch03-engine-live-suites`, `ch04-make-and-ci-lanes`, `ch05-docs-and-okf`.
- **Documentation & OKF Sync**: `[0/13 files documented]`.
- **Active Invariants**: Offline runs never start containers; live lanes run with `VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_ENGINES`; DuckDB adapters stay cloud-agnostic; PostgreSQL fixtures in `src/py/tests/integration/conftest.py` stay untouched.

[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine driver conformance matrix and `_resolve_live_config`.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_mysql_config` — `pytest-databases` service fixtures for MySQL, MariaDB, SQL Server, and Oracle.
[^ci-workflow]: `.github/workflows/ci.yml` — GitHub Actions CI jobs.
[^makefile]: `Makefile` — `duckdb-test`, `mysql-test`, `mssql-test`, `oracle-test`, `py-test`, `coverage`, `install`, and `PY_TEST_GROUPS`.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Cloud-agnostic DuckDB `CREATE SECRET` declaration.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — Dynamic secret refresh by generation, `needs_refresh()`, and interval.
[^gcs-token-cache]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — Native GCP token cache surface.
[^gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — `sync_token_cache` and `async_token_cache` factories.
[^gcs-rotation-tests]: `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_multi_session_token_generation_secret_rotation` — Test-local bearer secret providers.
[^oracle-native]: `src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config` — Live Oracle 26ai suite gate.
[^test-matrix-okf]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Test matrix and conformance knowledge chapter.
[^oracle-notify]: `src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker` — Oracle AQ listener dequeue loop.
[^pg-ch28]: `.agents/bundles/specs/native-postgres-driver/chapters/ch28-live-conformance-and-service-acceptance/spec.md` — PostgreSQL live conformance owned by `native-postgres-driver`.
