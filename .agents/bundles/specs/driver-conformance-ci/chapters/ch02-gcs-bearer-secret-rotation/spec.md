---
type: Spec
parent_flow: driver-conformance-ci
chapter_id: ch02-gcs-bearer-secret-rotation
title: "Chapter 2: GCS Bearer Secret Rotation"
description: Shipped SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider, DuckDBSecretConfig and credential-rejection hardening in the DuckDB drivers, and live httpfs bearer rotation acceptance against a loopback GCS endpoint.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-live-conformance-harness
replan_trigger: null
current_task: "driver-conformance-ci:ch02:t01"
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T00:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T00:30:00Z"
    commit: c8722c76
sources:
  - id: token-cache
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: Native token cache with generation, needs_refresh, invalidate, get_token
  - id: gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig.sync_token_cache
    title: GCSConfig token cache factories and GCS_DEFAULT_SCOPES
  - id: storage-init
    resource: src/py/vertebra/storage/__init__.py
    title: vertebra.storage public exports
  - id: duckdb-secret
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
    title: DuckDB secret declaration and CREATE SECRET rendering
  - id: duckdb-refresh
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver._refresh_dynamic_secrets
    title: Generation, needs_refresh, and interval-driven secret refresh
  - id: rotation-tests
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py::test_async_duckdb_driver_token_generation_rotation
    title: Rotation tests with test-local providers and private-state assertions
tags:
  - duckdb
  - gcs
  - adc
  - secrets
---

# Chapter 2: GCS Bearer Secret Rotation

<!-- truth: start -->
- `vertebra.storage.SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` wrap `SyncGcpTokenCache` / `AsyncGcpTokenCache`, return `{"bearer_token": <access_token>}`, and expose `generation`, `needs_refresh()`, `invalidate(failed_generation)`, `token_cache`, and `from_config(GCSConfig, ...)` [^token-cache] [^gcs-config] [^storage-init].
- `DuckDBSecretConfig.__post_init__` raises `ImproperConfigurationError` for `persistent=True` with a `value_provider` and for negative or non-finite `refresh_interval_seconds` [^duckdb-secret].
- DuckDB drivers call `invalidate(applied_generation)` on providers that expose it when a statement fails with `(HTTP 401)` or `(HTTP 403)`, then re-raise; they never retry the statement [^duckdb-refresh].
- `src/py/tests/integration/db/test_duckdb_native.py` proves bearer rotation through real `httpfs` (`secret_type="gcs"`) against a loopback GCS endpoint when `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Docs and the `0.3.0` changelog advertise DuckDB `httpfs` GCS access with active ADC rotation, but no shipped object turns a GCP token cache into a DuckDB secret provider. `test_gcs_token_refresh.py` defines private `_SyncBearerSecretProvider` / `_SyncProvider` classes, uses `secret_type="http"`, and asserts on `driver._secret_generations` [^rotation-tests]. The drivers already rotate any provider by `generation`, `needs_refresh()`, or interval [^duckdb-refresh], so the missing pieces are the provider, invalidation on credential rejection, configuration validation, and live `httpfs` evidence. The engine driver stays cloud-agnostic; GCS code lives in `vertebra.storage` next to `GCSConfig` [^gcs-config].

### 1.2 Non-Goals

- GCP imports in `vertebra.db.adapters.duckdb` or `vertebra._duckdb`.
- Automatic statement retry after credential rejection.
- S3/Azure/R2 provider classes (any callable already works as `value_provider`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Token cache | `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache`, `::AsyncGcpTokenCache`, `::GcpTokenSnapshot` [^token-cache] | Remote-owned OAuth2 behind native cache; tests use a loopback token server. |
| GCS config | `src/py/vertebra/storage/_store.py::GCSConfig.sync_token_cache`, `::GCSConfig.async_token_cache`, `::GCS_DEFAULT_SCOPES` [^gcs-config] | Factory inputs for `from_config`. |
| DuckDB secrets | `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig.to_create_secret_sql`, `::DuckDBSecretConfig.effective_refresh_interval` [^duckdb-secret] | Value object rendering DDL. |
| DuckDB refresh | `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver._refresh_dynamic_secrets`, `::AsyncDuckDBDriver._refresh_dynamic_secrets_async`, `::_probe_provider_generation` [^duckdb-refresh] | Port: `value_provider`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database Drivers & Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Object Storage](../../../../knowledge/domains/storage/object-storage.md), `.agents/skills/python-framework/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R11**: Shipped sync/async GCS bearer secret providers exported from `vertebra.storage` (`t01`).
- **R12**: `DuckDBSecretConfig` validation (`t02`).
- **R13**: Credential-rejection invalidation in both DuckDB drivers (`t02`).
- **R14**: Live `httpfs` bearer rotation acceptance (`t03`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
class SyncGcsBearerSecretProvider:
    __slots__ = ("_token_cache",)
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
    __slots__ = ("_token_cache",)
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

class DuckDBSecretConfig(FrozenBaseStruct, frozen=True, gc=True):
    def __post_init__(self) -> None: ...

_CREDENTIAL_REJECTION_PATTERN: Final[re.Pattern[str]] = re.compile(r"\(HTTP (?:401|403)\)")
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Application → `SyncGcsBearerSecretProvider.from_config(GCSConfig(bucket=..., application_credentials=...))` → `GCSConfig.sync_token_cache(allow_ambient_adc=..., scopes=...)`.
- `DuckDBConfig(secrets=(DuckDBSecretConfig(name="gcs_lake", secret_type="gcs", value_provider=provider, scope="gs://lake/"),), extensions=("httpfs",))` → `SyncDuckDBDriver._refresh_dynamic_secrets` → `_probe_provider_generation(provider)` / `_probe_provider_needs_refresh(provider)` → `provider()` → `SyncGcpTokenCache.get_token()` → `DuckDBSecretConfig.to_create_secret_sql({"bearer_token": ...})` → `CREATE OR REPLACE SECRET gcs_lake (TYPE gcs, BEARER_TOKEN '...', SCOPE 'gs://lake/')`.
- Statement raises `DatabaseError("... (HTTP 401)")` → `SyncDuckDBDriver._invalidate_rejected_credentials(exc)` → `provider.invalidate(self._secret_generations[key])` → re-raise → next statement sees `generation` change and re-mints.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Token fetch fails | `get_token()` raises `GoogleCloudAuthError` | Propagates from provider; driver re-raises when `required=True` | Fail loud |
| Async provider on sync driver | `AsyncGcsBearerSecretProvider` with `SyncDuckDBDriver` | Existing `ImproperConfigurationError` ("requires a synchronous `value_provider`"); coroutine closed | Sync/async split |
| Persistent dynamic secret | `persistent=True, value_provider=p` | `ImproperConfigurationError("DuckDB secret 'gcs_lake' cannot combine persistent=True with value_provider; rotated credentials stay in memory")` | Tokens never written to disk |
| Invalid interval | `refresh_interval_seconds` in `(-1.0, float("nan"), float("inf"))` | `ImproperConfigurationError("DuckDB secret 'gcs_lake' refresh_interval_seconds must be a finite number >= 0")` | No silent clamp |
| Credential rejection | Statement error text contains `(HTTP 401)` or `(HTTP 403)` | Each dynamic secret whose provider exposes callable `invalidate` and has an applied generation receives `invalidate(applied_gen)`; original exception re-raised unchanged | No retry, no swallow |
| Non-credential error | `(HTTP 404)` or syntax error | No invalidation | Precise trigger |
| Provider without `invalidate` | Plain callable provider | Skipped silently by design (nothing to invalidate) | Generic port |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a loopback token server and an ADC file, when `SyncGcsBearerSecretProvider.from_config(cfg)()` is called twice around `invalidate(1)`, then it returns `{"bearer_token": "mock-oauth-token-user-1"}` then `{"bearer_token": "mock-oauth-token-user-2"}` and `generation` advances `1 → 2`.
- Given a `SyncDuckDBDriver` with a provider exposing `invalidate`, when `SELECT error('HTTP GET error on ''gs://b/x'' (HTTP 401)')` fails, then the provider records `invalidate(1)` and the `DatabaseError` propagates.
- Given `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1`, when the live suite reads `gs://vtb-bucket/data.csv` twice around `provider.invalidate(1)`, then the loopback endpoint records `Bearer tok-1` then `Bearer tok-2`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Provider shape | Callable object with `generation` / `needs_refresh()` / `invalidate()` probed by the existing driver | `DuckDBSecretConfig.from_gcs(...)`: puts GCS knowledge in the engine adapter; a closure: loses `generation` probing. |
| Module | New `vertebra/storage/_secrets.py` re-exported from `vertebra.storage` | Appending to the 3,299-line `_store.py`: worse locality for an unrelated concern. |
| Rejection detection | Regex on the DuckDB error text `(HTTP 401)` / `(HTTP 403)` | `vendor_code`: DuckDB reports no HTTP status code field; checking only `OperationalError` type: too broad. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; documented in `ch05 / t01` (`storage-auth-and-signing.md`, `analytics-and-cdc.md`, `reference/storage.md`), `ch05 / t03` (`database-drivers-and-dialers.md`), and `ch05 / t04` (limitations).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R11 | [t01](tasks/t01-gcs-bearer-secret-providers.md) | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py` |
| R12, R13 | [t02](tasks/t02-duckdb-secret-lifecycle-hardening.md) | `behavior_tdd` | `src/py/tests/unit/db/test_duckdb_driver.py` |
| R14 | [t03](tasks/t03-live-httpfs-bearer-rotation.md) | `integration_acceptance` | `src/py/tests/integration/db/test_duckdb_native.py` |

## 5. Implementation Plan

- [ ] driver-conformance-ci:ch02:t01 — Ship the sync and async GCS bearer secret providers and rotate DuckDB secrets through them (tasks/t01-gcs-bearer-secret-providers.md)
- [ ] driver-conformance-ci:ch02:t02 — Harden DuckDB secret validation and invalidate providers on credential rejection (tasks/t02-duckdb-secret-lifecycle-hardening.md)
- [ ] driver-conformance-ci:ch02:t03 — Prove bearer rotation through live httpfs against a loopback GCS endpoint (tasks/t03-live-httpfs-bearer-rotation.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/db/test_duckdb_driver.py -q && VERTEBRA_CONFORMANCE_ENGINES=duckdb VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1 uv run --no-sync pytest src/py/tests/integration/db/test_duckdb_native.py -q -rs && uv run --no-sync mypy src/py/vertebra/storage src/py/vertebra/db/adapters/duckdb && uv run --no-sync ruff check src/py/vertebra/storage src/py/vertebra/db/adapters/duckdb src/py/tests/unit/storage src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/integration/db/test_duckdb_native.py` exits `0` (the live step needs network access to install `httpfs`).

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 1`, `plan_commit: null`, HEAD `c8722c76`; `depends_on_chapters: [ch01-live-conformance-harness]`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`0/3` completed).
- **Active Invariants**: `vertebra.db.adapters.duckdb` imports nothing from `vertebra.storage` or GCP modules; tests assert observable behavior (DDL, recorded headers, token-server calls), never `_secret_generations`.

[^token-cache]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — Native token cache surface.
[^gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig.sync_token_cache` — Token cache factories and `GCS_DEFAULT_SCOPES`.
[^storage-init]: `src/py/vertebra/storage/__init__.py` — `vertebra.storage` public exports.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — DuckDB secret declaration.
[^duckdb-refresh]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver._refresh_dynamic_secrets` — Dynamic secret refresh.
[^rotation-tests]: `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_async_duckdb_driver_token_generation_rotation` — Rotation tests with test-local providers.
