---
type: Spec
flow_id: driver-conformance-ci
chapter_id: ch01-duckdb-gcs-secrets-and-oracle-notify
title: "Chapter 01: DuckDB GCS Bearer Secret Rotation and Oracle Notify Wait Budget"
description: "Implement SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider over GcpTokenCache, harden DuckDBSecretConfig validation and 401/403 credential invalidation with fail-closed startup cleanup, and enforce OracleNotifyConfig.wait_timeout_seconds idle session liveness."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - duckdb
  - gcs
  - secrets
  - oracle
  - notifications
sources:
  - id: token-cache
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: "Native GCP token cache with generation, needs_refresh, invalidate, and get_token"
  - id: gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: "GCSConfig sync_token_cache, async_token_cache, and GCS_DEFAULT_SCOPES"
  - id: duckdb-secret
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
    title: "DuckDBSecretConfig declaration and effective_refresh_interval"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: "SyncDuckDBDriver and AsyncDuckDBDriver dynamic secret refresh lifecycle"
  - id: oracle-notify
    resource: src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker
    title: "Oracle AQ listener dequeue loop discarding wait_timeout_seconds"
---

# Specification: Chapter 01 — DuckDB GCS Bearer Secret Rotation and Oracle Notify Wait Budget

<!-- truth: start -->
- **Shipped GCS Secret Providers (`t01`)**: `vertebra.storage.{SyncGcsBearerSecretProvider,AsyncGcsBearerSecretProvider}` wrap `SyncGcpTokenCache` / `AsyncGcpTokenCache`, return `{"bearer_token": <access_token>}`, and expose `from_config(GCSConfig, ...)`, `generation`, `token_cache`, `needs_refresh()`, and `invalidate(failed_generation)` [^token-cache] [^gcs-config].
- **Cloud-Agnostic DuckDB Secret Hardening (`t02`)**: `DuckDBSecretConfig.__post_init__` rejects `persistent=True` + `value_provider` and `bool` / non-finite / `<= 0` `refresh_interval_seconds` with `ImproperConfigurationError`; `SyncDuckDBDriver` and `AsyncDuckDBDriver` invalidate providers on `(HTTP 401)`/`(HTTP 403)` errors without retrying and close `self._client` if initial `required=True` secret refresh raises [^duckdb-secret] [^duckdb-driver].
- **Oracle AQ Idle Liveness (`t03`)**: `vertebra-oracle/src/notify.rs` retains `1s` `DBMS_AQ.DEQUEUE` slices for fast shutdown and executes `SELECT 1 FROM DUAL` whenever cumulative idle time reaches `wait_timeout_seconds.max(1)` [^oracle-notify].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`test_gcs_token_refresh.py` currently relies on private test-local provider stubs and inspects `driver._secret_generations`, while `DuckDBSecretConfig` silently clamps invalid intervals, `SyncDuckDBDriver`/`AsyncDuckDBDriver` leak `self._client` if startup secret refresh fails and keep reusing revoked `401`/`403` tokens, and `vertebra-oracle/src/notify.rs:338` discards `wait_timeout_seconds` [^duckdb-secret] [^duckdb-driver] [^oracle-notify]. See root [`../../spec.md`](../../spec.md).

## 2. Requirements, Concrete Contracts & Architecture

```python
class SyncGcsBearerSecretProvider:
    __slots__ = ("_token_cache",)
    def __init__(self, token_cache: SyncGcpTokenCache) -> None: ...
    @classmethod
    def from_config(
        cls, config: GCSConfig, *, allow_ambient_adc: bool = True, scopes: Sequence[str] = GCS_DEFAULT_SCOPES
    ) -> Self: ...
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
    def from_config(
        cls, config: GCSConfig, *, allow_ambient_adc: bool = True, scopes: Sequence[str] = GCS_DEFAULT_SCOPES
    ) -> Self: ...
    @property
    def generation(self) -> int: ...
    @property
    def token_cache(self) -> AsyncGcpTokenCache: ...
    def needs_refresh(self) -> bool: ...
    def invalidate(self, failed_generation: int) -> None: ...
    async def __call__(self) -> dict[str, str]: ...
```

- **`DuckDBSecretConfig.__post_init__` (`duckdb/config.py`)**: Reject `persistent=True` when `value_provider is not None` (`ImproperConfigurationError`); reject `isinstance(self.refresh_interval_seconds, bool)`, `not math.isfinite(self.refresh_interval_seconds)`, or `self.refresh_interval_seconds <= 0` (`ImproperConfigurationError`), removing silent `max(0.0, ...)` clamping in `effective_refresh_interval`.
- **Credential Rejection & Startup Cleanup (`duckdb/driver.py`)**: Match `_CREDENTIAL_REJECTION_PATTERN = re.compile(r"\(HTTP (?:401|403)\)")` across statement/query/Arrow execution errors; invoke `provider.invalidate(applied_generation)` for every dynamic secret with an applied generation whose provider exposes callable `invalidate`, then re-raise the original exception unchanged. Wrap initial `_refresh_dynamic_secrets()` / `_refresh_dynamic_secrets_async()` during client bootstrap so any raised exception closes `self._client` and resets it to `None` before propagating.
- **Oracle AQ Idle Liveness (`src/rs/vertebra-oracle/src/notify.rs`)**: Track `idle_started_at: Instant` across empty `1s` `DBMS_AQ.DEQUEUE` slices; whenever `idle_started_at.elapsed() >= Duration::from_secs(wait_timeout_seconds.max(1))`, run a session liveness check (`SELECT 1 FROM DUAL`) via an injectable session-ops trait seam so unit tests deterministically verify ping counts and dead-session reconnect/error propagation without live Oracle containers.

## 3. Documentation & OKF Knowledge Plan

Deferred to [`ch04-docs-and-okf-sync`](../ch04-docs-and-okf-sync/spec.md) so `storage.md`, `analytics-and-cdc.md`, `database-drivers-and-dialers.md`, and `database-driver-limitations.md` are edited once.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` GCS bearer providers | `t01` | `src/py/vertebra/storage/{_secrets,__init__}.py` | `pytest src/py/tests/unit/storage/test_gcs_token_refresh.py` |
| `R2` DuckDB secret lifecycle | `t02` | `src/py/vertebra/db/adapters/duckdb/{config,driver}.py` | `pytest src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/storage/test_gcs_token_refresh.py` |
| `R3` Oracle notify wait budget | `t03` | `src/rs/vertebra-oracle/src/notify.rs` | `cargo test -p vertebra-oracle notify:: && pytest src/py/tests/unit/db/test_oracle_adapter.py` |

## 5. Implementation Plan

- [x] [a7face33] [t01-gcs-bearer-secret-providers.md](tasks/t01-gcs-bearer-secret-providers.md) — Implement `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` over `GcpTokenCache` and export from `vertebra.storage`.
- [x] [a7face33] [t02-duckdb-secret-validation-and-invalidation.md](tasks/t02-duckdb-secret-validation-and-invalidation.md) — Add `DuckDBSecretConfig.__post_init__` validation, `(HTTP 401)`/`(HTTP 403)` provider invalidation, and fail-closed startup `_client` cleanup.
- [x] [75f2d8d7] [t03-oracle-notify-wait-budget.md](tasks/t03-oracle-notify-wait-budget.md) — Enforce `OracleNotifyConfig.wait_timeout_seconds` idle session liveness pings in `vertebra-oracle/src/notify.rs`.

## 6. Current State & Remaining Plan Snapshot

- **Status**: All `3` tasks (`t01`–`t03`) completed (`75f2d8d7`, `a7face33`).
- **Fast `< 60s` Offline Chapter Gate**: `UV_NO_SYNC=1 PYO3_PYTHON="$PWD/.venv/bin/python" make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle notify:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings && uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/db/test_oracle_adapter.py -q && uv run --no-sync ruff check src/py/vertebra/storage src/py/vertebra/db/adapters/duckdb src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/db/test_duckdb_driver.py && uv run --no-sync mypy src/py/vertebra/storage src/py/vertebra/db/adapters/duckdb`.

[^token-cache]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — Native GCP token cache with `generation`, `needs_refresh`, `invalidate`, and `get_token`.
[^gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — `GCSConfig` `sync_token_cache`, `async_token_cache`, and `GCS_DEFAULT_SCOPES`.
[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — `DuckDBSecretConfig` declaration and `effective_refresh_interval`.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — `SyncDuckDBDriver` and `AsyncDuckDBDriver` dynamic secret refresh lifecycle.
[^oracle-notify]: `src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker` — Oracle AQ listener dequeue loop discarding `wait_timeout_seconds`.
