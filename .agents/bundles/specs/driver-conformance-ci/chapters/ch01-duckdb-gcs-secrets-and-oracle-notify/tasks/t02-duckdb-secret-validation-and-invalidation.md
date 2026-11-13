---
type: Task
id: "driver-conformance-ci:ch01:t02"
flow_id: driver-conformance-ci
chapter_id: ch01-duckdb-gcs-secrets-and-oracle-notify
title: "Harden DuckDBSecretConfig validation, 401/403 invalidation, and startup cleanup"
description: "Validate DuckDBSecretConfig at construction, invalidate dynamic providers on HTTP 401/403 errors, and close _client on failed initial secret refresh."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "driver-conformance-ci:ch01:t01"
files:
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
read_set:
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/storage/_secrets.py
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 210
tags:
  - duckdb
  - secrets
  - validation
  - lifecycle
sources:
  - id: duckdb-secret
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
    title: "DuckDBSecretConfig declaration and interval handling"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: "SyncDuckDBDriver and AsyncDuckDBDriver secret refresh and client bootstrap"
---

# Task `t02`: Harden `DuckDBSecretConfig` Validation, `401`/`403` Invalidation, and Startup Cleanup

## Objective & Scope Boundary

Implement `DuckDBSecretConfig.__post_init__`, `(HTTP 401)`/`(HTTP 403)` provider invalidation, and fail-closed `_client` cleanup on startup secret-refresh failure per [`../spec.md` §2](../spec.md) [^duckdb-secret] [^duckdb-driver]. Out of scope: adding any GCP imports to `vertebra.db.adapters.duckdb` or auto-retrying failed SQL statements.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/duckdb/{config,driver}.py` — `DuckDBSecretConfig` and sync/async driver secret refresh hooks.
- `src/py/vertebra/storage/_secrets.py` — `invalidate(failed_generation)` contract from `t01`.
- `src/py/tests/unit/{db/test_duckdb_driver,storage/test_gcs_token_refresh}.py` — Existing DuckDB secret and driver lifecycle tests.

## Contracts & Edge Cases

- **`DuckDBSecretConfig.__post_init__`**: Raise `ImproperConfigurationError` if `self.persistent and self.value_provider is not None`, or if `isinstance(self.refresh_interval_seconds, bool) or not math.isfinite(self.refresh_interval_seconds) or self.refresh_interval_seconds <= 0`.
- **Credential Rejection (`_invalidate_rejected_credentials`)**: When any statement raises an exception matching `\(HTTP (?:401|403)\)`, invoke `provider.invalidate(applied_gen)` on each dynamic secret with an applied generation whose `value_provider` exposes callable `invalidate`, then re-raise the original exception; ignore `(HTTP 404)`, syntax errors, and plain callables without `invalidate`.
- **Startup Cleanup**: If initial `_refresh_dynamic_secrets` / `_refresh_dynamic_secrets_async` raises when bootstrapping `self._client`, close `self._client` and clear `self._client = None` before re-raising so no native DuckDB handle leaks.

## Test Matrix

- Parameterized `@pytest.mark.parametrize` cases covering `persistent=True` + `value_provider`, `refresh_interval_seconds` in `(True, False, 0.0, -1.0, float("nan"), float("inf"))`, `(HTTP 401)` and `(HTTP 403)` invalidation followed by next-query secret rotation vs `(HTTP 404)` non-invalidation, and startup `required=True` provider failure closing the underlying client.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/storage/test_gcs_token_refresh.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/duckdb src/py/tests/unit/db/test_duckdb_driver.py && uv run --no-sync mypy src/py/vertebra/db/adapters/duckdb
```

## Acceptance Criteria

- [ ] `vertebra.db.adapters.duckdb` imports zero `vertebra.storage` or GCP symbols while rotating and invalidating `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider`.
- [ ] Invalid secret configs fail at construction, `401`/`403` errors invalidate applied generations, and failed startup refresh leaves zero leaked clients.

[^duckdb-secret]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — `DuckDBSecretConfig` declaration and interval handling.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — `SyncDuckDBDriver` and `AsyncDuckDBDriver` secret refresh and client bootstrap.
