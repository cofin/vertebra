---
type: Task
id: "driver-conformance-ci:ch01:t01"
flow_id: driver-conformance-ci
chapter_id: ch01-duckdb-gcs-secrets-and-oracle-notify
title: "Implement SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider"
description: "Ship sync and async GCS bearer secret providers wrapping GcpTokenCache in vertebra.storage."
status: stable
state: completed
commit: a7face33
last_verified_commit: a7face33
last_verified_at: "2026-11-13T15:25:00Z"
verification_evidence: "pytest test_gcs_token_refresh.py + ruff + mypy clean"
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/storage/_secrets.py
  - src/py/vertebra/storage/__init__.py
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
read_set:
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 185
tags:
  - storage
  - gcs
  - duckdb
  - secrets
sources:
  - id: token-cache
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: "SyncGcpTokenCache and AsyncGcpTokenCache methods"
  - id: gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: "GCSConfig token cache builders and GCS_DEFAULT_SCOPES"
---

# Task `t01`: Implement `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider`

## Objective & Scope Boundary

Implement `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` in `src/py/vertebra/storage/_secrets.py` and re-export them from `vertebra.storage` per [`../spec.md` §2](../spec.md) [^token-cache] [^gcs-config]. Out of scope: modifying `DuckDBSecretConfig` or `duckdb/driver.py` (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/_vertebra.pyi` — `SyncGcpTokenCache` and `AsyncGcpTokenCache` signatures.
- `src/py/vertebra/storage/{_store,__init__}.py` — `GCSConfig.{sync_token_cache,async_token_cache}`, `GCS_DEFAULT_SCOPES`, and package exports.
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — Existing loopback token rotation tests to migrate off private test-local providers.

## Contracts & Edge Cases

- Implement `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` with `__slots__ = ("_token_cache",)` and exact signatures in `../spec.md` §2 (`from_config`, `generation`, `token_cache`, `needs_refresh`, `invalidate`, `__call__`).
- `__call__` fetches a `GcpTokenSnapshot` via `self._token_cache.get_token()` (awaited on async) and returns `{"bearer_token": snapshot.access_token}`; `GoogleCloudAuthError` propagates untouched.
- Replace private `_SyncBearerSecretProvider` / `_AsyncBearerSecretProvider` classes and private `driver._secret_generations` inspection in `test_gcs_token_refresh.py` with public shipped providers and observable DDL/token assertions.

## Test Matrix

- Parameterized sync and async loopback OAuth2 token-server tests verifying `from_config` custom `scopes`/`allow_ambient_adc`, `generation` advancement across `invalidate(1)`, `needs_refresh()` delegation, `GoogleCloudAuthError` propagation, and `ImproperConfigurationError` when passing `AsyncGcsBearerSecretProvider` to `SyncDuckDBDriver`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q && uv run --no-sync ruff check src/py/vertebra/storage src/py/tests/unit/storage/test_gcs_token_refresh.py && uv run --no-sync mypy src/py/vertebra/storage
```

## Acceptance Criteria

- [x] `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` are exported in `vertebra.storage.__all__` with Google-style docstrings and zero inline `#` comments.
- [x] `test_gcs_token_refresh.py` exercises the shipped providers without inspecting private driver attributes.

[^token-cache]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — `SyncGcpTokenCache` and `AsyncGcpTokenCache` methods.
[^gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — `GCSConfig` token cache builders and `GCS_DEFAULT_SCOPES`.
