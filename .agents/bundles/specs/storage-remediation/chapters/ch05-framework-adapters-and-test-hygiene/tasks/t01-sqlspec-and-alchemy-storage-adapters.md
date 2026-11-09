---
type: Task
id: "storage-remediation:ch05:t01"
chapter_id: ch05-framework-adapters-and-test-hygiene
title: Clean up SQLSpec and Advanced Alchemy ObjectStoreBackend method ordering, DuckDB token validation, and StorageConfig forwarding
description: Delete _IPCBuffer, place public methods strictly above private helpers, check supports_signing, and fail loud on missing DuckDB GCS tokens in extensions/sqlspec/storage.py, and forward signing and token_cache on StorageConfig in extensions/alchemy/storage.py.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/extensions/alchemy/storage.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/extensions/alchemy/storage.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 140
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-09T01:04:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T01:10:40Z"
last_verified_commit: 3b24b0af
verification_evidence: "pytest src/py/tests/unit/extensions (628 passed); ruff check/format and mypy clean"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T01:10:40Z"
commit: 3b24b0af
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-09T01:10:40Z"
    commit: 3b24b0af
sources:
  - id: py-sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: SQLSpec ObjectStoreBackend and DuckDB GCS secret builder
  - id: py-alchemy-storage
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: Advanced Alchemy ObjectStoreBackend
tags:
  - storage
  - sqlspec
  - alchemy
  - tdd
---

# Task t01: SQLSpec and Advanced Alchemy Storage Adapter Hardening

## Objective & Scope Boundary

Update `src/py/vertebra/extensions/sqlspec/storage.py` [^py-sqlspec-storage] and `src/py/vertebra/extensions/alchemy/storage.py` [^py-alchemy-storage] (`2` files):
1. In `extensions/sqlspec/storage.py`: delete `_IPCBuffer` (using `pa.BufferOutputStream()` directly), move all private `_...` methods on `ObjectStoreBackend` strictly below all public sync and async methods (`PY-01`), check `self._sync_store.supports_signing` consistently in `supports_signing`, and raise `ImproperConfigurationError` from `build_duckdb_gcs_secret_sql` when neither `bearer_token` nor `config.resolve_adc_token()` yields a non-empty bearer token.
2. In `extensions/alchemy/storage.py`: preserve Advanced Alchemy's `key: str = "default", fs: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, *, store: ... = None, prefix: str = "", signing: SigningConfig | None = None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None, **kwargs: Any` signature on `ObjectStoreBackend.__init__` (popping/excluding `signing` and `token_cache` before `super().__init__`), forward `signing=signing, token_cache=token_cache` on both `_STORAGE_CONFIGS` (`SyncObjectStore(target, signing=signing, token_cache=token_cache)`) and `str` URI branches, forward `signing` and `token_cache` in `register_storage_backend`, check `self._sync_store.supports_signing` consistently, and ensure all private `_...` methods sit below public methods.

Out of scope: Django storage adapter (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend`, `build_duckdb_gcs_secret_sql` [^py-sqlspec-storage].
- `src/py/vertebra/extensions/alchemy/storage.py` — `ObjectStoreBackend`, `register_storage_backend` [^py-alchemy-storage].
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Canonical SQLSpec and Advanced Alchemy storage integration tests.

## Symbol & Signature Contracts

- `build_duckdb_gcs_secret_sql(secret_name: str = "vertebra_gcs", config: GCSConfig | None = None, *, bearer_token: str | None = None) -> str` raises `ImproperConfigurationError` on missing/blank bearer token.
- `alchemy.storage.ObjectStoreBackend.__init__(self, key: str = "default", fs: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, *, store: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, prefix: str = "", signing: SigningConfig | None = None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None, **kwargs: Any) -> None`
- `alchemy.storage.register_storage_backend(..., signing: SigningConfig | None = None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None) -> ObjectStoreBackend`

## Edge Cases & Error Semantics

- Whitespace-only `bearer_token="   "` in `build_duckdb_gcs_secret_sql` raises `ImproperConfigurationError`; NUL/CR/LF in the token raise `ValueError`; `'` is doubled in the rendered literal.
- Single quotes inside resolved bearer token or secret name are escaped / validated safely.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_data_storage_integrations.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `build_duckdb_gcs_secret_sql` without token | `build_duckdb_gcs_secret_sql("my_secret")` | Raises `ImproperConfigurationError`. |
| Alchemy `ObjectStoreBackend` & `register_storage_backend` with `StorageConfig` + `signing` + `token_cache` | `ObjectStoreBackend("gcs", GCSConfig(bucket="b", endpoint=url, allow_http=True), signing=sign_cfg, token_cache=cache)` | `backend.sync_store.token_cache` and `backend.sync_store.supports_signing` reflect forwarded `token_cache` and `signing`; passing them alongside a prebuilt store raises `ImproperConfigurationError`. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/alchemy/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/alchemy/storage.py
uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q
```

## Acceptance Criteria

- [x] [3b24b0af] `sqlspec/storage.py` has zero `_IPCBuffer`, places all `_...` methods after public methods, uses `supports_signing`, and raises `ImproperConfigurationError` on missing DuckDB GCS tokens (R5.1).
- [x] [3b24b0af] `alchemy/storage.py` forwards `signing` and `token_cache` when given a `StorageConfig` (R5.1).

## Notes & Discoveries

- [2026-11-09 01:10] `build_duckdb_gcs_secret_sql` keeps its existing `(secret_name, config, *, bearer_token) -> (sql, params)` seam with the token bound as a parameter (quote-safe) and raises `ImproperConfigurationError` on a missing/blank token. The Alchemy `ObjectStoreBackend` raises `ImproperConfigurationError` when `signing`/`token_cache` accompany a prebuilt store instead of silently dropping them, and exposes `supports_signing` like the SQLSpec backend.
- [2026-11-08 20:55] Import `ImproperConfigurationError` from `vertebra.typing` (`PY-08`).

[^py-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec `ObjectStoreBackend` and DuckDB GCS secret builder.
[^py-alchemy-storage]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy `ObjectStoreBackend`.
