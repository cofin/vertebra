---
type: Task
id: "storage-remediation:ch05:t01"
chapter_id: ch05-framework-adapters-and-test-hygiene
title: Clean up SQLSpec and Advanced Alchemy ObjectStoreBackend method ordering, DuckDB token validation, and StorageConfig forwarding
description: Delete _IPCBuffer, place public methods strictly above private helpers, check supports_signing, and fail loud on missing DuckDB GCS tokens in extensions/sqlspec/storage.py, and forward signing and token_cache on StorageConfig in extensions/alchemy/storage.py.
status: stable
state: in_progress
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
state_revision: 1
claimed_by: executor
claimed_at: "2026-11-09T01:04:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T01:04:00Z"
commit: null
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
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
1. In `extensions/sqlspec/storage.py`: delete `_IPCBuffer` (using `pa.BufferOutputStream()` directly), move all private `_...` methods on `ObjectStoreBackend` strictly below all public sync and async methods (`PY-01`), check `self._sync_store.supports_signing` consistently in `supports_signing`, and raise `ImproperConfigurationError` from `build_duckdb_gcs_secret_sql` when neither `token_cache` nor `access_token` yields a non-empty bearer token.
2. In `extensions/alchemy/storage.py`: preserve Advanced Alchemy's `key: str = "default", fs: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, *, store: ... = None, prefix: str = "", signing: SigningConfig | None = None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None, **kwargs: Any` signature on `ObjectStoreBackend.__init__` (popping/excluding `signing` and `token_cache` before `super().__init__`), forward `signing=signing, token_cache=token_cache` on both `_STORAGE_CONFIGS` (`SyncObjectStore(target, signing=signing, token_cache=token_cache)`) and `str` URI branches, forward `signing` and `token_cache` in `register_storage_backend`, check `self._sync_store.supports_signing` consistently, and ensure all private `_...` methods sit below public methods.

Out of scope: Django storage adapter (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend`, `build_duckdb_gcs_secret_sql` [^py-sqlspec-storage].
- `src/py/vertebra/extensions/alchemy/storage.py` — `ObjectStoreBackend`, `register_storage_backend` [^py-alchemy-storage].
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Canonical SQLSpec and Advanced Alchemy storage integration tests.

## Symbol & Signature Contracts

- `build_duckdb_gcs_secret_sql(...) -> str` raises `ImproperConfigurationError` on missing/blank bearer token.
- `alchemy.storage.ObjectStoreBackend.__init__(self, key: str = "default", fs: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, *, store: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, prefix: str = "", signing: SigningConfig | None = None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None, **kwargs: Any) -> None`
- `alchemy.storage.register_storage_backend(..., signing: SigningConfig | None = None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None) -> ObjectStoreBackend`

## Edge Cases & Error Semantics

- Whitespace-only `access_token="   "` in `build_duckdb_gcs_secret_sql` raises `ImproperConfigurationError`.
- Single quotes inside resolved bearer token or secret name are escaped / validated safely.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_data_storage_integrations.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `build_duckdb_gcs_secret_sql` without token | `build_duckdb_gcs_secret_sql("my_secret")` | Raises `ImproperConfigurationError`. |
| Alchemy `ObjectStoreBackend` & `register_storage_backend` with `StorageConfig` + `signing` + `token_cache` | `ObjectStoreBackend("gcs", GCSConfig(bucket="b", endpoint=url, allow_http=True), signing=sign_cfg, token_cache=cache)` | `backend.sync_store._gcp_token_cache` and `backend.sync_store.supports_signing` reflect forwarded `token_cache` and `signing`. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/alchemy/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/alchemy/storage.py
uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q
```

## Acceptance Criteria

- [ ] `sqlspec/storage.py` has zero `_IPCBuffer`, places all `_...` methods after public methods, uses `supports_signing`, and raises `ImproperConfigurationError` on missing DuckDB GCS tokens (R5.1).
- [ ] `alchemy/storage.py` forwards `signing` and `token_cache` when given a `StorageConfig` (R5.1).

## Notes & Discoveries

- [2026-11-08 20:55] Import `ImproperConfigurationError` from `vertebra.typing` (`PY-08`).

[^py-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec `ObjectStoreBackend` and DuckDB GCS secret builder.
[^py-alchemy-storage]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy `ObjectStoreBackend`.
