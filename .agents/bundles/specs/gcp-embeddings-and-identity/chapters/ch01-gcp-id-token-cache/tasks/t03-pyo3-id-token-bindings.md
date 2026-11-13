---
type: Task
id: "gcp-embeddings-and-identity:ch01:t03"
flow_id: gcp-embeddings-and-identity
chapter_id: ch01-gcp-id-token-cache
title: "Add PyO3 SyncGcpIdTokenCache, AsyncGcpIdTokenCache, and GcpIdTokenSnapshot Bindings"
description: "Expose detached-GIL SyncGcpIdTokenCache, AsyncGcpIdTokenCache, and GcpIdTokenSnapshot classes on vertebra._vertebra."
status: stable
state: completed
commit: a601dee4
last_verified_commit: a601dee4
last_verified_at: "2026-11-13T04:49:30Z"
verification_evidence: "maturin develop + stubtest vertebra._vertebra + pytest (19 passed, 2 skipped)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t02"
files:
  - src/rs/vertebra-py/src/gcp_identity.rs
  - src/rs/vertebra-py/src/lib.rs
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
read_set:
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-py/src/gcp_auth.rs
  - src/rs/vertebra-py/src/errors.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 210
tags:
  - gcp
  - identity
  - pyo3
  - python
sources:
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PyAsyncGcpTokenCache
    title: "Sibling PyO3 access-token cache binding pattern and detached-GIL execution"
  - id: py-lib
    resource: src/rs/vertebra-py/src/lib.rs
    title: "vertebra._vertebra module registration with gil_used = false"
  - id: py-auth-tests
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py
    title: "Python GCP token cache unit suite with _MockTokenServer"
---

# Task `t03`: Add PyO3 `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, and `GcpIdTokenSnapshot` Bindings

## Objective & Scope Boundary

Create `src/rs/vertebra-py/src/gcp_identity.rs` and register `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` on `vertebra._vertebra` (`gil_used = false`) per [`../spec.md` §2](../spec.md) [^py-gcp-auth] [^py-lib]. Out of scope: `_vertebra.pyi` stubs and `vertebra.storage` exports (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_identity.rs` — Rust `GcpIdTokenCache` and `validate_id_token_audience` from `t01`–`t02`.
- `src/rs/vertebra-py/src/gcp_auth.rs` — `build_token_cache`, `block_on_db`, and `spawn_rust_db_task` pattern [^py-gcp-auth].
- `src/rs/vertebra-py/src/errors.rs` — `auth_error_to_py` (`GoogleCloudAuthError`).
- `src/rs/vertebra-py/src/lib.rs` — submodule registration [^py-lib].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — `_MockTokenServer` loopback endpoints [^py-auth-tests].

## Contracts & Edge Cases

- `PyGcpIdTokenSnapshot` (`GcpIdTokenSnapshot`, `frozen, immutable_type, from_py_object`): read-only `id_token`, `audience`, `expires_at_unix_ms`, `generation`; `__repr__` shows `audience`, `expires_at_unix_ms`, and `generation` while never emitting `id_token`.
- `PySyncGcpIdTokenCache` (`SyncGcpIdTokenCache`) and `PyAsyncGcpIdTokenCache` (`AsyncGcpIdTokenCache`): share `Arc<GcpIdTokenCache>` via `to_async()` / `to_sync()`; validate `audience` synchronously via `validate_id_token_audience` raising `PyValueError` before spawning tasks; run sync `get_token` inside `py.detach(|| block_on_db(...))` and async `get_token` via `spawn_rust_db_task` returning `RustDbTaskAwaitable`; map minting/audience-mismatch errors via `auth_error_to_py` (`GoogleCloudAuthError`) and invalid construction options to `PyValueError`.

## Test Matrix

- Parametrized tests in `src/py/tests/unit/storage/test_gcs_token_refresh.py` (extending `_MockTokenServer` with `/identity` and `:generateIdToken` routes): sync/async parity and shared `generation` across `to_async()`/`to_sync()`; `authorized_user` ADC `client_id` success vs Cloud Run audience `GoogleCloudAuthError` naming `client_id`, `target_service_account`, and `programmaticClients`; empty/control-char audience `ValueError` with zero HTTP requests; metadata `500` -> `GoogleCloudAuthError`; token-redacted `repr(snapshot)`.

## Verification

```bash
uv run --no-sync maturin develop --uv && uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -k id_token -q
```

## Acceptance Criteria

- [x] `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` operate under `gil_used = false` with detached-GIL refreshes and exact `ValueError` / `GoogleCloudAuthError` semantics (`R3`).
- [x] Zero regressions in `test_gcs_token_refresh.py`.

[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PyAsyncGcpTokenCache` — Sibling PyO3 access-token cache binding pattern and detached-GIL execution.
[^py-lib]: `src/rs/vertebra-py/src/lib.rs` — `vertebra._vertebra` module registration with `gil_used = false`.
[^py-auth-tests]: `src/py/tests/unit/storage/test_gcs_token_refresh.py` — Python GCP token cache unit suite with `_MockTokenServer`.
