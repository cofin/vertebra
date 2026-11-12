---
type: Task
id: "gcp-embeddings-and-identity:ch02:t01"
chapter_id: ch02-id-token-python-surface
title: Expose sync and async ID token caches through PyO3
description: Add vertebra-py gcp_identity.rs with GcpIdTokenSnapshot, SyncGcpIdTokenCache, and AsyncGcpIdTokenCache over Arc<GcpIdTokenCache>, register them on vertebra._vertebra, and cover them with mock-server Python tests.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t04"
read_set:
  - src/rs/vertebra-py/src/gcp_auth.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-py/src/errors.rs
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch02-id-token-python-surface/spec.md
files:
  - src/rs/vertebra-py/src/gcp_identity.rs
  - src/rs/vertebra-py/src/lib.rs
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 230
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
commit: null
sources:
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::build_token_cache
    title: allow_ambient_adc option injection and sync/async binding pattern
  - id: core-identity
    resource: src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache
    title: Native per-audience ID token cache from ch01
  - id: py-errors
    resource: src/rs/vertebra-py/src/errors.rs::auth_error_to_py
    title: GoogleCloudAuthError mapping for auth failures
  - id: py-auth-tests
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py::_MockTokenServer
    title: Mock OAuth, metadata, and IAM server
tags:
  - pyo3
  - identity
  - free-threaded
---

# Task t01: Expose Sync and Async ID Token Caches Through PyO3

## Objective & Scope Boundary

Create `src/rs/vertebra-py/src/gcp_identity.rs` and register it from `src/rs/vertebra-py/src/lib.rs` (`mod gcp_identity;` plus `gcp_identity::register(m)?` next to `gcp_auth::register(m)?`). The bindings wrap `vertebra_core::gcp_identity::GcpIdTokenCache` [^core-identity] following `gcp_auth.rs` [^py-gcp-auth]. Extend `_MockTokenServer` in the canonical test file with ID token routes.

Out of scope: `.pyi` stubs and `vertebra.storage` re-exports (`t02`). Tests in this task import the classes directly from `vertebra._vertebra`; pytest passes at runtime, and mypy over the test module passes once `t02` lands the stubs, which is why the chapter checkpoint (not this task) runs `make lint-all`.

## Bounded Read Manifest

- `src/rs/vertebra-py/src/gcp_auth.rs::build_token_cache`, `::PySyncGcpTokenCache`, `::PyAsyncGcpTokenCache`, `::PyGcpTokenSnapshot`, `::register`.
- `src/rs/vertebra-py/src/lib.rs` (module declarations and `gcp_auth::register(m)?` call site).
- `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache`, `::validate_id_token_audience`, `::GcpIdTokenSnapshot`.
- `src/rs/vertebra-py/src/errors.rs::auth_error_to_py` [^py-errors].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py::_MockTokenServer`, `::token_server` [^py-auth-tests].
- Chapter spec.

## Symbol & Signature Contracts

```rust
use vertebra_core::gcp_identity::{
    GcpIdTokenCache as NativeGcpIdTokenCache, GcpIdTokenSnapshot as NativeGcpIdTokenSnapshot,
    validate_id_token_audience,
};

#[pyclass(name = "GcpIdTokenSnapshot", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct PyGcpIdTokenSnapshot {
    #[pyo3(get)] pub id_token: String,
    #[pyo3(get)] pub audience: String,
    #[pyo3(get)] pub expires_at_unix_ms: u64,
    #[pyo3(get)] pub generation: u64,
}
impl From<&NativeGcpIdTokenSnapshot> for PyGcpIdTokenSnapshot;
#[pymethods] impl PyGcpIdTokenSnapshot {
    #[new] #[pyo3(signature = (*, id_token, audience, expires_at_unix_ms, generation))]
    fn new(id_token: String, audience: String, expires_at_unix_ms: u64, generation: u64) -> Self;
    fn __repr__(&self) -> String; // "GcpIdTokenSnapshot(audience='...', generation=N, expires_at_unix_ms=M)"
}

fn build_id_token_cache(py: Python<'_>, options: Option<HashMap<String, String>>, allow_ambient_adc: bool)
    -> PyResult<Arc<NativeGcpIdTokenCache>>; // inserts allow_ambient_adc=false when requested; py.detach(from_options); Err -> PyValueError
fn checked_audience(audience: &str) -> PyResult<String>; // validate_id_token_audience -> PyValueError

#[pyclass(name = "SyncGcpIdTokenCache", frozen, immutable_type)]
pub struct PySyncGcpIdTokenCache { inner: Arc<NativeGcpIdTokenCache> }
#[pymethods] impl PySyncGcpIdTokenCache {
    #[new] #[pyo3(signature = (options=None, *, allow_ambient_adc=true))]
    fn new(py: Python<'_>, options: Option<HashMap<String, String>>, allow_ambient_adc: bool) -> PyResult<Self>;
    fn to_async(&self) -> PyAsyncGcpIdTokenCache;
    #[getter] fn generation(&self) -> u64;
    #[getter] fn refresh_skew_ms(&self) -> u64;
    #[pyo3(signature = (audience, *, now_unix_ms=None))]
    fn needs_refresh(&self, audience: &str, now_unix_ms: Option<u64>) -> PyResult<bool>;
    fn invalidate(&self, audience: &str, failed_generation: u64) -> PyResult<()>;
    #[pyo3(signature = (audience, *, force_refresh=false))]
    fn get_token(&self, py: Python<'_>, audience: &str, force_refresh: bool) -> PyResult<PyGcpIdTokenSnapshot>;
}
#[pyclass(name = "AsyncGcpIdTokenCache", frozen, immutable_type)]
pub struct PyAsyncGcpIdTokenCache { inner: Arc<NativeGcpIdTokenCache> }
// same methods; to_sync(); get_token -> PyResult<RustDbTaskAwaitable> via spawn_rust_db_task
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()>;
```

Sync `get_token`: returns the cached snapshot without detaching when `get_cached(audience, current_unix_ms())` hits and `force_refresh` is false; otherwise `py.detach(|| block_on_db(cache.get_or_refresh(&aud, force_refresh)))` and maps errors with `auth_error_to_py`.

## Edge Cases & Error Semantics

- Construction errors (no credentials, rejected source, bad option values) raise `ValueError` with the native message.
- Audience errors raise `ValueError` before any task spawn.
- Upstream HTTP failures raise `GoogleCloudAuthError`.
- `get_token` on the async class from a thread with no running loop follows existing `RustDbTaskAwaitable` semantics (awaitable only inside a loop).
- Free-threaded builds: classes are `frozen` with no interior Python state, so concurrent `get_token` calls from many threads share the native single flight.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_gcs_token_refresh.py`. `_MockTokenServer` gains `identity_calls`, `generate_id_token_calls`, `last_identity_query: dict[str, list[str]]`, and `id_token_audience_override: str | None`; `do_GET` serves `/computeMetadata/v1/instance/service-accounts/<account>/identity` returning an unsigned test JWT (`header.payload.sig` with base64url JSON `{"aud": <audience or override>, "exp": now+3600}`), and `do_POST` serves `:generateIdToken` returning `{"token": <jwt>}` and `/token` returning `{"id_token": <jwt>}` when the posted assertion carries `target_audience`. A module-level `_make_test_jwt(audience: str, exp: int) -> str` helper builds tokens.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_id_token_metadata_sync_and_async_share_cache` | `SyncGcpIdTokenCache({"use_metadata_server": "true", "metadata_host": server.base_url})`; `get_token("https://svc.run.app")` then `await cache.to_async().get_token("https://svc.run.app")` | Same `id_token`; `audience == "https://svc.run.app"`; `server.identity_calls == 1`; `last_identity_query == {"audience": ["https://svc.run.app"], "format": ["full"]}` |
| `test_id_token_impersonated_post_body` | Options with `target_service_account="svc@p.iam.gserviceaccount.com"`, `iam_endpoint=server.base_url`, metadata base | `generate_id_token_calls == 1`; `json.loads(server.last_post_body) == {"audience": "https://svc", "includeEmail": True}` |
| `test_id_token_authorized_user_client_audience` | ADC file `{"type": "authorized_user", "client_id": "123-abc.apps.googleusercontent.com", "client_secret": "s", "refresh_token": "r", "token_uri": server.base_url + "/token"}` via `GOOGLE_APPLICATION_CREDENTIALS`; `SyncGcpIdTokenCache().get_token("123-abc.apps.googleusercontent.com")` | Snapshot `audience == "123-abc.apps.googleusercontent.com"`; posted form has `grant_type=refresh_token` and no `scope` |
| `test_id_token_authorized_user_audience_mismatch` | Same ADC file; `get_token("https://svc-abc-uc.a.run.app")` | `pytest.raises(GoogleCloudAuthError)` whose message contains `'123-abc.apps.googleusercontent.com' (the ADC client_id)`, `target_service_account`, and `IAP programmaticClients`; no request reaches `/token` |
| `test_id_token_invalid_audience_raises_before_io` | `get_token("")`, `get_token("a" * 2049)` | `ValueError`; `identity_calls == 0` |
| `test_id_token_audience_mismatch_raises_auth_error` | `id_token_audience_override = "https://other"` | `pytest.raises(GoogleCloudAuthError, match="audience mismatch")` |
| `test_id_token_invalidate_and_repr` | After one fetch, `invalidate(aud, snap.generation)` then `get_token(aud)` | `identity_calls == 2`; second `generation == snap.generation + 1`; `snap.id_token not in repr(snap)` |
| `test_id_token_concurrent_threads_single_flight` | `16` threads (`ThreadPoolExecutor`) call `get_token("https://svc")` on one sync cache | `identity_calls == 1`; all results equal |

## Verification

```bash
make dev
uv run pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q -k id_token
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
uv run ruff check src/py/tests/unit/storage/test_gcs_token_refresh.py
```

Write the tests first (Red: `ImportError` for the new names), implement, rebuild, re-run. Commit as `feat(py): expose GCP ID token caches`.

## Acceptance Criteria

- [ ] `vertebra._vertebra` exposes `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache` sharing one native cache across `to_async`/`to_sync` (R3).
- [ ] Sync refresh detaches the GIL; async refresh returns `RustDbTaskAwaitable` (R3).
- [ ] Audience validation raises `ValueError`; fetch failures raise `GoogleCloudAuthError`; repr hides tokens (R3).

## Notes & Discoveries

- [2026-11-12 00:35] `t02` lands the stubs; run `t01` and `t02` back to back so `make lint-all` (mypy over tests) stays green at the chapter checkpoint.

[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::build_token_cache` — Binding pattern copied here.
[^core-identity]: `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache` — Native cache.
[^py-errors]: `src/rs/vertebra-py/src/errors.rs::auth_error_to_py` — Auth error mapping.
[^py-auth-tests]: `src/py/tests/unit/storage/test_gcs_token_refresh.py::_MockTokenServer` — Mock server extended here.
