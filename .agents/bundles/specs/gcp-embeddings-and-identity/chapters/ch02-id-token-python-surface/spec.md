---
type: Spec
parent_flow: gcp-embeddings-and-identity
chapter_id: ch02-id-token-python-surface
title: "Chapter 02: ID Token Python Surface"
description: PyO3 GcpIdTokenSnapshot, SyncGcpIdTokenCache, and AsyncGcpIdTokenCache on vertebra._vertebra with stubs and vertebra.storage re-exports.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-id-token-core
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:35:00Z"
sources:
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PyAsyncGcpTokenCache
    title: Sibling PyO3 access-token cache bindings used as the binding pattern
  - id: py-lib
    resource: src/rs/vertebra-py/src/lib.rs
    title: vertebra._vertebra module registration
  - id: py-stub
    resource: src/py/vertebra/_vertebra.pyi
    title: Native stub with GcpTokenSnapshot, SyncGcpTokenCache, and AsyncGcpTokenCache
  - id: storage-init
    resource: src/py/vertebra/storage/__init__.py
    title: Public re-export home for GCP credential caches
  - id: py-auth-tests
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py
    title: Canonical Python GCP auth tests with _MockTokenServer
tags:
  - gcp
  - identity
  - pyo3
  - python
---

# Chapter 02: ID Token Python Surface

<!-- truth: start -->
- **Scope**: `ch02:t01` adds `src/rs/vertebra-py/src/gcp_identity.rs` with `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache`, registered on `vertebra._vertebra` [^py-gcp-auth] [^py-lib]; `ch02:t02` adds stubs to `_vertebra.pyi` and re-exports from `vertebra.storage` [^py-stub] [^storage-init].
- **Invariants**: sync refreshes detach the GIL through `block_on_db`; async refreshes return `RustDbTaskAwaitable` via `spawn_rust_db_task`; audience validation raises `ValueError` before network I/O; fetch failures raise `GoogleCloudAuthError`; `__repr__` never prints tokens.
- **Verification Gate**: `make dev && uv run pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q -k id_token && uv run python -m mypy.stubtest vertebra._vertebra`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`GcpIdTokenCache` from `ch01` is reachable only from Rust. Python callers (the MCP transports in `mcp-protocol-completeness`, `RemoteEmbedder` in `ch05`) need sync and async handles that share one native cache, matching the `SyncGcpTokenCache` / `AsyncGcpTokenCache` split [^py-gcp-auth].

### 1.2 Non-Goals

- A C-ABI capsule for ID token caches; no other wheel consumes them.
- MCP or HTTP client integration.

### 1.3 Upstream Baseline & Code Analysis

- `src/rs/vertebra-py/src/gcp_auth.rs` defines `build_token_cache` (adds `allow_ambient_adc=false` to options), frozen `#[pyclass]` wrappers over `Arc<NativeGcpTokenCache>`, `get_token` via `spawn_rust_db_task` (async) or `block_on_db` inside `py.detach` (sync), and errors via `crate::errors::auth_error_to_py`.
- `vertebra.storage/__init__.py` re-exports the access-token caches from `vertebra.storage._store`, which imports them from `vertebra._vertebra`.
- `test_gcs_token_refresh.py::_MockTokenServer` [^py-auth-tests] serves `/token`, metadata, and IAM endpoints; `ch02:t01` extends its handler with `/identity` and `:generateIdToken` routes.

Applicable guidance: [pyo3-bridge SKILL.md](../../../../../skills/pyo3-bridge/SKILL.md), [pyo3-ffi-and-batch-dispatch.md](../../../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

```rust
// src/rs/vertebra-py/src/gcp_identity.rs
#[pyclass(name = "GcpIdTokenSnapshot", frozen, immutable_type, from_py_object)]
pub struct PyGcpIdTokenSnapshot { #[pyo3(get)] pub id_token: String, #[pyo3(get)] pub audience: String, #[pyo3(get)] pub expires_at_unix_ms: u64, #[pyo3(get)] pub generation: u64 }
#[pyclass(name = "SyncGcpIdTokenCache", frozen, immutable_type)]
pub struct PySyncGcpIdTokenCache { inner: Arc<GcpIdTokenCache> }
#[pyclass(name = "AsyncGcpIdTokenCache", frozen, immutable_type)]
pub struct PyAsyncGcpIdTokenCache { inner: Arc<GcpIdTokenCache> }
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()>;
```

```python
@final
class SyncGcpIdTokenCache:
    def __new__(cls, options: dict[str, str] | None = None, *, allow_ambient_adc: bool = True) -> SyncGcpIdTokenCache: ...
    def to_async(self) -> AsyncGcpIdTokenCache: ...
    @property
    def generation(self) -> int: ...
    @property
    def refresh_skew_ms(self) -> int: ...
    def needs_refresh(self, audience: str, *, now_unix_ms: int | None = None) -> bool: ...
    def invalidate(self, audience: str, failed_generation: int) -> None: ...
    def get_token(self, audience: str, *, force_refresh: bool = False) -> GcpIdTokenSnapshot: ...
```

### Caller -> Seam -> Collaborator Map

- Python caller -> `AsyncGcpIdTokenCache.get_token(audience)` -> `validate_id_token_audience` (raises `ValueError`) -> `spawn_rust_db_task(GcpIdTokenCache::get_or_refresh)` -> `PyGcpIdTokenSnapshot` or `auth_error_to_py` (`GoogleCloudAuthError`).
- `vertebra.storage` -> re-export of the three classes from `vertebra._vertebra`.

### Edge-Case & Invariant Matrix

| Scenario | Input | Expected | Owner |
| --- | --- | --- | --- |
| User ADC client audience | ADC `authorized_user`, audience equal to its `client_id` | Snapshot from the refresh-token grant | `t01` |
| User ADC other audience | ADC `authorized_user`, audience `https://svc-abc-uc.a.run.app` | `GoogleCloudAuthError` naming the `client_id`, `target_service_account`, and IAP `programmaticClients` | `t01` |
| Empty audience | `get_token("")` | `ValueError`, no request | `t01` |
| Shared cache | `sync = SyncGcpIdTokenCache(opts)`; `async_ = sync.to_async()` | Both see the same `generation` after one refresh | `t01` |
| Fetch failure | Metadata identity returns `500` | `GoogleCloudAuthError` | `t01` |
| Repr | `repr(snapshot)` | Contains `audience` and `generation`, not the token | `t01` |
| Stub drift | `mypy.stubtest vertebra._vertebra` | Exit `0` | `t02` |
| Re-export | `vertebra.storage.SyncGcpIdTokenCache is vertebra._vertebra.SyncGcpIdTokenCache` | `True` | `t02` |

### Alternatives Comparison

- Adding ID token methods to the existing `SyncGcpTokenCache` classes would mix two cache semantics in one Python type; separate classes keep each surface narrow and mirror the Rust split.
- Re-exporting from a new `vertebra.gcp` package would add a public module for three classes; `vertebra.storage` already hosts the GCP credential caches and their docs.

## 3. Documentation & OKF Knowledge Plan

- Documentation for these classes lands in `ch06:t01` (`docs/usage/storage-auth-and-signing.md`, `docs/reference/storage.md`) and `ch06:t02` (`pyo3-ffi-and-batch-dispatch.md`, `object-storage.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical Test |
| --- | --- | --- | --- |
| R3 (bindings) | `t01` | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_id_token_*` |
| R3 (stubs, re-exports) | `t02` | `static_validation` | `mypy.stubtest vertebra._vertebra`; `test_gcs_token_refresh.py::test_id_token_caches_reexported_from_storage` |

## 5. Implementation Plan

- [ ] t01 — PyO3 ID token caches and snapshot (tasks/t01-pyo3-id-token-caches.md)
- [ ] t02 — ID token stubs and `vertebra.storage` re-exports (tasks/t02-id-token-stubs-and-storage-exports.md)
- **Chapter Recovery Checkpoint**: `make dev && uv run python -m mypy.stubtest vertebra._vertebra && uv run pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/2` tasks completed; depends on `ch01` completion.

[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PyAsyncGcpTokenCache` — Binding pattern.
[^py-lib]: `src/rs/vertebra-py/src/lib.rs` — Module registration.
[^py-stub]: `src/py/vertebra/_vertebra.pyi` — Native stub.
[^storage-init]: `src/py/vertebra/storage/__init__.py` — Public re-exports.
[^py-auth-tests]: `src/py/tests/unit/storage/test_gcs_token_refresh.py` — Canonical tests for this chapter.
