---
type: Task
id: "gcp-embeddings-and-identity:ch02:t02"
chapter_id: ch02-id-token-python-surface
title: Add ID token stubs and vertebra.storage re-exports
description: Declare GcpIdTokenSnapshot, SyncGcpIdTokenCache, and AsyncGcpIdTokenCache in _vertebra.pyi (including __all__) and re-export them from vertebra.storage.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "gcp-embeddings-and-identity:ch02:t01"
read_set:
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/storage/__init__.py
  - src/rs/vertebra-py/src/gcp_identity.rs
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch02-id-token-python-surface/spec.md
files:
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/storage/__init__.py
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 80
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
  - id: py-stub
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: Existing access-token cache stubs and module __all__
  - id: storage-init
    resource: src/py/vertebra/storage/__init__.py
    title: Public storage package exports
tags:
  - stubs
  - python
  - identity
---

# Task t02: Add ID Token Stubs and `vertebra.storage` Re-Exports

## Objective & Scope Boundary

Add typed stubs for the three classes from `ch02:t01` to `src/py/vertebra/_vertebra.pyi` (after `AsyncGcpTokenCache`, plus entries in the stub `__all__` tuple in sorted position) [^py-stub], and re-export them from `src/py/vertebra/storage/__init__.py` with a direct `from vertebra._vertebra import AsyncGcpIdTokenCache, GcpIdTokenSnapshot, SyncGcpIdTokenCache` and sorted `__all__` entries [^storage-init].

Out of scope: `vertebra.storage._store` changes, docs (`ch06`).

## Bounded Read Manifest

- `src/py/vertebra/_vertebra.pyi::GcpTokenSnapshot`, `::SyncGcpTokenCache`, `::AsyncGcpTokenCache`, `::RustDbTaskAwaitable`, module `__all__`.
- `src/py/vertebra/storage/__init__.py`.
- `src/rs/vertebra-py/src/gcp_identity.rs` (signatures from `t01`).
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` (imports block).
- Chapter spec.

## Symbol & Signature Contracts

```python
@final
class GcpIdTokenSnapshot:
    def __new__(cls, *, id_token: str, audience: str, expires_at_unix_ms: int, generation: int) -> GcpIdTokenSnapshot: ...
    @property
    def id_token(self) -> str: ...
    @property
    def audience(self) -> str: ...
    @property
    def expires_at_unix_ms(self) -> int: ...
    @property
    def generation(self) -> int: ...
    def __repr__(self) -> str: ...

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

@final
class AsyncGcpIdTokenCache:
    def __new__(cls, options: dict[str, str] | None = None, *, allow_ambient_adc: bool = True) -> AsyncGcpIdTokenCache: ...
    def to_sync(self) -> SyncGcpIdTokenCache: ...
    @property
    def generation(self) -> int: ...
    @property
    def refresh_skew_ms(self) -> int: ...
    def needs_refresh(self, audience: str, *, now_unix_ms: int | None = None) -> bool: ...
    def invalidate(self, audience: str, failed_generation: int) -> None: ...
    def get_token(self, audience: str, *, force_refresh: bool = False) -> RustDbTaskAwaitable[GcpIdTokenSnapshot]: ...
```

## Edge Cases & Error Semantics

- Stub signatures must match runtime `__text_signature__` exactly; `mypy.stubtest` failures block the task.
- `vertebra.storage` must not import `vertebra._storage` for these names.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_gcs_token_refresh.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_id_token_caches_reexported_from_storage` | `import vertebra.storage as storage; import vertebra._vertebra as native` | `storage.SyncGcpIdTokenCache is native.SyncGcpIdTokenCache`, same for `AsyncGcpIdTokenCache` and `GcpIdTokenSnapshot`; all three names in `storage.__all__` |
| stubtest gate | `uv run python -m mypy.stubtest vertebra._vertebra` | Exit `0` |
| mypy gate | `uv run mypy src/py/vertebra/storage src/py/tests/unit/storage/test_gcs_token_refresh.py` | Exit `0` |

## Verification

```bash
make dev
uv run python -m mypy.stubtest vertebra._vertebra
uv run mypy src/py/vertebra/storage src/py/tests/unit/storage/test_gcs_token_refresh.py
uv run pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q -k id_token
uv run ruff check src/py/vertebra/storage/__init__.py
```

Commit as `feat(storage): re-export GCP ID token caches`.

## Acceptance Criteria

- [ ] Stubs match runtime signatures (`mypy.stubtest` exit `0`) (R3).
- [ ] `vertebra.storage` re-exports the three classes by identity (R3).

## Notes & Discoveries

- None yet.

[^py-stub]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — Stub pattern.
[^storage-init]: `src/py/vertebra/storage/__init__.py` — Public export list.
