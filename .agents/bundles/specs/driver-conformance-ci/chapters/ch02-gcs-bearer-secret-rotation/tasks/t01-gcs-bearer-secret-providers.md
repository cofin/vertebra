---
type: Task
id: "driver-conformance-ci:ch02:t01"
chapter_id: ch02-gcs-bearer-secret-rotation
title: Ship the sync and async GCS bearer secret providers and rotate DuckDB secrets through them
description: Add SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider in vertebra.storage, export them, and replace the test-local bearer providers in the DuckDB rotation tests with the shipped classes using observable assertions.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-conformance-ci:ch01:t02"
read_set:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
files:
  - src/py/vertebra/storage/_secrets.py
  - src/py/vertebra/storage/__init__.py
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
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: GCSConfig token cache factories and GCS_DEFAULT_SCOPES
  - id: token-cache
    resource: src/py/vertebra/_vertebra.pyi::AsyncGcpTokenCache
    title: Sync and async native token caches
  - id: rotation-tests
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_multi_session_token_generation_secret_rotation
    title: Existing DuckDB rotation tests and _MockTokenServer
  - id: probe
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::_probe_provider_generation
    title: Provider generation and needs_refresh probing
tags:
  - storage
  - gcs
  - duckdb
  - tdd
---

# Task t01: Ship the Sync and Async GCS Bearer Secret Providers and Rotate DuckDB Secrets Through Them

## Objective & Scope Boundary

1. Create `src/py/vertebra/storage/_secrets.py` with `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` [^gcs-config] [^token-cache].
2. Export both from `src/py/vertebra/storage/__init__.py` (import block and alphabetically sorted `__all__`).
3. In `src/py/tests/unit/storage/test_gcs_token_refresh.py`, add provider unit tests and rewrite `test_duckdb_multi_session_token_generation_secret_rotation` and `test_async_duckdb_driver_token_generation_rotation` to use the shipped providers (keeping their current `secret_type="http"`, which these tests already execute at HEAD), deleting the test-local `_SyncBearerSecretProvider` / `_SyncProvider` classes and every `driver._secret_generations` assertion [^rotation-tests]. The `gcs` secret type is exercised through live `httpfs` in `t03`.

Out of scope: DuckDB driver/config changes (`t02`), live `httpfs` reads (`t03`), docs (ch05).

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — `GCSConfig.sync_token_cache`, `GCSConfig.async_token_cache`, `GCS_DEFAULT_SCOPES` [^gcs-config].
- `src/py/vertebra/storage/__init__.py` — export block.
- `src/py/vertebra/_vertebra.pyi` — `SyncGcpTokenCache`, `AsyncGcpTokenCache`, `GcpTokenSnapshot` [^token-cache].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — `_MockTokenServer`, `token_server` fixture, the two DuckDB rotation tests [^rotation-tests].
- `src/py/vertebra/db/adapters/duckdb/driver.py` — `_probe_provider_generation`, `_probe_provider_needs_refresh`, `SyncDuckDBDriver.refresh_secrets` [^probe].

## Symbol & Signature Contracts

```python
"""GCS OAuth2 bearer-token secret providers for engines that accept rotating credential mappings."""

from collections.abc import Sequence
from typing import Self

from vertebra._vertebra import AsyncGcpTokenCache, SyncGcpTokenCache
from vertebra.storage._store import GCS_DEFAULT_SCOPES, GCSConfig

__all__ = ("AsyncGcsBearerSecretProvider", "SyncGcsBearerSecretProvider")


class SyncGcsBearerSecretProvider:
    """Synchronous `value_provider` returning `{"bearer_token": <token>}` from a `SyncGcpTokenCache`."""

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
    """Asynchronous `value_provider` returning `{"bearer_token": <token>}` from an `AsyncGcpTokenCache`."""

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
```

- `__init__` raises `TypeError("SyncGcsBearerSecretProvider requires a SyncGcpTokenCache")` (async: `AsyncGcpTokenCache`) when given the other cache type or any other object.
- `from_config` returns `cls(config.sync_token_cache(allow_ambient_adc=allow_ambient_adc, scopes=scopes))` (async: `async_token_cache`).
- `generation` → `int(self._token_cache.generation)`; `needs_refresh()` → `bool(self._token_cache.needs_refresh())`; `invalidate(g)` → `self._token_cache.invalidate(g)`.
- `__call__` → `{"bearer_token": snapshot.access_token}` from `get_token()` (async: `await get_token()`); exceptions from the cache propagate unchanged.
- The driver probe `_probe_provider_generation(provider)` resolves `provider.generation` directly because instances have no `__self__`.

## Edge Cases & Error Semantics

- `SyncDuckDBDriver` given `AsyncGcsBearerSecretProvider` raises the existing `ImproperConfigurationError` matching `requires a synchronous`.
- `AsyncDuckDBDriver` accepts both providers.
- `get_token()` raising `GoogleCloudAuthError` propagates through `provider()` and `SyncDuckDBDriver(...)` construction when the secret is `required=True`.
- The rewritten driver tests keep `secret_type="http"` so they stay independent of `httpfs` installation; observable evidence is the loopback token server's `oauth_calls` count and the DDL rendered by `DuckDBSecretConfig.to_create_secret_sql(provider())`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_gcs_token_refresh.py`. Dependency category: remote-owned OAuth2 endpoint replaced by the existing loopback `_MockTokenServer` (`token_server` fixture) and ADC `authorized_user` files in `tmp_path`.
- Sync/async variants use `@pytest.mark.parametrize` or separate `anyio` tests; no loops over cases.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Sync mint | `SyncGcsBearerSecretProvider.from_config(GCSConfig(bucket="b", application_credentials=str(adc)))()` | `{"bearer_token": "mock-oauth-token-user-1"}`; `provider.generation == 1` |
| Sync invalidate | then `provider.invalidate(1)`; `provider()` | `{"bearer_token": "mock-oauth-token-user-2"}`; `generation == 2`; `token_server.oauth_calls == 2` |
| Async mint | `await AsyncGcsBearerSecretProvider.from_config(cfg)()` | `{"bearer_token": "mock-oauth-token-user-1"}` |
| Wrong cache type | `SyncGcsBearerSecretProvider(cfg.async_token_cache())` | `TypeError` matching `requires a SyncGcpTokenCache` |
| Exported names | `from vertebra.storage import AsyncGcsBearerSecretProvider, SyncGcsBearerSecretProvider` | Import succeeds; both names in `vertebra.storage.__all__` |
| Two drivers share one cache | two `SyncDuckDBDriver`s with providers over one `SyncGcpTokenCache`; `select("SELECT 1 AS v")`; `cache.invalidate(1)`; `select` again on both | Both return `[{"v": 1}]`; `token_server.oauth_calls == 2` after the second round (single-flight across drivers) |
| Async driver rotation | `AsyncDuckDBDriver` with `AsyncGcsBearerSecretProvider`; select; `provider.invalidate(1)`; select | `token_server.oauth_calls == 2`; second rendered DDL contains `BEARER_TOKEN 'mock-oauth-token-user-2'` (captured via `DuckDBSecretConfig.to_create_secret_sql(await provider())`) |
| Sync driver rejects async provider | `SyncDuckDBDriver(DuckDBConfig(secrets=(DuckDBSecretConfig(name="g", secret_type="http", value_provider=AsyncGcsBearerSecretProvider(...)),)))` | `ImproperConfigurationError` matching `requires a synchronous` |

## Verification

- Red: `uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -k "bearer or rotation" -q` fails (import error).
- Green: `make dev && uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q`.
- Private-state check: `! git grep -n "_secret_generations\|_SyncBearerSecretProvider\|class _SyncProvider" -- src/py/tests/unit/storage/test_gcs_token_refresh.py`.
- Lint/type: `uv run --no-sync ruff check src/py/vertebra/storage src/py/tests/unit/storage/test_gcs_token_refresh.py && uv run --no-sync mypy src/py/vertebra/storage`.

## Acceptance Criteria

- [ ] Both providers match the contracts and are exported from `vertebra.storage` (R11).
- [ ] DuckDB rotation tests use the shipped providers with observable assertions only (R11).
- [ ] `vertebra.db.adapters.duckdb` gains no import of `vertebra.storage` (R11).

## Notes & Discoveries

- [2026-11-12 00:30] The existing tests already prove that a cache-wide `invalidate` re-mints for every driver sharing the cache; keep that scenario with the shipped class.

[^gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — Token cache factories and `GCS_DEFAULT_SCOPES`.
[^token-cache]: `src/py/vertebra/_vertebra.pyi::AsyncGcpTokenCache` — Sync and async native token caches.
[^rotation-tests]: `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_multi_session_token_generation_secret_rotation` — Existing rotation tests and `_MockTokenServer`.
[^probe]: `src/py/vertebra/db/adapters/duckdb/driver.py::_probe_provider_generation` — Provider probing.
