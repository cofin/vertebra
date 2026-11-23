"""GCS bearer-token secret providers backed by native GCP token caches."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Self

from vertebra._vertebra import AsyncGcpTokenCache, SyncGcpTokenCache
from vertebra.storage._config import GCS_DEFAULT_SCOPES, GCSConfig

__all__ = (
    "AsyncGcsBearerSecretProvider",
    "SyncGcsBearerSecretProvider",
)


class SyncGcsBearerSecretProvider:
    """Callable synchronous DuckDB GCS secret provider backed by `SyncGcpTokenCache`."""

    __slots__ = ("_token_cache",)

    def __init__(self, token_cache: SyncGcpTokenCache) -> None:
        self._token_cache = token_cache

    @classmethod
    def from_config(
        cls,
        config: GCSConfig,
        *,
        allow_ambient_adc: bool = True,
        scopes: Sequence[str] = GCS_DEFAULT_SCOPES,
    ) -> Self:
        """Build a synchronous secret provider configured from a `GCSConfig`."""
        return cls(config.sync_token_cache(allow_ambient_adc=allow_ambient_adc, scopes=scopes))

    @property
    def generation(self) -> int:
        """Return the current credential generation tracked by the token cache."""
        return int(self._token_cache.generation)

    @property
    def token_cache(self) -> SyncGcpTokenCache:
        """Return the underlying `SyncGcpTokenCache` instance."""
        return self._token_cache

    def needs_refresh(self) -> bool:
        """Return True when the underlying token cache requires refresh."""
        return bool(self._token_cache.needs_refresh())

    def invalidate(self, failed_generation: int) -> None:
        """Invalidate a rejected credential generation on the underlying token cache."""
        self._token_cache.invalidate(failed_generation)

    def __call__(self) -> dict[str, str]:
        """Resolve an OAuth2 bearer token mapping for DuckDB `CREATE SECRET`."""
        snapshot = self._token_cache.get_token()
        return {"bearer_token": str(snapshot.access_token)}


class AsyncGcsBearerSecretProvider:
    """Callable asynchronous DuckDB GCS secret provider backed by `AsyncGcpTokenCache`."""

    __slots__ = ("_token_cache",)

    def __init__(self, token_cache: AsyncGcpTokenCache) -> None:
        self._token_cache = token_cache

    @classmethod
    def from_config(
        cls,
        config: GCSConfig,
        *,
        allow_ambient_adc: bool = True,
        scopes: Sequence[str] = GCS_DEFAULT_SCOPES,
    ) -> Self:
        """Build an asynchronous secret provider configured from a `GCSConfig`."""
        return cls(config.async_token_cache(allow_ambient_adc=allow_ambient_adc, scopes=scopes))

    @property
    def generation(self) -> int:
        """Return the current credential generation tracked by the token cache."""
        return int(self._token_cache.generation)

    @property
    def token_cache(self) -> AsyncGcpTokenCache:
        """Return the underlying `AsyncGcpTokenCache` instance."""
        return self._token_cache

    def needs_refresh(self) -> bool:
        """Return True when the underlying token cache requires refresh."""
        return bool(self._token_cache.needs_refresh())

    def invalidate(self, failed_generation: int) -> None:
        """Invalidate a rejected credential generation on the underlying token cache."""
        self._token_cache.invalidate(failed_generation)

    async def __call__(self) -> dict[str, str]:
        """Asynchronously resolve an OAuth2 bearer token mapping for DuckDB `CREATE SECRET`."""
        snapshot = await self._token_cache.get_token()
        return {"bearer_token": str(snapshot.access_token)}
