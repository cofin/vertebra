"""Oracle synchronous and asynchronous SQLSpec adapter configuration classes."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, ClassVar

from vertebra.db.adapters.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.oracle.adk import (
    OracleAsyncADKArtifactStore,
    OracleAsyncADKMemoryStore,
    OracleAsyncADKStore,
    OracleSyncADKArtifactStore,
    OracleSyncADKMemoryStore,
    OracleSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.oracle.core import OracleVersionCache
from vertebra.extensions.sqlspec.adapters.oracle.driver import OracleAsyncDriver, OracleSyncDriver
from vertebra.extensions.sqlspec.adapters.oracle.events import (
    OracleAsyncEventsBackend,
    OracleEventQueueStore,
    OracleSyncEventsBackend,
)
from vertebra.extensions.sqlspec.adapters.oracle.litestar import OracleLitestarStore

__all__ = ("OracleAsyncConfig", "OracleSyncConfig")


def _extract_oracle_url(connection_config: dict[str, Any], kwargs: dict[str, Any]) -> str | None:
    """Resolve Oracle URL/DSN aliases cleanly from `connection_config` or kwargs."""
    for source in (kwargs, connection_config):
        for key in ("url", "dsn", "connection_string"):
            val = source.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return None


def _build_oracle_connection_config(
    config: OracleConfig | str | None,
    connection_config: dict[str, Any] | None,
    kwargs: dict[str, Any],
) -> OracleConfig:
    """Build native `OracleConfig` from explicit config, URL/DSN, or keyword options."""
    if isinstance(config, OracleConfig):
        return config
    merged: dict[str, Any] = dict(connection_config or {})
    merged.update(kwargs)
    url = config if isinstance(config, str) else _extract_oracle_url(merged, {})
    for alias in ("url", "dsn", "connection_string"):
        merged.pop(alias, None)
    if url is not None:
        base = OracleConfig.from_url(url)
        return replace(base, **merged) if merged else base
    return OracleConfig(**merged) if merged else OracleConfig()


class OracleSyncConfig(SyncAdapterConfig[OracleConfig, OracleSyncDriver]):
    """Synchronous Oracle SQLSpec adapter configuration over `SyncOracleDriver`."""

    adapter_name: ClassVar[str] = "vertebra_oracle"
    dialect: ClassVar[str] = "oracle"
    is_async: ClassVar[bool] = False
    supports_transactional_ddl: ClassVar[bool] = False
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[SyncOracleDriver]] = SyncOracleDriver
    connection_type: ClassVar[type[SyncOracleDriver]] = SyncOracleDriver
    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver
    adk_store_class: ClassVar[type[OracleSyncADKStore]] = OracleSyncADKStore
    adk_memory_store_class: ClassVar[type[OracleSyncADKMemoryStore]] = OracleSyncADKMemoryStore
    adk_artifact_store_class: ClassVar[type[OracleSyncADKArtifactStore]] = OracleSyncADKArtifactStore
    events_store_class: ClassVar[type[OracleEventQueueStore]] = OracleEventQueueStore
    events_backend_class: ClassVar[type[OracleSyncEventsBackend]] = OracleSyncEventsBackend
    litestar_store_class: ClassVar[type[OracleLitestarStore]] = OracleLitestarStore

    def __init__(self, config: OracleConfig | str | None = None, **kwargs: Any) -> None:
        """Initialize `OracleSyncConfig` with a pool-scoped `OracleVersionCache`."""
        resolved = _build_oracle_connection_config(config, None, {}) if isinstance(config, str) else config
        super().__init__(resolved, **kwargs)
        self._oracle_version_cache = OracleVersionCache()

    def build_connection_config(
        self,
        config: OracleConfig | str | None,
        connection_config: dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> OracleConfig:
        """Build `OracleConfig` supporting `url`/`dsn`/`connection_string` aliases."""
        return _build_oracle_connection_config(config, connection_config, kwargs)


class OracleAsyncConfig(AsyncAdapterConfig[OracleConfig, OracleAsyncDriver]):
    """Asynchronous Oracle SQLSpec adapter configuration over `AsyncOracleDriver`."""

    adapter_name: ClassVar[str] = "vertebra_oracle"
    dialect: ClassVar[str] = "oracle"
    is_async: ClassVar[bool] = True
    supports_transactional_ddl: ClassVar[bool] = False
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[AsyncOracleDriver]] = AsyncOracleDriver
    connection_type: ClassVar[type[AsyncOracleDriver]] = AsyncOracleDriver
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver
    adk_store_class: ClassVar[type[OracleAsyncADKStore]] = OracleAsyncADKStore
    adk_memory_store_class: ClassVar[type[OracleAsyncADKMemoryStore]] = OracleAsyncADKMemoryStore
    adk_artifact_store_class: ClassVar[type[OracleAsyncADKArtifactStore]] = OracleAsyncADKArtifactStore
    events_store_class: ClassVar[type[OracleEventQueueStore]] = OracleEventQueueStore
    events_backend_class: ClassVar[type[OracleAsyncEventsBackend]] = OracleAsyncEventsBackend
    litestar_store_class: ClassVar[type[OracleLitestarStore]] = OracleLitestarStore

    def __init__(self, config: OracleConfig | str | None = None, **kwargs: Any) -> None:
        """Initialize `OracleAsyncConfig` with a pool-scoped `OracleVersionCache`."""
        resolved = _build_oracle_connection_config(config, None, {}) if isinstance(config, str) else config
        super().__init__(resolved, **kwargs)
        self._oracle_version_cache = OracleVersionCache()

    def build_connection_config(
        self,
        config: OracleConfig | str | None,
        connection_config: dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> OracleConfig:
        """Build `OracleConfig` supporting `url`/`dsn`/`connection_string` aliases."""
        return _build_oracle_connection_config(config, connection_config, kwargs)
