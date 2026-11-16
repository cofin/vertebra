"""PostgreSQL synchronous and asynchronous SQLSpec adapter configuration classes."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, ClassVar

from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.postgres.adk import (
    PostgresAsyncADKArtifactStore,
    PostgresAsyncADKMemoryStore,
    PostgresAsyncADKStore,
    PostgresSyncADKArtifactStore,
    PostgresSyncADKMemoryStore,
    PostgresSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.postgres.driver import PostgresAsyncDriver, PostgresSyncDriver
from vertebra.extensions.sqlspec.adapters.postgres.events import (
    PostgresAsyncEventsBackend,
    PostgresEventQueueStore,
    PostgresSyncEventsBackend,
)
from vertebra.extensions.sqlspec.adapters.postgres.litestar import PostgresLitestarStore

__all__ = ("PostgresAsyncConfig", "PostgresSyncConfig")


def _extract_pg_url(connection_config: dict[str, Any], kwargs: dict[str, Any]) -> str | None:
    """Resolve URL/DSN aliases cleanly from `connection_config` or keyword arguments."""
    for source in (kwargs, connection_config):
        for key in ("url", "dsn", "conninfo", "connection_string"):
            val = source.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return None


def _build_pg_connection_config(
    config: PostgreSQLConfig | str | None,
    connection_config: dict[str, Any] | None,
    kwargs: dict[str, Any],
) -> PostgreSQLConfig:
    """Build a native `PostgreSQLConfig` from explicit config, URL/DSN, or keyword options."""
    if isinstance(config, PostgreSQLConfig):
        return config
    merged: dict[str, Any] = dict(connection_config or {})
    merged.update(kwargs)
    url = config if isinstance(config, str) else _extract_pg_url(merged, {})
    for alias in ("url", "dsn", "conninfo", "connection_string"):
        merged.pop(alias, None)
    if url is not None:
        base = PostgreSQLConfig.from_url(url)
        return replace(base, **merged) if merged else base
    return PostgreSQLConfig(**merged) if merged else PostgreSQLConfig()


class PostgresSyncConfig(SyncAdapterConfig[PostgreSQLConfig, PostgresSyncDriver]):
    """Synchronous PostgreSQL SQLSpec adapter configuration over `SyncPostgreSQLDriver`."""

    adapter_name: ClassVar[str] = "vertebra_postgres"
    dialect: ClassVar[str] = "postgres"
    is_async: ClassVar[bool] = False
    supports_transactional_ddl: ClassVar[bool] = True
    _config_cls: ClassVar[type[PostgreSQLConfig]] = PostgreSQLConfig
    _driver_cls: ClassVar[type[SyncPostgreSQLDriver]] = SyncPostgreSQLDriver
    connection_type: ClassVar[type[SyncPostgreSQLDriver]] = SyncPostgreSQLDriver
    driver_type: ClassVar[type[PostgresSyncDriver]] = PostgresSyncDriver
    adk_store_class: ClassVar[type[PostgresSyncADKStore]] = PostgresSyncADKStore
    adk_memory_store_class: ClassVar[type[PostgresSyncADKMemoryStore]] = PostgresSyncADKMemoryStore
    adk_artifact_store_class: ClassVar[type[PostgresSyncADKArtifactStore]] = PostgresSyncADKArtifactStore
    events_store_class: ClassVar[type[PostgresEventQueueStore]] = PostgresEventQueueStore
    events_backend_class: ClassVar[type[PostgresSyncEventsBackend]] = PostgresSyncEventsBackend
    litestar_store_class: ClassVar[type[PostgresLitestarStore]] = PostgresLitestarStore

    def build_connection_config(
        self,
        config: PostgreSQLConfig | str | None,
        connection_config: dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> PostgreSQLConfig:
        """Build `PostgreSQLConfig` supporting `url`/`dsn`/`conninfo`/`connection_string` aliases."""
        return _build_pg_connection_config(config, connection_config, kwargs)

    def derive_direct_config(self) -> PostgresSyncConfig:
        """Return a direct-session `PostgresSyncConfig` bypassing transaction poolers."""
        return PostgresSyncConfig(
            self.driver_config.derive_direct_config(),
            bind_key=self.bind_key or "default",
            broker=self.broker,
            share_pool=False,
            arrow_threshold_rows=self.arrow_threshold_rows,
            statement_config=self.statement_config,
            migration_config=dict(self.migration_config),
            driver_features=dict(self.driver_features),
            extension_config=dict(self.extension_config),
            observability_config=self.observability_config,
        )


class PostgresAsyncConfig(AsyncAdapterConfig[PostgreSQLConfig, PostgresAsyncDriver]):
    """Asynchronous PostgreSQL SQLSpec adapter configuration over `AsyncPostgreSQLDriver`."""

    adapter_name: ClassVar[str] = "vertebra_postgres"
    dialect: ClassVar[str] = "postgres"
    is_async: ClassVar[bool] = True
    supports_transactional_ddl: ClassVar[bool] = True
    _config_cls: ClassVar[type[PostgreSQLConfig]] = PostgreSQLConfig
    _driver_cls: ClassVar[type[AsyncPostgreSQLDriver]] = AsyncPostgreSQLDriver
    connection_type: ClassVar[type[AsyncPostgreSQLDriver]] = AsyncPostgreSQLDriver
    driver_type: ClassVar[type[PostgresAsyncDriver]] = PostgresAsyncDriver
    adk_store_class: ClassVar[type[PostgresAsyncADKStore]] = PostgresAsyncADKStore
    adk_memory_store_class: ClassVar[type[PostgresAsyncADKMemoryStore]] = PostgresAsyncADKMemoryStore
    adk_artifact_store_class: ClassVar[type[PostgresAsyncADKArtifactStore]] = PostgresAsyncADKArtifactStore
    events_store_class: ClassVar[type[PostgresEventQueueStore]] = PostgresEventQueueStore
    events_backend_class: ClassVar[type[PostgresAsyncEventsBackend]] = PostgresAsyncEventsBackend
    litestar_store_class: ClassVar[type[PostgresLitestarStore]] = PostgresLitestarStore

    def build_connection_config(
        self,
        config: PostgreSQLConfig | str | None,
        connection_config: dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> PostgreSQLConfig:
        """Build `PostgreSQLConfig` supporting `url`/`dsn`/`conninfo`/`connection_string` aliases."""
        return _build_pg_connection_config(config, connection_config, kwargs)

    def derive_direct_config(self) -> PostgresAsyncConfig:
        """Return a direct-session `PostgresAsyncConfig` bypassing transaction poolers."""
        return PostgresAsyncConfig(
            self.driver_config.derive_direct_config(),
            bind_key=self.bind_key or "default",
            broker=self.broker,
            share_pool=False,
            arrow_threshold_rows=self.arrow_threshold_rows,
            statement_config=self.statement_config,
            migration_config=dict(self.migration_config),
            driver_features=dict(self.driver_features),
            extension_config=dict(self.extension_config),
            observability_config=self.observability_config,
        )
