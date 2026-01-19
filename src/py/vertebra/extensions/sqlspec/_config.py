"""Shared SQLSpec broker config helpers."""

from typing import TYPE_CHECKING, Any, ClassVar

from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig

from vertebra.resources import BrokerClientConfig

from ._shared import (
    AsyncBrokerConnection,
    AsyncBrokerPool,
    BrokerAsyncConnectionContext,
    BrokerAsyncSessionContext,
    BrokerSyncConnectionContext,
    BrokerSyncSessionContext,
    SyncBrokerConnection,
    SyncBrokerPool,
)

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


class VertebraSyncConfigBase(SyncDatabaseConfig[SyncBrokerConnection, SyncBrokerPool, Any]):
    """Base class for Vertebra broker-backed sync SQLSpec configs."""

    driver_type: ClassVar[type[Any]]
    connection_type: ClassVar[type[Any]] = SyncBrokerConnection
    default_statement_config: ClassVar["StatementConfig"]
    default_pool_name: ClassVar[str]

    def __init__(
        self,
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        max_clients: int = 5,
        connection_instance: "SyncBrokerPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        self.pool = pool or type(self).default_pool_name
        self.client_config = client_config or BrokerClientConfig()
        self.max_clients = max_clients
        super().__init__(
            connection_config={"pool": self.pool},
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or type(self).default_statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_connection(self) -> "SyncBrokerConnection":
        pool = self.provide_pool()
        return pool.acquire()

    def provide_connection(self, *args: Any, **kwargs: Any) -> "BrokerSyncConnectionContext":
        _ = (args, kwargs)
        return BrokerSyncConnectionContext(self.provide_pool())

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "BrokerSyncSessionContext":
        _ = (args, kwargs)
        handler = _SyncSessionHandler(self)
        return BrokerSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
            driver_type=type(self).driver_type,
        )

    def _create_pool(self) -> "SyncBrokerPool":
        return SyncBrokerPool(self.pool, self.client_config, max_clients=self.max_clients)

    def _close_pool(self) -> None:
        if self.connection_instance is not None:
            self.connection_instance.close()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "BrokerClientConfig": BrokerClientConfig,
            "BrokerSyncConnectionContext": BrokerSyncConnectionContext,
            "BrokerSyncSessionContext": BrokerSyncSessionContext,
            "SyncBrokerConnection": SyncBrokerConnection,
            "SyncBrokerPool": SyncBrokerPool,
        })
        return namespace


class VertebraAsyncConfigBase(AsyncDatabaseConfig[AsyncBrokerConnection, AsyncBrokerPool, Any]):
    """Base class for Vertebra broker-backed async SQLSpec configs."""

    driver_type: ClassVar[type[Any]]
    connection_type: ClassVar[type[Any]] = AsyncBrokerConnection
    default_statement_config: ClassVar["StatementConfig"]
    default_pool_name: ClassVar[str]

    def __init__(
        self,
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        max_clients: int = 5,
        connection_instance: "AsyncBrokerPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        self.pool = pool or type(self).default_pool_name
        self.client_config = client_config or BrokerClientConfig()
        self.max_clients = max_clients
        super().__init__(
            connection_config={"pool": self.pool},
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or type(self).default_statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    async def create_connection(self) -> "AsyncBrokerConnection":
        pool = self.provide_pool()
        return await pool.acquire()

    def provide_connection(self, *args: Any, **kwargs: Any) -> "BrokerAsyncConnectionContext":
        _ = (args, kwargs)
        return BrokerAsyncConnectionContext(self.provide_pool())

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "BrokerAsyncSessionContext":
        _ = (args, kwargs)
        handler = _AsyncSessionHandler(self)
        return BrokerAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
            driver_type=type(self).driver_type,
        )

    def _create_pool(self) -> "AsyncBrokerPool":
        return AsyncBrokerPool(self.pool, self.client_config, max_clients=self.max_clients)

    async def _close_pool(self) -> None:
        if self.connection_instance is not None:
            await self.connection_instance.close()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "BrokerClientConfig": BrokerClientConfig,
            "BrokerAsyncConnectionContext": BrokerAsyncConnectionContext,
            "BrokerAsyncSessionContext": BrokerAsyncSessionContext,
            "AsyncBrokerConnection": AsyncBrokerConnection,
            "AsyncBrokerPool": AsyncBrokerPool,
        })
        return namespace


class _SyncSessionHandler:
    __slots__ = ("_config",)

    def __init__(self, config: VertebraSyncConfigBase) -> None:
        self._config = config

    def acquire_connection(self) -> "SyncBrokerConnection":
        pool = self._config.provide_pool()
        return pool.acquire()

    def release_connection(self, connection: "SyncBrokerConnection") -> None:
        pool = self._config.provide_pool()
        pool.release(connection)


class _AsyncSessionHandler:
    __slots__ = ("_config",)

    def __init__(self, config: VertebraAsyncConfigBase) -> None:
        self._config = config

    async def acquire_connection(self) -> "AsyncBrokerConnection":
        pool = self._config.provide_pool()
        return await pool.acquire()

    async def release_connection(self, connection: "AsyncBrokerConnection") -> None:
        pool = self._config.provide_pool()
        await pool.release(connection)


__all__ = ("VertebraAsyncConfigBase", "VertebraSyncConfigBase")
