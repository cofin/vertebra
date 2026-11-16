"""BigQuery synchronous and asynchronous SQLSpec adapter configuration classes."""

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar

from sqlspec.core import StatementConfig

from vertebra.db.adapters.bigquery import (
    AsyncBigQueryDriver,
    BigQueryConfig,
    SyncBigQueryDriver,
)
from vertebra.db.driver import NotSupportedError
from vertebra.extensions.sqlspec._driver import (
    AsyncAdapterConfig,
    SyncAdapterConfig,
)
from vertebra.extensions.sqlspec.adapters.bigquery.adk import (
    BigQueryAsyncADKMemoryStore,
    BigQueryAsyncADKStore,
    BigQuerySyncADKMemoryStore,
    BigQuerySyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    BIGQUERY_DEFAULT_SCOPES,
    BigQueryConnectionShim,
    default_statement_config,
    resolve_bigquery_config,
)
from vertebra.extensions.sqlspec.adapters.bigquery.driver import (
    BigQueryAsyncDriver,
    BigQuerySyncDriver,
)
from vertebra.extensions.sqlspec.adapters.bigquery.events import BigQueryEventQueueStore
from vertebra.extensions.sqlspec.adapters.bigquery.litestar import BigQueryLitestarStore

_DEFAULT_ARROW_SHM_THRESHOLD_ROWS: int = 256

__all__ = (
    "BigQueryAsyncConfig",
    "BigQuerySyncConfig",
)


def _reject_tenant_predicate(tenant_predicate: Any) -> None:
    if tenant_predicate is not None:
        msg = "BigQuery does not support tenant_predicate scoping"
        raise NotSupportedError(msg)


def _merge_connection_config(
    config: BigQueryConfig | Mapping[str, Any] | str | None,
    connection_config: dict[str, Any] | None,
    *,
    dsn: str | None,
    project_id: str | None,
    dataset_id: str | None,
    location: str,
    credentials_path: str | None,
    scopes: Sequence[str] | None,
    session_enabled: bool,
    maximum_bytes_billed: int | None,
    rest_endpoint: str | None,
    storage_endpoint: str | None,
) -> tuple[BigQueryConfig, dict[str, Any]]:
    resolved = resolve_bigquery_config(
        config if config is not None else connection_config,
        dsn=dsn,
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
        credentials_path=credentials_path,
        scopes=scopes,
        session_enabled=session_enabled,
        maximum_bytes_billed=maximum_bytes_billed,
        rest_endpoint=rest_endpoint,
        storage_endpoint=storage_endpoint,
    )
    cfg_dict = dict(connection_config) if connection_config else {}
    if resolved.project_id:
        cfg_dict.setdefault("project_id", resolved.project_id)
    if resolved.dataset_id:
        cfg_dict.setdefault("dataset_id", resolved.dataset_id)
    cfg_dict.setdefault("location", resolved.location)
    return resolved, cfg_dict


class BigQuerySyncConfig(SyncAdapterConfig[BigQueryConfig, BigQuerySyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra ``SyncBigQueryDriver``."""

    adapter_name: ClassVar[str] = "vertebra_bigquery"
    dialect: ClassVar[str] = "bigquery"
    connection_type: ClassVar[type[Any]] = SyncBigQueryDriver
    _config_cls: ClassVar[type[BigQueryConfig]] = BigQueryConfig
    _driver_cls: ClassVar[type[SyncBigQueryDriver]] = SyncBigQueryDriver
    driver_type: ClassVar[type[BigQuerySyncDriver]] = BigQuerySyncDriver
    adk_store_class: ClassVar[type[Any]] = BigQuerySyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = BigQuerySyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = BigQueryEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = BigQueryLitestarStore

    def __init__(
        self,
        config: BigQueryConfig | Mapping[str, Any] | str | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        dataset_id: str | None = None,
        location: str = "US",
        credentials_path: str | None = None,
        scopes: Sequence[str] = BIGQUERY_DEFAULT_SCOPES,
        session_enabled: bool = False,
        maximum_bytes_billed: int | None = None,
        rest_endpoint: str | None = None,
        storage_endpoint: str | None = None,
        connection_instance: Any = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = _DEFAULT_ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        tenant_predicate: Any = None,
        **kwargs: Any,
    ) -> None:
        _reject_tenant_predicate(tenant_predicate)
        _ = kwargs
        resolved_cfg, merged_conn_cfg = _merge_connection_config(
            config,
            connection_config,
            dsn=dsn,
            project_id=project_id,
            dataset_id=dataset_id,
            location=location,
            credentials_path=credentials_path,
            scopes=scopes,
            session_enabled=session_enabled,
            maximum_bytes_billed=maximum_bytes_billed,
            rest_endpoint=rest_endpoint,
            storage_endpoint=storage_endpoint,
        )
        self._shim_driver: Any = None
        self._connection_shim: BigQueryConnectionShim | None = None
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config or default_statement_config(),
            connection_config=merged_conn_cfg,
            migration_config=migration_config,
            driver_features=driver_features,
            extension_config=extension_config,
            observability_config=observability_config,
        )
        self.connection_instance = connection_instance

    @property
    def project_id(self) -> str | None:
        """Return configured BigQuery project ID."""
        val = getattr(self.driver_config, "project_id", None)
        return str(val) if val else None

    @property
    def dataset_id(self) -> str | None:
        """Return configured BigQuery dataset ID."""
        val = getattr(self.driver_config, "dataset_id", None)
        return str(val) if val else None

    def create_driver(self) -> BigQuerySyncDriver:
        """Instantiate or return cached ``BigQuerySyncDriver`` honoring explicit ``connection_instance``."""
        if self.connection_instance is not None:
            if self._driver_instance is None or getattr(self._driver_instance, "_driver_closed", False):
                self._driver_instance = self._build_adapter_driver(self.connection_instance, shared_pool=False)
            return self._driver_instance
        return super().create_driver()

    def create_connection(self) -> BigQueryConnectionShim:
        """Create or reuse a ``BigQueryConnectionShim`` over a connected ``SyncBigQueryDriver``."""
        if self._connection_shim is not None:
            return self._connection_shim
        native = (
            self.connection_instance if self.connection_instance is not None else SyncBigQueryDriver(self.driver_config)
        )
        connect_fn = getattr(native, "connect", None)
        if callable(connect_fn) and not getattr(native, "is_connected", False):
            connect_fn()
        self._shim_driver = native
        self._connection_shim = BigQueryConnectionShim(native)
        return self._connection_shim

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Iterator[BigQuerySyncDriver]:
        """Yield a connected ``BigQuerySyncDriver`` session."""
        _ = (args, kwargs)
        driver = self.create_driver()
        conn = driver.connection
        connect_fn = getattr(conn, "connect", None)
        if callable(connect_fn) and not getattr(conn, "is_connected", True):
            connect_fn()
        yield driver

    def close(self) -> None:
        """Close cached connection shims and underlying driver resources."""
        if self._shim_driver is not None:
            close_fn = getattr(self._shim_driver, "close", None)
            if callable(close_fn) and self._shim_driver is not self.connection_instance:
                close_fn()
            self._shim_driver = None
        self._connection_shim = None
        super().close()


class BigQueryAsyncConfig(AsyncAdapterConfig[BigQueryConfig, BigQueryAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra ``AsyncBigQueryDriver``."""

    adapter_name: ClassVar[str] = "vertebra_bigquery"
    dialect: ClassVar[str] = "bigquery"
    connection_type: ClassVar[type[Any]] = AsyncBigQueryDriver
    _config_cls: ClassVar[type[BigQueryConfig]] = BigQueryConfig
    _driver_cls: ClassVar[type[AsyncBigQueryDriver]] = AsyncBigQueryDriver
    driver_type: ClassVar[type[BigQueryAsyncDriver]] = BigQueryAsyncDriver
    adk_store_class: ClassVar[type[Any]] = BigQueryAsyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = BigQueryAsyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = BigQueryEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = BigQueryLitestarStore

    def __init__(
        self,
        config: BigQueryConfig | Mapping[str, Any] | str | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        dataset_id: str | None = None,
        location: str = "US",
        credentials_path: str | None = None,
        scopes: Sequence[str] = BIGQUERY_DEFAULT_SCOPES,
        session_enabled: bool = False,
        maximum_bytes_billed: int | None = None,
        rest_endpoint: str | None = None,
        storage_endpoint: str | None = None,
        connection_instance: Any = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = _DEFAULT_ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        tenant_predicate: Any = None,
        **kwargs: Any,
    ) -> None:
        _reject_tenant_predicate(tenant_predicate)
        _ = kwargs
        resolved_cfg, merged_conn_cfg = _merge_connection_config(
            config,
            connection_config,
            dsn=dsn,
            project_id=project_id,
            dataset_id=dataset_id,
            location=location,
            credentials_path=credentials_path,
            scopes=scopes,
            session_enabled=session_enabled,
            maximum_bytes_billed=maximum_bytes_billed,
            rest_endpoint=rest_endpoint,
            storage_endpoint=storage_endpoint,
        )
        self._shim_sync_driver: SyncBigQueryDriver | None = None
        self._connection_shim: BigQueryConnectionShim | None = None
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config or default_statement_config(),
            connection_config=merged_conn_cfg,
            migration_config=migration_config,
            driver_features=driver_features,
            extension_config=extension_config,
            observability_config=observability_config,
        )
        self.connection_instance = connection_instance

    @property
    def project_id(self) -> str | None:
        """Return configured BigQuery project ID."""
        val = getattr(self.driver_config, "project_id", None)
        return str(val) if val else None

    @property
    def dataset_id(self) -> str | None:
        """Return configured BigQuery dataset ID."""
        val = getattr(self.driver_config, "dataset_id", None)
        return str(val) if val else None

    def create_driver(self) -> BigQueryAsyncDriver:
        """Instantiate or return cached ``BigQueryAsyncDriver`` honoring explicit ``connection_instance``."""
        if self.connection_instance is not None:
            if self._driver_instance is None or getattr(self._driver_instance, "_driver_closed", False):
                self._driver_instance = self._build_adapter_driver(self.connection_instance, shared_pool=False)
            return self._driver_instance
        return super().create_driver()

    async def create_connection(self) -> BigQueryConnectionShim:
        """Create and cache a single ``SyncBigQueryDriver`` shim across repeated connection requests."""
        if self._connection_shim is not None and self._shim_sync_driver is not None:
            return self._connection_shim
        sync_driver = SyncBigQueryDriver(self.driver_config)
        sync_driver.connect()
        self._shim_sync_driver = sync_driver
        self._connection_shim = BigQueryConnectionShim(sync_driver)
        return self._connection_shim

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncIterator[BigQueryAsyncDriver]:
        """Yield a connected ``BigQueryAsyncDriver`` session."""
        _ = (args, kwargs)
        driver = self.create_driver()
        conn = driver.connection
        connect_fn = getattr(conn, "connect", None)
        if callable(connect_fn) and not getattr(conn, "is_connected", True):
            maybe = connect_fn()
            if hasattr(maybe, "__await__"):
                await maybe
        yield driver

    async def close(self) -> None:
        """Close the cached sync shim driver and async adapter pool cleanly."""
        if self._shim_sync_driver is not None:
            self._shim_sync_driver.close()
            self._shim_sync_driver = None
        self._connection_shim = None
        await super().close()
