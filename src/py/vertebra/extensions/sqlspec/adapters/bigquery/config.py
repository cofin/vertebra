"""BigQuery synchronous and asynchronous SQLSpec adapter configuration classes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from urllib.parse import unquote, urlsplit

from sqlspec.core import StatementConfig

from vertebra.db.adapters.bigquery import (
    AsyncBigQueryDriver,
    BigQueryConfig,
    SyncBigQueryDriver,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.driver import NotSupportedError
from vertebra.extensions.sqlspec._driver import (
    AsyncAdapterConfig,
    LazyStoreAttr,
    SyncAdapterConfig,
)
from vertebra.extensions.sqlspec.adapters.bigquery._typing import (
    BigQueryAsyncConnectionContext,
    BigQueryAsyncSessionContext,
    BigQueryConnectionParams,
    BigQueryDriverFeatures,
    BigQueryPoolParams,
    BigQuerySyncConnectionContext,
    BigQuerySyncSessionContext,
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
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from uuid import UUID

__all__ = (
    "BigQueryAsyncConfig",
    "BigQuerySyncConfig",
)

_DERIVE_META_KEYS: frozenset[str] = frozenset(
    {
        "arrow_threshold_rows",
        "bind_key",
        "broker",
        "connection_config",
        "connection_config_overrides",
        "connection_instance",
        "driver_features",
        "driver_features_overrides",
        "extension_config",
        "is_replica",
        "migration_config",
        "observability_config",
        "replicas",
        "reset_session_on_release",
        "share_pool",
        "statement_config",
    }
)
_BIGQUERY_FEATURE_KEYS: frozenset[str] = frozenset(
    {
        "enable_native_storage",
        "job_retry_deadline",
        "json_deserializer",
        "json_serializer",
        "native_export_connection",
        "on_connection_create",
        "reset_session_on_release",
        "storage_backend",
    }
)


def _reject_tenant_predicate(tenant_predicate: Any) -> None:
    if tenant_predicate is not None:
        msg = "BigQuery does not support tenant_predicate scoping"
        raise NotSupportedError(msg)


def _merge_connection_config(
    config: BigQueryConfig | BigQueryConnectionParams | Mapping[str, Any] | str | None,
    connection_config: BigQueryConnectionParams | BigQueryPoolParams | Mapping[str, Any] | None,
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
    if isinstance(config, Mapping):
        for key, value in config.items():
            cfg_dict.setdefault(key, value)
    if resolved.project_id:
        cfg_dict.setdefault("project_id", resolved.project_id)
        cfg_dict.setdefault("project", resolved.project_id)
    if resolved.dataset_id:
        cfg_dict.setdefault("dataset_id", resolved.dataset_id)
    cfg_dict.setdefault("location", resolved.location)
    return resolved, cfg_dict


def _prepare_bigquery_init(
    config: BigQueryConfig | BigQueryConnectionParams | Mapping[str, Any] | str | None,
    connection_config: BigQueryConnectionParams | BigQueryPoolParams | Mapping[str, Any] | None,
    driver_features: BigQueryDriverFeatures | Mapping[str, Any] | None,
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
    connection_instance: Any,
    share_pool: bool | None,
    reset_session_on_release: bool | None,
    kwargs: dict[str, Any],
) -> tuple[BigQueryConfig, dict[str, Any], dict[str, Any], Any, bool, Any]:
    """Normalize BigQuery config, pool options, `driver_features`, and `connection_instance`."""
    raw_conn = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    pool_conn_instance = raw_conn.pop("connection_instance", None)
    pool_share = raw_conn.pop("share_pool", None)
    resolved_share_pool = (
        bool(share_pool) if share_pool is not None else bool(pool_share) if pool_share is not None else True
    )

    features = dict(driver_features) if isinstance(driver_features, Mapping) else {}
    if reset_session_on_release is not None:
        features["reset_session_on_release"] = bool(reset_session_on_release)
    elif "reset_session_on_release" in kwargs:
        features.setdefault("reset_session_on_release", bool(kwargs.pop("reset_session_on_release")))
    if "storage_backend" in kwargs:
        features.setdefault("storage_backend", kwargs.pop("storage_backend"))
    features_conn_instance = features.pop("connection_instance", None)
    user_hook = features.pop("on_connection_create", None)
    resolved_instance = (
        connection_instance
        if connection_instance is not None
        else pool_conn_instance
        if pool_conn_instance is not None
        else features_conn_instance
    )

    resolved_cfg, merged_conn_cfg = _merge_connection_config(
        config,
        raw_conn,
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
    return resolved_cfg, merged_conn_cfg, features, resolved_instance, resolved_share_pool, user_hook


def _build_bigquery_replica_overrides(adapter: Any, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
    """Build BigQuery replica `derive_pool` overrides from DSN URIs, project IDs, or mappings."""
    if isinstance(spec_input, bool):
        msg = f"{type(adapter).__name__} requires explicit replica project or connection configuration"
        raise ImproperConfigurationError(msg)
    if isinstance(spec_input, str):
        endpoint = spec_input.strip()
        if not endpoint:
            msg = "BigQuery replica specification must not be empty"
            raise ImproperConfigurationError(msg)
        parsed = urlsplit(endpoint)
        if parsed.scheme.lower() in {"bigquery", "bq"}:
            conn_overrides: dict[str, Any] = {"project_id": parsed.netloc, "project": parsed.netloc}
            dataset = unquote(parsed.path.lstrip("/"))
            if dataset:
                conn_overrides["dataset_id"] = dataset
            return {"connection_config_overrides": conn_overrides}
        return {"connection_config_overrides": {"project_id": endpoint, "project": endpoint}}
    data = dict(spec_input)
    if "connection_config_overrides" in data or "driver_features_overrides" in data:
        return data
    meta: dict[str, Any] = {}
    conn_overrides = {}
    for key, value in data.items():
        if key in _DERIVE_META_KEYS:
            meta[key] = value
        else:
            conn_overrides[key] = value
    if conn_overrides:
        meta["connection_config_overrides"] = conn_overrides
    return meta


def _derive_bigquery_adapter(adapter: Any, overrides: Mapping[str, Any]) -> Any:
    """Clone `BigQuerySyncConfig` or `BigQueryAsyncConfig` with non-mutating replica or feature overrides."""
    merged_features = dict(overrides.get("driver_features", adapter.driver_features))
    merged_features.pop("_statement_cache_holder", None)
    user_hook = getattr(adapter, "_user_connection_hook", None)
    if user_hook is not None and "on_connection_create" not in merged_features:
        merged_features["on_connection_create"] = user_hook
    feature_overrides = overrides.get("driver_features_overrides")
    if isinstance(feature_overrides, Mapping):
        merged_features.update(feature_overrides)
    if "reset_session_on_release" in overrides and overrides["reset_session_on_release"] is not None:
        merged_features["reset_session_on_release"] = bool(overrides["reset_session_on_release"])

    driver_cfg = adapter.driver_config
    merged_conn: dict[str, Any] = {
        "project_id": str(getattr(driver_cfg, "project_id", "") or ""),
        "location": str(getattr(driver_cfg, "location", "US") or "US"),
        "scopes": tuple(getattr(driver_cfg, "scopes", BIGQUERY_DEFAULT_SCOPES)),
    }
    dataset_val = getattr(driver_cfg, "dataset_id", None)
    if dataset_val is not None:
        merged_conn["dataset_id"] = dataset_val
    for attr in (
        "credentials_path",
        "maximum_bytes_billed",
        "rest_endpoint",
        "storage_endpoint",
        "job_timeout_ms",
        "priority",
        "labels",
        "quota_project_id",
        "access_token",
        "use_gcloud_cli",
        "pool_size",
        "statement_cache_capacity",
        "use_shm_cache",
        "arrow_compression",
        "max_read_streams",
        "ca_pem",
    ):
        val = getattr(driver_cfg, attr, None)
        if val is not None:
            merged_conn[attr] = val
    merged_conn.update(adapter.connection_config)

    override_conn = overrides.get("connection_config")
    if isinstance(override_conn, Mapping):
        merged_conn.update(override_conn)

    extra_updates: dict[str, Any] = {}
    conn_overrides = overrides.get("connection_config_overrides")
    if isinstance(conn_overrides, Mapping):
        extra_updates.update(conn_overrides)
    for key, value in overrides.items():
        if key in _DERIVE_META_KEYS:
            continue
        if key in _BIGQUERY_FEATURE_KEYS or key in adapter.driver_features:
            merged_features[key] = value
        else:
            extra_updates[key] = value

    if "project" in extra_updates and "project_id" not in extra_updates:
        extra_updates["project_id"] = extra_updates["project"]
    elif "project_id" in extra_updates and "project" not in extra_updates:
        extra_updates["project"] = extra_updates["project_id"]
    if "dataset" in extra_updates and "dataset_id" not in extra_updates:
        extra_updates["dataset_id"] = extra_updates["dataset"]

    has_dsn_override = any(isinstance(extra_updates.get(k), str) and extra_updates[k].strip() for k in ("dsn", "url"))
    if has_dsn_override:
        for key in ("project", "project_id", "dataset", "dataset_id", "dsn", "url"):
            merged_conn.pop(key, None)
    elif any(k in extra_updates for k in ("project", "project_id", "dataset", "dataset_id")):
        merged_conn.pop("dsn", None)
        merged_conn.pop("url", None)

    merged_conn.update(extra_updates)

    return type(adapter)(
        None,
        bind_key=str(overrides.get("bind_key", adapter.bind_key)),
        broker=overrides.get("broker", adapter.broker),
        share_pool=bool(overrides.get("share_pool", adapter.share_pool)),
        arrow_threshold_rows=int(overrides.get("arrow_threshold_rows", adapter.arrow_threshold_rows)),
        statement_config=overrides.get("statement_config", adapter.statement_config),
        connection_config=merged_conn,
        migration_config=(
            dict(overrides["migration_config"])
            if isinstance(overrides.get("migration_config"), Mapping)
            else dict(adapter.migration_config)
            if isinstance(adapter.migration_config, Mapping)
            else adapter.migration_config
        ),
        driver_features=merged_features,
        extension_config=dict(overrides.get("extension_config", adapter.extension_config)),
        observability_config=overrides.get("observability_config", adapter.observability_config),
        replicas=None,
        is_replica=bool(overrides.get("is_replica", False)),
    )


class BigQuerySyncConfig(
    SyncAdapterConfig[BigQueryConfig, BigQueryConnectionShim, SyncBigQueryDriver, BigQuerySyncDriver]
):
    """Synchronous SQLSpec adapter configuration backed by Vertebra ``SyncBigQueryDriver``."""

    __slots__ = ("_connection_shim", "_initialized_conn_ids", "_shim_driver", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_bigquery"
    dialect: ClassVar[str] = "bigquery"
    connection_type: ClassVar[type[Any]] = SyncBigQueryDriver
    _config_cls: ClassVar[type[BigQueryConfig]] = BigQueryConfig
    _driver_cls: ClassVar[type[SyncBigQueryDriver]] = SyncBigQueryDriver
    driver_type: ClassVar[type[BigQuerySyncDriver]] = BigQuerySyncDriver
    _connection_context_class: ClassVar[type[Any]] = BigQuerySyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = BigQuerySyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQuerySyncADKStore",
        package="google-adk",
        extra="adk",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQuerySyncADKMemoryStore",
        package="google-adk",
        extra="adk",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQuerySyncADKArtifactStore",
        package="google-adk",
        extra="adk",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.events",
        "BigQueryEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.litestar",
        "BigQueryLitestarStore",
        package="litestar",
        extra="litestar",
    )

    def __init__(
        self,
        config: BigQueryConfig | BigQueryConnectionParams | Mapping[str, Any] | str | None = None,
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
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: BigQueryConnectionParams | BigQueryPoolParams | dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: BigQueryDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        tenant_predicate: Any = None,
        **kwargs: Any,
    ) -> None:
        _reject_tenant_predicate(tenant_predicate)
        resolved_cfg, merged_conn_cfg, features, resolved_instance, resolved_share_pool, user_hook = (
            _prepare_bigquery_init(
                config,
                connection_config,
                driver_features,
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
                connection_instance=connection_instance,
                share_pool=share_pool,
                reset_session_on_release=reset_session_on_release,
                kwargs=kwargs,
            )
        )
        self._shim_driver: Any = None
        self._connection_shim: BigQueryConnectionShim | None = None
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = user_hook
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config or default_statement_config(),
            connection_config=merged_conn_cfg,
            connection_instance=resolved_instance,
            migration_config=migration_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

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

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractContextManager[BigQuerySyncDriver]:
        """Yield a fresh synchronous BigQuery SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractContextManager[Any]:
        """Yield a synchronous native BigQuery driver context forwarding positional and keyword options."""
        return super().provide_connection(*args, **kwargs)

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build BigQuery replica overrides from URIs, project identifiers, or explicit dictionaries."""
        return _build_bigquery_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous BigQuery configuration with non-mutating replica or feature overrides."""
        return cast("Self", _derive_bigquery_adapter(self, overrides))

    def _close_pool(self) -> None:
        """Close the synchronous BigQuery pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        super()._close_pool()

    def close(self) -> None:
        """Close cached connection shims and underlying driver resources."""
        if self._shim_driver is not None:
            close_fn = getattr(self._shim_driver, "close", None)
            if callable(close_fn) and self._shim_driver is not self.connection_instance:
                close_fn()
            self._shim_driver = None
        self._connection_shim = None
        super().close()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return BigQuery adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "BigQueryConfig": BigQueryConfig,
                "BigQueryConnectionParams": BigQueryConnectionParams,
                "BigQueryConnectionShim": BigQueryConnectionShim,
                "BigQueryDriverFeatures": BigQueryDriverFeatures,
                "BigQueryPoolParams": BigQueryPoolParams,
                "BigQuerySyncConnectionContext": BigQuerySyncConnectionContext,
                "BigQuerySyncDriver": BigQuerySyncDriver,
                "BigQuerySyncSessionContext": BigQuerySyncSessionContext,
                "SyncBigQueryDriver": SyncBigQueryDriver,
            }
        )
        return namespace


class BigQueryAsyncConfig(
    AsyncAdapterConfig[BigQueryConfig, BigQueryConnectionShim, AsyncBigQueryDriver, BigQueryAsyncDriver]
):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra ``AsyncBigQueryDriver``."""

    __slots__ = ("_connection_shim", "_initialized_conn_ids", "_shim_sync_driver", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_bigquery"
    dialect: ClassVar[str] = "bigquery"
    connection_type: ClassVar[type[Any]] = AsyncBigQueryDriver
    _config_cls: ClassVar[type[BigQueryConfig]] = BigQueryConfig
    _driver_cls: ClassVar[type[AsyncBigQueryDriver]] = AsyncBigQueryDriver
    driver_type: ClassVar[type[BigQueryAsyncDriver]] = BigQueryAsyncDriver
    _connection_context_class: ClassVar[type[Any]] = BigQueryAsyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = BigQueryAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQueryAsyncADKStore",
        package="google-adk",
        extra="adk",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQueryAsyncADKMemoryStore",
        package="google-adk",
        extra="adk",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQueryAsyncADKArtifactStore",
        package="google-adk",
        extra="adk",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.events",
        "BigQueryEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.bigquery.litestar",
        "BigQueryLitestarStore",
        package="litestar",
        extra="litestar",
    )

    def __init__(
        self,
        config: BigQueryConfig | BigQueryConnectionParams | Mapping[str, Any] | str | None = None,
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
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: BigQueryConnectionParams | BigQueryPoolParams | dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: BigQueryDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        tenant_predicate: Any = None,
        **kwargs: Any,
    ) -> None:
        _reject_tenant_predicate(tenant_predicate)
        resolved_cfg, merged_conn_cfg, features, resolved_instance, resolved_share_pool, user_hook = (
            _prepare_bigquery_init(
                config,
                connection_config,
                driver_features,
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
                connection_instance=connection_instance,
                share_pool=share_pool,
                reset_session_on_release=reset_session_on_release,
                kwargs=kwargs,
            )
        )
        self._shim_sync_driver: SyncBigQueryDriver | None = None
        self._connection_shim: BigQueryConnectionShim | None = None
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = user_hook
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config or default_statement_config(),
            connection_config=merged_conn_cfg,
            connection_instance=resolved_instance,
            migration_config=migration_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

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

    async def create_connection(self) -> BigQueryConnectionShim:
        """Create and cache a single ``SyncBigQueryDriver`` shim across repeated connection requests."""
        if self._connection_shim is not None and self._shim_sync_driver is not None:
            return self._connection_shim
        sync_driver = SyncBigQueryDriver(self.driver_config)
        sync_driver.connect()
        self._shim_sync_driver = sync_driver
        self._connection_shim = BigQueryConnectionShim(sync_driver)
        return self._connection_shim

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[BigQueryAsyncDriver]:
        """Yield a fresh asynchronous BigQuery SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractAsyncContextManager[Any]:
        """Yield an asynchronous native BigQuery driver context forwarding positional and keyword options."""
        return super().provide_connection(*args, **kwargs)

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build BigQuery replica overrides from URIs, project identifiers, or explicit dictionaries."""
        return _build_bigquery_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous BigQuery configuration with non-mutating replica or feature overrides."""
        return cast("Self", _derive_bigquery_adapter(self, overrides))

    async def _close_pool(self) -> None:
        """Close the asynchronous BigQuery pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        await super()._close_pool()

    async def close(self) -> None:
        """Close the cached sync shim driver and async adapter pool cleanly."""
        if self._shim_sync_driver is not None:
            self._shim_sync_driver.close()
            self._shim_sync_driver = None
        self._connection_shim = None
        await super().close()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return BigQuery adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "AsyncBigQueryDriver": AsyncBigQueryDriver,
                "BigQueryAsyncConnectionContext": BigQueryAsyncConnectionContext,
                "BigQueryAsyncDriver": BigQueryAsyncDriver,
                "BigQueryAsyncSessionContext": BigQueryAsyncSessionContext,
                "BigQueryConfig": BigQueryConfig,
                "BigQueryConnectionParams": BigQueryConnectionParams,
                "BigQueryConnectionShim": BigQueryConnectionShim,
                "BigQueryDriverFeatures": BigQueryDriverFeatures,
                "BigQueryPoolParams": BigQueryPoolParams,
            }
        )
        return namespace
