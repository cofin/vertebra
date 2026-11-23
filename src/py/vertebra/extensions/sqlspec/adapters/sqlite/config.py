"""SQLite synchronous and asynchronous SQLSpec adapter configurations."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlspec.core import StatementConfig

from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, LazyStoreAttr, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.sqlite._typing import (
    SQLiteAsyncConnectionContext,
    SQLiteAsyncSessionContext,
    SQLiteConnectionParams,
    SQLiteDriverFeatures,
    SQLitePoolParams,
    SQLiteSyncConnectionContext,
    SQLiteSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.sqlite.core import build_connection_config
from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver, SQLiteSyncDriver
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from uuid import UUID

__all__ = ("SQLiteAsyncConfig", "SQLiteSyncConfig")

_POOL_PARAM_KEYS: frozenset[str] = frozenset({"min_size", "max_size", "pool_recycle", "share_pool"})
_VALID_TRANSACTION_MODES: frozenset[str] = frozenset({"DEFERRED", "IMMEDIATE", "EXCLUSIVE"})
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
_URL_KEYS: tuple[str, ...] = ("url", "dsn", "conninfo", "connection_string")


def _has_readonly_uri(value: Any) -> bool:
    """Return whether `value` encodes a SQLite URI with `mode=ro`."""
    if not isinstance(value, str) or "mode=" not in value:
        return False
    query = dict(parse_qsl(urlsplit(value).query))
    return query.get("mode") == "ro"


def _is_read_only_sqlite(config: SQLiteConfig | None, conn_cfg: Mapping[str, Any]) -> bool:
    """Return whether SQLite config or connection parameters enforce read-only access."""
    if bool(conn_cfg.get("query_only")):
        return True
    for key in ("url", "dsn", "database", "path"):
        if _has_readonly_uri(conn_cfg.get(key)):
            return True
    if config is not None:
        return _has_readonly_uri(str(config.path)) or _has_readonly_uri(str(config.database or ""))
    return False


def _build_readonly_sqlite_uri(database: str) -> str:
    """Build a read-only `file:...?mode=ro` URI for a file-backed SQLite database."""
    uri = urlsplit(database) if database.startswith("file:") else None
    query = dict(parse_qsl(uri.query)) if uri is not None else {}
    if (
        not database
        or database == ":memory:"
        or (uri is not None and uri.path == ":memory:")
        or query.get("mode") == "memory"
    ):
        msg = "SQLite read replicas require a file-backed database"
        raise ImproperConfigurationError(msg)
    if uri is None:
        uri = urlsplit(Path(database).resolve().as_uri())
    query["mode"] = "ro"
    return urlunsplit((uri.scheme, uri.netloc, uri.path, urlencode(query), uri.fragment))


def _resolve_sqlite_config(
    config: SQLiteConfig | None,
    *,
    share_pool: bool,
    broker: Any,
    connection_config: Mapping[str, Any] | None,
    driver_features: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
) -> tuple[SQLiteConfig, Any]:
    config_broker = getattr(config, "broker", None) if config is not None else None
    if broker is not None:
        resolved_broker = broker
    elif config_broker is not None:
        resolved_broker = config_broker
    elif share_pool:
        resolved_broker = DatabaseRegistry.default_broker()
    else:
        resolved_broker = None
    if config is not None:
        return config, resolved_broker
    built = build_connection_config(
        connection_config=connection_config,
        driver_features=driver_features,
        broker=resolved_broker,
        overrides=kwargs,
    )
    return built, resolved_broker


def _prepare_sqlite_setup(
    config: SQLiteConfig | None,
    *,
    share_pool: bool | None,
    broker: Any,
    connection_config: SQLiteConnectionParams | SQLitePoolParams | Mapping[str, Any] | None,
    driver_features: SQLiteDriverFeatures | Mapping[str, Any] | None,
    reset_session_on_release: bool | None,
    kwargs: Mapping[str, Any],
) -> tuple[SQLiteConfig, Any, bool, dict[str, Any], dict[str, Any], bool]:
    """Normalize SQLite connection params, pool options, driver features, and read-only constraints."""
    raw_conn = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    if share_pool is not None:
        resolved_share_pool = bool(share_pool)
    elif "share_pool" in raw_conn:
        resolved_share_pool = bool(raw_conn["share_pool"])
    else:
        resolved_share_pool = True

    features: dict[str, Any] = dict(driver_features) if isinstance(driver_features, Mapping) else {}
    if reset_session_on_release is not None:
        features["reset_session_on_release"] = bool(reset_session_on_release)

    filtered_kwargs: dict[str, Any] = {}
    for key, value in kwargs.items():
        if key == "default_transaction_mode":
            features.setdefault("default_transaction_mode", value)
        elif key == "reset_session_on_release":
            features.setdefault("reset_session_on_release", bool(value))
        elif key in _POOL_PARAM_KEYS:
            raw_conn[key] = value
        else:
            filtered_kwargs[key] = value
            raw_conn[key] = value

    build_conn = {k: v for k, v in raw_conn.items() if k not in _POOL_PARAM_KEYS and k != "query_only"}
    build_overrides = {k: v for k, v in filtered_kwargs.items() if k not in _POOL_PARAM_KEYS and k != "query_only"}
    for source in (build_conn, build_overrides):
        url_val = source.get("url")
        if isinstance(url_val, str) and url_val.startswith("file:"):
            source.pop("url", None)
            source["database"] = url_val
            source["uri"] = True

    resolved_cfg, resolved_broker = _resolve_sqlite_config(
        config,
        share_pool=resolved_share_pool,
        broker=broker,
        connection_config=build_conn,
        driver_features=features,
        kwargs=build_overrides,
    )

    if "database" not in raw_conn and "path" not in raw_conn and not any(k in raw_conn for k in _URL_KEYS):
        raw_conn["database"] = str(resolved_cfg.database or resolved_cfg.path)
        raw_conn["path"] = str(resolved_cfg.path)
    elif "database" in raw_conn and "path" not in raw_conn:
        raw_conn["path"] = str(resolved_cfg.path)
    elif "path" in raw_conn and "database" not in raw_conn:
        raw_conn["database"] = str(resolved_cfg.database or resolved_cfg.path)

    is_read_only = _is_read_only_sqlite(resolved_cfg, raw_conn)
    tx_mode = features.get("default_transaction_mode")
    if tx_mode is not None:
        normalized_mode = str(tx_mode).upper()
        if normalized_mode not in _VALID_TRANSACTION_MODES:
            msg = f"Invalid SQLite default_transaction_mode: {tx_mode!r}"
            raise ImproperConfigurationError(msg)
        if is_read_only and normalized_mode != "DEFERRED":
            msg = "Read-only SQLite connections require DEFERRED transaction mode"
            raise ImproperConfigurationError(msg)
        features["default_transaction_mode"] = normalized_mode
    elif is_read_only:
        features["default_transaction_mode"] = "DEFERRED"

    return resolved_cfg, resolved_broker, resolved_share_pool, raw_conn, features, is_read_only


def _build_sqlite_replica_overrides(adapter: Any, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
    """Build SQLite replica `derive_pool` overrides supporting `replicas=True`, URI strings, and dicts."""
    if isinstance(spec_input, bool):
        if not spec_input:
            msg = f"{type(adapter).__name__} requires replicas=True or explicit replica endpoints"
            raise ImproperConfigurationError(msg)
        base_db = str(
            adapter.connection_config.get("database")
            or adapter.connection_config.get("path")
            or getattr(adapter.driver_config, "database", "")
            or getattr(adapter.driver_config, "path", "")
        )
        ro_uri = _build_readonly_sqlite_uri(base_db)
        adapter._serialize_writes = True
        return {
            "connection_config_overrides": {"database": ro_uri, "path": ro_uri, "uri": True, "query_only": True},
            "driver_features_overrides": {"default_transaction_mode": "DEFERRED"},
        }
    if isinstance(spec_input, str):
        if any(isinstance(adapter.connection_config.get(k), str) for k in ("url", "dsn")):
            return {"connection_config_overrides": {"url": spec_input}}
        return {"connection_config_overrides": {"database": spec_input, "path": spec_input}}
    return dict(spec_input)


def _derive_sqlite_adapter(adapter: Any, overrides: Mapping[str, Any]) -> Any:
    """Clone `SQLiteSyncConfig` or `SQLiteAsyncConfig` with non-mutating connection and feature overrides."""
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
        "wal_mode": bool(driver_cfg.wal_mode),
        "synchronous": str(driver_cfg.synchronous),
        "mmap_size": int(driver_cfg.mmap_size),
        "busy_timeout_ms": int(driver_cfg.busy_timeout_ms),
        "max_readers": int(driver_cfg.max_readers),
        "foreign_keys": bool(driver_cfg.foreign_keys),
        "enable_jsonb": bool(driver_cfg.enable_jsonb),
    }
    merged_conn.update(adapter.connection_config)
    override_conn = overrides.get("connection_config")
    if isinstance(override_conn, Mapping):
        merged_conn.update(override_conn)

    conn_overrides = overrides.get("connection_config_overrides")
    if isinstance(conn_overrides, Mapping):
        if any(alias in conn_overrides for alias in _URL_KEYS):
            for alias in (*_URL_KEYS, "database", "path"):
                merged_conn.pop(alias, None)
        if "database" in conn_overrides and "path" not in conn_overrides:
            merged_conn["path"] = str(conn_overrides["database"])
        elif "path" in conn_overrides and "database" not in conn_overrides:
            merged_conn["database"] = conn_overrides["path"]
        merged_conn.update(conn_overrides)

    for key, value in overrides.items():
        if key in _DERIVE_META_KEYS:
            continue
        if key in adapter.driver_features or key == "default_transaction_mode":
            merged_features[key] = value
        else:
            if key in _URL_KEYS:
                for alias in (*_URL_KEYS, "database", "path"):
                    merged_conn.pop(alias, None)
            elif key == "database" and "path" not in overrides:
                merged_conn["path"] = str(value)
            elif key == "path" and "database" not in overrides:
                merged_conn["database"] = value
            merged_conn[key] = value

    if _is_read_only_sqlite(None, merged_conn) and "default_transaction_mode" not in merged_features:
        merged_features["default_transaction_mode"] = "DEFERRED"

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


class SQLiteSyncConfig(SyncAdapterConfig[SQLiteConfig, SyncSQLiteDriver, SyncSQLiteDriver, SQLiteSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncSQLiteDriver`."""

    __slots__ = ("_initialized_conn_ids", "_is_read_only", "_serialize_writes", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_sqlite"
    dialect: ClassVar[str] = "sqlite"
    connection_type: ClassVar[type[Any]] = SyncSQLiteDriver
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[SyncSQLiteDriver]] = SyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteSyncDriver]] = SQLiteSyncDriver
    _connection_context_class: ClassVar[type[Any]] = SQLiteSyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = SQLiteSyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.adk",
        "SQLiteSyncADKStore",
        package="google-adk",
        extra="adk",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.adk",
        "SQLiteSyncADKMemoryStore",
        package="google-adk",
        extra="adk",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.adk",
        "SQLiteSyncADKArtifactStore",
        package="google-adk",
        extra="adk",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.events",
        "SQLiteEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.litestar",
        "SQLiteLitestarStore",
        package="litestar",
        extra="litestar",
    )

    def __init__(
        self,
        config: SQLiteConfig | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: SQLiteConnectionParams | SQLitePoolParams | dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: SQLiteDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous SQLite SQLSpec configuration supporting URLs, replicas, and session reset."""
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features, is_read_only = (
            _prepare_sqlite_setup(
                config,
                share_pool=share_pool,
                broker=broker,
                connection_config=connection_config,
                driver_features=driver_features,
                reset_session_on_release=reset_session_on_release,
                kwargs=kwargs,
            )
        )
        self._initialized_conn_ids: set[int] = set()
        self._is_read_only: bool = is_read_only
        self._serialize_writes: bool = False
        self._user_connection_hook: Any = features.pop("on_connection_create", None)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=normalized_conn,
            migration_config=migration_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractContextManager[SQLiteSyncDriver]:
        """Yield a fresh synchronous SQLite SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractContextManager[SyncSQLiteDriver]:
        """Yield a synchronous native SQLite driver context forwarding positional and keyword options."""
        return cast("AbstractContextManager[SyncSQLiteDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build SQLite replica overrides from `replicas=True`, URIs, or explicit dictionaries."""
        return _build_sqlite_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous SQLite configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_sqlite_adapter(self, overrides))

    def _close_pool(self) -> None:
        """Close the synchronous SQLite pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return SQLite adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "SQLiteConfig": SQLiteConfig,
                "SQLiteConnectionParams": SQLiteConnectionParams,
                "SQLiteDriverFeatures": SQLiteDriverFeatures,
                "SQLitePoolParams": SQLitePoolParams,
                "SQLiteSyncConnectionContext": SQLiteSyncConnectionContext,
                "SQLiteSyncDriver": SQLiteSyncDriver,
                "SQLiteSyncSessionContext": SQLiteSyncSessionContext,
                "SyncSQLiteDriver": SyncSQLiteDriver,
            }
        )
        return namespace


class SQLiteAsyncConfig(AsyncAdapterConfig[SQLiteConfig, AsyncSQLiteDriver, AsyncSQLiteDriver, SQLiteAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncSQLiteDriver`."""

    __slots__ = ("_initialized_conn_ids", "_is_read_only", "_serialize_writes", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_sqlite"
    dialect: ClassVar[str] = "sqlite"
    connection_type: ClassVar[type[Any]] = AsyncSQLiteDriver
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[AsyncSQLiteDriver]] = AsyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteAsyncDriver]] = SQLiteAsyncDriver
    _connection_context_class: ClassVar[type[Any]] = SQLiteAsyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = SQLiteAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.adk",
        "SQLiteAsyncADKStore",
        package="google-adk",
        extra="adk",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.adk",
        "SQLiteAsyncADKMemoryStore",
        package="google-adk",
        extra="adk",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.adk",
        "SQLiteAsyncADKArtifactStore",
        package="google-adk",
        extra="adk",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.events",
        "SQLiteEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.sqlite.litestar",
        "SQLiteLitestarStore",
        package="litestar",
        extra="litestar",
    )

    def __init__(
        self,
        config: SQLiteConfig | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: SQLiteConnectionParams | SQLitePoolParams | dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: SQLiteDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous SQLite SQLSpec configuration supporting URLs, replicas, and session reset."""
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features, is_read_only = (
            _prepare_sqlite_setup(
                config,
                share_pool=share_pool,
                broker=broker,
                connection_config=connection_config,
                driver_features=driver_features,
                reset_session_on_release=reset_session_on_release,
                kwargs=kwargs,
            )
        )
        self._initialized_conn_ids: set[int] = set()
        self._is_read_only: bool = is_read_only
        self._serialize_writes: bool = False
        self._user_connection_hook: Any = features.pop("on_connection_create", None)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=normalized_conn,
            migration_config=migration_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[SQLiteAsyncDriver]:
        """Yield a fresh asynchronous SQLite SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractAsyncContextManager[AsyncSQLiteDriver]:
        """Yield an asynchronous native SQLite driver context forwarding positional and keyword options."""
        return cast("AbstractAsyncContextManager[AsyncSQLiteDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build SQLite replica overrides from `replicas=True`, URIs, or explicit dictionaries."""
        return _build_sqlite_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous SQLite configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_sqlite_adapter(self, overrides))

    async def _close_pool(self) -> None:
        """Close the asynchronous SQLite pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        await super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return SQLite adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "AsyncSQLiteDriver": AsyncSQLiteDriver,
                "SQLiteAsyncConnectionContext": SQLiteAsyncConnectionContext,
                "SQLiteAsyncDriver": SQLiteAsyncDriver,
                "SQLiteAsyncSessionContext": SQLiteAsyncSessionContext,
                "SQLiteConfig": SQLiteConfig,
                "SQLiteConnectionParams": SQLiteConnectionParams,
                "SQLiteDriverFeatures": SQLiteDriverFeatures,
                "SQLitePoolParams": SQLitePoolParams,
            }
        )
        return namespace
