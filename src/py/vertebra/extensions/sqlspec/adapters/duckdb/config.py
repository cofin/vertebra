"""DuckDB synchronous and asynchronous SQLSpec adapter configurations."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from urllib.parse import urlparse

from sqlspec.core import StatementConfig

from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, LazyStoreAttr, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.duckdb._typing import (
    DuckDBAsyncConnectionContext,
    DuckDBAsyncSessionContext,
    DuckDBConnectionParams,
    DuckDBDriverFeatures,
    DuckDBPoolParams,
    DuckDBSyncConnectionContext,
    DuckDBSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.duckdb.core import build_connection_config, build_statement_config
from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBAsyncDriver, DuckDBSyncDriver
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from uuid import UUID

__all__ = ("DuckDBAsyncConfig", "DuckDBSyncConfig")

_POOL_ONLY_KEYS: frozenset[str] = frozenset({"min_size", "max_size", "pool_recycle", "share_pool"})
_URL_KEYS: tuple[str, ...] = ("url", "dsn")
_REMOTE_PREFIXES: tuple[str, ...] = ("md:", "motherduck:", "s3://", "gcs://", "http://", "https://")
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


def _extract_duckdb_target(source: Mapping[str, Any]) -> str:
    """Resolve the effective DuckDB database target path or URI from a parameter mapping."""
    memory_id = source.get("memory_id")
    if isinstance(memory_id, str) and memory_id.strip():
        return f":memory:{memory_id.strip()}"
    for key in ("database", "db", "path", "file"):
        val = source.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    for alias in _URL_KEYS:
        url_val = source.get(alias)
        if isinstance(url_val, str) and url_val.strip():
            raw = url_val.strip()
            if "://" in raw:
                parsed = urlparse(raw)
                if parsed.netloc == ":memory:" or (
                    not parsed.netloc and parsed.path in ("", "/", "/:memory:", ":memory:")
                ):
                    return ":memory:"
                if parsed.netloc:
                    return f"{parsed.netloc}{parsed.path}"
                return parsed.path[1:] if parsed.path.startswith("//") else parsed.path.lstrip("/")
            return raw
    return ""


def _is_in_memory_target(target: str) -> bool:
    """Return whether `target` names an in-memory DuckDB catalog."""
    cleaned = target.strip()
    return not cleaned or cleaned.startswith(":memory:") or cleaned == "memory"


def _validate_duckdb_replica_config(
    primary_config: Mapping[str, Any],
    replica_config: Mapping[str, Any],
    *,
    primary_driver_cfg: DuckDBConfig | None = None,
) -> None:
    """Reject in-memory replica loops and incompatible read-only access to the primary file."""
    target = _extract_duckdb_target(replica_config)
    if _is_in_memory_target(target):
        msg = "DuckDB replicas require a persistent snapshot or remote database"
        raise ImproperConfigurationError(msg)

    primary_target = _extract_duckdb_target(primary_config)
    if not primary_target and primary_driver_cfg is not None:
        primary_target = primary_driver_cfg.resolved_path()
    primary_read_only = bool(
        primary_config.get("read_only", primary_driver_cfg.read_only if primary_driver_cfg is not None else False)
    )
    if target.startswith(_REMOTE_PREFIXES) or primary_target.startswith(_REMOTE_PREFIXES) or primary_read_only:
        return
    if (
        not _is_in_memory_target(primary_target)
        and Path(target).expanduser().resolve() == Path(primary_target).expanduser().resolve()
    ):
        msg = "A DuckDB read replica cannot open the read-write primary file; use a separate snapshot"
        raise ImproperConfigurationError(msg)


def _resolve_duckdb_config(
    config: DuckDBConfig | None,
    *,
    share_pool: bool,
    broker: Any,
    connection_config: Mapping[str, Any] | None,
    driver_features: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
) -> tuple[DuckDBConfig, Any]:
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


def _prepare_duckdb_setup(
    config: DuckDBConfig | DuckDBConnectionParams | str | None,
    *,
    share_pool: bool | None,
    broker: Any,
    connection_config: DuckDBConnectionParams | DuckDBPoolParams | Mapping[str, Any] | None,
    driver_features: DuckDBDriverFeatures | Mapping[str, Any] | None,
    statement_config: StatementConfig | None,
    reset_session_on_release: bool | None,
    kwargs: Mapping[str, Any],
) -> tuple[DuckDBConfig, Any, bool, dict[str, Any], dict[str, Any], StatementConfig | None]:
    """Normalize DuckDB connection params, pool options, driver features, and statement config."""
    raw_conn: dict[str, Any] = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    if isinstance(config, Mapping):
        raw_conn.update(config)
    elif isinstance(config, str) and config.strip():
        stripped = config.strip()
        if "://" in stripped:
            raw_conn.setdefault("url", stripped)
        else:
            raw_conn.setdefault("database", stripped)

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
        if key == "reset_session_on_release":
            features.setdefault("reset_session_on_release", bool(value))
        elif key in _POOL_ONLY_KEYS:
            raw_conn[key] = value
        else:
            filtered_kwargs[key] = value
            raw_conn[key] = value

    build_conn = {k: v for k, v in raw_conn.items() if k not in _POOL_ONLY_KEYS}
    build_overrides = {k: v for k, v in filtered_kwargs.items() if k not in _POOL_ONLY_KEYS}
    if "min_size" in raw_conn and "pool_min_size" not in build_conn:
        build_conn["pool_min_size"] = int(raw_conn["min_size"])
    if "max_size" in raw_conn and "pool_max_size" not in build_conn:
        build_conn["pool_max_size"] = int(raw_conn["max_size"])
    if "pool_recycle" in raw_conn and "pool_recycle_seconds" not in build_conn:
        build_conn["pool_recycle_seconds"] = int(raw_conn["pool_recycle"])

    explicit_cfg = config if isinstance(config, DuckDBConfig) else None
    resolved_cfg, resolved_broker = _resolve_duckdb_config(
        explicit_cfg,
        share_pool=resolved_share_pool,
        broker=broker,
        connection_config=build_conn,
        driver_features=features,
        kwargs=build_overrides,
    )

    if "database" not in raw_conn and "path" not in raw_conn and not any(k in raw_conn for k in _URL_KEYS):
        resolved_target = resolved_cfg.resolved_path()
        raw_conn["database"] = resolved_target
        raw_conn["path"] = str(resolved_cfg.path if resolved_cfg.path != ":memory:" else resolved_target)
    elif "database" in raw_conn and "path" not in raw_conn:
        raw_conn["path"] = str(resolved_cfg.path)
    elif "path" in raw_conn and "database" not in raw_conn:
        raw_conn["database"] = str(resolved_cfg.database)

    if "read_only" not in raw_conn and resolved_cfg.read_only:
        raw_conn["read_only"] = True

    resolved_stmt_cfg = statement_config
    if resolved_stmt_cfg is None and callable(features.get("json_serializer")):
        resolved_stmt_cfg = build_statement_config(json_serializer=features["json_serializer"])

    return resolved_cfg, resolved_broker, resolved_share_pool, raw_conn, features, resolved_stmt_cfg


def _build_duckdb_replica_overrides(adapter: Any, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
    """Build DuckDB replica `derive_pool` overrides enforcing persistent snapshot/remote targets."""
    if isinstance(spec_input, bool):
        msg = "DuckDB replicas require a separate snapshot path or remote database"
        raise ImproperConfigurationError(msg)

    primary_cfg = getattr(adapter, "driver_config", None)
    if isinstance(spec_input, str):
        endpoint = spec_input.strip()
        if not endpoint:
            msg = "DuckDB replicas require a persistent snapshot or remote database"
            raise ImproperConfigurationError(msg)
        connection: dict[str, Any] = (
            {"url": endpoint, "read_only": True}
            if "://" in endpoint
            else {"database": endpoint, "path": endpoint, "read_only": True}
        )
        _validate_duckdb_replica_config(adapter.connection_config, connection, primary_driver_cfg=primary_cfg)
        return {"connection_config_overrides": connection}

    raw = dict(spec_input)
    if "connection_config_overrides" in raw or "driver_features_overrides" in raw:
        conn_overrides = dict(raw.get("connection_config_overrides", {}))
        merged_check = {**adapter.connection_config, **conn_overrides}
        if "read_only" not in conn_overrides and "read_only" not in raw:
            conn_overrides["read_only"] = True
            merged_check["read_only"] = True
            raw["connection_config_overrides"] = conn_overrides
        elif "read_only" in raw and "read_only" not in conn_overrides:
            conn_overrides["read_only"] = bool(raw["read_only"])
            merged_check["read_only"] = bool(raw["read_only"])
            raw["connection_config_overrides"] = conn_overrides
        _validate_duckdb_replica_config(adapter.connection_config, merged_check, primary_driver_cfg=primary_cfg)
        return raw

    meta: dict[str, Any] = {}
    conn_updates: dict[str, Any] = {}
    for key, value in raw.items():
        if key in _DERIVE_META_KEYS:
            meta[key] = value
        else:
            conn_updates[key] = value
    if "read_only" not in conn_updates:
        conn_updates["read_only"] = True
    if "database" in conn_updates and "path" not in conn_updates:
        conn_updates["path"] = str(conn_updates["database"])
    elif "path" in conn_updates and "database" not in conn_updates:
        conn_updates["database"] = str(conn_updates["path"])

    merged_check = {**adapter.connection_config, **conn_updates}
    _validate_duckdb_replica_config(adapter.connection_config, merged_check, primary_driver_cfg=primary_cfg)
    meta["connection_config_overrides"] = conn_updates
    return meta


def _derive_duckdb_adapter(adapter: Any, overrides: Mapping[str, Any]) -> Any:
    """Clone `DuckDBSyncConfig` or `DuckDBAsyncConfig` with non-mutating connection and feature overrides."""
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

    driver_cfg: DuckDBConfig = adapter.driver_config
    merged_conn: dict[str, Any] = {
        "database": driver_cfg.resolved_path(),
        "path": str(driver_cfg.path if driver_cfg.path != ":memory:" else driver_cfg.resolved_path()),
        "read_only": bool(driver_cfg.read_only),
        "memory_limit_mb": int(driver_cfg.memory_limit_mb),
        "threads": int(driver_cfg.threads),
        "parquet_metadata_cache": bool(driver_cfg.parquet_metadata_cache),
        "httpfs_connection_caching": bool(driver_cfg.httpfs_connection_caching),
        "enable_http_metadata_cache": bool(driver_cfg.enable_http_metadata_cache),
        "pool_min_size": int(driver_cfg.pool_min_size),
        "pool_max_size": int(driver_cfg.pool_max_size),
        "statement_cache_size": int(driver_cfg.statement_cache_size),
    }
    if driver_cfg.temp_directory is not None:
        merged_conn["temp_directory"] = driver_cfg.temp_directory
    if driver_cfg.config:
        merged_conn["config"] = dict(driver_cfg.config)
    if driver_cfg.extensions:
        merged_conn["extensions"] = driver_cfg.extensions
    if driver_cfg.secrets:
        merged_conn["secrets"] = driver_cfg.secrets
    if driver_cfg.init_statements:
        merged_conn["init_statements"] = driver_cfg.init_statements

    merged_conn.update(adapter.connection_config)
    override_conn = overrides.get("connection_config")
    if isinstance(override_conn, Mapping):
        merged_conn.update(override_conn)

    conn_overrides = overrides.get("connection_config_overrides")
    extra_updates: dict[str, Any] = dict(conn_overrides) if isinstance(conn_overrides, Mapping) else {}
    for key, value in overrides.items():
        if key in _DERIVE_META_KEYS:
            continue
        if key in adapter.driver_features:
            merged_features[key] = value
        else:
            extra_updates[key] = value

    if any(alias in extra_updates for alias in _URL_KEYS):
        for alias in (*_URL_KEYS, "database", "path", "db", "file", "memory_id"):
            merged_conn.pop(alias, None)
    elif any(key in extra_updates for key in ("database", "path", "db", "file", "memory_id")):
        for alias in _URL_KEYS:
            merged_conn.pop(alias, None)
        if "database" in extra_updates and "path" not in extra_updates:
            merged_conn["path"] = str(extra_updates["database"])
        elif "path" in extra_updates and "database" not in extra_updates:
            merged_conn["database"] = str(extra_updates["path"])

    merged_conn.update(extra_updates)
    is_replica = bool(overrides.get("is_replica", False))
    if is_replica:
        _validate_duckdb_replica_config(adapter.connection_config, merged_conn, primary_driver_cfg=driver_cfg)

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
        is_replica=is_replica,
    )


class DuckDBSyncConfig(SyncAdapterConfig[DuckDBConfig, SyncDuckDBDriver, SyncDuckDBDriver, DuckDBSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncDuckDBDriver`."""

    __slots__ = ("_baseline_databases", "_initialized_conn_ids", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_duckdb"
    dialect: ClassVar[str] = "duckdb"
    connection_type: ClassVar[type[Any]] = SyncDuckDBDriver
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[SyncDuckDBDriver]] = SyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBSyncDriver]] = DuckDBSyncDriver
    _connection_context_class: ClassVar[type[Any]] = DuckDBSyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = DuckDBSyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBSyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBSyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBSyncADKArtifactStore",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.events",
        "DuckDBEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.litestar",
        "DuckDBLitestarStore",
    )

    def __init__(
        self,
        config: DuckDBConfig | DuckDBConnectionParams | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: DuckDBConnectionParams | DuckDBPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: DuckDBDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous DuckDB SQLSpec configuration preserving isolated `:memory:<name>` DBs."""
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features, resolved_stmt_cfg = (
            _prepare_duckdb_setup(
                config,
                share_pool=share_pool,
                broker=broker,
                connection_config=connection_config,
                driver_features=driver_features,
                statement_config=statement_config,
                reset_session_on_release=reset_session_on_release,
                kwargs=kwargs,
            )
        )
        self._baseline_databases: dict[int, tuple[frozenset[str], str]] = {}
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = features.pop("on_connection_create", None)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=resolved_stmt_cfg,
            connection_config=normalized_conn,
            connection_instance=connection_instance,
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
    ) -> AbstractContextManager[DuckDBSyncDriver]:
        """Yield a fresh synchronous DuckDB SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractContextManager[SyncDuckDBDriver]:
        """Yield a synchronous native DuckDB driver context forwarding positional and keyword options."""
        return cast("AbstractContextManager[SyncDuckDBDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build DuckDB replica overrides rejecting invalid `:memory:` loops and preserving read-only access."""
        return _build_duckdb_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous DuckDB configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_duckdb_adapter(self, overrides))

    def _close_pool(self) -> None:
        """Close the synchronous DuckDB pool and clear tracked connection initialization state."""
        self._initialized_conn_ids.clear()
        self._baseline_databases.clear()
        super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return DuckDB adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "DuckDBConfig": DuckDBConfig,
                "DuckDBConnectionParams": DuckDBConnectionParams,
                "DuckDBDriverFeatures": DuckDBDriverFeatures,
                "DuckDBPoolParams": DuckDBPoolParams,
                "DuckDBSyncConnectionContext": DuckDBSyncConnectionContext,
                "DuckDBSyncDriver": DuckDBSyncDriver,
                "DuckDBSyncSessionContext": DuckDBSyncSessionContext,
                "SyncDuckDBDriver": SyncDuckDBDriver,
            }
        )
        return namespace


class DuckDBAsyncConfig(AsyncAdapterConfig[DuckDBConfig, AsyncDuckDBDriver, AsyncDuckDBDriver, DuckDBAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncDuckDBDriver`."""

    __slots__ = ("_baseline_databases", "_initialized_conn_ids", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_duckdb"
    dialect: ClassVar[str] = "duckdb"
    connection_type: ClassVar[type[Any]] = AsyncDuckDBDriver
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[AsyncDuckDBDriver]] = AsyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBAsyncDriver]] = DuckDBAsyncDriver
    _connection_context_class: ClassVar[type[Any]] = DuckDBAsyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = DuckDBAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBAsyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBAsyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBAsyncADKArtifactStore",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.events",
        "DuckDBEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.duckdb.litestar",
        "DuckDBLitestarStore",
    )

    def __init__(
        self,
        config: DuckDBConfig | DuckDBConnectionParams | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: DuckDBConnectionParams | DuckDBPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: DuckDBDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous DuckDB SQLSpec configuration preserving isolated `:memory:<name>` DBs."""
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features, resolved_stmt_cfg = (
            _prepare_duckdb_setup(
                config,
                share_pool=share_pool,
                broker=broker,
                connection_config=connection_config,
                driver_features=driver_features,
                statement_config=statement_config,
                reset_session_on_release=reset_session_on_release,
                kwargs=kwargs,
            )
        )
        self._baseline_databases: dict[int, tuple[frozenset[str], str]] = {}
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = features.pop("on_connection_create", None)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=resolved_stmt_cfg,
            connection_config=normalized_conn,
            connection_instance=connection_instance,
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
    ) -> AbstractAsyncContextManager[DuckDBAsyncDriver]:
        """Yield a fresh asynchronous DuckDB SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractAsyncContextManager[AsyncDuckDBDriver]:
        """Yield an asynchronous native DuckDB driver context forwarding positional and keyword options."""
        return cast("AbstractAsyncContextManager[AsyncDuckDBDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build DuckDB replica overrides rejecting invalid `:memory:` loops and preserving read-only access."""
        return _build_duckdb_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous DuckDB configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_duckdb_adapter(self, overrides))

    async def _close_pool(self) -> None:
        """Close the asynchronous DuckDB pool and clear tracked connection initialization state."""
        self._initialized_conn_ids.clear()
        self._baseline_databases.clear()
        await super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return DuckDB adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "AsyncDuckDBDriver": AsyncDuckDBDriver,
                "DuckDBAsyncConnectionContext": DuckDBAsyncConnectionContext,
                "DuckDBAsyncDriver": DuckDBAsyncDriver,
                "DuckDBAsyncSessionContext": DuckDBAsyncSessionContext,
                "DuckDBConfig": DuckDBConfig,
                "DuckDBConnectionParams": DuckDBConnectionParams,
                "DuckDBDriverFeatures": DuckDBDriverFeatures,
                "DuckDBPoolParams": DuckDBPoolParams,
            }
        )
        return namespace
