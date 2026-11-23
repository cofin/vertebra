"""Cloud Spanner and Spangres SQLSpec synchronous and asynchronous adapter configurations."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, cast
from urllib.parse import urlsplit

from sqlspec.core import StatementConfig

from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    SpannerConfig,
    StalenessBound,
    SyncSpannerDriver,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.driver import NotSupportedError
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import (
    AsyncAdapterConfig,
    LazyStoreAttr,
    SyncAdapterConfig,
)
from vertebra.extensions.sqlspec.adapters.spanner._typing import (
    SpannerAsyncConnectionContext,
    SpannerAsyncSessionContext,
    SpannerConnectionParams,
    SpannerDriverFeatures,
    SpannerPoolParams,
    SpannerSyncConnectionContext,
    SpannerSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.spanner.core import (
    ADAPTER_NAME,
    SpannerConnectionShim,
    default_statement_config,
    normalize_spanner_dialect,
    resolve_spanner_config,
    resolve_storage_backend,
    sqlspec_dialect_name,
)
from vertebra.extensions.sqlspec.adapters.spanner.driver import (
    SpannerAsyncDriver,
    SpannerSyncDriver,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from uuid import UUID

__all__ = (
    "SpangresAsyncConfig",
    "SpangresSyncConfig",
    "SpannerAsyncConfig",
    "SpannerSyncConfig",
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
_SPANNER_FEATURE_KEYS: frozenset[str] = frozenset(
    {
        "default_staleness",
        "enable_batch_write_api",
        "enable_events",
        "enable_sqlcommenter",
        "enable_uuid_conversion",
        "events_backend",
        "json_deserializer",
        "json_serializer",
        "on_connection_create",
        "reset_session_on_release",
        "statement_cache_size",
        "storage_backend",
    }
)
_SPANNER_STRUCT_FIELDS: frozenset[str] = frozenset(SpannerConfig.__struct_fields__)


class _ConfigDialectDescriptor:
    """Slot-compatible descriptor exposing class-level default dialect and per-instance overrides."""

    __slots__ = ("_default",)

    def __init__(self, default: str) -> None:
        self._default = default

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> str:
        _ = owner
        if instance is None:
            return self._default
        value = getattr(instance, "_dialect_value", None)
        return str(value) if value is not None else self._default

    def __set__(self, instance: Any, value: str) -> None:
        instance._dialect_value = str(value)


def _reject_tenant_settings(
    tenant_predicate: Any,
    tenant_id: Any,
    driver_features: Mapping[str, Any] | None,
) -> None:
    if tenant_predicate is not None or tenant_id is not None:
        msg = "Tenant session enforcement is unsupported for Cloud Spanner"
        raise NotSupportedError(msg)
    if driver_features and (
        driver_features.get("tenant_predicate") is not None or driver_features.get("tenant_id") is not None
    ):
        msg = "Tenant session enforcement is unsupported for Cloud Spanner"
        raise NotSupportedError(msg)


def _filter_driver_features(
    driver_features: Mapping[str, Any] | None,
    *,
    storage_backend: ObjectStoreBackend | str | None,
    statement_cache_size: int,
    enable_sqlcommenter: bool,
    reset_session_on_release: bool | None = None,
) -> dict[str, Any]:
    features = dict(driver_features or {})
    if storage_backend is not None and "storage_backend" not in features:
        features["storage_backend"] = storage_backend
    if "statement_cache_size" not in features:
        features["statement_cache_size"] = statement_cache_size
    if enable_sqlcommenter and "enable_sqlcommenter" not in features:
        features["enable_sqlcommenter"] = True
    if reset_session_on_release is not None:
        features["reset_session_on_release"] = bool(reset_session_on_release)
    return features


def _normalize_spanner_conn_dict(raw_conn: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(raw_conn)
    if "project" in normalized and "project_id" not in normalized:
        normalized["project_id"] = normalized.pop("project")
    else:
        normalized.pop("project", None)
    if "emulator_host" in normalized and "endpoint" not in normalized:
        normalized["endpoint"] = normalized.pop("emulator_host")
    else:
        normalized.pop("emulator_host", None)
    if "url" in normalized and "dsn" not in normalized:
        normalized["dsn"] = normalized.pop("url")
    else:
        normalized.pop("url", None)
    max_sessions = normalized.pop("max_sessions", None)
    min_sessions = normalized.pop("min_sessions", None)
    if "pool_size" not in normalized:
        if max_sessions is not None:
            normalized["pool_size"] = max(1, int(max_sessions))
        elif min_sessions is not None:
            normalized["pool_size"] = max(1, int(min_sessions))
    return normalized


def _prepare_spanner_init(
    config: SpannerConfig | SpannerConnectionParams | Mapping[str, Any] | str | None,
    *,
    dsn: str | None,
    project_id: str | None,
    instance_id: str | None,
    database_id: str | None,
    database_role: str | None,
    dialect: str,
    endpoint: str | None,
    admin_endpoint: str | None,
    pool_size: int,
    route_to_leader_enabled: bool,
    max_commit_delay_ms: int | None,
    data_boost_enabled: bool,
    max_partitions: int,
    credentials_path: str | None,
    service_account_key: str | dict[str, Any] | None,
    quota_project_id: str | None,
    impersonate_service_account: str | None,
    use_gcloud_cli: bool,
    scopes: Sequence[str] | None,
    access_token: str | None,
    use_shm_cache: bool,
    statement_cache_capacity: int,
    ca_pem: bytes | None,
    storage_backend: ObjectStoreBackend | str | None,
    connection_instance: Any,
    statement_cache_size: int,
    enable_sqlcommenter: bool,
    broker: Any,
    share_pool: bool | None,
    connection_config: SpannerConnectionParams | SpannerPoolParams | Mapping[str, Any] | None,
    driver_features: SpannerDriverFeatures | Mapping[str, Any] | None,
    reset_session_on_release: bool | None,
    kwargs: dict[str, Any],
) -> tuple[SpannerConfig, dict[str, Any], dict[str, Any], Any, Any, bool, Any]:
    """Normalize Spanner config, pool options, driver features, and connection instance."""
    raw_conn = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    if isinstance(config, Mapping):
        for key, value in config.items():
            raw_conn.setdefault(key, value)
    extra_kw = dict(kwargs)
    if "reset_session_on_release" in extra_kw and reset_session_on_release is None:
        reset_session_on_release = bool(extra_kw.pop("reset_session_on_release"))
    else:
        extra_kw.pop("reset_session_on_release", None)
    for key, value in extra_kw.items():
        raw_conn.setdefault(key, value)

    pool_conn_instance = raw_conn.pop("connection_instance", None)
    pool_share = raw_conn.pop("share_pool", None)
    resolved_share_pool = (
        bool(share_pool) if share_pool is not None else bool(pool_share) if pool_share is not None else True
    )
    staleness_default = raw_conn.pop("staleness", None)
    raw_conn.pop("extra", None)

    merged_features = _filter_driver_features(
        driver_features,
        storage_backend=storage_backend,
        statement_cache_size=statement_cache_size,
        enable_sqlcommenter=enable_sqlcommenter,
        reset_session_on_release=reset_session_on_release,
    )
    if staleness_default is not None and "default_staleness" not in merged_features:
        merged_features["default_staleness"] = staleness_default
    features_conn_instance = merged_features.pop("connection_instance", None)
    user_hook = merged_features.pop("on_connection_create", None)
    resolved_instance = (
        connection_instance
        if connection_instance is not None
        else pool_conn_instance
        if pool_conn_instance is not None
        else features_conn_instance
    )

    config_broker = getattr(config, "broker", None) if isinstance(config, SpannerConfig) else None
    resolved_broker = (
        broker
        if broker is not None
        else config_broker
        if config_broker is not None
        else DatabaseRegistry.default_broker()
        if resolved_share_pool
        else None
    )

    normalized_conn = _normalize_spanner_conn_dict(raw_conn)
    struct_map: dict[str, Any] = {key: value for key, value in normalized_conn.items() if key in _SPANNER_STRUCT_FIELDS}
    config_input: SpannerConfig | Mapping[str, Any] | str | None
    if isinstance(config, (SpannerConfig, str)):
        config_input = config
    elif isinstance(config, Mapping) or struct_map:
        config_input = struct_map
    else:
        config_input = None

    resolved_cfg = resolve_spanner_config(
        config_input,
        dsn=dsn,
        project_id=project_id,
        instance_id=instance_id,
        database_id=database_id,
        database_role=database_role,
        dialect=dialect,
        endpoint=endpoint,
        admin_endpoint=admin_endpoint,
        pool_size=pool_size,
        route_to_leader_enabled=route_to_leader_enabled,
        max_commit_delay_ms=max_commit_delay_ms,
        data_boost_enabled=data_boost_enabled,
        max_partitions=max_partitions,
        credentials_path=credentials_path,
        service_account_key=service_account_key,
        quota_project_id=quota_project_id,
        impersonate_service_account=impersonate_service_account,
        use_gcloud_cli=use_gcloud_cli,
        scopes=scopes,
        access_token=access_token,
        use_shm_cache=use_shm_cache,
        statement_cache_capacity=statement_cache_capacity,
        ca_pem=ca_pem,
        broker=resolved_broker,
    )
    conn_meta = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    if isinstance(config, Mapping):
        for key, value in config.items():
            conn_meta.setdefault(key, value)
    if resolved_cfg.project_id:
        conn_meta.setdefault("project_id", resolved_cfg.project_id)
    if resolved_cfg.instance_id:
        conn_meta.setdefault("instance_id", resolved_cfg.instance_id)
    if resolved_cfg.database_id:
        conn_meta.setdefault("database_id", resolved_cfg.database_id)
    if resolved_cfg.endpoint:
        conn_meta.setdefault("endpoint", resolved_cfg.endpoint)
    return resolved_cfg, conn_meta, merged_features, resolved_instance, resolved_broker, resolved_share_pool, user_hook


def _build_spanner_replica_overrides(adapter: Any, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
    """Build Spanner replica ``derive_pool`` overrides from DSN URIs, database IDs, or mappings."""
    if isinstance(spec_input, bool):
        msg = f"{type(adapter).__name__} requires explicit replica DSN, database, or connection configuration"
        raise ImproperConfigurationError(msg)
    if isinstance(spec_input, str):
        endpoint = spec_input.strip()
        if not endpoint:
            msg = "Spanner replica specification must not be empty"
            raise ImproperConfigurationError(msg)
        parsed = urlsplit(endpoint)
        scheme = parsed.scheme.lower()
        if scheme in {"spanner", "spangres"}:
            return {"connection_config_overrides": {"dsn": endpoint}}
        if scheme in {"http", "https"}:
            return {"connection_config_overrides": {"endpoint": endpoint}}
        return {"connection_config_overrides": {"database_id": endpoint}}
    data = dict(spec_input)
    if "connection_config_overrides" in data or "driver_features_overrides" in data:
        return data
    meta: dict[str, Any] = {}
    conn_overrides: dict[str, Any] = {}
    for key, value in data.items():
        if key in _DERIVE_META_KEYS:
            meta[key] = value
        else:
            conn_overrides[key] = value
    if conn_overrides:
        meta["connection_config_overrides"] = conn_overrides
    return meta


def _derive_spanner_adapter(adapter: Any, overrides: Mapping[str, Any]) -> Any:
    """Clone ``SpannerSyncConfig`` or ``SpannerAsyncConfig`` with non-mutating replica or feature overrides."""
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
    merged_conn: dict[str, Any] = {}
    for field_name in _SPANNER_STRUCT_FIELDS:
        if field_name == "broker":
            continue
        val = getattr(driver_cfg, field_name, None)
        if val is not None:
            merged_conn[field_name] = val
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
        if key in _SPANNER_FEATURE_KEYS or key in adapter.driver_features:
            merged_features[key] = value
        else:
            extra_updates[key] = value

    if "project" in extra_updates and "project_id" not in extra_updates:
        extra_updates["project_id"] = extra_updates["project"]
    if "emulator_host" in extra_updates and "endpoint" not in extra_updates:
        extra_updates["endpoint"] = extra_updates["emulator_host"]

    has_dsn_override = any(isinstance(extra_updates.get(k), str) and extra_updates[k].strip() for k in ("dsn", "url"))
    if has_dsn_override:
        for key in ("project", "project_id", "instance_id", "database_id", "dsn", "url"):
            merged_conn.pop(key, None)
    elif any(k in extra_updates for k in ("project", "project_id", "instance_id", "database_id")):
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
        storage_backend=merged_features.get("storage_backend", adapter.storage_backend),
        statement_cache_size=int(merged_features.get("statement_cache_size", adapter.statement_cache_size)),
        enable_sqlcommenter=bool(merged_features.get("enable_sqlcommenter", adapter.enable_sqlcommenter)),
        replicas=None,
        is_replica=bool(overrides.get("is_replica", False)),
    )


class _SyncSnapshotAsyncProxy:
    """Async adapter delegating ``SyncSpannerSnapshot`` operations via ``asyncio.to_thread``."""

    __slots__ = ("_snapshot",)

    def __init__(self, snapshot: Any) -> None:
        self._snapshot = snapshot

    @property
    def staleness(self) -> Any:
        """Return the underlying snapshot staleness bound."""
        return getattr(self._snapshot, "staleness", None)

    async def __aenter__(self) -> _SyncSnapshotAsyncProxy:
        enter = getattr(self._snapshot, "__enter__", None)
        if callable(enter):
            await asyncio.to_thread(enter)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        exit_fn = getattr(self._snapshot, "__exit__", None)
        if callable(exit_fn):
            await asyncio.to_thread(exit_fn, exc_type, exc, tb)

    async def query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a read-only query on a worker thread."""
        return list(await asyncio.to_thread(self._snapshot.query, sql, parameters, **kwargs))

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a read-only select on a worker thread."""
        return list(await asyncio.to_thread(self._snapshot.select, sql, parameters, **kwargs))

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an Arrow query on a worker thread."""
        return await asyncio.to_thread(self._snapshot.select_to_arrow, sql, parameters, **kwargs)


class _SyncSpannerAsyncProxy:
    """Async core-driver proxy offloading ``SyncSpannerDriver`` calls via ``asyncio.to_thread``."""

    __slots__ = ("_sync",)

    def __init__(self, sync_driver: SyncSpannerDriver) -> None:
        self._sync = sync_driver

    @property
    def config(self) -> SpannerConfig:
        """Return the underlying ``SpannerConfig``."""
        return self._sync.config

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the Spanner dialect literal from the synchronous driver."""
        return self._sync.dialect

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the normalized Spanner dialect literal."""
        return normalize_spanner_dialect(self._sync.config.dialect)

    @property
    def project_id(self) -> str:
        """Return the Google Cloud project ID."""
        return self._sync.project_id

    @property
    def instance_id(self) -> str:
        """Return the Cloud Spanner instance ID."""
        return self._sync.instance_id

    @property
    def database_id(self) -> str:
        """Return the Cloud Spanner database ID."""
        return self._sync.database_id

    @property
    def last_commit_stats(self) -> dict[str, Any]:
        """Return commit statistics recorded on the synchronous driver."""
        return dict(getattr(self._sync, "last_commit_stats", {}) or {})

    @property
    def active_transaction(self) -> Any:
        """Return the active interactive transaction on the synchronous driver, if any."""
        return getattr(self._sync, "active_transaction", None)

    @property
    def transaction_id(self) -> bytes | None:
        """Return the active transaction identifier bytes on the synchronous driver, if any."""
        return getattr(self._sync, "transaction_id", None)

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> _SyncSnapshotAsyncProxy:
        """Open a thread-offloaded async snapshot wrapper over ``SyncSpannerSnapshot``."""
        return _SyncSnapshotAsyncProxy(self._sync.snapshot(staleness, multi_use=multi_use))

    async def connect(self) -> _SyncSpannerAsyncProxy:
        """Connect the underlying synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.connect)
        return self

    async def close(self) -> None:
        """Close the underlying synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.close)

    async def begin(self, **kwargs: Any) -> None:
        """Begin a transaction on the synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.begin, **kwargs)

    async def commit(self) -> None:
        """Commit the active transaction on a worker thread."""
        await asyncio.to_thread(self._sync.commit)

    async def rollback(self) -> None:
        """Roll back the active transaction on a worker thread."""
        await asyncio.to_thread(self._sync.rollback)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute DML or DDL on the synchronous driver via ``asyncio.to_thread``."""
        return int(await asyncio.to_thread(self._sync.execute, sql, parameters, **kwargs))

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute parameterized batch DML on a worker thread."""
        return int(await asyncio.to_thread(self._sync.execute_many, sql, parameter_sets, **kwargs))

    async def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> list[int]:
        """Execute batch DML on a worker thread."""
        return list(await asyncio.to_thread(self._sync.execute_batch_dml, statements, **kwargs))

    async def execute_partitioned_dml(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute partitioned DML on a worker thread."""
        return int(await asyncio.to_thread(self._sync.execute_partitioned_dml, sql, parameters, **kwargs))

    async def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        """Execute a SQL script on a worker thread."""
        return int(await asyncio.to_thread(self._sync.execute_script, sql, parameters))

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query returning rows and column names on a worker thread."""
        if staleness is not None:
            return await asyncio.to_thread(
                self._sync.execute_with_description, sql, parameters, staleness=staleness, **kwargs
            )
        return await asyncio.to_thread(self._sync.execute_with_description, sql, parameters, **kwargs)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a select query on a worker thread."""
        if staleness is not None:
            return list(
                await asyncio.to_thread(
                    self._sync.select,
                    sql,
                    parameters,
                    schema_type=schema_type,
                    staleness=staleness,
                    **kwargs,
                )
            )
        return list(await asyncio.to_thread(self._sync.select, sql, parameters, schema_type=schema_type, **kwargs))

    async def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Execute Spanner DDL statements on a worker thread."""
        return str(await asyncio.to_thread(self._sync.update_ddl, statements))

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an Arrow read on a worker thread."""
        return await asyncio.to_thread(self._sync.select_to_arrow, sql, parameters, **kwargs)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        **kwargs: Any,
    ) -> int:
        """Load Arrow records on a worker thread."""
        return int(await asyncio.to_thread(self._sync.load_from_arrow, table_name, data, **kwargs))

    async def apply_mutations(
        self,
        mutations: Sequence[Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Apply Spanner mutations on a worker thread."""
        return dict(await asyncio.to_thread(self._sync.apply_mutations, mutations, **kwargs))

    async def batch_write(
        self,
        mutation_groups: Sequence[Any],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Execute BatchWrite on a worker thread."""
        return list(await asyncio.to_thread(self._sync.batch_write, mutation_groups, **kwargs))

    async def run_in_transaction(self, fn: Any, **kwargs: Any) -> Any:
        """Run a synchronous transaction callback on a worker thread."""
        return await asyncio.to_thread(self._sync.run_in_transaction, fn, **kwargs)


class SpannerSyncConfig(SyncAdapterConfig[SpannerConfig, SpannerConnectionShim, SyncSpannerDriver, SpannerSyncDriver]):
    """Synchronous SQLSpec adapter configuration for Cloud Spanner GoogleSQL and Spangres."""

    __slots__ = (
        "_cached_core",
        "_dialect_value",
        "_initialized_conn_ids",
        "_resolved_config",
        "_spanner_dialect",
        "_user_connection_hook",
        "enable_sqlcommenter",
        "statement_cache_size",
        "storage_backend",
        "tenant_id",
        "tenant_predicate",
    )

    adapter_name: ClassVar[str | None] = ADAPTER_NAME
    dialect: ClassVar[str] = cast("str", _ConfigDialectDescriptor("spanner"))
    is_async: ClassVar[bool] = False
    connection_type: ClassVar[type[Any]] = SyncSpannerDriver
    _config_cls: ClassVar[type[SpannerConfig]] = SpannerConfig
    _driver_cls: ClassVar[type[SyncSpannerDriver]] = SyncSpannerDriver
    driver_type: ClassVar[type[SpannerSyncDriver]] = SpannerSyncDriver
    _connection_context_class: ClassVar[type[Any]] = SpannerSyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = SpannerSyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerSyncADKStore",
        package="google-adk",
        extra="adk",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerSyncADKMemoryStore",
        package="google-adk",
        extra="adk",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerSyncADKArtifactStore",
        package="google-adk",
        extra="adk",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.events",
        "SpannerEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.litestar",
        "SpannerLitestarStore",
        package="litestar",
        extra="litestar",
    )

    def __init__(
        self,
        config: SpannerConfig | SpannerConnectionParams | Mapping[str, Any] | str | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        instance_id: str | None = None,
        database_id: str | None = None,
        database_role: str | None = None,
        dialect: str = "googlesql",
        endpoint: str | None = None,
        admin_endpoint: str | None = None,
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int | None = None,
        data_boost_enabled: bool = True,
        max_partitions: int = 4,
        credentials_path: str | None = None,
        service_account_key: str | dict[str, Any] | None = None,
        quota_project_id: str | None = None,
        impersonate_service_account: str | None = None,
        use_gcloud_cli: bool = False,
        scopes: Sequence[str] | None = None,
        access_token: str | None = None,
        use_shm_cache: bool = True,
        statement_cache_capacity: int = 512,
        ca_pem: bytes | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = False,
        tenant_predicate: Any = None,
        tenant_id: Any = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: SpannerConnectionParams | SpannerPoolParams | dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: SpannerDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        _reject_tenant_settings(tenant_predicate, tenant_id, driver_features)
        (
            resolved_cfg,
            conn_meta,
            merged_features,
            resolved_instance,
            resolved_broker,
            resolved_share_pool,
            user_hook,
        ) = _prepare_spanner_init(
            config,
            dsn=dsn,
            project_id=project_id,
            instance_id=instance_id,
            database_id=database_id,
            database_role=database_role,
            dialect=dialect,
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
            pool_size=pool_size,
            route_to_leader_enabled=route_to_leader_enabled,
            max_commit_delay_ms=max_commit_delay_ms,
            data_boost_enabled=data_boost_enabled,
            max_partitions=max_partitions,
            credentials_path=credentials_path,
            service_account_key=service_account_key,
            quota_project_id=quota_project_id,
            impersonate_service_account=impersonate_service_account,
            use_gcloud_cli=use_gcloud_cli,
            scopes=scopes,
            access_token=access_token,
            use_shm_cache=use_shm_cache,
            statement_cache_capacity=statement_cache_capacity,
            ca_pem=ca_pem,
            storage_backend=storage_backend,
            connection_instance=connection_instance,
            statement_cache_size=statement_cache_size,
            enable_sqlcommenter=enable_sqlcommenter,
            broker=broker,
            share_pool=share_pool,
            connection_config=connection_config,
            driver_features=driver_features,
            reset_session_on_release=reset_session_on_release,
            kwargs=kwargs,
        )
        self._spanner_dialect: Literal["googlesql", "spangres"] = normalize_spanner_dialect(resolved_cfg.dialect)
        self._dialect_value: str = sqlspec_dialect_name(self._spanner_dialect)
        self._resolved_config: SpannerConfig = resolved_cfg
        self.storage_backend: ObjectStoreBackend | str | None = merged_features.get("storage_backend", storage_backend)
        self.statement_cache_size: int = int(merged_features.get("statement_cache_size", statement_cache_size))
        self.enable_sqlcommenter: bool = bool(merged_features.get("enable_sqlcommenter", enable_sqlcommenter))
        self.tenant_predicate: Any = tenant_predicate
        self.tenant_id: Any = tenant_id
        self._cached_core: SyncSpannerDriver | None = None
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = user_hook
        effective_stmt_cfg = statement_config or default_statement_config(self._spanner_dialect)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=effective_stmt_cfg,
            connection_config=conn_meta,
            connection_instance=resolved_instance,
            migration_config=migration_config,
            driver_features=merged_features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the underlying Vertebra Spanner dialect literal (``googlesql`` or ``spangres``)."""
        return self._spanner_dialect

    def resolve_spanner_config(self) -> SpannerConfig:
        """Return the canonical ``SpannerConfig`` for this SQLSpec configuration."""
        return self._resolved_config

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve the configured ``ObjectStoreBackend`` instance, if any."""
        return resolve_storage_backend(self.storage_backend)

    def _resolve_core_driver(self) -> Any:
        if self.connection_instance is not None:
            if isinstance(self.connection_instance, SpannerConnectionShim):
                return self.connection_instance.driver
            return self.connection_instance
        if self._cached_core is None:
            self._cached_core = SyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_core

    def create_driver(self) -> SpannerSyncDriver:
        """Create a standalone ``SpannerSyncDriver`` over the resolved core driver."""
        return SpannerSyncDriver(
            self._resolve_core_driver(),
            statement_config=self.statement_config,
            driver_features=dict(self.driver_features),
            observability=self.observability_config,
            shared_pool=bool(self.share_pool),
            tenant_predicate=self.tenant_predicate,
            tenant_id=self.tenant_id,
            storage_backend=self.resolve_storage_backend(),
        )

    def provide_driver(
        self,
        broker: Any = None,
        statement_cache: Any = None,
    ) -> SpannerSyncDriver:
        """Return a configured ``SpannerSyncDriver`` instance."""
        _ = broker, statement_cache
        return self.create_driver()

    def create_connection(self) -> Any:
        """Return a ``SpannerConnectionShim`` over the synchronous core driver."""
        core = self._resolve_core_driver()
        return SpannerConnectionShim(core) if isinstance(core, SyncSpannerDriver) else core

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[SpannerSyncDriver]:
        """Yield an isolated ``SpannerSyncDriver`` for autocommit, ReadOnly snapshot, or ReadWrite transaction."""
        _reject_tenant_settings(kwargs.get("tenant_predicate"), kwargs.get("tenant_id"), None)
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            transaction=transaction,
            staleness=staleness,
            read_only=read_only,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractContextManager[Any]:
        """Yield a synchronous native Spanner driver context forwarding positional and keyword options."""
        return super().provide_connection(*args, **kwargs)

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build Spanner replica overrides from DSN URIs, database IDs, or explicit dictionaries."""
        return _build_spanner_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous Spanner configuration with non-mutating replica or feature overrides."""
        return cast("Self", _derive_spanner_adapter(self, overrides))

    def _close_pool(self) -> None:
        """Close the synchronous Spanner pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        super()._close_pool()

    def close(self) -> None:
        """Close the cached synchronous Spanner core driver and adapter pool."""
        if self._cached_core is not None:
            close_fn = getattr(self._cached_core, "close", None)
            if callable(close_fn):
                close_fn()
            self._cached_core = None
        super().close()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return Spanner adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "SpannerConfig": SpannerConfig,
                "SpannerConnectionParams": SpannerConnectionParams,
                "SpannerConnectionShim": SpannerConnectionShim,
                "SpannerDriverFeatures": SpannerDriverFeatures,
                "SpannerPoolParams": SpannerPoolParams,
                "SpannerSyncConnectionContext": SpannerSyncConnectionContext,
                "SpannerSyncDriver": SpannerSyncDriver,
                "SpannerSyncSessionContext": SpannerSyncSessionContext,
                "SyncSpannerDriver": SyncSpannerDriver,
            }
        )
        return namespace


class SpannerAsyncConfig(
    AsyncAdapterConfig[SpannerConfig, SpannerConnectionShim, AsyncSpannerDriver, SpannerAsyncDriver]
):
    """Asynchronous SQLSpec adapter configuration for Cloud Spanner GoogleSQL and Spangres."""

    __slots__ = (
        "_cached_core",
        "_cached_sync_core",
        "_dialect_value",
        "_initialized_conn_ids",
        "_resolved_config",
        "_spanner_dialect",
        "_user_connection_hook",
        "enable_sqlcommenter",
        "statement_cache_size",
        "storage_backend",
        "tenant_id",
        "tenant_predicate",
    )

    adapter_name: ClassVar[str | None] = ADAPTER_NAME
    dialect: ClassVar[str] = cast("str", _ConfigDialectDescriptor("spanner"))
    is_async: ClassVar[bool] = True
    connection_type: ClassVar[type[Any]] = AsyncSpannerDriver
    _config_cls: ClassVar[type[SpannerConfig]] = SpannerConfig
    _driver_cls: ClassVar[type[AsyncSpannerDriver]] = AsyncSpannerDriver
    driver_type: ClassVar[type[SpannerAsyncDriver]] = SpannerAsyncDriver
    _connection_context_class: ClassVar[type[Any]] = SpannerAsyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = SpannerAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerAsyncADKStore",
        package="google-adk",
        extra="adk",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerAsyncADKMemoryStore",
        package="google-adk",
        extra="adk",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerAsyncADKArtifactStore",
        package="google-adk",
        extra="adk",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.events",
        "SpannerEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.spanner.litestar",
        "SpannerLitestarStore",
        package="litestar",
        extra="litestar",
    )

    def __init__(
        self,
        config: SpannerConfig | SpannerConnectionParams | Mapping[str, Any] | str | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        instance_id: str | None = None,
        database_id: str | None = None,
        database_role: str | None = None,
        dialect: str = "googlesql",
        endpoint: str | None = None,
        admin_endpoint: str | None = None,
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int | None = None,
        data_boost_enabled: bool = True,
        max_partitions: int = 4,
        credentials_path: str | None = None,
        service_account_key: str | dict[str, Any] | None = None,
        quota_project_id: str | None = None,
        impersonate_service_account: str | None = None,
        use_gcloud_cli: bool = False,
        scopes: Sequence[str] | None = None,
        access_token: str | None = None,
        use_shm_cache: bool = True,
        statement_cache_capacity: int = 512,
        ca_pem: bytes | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = False,
        tenant_predicate: Any = None,
        tenant_id: Any = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: SpannerConnectionParams | SpannerPoolParams | dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: SpannerDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        _reject_tenant_settings(tenant_predicate, tenant_id, driver_features)
        (
            resolved_cfg,
            conn_meta,
            merged_features,
            resolved_instance,
            resolved_broker,
            resolved_share_pool,
            user_hook,
        ) = _prepare_spanner_init(
            config,
            dsn=dsn,
            project_id=project_id,
            instance_id=instance_id,
            database_id=database_id,
            database_role=database_role,
            dialect=dialect,
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
            pool_size=pool_size,
            route_to_leader_enabled=route_to_leader_enabled,
            max_commit_delay_ms=max_commit_delay_ms,
            data_boost_enabled=data_boost_enabled,
            max_partitions=max_partitions,
            credentials_path=credentials_path,
            service_account_key=service_account_key,
            quota_project_id=quota_project_id,
            impersonate_service_account=impersonate_service_account,
            use_gcloud_cli=use_gcloud_cli,
            scopes=scopes,
            access_token=access_token,
            use_shm_cache=use_shm_cache,
            statement_cache_capacity=statement_cache_capacity,
            ca_pem=ca_pem,
            storage_backend=storage_backend,
            connection_instance=connection_instance,
            statement_cache_size=statement_cache_size,
            enable_sqlcommenter=enable_sqlcommenter,
            broker=broker,
            share_pool=share_pool,
            connection_config=connection_config,
            driver_features=driver_features,
            reset_session_on_release=reset_session_on_release,
            kwargs=kwargs,
        )
        self._spanner_dialect: Literal["googlesql", "spangres"] = normalize_spanner_dialect(resolved_cfg.dialect)
        self._dialect_value: str = sqlspec_dialect_name(self._spanner_dialect)
        self._resolved_config: SpannerConfig = resolved_cfg
        self.storage_backend: ObjectStoreBackend | str | None = merged_features.get("storage_backend", storage_backend)
        self.statement_cache_size: int = int(merged_features.get("statement_cache_size", statement_cache_size))
        self.enable_sqlcommenter: bool = bool(merged_features.get("enable_sqlcommenter", enable_sqlcommenter))
        self.tenant_predicate: Any = tenant_predicate
        self.tenant_id: Any = tenant_id
        self._cached_core: AsyncSpannerDriver | None = None
        self._cached_sync_core: SyncSpannerDriver | None = None
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = user_hook
        effective_stmt_cfg = statement_config or default_statement_config(self._spanner_dialect)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=effective_stmt_cfg,
            connection_config=conn_meta,
            connection_instance=resolved_instance,
            migration_config=migration_config,
            driver_features=merged_features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the underlying Vertebra Spanner dialect literal (``googlesql`` or ``spangres``)."""
        return self._spanner_dialect

    def resolve_spanner_config(self) -> SpannerConfig:
        """Return the canonical ``SpannerConfig`` for this SQLSpec configuration."""
        return self._resolved_config

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve the configured ``ObjectStoreBackend`` instance, if any."""
        return resolve_storage_backend(self.storage_backend)

    def _adapt_async_connection(self, conn: Any) -> Any:
        if isinstance(conn, SpannerConnectionShim):
            return _SyncSpannerAsyncProxy(conn.driver)
        if isinstance(conn, SyncSpannerDriver):
            return _SyncSpannerAsyncProxy(conn)
        return conn

    def _resolve_core_driver(self) -> Any:
        if self.connection_instance is not None:
            return self._adapt_async_connection(self.connection_instance)
        if self._cached_core is None:
            self._cached_core = AsyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_core

    def _resolve_sync_core_driver(self) -> Any:
        if isinstance(self.connection_instance, SyncSpannerDriver):
            return self.connection_instance
        if isinstance(self.connection_instance, SpannerConnectionShim):
            return self.connection_instance.driver
        if self._cached_sync_core is None:
            self._cached_sync_core = SyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_sync_core

    def create_driver(self) -> SpannerAsyncDriver:
        """Create a standalone ``SpannerAsyncDriver`` over the resolved core driver."""
        return SpannerAsyncDriver(
            self._resolve_core_driver(),
            statement_config=self.statement_config,
            driver_features=dict(self.driver_features),
            observability=self.observability_config,
            shared_pool=bool(self.share_pool),
            tenant_predicate=self.tenant_predicate,
            tenant_id=self.tenant_id,
            storage_backend=self.resolve_storage_backend(),
        )

    def provide_driver(
        self,
        broker: Any = None,
        statement_cache: Any = None,
    ) -> SpannerAsyncDriver:
        """Return a configured ``SpannerAsyncDriver`` instance."""
        _ = broker, statement_cache
        return self.create_driver()

    def create_connection(self) -> Any:
        """Return a ``SpannerConnectionShim`` for synchronous SQLSpec tooling."""
        core = self._resolve_sync_core_driver()
        return SpannerConnectionShim(core) if isinstance(core, SyncSpannerDriver) else core

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[SpannerAsyncDriver]:
        """Yield an isolated ``SpannerAsyncDriver`` for autocommit, ReadOnly snapshot, or ReadWrite transaction."""
        _reject_tenant_settings(kwargs.get("tenant_predicate"), kwargs.get("tenant_id"), None)
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            transaction=transaction,
            staleness=staleness,
            read_only=read_only,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractAsyncContextManager[Any]:
        """Yield an asynchronous native Spanner driver context forwarding positional and keyword options."""
        return super().provide_connection(*args, **kwargs)

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build Spanner replica overrides from DSN URIs, database IDs, or explicit dictionaries."""
        return _build_spanner_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous Spanner configuration with non-mutating replica or feature overrides."""
        return cast("Self", _derive_spanner_adapter(self, overrides))

    async def _close_pool(self) -> None:
        """Close the asynchronous Spanner pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        await super()._close_pool()

    async def close(self) -> None:
        """Close cached asynchronous and synchronous Spanner core drivers and adapter pool."""
        if self._cached_core is not None:
            close_fn = getattr(self._cached_core, "close", None)
            if callable(close_fn):
                res = close_fn()
                if inspect.isawaitable(res):
                    await res
            self._cached_core = None
        if self._cached_sync_core is not None:
            sync_close_fn = getattr(self._cached_sync_core, "close", None)
            if callable(sync_close_fn):
                await asyncio.to_thread(sync_close_fn)
            self._cached_sync_core = None
        await super().close()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return Spanner adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "AsyncSpannerDriver": AsyncSpannerDriver,
                "SpannerAsyncConnectionContext": SpannerAsyncConnectionContext,
                "SpannerAsyncDriver": SpannerAsyncDriver,
                "SpannerAsyncSessionContext": SpannerAsyncSessionContext,
                "SpannerConfig": SpannerConfig,
                "SpannerConnectionParams": SpannerConnectionParams,
                "SpannerConnectionShim": SpannerConnectionShim,
                "SpannerDriverFeatures": SpannerDriverFeatures,
                "SpannerPoolParams": SpannerPoolParams,
            }
        )
        return namespace


class SpangresSyncConfig(SpannerSyncConfig):
    """Synchronous SQLSpec adapter configuration defaulting to Spanner PostgreSQL (``spangres``)."""

    __slots__ = ()

    dialect: ClassVar[str] = cast("str", _ConfigDialectDescriptor("spangres"))

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("dialect", "spangres")
        super().__init__(*args, **kwargs)
        self._dialect_value = "spangres"


class SpangresAsyncConfig(SpannerAsyncConfig):
    """Asynchronous SQLSpec adapter configuration defaulting to Spanner PostgreSQL (``spangres``)."""

    __slots__ = ()

    dialect: ClassVar[str] = cast("str", _ConfigDialectDescriptor("spangres"))

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("dialect", "spangres")
        super().__init__(*args, **kwargs)
        self._dialect_value = "spangres"
