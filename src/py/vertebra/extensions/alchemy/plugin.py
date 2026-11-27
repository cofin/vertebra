"""Advanced Alchemy integration factories, configuration helpers, and broker wiring."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol, cast, overload
from urllib.parse import unquote

from advanced_alchemy.config import EngineConfig as BaseEngineConfig

from vertebra.cache import SyncCacheStore
from vertebra.db.registry import SCHEME_REGISTRY
from vertebra.extensions.alchemy.cache import (
    CacheConfig,
    CacheManager,
    SyncCacheRegion,
    create_cache_config,
    msgspec_model_deserializer,
    msgspec_model_serializer,
)
from vertebra.extensions.alchemy.storage import ObjectStoreBackend, register_storage_backend
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, SyncBrokerPool
from vertebra.storage import AsyncObjectStore, SyncObjectStore
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional

try:
    from advanced_alchemy.extensions.litestar import (
        EngineConfig,
        SQLAlchemyAsyncConfig,
        SQLAlchemyPlugin,
        SQLAlchemySyncConfig,
    )
    from advanced_alchemy.types.file_object import storages
    from advanced_alchemy.utils.dataclass import Empty
except ImportError as exc:
    raise MissingDependencyError(
        "advanced-alchemy",
        install_package="advanced-alchemy",
        extra="alchemy",
    ) from exc

if TYPE_CHECKING:
    from collections.abc import Callable

    from advanced_alchemy.config import RoutingConfig
    from advanced_alchemy.extensions.fastapi import (
        AdvancedAlchemy as FastAPIAdvancedAlchemy,
    )
    from advanced_alchemy.extensions.fastapi import (
        SQLAlchemyAsyncConfig as FastAPISQLAlchemyAsyncConfig,
    )
    from advanced_alchemy.extensions.fastapi import (
        SQLAlchemySyncConfig as FastAPISQLAlchemySyncConfig,
    )
    from advanced_alchemy.extensions.flask import (
        AdvancedAlchemy as FlaskAdvancedAlchemy,
    )
    from advanced_alchemy.extensions.flask import (
        SQLAlchemyAsyncConfig as FlaskSQLAlchemyAsyncConfig,
    )
    from advanced_alchemy.extensions.flask import (
        SQLAlchemySyncConfig as FlaskSQLAlchemySyncConfig,
    )
    from advanced_alchemy.extensions.starlette import (
        AdvancedAlchemy as StarletteAdvancedAlchemy,
    )
    from advanced_alchemy.extensions.starlette import (
        SQLAlchemyAsyncConfig as StarletteSQLAlchemyAsyncConfig,
    )
    from advanced_alchemy.extensions.starlette import (
        SQLAlchemySyncConfig as StarletteSQLAlchemySyncConfig,
    )
    from fastapi import FastAPI
    from flask import Flask
    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine
    from starlette.applications import Starlette

    from vertebra.extensions.alchemy.broker import (
        AsyncSHMSessionBroker,
        SyncSHMSessionBroker,
    )
    from vertebra.extensions.broker.transport import (
        AsyncBrokerLeaseSource,
        AsyncSHMBrokerTransport,
        SyncBrokerLeaseSource,
        SyncSHMBrokerTransport,
    )

    class _FrameworkExtensionModule(Protocol):
        EngineConfig: type[BaseEngineConfig]
        SQLAlchemyAsyncConfig: type[AsyncAlchemyConfig]
        SQLAlchemySyncConfig: type[SyncAlchemyConfig]
        AdvancedAlchemy: Callable[..., object]


type FrameworkHost = Literal["litestar", "fastapi", "starlette", "flask"]
type AsyncAlchemyConfig = (
    SQLAlchemyAsyncConfig | FastAPISQLAlchemyAsyncConfig | StarletteSQLAlchemyAsyncConfig | FlaskSQLAlchemyAsyncConfig
)
type SyncAlchemyConfig = (
    SQLAlchemySyncConfig | FastAPISQLAlchemySyncConfig | StarletteSQLAlchemySyncConfig | FlaskSQLAlchemySyncConfig
)
type BrokerTransportInput = (
    SyncSHMSessionBroker
    | AsyncSHMSessionBroker
    | SyncBrokerLeaseSource
    | AsyncBrokerLeaseSource
    | SyncSHMBrokerTransport
    | AsyncSHMBrokerTransport
)

_SUPPORTED_FRAMEWORKS: tuple[FrameworkHost, ...] = ("litestar", "fastapi", "starlette", "flask")

__all__ = (
    "create_alchemy_plugin",
    "create_async_alchemy_config",
    "create_fastapi_alchemy",
    "create_flask_alchemy",
    "create_starlette_alchemy",
    "create_sync_alchemy_config",
    "normalize_alchemy_url",
)


def normalize_alchemy_url(connection_string: str, *, is_async: bool = True) -> str:
    """Normalize shorthand URLs across registered database configs into canonical `+vtb` / `+vtb_async` dialect URLs."""
    if "://" not in connection_string:
        return connection_string
    scheme, rest = connection_string.split("://", 1)
    if "+" in scheme:
        return connection_string
    lowered = scheme.lower()
    driver_suffix = "+vtb_async" if is_async else "+vtb"
    cfg_cls = SCHEME_REGISTRY.get(lowered)
    if cfg_cls is not None:
        target_engine = "spangres" if lowered == "spangres" else cfg_cls.url_schemes[0]
        if target_engine in (
            "postgresql",
            "mysql",
            "sqlite",
            "oracle",
            "mssql",
            "duckdb",
            "bigquery",
            "spanner",
            "spangres",
        ):
            return f"{target_engine}{driver_suffix}://{rest}"
    return connection_string


def _import_framework_extension(framework: Literal["fastapi", "starlette", "flask"]) -> _FrameworkExtensionModule:
    """Import the Advanced Alchemy extension module for an optional web framework host."""
    if import_optional(framework) is None:
        raise MissingDependencyError(framework, install_package=framework, extra=framework)
    mod = import_optional(f"advanced_alchemy.extensions.{framework}")
    if mod is None:
        raise MissingDependencyError(framework, install_package=framework, extra=framework)
    return cast("_FrameworkExtensionModule", mod)


def _resolve_framework_types(
    framework: FrameworkHost,
    *,
    is_async: bool,
) -> tuple[type[AsyncAlchemyConfig] | type[SyncAlchemyConfig], type[BaseEngineConfig]]:
    """Resolve the framework-specific SQLAlchemy config and EngineConfig classes."""
    if framework not in _SUPPORTED_FRAMEWORKS:
        msg = f"Unsupported framework host: {framework!r}. Expected one of {_SUPPORTED_FRAMEWORKS}"
        raise ValueError(msg)
    if framework == "litestar":
        cfg_cls: type[AsyncAlchemyConfig] | type[SyncAlchemyConfig] = (
            SQLAlchemyAsyncConfig if is_async else SQLAlchemySyncConfig
        )
        return cfg_cls, EngineConfig
    mod = _import_framework_extension(framework)
    resolved_cfg: type[AsyncAlchemyConfig] | type[SyncAlchemyConfig] = (
        mod.SQLAlchemyAsyncConfig if is_async else mod.SQLAlchemySyncConfig
    )
    return resolved_cfg, mod.EngineConfig


def _resolve_probe_url(
    connection_string: str | None,
    routing_config: RoutingConfig | None,
    engine_instance: Engine | AsyncEngine | None,
) -> str:
    """Resolve a representative URL string for memory and dialect detection."""
    if connection_string is not None:
        return connection_string
    if routing_config is not None:
        if routing_config.primary_connection_string:
            return routing_config.primary_connection_string
        default_configs = routing_config.get_engine_configs(routing_config.default_group)
        if default_configs:
            return default_configs[0].connection_string or ""
        return ""
    if engine_instance is not None:
        return str(engine_instance.url)
    return ""


def _build_alchemy_config[ConfigT](
    *,
    fn_name: str,
    is_async: bool,
    connection_string: str | None,
    framework: FrameworkHost,
    routing_config: RoutingConfig | None,
    engine_instance: Engine | AsyncEngine | None,
    broker: BrokerTransportInput | None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None,
    storage_key: str,
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None,
    config_cls: type[ConfigT] | None,
    kwargs: dict[str, object],
) -> ConfigT | AsyncAlchemyConfig | SyncAlchemyConfig:
    """Validate source cardinality, resolve host config types, and construct an Alchemy config."""
    normalized_conn = connection_string if connection_string is not None and connection_string.strip() else None
    source_count = sum(source is not None for source in (normalized_conn, routing_config, engine_instance))
    if source_count == 0:
        msg = f"{fn_name} requires one of connection_string, routing_config, or engine_instance"
        raise ValueError(msg)
    if source_count > 1:
        msg = f"{fn_name} accepts only one of connection_string, routing_config, or engine_instance"
        raise ValueError(msg)

    resolved_host_cfg_cls, engine_config_cls = _resolve_framework_types(framework, is_async=is_async)
    target_cls: Callable[..., ConfigT | AsyncAlchemyConfig | SyncAlchemyConfig] = (
        cast("Callable[..., ConfigT]", config_cls)
        if config_cls is not None
        else cast("Callable[..., AsyncAlchemyConfig | SyncAlchemyConfig]", resolved_host_cfg_cls)
    )

    resolved_url = normalize_alchemy_url(normalized_conn, is_async=is_async) if normalized_conn is not None else None
    _register_storage_if_provided(storage_backend, storage_key)

    probe_url = _resolve_probe_url(resolved_url, routing_config, engine_instance)
    _prepare_engine_config(
        probe_url,
        broker,
        kwargs,
        engine_config_cls=engine_config_cls,
        is_async=is_async,
        has_engine_instance=engine_instance is not None,
    )
    _prepare_cache_config(cache_config, kwargs)

    if resolved_url is not None:
        kwargs["connection_string"] = resolved_url
    if routing_config is not None:
        kwargs["routing_config"] = routing_config
    if engine_instance is not None:
        kwargs["engine_instance"] = engine_instance

    return target_cls(**kwargs)


@overload
def create_async_alchemy_config[ConfigT](
    connection_string: str | None = None,
    *,
    framework: FrameworkHost = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: AsyncEngine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: type[ConfigT],
    **kwargs: object,
) -> ConfigT: ...


@overload
def create_async_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["litestar"] = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: AsyncEngine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> SQLAlchemyAsyncConfig: ...


@overload
def create_async_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["fastapi"],
    routing_config: RoutingConfig | None = None,
    engine_instance: AsyncEngine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> FastAPISQLAlchemyAsyncConfig: ...


@overload
def create_async_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["starlette"],
    routing_config: RoutingConfig | None = None,
    engine_instance: AsyncEngine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> StarletteSQLAlchemyAsyncConfig: ...


@overload
def create_async_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["flask"],
    routing_config: RoutingConfig | None = None,
    engine_instance: AsyncEngine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> FlaskSQLAlchemyAsyncConfig: ...


@overload
def create_async_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: FrameworkHost = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: AsyncEngine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> AsyncAlchemyConfig: ...


def create_async_alchemy_config[ConfigT](
    connection_string: str | None = None,
    *,
    framework: FrameworkHost = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: AsyncEngine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: type[ConfigT] | None = None,
    **kwargs: object,
) -> ConfigT | AsyncAlchemyConfig:
    """Create an Advanced Alchemy async config with optional broker, storage, and cache."""
    return cast(
        "ConfigT | AsyncAlchemyConfig",
        _build_alchemy_config(
            fn_name="create_async_alchemy_config",
            is_async=True,
            connection_string=connection_string,
            framework=framework,
            routing_config=routing_config,
            engine_instance=engine_instance,
            broker=broker,
            storage_backend=storage_backend,
            storage_key=storage_key,
            cache_config=cache_config,
            config_cls=config_cls,
            kwargs=dict(kwargs),
        ),
    )


@overload
def create_sync_alchemy_config[ConfigT](
    connection_string: str | None = None,
    *,
    framework: FrameworkHost = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: Engine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: type[ConfigT],
    **kwargs: object,
) -> ConfigT: ...


@overload
def create_sync_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["litestar"] = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: Engine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> SQLAlchemySyncConfig: ...


@overload
def create_sync_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["fastapi"],
    routing_config: RoutingConfig | None = None,
    engine_instance: Engine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> FastAPISQLAlchemySyncConfig: ...


@overload
def create_sync_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["starlette"],
    routing_config: RoutingConfig | None = None,
    engine_instance: Engine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> StarletteSQLAlchemySyncConfig: ...


@overload
def create_sync_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: Literal["flask"],
    routing_config: RoutingConfig | None = None,
    engine_instance: Engine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> FlaskSQLAlchemySyncConfig: ...


@overload
def create_sync_alchemy_config(
    connection_string: str | None = None,
    *,
    framework: FrameworkHost = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: Engine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: None = None,
    **kwargs: object,
) -> SyncAlchemyConfig: ...


def create_sync_alchemy_config[ConfigT](
    connection_string: str | None = None,
    *,
    framework: FrameworkHost = "litestar",
    routing_config: RoutingConfig | None = None,
    engine_instance: Engine | None = None,
    broker: BrokerTransportInput | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None = None,
    config_cls: type[ConfigT] | None = None,
    **kwargs: object,
) -> ConfigT | SyncAlchemyConfig:
    """Create an Advanced Alchemy sync config with optional broker, storage, and cache."""
    return cast(
        "ConfigT | SyncAlchemyConfig",
        _build_alchemy_config(
            fn_name="create_sync_alchemy_config",
            is_async=False,
            connection_string=connection_string,
            framework=framework,
            routing_config=routing_config,
            engine_instance=engine_instance,
            broker=broker,
            storage_backend=storage_backend,
            storage_key=storage_key,
            cache_config=cache_config,
            config_cls=config_cls,
            kwargs=dict(kwargs),
        ),
    )


def create_alchemy_plugin(
    *configs: SQLAlchemyAsyncConfig | SQLAlchemySyncConfig,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    **kwargs: object,
) -> SQLAlchemyPlugin:
    """Create an Advanced Alchemy Litestar SQLAlchemyPlugin from one or more engine configurations."""
    if not configs:
        msg = "create_alchemy_plugin requires at least one configuration instance"
        raise ValueError(msg)
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg: SQLAlchemyAsyncConfig | SQLAlchemySyncConfig | list[SQLAlchemyAsyncConfig | SQLAlchemySyncConfig] = (
        configs[0] if len(configs) == 1 else list(configs)
    )
    return cast("Callable[..., SQLAlchemyPlugin]", SQLAlchemyPlugin)(config=cfg_arg, **kwargs)


def create_fastapi_alchemy(
    *configs: FastAPISQLAlchemyAsyncConfig | FastAPISQLAlchemySyncConfig,
    app: FastAPI | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
) -> FastAPIAdvancedAlchemy:
    """Create an Advanced Alchemy FastAPI extension instance with lazy framework import."""
    if not configs:
        msg = "create_fastapi_alchemy requires at least one configuration instance"
        raise ValueError(msg)
    mod = _import_framework_extension("fastapi")
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg = configs[0] if len(configs) == 1 else list(configs)
    return cast("FastAPIAdvancedAlchemy", mod.AdvancedAlchemy(config=cfg_arg, app=app))


def create_starlette_alchemy(
    *configs: StarletteSQLAlchemyAsyncConfig | StarletteSQLAlchemySyncConfig,
    app: Starlette | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
) -> StarletteAdvancedAlchemy:
    """Create an Advanced Alchemy Starlette extension instance with lazy framework import."""
    if not configs:
        msg = "create_starlette_alchemy requires at least one configuration instance"
        raise ValueError(msg)
    mod = _import_framework_extension("starlette")
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg = configs[0] if len(configs) == 1 else list(configs)
    return cast("StarletteAdvancedAlchemy", mod.AdvancedAlchemy(config=cfg_arg, app=app))


def create_flask_alchemy(
    *configs: FlaskSQLAlchemyAsyncConfig | FlaskSQLAlchemySyncConfig,
    app: Flask | None = None,
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None = None,
    storage_key: str = "default",
    **kwargs: object,
) -> FlaskAdvancedAlchemy:
    """Create an Advanced Alchemy Flask extension instance with lazy framework import."""
    if not configs:
        msg = "create_flask_alchemy requires at least one configuration instance"
        raise ValueError(msg)
    mod = _import_framework_extension("flask")
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg = configs[0] if len(configs) == 1 else list(configs)
    return cast("FlaskAdvancedAlchemy", mod.AdvancedAlchemy(config=cfg_arg, app=app, **kwargs))


def _prepare_engine_config(
    probe_url: str,
    broker: BrokerTransportInput | None,
    kwargs: dict[str, object],
    engine_config_cls: type[BaseEngineConfig] = EngineConfig,
    *,
    is_async: bool = False,
    has_engine_instance: bool = False,
) -> None:
    """Configure EngineConfig defaults, broker leasing connection args, and SyncBrokerPool/AsyncBrokerPool."""
    existing = kwargs.get("engine_config")
    if has_engine_instance:
        if isinstance(existing, BaseEngineConfig):
            kwargs["engine_config"] = existing
        elif isinstance(existing, dict):
            kwargs["engine_config"] = cast("Callable[..., BaseEngineConfig]", engine_config_cls)(**existing)
        return

    decoded_conn = unquote(probe_url)
    is_memory = ":memory:" in decoded_conn or "mode=memory" in decoded_conn or "memory://" in decoded_conn
    if isinstance(existing, BaseEngineConfig):
        cfg = existing
    elif isinstance(existing, dict):
        cfg_dict: dict[str, object] = dict(existing)
        if not is_memory:
            cfg_dict.setdefault("pool_pre_ping", True)
            cfg_dict.setdefault("pool_recycle", 1800)
        cfg = cast("Callable[..., BaseEngineConfig]", engine_config_cls)(**cfg_dict)
    else:
        cfg = (
            cast("Callable[..., BaseEngineConfig]", engine_config_cls)(pool_pre_ping=True, pool_recycle=1800)
            if not is_memory
            else engine_config_cls()
        )

    if not is_memory:
        if cfg.pool_pre_ping is Empty:
            cfg.pool_pre_ping = True
        if cfg.pool_recycle is Empty:
            cfg.pool_recycle = 1800

    scheme = probe_url.split("://", 1)[0].lower() if "://" in probe_url else ""
    is_vtb_dialect = any(marker in scheme for marker in ("+vtb", "+vertebra"))
    if broker is not None:
        pool_cls = AsyncBrokerPool if is_async else SyncBrokerPool
        if cfg.poolclass is Empty:
            cfg.poolclass = pool_cls.bind_class(broker=broker, url=probe_url)
        if is_vtb_dialect:
            existing_args = cfg.connect_args if isinstance(cfg.connect_args, dict) else {}
            connect_args: dict[str, object] = dict(existing_args)
            if probe_url:
                connect_args.setdefault("url", probe_url)
            if "broker" not in connect_args:
                connect_args["broker"] = broker
                connect_args.setdefault("lease_managed", True)
            cfg.connect_args = connect_args

    kwargs["engine_config"] = cfg


def _prepare_cache_config(
    cache_config: CacheConfig | SyncCacheRegion | SyncCacheStore | str | None,
    kwargs: dict[str, object],
) -> None:
    """Normalize `cache_config` into a `CacheConfig` and attach upstream `CacheManager`."""
    if cache_config is None:
        return
    if isinstance(cache_config, SyncCacheStore):
        region = SyncCacheRegion(cache_config)

        def _store_region_factory(_cfg: CacheConfig) -> SyncCacheRegion:
            return region

        resolved_cfg = CacheConfig(
            serializer=msgspec_model_serializer,
            deserializer=msgspec_model_deserializer,
            region_factory=_store_region_factory,
        )
    elif isinstance(cache_config, SyncCacheRegion):
        target_region = cache_config

        def _region_factory(_cfg: CacheConfig) -> SyncCacheRegion:
            return target_region

        resolved_cfg = CacheConfig(
            serializer=msgspec_model_serializer,
            deserializer=msgspec_model_deserializer,
            region_factory=_region_factory,
        )
    elif isinstance(cache_config, str):
        resolved_cfg = create_cache_config(url=cache_config)
    else:
        resolved_cfg = cache_config
    existing_mgr = kwargs.get("cache_manager")
    manager = existing_mgr if isinstance(existing_mgr, CacheManager) else CacheManager(resolved_cfg)
    kwargs["cache_config"] = resolved_cfg
    kwargs.setdefault("cache_manager", manager)


def _register_storage_if_provided(
    storage_backend: ObjectStoreBackend | SyncObjectStore | AsyncObjectStore | str | None,
    storage_key: str,
) -> None:
    """Register `storage_backend` in Advanced Alchemy's `storages` registry when provided."""
    if storage_backend is None:
        return
    if isinstance(storage_backend, ObjectStoreBackend):
        for existing_key in storages.registered_backends():
            if storages.get_backend(existing_key) is storage_backend and existing_key != storage_key:
                msg = f"StorageBackend is already registered under key {existing_key!r}"
                raise ValueError(msg)
        storage_backend.key = storage_key
        storages.register_backend(storage_backend)
    else:
        register_storage_backend(storage_backend, key=storage_key)
