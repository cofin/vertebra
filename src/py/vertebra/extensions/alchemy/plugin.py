"""Advanced Alchemy integration factories, configuration helpers, and broker wiring."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.pool import NullPool

from vertebra.extensions.alchemy.cache import (
    AsyncCacheManager,
    CacheConfig,
    SQLAlchemyCacheBackend,
    create_cache_config,
    setup_cache_listeners,
)
from vertebra.extensions.alchemy.storage import ObjectStoreBackend, register_storage_backend
from vertebra.storage import ObjectStore
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
    from vertebra.extensions.alchemy.broker import BrokerTransport, SHMSessionBroker

__all__ = (
    "create_alchemy_plugin",
    "create_async_alchemy_config",
    "create_fastapi_alchemy",
    "create_flask_alchemy",
    "create_starlette_alchemy",
    "create_sync_alchemy_config",
)


def _prepare_engine_config(
    connection_string: str,
    broker: SHMSessionBroker | BrokerTransport | None,
    kwargs: dict[str, Any],
    engine_config_cls: type[Any] = EngineConfig,
) -> None:
    """Configure EngineConfig defaults, broker leasing connection args, and NullPool."""
    is_memory = ":memory:" in connection_string or "memory://" in connection_string
    existing = kwargs.get("engine_config")
    if isinstance(existing, engine_config_cls):
        cfg = existing
    elif isinstance(existing, dict):
        cfg_dict = dict(existing)
        if not is_memory:
            cfg_dict.setdefault("pool_pre_ping", True)
            cfg_dict.setdefault("pool_recycle", 1800)
        cfg = engine_config_cls(**cfg_dict)
    else:
        cfg = engine_config_cls(pool_pre_ping=True, pool_recycle=1800) if not is_memory else engine_config_cls()

    if not is_memory:
        if cfg.pool_pre_ping is Empty or cfg.pool_pre_ping is None:
            cfg.pool_pre_ping = True
        if cfg.pool_recycle is Empty or cfg.pool_recycle is None:
            cfg.pool_recycle = 1800

    if broker is not None:
        cfg.poolclass = NullPool
        conn_args = dict(cfg.connect_args) if isinstance(cfg.connect_args, dict) else {}
        conn_args["broker"] = broker
        cfg.connect_args = conn_args

    kwargs["engine_config"] = cfg


def _prepare_cache_config(
    cache_config: CacheConfig | SQLAlchemyCacheBackend | str | None,
    kwargs: dict[str, Any],
) -> None:
    """Normalize `cache_config` into a `CacheConfig` and attach `AsyncCacheManager`."""
    if cache_config is None:
        return
    if isinstance(cache_config, SQLAlchemyCacheBackend):
        resolved_cfg = create_cache_config(backend=cache_config)
        manager = AsyncCacheManager(resolved_cfg, backend=cache_config)
    elif isinstance(cache_config, str):
        resolved_cfg = create_cache_config(url=cache_config)
        manager = AsyncCacheManager(resolved_cfg)
    else:
        resolved_cfg = cache_config
        existing_mgr = kwargs.get("cache_manager")
        manager = existing_mgr if isinstance(existing_mgr, AsyncCacheManager) else AsyncCacheManager(resolved_cfg)
    setup_cache_listeners()
    kwargs["cache_config"] = resolved_cfg
    kwargs.setdefault("cache_manager", manager)


def _register_storage_if_provided(
    storage_backend: ObjectStoreBackend | ObjectStore | str | None,
    storage_key: str,
) -> None:
    """Register `storage_backend` in Advanced Alchemy's `storages` registry when provided."""
    if storage_backend is None:
        return
    if isinstance(storage_backend, ObjectStoreBackend):
        storages.register_backend(storage_backend)
    else:
        register_storage_backend(storage_backend, key=storage_key)


def create_async_alchemy_config(
    connection_string: str,
    *,
    broker: SHMSessionBroker | BrokerTransport | None = None,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SQLAlchemyCacheBackend | str | None = None,
    config_cls: type[Any] = SQLAlchemyAsyncConfig,
    **kwargs: Any,
) -> Any:
    """Create an Advanced Alchemy SQLAlchemyAsyncConfig with optional broker, storage, and cache."""
    _register_storage_if_provided(storage_backend, storage_key)
    _prepare_engine_config(connection_string, broker, kwargs)
    _prepare_cache_config(cache_config, kwargs)
    return config_cls(connection_string=connection_string, **kwargs)


def create_sync_alchemy_config(
    connection_string: str,
    *,
    broker: SHMSessionBroker | BrokerTransport | None = None,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
    cache_config: CacheConfig | SQLAlchemyCacheBackend | str | None = None,
    config_cls: type[Any] = SQLAlchemySyncConfig,
    **kwargs: Any,
) -> Any:
    """Create an Advanced Alchemy SQLAlchemySyncConfig with optional broker, storage, and cache."""
    _register_storage_if_provided(storage_backend, storage_key)
    _prepare_engine_config(connection_string, broker, kwargs)
    _prepare_cache_config(cache_config, kwargs)
    return config_cls(connection_string=connection_string, **kwargs)


def create_alchemy_plugin(
    *configs: SQLAlchemyAsyncConfig | SQLAlchemySyncConfig,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
    **kwargs: Any,
) -> SQLAlchemyPlugin:
    """Create an Advanced Alchemy Litestar SQLAlchemyPlugin from one or more engine configurations."""
    if not configs:
        msg = "create_alchemy_plugin requires at least one configuration instance"
        raise ValueError(msg)
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg: SQLAlchemyAsyncConfig | SQLAlchemySyncConfig | list[SQLAlchemyAsyncConfig | SQLAlchemySyncConfig] = (
        configs[0] if len(configs) == 1 else list(configs)
    )
    return SQLAlchemyPlugin(config=cfg_arg, **kwargs)


def create_fastapi_alchemy(
    *configs: Any,
    app: Any = None,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
) -> Any:
    """Create an Advanced Alchemy FastAPI extension instance with lazy framework import."""
    if not configs:
        msg = "create_fastapi_alchemy requires at least one configuration instance"
        raise ValueError(msg)
    mod = import_optional("advanced_alchemy.extensions.fastapi")
    if mod is None:
        raise MissingDependencyError("fastapi", install_package="fastapi", extra="fastapi")
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg = configs[0] if len(configs) == 1 else list(configs)
    return mod.AdvancedAlchemy(config=cfg_arg, app=app)


def create_starlette_alchemy(
    *configs: Any,
    app: Any = None,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
) -> Any:
    """Create an Advanced Alchemy Starlette extension instance with lazy framework import."""
    if not configs:
        msg = "create_starlette_alchemy requires at least one configuration instance"
        raise ValueError(msg)
    mod = import_optional("advanced_alchemy.extensions.starlette")
    if mod is None:
        raise MissingDependencyError("starlette", install_package="starlette", extra="starlette")
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg = configs[0] if len(configs) == 1 else list(configs)
    return mod.AdvancedAlchemy(config=cfg_arg, app=app)


def create_flask_alchemy(
    *configs: Any,
    app: Any = None,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
    **kwargs: Any,
) -> Any:
    """Create an Advanced Alchemy Flask extension instance with lazy framework import."""
    if not configs:
        msg = "create_flask_alchemy requires at least one configuration instance"
        raise ValueError(msg)
    mod = import_optional("advanced_alchemy.extensions.flask")
    if mod is None:
        raise MissingDependencyError("flask", install_package="flask", extra="flask")
    _register_storage_if_provided(storage_backend, storage_key)
    cfg_arg = configs[0] if len(configs) == 1 else list(configs)
    return mod.AdvancedAlchemy(config=cfg_arg, app=app, **kwargs)
