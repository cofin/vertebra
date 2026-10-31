"""Advanced Alchemy integration factories, configuration helpers, and broker wiring."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.pool import NullPool

from vertebra.extensions.alchemy.storage import ObjectStoreBackend, register_storage_backend
from vertebra.storage import ObjectStore
from vertebra.typing import MissingDependencyError

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
    "create_sync_alchemy_config",
)


def _prepare_engine_config(
    connection_string: str,
    broker: SHMSessionBroker | BrokerTransport | None,
    kwargs: dict[str, Any],
) -> None:
    """Configure EngineConfig defaults, broker leasing connection args, and NullPool."""
    is_memory = ":memory:" in connection_string or "memory://" in connection_string
    existing = kwargs.get("engine_config")
    if isinstance(existing, EngineConfig):
        cfg = existing
    elif isinstance(existing, dict):
        cfg_dict = dict(existing)
        if not is_memory:
            cfg_dict.setdefault("pool_pre_ping", True)
            cfg_dict.setdefault("pool_recycle", 1800)
        cfg = EngineConfig(**cfg_dict)
    else:
        cfg = EngineConfig(pool_pre_ping=True, pool_recycle=1800) if not is_memory else EngineConfig()

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


def create_async_alchemy_config(
    connection_string: str,
    *,
    broker: SHMSessionBroker | BrokerTransport | None = None,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
    **kwargs: Any,
) -> SQLAlchemyAsyncConfig:
    """Create an Advanced Alchemy SQLAlchemyAsyncConfig with optional broker and storage backend."""
    if storage_backend is not None:
        if isinstance(storage_backend, ObjectStoreBackend):
            storages.register_backend(storage_backend)
        else:
            register_storage_backend(storage_backend, key=storage_key)

    _prepare_engine_config(connection_string, broker, kwargs)
    return SQLAlchemyAsyncConfig(connection_string=connection_string, **kwargs)


def create_sync_alchemy_config(
    connection_string: str,
    *,
    broker: SHMSessionBroker | BrokerTransport | None = None,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
    **kwargs: Any,
) -> SQLAlchemySyncConfig:
    """Create an Advanced Alchemy SQLAlchemySyncConfig with optional broker and storage backend."""
    if storage_backend is not None:
        if isinstance(storage_backend, ObjectStoreBackend):
            storages.register_backend(storage_backend)
        else:
            register_storage_backend(storage_backend, key=storage_key)

    _prepare_engine_config(connection_string, broker, kwargs)
    return SQLAlchemySyncConfig(connection_string=connection_string, **kwargs)


def create_alchemy_plugin(
    *configs: SQLAlchemyAsyncConfig | SQLAlchemySyncConfig,
    storage_backend: ObjectStoreBackend | ObjectStore | str | None = None,
    storage_key: str = "default",
    **kwargs: Any,
) -> SQLAlchemyPlugin:
    """Create an Advanced Alchemy SQLAlchemyPlugin from one or more engine configurations."""
    if not configs:
        msg = "create_alchemy_plugin requires at least one configuration instance"
        raise ValueError(msg)
    if storage_backend is not None:
        if isinstance(storage_backend, ObjectStoreBackend):
            storages.register_backend(storage_backend)
        else:
            register_storage_backend(storage_backend, key=storage_key)
    cfg_arg: SQLAlchemyAsyncConfig | SQLAlchemySyncConfig | list[SQLAlchemyAsyncConfig | SQLAlchemySyncConfig] = (
        configs[0] if len(configs) == 1 else list(configs)
    )
    return SQLAlchemyPlugin(config=cfg_arg, **kwargs)
