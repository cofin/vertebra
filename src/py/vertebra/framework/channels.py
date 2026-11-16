"""Framework channel bus and first-party channels backend across 5 notification transports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self, cast

from vertebra.db.driver import (
    AsyncChangeSource,
    AsyncNotificationSource,
    SyncNotificationSource,
)
from vertebra.server._channels import ChannelBus, ChannelEnvelope
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from vertebra.db.cdc import ChangeFeed

__all__ = (
    "ChannelBus",
    "ChannelEnvelope",
    "ChannelsBackend",
    "create_channels_backend",
)


class ChannelsBackend(ChannelBus):
    """Unified first-party channels backend supporting Memory, Redis, Valkey, Postgres, and Oracle."""

    __slots__ = ()

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        history: int = 0,
        node_id: str | None = None,
        maxsize: int = 256,
        change_feed: ChangeFeed | None = None,
        **kwargs: Any,
    ) -> Self:
        """Create a ChannelsBackend instance automatically selected from a backend connection URL."""
        return cast(
            Self,
            create_channels_backend(
                url=url,
                history=history,
                node_id=node_id,
                maxsize=maxsize,
                change_feed=change_feed,
                **kwargs,
            ),
        )


def create_channels_backend(
    backend: str | ChannelBus | None = "memory",
    *,
    url: str | None = None,
    history: int = 0,
    notification_source: AsyncNotificationSource | None = None,
    sync_notification_source: SyncNotificationSource | None = None,
    change_source: AsyncChangeSource | None = None,
    change_feed: ChangeFeed | None = None,
    node_id: str | None = None,
    maxsize: int = 256,
    **kwargs: Any,
) -> ChannelsBackend:
    """Instantiate a ChannelsBackend wired to the requested transport capability."""
    if isinstance(backend, ChannelsBackend):
        return backend
    if isinstance(backend, ChannelBus):
        return ChannelsBackend(
            backend=backend,
            history=history,
            notification_source=notification_source,
            sync_notification_source=sync_notification_source,
            change_source=change_source,
            change_feed=change_feed,
            node_id=node_id,
            maxsize=maxsize,
        )

    resolved_backend = (backend or "memory").lower()
    if url is not None:
        lowered_url = url.lower()
        if lowered_url.startswith(("redis://", "rediss://")):
            resolved_backend = "redis"
        elif lowered_url.startswith(("valkey://", "valkeys://")):
            resolved_backend = "valkey"
        elif lowered_url.startswith(("postgres://", "postgresql://")):
            resolved_backend = "postgres"
        elif lowered_url.startswith("oracle://"):
            resolved_backend = "oracle"
        elif lowered_url.startswith("memory://"):
            resolved_backend = "memory"

    async_source = notification_source
    sync_source = sync_notification_source

    if async_source is None and sync_source is None:
        if resolved_backend == "redis":
            redis_mod = import_optional("vertebra.db.adapters.redis")
            if redis_mod is not None:
                config_cls = redis_mod.RedisConfig
                async_driver_cls = redis_mod.AsyncRedisDriver
                sync_driver_cls = redis_mod.SyncRedisDriver
                async_source_cls = redis_mod.AsyncRedisNotificationSource
                sync_source_cls = redis_mod.SyncRedisNotificationSource
                cfg = config_cls(url=url, **kwargs) if url else config_cls(**kwargs)
                async_source = async_source_cls(async_driver_cls(cfg))
                sync_source = sync_source_cls(sync_driver_cls(cfg))
        elif resolved_backend == "valkey":
            valkey_mod = import_optional("vertebra.db.adapters.valkey")
            if valkey_mod is not None:
                config_cls = valkey_mod.ValkeyConfig
                async_driver_cls = valkey_mod.AsyncValkeyDriver
                sync_driver_cls = valkey_mod.SyncValkeyDriver
                async_source_cls = valkey_mod.AsyncValkeyNotificationSource
                sync_source_cls = valkey_mod.SyncValkeyNotificationSource
                cfg = config_cls(url=url, **kwargs) if url else config_cls(**kwargs)
                async_source = async_source_cls(async_driver_cls(cfg))
                sync_source = sync_source_cls(sync_driver_cls(cfg))
        elif resolved_backend in ("postgres", "postgresql"):
            pg_mod = import_optional("vertebra.db.adapters.postgres")
            if pg_mod is not None:
                config_cls = pg_mod.PostgreSQLConfig
                async_driver_cls = pg_mod.AsyncPostgreSQLDriver
                sync_driver_cls = pg_mod.SyncPostgreSQLDriver
                async_source_cls = pg_mod.AsyncPostgresNotificationSource
                sync_source_cls = pg_mod.SyncPostgresNotificationSource
                cfg = config_cls(dsn=url, **kwargs) if url else config_cls(**kwargs)
                async_source = async_source_cls(async_driver_cls(cfg))
                sync_source = sync_source_cls(sync_driver_cls(cfg))
        elif resolved_backend == "oracle":
            oracle_mod = import_optional("vertebra.db.adapters.oracle")
            if oracle_mod is not None:
                config_cls = oracle_mod.OracleConfig
                async_driver_cls = oracle_mod.AsyncOracleDriver
                sync_driver_cls = oracle_mod.SyncOracleDriver
                async_source_cls = oracle_mod.AsyncOracleNotificationSource
                sync_source_cls = oracle_mod.SyncOracleNotificationSource
                cfg = config_cls(dsn=url, **kwargs) if url else config_cls(**kwargs)
                async_source = async_source_cls(async_driver_cls(cfg))
                sync_source = sync_source_cls(sync_driver_cls(cfg))

    return ChannelsBackend(
        backend=resolved_backend,
        history=history,
        notification_source=async_source,
        sync_notification_source=sync_source,
        change_source=change_source,
        change_feed=change_feed,
        node_id=node_id,
        maxsize=maxsize,
    )
