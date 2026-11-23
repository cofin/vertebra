"""Redis 7.2+/8.0+ configuration, cluster redirect directives, and Pub/Sub messages."""

from __future__ import annotations

from typing import ClassVar

from vertebra.db.adapters.redis.core import ClusterRedirect, PubSubMessage
from vertebra.db.driver._common import REDIS_DIALECT_TAG
from vertebra.db.driver._resp import BaseRESPConfig

__all__ = (
    "ClusterRedirect",
    "PubSubMessage",
    "RedisConfig",
)


class RedisConfig(BaseRESPConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for Redis 7.2+ / 8.0+ standalone, Sentinel, and Cluster connections."""

    dialect: ClassVar[str] = "redis"
    dialect_tag: ClassVar[int] = REDIS_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("redis", "rediss")
    driver_module: ClassVar[str] = "vertebra.db.adapters.redis.driver"
    sync_driver_name: ClassVar[str] = "SyncRedisDriver"
    async_driver_name: ClassVar[str] = "AsyncRedisDriver"

    def __repr__(self) -> str:
        """Return a redacted representation that omits passwords and token secrets."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        provider_name = type(provider).__name__ if provider is not None else None
        return (
            f"RedisConfig(url={self._redact_url(self.url)!r}, username={self.username!r}, "
            f"password={'***' if self.password is not None else None!r}, "
            f"protocol={self.protocol!r}, topology={self.topology!r}, "
            f"enable_gcp_iam_auth={self.enable_gcp_iam_auth!r}, "
            f"token_provider={provider_name!r})"
        )

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`rediss://`)."""
        return self.url.startswith("rediss://")
