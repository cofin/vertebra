"""Valkey 8.0+ / 9.0+ RESP3 configuration supporting Cluster, Sentinel, and GCP Memorystore IAM."""

from __future__ import annotations

from typing import Any, ClassVar, Self

import msgspec

from vertebra.db.adapters.valkey.core import ClusterRedirect, PubSubMessage
from vertebra.db.driver._common import VALKEY_DIALECT_TAG
from vertebra.db.driver._resp import BaseRESPConfig, is_resp_auth_rejection

__all__ = (
    "ClusterRedirect",
    "PubSubMessage",
    "ValkeyConfig",
)


class ValkeyConfig(BaseRESPConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for Valkey 8.0+ / 9.0+ standalone, Sentinel, Cluster, and GCP Memorystore."""

    dialect: ClassVar[str] = "valkey"
    dialect_tag: ClassVar[int] = VALKEY_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("valkey", "valkeys")
    driver_module: ClassVar[str] = "vertebra.db.adapters.valkey.driver"
    sync_driver_name: ClassVar[str] = "SyncValkeyDriver"
    async_driver_name: ClassVar[str] = "AsyncValkeyDriver"

    url: str = "valkey://localhost:6379/0"
    client_name: str = "vertebra-valkey"
    enable_client_side_caching: bool = True
    enable_client_tracking: bool = True
    enable_gcp_memorystore_iam: bool = False
    memorystore_instance_id: str | None = None

    def __repr__(self) -> str:
        """Return a redacted representation that omits passwords and token secrets."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        provider_name = type(provider).__name__ if provider is not None else None
        return (
            f"ValkeyConfig(url={self._redact_url(self.url)!r}, username={self.username!r}, "
            f"password={'***' if self.password is not None else None!r}, "
            f"protocol={self.protocol!r}, topology={self.topology!r}, "
            f"enable_gcp_memorystore_iam={self.enable_gcp_memorystore_iam!r}, "
            f"token_provider={provider_name!r})"
        )

    @property
    def uses_tls(self) -> bool:
        """Return True if the URL scheme uses TLS (`valkeys://` or `rediss://`)."""
        return self.url.startswith(("valkeys://", "rediss://"))

    @property
    def effective_tracking_enabled(self) -> bool:
        """Return True when RESP3 client-side tracking/caching is enabled."""
        return bool(self.enable_client_side_caching and self.enable_client_tracking)

    def with_resolved_token(self, token: str | None) -> Self:
        """Return a copy carrying a resolved bearer token as password with GCP IAM flags cleared."""
        return msgspec.structs.replace(
            self,
            password=token if token is not None else self.password,
            token_provider=None,
            iam_token_provider=None,
            enable_gcp_memorystore_iam=False,
            iam_auth_enabled=False,
            enable_gcp_iam_auth=False,
        )

    def to_redis_config(self) -> Any:
        """Convert to equivalent configuration object."""
        return self


def is_valkey_auth_rejection(exc: BaseException) -> bool:
    """Return True when ``exc`` represents a Valkey authentication rejection."""
    return is_resp_auth_rejection(exc)
