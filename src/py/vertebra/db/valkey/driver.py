"""Synchronous and asynchronous Valkey 8.0+/9.0+ RESP3 drivers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.redis.driver import AsyncRedisDriver, SyncRedisDriver
from vertebra.db.valkey.config import ValkeyConfig

if TYPE_CHECKING:
    import socket
    from collections.abc import Callable

__all__ = (
    "AsyncValkeyDriver",
    "SyncValkeyDriver",
)


class SyncValkeyDriver(SyncRedisDriver):
    """Synchronous Valkey 8.0+/9.0+ driver with RESP3 client-side cache tracking and topology routing."""

    dialect: ClassVar[str] = "valkey"
    dialect_tag: ClassVar[int] = 8

    __slots__ = ("_valkey_config",)

    def __init__(
        self,
        config: ValkeyConfig | None = None,
        *,
        broker: Any = None,
        socket_factory: Callable[[tuple[str, int]], socket.socket] | None = None,
    ) -> None:
        self._valkey_config = config or ValkeyConfig()
        super().__init__(self._valkey_config, broker=broker, socket_factory=socket_factory)

    @property
    def _config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._valkey_config


class AsyncValkeyDriver(AsyncRedisDriver):
    """Asynchronous Valkey 8.0+/9.0+ RESP3 driver with client-side cache tracking and topology routing."""

    dialect: ClassVar[str] = "valkey"
    dialect_tag: ClassVar[int] = 8

    __slots__ = ("_valkey_config",)

    def __init__(self, config: ValkeyConfig | None = None, *, broker: Any = None) -> None:
        self._valkey_config = config or ValkeyConfig()
        super().__init__(self._valkey_config, broker=broker)

    @property
    def _config(self) -> ValkeyConfig:
        """Return the active ValkeyConfig."""
        return self._valkey_config
