"""Adapter-owned PostgreSQL live notification and event driver protocols."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from vertebra._postgres import AsyncPostgresListener
    from vertebra.db.adapters.postgres.config import PostgreSQLConfig

__all__ = ("PostgresNotificationDriver",)


class PostgresNotificationDriver(Protocol):
    """Structural protocol for PostgreSQL drivers supporting notification publish and listen."""

    @property
    def config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQL driver configuration."""
        ...

    async def _open_notification_listener(
        self,
        channels: Sequence[str],
        *,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> AsyncPostgresListener:
        """Open a dedicated non-pooled direct/session PostgreSQL notification listener."""
        ...

    async def _publish_notification(self, channel: str, payload: str) -> None:
        """Publish a parameterized PostgreSQL pg_notify payload on channel."""
        ...
