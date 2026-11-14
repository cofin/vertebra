"""Adapter-owned PostgreSQL live notification and event driver protocols."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from vertebra._postgres import AsyncPostgresListener, AsyncPostgresReplication
    from vertebra.db.adapters.postgres.change_stream import PostgresChangeConfig
    from vertebra.db.adapters.postgres.config import PostgreSQLConfig
    from vertebra.db.adapters.postgres.types import PostgresReplicationIdentity

__all__ = (
    "EventSessionRegistry",
    "PostgresNotificationDriver",
    "PostgresReplicationDriver",
)


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


@runtime_checkable
class PostgresReplicationDriver(Protocol):
    """Structural protocol for PostgreSQL drivers supporting logical replication CDC streams."""

    @property
    def config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQL driver configuration."""
        ...

    async def _replication_identity(
        self,
        slot: str,
        publication: str,
    ) -> PostgresReplicationIdentity:
        """Fetch server system identity and replication slot/publication metadata."""
        ...

    async def _open_replication_stream(
        self,
        config: PostgresChangeConfig,
        *,
        start_lsn: int | None = None,
        reconnect: bool = False,
    ) -> AsyncPostgresReplication:
        """Open a dedicated non-pooled logical replication stream."""
        ...


class EventSessionRegistry:
    """Registry pairing live notification/CDC handles with dedicated native clients.

    Tracks open ``(handle, client)`` pairs, prunes closed handles automatically,
    and performs bounded deterministic cleanup in ``close_all()`` without masking
    any primary exception raised by earlier close operations.
    """

    def __init__(self) -> None:
        """Initialize an empty session registry."""
        self._entries: list[tuple[Any, Any]] = []
        self._lock = asyncio.Lock()

    def prune_closed(self) -> int:
        """Remove entries whose handles are already closed and return removed count."""
        before = len(self._entries)
        self._entries = [(handle, client) for handle, client in self._entries if not self._is_closed(handle)]
        return before - len(self._entries)

    def register(self, handle: Any, client: Any) -> None:
        """Register an active ``(handle, client)`` pair after pruning closed entries."""
        self.prune_closed()
        for existing_handle, _ in self._entries:
            if existing_handle is handle:
                return
        self._entries.append((handle, client))

    def unregister(self, handle: Any) -> None:
        """Remove the entry associated with ``handle`` if present."""
        self._entries = [
            (existing_handle, client) for existing_handle, client in self._entries if existing_handle is not handle
        ]

    def __len__(self) -> int:
        """Return the count of still-open registered pairs."""
        self.prune_closed()
        return len(self._entries)

    async def close_all(self) -> None:
        """Close all registered ``(handle, client)`` pairs deterministically.

        Always attempts to close every registered pair even if closing an earlier
        pair raises, re-raising only the first non-cancelled failure (or cancellation)
        so primary exceptions are never masked by cleanup failures.
        """
        async with self._lock:
            entries = list(self._entries)
            self._entries.clear()
        first_error: BaseException | None = None
        for handle, client in reversed(entries):
            try:
                await _close_entry(handle, client)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def _is_closed(self, handle: Any) -> bool:
        """Return whether a tracked handle reports itself as closed."""
        closed_attr = getattr(handle, "closed", None)
        if callable(closed_attr):
            return bool(closed_attr())
        if isinstance(closed_attr, bool):
            return closed_attr
        return False


async def _close_awaitable(fn: Callable[[], Any]) -> None:
    """Invoke a sync or async close callable and await if needed."""
    result = fn()
    if inspect.isawaitable(result):
        await result


async def _close_entry(handle: Any, client: Any) -> None:
    """Close a tracked handle and its owning client cleanly."""
    handle_error: BaseException | None = None
    try:
        aclose = getattr(handle, "aclose", None)
        if callable(aclose):
            await _close_awaitable(aclose)
        else:
            close = getattr(handle, "close", None)
            if callable(close):
                await _close_awaitable(close)
    except BaseException as exc:
        handle_error = exc
    finally:
        client_error: BaseException | None = None
        try:
            client_close = getattr(client, "close", None)
            if callable(client_close):
                await _close_awaitable(client_close)
            else:
                client_aclose = getattr(client, "aclose", None)
                if callable(client_aclose):
                    await _close_awaitable(client_aclose)
        except BaseException as exc:
            client_error = exc
        if handle_error is not None:
            raise handle_error
        if client_error is not None:
            raise client_error
