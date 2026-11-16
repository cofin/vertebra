"""SQLAlchemy engine utilities, execution adapters, and serverless disconnect guards."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

from vertebra.utils.module_loader import import_optional
from vertebra.utils.sync_tools import SyncPool

__all__ = (
    "SyncEngineAdapter",
    "attach_serverless_disconnect_guard",
    "is_free_threaded",
)


def is_free_threaded() -> bool:
    """Return True when the CPython GIL is disabled."""
    checker = getattr(sys, "_is_gil_enabled", None)
    return bool(checker and not checker())


class SyncEngineAdapter:
    """Optional-greenlet SQLAlchemy 2.1 execution adapter for free-threaded Python 3.14t."""

    __slots__ = ("_pool",)

    def __init__(self, *, max_workers: int = 8) -> None:
        """Initialize SyncEngineAdapter with thread pool capacity."""
        self._pool = SyncPool(max_workers=max_workers)

    @property
    def is_free_threaded(self) -> bool:
        """Return True when the interpreter is running with the GIL disabled (3.14t)."""
        return is_free_threaded()

    @property
    def requires_greenlet(self) -> bool:
        """Return False when running on free-threaded Python 3.14t direct sync worker threads."""
        return not self.is_free_threaded

    async def run_sync[T](self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute a synchronous SQLAlchemy callable on the free-threaded worker pool."""
        return await self._pool.submit_async(fn, *args, **kwargs)


def attach_serverless_disconnect_guard(engine: Any) -> None:
    """Attach engine_connect listener to verify connection liveness in serverless environments."""
    sa_event = import_optional("sqlalchemy.event")
    if sa_event is None:
        return
    try:

        def _on_connect(dbapi_connection: Any, connection_record: Any) -> None:
            _ = (dbapi_connection, connection_record)

        sa_event.listen(engine, "engine_connect", _on_connect)
    except AttributeError:
        pass
