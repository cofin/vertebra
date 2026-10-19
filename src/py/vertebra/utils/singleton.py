"""Singleton pattern implementation for Vertebra.

Provides a thread-safe metaclass for implementing the singleton pattern.
Aligned with vtb.
"""

import threading
from typing import Any, cast

__all__ = ("SingletonMeta",)


class SingletonMeta(type):
    """Metaclass for singleton pattern."""

    _instances: dict[type, object] = {}
    _lock = threading.Lock()

    def __call__[T](cls: type[T], *args: Any, **kwargs: Any) -> T:
        """Call method for the singleton metaclass."""
        if cls not in SingletonMeta._instances:
            with SingletonMeta._lock:
                if cls not in SingletonMeta._instances:
                    instance = type.__call__(cls, *args, **kwargs)
                    SingletonMeta._instances[cls] = instance
        return cast("T", SingletonMeta._instances[cls])
