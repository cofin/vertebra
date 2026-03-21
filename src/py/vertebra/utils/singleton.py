"""Singleton pattern implementation for Vertebra.

Provides a thread-safe metaclass for implementing the singleton pattern.
Aligned with vtb.
"""

import threading
from typing import Any, TypeVar

__all__ = ("SingletonMeta",)

_T = TypeVar("_T")


class SingletonMeta(type):
    """Metaclass for singleton pattern."""

    _instances: dict[type, object] = {}
    _lock = threading.Lock()

    def __call__(cls: type[_T], *args: Any, **kwargs: Any) -> _T:
        """Call method for the singleton metaclass."""
        if cls not in SingletonMeta._instances:
            with SingletonMeta._lock:
                if cls not in SingletonMeta._instances:
                    instance = super().__call__(*args, **kwargs)  # type: ignore[misc]
                    SingletonMeta._instances[cls] = instance
        return SingletonMeta._instances[cls]  # type: ignore[return-value]
