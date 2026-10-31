"""Vertebra HTTP Server — multi-protocol support.

Sub-packages::

    from vertebra.server.worker import Worker, run_worker
    from vertebra.server._asgi import AsgiRunner
    from vertebra.server._wsgi import WsgiAdapter
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.server.worker import Worker, run_worker

__all__ = (
    "Worker",
    "run_worker",
)

__lazy_modules__ = [
    "vertebra.server.worker",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "Worker": ("vertebra.server.worker", "Worker"),
    "run_worker": ("vertebra.server.worker", "run_worker"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
