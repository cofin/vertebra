"""Vertebra HTTP Server — multi-protocol support.

Sub-packages::

    from vertebra.server._native import run_worker
    from vertebra.server._asgi import AsgiAdapter, AsgiRunner
    from vertebra.server._wsgi import WsgiAdapter, WsgiRunner
"""

from vertebra.server._native import run_worker

__all__ = ("run_worker",)
