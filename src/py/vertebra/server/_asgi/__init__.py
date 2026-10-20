"""Vertebra ASGI adapter — ASGI 3.0 compatibility layer.

from vertebra.server._asgi import AsgiAdapter, AsgiRunner
"""

from vertebra.server._asgi._adapter import AsgiAdapter
from vertebra.server._asgi._extensions import (
    FastApiExtension,
    HeaderFilter,
    LitestarExtension,
    compile_guard_pattern,
    get_extensions,
    is_static_route,
    register_extension,
)
from vertebra.server._asgi._queues import (
    AsgiReceive,
    AsgiResponse,
    AsgiSend,
)
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.server._asgi._scope import AsgiScope, build_scope

__all__ = (
    "AsgiAdapter",
    "AsgiReceive",
    "AsgiResponse",
    "AsgiRunner",
    "AsgiScope",
    "AsgiSend",
    "FastApiExtension",
    "HeaderFilter",
    "LitestarExtension",
    "build_scope",
    "compile_guard_pattern",
    "get_extensions",
    "is_static_route",
    "register_extension",
)
