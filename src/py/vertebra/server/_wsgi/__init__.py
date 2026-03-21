"""Vertebra WSGI adapter — PEP 3333 compatibility layer.

from vertebra.server._wsgi import WsgiAdapter, WsgiEnviron
"""

from vertebra.server._wsgi._adapter import WsgiAdapter, WsgiError
from vertebra.server._wsgi._environ import BodyStream, WsgiEnviron
from vertebra.server._wsgi._extensions import (
    DjangoWsgiApp,
    FlaskWsgiApp,
    LargeBodyBypass,
    configure_django,
    create_flask_app,
    wrap_with_error_handler,
)
from vertebra.server._wsgi._runner import WsgiRunner

__all__ = [
    "BodyStream",
    "DjangoWsgiApp",
    "FlaskWsgiApp",
    "LargeBodyBypass",
    "WsgiAdapter",
    "WsgiEnviron",
    "WsgiError",
    "WsgiRunner",
    "configure_django",
    "create_flask_app",
    "wrap_with_error_handler",
]
