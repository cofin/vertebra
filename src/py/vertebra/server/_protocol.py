"""Gateway protocol detection (ASGI, WSGI, RSGI) for application entrypoints."""

import inspect
from typing import Any, Literal

__all__ = (
    "InterfaceMode",
    "detect_interface",
    "is_rsgi_app",
)

type InterfaceMode = Literal["auto", "asgi", "rsgi", "wsgi"]


def is_rsgi_app(app: Any) -> bool:
    """Return True if the application object exposes an RSGI __rsgi__ entrypoint."""
    return callable(getattr(app, "__rsgi__", None))


def detect_interface(app: Any, configured: InterfaceMode = "auto") -> Literal["asgi", "rsgi", "wsgi"]:
    """Resolve the gateway protocol interface for an application callable."""
    if configured != "auto":
        return configured
    if is_rsgi_app(app):
        return "rsgi"
    if inspect.iscoroutinefunction(app) or (callable(app) and inspect.iscoroutinefunction(type(app).__call__)):
        return "asgi"
    try:
        sig = inspect.signature(app)
        if len(sig.parameters) == 3:
            return "asgi"
        if len(sig.parameters) == 2:
            return "wsgi"
    except (TypeError, ValueError):
        pass
    return "asgi"
