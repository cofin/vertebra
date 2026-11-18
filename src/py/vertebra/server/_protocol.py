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
    app_cls: Any = type(app)
    is_async_fn = inspect.iscoroutinefunction(app) or (callable(app) and inspect.iscoroutinefunction(app_cls.__call__))
    try:
        sig = inspect.signature(app)
        positional_params = [
            p
            for p in sig.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
        if not has_varargs:
            if len(positional_params) == 3:
                return "asgi"
            if len(positional_params) == 2:
                return "rsgi" if is_async_fn else "wsgi"
    except (TypeError, ValueError):
        pass
    return "asgi"
