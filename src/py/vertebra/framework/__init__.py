"""Vertebra application framework and modular subdomains."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.framework import (
        asyncapi,
        autowire,
        channels,
        di,
        email,
        htmx,
        inertia,
        live,
        openapi,
        procedures,
        queues,
        security,
        ssr,
        stores,
    )
    from vertebra.framework._app import App
    from vertebra.framework._router import RouteDef, Router, delete, get, patch, post, put

__all__ = (
    "App",
    "RouteDef",
    "Router",
    "asyncapi",
    "autowire",
    "channels",
    "delete",
    "di",
    "email",
    "get",
    "htmx",
    "inertia",
    "live",
    "openapi",
    "patch",
    "post",
    "procedures",
    "put",
    "queues",
    "security",
    "ssr",
    "stores",
)

__lazy_modules__ = [
    "vertebra.framework._app",
    "vertebra.framework._router",
    "vertebra.framework.asyncapi",
    "vertebra.framework.autowire",
    "vertebra.framework.channels",
    "vertebra.framework.di",
    "vertebra.framework.email",
    "vertebra.framework.htmx",
    "vertebra.framework.inertia",
    "vertebra.framework.live",
    "vertebra.framework.openapi",
    "vertebra.framework.procedures",
    "vertebra.framework.queues",
    "vertebra.framework.security",
    "vertebra.framework.ssr",
    "vertebra.framework.stores",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "App": ("vertebra.framework._app", "App"),
    "RouteDef": ("vertebra.framework._router", "RouteDef"),
    "Router": ("vertebra.framework._router", "Router"),
    "delete": ("vertebra.framework._router", "delete"),
    "get": ("vertebra.framework._router", "get"),
    "patch": ("vertebra.framework._router", "patch"),
    "post": ("vertebra.framework._router", "post"),
    "put": ("vertebra.framework._router", "put"),
    "ParamSpec": ("typing", "ParamSpec"),
    "asyncapi": ("vertebra.framework.asyncapi", ""),
    "autowire": ("vertebra.framework.autowire", ""),
    "channels": ("vertebra.framework.channels", ""),
    "di": ("vertebra.framework.di", ""),
    "email": ("vertebra.framework.email", ""),
    "htmx": ("vertebra.framework.htmx", ""),
    "inertia": ("vertebra.framework.inertia", ""),
    "live": ("vertebra.framework.live", ""),
    "openapi": ("vertebra.framework.openapi", ""),
    "procedures": ("vertebra.framework.procedures", ""),
    "queues": ("vertebra.framework.queues", ""),
    "security": ("vertebra.framework.security", ""),
    "ssr": ("vertebra.framework.ssr", ""),
    "stores": ("vertebra.framework.stores", ""),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
