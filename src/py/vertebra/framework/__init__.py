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
    from vertebra.framework._router import (
        Controller,
        Route,
        Router,
        delete,
        get,
        patch,
        post,
        put,
    )
    from vertebra.framework._welcome import (
        WelcomePageRenderer,
        WelcomeRouteItem,
        WelcomeSnapshot,
    )
    from vertebra.framework.di import Container, DependencyGraph, Inject, Provide
    from vertebra.framework.procedures import (
        Procedure,
        ProcedureRouter,
        mutation,
        query,
        subscription,
    )

__all__ = (
    "App",
    "Container",
    "Controller",
    "DependencyGraph",
    "Inject",
    "Procedure",
    "ProcedureRouter",
    "Provide",
    "Route",
    "Router",
    "WelcomePageRenderer",
    "WelcomeRouteItem",
    "WelcomeSnapshot",
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
    "mutation",
    "openapi",
    "patch",
    "post",
    "procedures",
    "put",
    "query",
    "queues",
    "security",
    "ssr",
    "stores",
    "subscription",
)

__lazy_modules__ = [
    "vertebra.framework._app",
    "vertebra.framework._router",
    "vertebra.framework._welcome",
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
    "Container": ("vertebra.framework.di", "Container"),
    "Controller": ("vertebra.framework._router", "Controller"),
    "DependencyGraph": ("vertebra.framework.di", "DependencyGraph"),
    "Inject": ("vertebra.framework.di", "Inject"),
    "Procedure": ("vertebra.framework.procedures", "Procedure"),
    "ProcedureRouter": ("vertebra.framework.procedures", "ProcedureRouter"),
    "Provide": ("vertebra.framework.di", "Provide"),
    "Route": ("vertebra.framework._router", "Route"),
    "Router": ("vertebra.framework._router", "Router"),
    "WelcomePageRenderer": ("vertebra.framework._welcome", "WelcomePageRenderer"),
    "WelcomeRouteItem": ("vertebra.framework._welcome", "WelcomeRouteItem"),
    "WelcomeSnapshot": ("vertebra.framework._welcome", "WelcomeSnapshot"),
    "delete": ("vertebra.framework._router", "delete"),
    "get": ("vertebra.framework._router", "get"),
    "mutation": ("vertebra.framework.procedures", "mutation"),
    "patch": ("vertebra.framework._router", "patch"),
    "post": ("vertebra.framework._router", "post"),
    "put": ("vertebra.framework._router", "put"),
    "query": ("vertebra.framework.procedures", "query"),
    "subscription": ("vertebra.framework.procedures", "subscription"),
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
