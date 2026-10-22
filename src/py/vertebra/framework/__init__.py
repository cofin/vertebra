"""Vertebra application framework and modular subdomains."""

from __future__ import annotations

from vertebra.framework import (
    a2a,
    asyncapi,
    autowire,
    channels,
    di,
    email,
    htmx,
    inertia,
    live,
    mcp,
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
    "a2a",
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
    "mcp",
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
