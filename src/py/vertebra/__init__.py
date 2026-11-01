"""Vertebra — Python-first application backbone with Rust IPC primitives.

Example::

    from vertebra import App

    app = App()


    @app.get("/hello")
    def hello() -> str:
        return "world"
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vertebra.embeddings import Embedder, VectorBatchView, VectorView
    from vertebra.framework._app import App, HandlerInfo, _make_handler_id
    from vertebra.framework._router import (
        Controller,
        Route,
        Router,
        delete,
        extract_path_params,
        get,
        handler_id,
        patch,
        post,
        put,
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
    "Embedder",
    "HandlerInfo",
    "Inject",
    "Procedure",
    "ProcedureRouter",
    "Provide",
    "Route",
    "Router",
    "VectorBatchView",
    "VectorView",
    "__version__",
    "_make_handler_id",
    "delete",
    "extract_path_params",
    "get",
    "handler_id",
    "mutation",
    "patch",
    "post",
    "put",
    "query",
    "subscription",
)

__version__ = "0.3.0"

__lazy_modules__ = [
    "vertebra.agent",
    "vertebra.build",
    "vertebra.cli",
    "vertebra.db",
    "vertebra.embeddings",
    "vertebra.extensions",
    "vertebra.framework",
    "vertebra.server",
    "vertebra.storage",
    "vertebra.supervisor",
    "vertebra.telemetry",
    "vertebra.typing",
    "vertebra.utils",
]

_APP_EXPORTS = frozenset({"App", "HandlerInfo", "_make_handler_id"})
_ROUTER_EXPORTS = frozenset(
    {
        "Controller",
        "Route",
        "Router",
        "delete",
        "extract_path_params",
        "get",
        "handler_id",
        "patch",
        "post",
        "put",
    }
)
_DI_EXPORTS = frozenset({"Container", "DependencyGraph", "Inject", "Provide"})
_PROCEDURE_EXPORTS = frozenset(
    {
        "Procedure",
        "ProcedureRouter",
        "mutation",
        "query",
        "subscription",
    }
)
_EMBEDDING_EXPORTS = frozenset({"Embedder", "VectorBatchView", "VectorView"})


def __getattr__(name: str) -> Any:
    """Lazy-load framework exports on first attribute access."""
    if name in _APP_EXPORTS:
        from vertebra.framework import _app

        value = getattr(_app, name)
        globals()[name] = value
        return value
    if name in _ROUTER_EXPORTS:
        from vertebra.framework import _router

        value = getattr(_router, name)
        globals()[name] = value
        return value
    if name in _DI_EXPORTS:
        from vertebra.framework import di

        value = getattr(di, name)
        globals()[name] = value
        return value
    if name in _PROCEDURE_EXPORTS:
        from vertebra.framework import procedures

        value = getattr(procedures, name)
        globals()[name] = value
        return value
    if name in _EMBEDDING_EXPORTS:
        from vertebra import embeddings

        value = getattr(embeddings, name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return public export symbols and module globals for dir()."""
    return sorted(set(__all__) | set(globals().keys()))
