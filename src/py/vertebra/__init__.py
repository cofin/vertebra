"""Vertebra — Python-first application backbone with Rust IPC primitives.

Example::

    from vertebra import VertebraApp

    app = VertebraApp()

    @app.get("/hello")
    def hello() -> str:
        return "world"
"""

from vertebra.framework._app import App, HandlerInfo, VertebraApp, _make_handler_id
from vertebra.framework._router import (
    RouteDef,
    Router,
    delete,
    extract_path_params,
    get,
    handler_id,
    patch,
    post,
    put,
)

__version__ = "0.1.0"

__all__ = (
    # Application
    "App",
    "HandlerInfo",
    "VertebraApp",
    # Routing
    "Router",
    "RouteDef",
    "delete",
    "extract_path_params",
    "get",
    "handler_id",
    "patch",
    "post",
    "put",
    # Meta
    "__version__",
    "_make_handler_id",
)
