"""VertebraPlugin: unifies Rust route compilation, WSGI C-API fastpath, and ecosystem bridges."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra._vertebra import Request, Response, WsgiEnviron, run_wsgi_callable
from vertebra.extensions.flask.config import VertebraConfig
from vertebra.extensions.flask.fastpath import FastPath
from vertebra.extensions.flask.mcp import MCPRegistry, WsgiMcpApp, attach_mcp_routes
from vertebra.extensions.flask.offload import offload_middlewares
from vertebra.extensions.flask.openapi import attach_openapi_docs, extract_openapi_schema
from vertebra.extensions.flask.router import RouteCompiler
from vertebra.extensions.flask.vite import ViteAssetBridge, attach_vite

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from vertebra.extensions.flask.offload import OffloadPlan
    from vertebra.extensions.flask.router import CompiledRouter

__all__ = ("VertebraPlugin",)


class VertebraPlugin:
    """First-class Vertebra integration plugin for Flask applications."""

    __slots__ = (
        "_app",
        "_compiler",
        "_config",
        "_fastpath",
        "_mcp_app",
        "_mcp_registry",
        "_offload_bundle",
        "_openapi_schema",
        "_original_wsgi_app",
        "_vite_bridge",
    )

    def __init__(
        self,
        app: Any = None,
        config: VertebraConfig | None = None,
    ) -> None:
        self._config = config or VertebraConfig()
        self._app: Any = None
        self._compiler: RouteCompiler | None = None
        self._fastpath: FastPath | None = None
        self._offload_bundle: OffloadPlan | None = None
        self._openapi_schema: dict[str, Any] | None = None
        self._mcp_registry: MCPRegistry | None = None
        self._mcp_app: WsgiMcpApp | None = None
        self._vite_bridge: ViteAssetBridge | None = None
        self._original_wsgi_app: Callable[..., Iterable[bytes]] | None = None
        if app is not None:
            self.init_app(app)

    @property
    def config(self) -> VertebraConfig:
        """Return the ``VertebraConfig``."""
        return self._config

    @property
    def compiler(self) -> RouteCompiler | None:
        """Return the ``RouteCompiler`` instance."""
        return self._compiler

    @property
    def compiled_router(self) -> CompiledRouter | None:
        """Return the compiled Rust ``CompiledRouter``."""
        return self._compiler.router if self._compiler is not None else None

    @property
    def fastpath(self) -> FastPath | None:
        """Return the ``FastPath`` dispatcher."""
        return self._fastpath

    @property
    def offload_bundle(self) -> OffloadPlan | None:
        """Return the ``OffloadPlan``."""
        return self._offload_bundle

    @property
    def openapi_schema(self) -> dict[str, Any] | None:
        """Return the generated OpenAPI 3.1.0 schema dictionary."""
        return self._openapi_schema

    @property
    def mcp_registry(self) -> MCPRegistry | None:
        """Return the ``MCPRegistry`` if MCP is configured."""
        return self._mcp_registry

    @property
    def vite_bridge(self) -> ViteAssetBridge | None:
        """Return the ``ViteAssetBridge`` if Vite is configured."""
        return self._vite_bridge

    def init_app(self, app: Any) -> VertebraPlugin:
        """Bind this ``VertebraPlugin`` to a Flask application and compile routes and fastpaths."""
        self._app = app
        if self._config.vite is not None:
            self._vite_bridge = attach_vite(app, self._config.vite)
        if self._config.mcp is not None:
            self._mcp_registry = attach_mcp_routes(app, self._config.mcp)
            self._mcp_app = getattr(self._mcp_registry, "_mcp_app", None)
        if self._config.openapi is not None:
            self._openapi_schema = attach_openapi_docs(app, self._config.openapi)
        else:
            self._openapi_schema = extract_openapi_schema(app)

        if self._config.compile_routes:
            compiler = RouteCompiler()
            compiler.compile_app(app)
            self._compiler = compiler
            if self._config.enable_fastpath:
                self._fastpath = FastPath(compiler)

        if self._config.offload_middlewares:
            openapi_path = self._config.openapi.schema_path if self._config.openapi is not None else "/openapi.json"
            self._offload_bundle = offload_middlewares(
                app,
                openapi_schema=self._openapi_schema,
                openapi_path=openapi_path,
            )

        orig_wsgi = getattr(app, "wsgi_app", None)
        if callable(orig_wsgi):
            self._original_wsgi_app = orig_wsgi
            app.wsgi_app = self._wrap_wsgi_app(orig_wsgi)

        setattr(app, self._config.app_attribute, self)
        extensions = getattr(app, "extensions", None)
        if isinstance(extensions, dict):
            extensions["vertebra"] = self
        return self

    def handle_request(self, request: Request) -> Response:
        """Dispatch a Vertebra ``Request`` via fastpath or fallback to ``run_wsgi_callable``."""
        if self._mcp_app is not None and self._config.mcp is not None and request.path == self._config.mcp.path:
            return self._mcp_app.handle_vertebra_request(request)
        if self._fastpath is not None:
            fast_resp = self._fastpath.dispatch(request)
            if fast_resp is not None:
                return fast_resp
        environ = request.wsgi_environ()
        wsgi_target = getattr(self._app, "wsgi_app", self._app)
        return run_wsgi_callable(wsgi_target, environ, int(request.request_id))

    def _wrap_wsgi_app(
        self,
        inner_wsgi: Callable[..., Iterable[bytes]],
    ) -> Callable[..., Iterable[bytes]]:
        """Wrap ``app.wsgi_app`` so eligible routes execute via the zero-copy C-API fastpath."""

        def _wsgi_entry(
            environ: WsgiEnviron | dict[str, Any],
            start_response: Callable[..., Any],
        ) -> Iterable[bytes]:
            if (
                self._mcp_app is not None
                and self._config.mcp is not None
                and str(environ.get("PATH_INFO", "/") or "/") == self._config.mcp.path
            ):
                return self._mcp_app(environ, start_response)
            if self._fastpath is not None:
                fast_chunks = self._fastpath.dispatch_wsgi(environ, start_response)
                if fast_chunks is not None:
                    return fast_chunks
            return inner_wsgi(environ, start_response)

        return _wsgi_entry
