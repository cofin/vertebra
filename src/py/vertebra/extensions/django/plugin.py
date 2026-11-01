"""DjangoPlugin: unifies Rust route compilation, WSGI/ASGI C-API fastpath, and ecosystem bridges."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra._vertebra import Request, Response, WsgiEnviron, run_wsgi_callable
from vertebra.extensions.django.config import DjangoConfig
from vertebra.extensions.django.fastpath import FastPath, dispatch_django_direct
from vertebra.extensions.django.mcp import MCPRegistry, attach_mcp_routes, dispatch_mcp_request
from vertebra.extensions.django.offload import offload_middlewares
from vertebra.extensions.django.openapi import attach_openapi_docs, extract_openapi_schema
from vertebra.extensions.django.router import RouteCompiler
from vertebra.extensions.django.vite import ViteBridge, attach_vite

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from vertebra.extensions.django.offload import OffloadPlan
    from vertebra.extensions.django.router import CompiledRouter

__all__ = ("DjangoPlugin",)


class DjangoPlugin:
    """First-class Vertebra integration plugin for Django applications."""

    __slots__ = (
        "_compiler",
        "_config",
        "_fastpath",
        "_mcp_registry",
        "_offload_bundle",
        "_openapi_schema",
        "_settings",
        "_static_responses",
        "_urlpatterns",
        "_vite_bridge",
        "_wsgi_app",
    )

    def __init__(
        self,
        urlpatterns: list[Any] | None = None,
        config: DjangoConfig | None = None,
        *,
        settings: Any = None,
        wsgi_app: Callable[..., Iterable[bytes]] | None = None,
    ) -> None:
        self._config = config or DjangoConfig()
        self._urlpatterns = urlpatterns
        self._settings = settings
        self._wsgi_app = wsgi_app
        self._compiler: RouteCompiler | None = None
        self._fastpath: FastPath | None = None
        self._offload_bundle: OffloadPlan | None = None
        self._openapi_schema: dict[str, Any] | None = None
        self._mcp_registry: MCPRegistry | None = None
        self._vite_bridge: ViteBridge | None = None
        self._static_responses: dict[str, Response] = {}
        if urlpatterns is not None or settings is not None:
            self.setup(urlpatterns=urlpatterns, settings=settings, wsgi_app=wsgi_app)

    @property
    def config(self) -> DjangoConfig:
        """Return the ``DjangoConfig``."""
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
    def vite_bridge(self) -> ViteBridge | None:
        """Return the ``ViteBridge`` if Vite is configured."""
        return self._vite_bridge

    def setup(
        self,
        urlpatterns: list[Any] | None = None,
        *,
        settings: Any = None,
        wsgi_app: Callable[..., Iterable[bytes]] | None = None,
    ) -> DjangoPlugin:
        """Compile Django ``urlpatterns``, configure OpenAPI/MCP/Vite, and extract middleware offloads."""
        if urlpatterns is not None:
            self._urlpatterns = urlpatterns
        if settings is not None:
            self._settings = settings
        if wsgi_app is not None:
            self._wsgi_app = wsgi_app

        if self._config.vite is not None:
            self._vite_bridge = attach_vite(self, self._config.vite)

        if self._config.compile_routes:
            compiler = RouteCompiler()
            if self._urlpatterns is not None:
                compiler.compile_urlpatterns(self._urlpatterns)
            elif self._config.root_urlconf is not None:
                compiler.compile_urlconf(self._config.root_urlconf)
            self._compiler = compiler
            if self._config.enable_fastpath:
                self._fastpath = FastPath(compiler)

        if self._config.mcp is not None:
            mcp_reg, mcp_static = attach_mcp_routes(self._compiler or self._urlpatterns, self._config.mcp)
            self._mcp_registry = mcp_reg
            self._static_responses.update(mcp_static)

        if self._config.openapi is not None:
            schema, docs_static = attach_openapi_docs(
                self._urlpatterns,
                self._config.openapi,
                compiler=self._compiler,
            )
            self._openapi_schema = schema
            self._static_responses.update(docs_static)
        elif self._compiler is not None:
            self._openapi_schema = extract_openapi_schema(self._compiler)

        if self._config.offload_middlewares:
            openapi_path = self._config.openapi.schema_path if self._config.openapi is not None else "/openapi.json"
            self._offload_bundle = offload_middlewares(
                self._settings,
                openapi_schema=self._openapi_schema,
                openapi_path=openapi_path,
            )

        return self

    def wrap_wsgi_application(
        self,
        wsgi_app: Callable[..., Iterable[bytes]],
    ) -> Callable[..., Iterable[bytes]]:
        """Wrap a Django WSGI callable with zero-copy fastpath and static doc/MCP routes."""
        self._wsgi_app = wsgi_app

        def _entry(
            environ: WsgiEnviron | dict[str, Any],
            start_response: Callable[..., Any],
        ) -> Iterable[bytes]:
            path = str(environ.get("PATH_INFO", "/") or "/")
            method = str(environ.get("REQUEST_METHOD", "GET") or "GET").upper()
            if method == "GET" and path in self._static_responses:
                resp = self._static_responses[path]
                headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in resp.headers]
                start_response(f"{resp.status} OK", headers)
                return [bytes(resp.body)]
            if (
                method == "POST"
                and self._mcp_registry is not None
                and self._config.mcp is not None
                and path == self._config.mcp.path
            ):
                if isinstance(environ, WsgiEnviron):
                    req = environ.request()
                    resp = dispatch_mcp_request(self._mcp_registry, req, self._config.mcp)
                else:
                    wsgi_in = environ.get("wsgi.input")
                    raw = wsgi_in.read() if wsgi_in is not None and hasattr(wsgi_in, "read") else b"{}"
                    resp = dispatch_mcp_request(self._mcp_registry, raw, self._config.mcp)
                headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in resp.headers]
                start_response(f"{resp.status} OK", headers)
                return [bytes(resp.body)]
            if self._fastpath is not None:
                chunks = self._fastpath.dispatch_wsgi(environ, start_response)
                if chunks is not None:
                    return chunks
            return wsgi_app(environ, start_response)

        return _entry

    def handle_request(self, request: Request) -> Response:
        """Dispatch a Vertebra ``Request`` via static docs/MCP, fastpath, or WSGI fallback."""
        method = request.method.upper()
        path = request.path
        if method == "GET" and path in self._static_responses:
            return self._static_responses[path]
        if (
            method == "POST"
            and self._mcp_registry is not None
            and self._config.mcp is not None
            and path == self._config.mcp.path
        ):
            return dispatch_mcp_request(self._mcp_registry, request, self._config.mcp)
        if self._fastpath is not None:
            matched = self._fastpath.match(method, path)
            if matched is not None:
                compiled, params = matched
                if getattr(compiled.callback, "__vertebra_fastpath__", False) and not compiled.is_async:
                    return dispatch_django_direct(compiled, request, params)
        if self._wsgi_app is not None:
            environ = request.wsgi_environ()
            return run_wsgi_callable(self._wsgi_app, environ, int(request.request_id))
        return Response(status=404, headers=[("content-type", "text/plain")], body=b"Not Found")
