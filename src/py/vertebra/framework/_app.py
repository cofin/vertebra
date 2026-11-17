"""Application container for Vertebra.

Provides routing, Controller and ProcedureRouter registration, hierarchical DI,
sub-application mounting, handler ID indexing, and startup/shutdown lifecycle hooks.
"""

import contextlib
import functools
import inspect
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast

import msgspec

from vertebra._vertebra import Response
from vertebra.framework._router import (
    Controller,
    Route,
    Router,
    _compile_path_template,
    _make_handler_id,
    _normalize_path,
    _normalize_prefix,
)
from vertebra.framework._welcome import WelcomePageRenderer
from vertebra.framework.di import Container, DependencyGraph
from vertebra.framework.openapi import OpenAPIConfig, OpenAPISpecBuilder
from vertebra.framework.procedures import Procedure, ProcedureRouter
from vertebra.typing import LifecycleHook
from vertebra.utils.serializers import encode_grpc_frame_into

__all__ = ("App", "HandlerInfo", "LifecycleHook", "_make_handler_id")


class HandlerInfo(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Metadata stored per registered handler, serializable via msgspec."""

    handler_id: int
    method: str
    path: str
    handler_name: str
    sync_to_thread: bool = False


class App:
    """High-performance application container for routing, DI, procedures, and lifecycle hooks."""

    __slots__ = (
        "_container",
        "_controllers",
        "_databases",
        "_debug",
        "_dependencies",
        "_guards",
        "_handler_index",
        "_handlers",
        "_mounts",
        "_openapi_builder",
        "_openapi_config",
        "_plugins",
        "_procedure_router",
        "_routers",
        "_routes",
        "_shutdown_hooks",
        "_startup_hooks",
        "_welcome_route_active",
    )

    def __init__(
        self,
        *,
        route_handlers: Sequence[Any] = (),
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        plugins: Sequence[Any] = (),
        container: Container | None = None,
        debug: bool = False,
    ) -> None:
        """Initialize an App with optional route_handlers, dependencies, guards, plugins, DI container, and debug flag."""
        self._debug: bool = bool(debug)
        self._welcome_route_active: bool = False
        self._routers: list[Router] = []
        self._controllers: list[type[Controller] | Controller] = []
        self._routes: list[Route] = []
        self._databases: list[Any] = []
        self._plugins: list[Any] = []
        self._startup_hooks: list[LifecycleHook] = []
        self._shutdown_hooks: list[LifecycleHook] = []
        self._handler_index: dict[int, HandlerInfo] = {}
        self._handlers: dict[int, Callable[..., Any]] = {}
        self._mounts: list[tuple[str, Any, str]] = []
        self._openapi_config: Any = None
        self._openapi_builder: Any = None
        self._dependencies: dict[str, Any] = dict(dependencies) if dependencies else {}
        self._guards: tuple[Callable[..., Any], ...] = tuple(guards) if guards else ()
        if container is not None:
            self._container = container
            for key, prov in self._dependencies.items():
                self._container.graph.register(key, prov)
            if self._container.graph.providers:
                self._container.graph.compile()
        else:
            self._container = Container(DependencyGraph(self._dependencies))
        self._procedure_router = ProcedureRouter(
            dependencies=self._dependencies,
            guards=self._guards,
            container=self._container,
        )
        for item in route_handlers:
            self.register(item)
        for plugin in plugins:
            self.register(plugin)

    @property
    def routes(self) -> tuple[Route, ...]:
        """Return all registered route definitions in registration order."""
        return tuple(self._routes)

    @property
    def routers(self) -> tuple[Router, ...]:
        """Return all registered routers."""
        return tuple(self._routers)

    @property
    def controllers(self) -> tuple[type[Controller] | Controller, ...]:
        """Return all registered Controllers."""
        return tuple(self._controllers)

    @property
    def procedures(self) -> dict[str, Procedure]:
        """Return all registered RPC Procedures keyed by name."""
        return self._procedure_router.procedures

    @property
    def procedure_router(self) -> ProcedureRouter:
        """Return the application's primary ProcedureRouter."""
        return self._procedure_router

    @property
    def container(self) -> Container:
        """Return the application's root DI Container."""
        return self._container

    @property
    def dependencies(self) -> dict[str, Any]:
        """Return application-level dependency providers."""
        return dict(self._dependencies)

    @property
    def guards(self) -> tuple[Callable[..., Any], ...]:
        """Return application-level guard callables."""
        return self._guards

    @property
    def handler_index(self) -> dict[int, HandlerInfo]:
        """Mapping from stable uint64 handler_id to HandlerInfo."""
        return dict(self._handler_index)

    @property
    def mounts(self) -> tuple[tuple[str, Any, str], ...]:
        """All mounted sub-applications as (prefix, app, protocol) tuples."""
        return tuple(self._mounts)

    @property
    def databases(self) -> tuple[Any, ...]:
        """Return all registered database configurations."""
        return tuple(self._databases)

    @property
    def plugins(self) -> tuple[Any, ...]:
        """Return all registered application plugins."""
        return tuple(self._plugins)

    @property
    def debug(self) -> bool:
        """Return whether debug/development mode is enabled on this App instance."""
        return self._debug

    def add_route(self, route: Route) -> None:
        """Append a single Route definition, bind its DI context, and index its handler."""
        effective_route = (
            route.with_context(dependencies=self._dependencies, guards=self._guards)
            if (self._dependencies or self._guards)
            else route
        )
        method_upper = effective_route.method.upper()
        if effective_route.path == "/" and method_upper in {"GET", "ANY"} and self._welcome_route_active:
            welcome_id = _make_handler_id("GET", "/")
            self._handler_index.pop(welcome_id, None)
            self._handlers.pop(welcome_id, None)
            self._welcome_route_active = False
        self._routes.append(effective_route)
        handler_id = _make_handler_id(effective_route.method, effective_route.path)
        info = HandlerInfo(
            handler_id=handler_id,
            method=method_upper,
            path=effective_route.path,
            handler_name=getattr(effective_route.handler, "__qualname__", repr(effective_route.handler)),
            sync_to_thread=effective_route.sync_to_thread,
        )
        self._handler_index[handler_id] = info
        handler_fn = effective_route.handler

        route_container = self._container.create_child(effective_route.dependencies)
        has_route_meta = bool(
            effective_route.segments
            or effective_route.dependencies
            or effective_route.guards
            or effective_route.status_code != 200
            or route_container.graph.providers
        )

        existing_route = getattr(handler_fn, "__vertebra_bound_route__", None)
        if existing_route is not None and existing_route != effective_route:
            handler_fn = _clone_handler_callable(handler_fn)

        if effective_route.sync_to_thread and not getattr(handler_fn, "__vertebra_sync_to_thread__", False):
            try:
                cast("Any", handler_fn).__vertebra_sync_to_thread__ = True
            except (AttributeError, TypeError):
                handler_fn = _clone_handler_callable(handler_fn)
                cast("Any", handler_fn).__vertebra_sync_to_thread__ = True

        if has_route_meta:
            try:
                cast("Any", handler_fn).__vertebra_bound_route__ = effective_route
                cast("Any", handler_fn).__vertebra_container__ = route_container
            except (AttributeError, TypeError):
                handler_fn = _clone_handler_callable(handler_fn)
                cast("Any", handler_fn).__vertebra_bound_route__ = effective_route
                cast("Any", handler_fn).__vertebra_container__ = route_container

        self._handlers[handler_id] = handler_fn

    def register(self, target: Any) -> Any:
        """Register a Router, Controller, ProcedureRouter, Procedure, Route, or route-decorated callable."""
        if isinstance(target, Route):
            self.add_route(target)
            return target

        if isinstance(target, Router):
            self._routers.append(target)
            for route in target.routes:
                self.add_route(route)
            for proc in target.procedures:
                self._register_procedure_and_mount(proc, prefix=self._procedure_router.prefix)
            return target

        if isinstance(target, ProcedureRouter):
            for proc in target.procedures.values():
                self._register_procedure_and_mount(proc, prefix=target.prefix)
            return target

        if isinstance(target, Procedure):
            self._register_procedure_and_mount(target, prefix=self._procedure_router.prefix)
            return target

        if isinstance(target, Controller):
            self._controllers.append(target)
            ctrl_cls = type(target)
            for route in ctrl_cls.get_routes(target):
                self.add_route(route)
            for proc in ctrl_cls.get_procedures(target):
                self._register_procedure_and_mount(proc, prefix=self._procedure_router.prefix)
            return target

        if isinstance(target, type) and issubclass(target, Controller):
            self._controllers.append(target)
            ctrl_type: type[Controller] = target
            for route in ctrl_type.get_routes(None):
                self.add_route(route)
            for proc in ctrl_type.get_procedures(None):
                self._register_procedure_and_mount(proc, prefix=self._procedure_router.prefix)
            return target

        if callable(target):
            routes = getattr(target, "__vertebra_routes__", None)
            if routes:
                for route in routes:
                    self.add_route(route)
                return target

        provide_session = getattr(target, "provide_session", None)
        provide_connection = getattr(target, "provide_connection", None)
        if provide_session is not None or provide_connection is not None:
            self._databases.append(target)
            bind_key = getattr(target, "bind_key", None)
            session_key = f"{bind_key}_session" if bind_key and bind_key != "default" else "db_session"
            conn_key = f"{bind_key}_connection" if bind_key and bind_key != "default" else "db_connection"
            if session_key not in self._dependencies and provide_session is not None:
                self._dependencies[session_key] = provide_session
                self._container.graph.register(session_key, provide_session)
            if conn_key not in self._dependencies and provide_connection is not None:
                self._dependencies[conn_key] = provide_connection
                self._container.graph.register(conn_key, provide_connection)
            return target

        if hasattr(target, "on_app_init") or hasattr(target, "on_cli_init") or hasattr(target, "server_lifespan"):
            if target not in self._plugins:
                self._plugins.append(target)
            on_init = getattr(target, "on_app_init", None)
            if callable(on_init):
                with contextlib.suppress(TypeError, AttributeError):
                    on_init(self)
            return target

        msg = "register expects a Router, Controller, ProcedureRouter, Procedure, Route, plugin, or decorated handler"
        raise TypeError(msg)

    @contextlib.contextmanager
    def lifespan(self) -> Iterator[None]:
        """Enter server_lifespan context managers for all registered CLI/server plugins."""
        with contextlib.ExitStack() as stack:
            for plugin in self._plugins:
                lifespan_fn = getattr(plugin, "server_lifespan", None)
                if callable(lifespan_fn):
                    cm = lifespan_fn(self)
                    if hasattr(cm, "__enter__"):
                        stack.enter_context(cast("contextlib.AbstractContextManager[Any]", cm))
            yield

    async def call_procedure(self, name: str, payload: Any = None, *, request: Any = None) -> Any:
        """Invoke a registered RPC Procedure by name using the application's DI container."""
        return await self._procedure_router.call(name, payload, request=request, container=self._container)

    def get(
        self,
        path: str,
        *,
        sync_to_thread: bool = False,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
        status_code: int = 200,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a GET handler."""
        return self._route(
            "GET",
            path,
            sync_to_thread=sync_to_thread,
            dependencies=dependencies,
            guards=guards,
            tags=tags,
            status_code=status_code,
        )

    def post(
        self,
        path: str,
        *,
        sync_to_thread: bool = False,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
        status_code: int = 200,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a POST handler."""
        return self._route(
            "POST",
            path,
            sync_to_thread=sync_to_thread,
            dependencies=dependencies,
            guards=guards,
            tags=tags,
            status_code=status_code,
        )

    def put(
        self,
        path: str,
        *,
        sync_to_thread: bool = False,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
        status_code: int = 200,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a PUT handler."""
        return self._route(
            "PUT",
            path,
            sync_to_thread=sync_to_thread,
            dependencies=dependencies,
            guards=guards,
            tags=tags,
            status_code=status_code,
        )

    def delete(
        self,
        path: str,
        *,
        sync_to_thread: bool = False,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
        status_code: int = 200,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a DELETE handler."""
        return self._route(
            "DELETE",
            path,
            sync_to_thread=sync_to_thread,
            dependencies=dependencies,
            guards=guards,
            tags=tags,
            status_code=status_code,
        )

    def patch(
        self,
        path: str,
        *,
        sync_to_thread: bool = False,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
        status_code: int = 200,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a PATCH handler."""
        return self._route(
            "PATCH",
            path,
            sync_to_thread=sync_to_thread,
            dependencies=dependencies,
            guards=guards,
            tags=tags,
            status_code=status_code,
        )

    def mount(self, prefix: str, sub_app: Any, *, protocol: str = "auto") -> None:
        """Mount a WSGI or ASGI sub-application under a path prefix."""
        norm_prefix = _normalize_prefix(prefix)
        if norm_prefix in {"", "/"} and self._welcome_route_active:
            welcome_id = _make_handler_id("GET", "/")
            self._handler_index.pop(welcome_id, None)
            self._handlers.pop(welcome_id, None)
            self._welcome_route_active = False
        self._mounts.append((norm_prefix, sub_app, protocol))

    def ensure_dev_welcome_route(self, *, force_dev: bool = False) -> bool:
        """Register the development welcome route on unclaimed ``GET /`` when debug or ``VERTEBRA_DEV`` is active."""
        dev_env = os.environ.get("VERTEBRA_DEV", "").strip().lower() in {"1", "true", "yes", "on"}
        is_dev = bool(force_dev or self._debug or dev_env)
        has_user_get_root = any(route.path == "/" and route.method.upper() in {"GET", "ANY"} for route in self._routes)
        has_root_mount = any(prefix in {"", "/"} for prefix, _, _ in self._mounts)
        welcome_id = _make_handler_id("GET", "/")

        if not is_dev or has_user_get_root or has_root_mount:
            if self._welcome_route_active:
                if not any(route.path == "/" and route.method.upper() == "GET" for route in self._routes):
                    self._handler_index.pop(welcome_id, None)
                    self._handlers.pop(welcome_id, None)
                self._welcome_route_active = False
            return False

        if self._welcome_route_active and welcome_id in self._handlers:
            return True

        welcome_handler = WelcomePageRenderer.make_handler(self)
        self._handler_index[welcome_id] = HandlerInfo(
            handler_id=welcome_id,
            method="GET",
            path="/",
            handler_name="WelcomePageRenderer.handle_welcome",
            sync_to_thread=False,
        )
        self._handlers[welcome_id] = welcome_handler
        self._welcome_route_active = True
        return True

    def enable_docs(self, config: Any = None) -> Any:
        """Enable OpenAPI schema and configured interactive documentation portals."""
        cfg = config or OpenAPIConfig()
        builder = OpenAPISpecBuilder.from_app(self, config=cfg)
        self._openapi_config = cfg
        self._openapi_builder = builder

        @self.get(cfg.schema_path)
        def _openapi_schema() -> dict[str, Any]:
            return builder.build()

        _openapi_schema.__name__ = "openapi_schema"
        _openapi_schema.__qualname__ = "App.openapi_schema"
        schema_id = _make_handler_id("GET", cfg.schema_path)
        if schema_id in self._handler_index:
            prev = self._handler_index[schema_id]
            self._handler_index[schema_id] = HandlerInfo(
                handler_id=prev.handler_id,
                method=prev.method,
                path=prev.path,
                handler_name="App.openapi_schema",
                sync_to_thread=prev.sync_to_thread,
            )

        for docs_path, docs_html in cfg.render_docs_routes().items():

            def _make_docs_handler(html_payload: str, route_label: str) -> Callable[[], Response]:
                html_bytes = html_payload.encode("utf-8")

                def _docs_handler() -> Response:
                    return Response(200, html_bytes, [("content-type", "text/html; charset=utf-8")])

                slug = route_label.strip("/").replace("/", "_") or "docs"
                _docs_handler.__name__ = f"docs_{slug}"
                _docs_handler.__qualname__ = f"App.docs_{slug}"
                return _docs_handler

            self.get(docs_path)(_make_docs_handler(docs_html, docs_path))

        return builder

    def on_startup(self, fn: LifecycleHook | None = None) -> Any:
        """Register a startup lifecycle hook directly or as a decorator."""
        if fn is None:

            def decorator(func: LifecycleHook) -> LifecycleHook:
                self._startup_hooks.append(func)
                return func

            return decorator
        self._startup_hooks.append(fn)
        return fn

    def on_shutdown(self, fn: LifecycleHook | None = None) -> Any:
        """Register a shutdown lifecycle hook directly or as a decorator."""
        if fn is None:

            def decorator(func: LifecycleHook) -> LifecycleHook:
                self._shutdown_hooks.append(func)
                return func

            return decorator
        self._shutdown_hooks.append(fn)
        return fn

    async def startup(self) -> None:
        """Execute all registered startup hooks sequentially and initialize database pools."""
        for db in self._databases:
            create_pool_fn = getattr(db, "_create_pool", None) or getattr(db, "create_pool", None)
            if callable(create_pool_fn):
                res = create_pool_fn()
                if inspect.isawaitable(res):
                    await res
        await self._run_hooks(self._startup_hooks)

    async def shutdown(self) -> None:
        """Execute all registered shutdown hooks sequentially and close singleton DI resources."""
        try:
            await self._run_hooks(self._shutdown_hooks)
            for db in self._databases:
                close_pool_fn = (
                    getattr(db, "_close_pool", None) or getattr(db, "close_pool", None) or getattr(db, "close", None)
                )
                if callable(close_pool_fn):
                    res = close_pool_fn()
                    if inspect.isawaitable(res):
                        await res
        finally:
            await self._container.aclose()

    def _register_procedure_and_mount(self, proc: Procedure, *, prefix: str) -> None:
        """Register a Procedure in the app's ProcedureRouter and mount its HTTP and gRPC endpoints."""
        bound = self._procedure_router.register(proc)
        norm_prefix = _normalize_prefix(prefix) or "/_rpc"
        rpc_path = f"{norm_prefix}/{bound.name}"
        grpc_path = f"/vertebra.rpc.ProcedureService/{bound.name}"
        proc_name = bound.name

        async def _rpc_http_handler(request: Any) -> Any:
            has_zero_copy = hasattr(request, "decode_msgspec") or hasattr(request, "body_view")
            payload = None if has_zero_copy else (getattr(request, "body", None) or None)
            result = await self._procedure_router.call(
                proc_name,
                payload,
                request=request,
                container=self._container,
            )
            headers_obj = getattr(request, "headers", None)
            req_ct = (
                headers_obj.get("content-type") if headers_obj is not None and hasattr(headers_obj, "get") else None
            )
            if req_ct and str(req_ct).startswith("application/grpc"):
                ct_str = str(req_ct)
                frame_mv = encode_grpc_frame_into(result, format=ct_str)
                return Response.from_buffer(
                    200,
                    frame_mv,
                    [("content-type", ct_str), ("grpc-status", "0")],
                    getattr(request, "request_id", 0),
                )
            return result

        _rpc_http_handler.__name__ = f"rpc_{proc_name}"
        _rpc_http_handler.__qualname__ = f"App.rpc_{proc_name}"
        self.add_route(Route(method="POST", path=rpc_path, handler=_rpc_http_handler, tags=("rpc",)))
        self.add_route(Route(method="POST", path=grpc_path, handler=_rpc_http_handler, tags=("rpc", "grpc")))
        if bound.kind == "query":
            self.add_route(Route(method="GET", path=rpc_path, handler=_rpc_http_handler, tags=("rpc",)))

    def _route(
        self,
        method: str,
        path: str,
        *,
        sync_to_thread: bool = False,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
        status_code: int = 200,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Create and register a Route on this App instance."""
        normalized = _normalize_path(path)

        def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
            segments = _compile_path_template(normalized)
            route = Route(
                method=method.upper(),
                path=normalized,
                handler=handler,
                segments=segments,
                sync_to_thread=sync_to_thread,
                dependencies=dict(dependencies) if dependencies else {},
                guards=tuple(guards) if guards else (),
                tags=tuple(tags),
                status_code=status_code,
            )
            self.add_route(route)
            return handler

        return decorator

    @staticmethod
    async def _run_hooks(hooks: list[LifecycleHook]) -> None:
        """Execute sync or async lifecycle hooks in order."""
        for hook in hooks:
            result = hook()
            if inspect.isawaitable(result):
                await result


def _clone_handler_callable(handler_fn: Callable[..., Any]) -> Callable[..., Any]:
    """Create a thin wrapper around handler_fn when attribute assignment or reuse requires isolation."""
    if inspect.iscoroutinefunction(handler_fn):

        @functools.wraps(handler_fn)
        async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await handler_fn(*args, **kwargs)

        return _async_wrapper

    @functools.wraps(handler_fn)
    def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        return handler_fn(*args, **kwargs)

    return _sync_wrapper
