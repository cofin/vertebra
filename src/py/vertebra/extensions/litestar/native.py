from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from litestar import Litestar


@dataclass
class LitestarRoute:
    path: str
    methods: set[str]
    handler: Any
    handler_id: int
    is_websocket: bool = False
    guards: list[Callable] = field(default_factory=list)
    dependencies: dict[str, Any] = field(default_factory=dict)
    is_asgi: bool = False
    is_static: bool = False
    handler_fn: Callable | None = None


class VertebraRequest:
    """A high-performance request object for the native protocol."""

    __slots__ = ("_headers", "_json", "_message", "_user")

    def __init__(self, message: dict[str, Any]) -> None:
        self._message = message
        self._json = None
        self._headers = None
        self._user = None

    @property
    def method(self) -> str:
        return self._message.get("method", "GET")

    @property
    def path(self) -> str:
        return self._message.get("path", "/")

    @property
    def headers(self) -> dict[str, str]:
        if self._headers is None:
            self._headers = dict(self._message.get("headers", []))
        return self._headers

    @property
    def body(self) -> bytes:
        return self._message.get("body", b"")

    @property
    def path_params(self) -> dict[str, str]:
        return self._message.get("path_params", {})

    @property
    def auth_claims(self) -> dict[str, Any] | None:
        return self._message.get("auth_claims")

    @property
    def user(self) -> Any:
        if self._user is None:
            claims = self.auth_claims
            if claims:
                from vertebra.auth.models import VertebraUserMixin
                from vertebra.auth.proxy import LazyUserProxy

                # Default to VertebraUserMixin if no specific model is configured
                # In a real app, this should be configurable.
                async def noop_fetcher(user_id: str, ctx: Any) -> Any:
                    return VertebraUserMixin(**claims)

                self._user = LazyUserProxy(VertebraUserMixin, claims, noop_fetcher)
            else:
                from vertebra.auth.models import AnonymousUser

                self._user = AnonymousUser()
        return self._user

    def json(self) -> Any:
        if self._json is None:
            import json

            self._json = json.loads(self.body)
        return self._json


def reflect_routes(app: Litestar) -> list[LitestarRoute]:
    """Reflect all routes from a Litestar application.

    Traverses the application's route tree and extracts metadata for each handler.
    Assigns a stable integer ID to each route handler for use in the Rust router.
    """
    from litestar.routes import ASGIRoute, HTTPRoute, WebSocketRoute

    routes: list[LitestarRoute] = []

    # Litestar's app.routes contains the flattened list of routes
    for route in app.routes:
        if isinstance(route, HTTPRoute):
            for method, handler_tuple in route.route_handler_map.items():
                handler = handler_tuple[0]
                # In Litestar, the handler object itself is what we want.
                # The handler_id should be stable. We can use hash of the handler or id().
                # For now, we'll use id() but in a real app we might want something more stable across restarts if possible.
                # However, since this is built at startup, id() is fine for the session.

                # Check if we already have this handler for this path (different methods might share handlers)
                # We identify by handler object ID + path
                existing = next((r for r in routes if r.path == route.path and r.handler_id == id(handler)), None)
                if existing:
                    existing.methods.add(method)
                    continue

                routes.append(
                    LitestarRoute(
                        path=route.path,
                        methods={method},
                        handler=handler,
                        handler_id=id(handler),
                        is_websocket=False,
                        guards=handler.resolve_guards(),
                        dependencies=handler.resolve_dependencies(),
                        handler_fn=handler.fn,
                    )
                )
        elif isinstance(route, WebSocketRoute):
            handler = route.route_handler
            routes.append(
                LitestarRoute(
                    path=route.path,
                    methods={"WEBSOCKET"},
                    handler=handler,
                    handler_id=id(handler),
                    is_websocket=True,
                    guards=handler.resolve_guards(),
                    dependencies=handler.resolve_dependencies(),
                    handler_fn=handler.fn,
                )
            )
        elif isinstance(route, ASGIRoute):
            # ASGI routes (like static files) are opaque to us.
            # We treat them as fallback candidates but we still capture them if we want to detect static files.
            # For now, we mark them is_asgi=True.
            handler = route.route_handler
            routes.append(
                LitestarRoute(
                    path=route.path,
                    methods={"GET", "HEAD"},  # Typically ASGIRoutes are for GET/HEAD but could be anything
                    handler=handler,
                    handler_id=id(handler),
                    is_websocket=False,
                    is_asgi=True,
                    handler_fn=getattr(handler, "fn", None),
                )
            )

    return routes


def reflect_static_mounts(app: Litestar) -> dict[str, str]:
    """Reflect static file mounts from a Litestar application.

    Returns a dict of {path_prefix: directory_path}.
    """

    mounts = {}

    # This is a heuristic. Litestar stores static handlers in the route tree.
    # We look for ASGIRoutes that look like static file handlers.
    # Actually, Litestar 2.x often handles static files via `static_files_config`.

    # Check app.static_files_config
    if hasattr(app, "static_files_config"):
        # It's a list of StaticFilesConfig
        for config in app.static_files_config:
            for dir_path in config.directories:
                # config.path is the url path
                mounts[config.path] = str(dir_path)

    return mounts


from enum import Enum, auto


class RouteCapability(Enum):
    NATIVE = auto()
    ASGI_FALLBACK = auto()


def classify_route(route: LitestarRoute) -> RouteCapability:
    """Classify a route's capability to be served via the native pipeline.

    Returns:
        RouteCapability: NATIVE if safe for direct dispatch, ASGI_FALLBACK otherwise.
    """
    import inspect
    from typing import Annotated, get_args, get_origin

    from litestar.di import Provide
    from litestar.enums import RequestEncodingType
    from litestar.params import BodyKwarg
    from litestar.response import File, Redirect, Stream, Template

    # 1. ASGI Routes and WebSockets are fallback for now (until WS supported)
    if route.is_asgi or route.is_websocket:
        return RouteCapability.ASGI_FALLBACK

    # 2. Check dependencies for unsupported types
    # Unsupported: 'Dependency()' objects (dynamic), 'Inject' markers from extensions
    for dep in route.dependencies.values():
        if not isinstance(dep, Provide):
            # If it's not a Provide (e.g. it's a Dependency object or other), fallback
            return RouteCapability.ASGI_FALLBACK

    # 3. Check for specific parameter types that imply unsupported bodies (Multipart)
    # We inspect the handler object (HTTPRouteHandler)
    handler = route.handler
    # Check resolved_data_dto or logic? Litestar handlers usually have 'media_type' or 'encoding' in Body.
    # We can check handler.resolve_request_encoding()? No, that's internal.
    # But we can inspect the function parameters for Body(media_type=RequestEncodingType.MULTI_PART)

    fn = route.handler_fn or handler.fn
    sig = inspect.signature(fn)

    for param in sig.parameters.values():
        # Check for Body info in metadata
        # Litestar uses Annotated[T, Body(...)]
        # Note: Annotated is special, get_origin returns Annotated itself (if imported from typing)
        if get_origin(param.annotation) is Annotated:
            args = get_args(param.annotation)
            for arg in args:
                if isinstance(arg, BodyKwarg) and arg.media_type == RequestEncodingType.MULTI_PART:
                    return RouteCapability.ASGI_FALLBACK

        # Check for 'dependency' marker usage in params that wasn't caught by route.dependencies?
        # route.dependencies covers it usually.

    # 4. Check return annotations for unsupported response types
    # e.g. File, Stream, Template, InertiaRedirect (which inherits from Response or similar)
    ret = sig.return_annotation
    if ret is not inspect.Signature.empty:
        # Check if return type is a class that we fallback for
        # We unwrap generic Response[T] -> Response
        origin = get_origin(ret)
        if origin:
            if issubclass(origin, (File, Stream, Template, Redirect)):
                return RouteCapability.ASGI_FALLBACK
        elif isinstance(ret, type):
            if issubclass(ret, (File, Stream, Template, Redirect)):
                return RouteCapability.ASGI_FALLBACK
            # Check for InertiaRedirect by name if we don't import it?
            if ret.__name__ == "InertiaRedirect":
                return RouteCapability.ASGI_FALLBACK

    # 5. Check handler module for built-in static handlers
    # Litestar's static file handler is usually in litestar.static_files or similar
    if fn.__module__.startswith("litestar.static_files"):
        return RouteCapability.ASGI_FALLBACK

    return RouteCapability.NATIVE


def build_router(routes: list[LitestarRoute]) -> Any:
    """Build a native Rust router from reflected routes.

    Returns a vertebra._vertebra.Router instance.
    """
    from vertebra._vertebra import Router

    router = Router()
    for route in routes:
        # Filter unsupported routes
        if classify_route(route) != RouteCapability.NATIVE:
            continue

        # Convert path from Litestar format to matchit format
        # Litestar uses {param:type}, matchit uses :param
        path = route.path
        import re

        # Simple regex to convert {param} or {param:type} to :param
        path = re.sub(r"\{([^:]+)(:[^}]+)?\}", r":\1", path)

        # We prefix the path with the method to support multiple methods on the same path
        for method in route.methods:
            router.insert(f"/{method}{path}", route.handler_id)

    return router
