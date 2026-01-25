from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
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

    __slots__ = ("_headers", "_json", "_message", "_query_params", "_user")

    def __init__(self, message: dict[str, Any]) -> None:
        self._message = message
        self._json = None
        self._headers = None
        self._query_params = None
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
    def query_params(self) -> dict[str, list[str]]:
        if self._query_params is None:
            from urllib.parse import parse_qs

            query = self._message.get("query")
            if not query:
                raw_query = self._message.get("queryString")
                query = raw_query.decode() if isinstance(raw_query, (bytes, bytearray)) else ""
            self._query_params = parse_qs(str(query), keep_blank_values=True)
        return self._query_params

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

                self._user = LazyUserProxy(VertebraUserMixin, claims, noop_fetcher, context=self)
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

    mounts: dict[str, str] = {}

    # Prefer the internal config attribute to avoid deprecated property access.
    static_configs = getattr(app, "_static_files_config", None)
    if static_configs:
        for config in static_configs:
            for dir_path in config.directories:
                mounts[config.path] = str(dir_path)

    def add_mount(route_path: str, directories: list[Any]) -> None:
        if not directories:
            return
        mount_path = route_path
        if "/{file_path:path}" in mount_path:
            mount_path = mount_path.split("/{file_path:path}")[0]
        mount_path = mount_path.rstrip("/") or "/"
        if mount_path in mounts:
            return
        for dir_path in directories:
            mounts[mount_path] = str(dir_path)

    # Fallback: infer from create_static_files_router handlers in the route tree.
    from litestar.routes import HTTPRoute

    for route in app.routes:
        if not isinstance(route, HTTPRoute):
            continue
        for handler_tuple in route.route_handler_map.values():
            handler = handler_tuple[0]
            fn = getattr(handler, "fn", None)
            qualname = getattr(fn, "__qualname__", "")
            if not fn or not qualname.startswith("create_static_files_router.<locals>."):
                continue
            directories = []
            for cell in fn.__closure__ or ():
                cell_value = cell.cell_contents
                cell_dirs = getattr(cell_value, "directories", None)
                if cell_dirs:
                    directories = list(cell_dirs)
                    break
            add_mount(route.path, directories)

    return mounts


class RouteCapability(Enum):
    NATIVE = auto()
    ASGI_FALLBACK = auto()


def classify_route(route: LitestarRoute) -> RouteCapability:  # noqa: C901
    """Classify a route's capability to be served via the native pipeline.

    Returns:
        RouteCapability: NATIVE if safe for direct dispatch, ASGI_FALLBACK otherwise.
    """
    import inspect
    import types
    from collections.abc import Iterable
    from typing import Annotated, Union, get_args, get_origin

    from litestar.datastructures import UploadFile
    from litestar.di import Provide
    from litestar.enums import RequestEncodingType
    from litestar.params import BodyKwarg
    from litestar.response import File, Redirect, Stream, Template

    def is_multipart_media(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, RequestEncodingType):
            return value == RequestEncodingType.MULTI_PART
        if isinstance(value, str):
            return "multipart/form-data" in value.lower()
        # Enum-like objects
        enum_value = getattr(value, "value", None)
        return bool(isinstance(enum_value, str) and "multipart/form-data" in enum_value.lower())

    def unwrap_annotation(annotation: Any) -> Any:
        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            if args:
                return args[0]
        return annotation

    def contains_upload_file(annotation: Any) -> bool:
        annotation = unwrap_annotation(annotation)
        if annotation is inspect.Signature.empty:
            return False
        if isinstance(annotation, type) and issubclass(annotation, UploadFile):
            return True
        origin = get_origin(annotation)
        if origin in (list, set, tuple, Iterable):
            return any(contains_upload_file(arg) for arg in get_args(annotation))
        if origin in (Union, types.UnionType):
            return any(contains_upload_file(arg) for arg in get_args(annotation))
        if origin is None:
            return False
        return any(contains_upload_file(arg) for arg in get_args(annotation))

    def return_falls_back(ret: Any) -> bool:
        ret = unwrap_annotation(ret)
        if ret is inspect.Signature.empty:
            return False
        origin = get_origin(ret)
        if origin in (Union, types.UnionType):
            return any(return_falls_back(arg) for arg in get_args(ret))
        if isinstance(origin, type) and issubclass(origin, (File, Stream, Template, Redirect)):
            return True
        if isinstance(ret, type) and issubclass(ret, (File, Stream, Template, Redirect)):
            return True
        return bool(isinstance(ret, type) and ret.__name__ == "InertiaRedirect")

    # 1. ASGI Routes and WebSockets are fallback for now (until WS supported)
    if route.is_asgi or route.is_websocket:
        return RouteCapability.ASGI_FALLBACK

    # 2. Check dependencies for unsupported types
    # Unsupported: 'Dependency()' objects (dynamic), 'Inject' markers from extensions
    for dep in route.dependencies.values():
        if not isinstance(dep, Provide):
            # If it's not a Provide (e.g. it's a Dependency object or other), fallback
            return RouteCapability.ASGI_FALLBACK

    # 3. Check for multipart signals on handler or parameters
    handler = route.handler
    fn = route.handler_fn or handler.fn
    sig = inspect.signature(fn)

    handler_media = [
        getattr(handler, "media_type", None),
        getattr(handler, "request_encoding", None),
        getattr(handler, "request_media_type", None),
        getattr(handler, "content_type", None),
    ]
    if any(is_multipart_media(value) for value in handler_media):
        return RouteCapability.ASGI_FALLBACK

    for param in sig.parameters.values():
        if get_origin(param.annotation) is Annotated:
            args = get_args(param.annotation)
            for arg in args:
                if isinstance(arg, BodyKwarg) and is_multipart_media(arg.media_type):
                    return RouteCapability.ASGI_FALLBACK
        if contains_upload_file(param.annotation):
            return RouteCapability.ASGI_FALLBACK

    # 4. Check return annotations for unsupported response types
    # e.g. File, Stream, Template, InertiaRedirect (which inherits from Response or similar)
    ret = sig.return_annotation
    if return_falls_back(ret):
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
