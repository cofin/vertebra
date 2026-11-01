"""Route metadata collection for frontend codegen across HTTP and WebSocket routes."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Literal

import msgspec

from vertebra.build.codegen._extractor import TypeExtractor
from vertebra.build.codegen._ir import TypeNode, UnknownType
from vertebra.framework._app import App
from vertebra.framework._router import Route
from vertebra.framework.di import Container, inspect_callable_bindings

__all__ = (
    "ChannelKeyAllocator",
    "RouteCollector",
    "RouteMetadata",
)

_INTERNAL_WS_HANDLER_NAMES: frozenset[str] = frozenset(
    {
        "_vite_hmr_ws",
        "_vite_ssr_ws",
        "_channels_ws_handler",
        "vite_hmr",
        "channels_ws",
    }
)


class ChannelKeyAllocator:
    """Allocate deterministic, collision-free AsyncAPI/WebSocket channel keys."""

    __slots__ = ("_used",)

    def __init__(self) -> None:
        self._used: set[str] = set()

    def allocate(self, path: str, preferred_name: str | None = None) -> str:
        """Allocate a unique channel key from a preferred name or route path."""
        base = preferred_name or self._slug_from_path(path)
        candidate = base
        suffix = 2
        while candidate in self._used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        self._used.add(candidate)
        return candidate

    @staticmethod
    def _slug_from_path(path: str) -> str:
        segments: list[str] = []
        for raw in path.strip("/").split("/"):
            if not raw:
                continue
            if raw.startswith("{") and raw.endswith("}"):
                param_name = raw[1:-1].split(":", 1)[0].strip()
                segments.append(f"by_{param_name}")
            elif raw.startswith("<") and raw.endswith(">"):
                inner = raw[1:-1]
                param_name = inner.split(":", 1)[-1].strip()
                segments.append(f"by_{param_name}")
            else:
                segments.append(raw.replace("-", "_").replace(".", "_"))
        return "_".join(segments) if segments else "root"


class RouteMetadata(msgspec.Struct, frozen=True, gc=False):
    """Codegen metadata derived from a registered HTTP or WebSocket route."""

    operation_id: str
    handler_fn: Any
    source_file: Path
    params: dict[str, TypeNode]
    return_type: TypeNode
    path: str
    method: str
    handler_name: str
    protocol: Literal["http", "websocket"] = "http"
    channel_key: str | None = None
    receive_schema: TypeNode | None = None
    send_schema: TypeNode | None = None


class RouteCollector:
    """Collect HTTP and WebSocket route metadata across supported framework applications."""

    def __init__(self, extractor: TypeExtractor | None = None) -> None:
        self._extractor = extractor or TypeExtractor()

    def collect(self, app: Any) -> list[RouteMetadata]:
        """Discover all HTTP and WebSocket routes in the given application object."""
        allocator = ChannelKeyAllocator()
        if isinstance(app, App):
            return self._collect_vertebra(app, allocator)

        cls_name = type(app).__name__
        mod_name = type(app).__module__
        if cls_name == "Litestar" or mod_name.startswith("litestar"):
            return self._collect_litestar(app, allocator)
        if cls_name in ("FastAPI", "Starlette") or mod_name.startswith(("fastapi", "starlette")):
            return self._collect_asgi_routes(app, allocator)
        if cls_name == "Flask" or mod_name.startswith("flask"):
            return self._collect_flask(app, allocator)
        return []

    def _collect_vertebra(self, app: App, allocator: ChannelKeyAllocator) -> list[RouteMetadata]:
        collected: list[RouteMetadata] = []
        for route in app.routes:
            collected.append(self._from_route(route, app_container=app.container, allocator=allocator))
        ws_routes = getattr(app, "ws_routes", None) or getattr(app, "_ws_routes", None) or ()
        for ws_route in ws_routes:
            handler = getattr(ws_route, "handler", ws_route)
            handler_name = getattr(handler, "__name__", getattr(handler, "__qualname__", "ws_handler"))
            if handler_name in _INTERNAL_WS_HANDLER_NAMES:
                continue
            path = getattr(ws_route, "path", "/ws")
            collected.append(self._from_websocket_handler(path, handler, allocator=allocator))
        return collected

    def _collect_litestar(self, app: Any, allocator: ChannelKeyAllocator) -> list[RouteMetadata]:
        collected: list[RouteMetadata] = []
        routes = getattr(app, "routes", ()) or ()
        for route in routes:
            route_path = getattr(route, "path", "/")
            r_cls_name = type(route).__name__
            if r_cls_name == "WebSocketRoute":
                handler_obj = getattr(route, "route_handler", None)
                fn = getattr(handler_obj, "fn", handler_obj)
                fn_name = getattr(fn, "__name__", getattr(handler_obj, "name", "ws_handler"))
                opt = getattr(handler_obj, "opt", {}) or {}
                if fn_name in _INTERNAL_WS_HANDLER_NAMES or opt.get("exclude_from_schema") or opt.get("_internal_ws"):
                    continue
                collected.append(
                    self._from_websocket_handler(
                        route_path,
                        fn,
                        allocator=allocator,
                        preferred_name=getattr(handler_obj, "name", None),
                    )
                )
                continue

            handler_map = getattr(route, "route_handler_map", None)
            if isinstance(handler_map, dict):
                for method_key, handler_obj in handler_map.items():
                    method_str = str(getattr(method_key, "value", method_key)).upper()
                    if method_str == "HEAD":
                        continue
                    fn = getattr(handler_obj, "fn", handler_obj)
                    if method_str == "WEBSOCKET":
                        fn_name = getattr(fn, "__name__", "ws_handler")
                        if fn_name in _INTERNAL_WS_HANDLER_NAMES:
                            continue
                        collected.append(self._from_websocket_handler(route_path, fn, allocator=allocator))
                    else:
                        collected.append(self._from_generic_http_handler(method_str, route_path, fn))
        return collected

    def _collect_asgi_routes(self, app: Any, allocator: ChannelKeyAllocator) -> list[RouteMetadata]:
        collected: list[RouteMetadata] = []
        routes = getattr(app, "routes", ()) or ()
        for route in routes:
            r_cls = type(route).__name__
            path = getattr(route, "path", "/")
            endpoint = getattr(route, "endpoint", None)
            if endpoint is None:
                continue
            fn_name = getattr(endpoint, "__name__", "")
            if r_cls in ("WebSocketRoute", "APIWebSocketRoute"):
                if fn_name in _INTERNAL_WS_HANDLER_NAMES:
                    continue
                collected.append(
                    self._from_websocket_handler(
                        path,
                        endpoint,
                        allocator=allocator,
                        preferred_name=getattr(route, "name", None),
                    )
                )
            elif r_cls in ("Route", "APIRoute"):
                methods = getattr(route, "methods", None) or ("GET",)
                for m in sorted(methods):
                    m_up = str(m).upper()
                    if m_up in ("HEAD", "OPTIONS"):
                        continue
                    collected.append(self._from_generic_http_handler(m_up, path, endpoint))
        return collected

    def _collect_flask(self, app: Any, allocator: ChannelKeyAllocator) -> list[RouteMetadata]:
        _ = allocator
        collected: list[RouteMetadata] = []
        url_map = getattr(app, "url_map", None)
        view_functions = getattr(app, "view_functions", {}) or {}
        if url_map is None:
            return collected
        for rule in url_map.iter_rules():
            if rule.endpoint == "static":
                continue
            view_fn = view_functions.get(rule.endpoint)
            if view_fn is None:
                continue
            methods = sorted(m for m in (rule.methods or ()) if m not in ("HEAD", "OPTIONS"))
            for m in methods:
                collected.append(self._from_generic_http_handler(m, str(rule.rule), view_fn))
        return collected

    def _from_route(
        self,
        route: Route,
        *,
        app_container: Container | None = None,
        allocator: ChannelKeyAllocator | None = None,
    ) -> RouteMetadata:
        if route.method.upper() in ("WS", "WEBSOCKET"):
            return self._from_websocket_handler(
                route.path,
                route.handler,
                allocator=allocator or ChannelKeyAllocator(),
            )
        handler = route.handler
        params, return_type = self._extractor.extract(handler)
        route_container = (
            app_container.create_child(route.dependencies)
            if app_container is not None
            else Container(route.dependencies)
        )
        excluded_names: set[str] = {"request", "req", "_controller_self", "socket", "ws", "websocket"}
        for binding in inspect_callable_bindings(handler):
            if binding.is_explicit_inject:
                excluded_names.add(binding.name)
                continue
            dep_key = route_container.graph.resolve_param_key(binding)
            if dep_key is not None and dep_key in route_container.graph.providers:
                excluded_names.add(binding.name)

        filtered_params = {k: v for k, v in params.items() if k not in excluded_names}
        handler_name = getattr(handler, "__name__", getattr(handler, "__qualname__", "handler"))
        source_file = self._safe_source_file(handler)

        return RouteMetadata(
            operation_id=self._operation_id(route.method, route.path),
            handler_fn=handler,
            source_file=source_file,
            params=filtered_params,
            return_type=return_type if not isinstance(return_type, UnknownType) else UnknownType(),
            path=route.path,
            method=route.method,
            handler_name=handler_name,
            protocol="http",
        )

    def _from_generic_http_handler(self, method: str, path: str, handler: Any) -> RouteMetadata:
        params, return_type = self._extractor.extract(handler)
        excluded_names = {"request", "req", "self", "cls", "_controller_self", "socket", "ws", "websocket"}
        filtered_params = {k: v for k, v in params.items() if k not in excluded_names}
        handler_name = getattr(handler, "__name__", getattr(handler, "__qualname__", "handler"))
        return RouteMetadata(
            operation_id=self._operation_id(method, path),
            handler_fn=handler,
            source_file=self._safe_source_file(handler),
            params=filtered_params,
            return_type=return_type if not isinstance(return_type, UnknownType) else UnknownType(),
            path=path,
            method=method.upper(),
            handler_name=handler_name,
            protocol="http",
        )

    def _from_websocket_handler(
        self,
        path: str,
        handler: Any,
        *,
        allocator: ChannelKeyAllocator,
        preferred_name: str | None = None,
    ) -> RouteMetadata:
        params, return_type = self._extractor.extract(handler)
        excluded_names = {"request", "req", "self", "cls", "_controller_self", "socket", "ws", "websocket"}
        receive_schema: TypeNode | None = None
        for key in ("data", "payload", "message"):
            if key in params and not isinstance(params[key], UnknownType):
                receive_schema = params[key]
                excluded_names.add(key)
                break
        filtered_params = {k: v for k, v in params.items() if k not in excluded_names}
        send_schema: TypeNode | None = None if isinstance(return_type, UnknownType) else return_type
        handler_name = getattr(handler, "__name__", getattr(handler, "__qualname__", "ws_handler"))
        channel_key = allocator.allocate(path, preferred_name=preferred_name)
        return RouteMetadata(
            operation_id=self._operation_id("ws", path),
            handler_fn=handler,
            source_file=self._safe_source_file(handler),
            params=filtered_params,
            return_type=return_type,
            path=path,
            method="WEBSOCKET",
            handler_name=handler_name,
            protocol="websocket",
            channel_key=channel_key,
            receive_schema=receive_schema,
            send_schema=send_schema,
        )

    @staticmethod
    def _safe_source_file(handler: Any) -> Path:
        try:
            src = inspect.getsourcefile(handler)
            return Path(src) if src else Path("<unknown>")
        except TypeError:
            return Path("<unknown>")

    @staticmethod
    def _operation_id(method: str, path: str) -> str:
        parts = [method.lower()]
        for segment in path.strip("/").split("/"):
            if not segment:
                continue
            if segment.startswith("{") and segment.endswith("}"):
                name = segment[1:-1].split(":", 1)[0].strip()
                parts.append(f"by_{name}")
                continue
            if segment.startswith("<") and segment.endswith(">"):
                name = segment[1:-1].split(":", 1)[-1].strip()
                parts.append(f"by_{name}")
                continue
            parts.append(segment.replace("-", "_"))
        if len(parts) == 1:
            parts.append("root")
        return ".".join(parts)
