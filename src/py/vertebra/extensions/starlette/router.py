"""Starlette route extraction and typed path parameter conversion for Rust matchit 0.9."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.server._routing import (
    PARAM_CONVERTERS,
    BaseRouteCompiler,
    _compile_match_regex,
    _extract_param_types_from_path,
    convert_path_params,
    make_handler_id,
    normalize_path_for_matchit,
)
from vertebra.server._routing import ExtractedRoute as BaseExtractedRoute
from vertebra.server._routing import RouterTable as BaseRouterTable

if TYPE_CHECKING:
    import re

__all__ = (
    "PARAM_CONVERTERS",
    "ExtractedRoute",
    "RouteCompiler",
    "RouterTable",
    "convert_path_params",
    "extract_route_entries",
    "extract_routes",
    "normalize_path_for_matchit",
)


class ExtractedRoute(BaseExtractedRoute, frozen=True, gc=False, kw_only=True):
    """Pre-compiled Starlette route descriptor indexed by stable uint64 handler_id."""

    route: Any = None
    endpoint: Any = None


class RouterTable(BaseRouterTable):
    """Lookup table mapping Rust handler_id and (method, path) to ExtractedRoute."""

    __slots__ = ()

    def get_by_handler_id(self, handler_id: int) -> ExtractedRoute | None:
        """Look up an extracted route by its stable uint64 handler_id."""
        entry = super().get_by_handler_id(handler_id)
        return entry if isinstance(entry, ExtractedRoute) else None

    def match(self, method: str, path: str) -> ExtractedRoute | None:
        """Match a request method and path to an ExtractedRoute."""
        entry = super().match(method, path)
        return entry if isinstance(entry, ExtractedRoute) else None

    def match_with_params(self, method: str, path: str) -> tuple[ExtractedRoute, dict[str, Any]] | None:
        """Match a request method and path and return the route entry with converted path parameters."""
        matched = super().match_with_params(method, path)
        if matched is None:
            return None
        entry, params = matched
        if isinstance(entry, ExtractedRoute):
            return entry, params
        return None


class RouteCompiler(BaseRouteCompiler):
    """Compiler extracting Starlette Route and Mount trees into a RouterTable."""

    __slots__ = ()

    def compile(self, app: Any) -> RouterTable:
        """Compile all HTTP routes on the given Starlette application into a RouterTable."""
        return extract_routes(app)

    def extract_entries(self, app: Any) -> list[ExtractedRoute]:
        """Return all extracted route descriptors as a list."""
        return extract_route_entries(app)


def extract_routes(app: Any) -> RouterTable:
    """Recursively extract all HTTP routes from a Starlette application or router."""
    extracted: dict[int, ExtractedRoute] = {}
    raw_routes = getattr(app, "routes", None)
    if raw_routes is None and hasattr(app, "router"):
        raw_routes = getattr(app.router, "routes", ())
    _walk_routes(raw_routes or (), prefix="", out=extracted)
    return RouterTable(extracted)


def extract_route_entries(app: Any) -> list[ExtractedRoute]:
    """Return a flat list of ExtractedRoute descriptors from a Starlette app."""
    return list(extract_routes(app).routes.values())


def _join_mount_path(prefix: str, path: str) -> str:
    clean_prefix = prefix.rstrip("/")
    clean_path = path if path.startswith("/") else f"/{path}"
    if not clean_prefix:
        return clean_path
    if clean_path == "/":
        return clean_prefix or "/"
    return f"{clean_prefix}{clean_path}"


def _walk_routes(routes: Any, *, prefix: str, out: dict[int, ExtractedRoute]) -> None:
    for route in routes:
        cls_name = type(route).__name__
        route_path = getattr(route, "path", None)
        sub_routes = getattr(route, "routes", None)
        if sub_routes is not None and isinstance(route_path, str):
            mounted_prefix = _join_mount_path(prefix, route_path)
            _walk_routes(sub_routes, prefix=mounted_prefix, out=out)
            continue

        if cls_name == "WebSocketRoute":
            continue

        endpoint = getattr(route, "endpoint", None)
        methods = getattr(route, "methods", None)
        if not isinstance(route_path, str) or endpoint is None:
            continue

        full_path = _join_mount_path(prefix, route_path)
        matchit_path = normalize_path_for_matchit(full_path)
        param_types = _extract_param_types_from_path(full_path)
        regex_pattern: re.Pattern[str] = _compile_match_regex(full_path)
        route_name = getattr(route, "name", None)

        resolved_methods = tuple(str(m).upper() for m in methods) if methods else ("GET", "HEAD")
        for method_upper in resolved_methods:
            handler_id = make_handler_id(method_upper, matchit_path)
            out[handler_id] = ExtractedRoute(
                handler_id=handler_id,
                method=method_upper,
                path_template=full_path,
                matchit_path=matchit_path,
                route=route,
                endpoint=endpoint,
                handler=endpoint,
                name=route_name if isinstance(route_name, str) else None,
                param_types=param_types,
                regex_pattern=regex_pattern,
            )
