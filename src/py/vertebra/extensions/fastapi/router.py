"""FastAPI APIRoute extraction and typed path parameter conversion for Rust matchit 0.9."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgspec

from vertebra.server._routing import (
    convert_path_params,
    make_handler_id,
    normalize_path_for_matchit,
)

__all__ = (
    "ExtractedRoute",
    "RouteCompiler",
    "RouterTable",
    "convert_path_params",
    "extract_route_entries",
    "extract_routes",
    "normalize_path_for_matchit",
)

_SEGMENT_PARAM_RE = re.compile(r"^\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([a-zA-Z0-9_]+))?\}$")
_ANNOTATION_TYPE_NAMES: dict[Any, str] = {
    int: "int",
    float: "float",
    UUID: "uuid",
    Decimal: "decimal",
    date: "date",
    datetime: "datetime",
    time: "time",
    timedelta: "timedelta",
}


class ExtractedRoute(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pre-compiled FastAPI APIRoute descriptor indexed by stable uint64 handler_id."""

    handler_id: int
    method: str
    path_template: str
    matchit_path: str
    route: Any
    endpoint: Any
    dependant: Any
    body_field: Any
    response_field: Any
    status_code: int
    name: str | None
    param_types: dict[str, str]
    regex_pattern: re.Pattern[str]


class RouterTable:
    """Lookup table mapping Rust handler_id and (method, path) to FastAPI ExtractedRoute."""

    __slots__ = ("_by_method", "routes")

    def __init__(self, routes: dict[int, ExtractedRoute]) -> None:
        self.routes = routes
        by_method: dict[str, list[ExtractedRoute]] = {}
        for entry in routes.values():
            by_method.setdefault(entry.method, []).append(entry)
        self._by_method = by_method

    def get_by_handler_id(self, handler_id: int) -> ExtractedRoute | None:
        """Look up an extracted route by its stable uint64 handler_id."""
        return self.routes.get(handler_id)

    def match(self, method: str, path: str) -> ExtractedRoute | None:
        """Match a request method and path to an ExtractedRoute."""
        for entry in self._by_method.get(method.upper(), ()):
            if entry.regex_pattern.match(path) is not None:
                return entry
        return None

    def match_with_params(self, method: str, path: str) -> tuple[ExtractedRoute, dict[str, Any]] | None:
        """Match a request method and path and return the route entry with converted path parameters."""
        for entry in self._by_method.get(method.upper(), ()):
            matched = entry.regex_pattern.match(path)
            if matched is not None:
                raw_groups = matched.groupdict()
                params = convert_path_params(raw_groups, entry.param_types) if raw_groups else {}
                return entry, params
        return None

    def allowed_methods(self, path: str) -> list[str]:
        """Return a sorted list of HTTP methods matching path for 405 Allow generation."""
        allowed: set[str] = set()
        for method, entries in self._by_method.items():
            for entry in entries:
                if entry.regex_pattern.match(path) is not None:
                    allowed.add(method)
                    break
        return sorted(allowed)

    def convert_params(
        self,
        route_entry: ExtractedRoute,
        raw_params: dict[str, str],
    ) -> dict[str, Any]:
        """Convert raw string path parameters using the route's pre-compiled converters."""
        return convert_path_params(raw_params, route_entry.param_types)


class RouteCompiler:
    """Compiler extracting FastAPI APIRoute and Mount trees into a RouterTable."""

    __slots__ = ()

    def compile(self, app: Any) -> RouterTable:
        """Compile all HTTP routes on the given FastAPI application into a RouterTable."""
        return extract_routes(app)

    def extract_entries(self, app: Any) -> list[ExtractedRoute]:
        """Return all extracted route descriptors as a list."""
        return extract_route_entries(app)


def extract_routes(app: Any) -> RouterTable:
    """Recursively extract all HTTP routes from a FastAPI application or APIRouter."""
    extracted: dict[int, ExtractedRoute] = {}
    raw_routes = getattr(app, "routes", None)
    if raw_routes is None and hasattr(app, "router"):
        raw_routes = getattr(app.router, "routes", ())
    _walk_fastapi_routes(raw_routes or (), prefix="", out=extracted)
    return RouterTable(extracted)


def extract_route_entries(app: Any) -> list[ExtractedRoute]:
    """Return a flat list of ExtractedRoute descriptors from a FastAPI app."""
    return list(extract_routes(app).routes.values())


def _join_mount_path(prefix: str, path: str) -> str:
    clean_prefix = prefix.rstrip("/")
    clean_path = path if path.startswith("/") else f"/{path}"
    if not clean_prefix:
        return clean_path
    if clean_path == "/":
        return clean_prefix or "/"
    return f"{clean_prefix}{clean_path}"


def _extract_param_types_from_path(path: str, dependant: Any = None) -> dict[str, str]:
    param_types: dict[str, str] = {}
    for segment in path.split("/"):
        matched = _SEGMENT_PARAM_RE.match(segment)
        if matched is not None:
            param_types[matched.group(1)] = matched.group(2) or "str"
    for field in getattr(dependant, "path_params", None) or ():
        name = getattr(field, "name", None)
        if not isinstance(name, str):
            continue
        if param_types.get(name, "str") != "str":
            continue
        ann = getattr(field, "type_", None) or getattr(getattr(field, "field_info", None), "annotation", None)
        type_name = _ANNOTATION_TYPE_NAMES.get(ann)
        if type_name is not None:
            param_types[name] = type_name
    return param_types


def _compile_match_regex(path_template: str) -> re.Pattern[str]:
    parts: list[str] = []
    for segment in path_template.split("/"):
        matched = _SEGMENT_PARAM_RE.match(segment)
        if matched is None:
            parts.append(re.escape(segment))
            continue
        param_name, param_type = matched.group(1), matched.group(2) or "str"
        if param_type == "path":
            parts.append(f"(?P<{param_name}>.+)")
        else:
            parts.append(f"(?P<{param_name}>[^/]+)")
    pattern = "/".join(parts) or "/"
    return re.compile(f"^{pattern}$")


def _walk_fastapi_routes(routes: Any, *, prefix: str, out: dict[int, ExtractedRoute]) -> None:
    for route in routes:
        cls_name = type(route).__name__
        route_path = getattr(route, "path", None)
        sub_routes = getattr(route, "routes", None)
        if sub_routes is not None and isinstance(route_path, str):
            mounted_prefix = _join_mount_path(prefix, route_path)
            _walk_fastapi_routes(sub_routes, prefix=mounted_prefix, out=out)
            continue

        if cls_name in {"WebSocketRoute", "APIWebSocketRoute"}:
            continue

        endpoint = getattr(route, "endpoint", None)
        methods = getattr(route, "methods", None)
        if not isinstance(route_path, str) or endpoint is None:
            continue

        full_path = _join_mount_path(prefix, route_path)
        matchit_path = normalize_path_for_matchit(full_path)
        dependant = getattr(route, "dependant", None)
        param_types = _extract_param_types_from_path(full_path, dependant=dependant)
        regex_pattern = _compile_match_regex(full_path)
        route_name = getattr(route, "name", None)
        body_field = getattr(route, "body_field", None)
        response_field = getattr(route, "secure_cloned_response_field", None) or getattr(route, "response_field", None)
        raw_status = getattr(route, "status_code", None)
        status_code = int(raw_status) if isinstance(raw_status, int) else 200

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
                dependant=dependant,
                body_field=body_field,
                response_field=response_field,
                status_code=status_code,
                name=route_name if isinstance(route_name, str) else None,
                param_types=param_types,
                regex_pattern=regex_pattern,
            )
