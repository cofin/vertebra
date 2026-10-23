"""Litestar v2 and v3 route extraction and typed path parameter conversion for Rust matchit 0.9."""

import re
from typing import Any

import msgspec

from vertebra.framework._router import PARAM_CONVERTERS, _make_handler_id

__all__ = (
    "ExtractedLitestarRoute",
    "LitestarRouteCompiler",
    "LitestarRouterTable",
    "_unpack_route_handler_map_entry",
    "convert_litestar_path_params",
    "extract_litestar_route_entries",
    "extract_litestar_routes",
    "normalize_litestar_path_for_matchit",
)


class ExtractedLitestarRoute(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pre-compiled Litestar route entry indexed by stable uint64 handler_id."""

    handler_id: int
    method: str
    path_template: str
    matchit_path: str
    route: Any
    route_handler: Any
    kwargs_model: Any
    param_types: dict[str, str]
    regex_pattern: re.Pattern[str]


class LitestarRouterTable:
    """Lookup table mapping Rust handler_id and (method, path) to ExtractedLitestarRoute."""

    __slots__ = ("_by_method", "routes")

    def __init__(self, routes: dict[int, ExtractedLitestarRoute]) -> None:
        self.routes = routes
        by_method: dict[str, list[ExtractedLitestarRoute]] = {}
        for entry in routes.values():
            by_method.setdefault(entry.method, []).append(entry)
        self._by_method = by_method

    def get_by_handler_id(self, handler_id: int) -> ExtractedLitestarRoute | None:
        """Look up an extracted route by its stable uint64 handler_id."""
        return self.routes.get(handler_id)

    def match(self, method: str, path: str) -> ExtractedLitestarRoute | None:
        """Match a request method and path to an ExtractedLitestarRoute."""
        for entry in self._by_method.get(method.upper(), ()):
            if entry.regex_pattern.match(path) is not None:
                return entry
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
        route_entry: ExtractedLitestarRoute,
        raw_params: dict[str, str],
    ) -> dict[str, Any]:
        """Convert raw string path parameters using the route's pre-compiled converters."""
        return convert_litestar_path_params(raw_params, route_entry.param_types)


class LitestarRouteCompiler:
    """Compiler extracting Litestar v2 and v3 route handlers into a LitestarRouterTable."""

    __slots__ = ()

    def compile(self, app: Any) -> LitestarRouterTable:
        """Compile all HTTP routes on the given Litestar application into a LitestarRouterTable."""
        return extract_litestar_routes(app)

    def extract_entries(self, app: Any) -> list[ExtractedLitestarRoute]:
        """Return all extracted route descriptors as a list."""
        return extract_litestar_route_entries(app)


def normalize_litestar_path_for_matchit(path: str) -> str:
    """Convert a Litestar path template into a matchit 0.9 route pattern."""
    if "{" not in path:
        return path
    segments: list[str] = []
    for segment in path.split("/"):
        matched = _SEGMENT_PARAM_RE.match(segment)
        if matched is None:
            segments.append(segment)
            continue
        param_name, param_type = matched.group(1), matched.group(2) or "str"
        if param_type == "path":
            segments.append(f"{{*{param_name}}}")
        else:
            segments.append(f"{{{param_name}}}")
    return "/".join(segments)


def convert_litestar_path_params(
    raw_params: dict[str, str],
    param_types: dict[str, str],
) -> dict[str, Any]:
    """Convert raw string path parameters extracted by matchit into typed Python objects."""
    converted: dict[str, Any] = {}
    for name, raw_val in raw_params.items():
        type_name = param_types.get(name, "str")
        converter = PARAM_CONVERTERS.get(type_name, str)
        converted[name] = converter(raw_val)
    return converted


def extract_litestar_routes(app: Any) -> LitestarRouterTable:
    """Extract all HTTP routes from a Litestar v2 or v3 app by inspecting app.routes."""
    extracted: dict[int, ExtractedLitestarRoute] = {}
    for route in getattr(app, "routes", ()):
        path_template = getattr(route, "path", None)
        handler_map = getattr(route, "route_handler_map", None)
        if not isinstance(path_template, str) or not isinstance(handler_map, dict):
            continue
        matchit_path = normalize_litestar_path_for_matchit(path_template)
        param_types = _extract_param_types_from_path(path_template)
        regex_pattern = _compile_match_regex(path_template)

        for method, raw_entry in handler_map.items():
            method_upper = str(method).upper()
            route_handler, kwargs_model = _unpack_route_handler_map_entry(raw_entry)
            if kwargs_model is None and hasattr(route_handler, "_get_kwargs_model_for_route"):
                path_parameters = getattr(route, "path_parameters", ())
                kwargs_model = route_handler._get_kwargs_model_for_route(path_parameters)
            handler_id = _make_handler_id(method_upper, matchit_path)
            extracted[handler_id] = ExtractedLitestarRoute(
                handler_id=handler_id,
                method=method_upper,
                path_template=path_template,
                matchit_path=matchit_path,
                route=route,
                route_handler=route_handler,
                kwargs_model=kwargs_model,
                param_types=param_types,
                regex_pattern=regex_pattern,
            )
    return LitestarRouterTable(extracted)


def extract_litestar_route_entries(app: Any) -> list[ExtractedLitestarRoute]:
    """Return a flat list of ExtractedLitestarRoute descriptors from a Litestar v2 or v3 app."""
    return list(extract_litestar_routes(app).routes.values())


_SEGMENT_PARAM_RE = re.compile(r"^\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([a-zA-Z0-9_]+))?\}$")


def _unpack_route_handler_map_entry(entry: Any) -> tuple[Any, Any]:
    return entry if isinstance(entry, tuple) else (entry, None)


def _extract_param_types_from_path(path: str) -> dict[str, str]:
    param_types: dict[str, str] = {}
    for segment in path.split("/"):
        matched = _SEGMENT_PARAM_RE.match(segment)
        if matched is not None:
            param_types[matched.group(1)] = matched.group(2) or "str"
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
