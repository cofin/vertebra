"""Litestar v2 and v3 route extraction and typed path parameter conversion for Rust matchit 0.9."""

from __future__ import annotations

from typing import Any

from vertebra.server._routing import (
    BaseRouteCompiler,
    RouterTable,
    _compile_match_regex,
    _extract_param_types_from_path,
    convert_path_params,
    make_handler_id,
    normalize_path_for_matchit,
)
from vertebra.server._routing import (
    ExtractedRoute as _BaseExtractedRoute,
)

__all__ = (
    "BaseRouteCompiler",
    "ExtractedRoute",
    "RouteCompiler",
    "RouterTable",
    "_unpack_route_handler_map_entry",
    "convert_path_params",
    "extract_route_entries",
    "extract_routes",
    "normalize_path_for_matchit",
)


class ExtractedRoute(_BaseExtractedRoute, frozen=True, gc=False, kw_only=True):
    """Pre-compiled Litestar route entry indexed by stable uint64 handler_id."""

    route: Any = None
    route_handler: Any = None
    kwargs_model: Any = None


class RouteCompiler(BaseRouteCompiler):
    """Compiler extracting Litestar v2 and v3 route handlers into a RouterTable."""

    __slots__ = ()

    def compile(self, app: Any) -> RouterTable:
        """Compile all HTTP routes on the given Litestar application into a RouterTable."""
        plugins = getattr(getattr(app, "plugins", None), "plugins", getattr(app, "plugins", ())) or ()
        for plugin in plugins:
            if type(plugin).__name__ == "AutowirePlugin" and not getattr(plugin, "_autowire_executed", False):
                on_init = getattr(plugin, "on_app_init", None)
                if callable(on_init) and hasattr(app, "route_handlers"):
                    plugin._autowire_executed = True
                    on_init(app)
        return extract_routes(app)

    def extract_entries(self, app: Any) -> list[ExtractedRoute]:
        """Return all extracted route descriptors as a list."""
        return extract_route_entries(app)


def extract_routes(app: Any) -> RouterTable:
    """Extract all HTTP routes from a Litestar v2 or v3 app by inspecting app.routes."""
    extracted: dict[int, ExtractedRoute] = {}
    for route in getattr(app, "routes", ()):
        path_template = getattr(route, "path", None)
        handler_map = getattr(route, "route_handler_map", None)
        if not isinstance(path_template, str) or not isinstance(handler_map, dict):
            continue
        matchit_path = normalize_path_for_matchit(path_template)
        param_types = _extract_param_types_from_path(path_template)
        regex_pattern = _compile_match_regex(path_template)

        for method, raw_entry in handler_map.items():
            method_upper = str(method).upper()
            route_handler, kwargs_model = _unpack_route_handler_map_entry(raw_entry)
            if kwargs_model is None and hasattr(route_handler, "_get_kwargs_model_for_route"):
                path_parameters = getattr(route, "path_parameters", ())
                kwargs_model = route_handler._get_kwargs_model_for_route(path_parameters)
            handler_id = make_handler_id(method_upper, matchit_path)
            extracted[handler_id] = ExtractedRoute(
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
    return RouterTable(extracted)


def extract_route_entries(app: Any) -> list[ExtractedRoute]:
    """Return a flat list of ExtractedRoute descriptors from a Litestar v2 or v3 app."""
    return list(extract_routes(app).routes.values())


def _unpack_route_handler_map_entry(entry: Any) -> tuple[Any, Any]:
    return entry if isinstance(entry, tuple) else (entry, None)
