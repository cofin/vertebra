"""Route metadata collection for frontend codegen."""

import inspect
from pathlib import Path
from typing import Any

import msgspec

from vertebra.build.codegen._extractor import TypeExtractor
from vertebra.build.codegen._ir import TypeNode, UnknownType
from vertebra.framework._app import App
from vertebra.framework._router import Route
from vertebra.framework.di import Container, inspect_callable_bindings

__all__ = ("RouteCollector", "RouteMetadata")


class RouteMetadata(msgspec.Struct, frozen=True, gc=False):
    """Codegen metadata derived from a registered HTTP route."""

    operation_id: str
    handler_fn: Any
    source_file: Path
    params: dict[str, TypeNode]
    return_type: TypeNode
    path: str
    method: str
    handler_name: str


class RouteCollector:
    """Collect route metadata from a Vertebra ``App`` instance."""

    def __init__(self, extractor: TypeExtractor | None = None) -> None:
        self._extractor = extractor or TypeExtractor()

    def collect(self, app: Any) -> list[RouteMetadata]:
        """Discover all routes in the given application object."""
        if not isinstance(app, App):
            return []
        return [self._from_route(route, app_container=app.container) for route in app.routes]

    def _from_route(self, route: Route, *, app_container: Container | None = None) -> RouteMetadata:
        handler = route.handler
        params, return_type = self._extractor.extract(handler)
        route_container = (
            app_container.create_child(route.dependencies)
            if app_container is not None
            else Container(route.dependencies)
        )
        excluded_names: set[str] = {"request", "req", "_controller_self"}
        for binding in inspect_callable_bindings(handler):
            if binding.is_explicit_inject:
                excluded_names.add(binding.name)
                continue
            dep_key = route_container.graph.resolve_param_key(binding)
            if dep_key is not None and dep_key in route_container.graph.providers:
                excluded_names.add(binding.name)

        filtered_params = {k: v for k, v in params.items() if k not in excluded_names}
        handler_name = getattr(handler, "__name__", getattr(handler, "__qualname__", "handler"))
        source_file = Path(inspect.getsourcefile(handler) or "<unknown>")

        return RouteMetadata(
            operation_id=self._operation_id(route.method, route.path),
            handler_fn=handler,
            source_file=source_file,
            params=filtered_params,
            return_type=return_type if not isinstance(return_type, UnknownType) else UnknownType(),
            path=route.path,
            method=route.method,
            handler_name=handler_name,
        )

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
            parts.append(segment.replace("-", "_"))
        if len(parts) == 1:
            parts.append("root")
        return ".".join(parts)
