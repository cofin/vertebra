"""Manifest generation for route-driven frontend codegen."""

from typing import TYPE_CHECKING, Any

from vertebra.build.codegen._metadata import RouteMetadata
from vertebra.utils.serializers import format_json

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ("ManifestGenerator",)


class ManifestGenerator:
    """Generate a JSON manifest from collected routes."""

    def generate(self, routes: "Iterable[RouteMetadata]") -> str:
        """Render the route manifest as formatted JSON."""
        route_entries: dict[str, dict[str, Any]] = {}
        for route in sorted(routes, key=lambda r: r.operation_id):
            safe_id = route.operation_id.replace(".", "_")
            route_entries[route.operation_id] = {
                "handlerName": route.handler_name,
                "method": route.method,
                "paramsSchema": f"{safe_id}Params",
                "procedureId": route.operation_id,
                "responseSchema": f"{safe_id}Response",
                "route": route.path,
            }
        payload = {"routes": route_entries, "version": 1}
        return format_json(payload, indent=2)
