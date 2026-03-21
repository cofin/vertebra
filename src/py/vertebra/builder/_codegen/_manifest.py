"""Manifest generation for route-driven frontend codegen."""

import json
from typing import TYPE_CHECKING, Any

from vertebra.builder._codegen._metadata import RouteMetadata

if TYPE_CHECKING:
    from collections.abc import Iterable


class RouteManifest:
    """Generate a JSON manifest from collected routes."""

    def generate(self, routes: "Iterable[RouteMetadata]") -> str:
        route_entries: dict[str, dict[str, Any]] = {}
        for route in routes:
            safe_id = route.operation_id.replace(".", "_")
            route_entries[route.operation_id] = {
                "procedureId": route.operation_id,
                "route": route.path,
                "method": route.method,
                "handlerName": route.handler_name,
                "paramsSchema": f"{safe_id}Params",
                "responseSchema": f"{safe_id}Response",
            }
        return json.dumps({"version": 1, "routes": route_entries}, indent=2, sort_keys=True)


__all__ = ("RouteManifest",)
