"""Manifest generation for route-driven frontend codegen."""

from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.builder._codegen._metadata import RouteMetadata
from vertebra.utils.serializers._json import encode_json

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ("RouteManifest",)


class RouteManifest:
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
        return msgspec.json.format(encode_json(payload, as_bytes=True), indent=2).decode("utf-8")
