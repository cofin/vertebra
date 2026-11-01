"""Manifest generation for route-driven frontend codegen across HTTP and WebSocket routes."""

from typing import TYPE_CHECKING, Any

from vertebra.build.codegen._metadata import RouteMetadata
from vertebra.utils.serializers import format_json

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ("ManifestGenerator",)


class ManifestGenerator:
    """Generate a JSON manifest from collected HTTP and WebSocket routes."""

    def generate(self, routes: "Iterable[RouteMetadata]") -> str:
        """Render the route manifest as formatted JSON."""
        route_entries: dict[str, dict[str, Any]] = {}
        ws_entries: dict[str, dict[str, Any]] = {}
        for route in sorted(routes, key=lambda r: r.operation_id):
            safe_id = route.operation_id.replace(".", "_")
            entry: dict[str, Any] = {
                "handlerName": route.handler_name,
                "method": route.method,
                "paramsSchema": f"{safe_id}Params",
                "procedureId": route.operation_id,
                "protocol": route.protocol,
                "responseSchema": f"{safe_id}Response",
                "route": route.path,
            }
            if route.channel_key is not None:
                entry["channelKey"] = route.channel_key
            if route.protocol == "websocket":
                ws_key = route.channel_key or route.operation_id
                ws_entries[ws_key] = entry
            route_entries[route.operation_id] = entry
        payload: dict[str, Any] = {"routes": route_entries, "version": 1}
        if ws_entries:
            payload["websocketRoutes"] = ws_entries
        return format_json(payload, indent=2)
