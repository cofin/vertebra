"""Route collection for frontend codegen."""

from typing import Any

from vertebra.builder._codegen._metadata import RouteMetadata
from vertebra.framework._app import VertebraApp


class RouteCollector:
    """Collects route metadata from a Vertebra application."""

    def collect(self, app: Any) -> list[RouteMetadata]:
        """Discover all routes in the given application object."""
        if not isinstance(app, VertebraApp):
            # If it's a raw App or other ASGI app, we might need a different strategy.
            # For now, focus on VertebraApp.
            return []

        routes: list[RouteMetadata] = []
        for handler_id, info in app.handler_index.items():
            handler = app._handlers[handler_id]
            # In a real implementation, we'd use TypeExtractor here to get
            # params and return type from the handler signature.
            # For now, create basic metadata.
            from vertebra.builder._codegen._extractor import TypeExtractor

            extractor = TypeExtractor()

            params, return_type = extractor.extract(handler)

            routes.append(
                RouteMetadata(
                    operation_id=info.handler_name,
                    path=info.path,
                    method=info.method,
                    handler_name=info.handler_name,
                    params=params,
                    return_type=return_type,
                )
            )

        return routes


__all__ = ("RouteCollector",)
