"""Route collection for frontend codegen."""

from typing import Any

from vertebra.builder._codegen._extractor import TypeExtractor
from vertebra.builder._codegen._metadata import RouteMetadata
from vertebra.framework._app import VertebraApp


class RouteCollector:
    """Collects route metadata from a Vertebra application."""

    def collect(self, app: Any) -> list[RouteMetadata]:
        """Discover all routes in the given application object."""
        if not isinstance(app, VertebraApp):
            return []

        from vertebra.builder._codegen._metadata import RouteCollector as MetadataCollector

        collector = MetadataCollector(extractor=TypeExtractor())
        return collector.collect(app)


__all__ = ("RouteCollector",)
