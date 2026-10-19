"""Route collection for frontend codegen."""

from typing import Any

from vertebra.builder._codegen._extractor import TypeExtractor
from vertebra.builder._codegen._metadata import RouteCollector as _MetadataCollector
from vertebra.builder._codegen._metadata import RouteMetadata
from vertebra.framework._app import VertebraApp

__all__ = ("RouteCollector",)


class RouteCollector:
    """Collect route metadata from a Vertebra application."""

    def collect(self, app: Any) -> list[RouteMetadata]:
        """Discover all routes in the given application object."""
        if not isinstance(app, VertebraApp):
            return []

        collector = _MetadataCollector(extractor=TypeExtractor())
        return collector.collect(app)
