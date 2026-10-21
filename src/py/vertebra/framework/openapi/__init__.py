"""OpenAPI 3.1.0 specification generator and Scalar UI configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import msgspec

__all__ = (
    "OpenAPIConfig",
    "OpenAPISpecBuilder",
    "ScalarDocsConfig",
)


@dataclass(frozen=True, slots=True)
class ScalarDocsConfig:
    """Scalar interactive API documentation UI configuration."""

    path: str = "/docs"
    theme: str = "kepler"
    dark_mode: bool = True


@dataclass(frozen=True, slots=True)
class OpenAPIConfig:
    """OpenAPI 3.1.0 document configuration."""

    title: str = "Vertebra API"
    version: str = "0.1.0"
    description: str = ""
    openapi_version: str = "3.1.0"
    schema_path: str = "/openapi.json"
    scalar: ScalarDocsConfig = field(default_factory=ScalarDocsConfig)


class OpenAPISpecBuilder:
    """Constructs OpenAPI 3.1.0 documents from route metadata and ``msgspec.Struct`` types."""

    def __init__(self, config: OpenAPIConfig | None = None) -> None:
        self._config = config or OpenAPIConfig()
        self._paths: dict[str, dict[str, Any]] = {}
        self._schemas: dict[str, dict[str, Any]] = {}

    @property
    def config(self) -> OpenAPIConfig:
        """Return the ``OpenAPIConfig``."""
        return self._config

    def add_operation(
        self,
        path: str,
        method: str,
        *,
        summary: str = "",
        operation_id: str | None = None,
        response_schema: type[Any] | None = None,
        tags: tuple[str, ...] = (),
    ) -> None:
        """Register an HTTP operation in the OpenAPI 3.1.0 path map."""
        method_lower = method.lower()
        op: dict[str, Any] = {
            "summary": summary,
            "operationId": operation_id or f"{method_lower}_{path.strip('/').replace('/', '_') or 'root'}",
            "responses": {"200": {"description": "Successful Response"}},
        }
        if tags:
            op["tags"] = list(tags)
        if response_schema is not None:
            schema_name = getattr(response_schema, "__name__", "ResponseModel")
            _, components = msgspec.json.schema_components((response_schema,))
            self._schemas.update(components)
            op["responses"]["200"]["content"] = {
                "application/json": {"schema": {"$ref": f"#/components/schemas/{schema_name}"}}
            }
        self._paths.setdefault(path, {})[method_lower] = op

    def build(self) -> dict[str, Any]:
        """Build and return the complete OpenAPI 3.1.0 specification dictionary."""
        doc: dict[str, Any] = {
            "openapi": self._config.openapi_version,
            "info": {
                "title": self._config.title,
                "version": self._config.version,
                "description": self._config.description,
            },
            "paths": dict(self._paths),
        }
        if self._schemas:
            doc["components"] = {"schemas": dict(self._schemas)}
        return doc
