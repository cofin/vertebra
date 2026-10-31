"""OpenAPI 3.1.0 specification generator and Scalar UI configuration."""

from __future__ import annotations

import inspect
import typing
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.framework.di import inspect_callable_bindings
from vertebra.utils.serializers import format_json

if TYPE_CHECKING:
    from vertebra.framework._app import App

__all__ = (
    "OpenAPIConfig",
    "OpenAPISpecBuilder",
    "ScalarDocsConfig",
)


class ScalarDocsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Scalar interactive API documentation UI configuration."""

    path: str = "/docs"
    theme: str = "kepler"
    dark_mode: bool = True


class OpenAPIConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """OpenAPI 3.1.0 document configuration."""

    title: str = "Vertebra API"
    version: str = "0.1.0"
    description: str = ""
    openapi_version: str = "3.1.0"
    schema_path: str = "/openapi.json"
    scalar: ScalarDocsConfig = msgspec.field(default_factory=ScalarDocsConfig)


class OpenAPISpecBuilder:
    """Constructs OpenAPI 3.1.0 documents from route metadata and msgspec.Struct types."""

    __slots__ = ("_config", "_paths", "_schemas")

    def __init__(self, config: OpenAPIConfig | None = None) -> None:
        """Initialize builder with OpenAPI configuration."""
        self._config = config or OpenAPIConfig()
        self._paths: dict[str, dict[str, Any]] = {}
        self._schemas: dict[str, dict[str, Any]] = {}

    @property
    def config(self) -> OpenAPIConfig:
        """Return the OpenAPIConfig."""
        return self._config

    @classmethod
    def from_app(cls, app: App, config: OpenAPIConfig | None = None) -> OpenAPISpecBuilder:
        """Build an OpenAPISpecBuilder by inspecting routes on an App instance."""
        cfg = config or OpenAPIConfig()
        builder = cls(config=cfg)
        for route in app.routes:
            if route.path in (cfg.schema_path, cfg.scalar.path):
                continue
            handler = route.handler
            resp_schema = None
            try:
                type_hints = typing.get_type_hints(handler)
                resp_schema = type_hints.get("return")
            except Exception:
                sig = inspect.signature(handler)
                if sig.return_annotation is not inspect.Signature.empty and isinstance(sig.return_annotation, type):
                    resp_schema = sig.return_annotation

            if resp_schema is type(None):
                resp_schema = None

            route_container = app.container.create_child(route.dependencies)
            request_body_schema: type[Any] | None = None
            for binding in inspect_callable_bindings(handler):
                if binding.is_explicit_inject:
                    continue
                dep_key = route_container.graph.resolve_param_key(binding)
                if dep_key is not None and dep_key in route_container.graph.providers:
                    continue
                if inspect.isclass(binding.annotation) and issubclass(binding.annotation, msgspec.Struct):
                    request_body_schema = binding.annotation
                    break

            builder.add_operation(
                path=route.path,
                method=route.method,
                summary=inspect.getdoc(handler) or "",
                operation_id=getattr(handler, "__name__", None),
                response_schema=resp_schema,
                request_body_schema=request_body_schema,
                tags=route.tags,
            )
        return builder

    def add_operation(
        self,
        path: str,
        method: str,
        *,
        summary: str = "",
        operation_id: str | None = None,
        response_schema: type[Any] | None = None,
        request_body_schema: type[Any] | None = None,
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
        if request_body_schema is not None and inspect.isclass(request_body_schema):
            body_name = getattr(request_body_schema, "__name__", "RequestModel")
            _, body_components = msgspec.json.schema_components((request_body_schema,))
            self._schemas.update(body_components)
            op["requestBody"] = {
                "required": True,
                "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{body_name}"}}},
            }
        if response_schema is not None and inspect.isclass(response_schema):
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

    def to_json(self) -> str:
        """Serialize the complete OpenAPI 3.1.0 specification dictionary to formatted JSON."""
        return format_json(self.build())
