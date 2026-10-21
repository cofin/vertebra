"""OpenAPI 3.1.0 specification generator with Litestar v2 and v3 parameter marker support."""

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

import msgspec

from vertebra.build.codegen._ir import (
    AnnotatedType,
    ArrayType,
    GenericType,
    LiteralType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    TupleType,
    TypeNode,
    UnionType,
)
from vertebra.build.codegen._metadata import RouteMetadata
from vertebra.build.codegen.procedures import resolve_parameter_node

__all__ = ("OpenAPIGenerator",)

_PATH_PARAM_PATTERN = re.compile(r"\{(\w+)(?::\w+)?\}")
_SCHEMA_REF_PREFIX = f"{chr(35)}/components/schemas/"


@dataclass(slots=True)
class OpenAPIGenerator:
    """Generate OpenAPI 3.1.0 specifications from collected RouteMetadata."""

    declared: dict[str, ObjectType] = field(default_factory=dict)

    def generate(
        self,
        routes: Iterable[RouteMetadata],
        *,
        title: str = "Vertebra API",
        version: str = "1.0.0",
    ) -> dict[str, Any]:
        """Build an OpenAPI 3.1.0 dictionary from the given routes."""
        route_list = list(routes)
        for route in route_list:
            for param_name, param_node in route.params.items():
                res = resolve_parameter_node(param_name, param_node)
                if res.include_in_schema:
                    self._collect_named_types((res.base_type,))
            self._collect_named_types((route.return_type,))

        paths: dict[str, dict[str, Any]] = {}
        for route in sorted(route_list, key=lambda r: (self._normalize_path(r.path), r.method, r.handler_name)):
            norm_path = self._normalize_path(route.path)
            path_params = set(_PATH_PARAM_PATTERN.findall(route.path))
            path_item = paths.setdefault(norm_path, {})
            path_item[route.method.lower()] = self._build_operation(route, path_params)

        schemas: dict[str, Any] = {}
        for name in sorted(self.declared):
            schemas[name] = self._type_to_schema(self.declared[name], in_component=True)

        return {
            "openapi": "3.1.0",
            "info": {"title": title, "version": version},
            "paths": paths,
            "components": {"schemas": schemas},
        }

    def generate_json(
        self,
        routes: Iterable[RouteMetadata],
        *,
        title: str = "Vertebra API",
        version: str = "1.0.0",
    ) -> str:
        """Render the OpenAPI 3.1.0 specification as a JSON string."""
        doc = self.generate(routes, title=title, version=version)
        return msgspec.json.encode(doc).decode("utf-8")

    def _collect_named_types(self, nodes: Iterable[TypeNode]) -> None:
        for node in nodes:
            if isinstance(node, ObjectType) and node.name:
                self.declared.setdefault(node.name, node)
            for child in node.iter_children():
                self._collect_named_types((child,))

    @staticmethod
    def _normalize_path(path: str) -> str:
        return _PATH_PARAM_PATTERN.sub(lambda m: "{" + m.group(1) + "}", path)

    def _build_operation(self, route: RouteMetadata, path_params: set[str]) -> dict[str, Any]:
        parameters: list[dict[str, Any]] = []
        request_body_content: dict[str, Any] = {}

        for param_name, param_node in route.params.items():
            res = resolve_parameter_node(param_name, param_node, path_param_names=path_params)
            if not res.include_in_schema:
                continue

            schema = self._apply_constraints(self._type_to_schema(res.base_type), res.constraints)
            if res.location == "body":
                media = res.media_type or "application/json"
                request_body_content[media] = {"schema": schema}
            else:
                is_required = True if res.location == "path" else res.required
                parameters.append(
                    {
                        "name": res.external_name,
                        "in": res.location,
                        "required": is_required,
                        "schema": schema,
                    }
                )

        operation: dict[str, Any] = {
            "operationId": route.operation_id.replace(".", "_"),
            "parameters": parameters,
            "responses": {
                "200": {
                    "description": "Successful response",
                    "content": {
                        "application/json": {
                            "schema": self._type_to_schema(route.return_type),
                        }
                    },
                }
            },
        }
        if request_body_content:
            operation["requestBody"] = {
                "required": True,
                "content": request_body_content,
            }
        return operation

    def _type_to_schema(self, node: TypeNode, *, in_component: bool = False) -> dict[str, Any]:
        if isinstance(node, AnnotatedType):
            res = resolve_parameter_node("_", node)
            return self._apply_constraints(
                self._type_to_schema(res.base_type, in_component=in_component),
                res.constraints,
            )

        if isinstance(node, PrimitiveType):
            return self._primitive_schema(node.kind)

        if isinstance(node, ArrayType):
            return {"type": "array", "items": self._type_to_schema(node.item)}

        if isinstance(node, TupleType):
            items = [self._type_to_schema(item) for item in node.items]
            return {
                "type": "array",
                "prefixItems": items,
                "minItems": len(items),
                "maxItems": len(items),
            }

        if isinstance(node, UnionType):
            return {"oneOf": [self._type_to_schema(variant) for variant in node.variants]}

        if isinstance(node, LiteralType):
            return {"const": node.value}

        if isinstance(node, NamedTypeRef):
            return {"$ref": f"{_SCHEMA_REF_PREFIX}{node.name}"}

        if isinstance(node, ObjectType):
            if node.name and node.name in self.declared and not in_component:
                return {"$ref": f"{_SCHEMA_REF_PREFIX}{node.name}"}
            props = {f_name: self._type_to_schema(f_type) for f_name, f_type in sorted(node.fields.items())}
            required = sorted(f_name for f_name in node.fields if f_name not in node.optional_fields)
            result: dict[str, Any] = {"type": "object", "properties": props}
            if required:
                result["required"] = required
            return result

        if isinstance(node, GenericType) and node.name == "Record" and len(node.params) == 2:
            return {"type": "object", "additionalProperties": self._type_to_schema(node.params[1])}

        return {}

    @staticmethod
    def _primitive_schema(kind: str) -> dict[str, Any]:
        mapping: dict[str, dict[str, Any]] = {
            "string": {"type": "string"},
            "number": {"type": "number"},
            "boolean": {"type": "boolean"},
            "null": {"type": "null"},
            "Uint8Array": {"type": "string", "format": "binary"},
        }
        return mapping.get(kind, {})

    @staticmethod
    def _apply_constraints(
        schema: dict[str, Any],
        constraints: Sequence[tuple[str, object]],
    ) -> dict[str, Any]:
        result = dict(schema)
        for key, value in constraints:
            if key == "ge":
                result["minimum"] = value
            elif key == "gt":
                result["minimum"] = value
                result["exclusiveMinimum"] = True
            elif key == "le":
                result["maximum"] = value
            elif key == "lt":
                result["maximum"] = value
                result["exclusiveMaximum"] = True
            elif key == "multiple_of":
                result["multipleOf"] = value
            elif key == "min_length":
                result["minLength"] = value
            elif key == "max_length":
                result["maxLength"] = value
            elif key == "pattern":
                result["pattern"] = value
            elif key == "default":
                result["default"] = value
        return result
