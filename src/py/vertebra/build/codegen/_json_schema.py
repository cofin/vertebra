"""JSON Schema to TypeNode conversion and parameter marker resolution."""

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
    UnknownType,
)
from vertebra.build.codegen.markers import (
    BodyMarker,
    LocationMarker,
    NamedDependencyMarker,
    ParameterLocation,
    ParameterMarker,
    SkipValidationMarker,
)

__all__ = (
    "ParameterResolution",
    "json_schema_to_type_node",
    "resolve_parameter_node",
)


class ParameterResolution(msgspec.Struct, frozen=True, gc=False):
    """Normalized parameter metadata extracted from Litestar v2 and v3 Annotated markers."""

    name: str
    external_name: str
    base_type: TypeNode
    location: ParameterLocation
    media_type: str | None
    constraints: tuple[tuple[str, object], ...]
    include_in_schema: bool
    skip_validation: bool
    required: bool = True


def json_schema_to_type_node(schema: dict[str, Any], *, name: str | None = None) -> TypeNode:
    """Convert a JSON Schema dict into a TypeNode IR node with optional constraints."""
    ref = schema.get("$ref")
    if isinstance(ref, str) and "/" in ref:
        return NamedTypeRef(ref.rsplit("/", 1)[-1])

    if "const" in schema:
        return LiteralType(schema["const"])

    enum_values = schema.get("enum")
    if isinstance(enum_values, list) and enum_values:
        if len(enum_values) == 1:
            return LiteralType(enum_values[0])
        return UnionType(tuple(LiteralType(item) for item in enum_values))

    for union_key in ("oneOf", "anyOf"):
        variants = schema.get(union_key)
        if isinstance(variants, list) and variants:
            return UnionType(tuple(json_schema_to_type_node(v) for v in variants if isinstance(v, dict)))

    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        return UnionType(tuple(json_schema_to_type_node({**schema, "type": t}) for t in schema_type))

    constraints: list[tuple[str, object]] = []
    if "minimum" in schema:
        key = "gt" if schema.get("exclusiveMinimum") is True else "ge"
        constraints.append((key, schema["minimum"]))
    elif isinstance(schema.get("exclusiveMinimum"), (int, float)):
        constraints.append(("gt", schema["exclusiveMinimum"]))

    if "maximum" in schema:
        key = "lt" if schema.get("exclusiveMaximum") is True else "le"
        constraints.append((key, schema["maximum"]))
    elif isinstance(schema.get("exclusiveMaximum"), (int, float)):
        constraints.append(("lt", schema["exclusiveMaximum"]))

    if "multipleOf" in schema:
        constraints.append(("multiple_of", schema["multipleOf"]))
    if "minLength" in schema:
        constraints.append(("min_length", schema["minLength"]))
    if "maxLength" in schema:
        constraints.append(("max_length", schema["maxLength"]))
    if "pattern" in schema:
        constraints.append(("pattern", schema["pattern"]))

    base_node: TypeNode
    if schema_type == "string":
        base_node = PrimitiveType("Uint8Array") if schema.get("format") == "binary" else PrimitiveType("string")
    elif schema_type in {"number", "integer"}:
        base_node = PrimitiveType("number")
    elif schema_type == "boolean":
        base_node = PrimitiveType("boolean")
    elif schema_type == "null":
        base_node = PrimitiveType("null")
    elif schema_type == "array":
        prefix_items = schema.get("prefixItems")
        if isinstance(prefix_items, list):
            base_node = TupleType(tuple(json_schema_to_type_node(i) for i in prefix_items if isinstance(i, dict)))
        else:
            items = schema.get("items")
            item_node = json_schema_to_type_node(items) if isinstance(items, dict) else UnknownType()
            base_node = ArrayType(item_node)
    elif schema_type == "object" or "properties" in schema:
        props = schema.get("properties")
        if isinstance(props, dict):
            req_set = set(schema.get("required", ()))
            fields: dict[str, TypeNode] = {}
            opt_fields: set[str] = set()
            for prop_name, prop_schema in props.items():
                if isinstance(prop_schema, dict):
                    fields[prop_name] = json_schema_to_type_node(prop_schema)
                    if prop_name not in req_set:
                        opt_fields.add(prop_name)
            base_node = ObjectType(
                fields=fields,
                name=name or schema.get("title"),
                optional_fields=opt_fields,
            )
        elif isinstance(schema.get("additionalProperties"), dict):
            val_node = json_schema_to_type_node(schema["additionalProperties"])
            base_node = GenericType("Record", (PrimitiveType("string"), val_node))
        else:
            base_node = ObjectType(fields={}, name=name or schema.get("title"))
    else:
        base_node = UnknownType()

    if constraints:
        return AnnotatedType(base=base_node, metadata=tuple(constraints))
    return base_node


def resolve_parameter_node(
    param_name: str,
    node: TypeNode,
    *,
    path_param_names: set[str] | None = None,
) -> ParameterResolution:
    """Inspect a TypeNode and unwrap Litestar v2 and v3 Annotated markers into a ParameterResolution."""
    metadata_items: list[object] = []
    cursor = node
    while isinstance(cursor, AnnotatedType):
        metadata_items.extend(cursor.metadata)
        cursor = cursor.base

    location: ParameterLocation = "path" if (path_param_names and param_name in path_param_names) else "query"
    external_name = param_name
    media_type: str | None = None
    include_in_schema = True
    skip_validation = False
    required = True
    constraints: list[tuple[str, object]] = []

    for meta in metadata_items:
        if isinstance(meta, NamedDependencyMarker) or type(meta).__name__ in {
            "DependencyKwarg",
            "NamedDependencyMarker",
        }:
            include_in_schema = False
            continue

        if isinstance(meta, SkipValidationMarker) or getattr(meta, "skip_validation", False) is True:
            skip_validation = True
            continue

        if isinstance(meta, LocationMarker):
            location = meta.location
            if meta.media_type is not None:
                media_type = meta.media_type
            continue

        if isinstance(meta, BodyMarker) or type(meta).__name__ == "BodyKwarg":
            location = "body"
            raw_media = getattr(meta, "media_type", "application/json")
            media_type = str(raw_media) if raw_media else "application/json"
            if getattr(meta, "include_in_schema", True) is False:
                include_in_schema = False
            continue

        meta_cls_name = type(meta).__name__
        if isinstance(meta, ParameterMarker) or meta_cls_name in {
            "ParameterKwarg",
            "PathParameter",
            "QueryParameter",
            "HeaderParameter",
            "CookieParameter",
        }:
            if getattr(meta, "include_in_schema", True) is False:
                include_in_schema = False
            if meta_cls_name == "PathParameter":
                location = "path"
            elif meta_cls_name == "QueryParameter":
                location = "query"
            elif meta_cls_name == "HeaderParameter":
                location = "header"
            elif meta_cls_name == "CookieParameter":
                location = "cookie"
            alias_name = getattr(meta, "name", None)
            if alias_name:
                external_name = str(alias_name)
            header_val = getattr(meta, "header", None)
            cookie_val = getattr(meta, "cookie", None)
            query_val = getattr(meta, "query", None)
            if header_val:
                location = "header"
                external_name = str(header_val)
            elif cookie_val:
                location = "cookie"
                external_name = str(cookie_val)
            elif query_val:
                location = "query"
                external_name = str(query_val)
            if getattr(meta, "required", True) is False:
                required = False
            for key in ("ge", "gt", "le", "lt", "multiple_of", "min_length", "max_length", "pattern", "default"):
                val = getattr(meta, key, None)
                if val is not None:
                    constraints.append((key, val))
            continue

        if isinstance(meta, tuple) and len(meta) == 2 and isinstance(meta[0], str):
            constraints.append((meta[0], meta[1]))
            continue

        for key in ("ge", "gt", "le", "lt", "multiple_of", "min_length", "max_length", "pattern"):
            val = getattr(meta, key, None)
            if val is not None:
                constraints.append((key, val))

    if (
        isinstance(cursor, ObjectType)
        and cursor.name
        and location == "query"
        and not (path_param_names and param_name in path_param_names)
        and not any(
            isinstance(m, (LocationMarker, ParameterMarker))
            or type(m).__name__
            in {
                "ParameterKwarg",
                "PathParameter",
                "QueryParameter",
                "HeaderParameter",
                "CookieParameter",
            }
            for m in metadata_items
        )
    ):
        location = "body"
        media_type = media_type or "application/json"

    return ParameterResolution(
        name=param_name,
        external_name=external_name,
        base_type=cursor,
        location=location,
        media_type=media_type,
        constraints=tuple(constraints),
        include_in_schema=include_in_schema,
        skip_validation=skip_validation,
        required=required,
    )
