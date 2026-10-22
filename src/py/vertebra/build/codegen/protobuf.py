"""Protobuf v3 (.proto) message and gRPC service definition generator."""

from collections.abc import Iterable

from vertebra.build.codegen._emitter import SchemaEmitter
from vertebra.build.codegen._ir import (
    AnnotatedType,
    ArrayType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    StreamType,
    TypeNode,
    UnionType,
)
from vertebra.build.codegen._json_schema import resolve_parameter_node
from vertebra.build.codegen._metadata import RouteMetadata

__all__ = ("ProtobufGenerator",)


class ProtobufGenerator(SchemaEmitter):
    """Generate .proto v3 message and gRPC service blocks from RouteMetadata."""

    __slots__ = ("package", "service_name")

    def __init__(
        self,
        package: str = "vertebra.v1",
        service_name: str = "VertebraService",
        declared: dict[str, ObjectType] | None = None,
    ) -> None:
        super().__init__(declared=declared)
        self.package = package
        self.service_name = service_name

    def generate(self, routes: Iterable[RouteMetadata]) -> str:
        """Render a complete proto3 definition file."""
        route_list = list(routes)
        synthetic_messages: list[tuple[str, dict[str, TypeNode]]] = []
        rpc_lines: list[str] = []

        for route in route_list:
            visible_fields: dict[str, TypeNode] = {}
            for param_name, param_node in route.params.items():
                res = resolve_parameter_node(param_name, param_node)
                if not res.include_in_schema:
                    continue
                visible_fields[param_name] = res.base_type
                self._collect_objects(res.base_type)
            self._collect_objects(route.return_type)

            rpc_name = self._to_pascal_case(route.handler_name)
            if len(visible_fields) == 1:
                only_node = next(iter(visible_fields.values()))
                if isinstance(only_node, ObjectType) and only_node.name:
                    req_type = only_node.name
                else:
                    req_type = f"{rpc_name}Request"
                    synthetic_messages.append((req_type, visible_fields))
            else:
                req_type = f"{rpc_name}Request"
                synthetic_messages.append((req_type, visible_fields))

            is_stream_resp = isinstance(route.return_type, StreamType)
            resp_node = route.return_type.item if isinstance(route.return_type, StreamType) else route.return_type
            if isinstance(resp_node, ObjectType) and resp_node.name:
                resp_type = resp_node.name
            else:
                resp_type = f"{rpc_name}Response"
                synthetic_messages.append((resp_type, {"result": resp_node}))

            stream_prefix = "stream " if is_stream_resp else ""
            rpc_lines.append(f"  rpc {rpc_name} ({req_type}) returns ({stream_prefix}{resp_type});")

        blocks: list[str] = [
            'syntax = "proto3";',
            f"package {self.package};",
        ]

        for name in sorted(self._declared):
            blocks.append(self._render_message(name, self._declared[name].fields, self._declared[name].optional_fields))

        for msg_name, msg_fields in synthetic_messages:
            if msg_name not in self._declared:
                blocks.append(self._render_message(msg_name, msg_fields, set()))

        service_block = [f"service {self.service_name} {{", *rpc_lines, "}"]
        blocks.append("\n".join(service_block))
        return "\n\n".join(blocks).strip() + "\n"

    def _render_message(
        self,
        name: str,
        fields: dict[str, TypeNode],
        optional_fields: set[str],
    ) -> str:
        lines = [f"message {name} {{"]
        for index, (field_name, field_type) in enumerate(fields.items(), start=1):
            proto_type = self._node_to_proto_type(field_type, is_optional=field_name in optional_fields)
            lines.append(f"  {proto_type} {field_name} = {index};")
        lines.append("}")
        return "\n".join(lines)

    def _node_to_proto_type(self, node: TypeNode, *, is_optional: bool = False) -> str:
        if isinstance(node, AnnotatedType):
            return self._node_to_proto_type(node.base, is_optional=is_optional)
        if isinstance(node, ArrayType):
            return f"repeated {self._node_to_proto_type(node.item)}"
        if isinstance(node, UnionType):
            non_null = [v for v in node.variants if not (isinstance(v, PrimitiveType) and v.kind == "null")]
            if len(non_null) == 1:
                return f"optional {self._node_to_proto_type(non_null[0])}"
            return "string"
        if isinstance(node, ObjectType) and node.name:
            prefix = "optional " if is_optional else ""
            return f"{prefix}{node.name}"
        if isinstance(node, NamedTypeRef):
            prefix = "optional " if is_optional else ""
            return f"{prefix}{node.name}"
        if isinstance(node, PrimitiveType):
            scalar = self._primitive_to_proto(node.kind)
            return f"optional {scalar}" if is_optional else scalar
        return "string"

    @staticmethod
    def _primitive_to_proto(kind: str) -> str:
        mapping = {
            "string": "string",
            "number": "double",
            "boolean": "bool",
            "Uint8Array": "bytes",
        }
        return mapping.get(kind, "string")

    @staticmethod
    def _to_pascal_case(identifier: str) -> str:
        return "".join(part.capitalize() for part in identifier.replace("-", "_").split("_") if part)
