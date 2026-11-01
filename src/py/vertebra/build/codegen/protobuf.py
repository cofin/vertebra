"""Bidirectional Protobuf v3 (.proto) and gRPC service generator and parser."""

from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.build.codegen._emitter import SchemaEmitter
from vertebra.build.codegen._extractor import TypeExtractor
from vertebra.build.codegen._ir import (
    AnnotatedType,
    ArrayType,
    GenericType,
    LiteralType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    StreamType,
    TypeNode,
    UnionType,
)
from vertebra.build.codegen._json_schema import resolve_parameter_node
from vertebra.build.codegen._metadata import RouteCollector, RouteMetadata

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = (
    "ProtoFieldSpec",
    "ProtoMessageSpec",
    "ProtoMethodSpec",
    "ProtoServiceSpec",
    "ProtoSpec",
    "ProtobufGenerator",
    "ProtobufParser",
)

_PROTO_COMMENT_RE = re.compile(r"//[^\n]*|/\*[\s\S]*?\*/")
_PROTO_PACKAGE_RE = re.compile(r"\bpackage\s+([a-zA-Z0-9_.]+)\s*;")
_PROTO_MESSAGE_RE = re.compile(r"\bmessage\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{([^{}]*)\}")
_PROTO_ENUM_RE = re.compile(r"\benum\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{([^{}]*)\}")
_PROTO_SERVICE_RE = re.compile(r"\bservice\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{([^{}]*)\}")
_PROTO_RPC_RE = re.compile(
    r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(stream\s+)?([A-Za-z0-9_.]+)\s*\)"
    r"\s*returns\s*\(\s*(stream\s+)?([A-Za-z0-9_.]+)\s*\)"
)
_PROTO_MAP_FIELD_RE = re.compile(
    r"^map<\s*([A-Za-z0-9_]+)\s*,\s*([A-Za-z0-9_.]+)\s*>\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)"
)
_PROTO_FIELD_RE = re.compile(r"^(?:(optional|repeated)\s+)?([A-Za-z0-9_.]+)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)")

_PROTO_TO_PRIMITIVE: dict[str, str] = {
    "string": "string",
    "bool": "boolean",
    "bytes": "Uint8Array",
    "double": "number",
    "float": "number",
    "int32": "number",
    "int64": "number",
    "uint32": "number",
    "uint64": "number",
    "sint32": "number",
    "sint64": "number",
    "fixed32": "number",
    "fixed64": "number",
    "sfixed32": "number",
    "sfixed64": "number",
}

_PROTO_TO_PYTHON_TYPE: dict[str, Any] = {
    "string": str,
    "bool": bool,
    "bytes": bytes,
    "double": float,
    "float": float,
    "int32": int,
    "int64": int,
    "uint32": int,
    "uint64": int,
    "sint32": int,
    "sint64": int,
    "fixed32": int,
    "fixed64": int,
    "sfixed32": int,
    "sfixed64": int,
}

_PROTO_TO_PYTHON_STR: dict[str, str] = {
    "string": "str",
    "bool": "bool",
    "bytes": "bytes",
    "double": "float",
    "float": "float",
    "int32": "int",
    "int64": "int",
    "uint32": "int",
    "uint64": "int",
    "sint32": "int",
    "sint64": "int",
    "fixed32": "int",
    "fixed64": "int",
    "sfixed32": "int",
    "sfixed64": "int",
}


class ProtoFieldSpec(msgspec.Struct, frozen=True, gc=False):
    """Parsed field specification inside a Protobuf v3 message."""

    name: str
    proto_type: str
    number: int
    repeated: bool = False
    optional: bool = False
    map_value_type: str | None = None


class ProtoMessageSpec(msgspec.Struct, frozen=True, gc=False):
    """Parsed Protobuf v3 message definition."""

    name: str
    fields: tuple[ProtoFieldSpec, ...]


class ProtoMethodSpec(msgspec.Struct, frozen=True, gc=False):
    """Parsed gRPC method definition inside a Protobuf service."""

    name: str
    input_type: str
    output_type: str
    client_streaming: bool = False
    server_streaming: bool = False


class ProtoServiceSpec(msgspec.Struct, frozen=True, gc=False):
    """Parsed gRPC service block from a .proto file."""

    name: str
    methods: tuple[ProtoMethodSpec, ...]


class ProtoSpec(msgspec.Struct, frozen=True, gc=False):
    """Complete parsed representation of a .proto v3 schema with codegen IR conversion."""

    package: str
    messages: dict[str, ProtoMessageSpec]
    enums: dict[str, tuple[str, ...]]
    services: dict[str, ProtoServiceSpec]
    declared_types: dict[str, ObjectType]
    routes: list[RouteMetadata]


class ProtobufParser:
    """Parse .proto v3 files into ProtoSpec, codegen IR (ObjectType + RouteMetadata), and runtime msgspec types."""

    @classmethod
    def parse(cls, proto_source: str | Path) -> ProtoSpec:
        """Parse a .proto source string or file path into a ProtoSpec."""
        text = proto_source.read_text(encoding="utf-8") if isinstance(proto_source, Path) else str(proto_source)
        cleaned = _PROTO_COMMENT_RE.sub("", text)

        pkg_match = _PROTO_PACKAGE_RE.search(cleaned)
        package = pkg_match.group(1) if pkg_match else "vertebra.v1"

        enums: dict[str, tuple[str, ...]] = {}
        for enum_match in _PROTO_ENUM_RE.finditer(cleaned):
            enum_name = enum_match.group(1)
            body = enum_match.group(2)
            members: list[str] = []
            for raw_line in body.split(";"):
                line = raw_line.strip()
                if "=" in line:
                    member_name = line.split("=", 1)[0].strip()
                    if member_name:
                        members.append(member_name)
            enums[enum_name] = tuple(members)

        messages: dict[str, ProtoMessageSpec] = {}
        for msg_match in _PROTO_MESSAGE_RE.finditer(cleaned):
            msg_name = msg_match.group(1)
            body = msg_match.group(2)
            fields: list[ProtoFieldSpec] = []
            for raw_stmt in body.split(";"):
                stmt = raw_stmt.strip()
                if not stmt or stmt.startswith(("option ", "reserved ")):
                    continue
                map_m = _PROTO_MAP_FIELD_RE.match(stmt)
                if map_m:
                    key_t, val_t, fname, fnum = map_m.groups()
                    fields.append(
                        ProtoFieldSpec(
                            name=fname,
                            proto_type=key_t,
                            number=int(fnum),
                            map_value_type=val_t,
                        )
                    )
                    continue
                field_m = _PROTO_FIELD_RE.match(stmt)
                if field_m:
                    qualifier, ftype, fname, fnum = field_m.groups()
                    fields.append(
                        ProtoFieldSpec(
                            name=fname,
                            proto_type=ftype.split(".")[-1],
                            number=int(fnum),
                            repeated=(qualifier == "repeated"),
                            optional=(qualifier == "optional"),
                        )
                    )
            messages[msg_name] = ProtoMessageSpec(name=msg_name, fields=tuple(fields))

        services: dict[str, ProtoServiceSpec] = {}
        for svc_match in _PROTO_SERVICE_RE.finditer(cleaned):
            svc_name = svc_match.group(1)
            svc_body = svc_match.group(2)
            methods: list[ProtoMethodSpec] = []
            for rpc_match in _PROTO_RPC_RE.finditer(svc_body):
                m_name, c_stream, in_type, s_stream, out_type = rpc_match.groups()
                methods.append(
                    ProtoMethodSpec(
                        name=m_name,
                        input_type=in_type.split(".")[-1],
                        output_type=out_type.split(".")[-1],
                        client_streaming=bool(c_stream),
                        server_streaming=bool(s_stream),
                    )
                )
            services[svc_name] = ProtoServiceSpec(name=svc_name, methods=tuple(methods))

        declared_types: dict[str, ObjectType] = {}
        for msg_name, msg_spec in messages.items():
            ir_fields: dict[str, TypeNode] = {}
            opt_fields: set[str] = set()
            for f in msg_spec.fields:
                node = cls._field_to_ir_node(f, messages, enums, declared_types)
                ir_fields[f.name] = node
                if f.optional:
                    opt_fields.add(f.name)
            declared_types[msg_name] = ObjectType(
                name=msg_name,
                fields=ir_fields,
                optional_fields=opt_fields,
            )

        for msg_name, obj_type in list(declared_types.items()):
            resolved_fields: dict[str, TypeNode] = {}
            for fname, fnode in obj_type.fields.items():
                if isinstance(fnode, NamedTypeRef) and fnode.name in declared_types:
                    resolved_fields[fname] = declared_types[fnode.name]
                elif (
                    isinstance(fnode, ArrayType)
                    and isinstance(fnode.item, NamedTypeRef)
                    and fnode.item.name in declared_types
                ):
                    resolved_fields[fname] = ArrayType(declared_types[fnode.item.name])
                else:
                    resolved_fields[fname] = fnode
            declared_types[msg_name] = ObjectType(
                name=msg_name,
                fields=resolved_fields,
                optional_fields=obj_type.optional_fields,
            )

        routes: list[RouteMetadata] = []
        for svc_name, svc in services.items():
            for method in svc.methods:
                in_obj = declared_types.get(method.input_type, ObjectType(name=method.input_type, fields={}))
                out_obj: TypeNode = declared_types.get(
                    method.output_type,
                    ObjectType(name=method.output_type, fields={}),
                )
                if method.server_streaming:
                    out_obj = StreamType(out_obj)
                snake_name = cls._to_snake_case(method.name)
                grpc_path = f"/{package}.{svc_name}/{method.name}"
                routes.append(
                    RouteMetadata(
                        operation_id=f"post.{package.replace('.', '_')}.{svc_name}.{snake_name}",
                        handler_fn=None,
                        source_file=Path(proto_source) if isinstance(proto_source, Path) else Path("<proto>"),
                        params={"body": in_obj},
                        return_type=out_obj,
                        path=grpc_path,
                        method="POST",
                        handler_name=snake_name,
                    )
                )

        return ProtoSpec(
            package=package,
            messages=messages,
            enums=enums,
            services=services,
            declared_types=declared_types,
            routes=routes,
        )

    @classmethod
    def _field_to_ir_node(
        cls,
        field: ProtoFieldSpec,
        messages: Mapping[str, ProtoMessageSpec],
        enums: Mapping[str, tuple[str, ...]],
        declared: Mapping[str, ObjectType],
    ) -> TypeNode:
        if field.map_value_type is not None:
            val_node = cls._scalar_or_ref_to_ir(field.map_value_type, messages, enums, declared)
            return GenericType(name="Record", params=(PrimitiveType("string"), val_node))
        base = cls._scalar_or_ref_to_ir(field.proto_type, messages, enums, declared)
        if field.repeated:
            return ArrayType(base)
        if field.optional:
            return UnionType((base, PrimitiveType("null")))
        return base

    @classmethod
    def _scalar_or_ref_to_ir(
        cls,
        proto_type: str,
        messages: Mapping[str, ProtoMessageSpec],
        enums: Mapping[str, tuple[str, ...]],
        declared: Mapping[str, ObjectType],
    ) -> TypeNode:
        if proto_type in _PROTO_TO_PRIMITIVE:
            return PrimitiveType(_PROTO_TO_PRIMITIVE[proto_type])
        if proto_type in enums:
            variants = tuple(LiteralType(member) for member in enums[proto_type])
            return UnionType(variants) if variants else PrimitiveType("string")
        if proto_type in declared:
            return declared[proto_type]
        if proto_type in messages:
            return NamedTypeRef(proto_type)
        return PrimitiveType("string")

    @staticmethod
    def _to_snake_case(name: str) -> str:
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


class ProtobufGenerator(SchemaEmitter):
    """Bidirectional .proto v3 generator and parser for routes, procedures, and msgspec structs."""

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
        """Render a complete proto3 definition file from RouteMetadata."""
        route_list = list(routes)
        synthetic_messages: list[tuple[str, dict[str, TypeNode]]] = []
        rpc_lines: list[str] = []
        seen_rpcs: set[str] = set()

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
            if rpc_name in seen_rpcs:
                continue
            seen_rpcs.add(rpc_name)

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

    def generate_from_app(self, app: Any) -> str:
        """Extract all routes and procedures from an App or ProcedureRouter and render a .proto v3 schema."""
        raw_routes = RouteCollector().collect(app)
        proc_routes = _extract_procedure_routes(app)
        proc_names = {r.handler_name for r in proc_routes}
        routes = [r for r in raw_routes if not (r.handler_name.startswith("rpc_") and r.handler_name[4:] in proc_names)]
        existing_handlers = {r.handler_name for r in routes}
        for pr in proc_routes:
            if pr.handler_name not in existing_handlers:
                routes.append(pr)
        return self.generate(routes)

    @staticmethod
    def parse_proto(proto_source: str | Path) -> ProtoSpec:
        """Parse a .proto v3 schema into a ProtoSpec with ObjectType and RouteMetadata IR."""
        return ProtobufParser.parse(proto_source)

    @classmethod
    def load_runtime_types(cls, proto_source: str | Path) -> dict[str, type[msgspec.Struct]]:
        """Dynamically construct msgspec.Struct classes (frozen=True, gc=False) from a .proto v3 schema."""
        spec = ProtobufParser.parse(proto_source)
        created: dict[str, type[msgspec.Struct]] = {}
        for msg_name, msg_spec in spec.messages.items():
            fields_def: list[tuple[str, Any] | tuple[str, Any, Any]] = []
            required_fields: list[tuple[str, Any]] = []
            optional_fields: list[tuple[str, Any, Any]] = []
            for f in msg_spec.fields:
                py_type = cls._resolve_runtime_field_type(f, created)
                if f.optional:
                    optional_fields.append((f.name, py_type | None, None))
                elif f.repeated:
                    list_type: Any = list.__class_getitem__(py_type)
                    optional_fields.append((f.name, list_type, msgspec.field(default_factory=list)))
                elif f.map_value_type is not None:
                    val_t: Any = _PROTO_TO_PYTHON_TYPE.get(f.map_value_type, created.get(f.map_value_type, Any))
                    dict_type: Any = dict.__class_getitem__((str, val_t))
                    optional_fields.append((f.name, dict_type, msgspec.field(default_factory=dict)))
                else:
                    required_fields.append((f.name, py_type))
            fields_def.extend(required_fields)
            fields_def.extend(optional_fields)
            struct_cls = msgspec.defstruct(
                msg_name,
                fields_def,
                frozen=True,
                gc=False,
            )
            created[msg_name] = struct_cls
        return created

    @classmethod
    def generate_python_stubs(cls, proto_source: str | Path) -> str:
        """Generate Python source code defining msgspec.Struct classes and a ProcedureRouter from a .proto schema."""
        spec = ProtobufParser.parse(proto_source)
        lines: list[str] = [
            '"""Generated by Vertebra ProtobufGenerator from .proto schema."""',
            "",
            "from __future__ import annotations",
            "",
            "from typing import Any",
            "",
            "import msgspec",
            "",
            "from vertebra.framework.procedures import ProcedureRouter",
        ]

        for msg_name, msg_spec in spec.messages.items():
            lines.append("")
            lines.append("")
            lines.append(f"class {msg_name}(msgspec.Struct, frozen=True, gc=False):")
            if not msg_spec.fields:
                lines.append("    pass")
                continue
            req_lines: list[str] = []
            opt_lines: list[str] = []
            for f in msg_spec.fields:
                base_str = _PROTO_TO_PYTHON_STR.get(f.proto_type, f.proto_type)
                if f.map_value_type is not None:
                    val_str = _PROTO_TO_PYTHON_STR.get(f.map_value_type, f.map_value_type)
                    opt_lines.append(f"    {f.name}: dict[str, {val_str}] = msgspec.field(default_factory=dict)")
                elif f.repeated:
                    opt_lines.append(f"    {f.name}: list[{base_str}] = msgspec.field(default_factory=list)")
                elif f.optional:
                    opt_lines.append(f"    {f.name}: {base_str} | None = None")
                else:
                    req_lines.append(f"    {f.name}: {base_str}")
            lines.extend(req_lines)
            lines.extend(opt_lines)

        for svc_name, svc in spec.services.items():
            lines.append("")
            lines.append("")
            lines.append(f"def create_{ProtobufParser._to_snake_case(svc_name)}_router() -> ProcedureRouter:")
            lines.append(f'    router = ProcedureRouter(prefix="/{spec.package}.{svc_name}")')
            for method in svc.methods:
                dec = "subscription" if method.server_streaming else "mutation"
                snake_m = ProtobufParser._to_snake_case(method.name)
                lines.append(
                    f'    @router.{dec}(name="{method.name}", input_type={method.input_type}, output_type={method.output_type})'
                )
                lines.append(f"    async def {snake_m}(payload: {method.input_type}) -> {method.output_type}:")
                lines.append(f'        msg = "Unimplemented gRPC method {svc_name}.{method.name}"')
                lines.append("        raise NotImplementedError(msg)")
            lines.append("    return router")

        return "\n".join(lines) + "\n"

    @staticmethod
    def _resolve_runtime_field_type(
        field: ProtoFieldSpec,
        created: Mapping[str, type[msgspec.Struct]],
    ) -> Any:
        if field.proto_type in _PROTO_TO_PYTHON_TYPE:
            return _PROTO_TO_PYTHON_TYPE[field.proto_type]
        if field.proto_type in created:
            return created[field.proto_type]
        return Any

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
        if isinstance(node, GenericType) and node.name == "Record" and len(node.params) == 2:
            val_proto = self._node_to_proto_type(node.params[1])
            return f"map<string, {val_proto}>"
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
        return "".join(part[:1].upper() + part[1:] for part in identifier.replace("-", "_").split("_") if part)


def _extract_procedure_routes(app: Any) -> list[RouteMetadata]:
    """Extract RouteMetadata from ProcedureRouter instances attached to an App or passed directly."""
    routers: list[Any] = []
    if hasattr(app, "procedures") and hasattr(app, "prefix"):
        routers.append(app)
    single_router = getattr(app, "_procedure_router", None) or getattr(app, "procedure_router", None)
    if single_router is not None:
        routers.append(single_router)
    app_routers = getattr(app, "_procedure_routers", None) or getattr(app, "procedure_routers", None)
    if app_routers:
        routers.extend(app_routers)

    extractor = TypeExtractor()
    extracted: list[RouteMetadata] = []
    for router in routers:
        prefix = getattr(router, "prefix", "/_rpc").rstrip("/")
        procs = getattr(router, "procedures", {})
        for proc_name, proc in procs.items():
            fn = getattr(proc, "fn", None)
            if fn is None:
                continue
            params, return_type = extractor.extract(fn)
            if getattr(proc, "input_type", None) is not None and not params:
                params = {"payload": extractor.from_hint(proc.input_type)}
            if getattr(proc, "output_type", None) is not None:
                return_type = extractor.from_hint(proc.output_type)
            if getattr(proc, "kind", None) == "subscription" and not isinstance(return_type, StreamType):
                return_type = StreamType(return_type)
            path = f"{prefix}/{proc_name}"
            extracted.append(
                RouteMetadata(
                    operation_id=f"rpc.{proc_name}",
                    handler_fn=fn,
                    source_file=Path(inspect.getsourcefile(fn) or "<procedure>"),
                    params=params,
                    return_type=return_type,
                    path=path,
                    method="POST",
                    handler_name=proc_name,
                )
            )
    return extracted
