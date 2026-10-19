"""Type IR nodes for Vertebra route codegen."""

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

__all__ = (
    "AnnotatedType",
    "ArrayType",
    "GenericType",
    "LiteralType",
    "NamedTypeRef",
    "ObjectType",
    "PrimitiveType",
    "RawType",
    "StreamType",
    "TupleType",
    "TypeNode",
    "UnionType",
    "UnknownType",
)


class TypeNode(msgspec.Struct, frozen=True, tag_field="_type", tag=True):
    """Base class for route codegen IR nodes."""

    def to_typescript(self) -> str:
        raise NotImplementedError

    def iter_children(self) -> "Iterable[TypeNode]":
        return ()


class PrimitiveType(TypeNode, frozen=True, tag="primitive"):
    kind: str

    def to_typescript(self) -> str:
        return self.kind


class ArrayType(TypeNode, frozen=True, tag="array"):
    item: TypeNode

    def to_typescript(self) -> str:
        return f"{self.item.to_typescript()}[]"

    def iter_children(self) -> "Iterable[TypeNode]":
        return (self.item,)


class ObjectType(TypeNode, frozen=True, tag="object"):
    fields: dict[str, TypeNode]
    name: "str | None" = None
    optional_fields: set[str] = msgspec.field(default_factory=set)

    def to_typescript(self) -> str:
        parts = ["{"]
        for field_name, field_type in self.fields.items():
            optional = "?" if field_name in self.optional_fields else ""
            parts.append(f"{field_name}{optional}: {field_type.to_typescript()};")
        parts.append("}")
        return " ".join(parts)

    def iter_children(self) -> "Iterable[TypeNode]":
        return self.fields.values()


class UnionType(TypeNode, frozen=True, tag="union"):
    variants: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        return " | ".join(variant.to_typescript() for variant in self.variants)

    def iter_children(self) -> "Iterable[TypeNode]":
        return self.variants


class LiteralType(TypeNode, frozen=True, tag="literal"):
    value: object

    def to_typescript(self) -> str:
        if self.value is None:
            return "null"
        if isinstance(self.value, str):
            escaped = self.value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'
        if isinstance(self.value, bool):
            return "true" if self.value else "false"
        return str(self.value)


class GenericType(TypeNode, frozen=True, tag="generic"):
    name: str
    params: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        rendered = ", ".join(param.to_typescript() for param in self.params)
        return f"{self.name}<{rendered}>"

    def iter_children(self) -> "Iterable[TypeNode]":
        return self.params


class TupleType(TypeNode, frozen=True, tag="tuple"):
    items: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        rendered = ", ".join(item.to_typescript() for item in self.items)
        return f"[{rendered}]"

    def iter_children(self) -> "Iterable[TypeNode]":
        return self.items


class RawType(TypeNode, frozen=True, tag="raw"):
    typescript: str

    def to_typescript(self) -> str:
        return self.typescript


class UnknownType(TypeNode, frozen=True, tag="unknown"):
    def to_typescript(self) -> str:
        return "unknown"


class NamedTypeRef(TypeNode, frozen=True, tag="named_ref"):
    name: str

    def to_typescript(self) -> str:
        return self.name


class AnnotatedType(TypeNode, frozen=True, tag="annotated"):
    base: TypeNode
    metadata: tuple[object, ...]

    def to_typescript(self) -> str:
        return self.base.to_typescript()

    def iter_children(self) -> "Iterable[TypeNode]":
        return (self.base,)


class StreamType(TypeNode, frozen=True, tag="stream"):
    item: TypeNode

    def to_typescript(self) -> str:
        return f"AsyncIterable<{self.item.to_typescript()}>"

    def iter_children(self) -> "Iterable[TypeNode]":
        return (self.item,)
