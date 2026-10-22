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


class TypeNode(msgspec.Struct, frozen=True, gc=False, tag_field="_type", tag=True):
    """Base class for route codegen IR nodes."""

    def to_typescript(self) -> str:
        """Render the TypeScript type expression for this IR node."""
        raise NotImplementedError

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield child TypeNode instances contained within this node."""
        return ()


class PrimitiveType(TypeNode, frozen=True, tag="primitive"):
    """Primitive scalar TypeScript type node."""

    kind: str

    def to_typescript(self) -> str:
        """Return the scalar TypeScript type string."""
        return self.kind


class ArrayType(TypeNode, frozen=True, tag="array"):
    """Homogeneous array TypeScript type node."""

    item: TypeNode

    def to_typescript(self) -> str:
        """Return the array TypeScript type string."""
        return f"{self.item.to_typescript()}[]"

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield the array item TypeNode."""
        return (self.item,)


class ObjectType(TypeNode, frozen=True, tag="object"):
    """Structured object or interface TypeScript type node."""

    fields: dict[str, TypeNode]
    name: "str | None" = None
    optional_fields: set[str] = msgspec.field(default_factory=set)

    def to_typescript(self) -> str:
        """Render an inline TypeScript object type literal."""
        parts = ["{"]
        for field_name, field_type in self.fields.items():
            optional = "?" if field_name in self.optional_fields else ""
            parts.append(f"{field_name}{optional}: {field_type.to_typescript()};")
        parts.append("}")
        return " ".join(parts)

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield all field TypeNodes."""
        return self.fields.values()


class UnionType(TypeNode, frozen=True, tag="union"):
    """Union TypeScript type node."""

    variants: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        """Render a pipe-delimited TypeScript union."""
        return " | ".join(variant.to_typescript() for variant in self.variants)

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield all union variant TypeNodes."""
        return self.variants


class LiteralType(TypeNode, frozen=True, tag="literal"):
    """Literal TypeScript type node."""

    value: object

    def to_typescript(self) -> str:
        """Render a literal TypeScript value expression."""
        if self.value is None:
            return "null"
        if isinstance(self.value, str):
            escaped = self.value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'
        if isinstance(self.value, bool):
            return "true" if self.value else "false"
        return str(self.value)


class GenericType(TypeNode, frozen=True, tag="generic"):
    """Parameterized generic TypeScript type node."""

    name: str
    params: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        """Render a parameterized TypeScript generic type."""
        rendered = ", ".join(param.to_typescript() for param in self.params)
        return f"{self.name}<{rendered}>"

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield generic parameter TypeNodes."""
        return self.params


class TupleType(TypeNode, frozen=True, tag="tuple"):
    """Fixed-length tuple TypeScript type node."""

    items: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        """Render a fixed-length TypeScript tuple type."""
        rendered = ", ".join(item.to_typescript() for item in self.items)
        return f"[{rendered}]"

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield tuple element TypeNodes."""
        return self.items


class RawType(TypeNode, frozen=True, tag="raw"):
    """Verbatim TypeScript type expression node."""

    typescript: str

    def to_typescript(self) -> str:
        """Return the raw TypeScript expression."""
        return self.typescript


class UnknownType(TypeNode, frozen=True, tag="unknown"):
    """Fallback TypeScript unknown type node."""

    def to_typescript(self) -> str:
        """Return the TypeScript unknown keyword."""
        return "unknown"


class NamedTypeRef(TypeNode, frozen=True, tag="named_ref"):
    """Reference to a named TypeScript interface or type alias."""

    name: str

    def to_typescript(self) -> str:
        """Return the referenced type identifier."""
        return self.name


class AnnotatedType(TypeNode, frozen=True, tag="annotated"):
    """TypeNode carrying typing.Annotated metadata markers."""

    base: TypeNode
    metadata: tuple[object, ...]

    def to_typescript(self) -> str:
        """Delegate TypeScript rendering to the underlying base TypeNode."""
        return self.base.to_typescript()

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield the underlying base TypeNode."""
        return (self.base,)


class StreamType(TypeNode, frozen=True, tag="stream"):
    """Async streaming TypeScript type node."""

    item: TypeNode

    def to_typescript(self) -> str:
        """Render an AsyncIterable TypeScript type."""
        return f"AsyncIterable<{self.item.to_typescript()}>"

    def iter_children(self) -> "Iterable[TypeNode]":
        """Yield the streamed item TypeNode."""
        return (self.item,)
