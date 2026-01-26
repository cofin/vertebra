"""Type IR nodes for the Vertebra type bridge."""

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


class TypeNode(msgspec.Struct, frozen=True, tag_field="_type", tag=True):
    """Base class for type IR nodes.

    Note: gc=False is NOT used because Type IR nodes form object graphs
    (e.g., ArrayType contains TypeNode, ObjectType contains dict of TypeNodes).
    """

    def to_typescript(self) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    def iter_children(self) -> "Iterable[TypeNode]":
        return ()


class PrimitiveType(TypeNode, frozen=True, tag="primitive"):
    """Primitive type (string, number, boolean, null, etc.)."""

    kind: str

    def to_typescript(self) -> str:
        return self.kind


class ArrayType(TypeNode, frozen=True, tag="array"):
    """Array type wrapping an item type."""

    item: TypeNode

    def to_typescript(self) -> str:
        return f"{self.item.to_typescript()}[]"

    def iter_children(self) -> "Iterable[TypeNode]":
        return (self.item,)


class ObjectType(TypeNode, frozen=True, tag="object"):
    """Object type with named fields."""

    fields: dict[str, TypeNode]
    name: str | None = None
    optional_fields: set[str] = msgspec.field(default_factory=set)
    type_params: tuple[str, ...] = ()
    type_param_constraints: dict[str, TypeNode] = msgspec.field(default_factory=dict)

    def to_typescript(self) -> str:
        parts: list[str] = ["{"]
        for field_name, field_type in self.fields.items():
            optional = "?" if field_name in self.optional_fields else ""
            parts.append(f"{field_name}{optional}: {field_type.to_typescript()};")
        parts.append("}")
        return " ".join(parts)

    def iter_children(self) -> "Iterable[TypeNode]":
        yield from self.fields.values()
        yield from self.type_param_constraints.values()


class UnionType(TypeNode, frozen=True, tag="union"):
    """Union of multiple type variants."""

    variants: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        return " | ".join(v.to_typescript() for v in self.variants)

    def iter_children(self) -> "Iterable[TypeNode]":
        return self.variants


class LiteralType(TypeNode, frozen=True, tag="literal"):
    """Literal type with a specific value."""

    value: object

    def to_typescript(self) -> str:
        if self.value is None:
            return "null"
        if isinstance(self.value, str):
            # manual escaping to avoid syntax errors
            escaped = self.value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'
        if isinstance(self.value, bool):
            return "true" if self.value else "false"
        return str(self.value)


class GenericType(TypeNode, frozen=True, tag="generic"):
    """Generic type with type parameters."""

    name: str
    params: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        inner = ", ".join(param.to_typescript() for param in self.params)
        return f"{self.name}<{inner}>"

    def iter_children(self) -> "Iterable[TypeNode]":
        return self.params


class TupleType(TypeNode, frozen=True, tag="tuple"):
    """Tuple type with ordered item types."""

    items: "Sequence[TypeNode]"

    def to_typescript(self) -> str:
        inner = ", ".join(item.to_typescript() for item in self.items)
        return f"[{inner}]"

    def iter_children(self) -> "Iterable[TypeNode]":
        return self.items


class RawType(TypeNode, frozen=True, tag="raw"):
    """Raw TypeScript type string."""

    typescript: str

    def to_typescript(self) -> str:
        return self.typescript


class UnknownType(TypeNode, frozen=True, tag="unknown"):
    """Unknown or unresolved type."""

    def to_typescript(self) -> str:
        return "unknown"


class NamedTypeRef(TypeNode, frozen=True, tag="named_ref"):
    """Reference to a named type."""

    name: str

    def to_typescript(self) -> str:
        return self.name


class AnnotatedType(TypeNode, frozen=True, tag="annotated"):
    """Type with additional metadata annotations."""

    base: TypeNode
    metadata: tuple[object, ...]

    def to_typescript(self) -> str:
        return self.base.to_typescript()

    def iter_children(self) -> "Iterable[TypeNode]":
        return (self.base,)


class SubscriptionType(TypeNode, frozen=True, tag="subscription"):
    """Represents a subscription to a stream of items."""

    item: TypeNode

    def to_typescript(self) -> str:
        return f"Subscription<{self.item.to_typescript()}>"

    def iter_children(self) -> "Iterable[TypeNode]":
        return (self.item,)


class StreamType(TypeNode, frozen=True, tag="stream"):
    """Represents a raw stream of items."""

    item: TypeNode

    def to_typescript(self) -> str:
        return f"AsyncIterable<{self.item.to_typescript()}>"

    def iter_children(self) -> "Iterable[TypeNode]":
        return (self.item,)
