"""Type IR nodes for the Vertebra type bridge."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence


class TypeNode:
    """Base class for type IR nodes."""

    def to_typescript(self) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    def iter_children(self) -> Iterable["TypeNode"]:
        return ()


@dataclass(frozen=True, slots=True)
class PrimitiveType(TypeNode):
    kind: str

    def to_typescript(self) -> str:
        return self.kind


@dataclass(frozen=True, slots=True)
class ArrayType(TypeNode):
    item: TypeNode

    def to_typescript(self) -> str:
        return f"{self.item.to_typescript()}[]"

    def iter_children(self) -> Iterable[TypeNode]:
        return (self.item,)


@dataclass(frozen=True, slots=True)
class ObjectType(TypeNode):
    fields: dict[str, TypeNode]
    name: str | None = None
    optional_fields: set[str] = field(default_factory=set)
    type_params: tuple[str, ...] = ()
    type_param_constraints: dict[str, TypeNode] = field(default_factory=dict)

    def to_typescript(self) -> str:
        parts: list[str] = ["{"]
        for field_name, field_type in self.fields.items():
            optional = "?" if field_name in self.optional_fields else ""
            parts.append(f"{field_name}{optional}: {field_type.to_typescript()};")
        parts.append("}")
        return " ".join(parts)

    def iter_children(self) -> Iterable[TypeNode]:
        for field_type in self.fields.values():
            yield field_type
        for constraint in self.type_param_constraints.values():
            yield constraint


@dataclass(frozen=True, slots=True)
class UnionType(TypeNode):
    variants: Sequence[TypeNode]

    def to_typescript(self) -> str:
        return " | ".join(v.to_typescript() for v in self.variants)

    def iter_children(self) -> Iterable[TypeNode]:
        return self.variants


@dataclass(frozen=True, slots=True)
class LiteralType(TypeNode):
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


@dataclass(frozen=True, slots=True)
class GenericType(TypeNode):
    name: str
    params: Sequence[TypeNode]

    def to_typescript(self) -> str:
        inner = ", ".join(param.to_typescript() for param in self.params)
        return f"{self.name}<{inner}>"

    def iter_children(self) -> Iterable[TypeNode]:
        return self.params


@dataclass(frozen=True, slots=True)
class TupleType(TypeNode):
    items: Sequence[TypeNode]

    def to_typescript(self) -> str:
        inner = ", ".join(item.to_typescript() for item in self.items)
        return f"[{inner}]"

    def iter_children(self) -> Iterable[TypeNode]:
        return self.items


@dataclass(frozen=True, slots=True)
class RawType(TypeNode):
    typescript: str

    def to_typescript(self) -> str:
        return self.typescript


@dataclass(frozen=True, slots=True)
class UnknownType(TypeNode):
    def to_typescript(self) -> str:
        return "unknown"


@dataclass(frozen=True, slots=True)
class NamedTypeRef(TypeNode):
    name: str

    def to_typescript(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class AnnotatedType(TypeNode):
    base: TypeNode
    metadata: tuple[object, ...]

    def to_typescript(self) -> str:
        return self.base.to_typescript()

    def iter_children(self) -> Iterable[TypeNode]:
        return (self.base,)
