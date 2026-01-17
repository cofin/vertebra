"""Types package."""

from __future__ import annotations

from vertebra.types.extractor import TypeExtractor
from vertebra.types.ir import (
    AnnotatedType,
    ArrayType,
    GenericType,
    LiteralType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    RawType,
    TupleType,
    TypeNode,
    UnionType,
    UnknownType,
)

__all__ = [
    "AnnotatedType",
    "ArrayType",
    "GenericType",
    "LiteralType",
    "NamedTypeRef",
    "ObjectType",
    "PrimitiveType",
    "RawType",
    "TupleType",
    "TypeExtractor",
    "TypeNode",
    "UnionType",
    "UnknownType",
]


def __getattr__(name: str):
    if name == "TypeScriptGenerator":
        from vertebra.types.typescript import TypeScriptGenerator

        return TypeScriptGenerator
    raise AttributeError(name)


__all__.append("TypeScriptGenerator")
