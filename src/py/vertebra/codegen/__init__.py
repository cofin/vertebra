"""Types package."""

from __future__ import annotations

from typing import Any

from vertebra.codegen.extractor import TypeExtractor
from vertebra.codegen.ir import (
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

__all__ = (
    "AnnotatedType",
    "ArrayType",
    "GenericType",
    "LiteralType",
    "NamedTypeRef",
    "ObjectType",
    "PrimitiveType",
    "RawType",
    "TupleType",
    "TypeBridge",
    "TypeBridgeApp",
    "TypeBridgeWatcher",
    "TypeExtractor",
    "TypeNode",
    "TypeScriptGenerator",
    "UnionType",
    "UnknownType",
)


def __getattr__(name: str) -> Any:
    if name == "TypeBridge":
        from vertebra.codegen.bridge import TypeBridge

        return TypeBridge
    if name == "TypeBridgeApp":
        from vertebra.codegen.bridge import TypeBridgeApp

        return TypeBridgeApp
    if name == "TypeScriptGenerator":
        from vertebra.codegen.typescript import TypeScriptGenerator

        return TypeScriptGenerator
    if name == "TypeBridgeWatcher":
        from vertebra.codegen.watch import TypeBridgeWatcher

        return TypeBridgeWatcher
    raise AttributeError(name)
