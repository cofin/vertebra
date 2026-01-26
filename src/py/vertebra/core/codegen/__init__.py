"""Code generation package - TypeBridge for Python to TypeScript."""

from __future__ import annotations

from vertebra.core.codegen.bridge import TypeBridge, TypeBridgeApp
from vertebra.core.codegen.extractor import TypeExtractor
from vertebra.core.codegen.ir import (
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
from vertebra.core.codegen.typescript import TypeScriptGenerator
from vertebra.core.codegen.watch import TypeBridgeWatcher

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
