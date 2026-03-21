"""Runtime type extraction from route handler annotations."""

import dataclasses
import inspect
import types
import typing
from collections.abc import AsyncGenerator, AsyncIterable, Generator, Iterable, Iterator
from typing import Any, get_args, get_origin

import msgspec

from vertebra.builder._codegen._ir import (
    AnnotatedType,
    ArrayType,
    GenericType,
    LiteralType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    RawType,
    StreamType,
    TupleType,
    TypeNode,
    UnionType,
    UnknownType,
)

_NODEFAULT = msgspec.NODEFAULT


class TypeExtractor:
    """Convert runtime Python type hints into route codegen IR."""

    def __init__(self) -> None:
        self._model_cache: dict[type[Any], ObjectType] = {}
        self._building: set[type[Any]] = set()

    def from_hint(self, annotation: Any) -> TypeNode:  # noqa: PLR0911
        if annotation is None or annotation is type(None):
            return PrimitiveType("null")
        if annotation is int or annotation is float:
            return PrimitiveType("number")
        if annotation is str:
            return PrimitiveType("string")
        if annotation is bool:
            return PrimitiveType("boolean")
        if annotation in {bytes, bytearray, memoryview}:
            return PrimitiveType("Uint8Array")
        if annotation in {Any, object, inspect._empty}:
            return UnknownType()
        if isinstance(annotation, typing.TypeVar):
            return RawType(annotation.__name__)

        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin is typing.Annotated:
            if not args:
                return UnknownType()
            return AnnotatedType(base=self.from_hint(args[0]), metadata=tuple(args[1:]))

        if origin is typing.Union or origin is types.UnionType or isinstance(annotation, types.UnionType):
            return self._from_union(args)

        if origin in {
            AsyncGenerator,
            AsyncIterable,
            Generator,
            Iterable,
            Iterator,
            typing.AsyncGenerator,
            typing.AsyncIterable,
            typing.Generator,
            typing.Iterable,
            typing.Iterator,
        }:
            return StreamType(self.from_hint(args[0] if args else Any))

        if origin in {list, set, dict, tuple}:
            return self._from_collection(origin, args)

        if origin is typing.Literal:
            return self._from_literal(args)

        if inspect.isclass(annotation):
            if self._is_msgspec_struct(annotation):
                return self._from_msgspec_struct(annotation)
            if dataclasses.is_dataclass(annotation):
                return self._from_dataclass(annotation)

        return UnknownType()

    def _from_collection(self, origin: Any, args: tuple[Any, ...]) -> TypeNode:
        if origin is list:
            return ArrayType(self.from_hint(args[0] if args else Any))
        if origin is set:
            return GenericType("Set", (self.from_hint(args[0] if args else Any),))
        if origin is dict:
            key = self.from_hint(args[0] if args else str)
            value = self.from_hint(args[1] if len(args) > 1 else Any)
            return GenericType("Record", (key, value))
        if origin is tuple:
            if len(args) == 2 and args[1] is Ellipsis:
                return ArrayType(self.from_hint(args[0]))
            return TupleType(tuple(self.from_hint(arg) for arg in args))
        return UnknownType()

    def _from_union(self, args: tuple[Any, ...]) -> TypeNode:
        if not args:
            return UnknownType()
        return UnionType(tuple(self.from_hint(arg) for arg in args))

    def _from_literal(self, args: tuple[Any, ...]) -> TypeNode:
        if not args:
            return UnknownType()
        if len(args) == 1:
            return LiteralType(args[0])
        return UnionType(tuple(LiteralType(arg) for arg in args))

    def _from_msgspec_struct(self, model: type[Any]) -> TypeNode:
        cached = self._model_cache.get(model)
        if cached is not None:
            return cached
        if model in self._building:
            return NamedTypeRef(model.__name__)

        self._building.add(model)
        try:
            fields: dict[str, TypeNode] = {}
            optional_fields: set[str] = set()
            for field in msgspec.structs.fields(model):
                fields[field.name] = self.from_hint(field.type)
                if field.default is not _NODEFAULT or field.default_factory is not _NODEFAULT:
                    optional_fields.add(field.name)
            result = ObjectType(fields=fields, name=model.__name__, optional_fields=optional_fields)
            self._model_cache[model] = result
            return result
        finally:
            self._building.remove(model)

    def _from_dataclass(self, model: type[Any]) -> TypeNode:
        cached = self._model_cache.get(model)
        if cached is not None:
            return cached
        if model in self._building:
            return NamedTypeRef(model.__name__)

        self._building.add(model)
        try:
            hints = typing.get_type_hints(model, include_extras=True)
            fields: dict[str, TypeNode] = {}
            optional_fields: set[str] = set()
            for field in dataclasses.fields(model):
                fields[field.name] = self.from_hint(hints.get(field.name, field.type))
                if field.default is not dataclasses.MISSING or field.default_factory is not dataclasses.MISSING:
                    optional_fields.add(field.name)
            result = ObjectType(fields=fields, name=model.__name__, optional_fields=optional_fields)
            self._model_cache[model] = result
            return result
        finally:
            self._building.remove(model)

    def _is_msgspec_struct(self, annotation: type[Any]) -> bool:
        return issubclass(annotation, msgspec.Struct)


__all__ = ("TypeExtractor",)
