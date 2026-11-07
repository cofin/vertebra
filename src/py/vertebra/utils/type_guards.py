"""Type guard functions and structural protocols for runtime type checking in Vertebra."""

from __future__ import annotations

import inspect
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import Field
from dataclasses import fields as dataclasses_fields
from dataclasses import is_dataclass as dataclasses_is_dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, is_typeddict, runtime_checkable

import msgspec
from msgspec import Struct

from vertebra import typing as vertebra_typing
from vertebra.typing import (
    ATTRS_INSTALLED,
    PYDANTIC_INSTALLED,
    UNSET,
    DataclassProtocol,
    Empty,
)

if TYPE_CHECKING:
    from vertebra.typing import BaseModelStub

__all__ = (
    "AsyncReadable",
    "DictProtocol",
    "SupportsClose",
    "SyncReadable",
    "dataclass_to_dict",
    "extract_dataclass_fields",
    "extract_dataclass_items",
    "get_msgspec_rename_config",
    "has_dict_attribute",
    "is_async_generator_callable",
    "is_async_readable",
    "is_attrs_instance",
    "is_attrs_schema",
    "is_coroutine_callable",
    "is_dataclass",
    "is_dataclass_instance",
    "is_dict",
    "is_dict_row",
    "is_generator_callable",
    "is_iterable_parameters",
    "is_mapping_like",
    "is_msgspec_struct",
    "is_msgspec_struct_type",
    "is_pydantic_model",
    "is_pydantic_model_type",
    "is_readable",
    "is_schema",
    "is_schema_or_dict",
    "is_schema_or_dict_sequence",
    "is_typed_dict",
    "supports_close",
)


@runtime_checkable
class DictProtocol(Protocol):
    """Protocol for objects with a ``__dict__`` attribute."""

    __dict__: dict[str, Any]


@runtime_checkable
class SyncReadable(Protocol):
    """Protocol for synchronous readable file-like streams."""

    def read(self, size: int = -1, /) -> Any:
        """Read up to ``size`` bytes or characters from the stream."""


@runtime_checkable
class AsyncReadable(Protocol):
    """Protocol for asynchronous readable file-like streams."""

    async def read(self, size: int = -1, /) -> Any:
        """Asynchronously read up to ``size`` bytes or characters from the stream."""


@runtime_checkable
class SupportsClose(Protocol):
    """Protocol for resources exposing a synchronous ``close()`` method."""

    def close(self) -> Any:
        """Close the underlying resource."""


def is_dataclass_instance(obj: Any) -> TypeGuard[DataclassProtocol]:
    """Check if an object is a dataclass instance (not a dataclass type)."""
    if isinstance(obj, type):
        return False
    return dataclasses_is_dataclass(obj)


def is_dataclass(obj: Any) -> TypeGuard[type[DataclassProtocol]]:
    """Check if an object is a dataclass type or instance."""
    return dataclasses_is_dataclass(obj)


def is_pydantic_model(obj: Any) -> TypeGuard[BaseModelStub]:
    """Check if a value is a Pydantic BaseModel class or instance."""
    if not PYDANTIC_INSTALLED:
        return False
    base_model = vertebra_typing.BaseModel
    if isinstance(obj, type):
        try:
            return issubclass(obj, base_model)
        except TypeError:
            return False
    return isinstance(obj, base_model)


def is_pydantic_model_type(obj: Any) -> TypeGuard[type[BaseModelStub]]:
    """Check if ``obj`` is a Pydantic BaseModel subclass rather than an instance."""
    if not isinstance(obj, type):
        return False
    if hasattr(obj, "__pydantic_validator__"):
        return True
    if not PYDANTIC_INSTALLED:
        return False
    try:
        return issubclass(obj, vertebra_typing.BaseModel)
    except TypeError:
        return False


def is_msgspec_struct(obj: Any) -> TypeGuard[Struct]:
    """Check if a value is a ``msgspec.Struct`` class or instance."""
    if isinstance(obj, type):
        try:
            return issubclass(obj, Struct)
        except TypeError:
            return False
    return isinstance(obj, Struct)


def is_msgspec_struct_type(obj: Any) -> TypeGuard[type[Struct]]:
    """Check if ``obj`` is a ``msgspec.Struct`` subclass rather than an instance."""
    if not isinstance(obj, type):
        return False
    try:
        return issubclass(obj, Struct)
    except TypeError:
        return False


def is_coroutine_callable(obj: Any) -> bool:
    """Check if ``obj`` is an async coroutine function or callable object with an async ``__call__``."""
    if inspect.iscoroutinefunction(obj):
        return True
    if callable(obj) and not isinstance(obj, type):
        return inspect.iscoroutinefunction(type(obj).__call__)
    return False


def is_async_generator_callable(obj: Any) -> bool:
    """Check if ``obj`` is an async generator function or callable object with an async generator ``__call__``."""
    if inspect.isasyncgenfunction(obj):
        return True
    if callable(obj) and not isinstance(obj, type):
        return inspect.isasyncgenfunction(type(obj).__call__)
    return False


def is_generator_callable(obj: Any) -> bool:
    """Check if ``obj`` is a generator function or callable object with a generator ``__call__``."""
    if inspect.isgeneratorfunction(obj):
        return True
    if callable(obj) and not isinstance(obj, type):
        return inspect.isgeneratorfunction(type(obj).__call__)
    return False


def is_attrs_instance(obj: Any) -> bool:
    """Check if a value is an ``attrs`` class instance."""
    if isinstance(obj, type) or not ATTRS_INSTALLED:
        return False
    return bool(vertebra_typing.attrs_has(obj.__class__))


def is_attrs_schema(obj: Any) -> bool:
    """Check if a value is an ``attrs`` class or instance."""
    if not ATTRS_INSTALLED:
        return False
    target = obj if isinstance(obj, type) else obj.__class__
    return bool(vertebra_typing.attrs_has(target))


def is_typed_dict(obj: Any) -> bool:
    """Check if ``obj`` is a ``typing.TypedDict`` class."""
    return isinstance(obj, type) and is_typeddict(obj)


def is_dict(obj: Any) -> TypeGuard[dict[str, Any]]:
    """Check if a value is an exact ``dict`` or ``Mapping`` instance."""
    return isinstance(obj, dict)


def is_dict_row(obj: Any) -> TypeGuard[dict[str, Any]]:
    """Check if a database row value is a ``dict``."""
    return isinstance(obj, dict)


def is_mapping_like(obj: Any) -> TypeGuard[Mapping[str, Any]]:
    """Check if ``obj`` implements ``collections.abc.Mapping``."""
    return isinstance(obj, Mapping)


def is_iterable_parameters(obj: Any) -> TypeGuard[Iterable[Any]]:
    """Check if ``obj`` is an iterable parameter sequence (excluding strings, bytes, and mappings)."""
    if isinstance(obj, (str, bytes, bytearray, memoryview, Mapping)):
        return False
    return isinstance(obj, Iterable)


def is_readable(obj: Any) -> TypeGuard[SyncReadable]:
    """Check if ``obj`` has a synchronous callable ``read`` method."""
    read_attr = getattr(obj, "read", None)
    return callable(read_attr) and not inspect.iscoroutinefunction(read_attr)


def is_async_readable(obj: Any) -> TypeGuard[AsyncReadable]:
    """Check if ``obj`` has an asynchronous coroutine ``read`` method."""
    read_attr = getattr(obj, "read", None)
    return callable(read_attr) and inspect.iscoroutinefunction(read_attr)


def supports_close(obj: Any) -> TypeGuard[SupportsClose]:
    """Check if ``obj`` exposes a callable ``close`` method."""
    close_attr = getattr(obj, "close", None)
    return callable(close_attr)


def is_schema(obj: Any) -> bool:
    """Check if ``obj`` is a msgspec Struct, dataclass, Pydantic model, or attrs class/instance."""
    return is_msgspec_struct(obj) or is_dataclass(obj) or is_pydantic_model(obj) or is_attrs_schema(obj)


def is_schema_or_dict(obj: Any) -> bool:
    """Check if ``obj`` is a schema instance (Struct, dataclass, Pydantic, attrs) or a ``dict``."""
    if isinstance(obj, type):
        return False
    return is_dict(obj) or is_schema(obj)


def is_schema_or_dict_sequence(obj: Any) -> TypeGuard[Sequence[Any]]:
    """Check if ``obj`` is a non-empty sequence of schema instances or dictionaries."""
    if isinstance(obj, (str, bytes, bytearray, memoryview, Mapping)) or not isinstance(obj, Sequence):
        return False
    if not obj:
        return False
    return is_schema_or_dict(obj[0])


def has_dict_attribute(obj: Any) -> TypeGuard[DictProtocol]:
    """Check if an object has a ``__dict__`` attribute."""
    return isinstance(obj, DictProtocol)


def get_msgspec_rename_config(schema_type: Any) -> str | None:
    """Inspect a ``msgspec.Struct`` type and return its field rename configuration (``'camel'``, ``'kebab'``, ``'pascal'``, or ``None``)."""
    target = schema_type if isinstance(schema_type, type) else type(schema_type)
    if not is_msgspec_struct(target):
        return None
    struct_config = getattr(target, "__struct_config__", None)
    if struct_config is not None:
        fields = getattr(target, "__struct_fields__", ())
        encode_fields = getattr(target, "__struct_encode_fields__", ())
        if fields and encode_fields and fields != encode_fields:
            for orig, encoded in zip(fields, encode_fields, strict=False):
                if orig != encoded:
                    if "-" in encoded:
                        return "kebab"
                    if encoded[:1].isupper():
                        return "pascal"
                    return "camel"
    try:
        type_info = msgspec.inspect.type_info(target)
    except Exception:
        return None
    if isinstance(type_info, msgspec.inspect.StructType):
        for f in type_info.fields:
            if f.name != f.encode_name:
                if "-" in f.encode_name:
                    return "kebab"
                if f.encode_name[:1].isupper():
                    return "pascal"
                return "camel"
    return None


def extract_dataclass_fields(
    obj: DataclassProtocol,
    *,
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: set[str] | frozenset[str] | None = None,
    exclude: set[str] | frozenset[str] | None = None,
) -> tuple[Field[Any], ...]:
    """Extract ``dataclasses.Field`` descriptors from a dataclass instance with filtering."""
    if not is_dataclass_instance(obj):
        msg = f"Expected a dataclass instance, got {type(obj)!r}"
        raise TypeError(msg)
    selected: list[Field[Any]] = []
    for field in dataclasses_fields(obj):
        if include is not None and field.name not in include:
            continue
        if exclude is not None and field.name in exclude:
            continue
        value = getattr(obj, field.name)
        if exclude_none and value is None:
            continue
        if exclude_empty and (value is Empty or value is UNSET):
            continue
        selected.append(field)
    return tuple(selected)


def extract_dataclass_items(
    obj: DataclassProtocol,
    *,
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: set[str] | frozenset[str] | None = None,
    exclude: set[str] | frozenset[str] | None = None,
) -> tuple[tuple[str, Any], ...]:
    """Extract ``(field_name, value)`` pairs from a dataclass instance with filtering."""
    filtered_fields = extract_dataclass_fields(
        obj,
        exclude_none=exclude_none,
        exclude_empty=exclude_empty,
        include=include,
        exclude=exclude,
    )
    return tuple((field.name, getattr(obj, field.name)) for field in filtered_fields)


def dataclass_to_dict(
    obj: DataclassProtocol,
    exclude_none: bool = False,
    exclude_empty: bool = False,
    convert_nested: bool = True,
) -> dict[str, Any]:
    """Convert a dataclass instance to a dictionary."""
    ret: dict[str, Any] = {}
    for field in extract_dataclass_fields(
        obj,
        exclude_none=exclude_none,
        exclude_empty=exclude_empty,
    ):
        value = getattr(obj, field.name)
        if is_dataclass_instance(value) and convert_nested:
            ret[field.name] = dataclass_to_dict(value, exclude_none=exclude_none, exclude_empty=exclude_empty)
        else:
            ret[field.name] = value
    return ret
