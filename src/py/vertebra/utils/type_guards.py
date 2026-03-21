"""Type guard functions for runtime type checking in Vertebra.

Aligned with the vtb utility pattern.
"""

from dataclasses import fields as dataclasses_fields
from dataclasses import is_dataclass as dataclasses_is_dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from vertebra._typing import (
    ATTRS_INSTALLED,
    MSGSPEC_INSTALLED,
    PYDANTIC_INSTALLED,
    BaseModel,
    DataclassProtocol,
    Struct,
    attrs_has,
)

if TYPE_CHECKING:
    from typing import TypeGuard

    from vertebra._typing import BaseModelStub, StructStub

__all__ = (
    "dataclass_to_dict",
    "has_dict_attribute",
    "is_attrs_instance",
    "is_dataclass",
    "is_dataclass_instance",
    "is_dict",
    "is_msgspec_struct",
    "is_pydantic_model",
)


@runtime_checkable
class DictProtocol(Protocol):
    """Protocol for objects with a __dict__ attribute."""

    __dict__: dict[str, Any]


def is_dataclass_instance(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass instance."""
    if isinstance(obj, type):
        return False
    return dataclasses_is_dataclass(obj)


def is_dataclass(obj: Any) -> "TypeGuard[type[DataclassProtocol]]":
    """Check if an object is a dataclass type."""
    return dataclasses_is_dataclass(obj)


def is_pydantic_model(obj: Any) -> "TypeGuard[BaseModelStub]":
    """Check if a value is a pydantic model class or instance."""
    if not PYDANTIC_INSTALLED:
        return False
    if isinstance(obj, type):
        try:
            return issubclass(obj, BaseModel)
        except TypeError:
            return False
    return isinstance(obj, BaseModel)


def is_msgspec_struct(obj: Any) -> "TypeGuard[StructStub]":
    """Check if a value is a msgspec struct class or instance."""
    if not MSGSPEC_INSTALLED:
        return False
    if isinstance(obj, type):
        try:
            return issubclass(obj, Struct)
        except TypeError:
            return False
    return isinstance(obj, Struct)


def is_attrs_instance(obj: Any) -> bool:
    """Check if a value is an attrs class instance."""
    return bool(ATTRS_INSTALLED) and attrs_has(obj.__class__)


def is_dict(obj: Any) -> "TypeGuard[dict[str, Any]]":
    """Check if a value is a dictionary."""
    return type(obj) is dict


def has_dict_attribute(obj: Any) -> "TypeGuard[DictProtocol]":
    """Check if an object has a __dict__ attribute."""
    return isinstance(obj, DictProtocol)


def dataclass_to_dict(
    obj: DataclassProtocol,
    exclude_none: bool = False,
    exclude_empty: bool = False,
    convert_nested: bool = True,
) -> dict[str, Any]:
    """Convert a dataclass instance to a dictionary."""
    ret = {}
    for field in dataclasses_fields(obj):
        value = getattr(obj, field.name)
        if exclude_none and value is None:
            continue
        # For now, exclude_empty is handled as exclude_none in this simple version
        if exclude_empty and value is None:
            continue

        if is_dataclass_instance(value) and convert_nested:
            ret[field.name] = dataclass_to_dict(value, exclude_none=exclude_none, exclude_empty=exclude_empty)
        else:
            ret[field.name] = value
    return cast("dict[str, Any]", ret)
