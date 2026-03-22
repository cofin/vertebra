# mypy: warn-unused-ignores=false
# ruff: noqa: RUF100, PLR0913, A002, DOC201, PLR6301, PLR0917, ARG004, ARG002, ARG001
"""Internal cross-cutting type aliases and library shims for Vertebra.

These types are shared across multiple subpackages (server/, framework/, etc.).
Import from here to avoid defining the same alias in multiple places.
Includes compatibility shims for optional dependencies.
"""

import enum
from collections.abc import Awaitable, Callable, Sequence
from enum import Enum
from importlib.util import find_spec
from typing import (
    Any,
    ClassVar,
    Literal,
    Protocol,
    dataclass_transform,
    runtime_checkable,
)

from typing_extensions import TypeVar

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

# ---------------------------------------------------------------------------
# Core Dependency Availability (Moved here to avoid circular imports)
# ---------------------------------------------------------------------------

_dependency_cache: dict[str, bool] = {}


def module_available(module_name: str) -> bool:
    """Return True if the given module can be resolved."""
    cached = _dependency_cache.get(module_name)
    if cached is not None:
        return cached

    try:
        is_available = find_spec(module_name) is not None
    except (ModuleNotFoundError, ValueError):
        is_available = False

    _dependency_cache[module_name] = is_available
    return is_available


class OptionalDependencyFlag:
    """Boolean-like wrapper that evaluates module availability lazily."""

    __slots__ = ("module_name",)

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name

    def __bool__(self) -> bool:
        return module_available(self.module_name)

    def __repr__(self) -> str:
        status = "available" if module_available(self.module_name) else "missing"
        return f"OptionalDependencyFlag(module='{self.module_name}', status='{status}')"


def dependency_flag(module_name: str) -> OptionalDependencyFlag:
    """Return a lazily evaluated flag for the supplied module name."""
    return OptionalDependencyFlag(module_name)


# ---------------------------------------------------------------------------
# Sentinel types
# ---------------------------------------------------------------------------


class EmptyType(Enum):
    """A sentinel type for empty values."""

    Empty = enum.auto()


Empty = EmptyType.Empty


class UnsetType(Enum):
    """A sentinel type for unset values."""

    Unset = enum.auto()


UNSET = UnsetType.Unset


# ---------------------------------------------------------------------------
# Native batch protocol types
# ---------------------------------------------------------------------------

HandlerID = int
"""Stable uint64 handler identifier (FNV-1a of method + path)."""

RawHeaders = list[tuple[str, str]]
"""HTTP headers as a list of (name, value) string pairs."""

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

LifecycleHook = Callable[[], Any | Awaitable[Any]]
"""Sync or async callable invoked at startup/shutdown."""


# ---------------------------------------------------------------------------
# Library Shims (from sqlspec)
# ---------------------------------------------------------------------------


@runtime_checkable
class DataclassProtocol(Protocol):
    """Protocol for instance checking dataclasses."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


class BaseModelStub:
    """Placeholder implementation."""

    model_fields: ClassVar[dict[str, Any]] = {}
    __slots__ = ("__dict__", "__pydantic_extra__", "__pydantic_fields_set__", "__pydantic_private__")

    def __init__(self, **data: Any) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def model_dump(
        self,
        /,
        *,
        include: Any | None = None,
        exclude: Any | None = None,
        context: Any | None = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal["none", "warn", "error"] = True,
        serialize_as_any: bool = False,
    ) -> dict[str, Any]:
        """Placeholder implementation."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


# Try to import real implementations at runtime
try:
    from pydantic import BaseModel as _RealBaseModel

    BaseModel = _RealBaseModel
except ImportError:
    BaseModel = BaseModelStub  # type: ignore[assignment]


@dataclass_transform()
class StructStub:
    """Placeholder implementation."""

    __struct_fields__: ClassVar[tuple[str, ...]] = ()
    __slots__ = ()

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


try:
    from msgspec import Struct as _RealStruct

    Struct = _RealStruct
except ImportError:
    Struct = StructStub  # type: ignore[assignment,misc]


try:
    import orjson  # noqa: F401
except ImportError:
    orjson = None  # type: ignore[assignment]  # noqa: F841


@dataclass_transform()
class AttrsInstanceStub:
    """Placeholder Implementation for attrs classes"""

    __attrs_attrs__: ClassVar[tuple[Any, ...]] = ()
    __slots__ = ()

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def attrs_asdict_stub(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Placeholder implementation"""
    return {}


try:
    from attrs import AttrsInstance as _RealAttrsInstance
    from attrs import asdict as _real_attrs_asdict
    from attrs import has as _real_attrs_has

    AttrsInstance = _RealAttrsInstance
    attrs_asdict = _real_attrs_asdict
    attrs_has = _real_attrs_has
except ImportError:
    AttrsInstance = AttrsInstanceStub  # type: ignore[assignment]
    attrs_asdict = attrs_asdict_stub

    def attrs_has(obj: Any) -> bool:
        return False


@runtime_checkable
class NumpyArrayStub(Protocol):
    """Protocol stub for numpy.ndarray."""

    def tolist(self) -> list[Any]: ...


try:
    from numpy import ndarray as NumpyArray
except ImportError:
    NumpyArray = NumpyArrayStub  # type: ignore[assignment]


class OffsetPagination[T]:
    """Container for data returned using limit/offset pagination."""

    __slots__ = ("items", "limit", "offset", "total")

    items: Sequence[T]
    limit: int
    offset: int
    total: int

    def __init__(self, items: Sequence[T], limit: int, offset: int, total: int) -> None:
        self.items = items
        self.limit = limit
        self.offset = offset
        self.total = total


# ---------------------------------------------------------------------------
# Installation Flags
# ---------------------------------------------------------------------------

ATTRS_INSTALLED = dependency_flag("attrs")
MSGSPEC_INSTALLED = dependency_flag("msgspec")
NUMPY_INSTALLED = dependency_flag("numpy")
ORJSON_INSTALLED = dependency_flag("orjson")
PYDANTIC_INSTALLED = dependency_flag("pydantic")
SQLSPEC_INSTALLED = dependency_flag("sqlspec")
UVLOOP_INSTALLED = dependency_flag("uvloop")
UUID_UTILS_INSTALLED = dependency_flag("uuid_utils")

__all__ = (
    "ATTRS_INSTALLED",
    "AttrsInstance",
    "BaseModel",
    "DataclassProtocol",
    "Empty",
    "EmptyType",
    "HandlerID",
    "LifecycleHook",
    "MSGSPEC_INSTALLED",
    "NUMPY_INSTALLED",
    "NumpyArray",
    "ORJSON_INSTALLED",
    "OffsetPagination",
    "PYDANTIC_INSTALLED",
    "RawHeaders",
    "SQLSPEC_INSTALLED",
    "Struct",
    "UNSET",
    "UVLOOP_INSTALLED",
    "UUID_UTILS_INSTALLED",
    "UnsetType",
    "attrs_asdict",
    "attrs_has",
    "module_available",
    "reset_dependency_cache",
)


def reset_dependency_cache(module_name: str | None = None) -> None:
    """Clear cached availability for one module or the entire cache."""
    if module_name is None:
        _dependency_cache.clear()
        return
    _dependency_cache.pop(module_name, None)
