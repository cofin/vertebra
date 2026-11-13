"""Internal cross-cutting type aliases, exceptions, and library shims for Vertebra.

These types are shared across multiple subpackages (server/, framework/, etc.).
Import from here to avoid defining the same alias in multiple places.
Includes lazy compatibility shims for optional dependencies.
"""

from __future__ import annotations

import contextlib
import enum
import sys
from collections.abc import Awaitable, Callable, Mapping
from enum import Enum
from importlib.util import find_spec
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    Protocol,
    runtime_checkable,
)

__all__ = (
    "ATTRS_INSTALLED",
    "NANOID_INSTALLED",
    "NUMPY_INSTALLED",
    "PYDANTIC_INSTALLED",
    "UNSET",
    "UUID_UTILS_INSTALLED",
    "UVLOOP_INSTALLED",
    "AttrsInstance",
    "BaseModel",
    "BaseModelStub",
    "BaseStruct",
    "CamelizedBaseStruct",
    "DataclassProtocol",
    "Empty",
    "EmptyType",
    "FrozenBaseStruct",
    "FrozenCamelizedStruct",
    "HandlerID",
    "ImproperConfigurationError",
    "LifecycleHook",
    "MissingDependencyError",
    "NumpyArray",
    "OptionalDependency",
    "PreconditionError",
    "RawHeaders",
    "Struct",
    "ToolPolicyError",
    "ToolProvider",
    "UnsupportedBackendError",
    "VertebraError",
    "attrs_asdict",
    "attrs_asdict_stub",
    "attrs_has",
    "dependency_flag",
    "module_available",
    "reset_dependency_cache",
)


class VertebraError(Exception):
    """Base exception for Vertebra runtime and configuration errors."""


class ToolPolicyError(VertebraError, PermissionError):
    """An adapter tool operation is denied by its explicit execution policy."""


@runtime_checkable
class ToolProvider(Protocol):
    """Supply typed asynchronous operations and side-effect-free approval policy."""

    def operations(self) -> Mapping[str, Callable[..., Awaitable[Any]]]:
        """Return named operation callables without resolving external resources."""
        ...

    def confirmation_reason(self, operation_name: str, arguments: Mapping[str, Any]) -> str | None:
        """Return an approval reason or raise ToolPolicyError for a denied operation."""
        ...


class MissingDependencyError(VertebraError, ImportError):
    """Raised when an optional dependency is required but unavailable."""

    def __init__(
        self,
        package: str,
        *,
        install_package: str | None = None,
        extra: str | None = None,
    ) -> None:
        self.package = package
        self.install_package = install_package or (f"vertebra-core[{extra}]" if extra else package)
        self.message = f"Optional dependency '{package}' is required but not installed."
        self.hint = f"Install it with: uv add {self.install_package}"
        super().__init__(self.message)

    def show(self, file: object = None) -> None:
        """Render the missing dependency warning and installation hint."""
        _ = file
        from vertebra.cli.theme import console

        console.warning(str(self))
        if self.hint:
            console.step(self.hint)


class ImproperConfigurationError(VertebraError, ValueError):
    """Raised when configuration is invalid."""


class PreconditionError(VertebraError, OSError):
    """Raised when a conditional storage or resource precondition fails."""


class UnsupportedBackendError(VertebraError):
    """Raised when a requested backend, driver, or provider is not supported."""


class OptionalDependency:
    """Boolean-like wrapper that evaluates module availability lazily."""

    __slots__ = ("module_name",)

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name

    def __bool__(self) -> bool:
        return module_available(self.module_name)

    def __repr__(self) -> str:
        status = "available" if module_available(self.module_name) else "missing"
        return f"OptionalDependency(module='{self.module_name}', status='{status}')"


class EmptyType(Enum):
    """A sentinel type for empty values."""

    Empty = enum.auto()


Empty = EmptyType.Empty
UNSET: Final = EmptyType.Empty

HandlerID = int
"""Stable uint64 handler identifier (FNV-1a of method + path)."""

RawHeaders = list[tuple[str, str]]
"""HTTP headers as a list of (name, value) string pairs."""

LifecycleHook = Callable[[], Any | Awaitable[Any]]
"""Sync or async callable invoked at startup/shutdown."""


@runtime_checkable
class DataclassProtocol(Protocol):
    """Protocol for instance checking dataclasses."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


class BaseModelStub:
    """Placeholder implementation for Pydantic BaseModel."""

    model_fields: ClassVar[dict[str, Any]] = {}
    __slots__ = ("__dict__", "__pydantic_extra__", "__pydantic_fields_set__", "__pydantic_private__")

    def __init__(self, **data: Any) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def model_dump(
        self,
        *,
        mode: str = "python",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return non-private instance attributes as a dictionary."""
        _ = (mode, kwargs)
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


def module_available(module_name: str) -> bool:
    """Return True if the given module can be resolved."""
    if module_name in sys.modules:
        return sys.modules[module_name] is not None

    cached = _dependency_cache.get(module_name)
    if cached is not None:
        return cached

    try:
        is_available = find_spec(module_name) is not None
    except (ModuleNotFoundError, ValueError):
        is_available = False

    _dependency_cache[module_name] = is_available
    return is_available


def reset_dependency_cache(module_name: str | None = None) -> None:
    """Clear cached availability flags for one module or the entire cache."""
    if module_name is None:
        _dependency_cache.clear()
        return
    _dependency_cache.pop(module_name, None)


def dependency_flag(module_name: str) -> OptionalDependency:
    """Return a lazily evaluated flag for the supplied module name."""
    return OptionalDependency(module_name)


def attrs_asdict_stub(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Placeholder implementation for attrs.asdict."""
    _ = (args, kwargs)
    return {}


ATTRS_INSTALLED = dependency_flag("attrs")
NANOID_INSTALLED = dependency_flag("fastnanoid")
NUMPY_INSTALLED = dependency_flag("numpy")
PYDANTIC_INSTALLED = dependency_flag("pydantic")
UVLOOP_INSTALLED = dependency_flag("uvloop")
UUID_UTILS_INSTALLED = dependency_flag("uuid_utils")

_dependency_cache: dict[str, bool] = {}


def _attrs_has_stub(obj: Any) -> bool:
    """Placeholder implementation for attrs.has."""
    _ = obj
    return False


_MSGSPEC_TYPING_ATTRS = frozenset(
    {
        "BaseStruct",
        "CamelizedBaseStruct",
        "FrozenBaseStruct",
        "FrozenCamelizedStruct",
        "Struct",
    }
)

if TYPE_CHECKING:
    import msgspec
    from msgspec import Struct

    class BaseStruct(Struct, gc=False, omit_defaults=True, repr_omit_defaults=True):
        """Mutable GC-free base struct for state objects."""

        def to_dict(self) -> dict[str, Any]:
            """Convert struct fields to a Python dictionary."""
            return msgspec.structs.asdict(self)

    class CamelizedBaseStruct(BaseStruct, rename="camel"):
        """Mutable GC-free base struct with camelCase wire field names."""

    class FrozenBaseStruct(Struct, frozen=True, gc=False, omit_defaults=True, repr_omit_defaults=True):
        """Immutable GC-free base struct for configs, metadata, and wire models."""

        def to_dict(self) -> dict[str, Any]:
            """Convert struct fields to a Python dictionary."""
            return msgspec.structs.asdict(self)

    class FrozenCamelizedStruct(FrozenBaseStruct, rename="camel", frozen=True):
        """Immutable GC-free base struct with camelCase wire field names."""

    BaseModel: Any = BaseModelStub
    AttrsInstance: Any = Any
    attrs_asdict: Callable[..., Any] = attrs_asdict_stub
    attrs_has: Callable[[Any], bool] = _attrs_has_stub
    NumpyArray: Any = Any


def __getattr__(name: str) -> Any:
    """Lazy-resolve optional third-party types and helpers on first access."""
    if name in _MSGSPEC_TYPING_ATTRS:
        import msgspec as _msgspec
        from msgspec import Struct as _RealStruct

        class _BaseStruct(_RealStruct, gc=False, omit_defaults=True, repr_omit_defaults=True):
            """Mutable GC-free base struct for state objects."""

            def to_dict(self) -> dict[str, Any]:
                """Convert struct fields to a Python dictionary."""
                return _msgspec.structs.asdict(self)

        class _CamelizedBaseStruct(_BaseStruct, rename="camel"):
            """Mutable GC-free base struct with camelCase wire field names."""

        class _FrozenBaseStruct(_RealStruct, frozen=True, gc=False, omit_defaults=True, repr_omit_defaults=True):
            """Immutable GC-free base struct for configs, metadata, and wire models."""

            def to_dict(self) -> dict[str, Any]:
                """Convert struct fields to a Python dictionary."""
                return _msgspec.structs.asdict(self)

        class _FrozenCamelizedStruct(_FrozenBaseStruct, rename="camel", frozen=True):
            """Immutable GC-free base struct with camelCase wire field names."""

        _BaseStruct.__name__ = _BaseStruct.__qualname__ = "BaseStruct"
        _CamelizedBaseStruct.__name__ = _CamelizedBaseStruct.__qualname__ = "CamelizedBaseStruct"
        _FrozenBaseStruct.__name__ = _FrozenBaseStruct.__qualname__ = "FrozenBaseStruct"
        _FrozenCamelizedStruct.__name__ = _FrozenCamelizedStruct.__qualname__ = "FrozenCamelizedStruct"

        globals()["Struct"] = _RealStruct
        globals()["BaseStruct"] = _BaseStruct
        globals()["CamelizedBaseStruct"] = _CamelizedBaseStruct
        globals()["FrozenBaseStruct"] = _FrozenBaseStruct
        globals()["FrozenCamelizedStruct"] = _FrozenCamelizedStruct
        return globals()[name]

    if name == "BaseModel":
        resolved: Any = BaseModelStub
        if PYDANTIC_INSTALLED:
            with contextlib.suppress(ImportError):
                from pydantic import BaseModel as _RealBaseModel

                resolved = _RealBaseModel
        globals()["BaseModel"] = resolved
        return resolved

    if name in {"AttrsInstance", "attrs_asdict", "attrs_has"}:
        resolved_instance: Any = Any
        resolved_asdict: Callable[..., Any] = attrs_asdict_stub
        resolved_has: Callable[[Any], bool] = _attrs_has_stub
        if ATTRS_INSTALLED:
            with contextlib.suppress(ImportError):
                from attrs import AttrsInstance as _RealAttrsInstance
                from attrs import asdict as _real_attrs_asdict
                from attrs import has as _real_attrs_has

                resolved_instance = _RealAttrsInstance
                resolved_asdict = _real_attrs_asdict
                resolved_has = _real_attrs_has
        globals()["AttrsInstance"] = resolved_instance
        globals()["attrs_asdict"] = resolved_asdict
        globals()["attrs_has"] = resolved_has
        return globals()[name]

    if name == "NumpyArray":
        resolved_np: Any = Any
        if NUMPY_INSTALLED:
            with contextlib.suppress(ImportError):
                from numpy import ndarray as _RealNumpyArray

                resolved_np = _RealNumpyArray
        globals()["NumpyArray"] = resolved_np
        return resolved_np

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
