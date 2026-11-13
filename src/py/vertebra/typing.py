"""Public typing facade for Vertebra applications and internal subpackages.

Provides user-facing type aliases, base structs, exceptions, sentinels, and
conditional exports for optional dependencies. Always import from ``vertebra.typing``
instead of ``vertebra._typing``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra import _typing
from vertebra._typing import (
    ATTRS_INSTALLED,
    NANOID_INSTALLED,
    NUMPY_INSTALLED,
    PYDANTIC_INSTALLED,
    UNSET,
    UUID_UTILS_INSTALLED,
    UVLOOP_INSTALLED,
    BaseModelStub,
    DataclassProtocol,
    Empty,
    EmptyType,
    HandlerID,
    ImproperConfigurationError,
    LifecycleHook,
    MissingDependencyError,
    OptionalDependency,
    PreconditionError,
    RawHeaders,
    ToolPolicyError,
    ToolProvider,
    UnsupportedBackendError,
    VertebraError,
    attrs_asdict_stub,
    dependency_flag,
    module_available,
    reset_dependency_cache,
)

if TYPE_CHECKING:
    from vertebra._typing import (
        AttrsInstance,
        BaseModel,
        BaseStruct,
        CamelizedBaseStruct,
        FrozenBaseStruct,
        FrozenCamelizedStruct,
        NumpyArray,
        Struct,
        attrs_asdict,
        attrs_has,
    )

    NDArray: Any = Any
    ndarray: Any = Any
    Table: Any = Any
    RecordBatch: Any = Any

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
    "NDArray",
    "NumpyArray",
    "OptionalDependency",
    "PreconditionError",
    "RawHeaders",
    "RecordBatch",
    "Struct",
    "ToolPolicyError",
    "ToolProvider",
    "Table",
    "UnsupportedBackendError",
    "VertebraError",
    "attrs_asdict",
    "attrs_asdict_stub",
    "attrs_has",
    "dependency_flag",
    "module_available",
    "ndarray",
    "reset_dependency_cache",
)


_LAZY_OPTIONAL_TYPES = frozenset({"BaseModel", "ndarray", "NDArray", "Table", "RecordBatch"})
_DELEGATED_TYPING_ATTRS = frozenset(
    {
        "AttrsInstance",
        "BaseStruct",
        "CamelizedBaseStruct",
        "FrozenBaseStruct",
        "FrozenCamelizedStruct",
        "NumpyArray",
        "Struct",
        "attrs_asdict",
        "attrs_has",
    }
)


def __getattr__(name: str) -> Any:
    """Lazy-load optional dependency types on first access."""
    if name in _DELEGATED_TYPING_ATTRS:
        val = getattr(_typing, name)
        globals()[name] = val
        return val

    if name not in _LAZY_OPTIONAL_TYPES:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)

    from vertebra.utils.module_loader import import_optional

    val = None
    if name == "BaseModel":
        pydantic_mod = import_optional("pydantic")
        if pydantic_mod is None:
            raise MissingDependencyError("pydantic", install_package="pydantic")
        val = pydantic_mod.BaseModel

    elif name in {"ndarray", "NDArray"}:
        np_mod = import_optional("numpy")
        if np_mod is None:
            raise MissingDependencyError("numpy", install_package="numpy")
        val = np_mod.typing.NDArray if name == "NDArray" else np_mod.ndarray

    elif name in {"Table", "RecordBatch"}:
        pa_mod = import_optional("pyarrow")
        if pa_mod is None:
            raise MissingDependencyError("pyarrow", install_package="pyarrow")
        val = getattr(pa_mod, name)

    globals()[name] = val
    return val


def __dir__() -> list[str]:
    """Return public export symbols and module globals for dir()."""
    return sorted(set(__all__) | set(globals().keys()))
