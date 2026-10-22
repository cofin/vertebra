"""Public typing facade for Vertebra applications.

Provides user-facing type aliases and conditional exports for optional
dependencies. Application developers import from here instead of reaching
into private modules.
"""

from typing import Any

from vertebra._typing import HandlerID, LifecycleHook, RawHeaders
from vertebra.utils.module_loader import MissingDependencyError, import_optional

__all__ = (
    "HandlerID",
    "LifecycleHook",
    "RawHeaders",
)


def __getattr__(name: str) -> Any:
    """Lazy-load optional dependency types on first access."""
    val: Any = None
    if name == "Struct":
        msgspec_mod = import_optional("msgspec")
        if msgspec_mod is None:
            raise MissingDependencyError("msgspec", install_package="msgspec")
        val = msgspec_mod.Struct

    elif name == "BaseModel":
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

    if val is not None:
        globals()[name] = val
        return val

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return public export symbols and module globals for dir()."""
    return sorted(
        set(__all__) | {"Struct", "BaseModel", "NDArray", "ndarray", "Table", "RecordBatch"} | set(globals().keys())
    )
