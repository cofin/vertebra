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
    if name == "Struct":
        msgspec_mod = import_optional("msgspec")
        if msgspec_mod is None:
            raise MissingDependencyError("msgspec", install_package="msgspec")
        return msgspec_mod.Struct

    if name == "BaseModel":
        pydantic_mod = import_optional("pydantic")
        if pydantic_mod is None:
            raise MissingDependencyError("pydantic", install_package="pydantic")
        return pydantic_mod.BaseModel

    if name in {"ndarray", "NDArray"}:
        np_mod = import_optional("numpy")
        if np_mod is None:
            raise MissingDependencyError("numpy", install_package="numpy")
        if name == "NDArray":
            return np_mod.typing.NDArray
        return np_mod.ndarray

    if name in {"Table", "RecordBatch"}:
        pa_mod = import_optional("pyarrow")
        if pa_mod is None:
            raise MissingDependencyError("pyarrow", install_package="pyarrow")
        return getattr(pa_mod, name)

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
