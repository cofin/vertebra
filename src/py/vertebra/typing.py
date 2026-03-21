"""Public typing facade for Vertebra applications.

Provides user-facing type aliases and conditional exports for optional
dependencies. Application developers import from here instead of reaching
into private modules.

Usage::

    from vertebra.typing import HandlerID, RawHeaders

    # Optional dep types — available if the package is installed,
    # otherwise raises MissingDependencyError at runtime.
    from vertebra.typing import Struct       # msgspec
    from vertebra.typing import BaseModel    # pydantic
"""

from typing import TYPE_CHECKING, Any

from vertebra._typing import HandlerID, LifecycleHook, RawHeaders
from vertebra.utils.module_loader import MissingDependencyError, module_available

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Re-exports from _typing
# ---------------------------------------------------------------------------

__all__ = (
    "HandlerID",
    "LifecycleHook",
    "RawHeaders",
)

# ---------------------------------------------------------------------------
# Optional dependency type facades
# ---------------------------------------------------------------------------


def __getattr__(name: str) -> Any:
    """Lazy-load optional dependency types on first access."""
    if name == "Struct":
        if not module_available("msgspec"):
            raise MissingDependencyError("msgspec", install_package="msgspec")
        import msgspec

        return msgspec.Struct

    if name == "BaseModel":
        if not module_available("pydantic"):
            raise MissingDependencyError("pydantic", install_package="pydantic")
        from pydantic import BaseModel

        return BaseModel

    if name == "ndarray" or name == "NDArray":
        if not module_available("numpy"):
            raise MissingDependencyError("numpy", install_package="numpy")
        import numpy as np

        if name == "NDArray":
            return np.typing.NDArray
        return np.ndarray

    if name == "Table":
        if not module_available("pyarrow"):
            raise MissingDependencyError("pyarrow", install_package="pyarrow")
        import pyarrow as pa

        return pa.Table

    if name == "RecordBatch":
        if not module_available("pyarrow"):
            raise MissingDependencyError("pyarrow", install_package="pyarrow")
        import pyarrow as pa

        return pa.RecordBatch

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
