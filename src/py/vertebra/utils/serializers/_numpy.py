"""NumPy serialization helpers for ``vertebra.utils.serializers``.

Aligned with vtb.
"""

from typing import Any

from vertebra.typing import NUMPY_INSTALLED
from vertebra.utils.module_loader import import_optional

__all__ = ("numpy_array_enc_hook",)


def numpy_array_enc_hook(value: Any) -> Any:
    """Encode NumPy arrays and scalars to JSON-compatible values."""
    if not NUMPY_INSTALLED:
        return value

    np = import_optional("numpy")
    if np is None:
        return value
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value
