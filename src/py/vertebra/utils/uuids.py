"""UUID and ID generation utilities with optional acceleration.

Provides wrapper functions for uuid3, uuid4, uuid5, uuid6, uuid7, and nanoid generation.
Uses uuid-utils and fastnanoid packages for performance when available,
falling back to standard library. Aligned with vtb.
"""

import warnings
from collections.abc import Callable
from importlib import import_module
from typing import Any, cast
from uuid import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500, UUID
from uuid import uuid3 as _stdlib_uuid3
from uuid import uuid4 as _stdlib_uuid4
from uuid import uuid5 as _stdlib_uuid5

__all__ = (
    "NAMESPACE_DNS",
    "NAMESPACE_OID",
    "NAMESPACE_URL",
    "NAMESPACE_X500",
    "NANOID_INSTALLED",
    "UUID_UTILS_INSTALLED",
    "nanoid",
    "uuid3",
    "uuid4",
    "uuid5",
    "uuid6",
    "uuid7",
)


def uuid3(name: str, namespace: UUID | None = None) -> UUID:
    """Generate a deterministic UUID (version 3) using MD5 hash."""
    module = _load_uuid_utils()
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    if module is None:
        return _stdlib_uuid3(namespace_value, name)
    return cast("UUID", module.uuid3(_convert_namespace(namespace_value, module), name))


def uuid4() -> UUID:
    """Generate a random UUID (version 4)."""
    module = _load_uuid_utils()
    if module is None:
        return _stdlib_uuid4()
    return cast("UUID", module.uuid4())


def uuid5(name: str, namespace: UUID | None = None) -> UUID:
    """Generate a deterministic UUID (version 5) using SHA-1 hash."""
    module = _load_uuid_utils()
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    if module is None:
        return _stdlib_uuid5(namespace_value, name)
    return cast("UUID", module.uuid5(_convert_namespace(namespace_value, module), name))


def uuid6() -> UUID:
    """Generate a time-ordered UUID (version 6)."""
    module = _load_uuid_utils()
    if module is None:
        warnings.warn(
            "uuid-utils not installed, falling back to uuid4 for UUID v6 generation. "
            "Install with: pip install vtb[uuid]",
            UserWarning,
            stacklevel=2,
        )
        return _stdlib_uuid4()
    return cast("UUID", module.uuid6())


def uuid7() -> UUID:
    """Generate a time-ordered UUID (version 7)."""
    module = _load_uuid_utils()
    if module is None:
        warnings.warn(
            "uuid-utils not installed, falling back to uuid4 for UUID v7 generation. "
            "Install with: pip install vtb[uuid]",
            UserWarning,
            stacklevel=2,
        )
        return _stdlib_uuid4()
    return cast("UUID", module.uuid7())


def nanoid() -> str:
    """Generate a Nano ID."""
    module = _load_nanoid()
    if module is None:
        warnings.warn(
            "fastnanoid not installed, falling back to uuid4.hex for Nano ID generation. "
            "Install with: pip install vtb[nanoid]",
            UserWarning,
            stacklevel=2,
        )
        return _nanoid_impl()
    return cast("str", module.generate())


class _Availability:
    """Lazy availability flag for optional dependencies."""

    __slots__ = ("_loader",)

    def __init__(self, loader: Callable[[], Any | None]) -> None:
        self._loader = loader

    def __bool__(self) -> bool:
        return self._loader() is not None


def _load_uuid_utils() -> Any | None:
    """Load uuid-utils when available."""
    try:
        return import_module("uuid_utils")
    except Exception:
        return None


def _load_nanoid() -> Any | None:
    """Load fastnanoid when available."""
    try:
        return import_module("fastnanoid")
    except Exception:
        return None


def _convert_namespace(namespace: Any, module: Any | None) -> Any:
    """Convert namespace to uuid-utils UUID when available."""
    if module is None:
        return namespace
    uuid_cls = module.UUID
    if isinstance(namespace, uuid_cls):
        return namespace
    return uuid_cls(str(namespace))


def _nanoid_impl() -> str:
    return _stdlib_uuid4().hex


UUID_UTILS_INSTALLED = _Availability(_load_uuid_utils)
NANOID_INSTALLED = _Availability(_load_nanoid)
