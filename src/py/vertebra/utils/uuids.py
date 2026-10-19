"""UUID and ID generation utilities with optional acceleration.

Provides wrapper functions for uuid3, uuid4, uuid5, uuid6, uuid7, and nanoid generation.
Uses uuid-utils and fastnanoid packages for performance when available,
falling back to standard library. Aligned with vtb.
"""

import warnings
from importlib import import_module
from typing import Any, Final, cast
from uuid import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500, UUID
from uuid import uuid3 as _stdlib_uuid3
from uuid import uuid4 as _stdlib_uuid4
from uuid import uuid5 as _stdlib_uuid5

from vertebra._typing import NANOID_INSTALLED, UUID_UTILS_INSTALLED

__all__ = (
    "NAMESPACE_DNS",
    "NAMESPACE_OID",
    "NAMESPACE_URL",
    "NAMESPACE_X500",
    "NANOID_INSTALLED",
    "UUID_UTILS_INSTALLED",
    "_load_uuid_utils",
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


_UNLOADED: Final[object] = object()
_uuid_utils_module: Any = _UNLOADED
_nanoid_module: Any = _UNLOADED


def _load_uuid_utils() -> Any | None:
    """Load and cache uuid-utils when available."""
    global _uuid_utils_module
    if _uuid_utils_module is not _UNLOADED:
        return _uuid_utils_module
    if not UUID_UTILS_INSTALLED:
        _uuid_utils_module = None
        return None
    try:
        _uuid_utils_module = import_module("uuid_utils")
    except Exception:
        _uuid_utils_module = None
    return _uuid_utils_module


def _load_nanoid() -> Any | None:
    """Load and cache fastnanoid when available."""
    global _nanoid_module
    if _nanoid_module is not _UNLOADED:
        return _nanoid_module
    if not NANOID_INSTALLED:
        _nanoid_module = None
        return None
    try:
        _nanoid_module = import_module("fastnanoid")
    except Exception:
        _nanoid_module = None
    return _nanoid_module


def _convert_namespace(namespace: Any, module: Any | None) -> Any:
    """Convert namespace to uuid-utils UUID when available."""
    if module is None:
        return namespace
    uuid_cls = module.UUID
    if isinstance(namespace, uuid_cls):
        return namespace
    return uuid_cls(str(namespace))


def _nanoid_impl() -> str:
    """Generate a fallback hexadecimal Nano ID."""
    return _stdlib_uuid4().hex
