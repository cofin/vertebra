"""UUID and ID generation utilities with optional acceleration.

Provides wrapper functions for uuid3, uuid4, and uuid5 generation.
Uses uuid-utils package for performance when available, falling back
to standard library.
"""

from __future__ import annotations

from typing import Any, ClassVar, Final, cast
from uuid import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500, UUID
from uuid import uuid3 as _stdlib_uuid3
from uuid import uuid4 as _stdlib_uuid4
from uuid import uuid5 as _stdlib_uuid5

from vertebra.typing import UUID_UTILS_INSTALLED
from vertebra.utils.module_loader import import_optional

__all__ = (
    "NAMESPACE_DNS",
    "NAMESPACE_OID",
    "NAMESPACE_URL",
    "NAMESPACE_X500",
    "UUID_UTILS_INSTALLED",
    "_load_uuid_utils",
    "uuid3",
    "uuid4",
    "uuid5",
)

_UNLOADED: Final[object] = object()


class _UuidBackend:
    """Class-scoped holder for caching optional uuid-utils module."""

    _module: ClassVar[Any] = _UNLOADED

    @classmethod
    def get(cls) -> Any | None:
        """Load and cache uuid-utils when available."""
        if cls._module is not _UNLOADED:
            return cls._module
        if not UUID_UTILS_INSTALLED:
            cls._module = None
            return None
        cls._module = import_optional("uuid_utils")
        return cls._module


def _load_uuid_utils() -> Any | None:
    """Load and cache uuid-utils when available."""
    return _UuidBackend.get()


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


def _convert_namespace(namespace: Any, module: Any | None) -> Any:
    """Convert namespace to uuid-utils UUID when available."""
    if module is None:
        return namespace
    uuid_cls = module.UUID
    if isinstance(namespace, uuid_cls):
        return namespace
    return uuid_cls(str(namespace))
