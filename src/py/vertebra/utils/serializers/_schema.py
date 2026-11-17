"""Msgspec base structs and struct field dumping helpers for serializers."""

from __future__ import annotations

import threading
import uuid as _uuid_mod
from collections.abc import Callable, Iterable
from typing import Any, ClassVar, Final, cast

import msgspec

from vertebra.typing import (
    UUID_UTILS_INSTALLED,
    BaseStruct,
    CamelizedBaseStruct,
    FrozenBaseStruct,
    FrozenCamelizedStruct,
)
from vertebra.utils.module_loader import import_optional_attr

__all__ = (
    "BaseStruct",
    "CamelizedBaseStruct",
    "FrozenBaseStruct",
    "FrozenCamelizedStruct",
    "_dump_msgspec_fields",
    "is_uuid_instance",
    "resolve_uuid_utils_type",
    "schema_components",
)

_UNLOADED_UUID_TYPE: Final[object] = object()
_UUID_CACHE_LOCK: Final[threading.Lock] = threading.Lock()


class _UUIDTypeCache:
    """Module-scoped cache for the optional uuid_utils.UUID class."""

    uuid_utils_type: ClassVar[type[Any] | None | object] = _UNLOADED_UUID_TYPE


def resolve_uuid_utils_type() -> type[Any] | None:
    """Lazy-resolve and cache the uuid-utils UUID type when installed."""
    cached = _UUIDTypeCache.uuid_utils_type
    if cached is not _UNLOADED_UUID_TYPE:
        return cast("type[Any] | None", cached)
    with _UUID_CACHE_LOCK:
        cached = _UUIDTypeCache.uuid_utils_type
        if cached is not _UNLOADED_UUID_TYPE:
            return cast("type[Any] | None", cached)
        if not UUID_UTILS_INSTALLED:
            _UUIDTypeCache.uuid_utils_type = None
            return None
        resolved = cast("type[Any] | None", import_optional_attr("uuid_utils", "UUID"))
        _UUIDTypeCache.uuid_utils_type = resolved
        return resolved


def is_uuid_instance(value: Any) -> bool:
    """Return True when value is an instance of stdlib UUID or uuid_utils.UUID."""
    if isinstance(value, _uuid_mod.UUID):
        return True
    uuid_type = resolve_uuid_utils_type()
    return uuid_type is not None and isinstance(value, uuid_type)


def _dump_msgspec_fields(value: Any) -> dict[str, Any]:
    """Dump fields of a msgspec Struct into a dictionary."""
    return {field_name: value.__getattribute__(field_name) for field_name in value.__struct_fields__}


def schema_components(
    types: Iterable[Any],
    *,
    schema_hook: Callable[[type], dict[str, Any]] | None = None,
    ref_template: str = "#/$defs/{name}",
) -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    """Extract normalized JSON Schema components and component definitions for types."""
    type_tuple = tuple(types)
    if not type_tuple:
        return (), {}
    schemas, defs = msgspec.json.schema_components(
        type_tuple,
        schema_hook=schema_hook,
        ref_template=ref_template,
    )
    normalized_schemas = tuple(dict(schema) for schema in schemas)
    normalized_defs = {str(name): dict(component) for name, component in defs.items()}
    return normalized_schemas, normalized_defs
