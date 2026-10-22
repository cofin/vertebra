"""Msgspec struct field dumping helper for serializers."""

from __future__ import annotations

from typing import Any

__all__ = ("_dump_msgspec_fields",)


def _dump_msgspec_fields(value: Any) -> dict[str, Any]:
    """Dump fields of a msgspec Struct into a dictionary."""
    return {field_name: value.__getattribute__(field_name) for field_name in value.__struct_fields__}
