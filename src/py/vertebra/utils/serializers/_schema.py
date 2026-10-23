"""Msgspec base structs and struct field dumping helpers for serializers."""

from __future__ import annotations

from typing import Any

from vertebra.typing import (
    BaseStruct,
    CamelizedBaseStruct,
    FrozenBaseStruct,
    FrozenCamelizedStruct,
)

__all__ = (
    "BaseStruct",
    "CamelizedBaseStruct",
    "FrozenBaseStruct",
    "FrozenCamelizedStruct",
    "_dump_msgspec_fields",
)


def _dump_msgspec_fields(value: Any) -> dict[str, Any]:
    """Dump fields of a msgspec Struct into a dictionary."""
    return {field_name: value.__getattribute__(field_name) for field_name in value.__struct_fields__}
