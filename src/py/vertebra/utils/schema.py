"""Schema hydration, recursive dictionary key transformation, and scalar value coercion."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, cast, overload
from uuid import UUID

import msgspec

from vertebra.utils.text import camelize, kebabize, pascalize
from vertebra.utils.type_guards import (
    dataclass_to_dict,
    get_msgspec_rename_config,
    is_attrs_schema,
    is_dataclass,
    is_dataclass_instance,
    is_msgspec_struct,
    is_pydantic_model,
    is_typed_dict,
)

__all__ = (
    "to_schema",
    "to_value_type",
    "transform_dict_keys",
)


def transform_dict_keys(data: Any, converter: Callable[[str], str]) -> Any:
    """Recursively transform dictionary keys across nested mappings, lists, and tuples."""
    if isinstance(data, Mapping):
        return {(converter(k) if isinstance(k, str) else k): transform_dict_keys(v, converter) for k, v in data.items()}
    if isinstance(data, list):
        return [transform_dict_keys(item, converter) for item in data]
    if isinstance(data, tuple):
        return tuple(transform_dict_keys(item, converter) for item in data)
    return data


@overload
def to_schema(data: Any, *, schema_type: None = None) -> Any: ...


@overload
def to_schema[T](data: Sequence[Any], *, schema_type: type[T]) -> list[T]: ...


@overload
def to_schema[T](data: Any, *, schema_type: type[T]) -> T: ...


def to_schema(data: Any, *, schema_type: Any = None) -> Any:
    """Convert a dictionary or sequence of dictionaries into ``schema_type``.

    Supports ``msgspec.Struct`` (including ``rename='camel'``, ``'kebab'``, or ``'pascal'``),
    standard library ``dataclasses``, ``TypedDict``, ``Pydantic`` models, and ``attrs`` classes.
    """
    if schema_type is None or data is None:
        return data

    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray, memoryview, Mapping)):
        return [_hydrate_single_schema(item, schema_type) for item in data]

    return _hydrate_single_schema(data, schema_type)


def to_value_type[T](value: Any, value_type: type[T]) -> T:
    """Coerce a raw scalar or mapping value into ``value_type``."""
    if value is None:
        return cast("T", value)
    if value_type is Any or value_type is object:
        return cast("T", value)

    if is_typed_dict(value_type):
        return cast("T", dict(value) if isinstance(value, Mapping) else value)

    if isinstance(value_type, type) and not is_typed_dict(value_type):
        try:
            if isinstance(value, value_type):
                return value
        except TypeError:
            pass

    if (
        is_msgspec_struct(value_type)
        or is_dataclass(value_type)
        or is_pydantic_model(value_type)
        or is_attrs_schema(value_type)
    ):
        return cast("T", _hydrate_single_schema(value, value_type))

    if value_type is bool:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on", "t", "y"}:
                return cast("T", True)
            if lowered in {"0", "false", "no", "off", "f", "n", ""}:
                return cast("T", False)
        return cast("T", bool(value))

    raw_value: Any = value
    if value_type is int:
        return cast("T", int(raw_value))
    if value_type is float:
        return cast("T", float(raw_value))
    if value_type is str:
        return cast("T", str(value))
    if value_type is bytes:
        if isinstance(value, str):
            return cast("T", value.encode("utf-8"))
        return cast("T", bytes(raw_value))
    if value_type is Decimal:
        return cast("T", Decimal(str(value)))
    if value_type is UUID:
        return cast("T", UUID(str(value)))
    if value_type is Path:
        return cast("T", Path(str(value)))
    if value_type is datetime and isinstance(value, str):
        return cast("T", datetime.fromisoformat(value))
    if value_type is date and isinstance(value, str):
        return cast("T", date.fromisoformat(value))
    if value_type is time and isinstance(value, str):
        return cast("T", time.fromisoformat(value))

    return msgspec.convert(value, type=value_type, strict=False)


def _hydrate_single_schema(item: Any, schema_type: Any) -> Any:
    """Hydrate a single row or mapping into ``schema_type``."""
    if is_typed_dict(schema_type):
        if isinstance(item, Mapping):
            return dict(item)
        if is_dataclass_instance(item):
            return dataclass_to_dict(item)
        if is_msgspec_struct(item):
            return msgspec.structs.asdict(item)
        return dict(item)

    if isinstance(schema_type, type):
        try:
            if isinstance(item, schema_type):
                return item
        except TypeError:
            pass

    if is_msgspec_struct(schema_type):
        payload = item
        if isinstance(item, Mapping):
            rename_cfg = get_msgspec_rename_config(schema_type)
            if rename_cfg == "camel":
                payload = transform_dict_keys(item, camelize)
            elif rename_cfg == "kebab":
                payload = transform_dict_keys(item, kebabize)
            elif rename_cfg == "pascal":
                payload = transform_dict_keys(item, pascalize)
        return msgspec.convert(payload, type=schema_type, strict=False)

    if is_dataclass(schema_type):
        if isinstance(item, Mapping):
            return schema_type(**item)
        return msgspec.convert(item, type=schema_type, strict=False)

    if is_pydantic_model(schema_type):
        model_validate = getattr(schema_type, "model_validate", None)
        if callable(model_validate):
            return model_validate(item)
        pydantic_cls = cast("Callable[..., Any]", schema_type)
        if isinstance(item, Mapping):
            return pydantic_cls(**item)
        return pydantic_cls(item)

    if is_attrs_schema(schema_type) and isinstance(item, Mapping):
        return schema_type(**item)

    if isinstance(item, Mapping) and callable(schema_type):
        return schema_type(**item)

    return msgspec.convert(item, type=schema_type, strict=False)
