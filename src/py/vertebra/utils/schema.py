"""Schema hydration, recursive dictionary key transformation, and scalar value coercion."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from types import GenericAlias, UnionType
from typing import Annotated, Any, Union, cast, get_args, get_origin, overload
from uuid import UUID

import msgspec

from vertebra.utils.type_guards import (
    dataclass_to_dict,
    is_attrs_schema,
    is_dataclass,
    is_dataclass_instance,
    is_msgspec_struct,
    is_msgspec_struct_type,
    is_pydantic_model,
    is_typed_dict,
)

__all__ = (
    "can_vectorcall_hydrate_schema",
    "to_schema",
    "to_value_type",
    "transform_dict_keys",
)

_SCHEMA_CACHE_MAX_SIZE = 1024
_NULLABLE_UNION_ARG_COUNT = 2
_MAPPING_TYPE_ARG_COUNT = 2
_SEQUENCE_ORIGINS: frozenset[Any] = frozenset({list, tuple, set, frozenset, Sequence, Collection, Iterable})

type _MsgspecFieldPlan = tuple[tuple[tuple[str, str, Any], ...], frozenset[str], frozenset[str], bool]
_MSGSPEC_FIELD_CACHE: dict[type[Any], _MsgspecFieldPlan] = {}


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
def to_schema(data: Any, *, schema_type: None = None, normalize_nested: bool = False) -> Any: ...


@overload
def to_schema[T](data: Sequence[Any], *, schema_type: type[T], normalize_nested: bool = False) -> list[T]: ...


@overload
def to_schema[T](data: Any, *, schema_type: type[T], normalize_nested: bool = False) -> T: ...


def to_schema(data: Any, *, schema_type: Any = None, normalize_nested: bool = False) -> Any:
    """Convert a dictionary or sequence of dictionaries into ``schema_type``.

    Supports ``msgspec.Struct`` (including ``rename='camel'``, ``'kebab'``, ``'pascal'``,
    callable ``rename``, and per-field ``msgspec.field(name=...)``), standard library
    ``dataclasses``, ``TypedDict``, ``Pydantic`` models, and ``attrs`` classes.

    Args:
        data: Input mapping, object, or sequence of rows to hydrate.
        schema_type: Target schema class. Returns ``data`` unchanged when ``None``.
        normalize_nested: Accept Python field names inside nested ``msgspec.Struct``
            fields from trusted SQL projections. Defaults to ``False``.

    Returns:
        Hydrated instance or list of hydrated instances matching ``schema_type``.
    """
    if schema_type is None or data is None:
        return data

    is_batch = _is_sequence_input(data)
    if is_msgspec_struct_type(schema_type):
        return _convert_msgspec(data, schema_type, is_batch=is_batch, normalize_nested=normalize_nested)

    if is_batch:
        return [_hydrate_single_schema(item, schema_type) for item in data]

    return _hydrate_single_schema(data, schema_type)


def can_vectorcall_hydrate_schema(schema_type: Any) -> bool:
    """Return True when ``schema_type`` can bypass ``msgspec.convert`` via C-API vectorcall."""
    if not isinstance(schema_type, type) or is_typed_dict(schema_type):
        return False
    if not is_msgspec_struct_type(schema_type):
        return True
    _, _, _, vectorcall_safe = _msgspec_field_plan(schema_type)
    return vectorcall_safe


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

    if is_msgspec_struct_type(value_type):
        return cast("T", _convert_msgspec(value, value_type, is_batch=False, normalize_nested=False))

    if is_dataclass(value_type) or is_pydantic_model(value_type) or is_attrs_schema(value_type):
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


def _is_sequence_input(value: Any) -> bool:
    """Return True for non-string, non-bytes, non-mapping sequences."""
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview, Mapping))


def _convert_msgspec(
    data: Any,
    schema_type: type[Any],
    *,
    is_batch: bool,
    normalize_nested: bool,
) -> Any:
    """Normalize declared ``msgspec.Struct`` field names and convert with ``msgspec.convert``."""
    transformed = (
        [_normalize_msgspec_struct(item, schema_type, normalize_nested=normalize_nested) for item in data]
        if is_batch
        else _normalize_msgspec_struct(data, schema_type, normalize_nested=normalize_nested)
    )
    target_type: Any = GenericAlias(list, (schema_type,)) if is_batch else schema_type
    return msgspec.convert(transformed, type=target_type, strict=False, from_attributes=True)


def _normalize_msgspec_struct(data: Any, schema_type: type[Any], *, normalize_nested: bool) -> Any:
    """Map top-level Python field names onto encoded Struct field names with optional nested recursion."""
    if isinstance(data, schema_type) or not isinstance(data, Mapping):
        return data

    fields, python_names, encoded_names, _ = _msgspec_field_plan(schema_type)
    normalized: dict[Any, Any] = {
        key: value for key, value in data.items() if key not in python_names and key not in encoded_names
    }
    for name, encode_name, _ in fields:
        if encode_name in data:
            normalized[encode_name] = data[encode_name]
        elif name in data:
            normalized[encode_name] = data[name]

    if normalize_nested:
        for _, encode_name, field_type in fields:
            if encode_name in normalized:
                normalized[encode_name] = _normalize_msgspec_input(normalized[encode_name], field_type)

    return normalized


def _normalize_msgspec_input(data: Any, target_type: Any) -> Any:
    """Recursively normalize nested ``msgspec.Struct`` fields along declared container annotations."""
    unwrapped = _unwrap_msgspec_target(target_type)
    if unwrapped is None:
        return data

    if is_msgspec_struct_type(unwrapped):
        return _normalize_msgspec_struct(data, unwrapped, normalize_nested=True)

    origin = get_origin(unwrapped)
    args = get_args(unwrapped)
    if origin is None or not args:
        return data

    if _is_mapping_origin(origin):
        if not isinstance(data, Mapping) or len(args) < _MAPPING_TYPE_ARG_COUNT:
            return data
        value_type = args[1]
        return {key: _normalize_msgspec_input(value, value_type) for key, value in data.items()}

    if origin in _SEQUENCE_ORIGINS or (isinstance(origin, type) and issubclass(origin, (Sequence, set, frozenset))):
        if isinstance(data, (str, bytes, bytearray, memoryview, Mapping)) or not isinstance(data, Iterable):
            return data
        if origin is tuple and len(args) > 1 and args[1] is not Ellipsis:
            return [
                _normalize_msgspec_input(value, args[index]) if index < len(args) else value
                for index, value in enumerate(data)
            ]
        item_type = next(iter(args), Any)
        return [_normalize_msgspec_input(value, item_type) for value in data]

    return data


def _unwrap_msgspec_target(target_type: Any) -> Any:
    """Unwrap ``Annotated`` and single-target ``Optional`` unions, returning ``None`` on multi-branch unions."""
    while get_origin(target_type) is Annotated:
        annotated_args = get_args(target_type)
        if not annotated_args:
            break
        target_type = next(iter(annotated_args), Any)

    origin = get_origin(target_type)
    if origin is Union or origin is UnionType:
        args = get_args(target_type)
        non_null = tuple(arg for arg in args if arg is not type(None))
        if len(args) == _NULLABLE_UNION_ARG_COUNT and len(non_null) == 1:
            return _unwrap_msgspec_target(next(iter(non_null)))
        return None

    return target_type


def _is_mapping_origin(origin: Any) -> bool:
    """Return True when ``origin`` is ``dict`` or a ``Mapping`` subclass."""
    return origin is dict or (isinstance(origin, type) and issubclass(origin, Mapping))


def _contains_msgspec_struct(target_type: Any) -> bool:
    """Return True when ``target_type`` references a ``msgspec.Struct`` directly or inside containers/unions."""
    while get_origin(target_type) is Annotated:
        annotated_args = get_args(target_type)
        if not annotated_args:
            break
        target_type = next(iter(annotated_args), Any)
    if is_msgspec_struct_type(target_type):
        return True
    args = get_args(target_type)
    return any(_contains_msgspec_struct(arg) for arg in args if arg is not type(None) and arg is not Ellipsis)


def _msgspec_field_plan(schema_type: type[Any]) -> _MsgspecFieldPlan:
    """Compile and cache ``(fields, python_names, encoded_names, vectorcall_safe)`` for ``schema_type``."""
    cached = _MSGSPEC_FIELD_CACHE.get(schema_type)
    if cached is not None:
        return cached

    try:
        fields = tuple(
            (field.name, field.encode_name, field.type) for field in msgspec.structs.fields(cast("Any", schema_type))
        )
        vectorcall_safe = all(
            name == encode_name and not _contains_msgspec_struct(field_type) for name, encode_name, field_type in fields
        )
    except (NameError, TypeError):
        raw_names = tuple(getattr(schema_type, "__struct_fields__", ()))
        raw_encoded = tuple(getattr(schema_type, "__struct_encode_fields__", ()))
        fields = tuple((name, encoded, Any) for name, encoded in zip(raw_names, raw_encoded, strict=False))
        vectorcall_safe = False

    python_names = frozenset(field[0] for field in fields)
    encoded_names = frozenset(field[1] for field in fields)
    plan: _MsgspecFieldPlan = (fields, python_names, encoded_names, vectorcall_safe)
    if len(_MSGSPEC_FIELD_CACHE) >= _SCHEMA_CACHE_MAX_SIZE:
        _MSGSPEC_FIELD_CACHE.pop(next(iter(_MSGSPEC_FIELD_CACHE)), None)
    _MSGSPEC_FIELD_CACHE[schema_type] = plan
    return plan


def _hydrate_single_schema(item: Any, schema_type: Any) -> Any:
    """Hydrate a single row or mapping into a non-Struct ``schema_type``."""
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
