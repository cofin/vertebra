"""Reusable type-converter builders for Decimal, UUID, ISO 8601 time, and sequence parameters."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Final
from uuid import UUID

from vertebra.utils.module_loader import import_optional_attr

__all__ = (
    "DEFAULT_SPECIAL_CHARS",
    "build_decimal_converter",
    "build_nested_decimal_normalizer",
    "build_uuid_coercions",
    "should_json_encode_sequence",
    "time_iso_convert",
)

DEFAULT_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "}", "[", "]", ":", ",", '"', "-"})


def build_decimal_converter(*, mode: str = "preserve") -> Callable[[Decimal], Any]:
    """Return a scalar ``Decimal`` converter function for ``mode`` (``'preserve'``, ``'str'``, or ``'float'``)."""
    normalized_mode = mode.strip().lower()
    if normalized_mode == "preserve":
        return _identity_decimal
    if normalized_mode == "str":
        return str
    if normalized_mode == "float":
        return float
    msg = f"Unsupported Decimal converter mode: {mode!r} (expected 'preserve', 'str', or 'float')"
    raise ValueError(msg)


def build_nested_decimal_normalizer(*, mode: str = "str") -> Callable[[Any], Any]:
    """Return a recursive normalizer that converts nested ``Decimal`` values inside mappings and sequences."""
    scalar_converter = build_decimal_converter(mode=mode)
    if mode.strip().lower() == "preserve":
        return _identity_any

    def _normalize(value: Any) -> Any:
        if isinstance(value, Decimal):
            return scalar_converter(value)
        if isinstance(value, Mapping):
            return {k: _normalize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_normalize(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_normalize(item) for item in value)
        return value

    return _normalize


def build_uuid_coercions(*, native: bool = False) -> dict[type[Any], Callable[[Any], Any]]:
    """Build a type-to-coercion mapping for ``uuid.UUID`` and optional ``uuid_utils.UUID``."""
    coercer: Callable[[Any], Any] = _coerce_stdlib_uuid if native else str
    mapping: dict[type[Any], Callable[[Any], Any]] = {UUID: coercer}
    ext_uuid_cls = import_optional_attr("uuid_utils", "UUID")
    if isinstance(ext_uuid_cls, type):
        mapping[ext_uuid_cls] = coercer
    return mapping


def should_json_encode_sequence(seq: Sequence[Any]) -> bool:
    """Return True when ``seq`` contains structured mapping/dict elements that require JSON encoding."""
    if isinstance(seq, (str, bytes, bytearray, memoryview)) or not seq:
        return False
    first = seq[0]
    return isinstance(first, Mapping)


def time_iso_convert(value: datetime | date | time) -> str:
    """Format a ``datetime``, ``date``, or ``time`` instance as an ISO 8601 string."""
    return value.isoformat()


def _coerce_stdlib_uuid(value: Any) -> UUID:
    """Convert a ``uuid_utils.UUID`` or ``uuid.UUID`` object to a standard ``uuid.UUID``."""
    if type(value) is UUID:
        return value
    int_val = getattr(value, "int", None)
    if isinstance(int_val, int):
        return UUID(int=int_val)
    return UUID(str(value))


def _identity_decimal(value: Decimal) -> Decimal:
    """Return ``value`` unchanged."""
    return value


def _identity_any(value: Any) -> Any:
    """Return ``value`` unchanged."""
    return value
