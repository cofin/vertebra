"""Low-level route path parameter converters and FNV-1a handler ID hashing shared across server and extensions."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

try:
    import vertebra._vertebra as _vtb

    _native_handler_id: Callable[[str, str], int] | None = getattr(_vtb, "handler_id", None)
except ImportError:
    _native_handler_id = None

__all__ = (
    "PARAM_CONVERTERS",
    "make_handler_id",
    "parse_path_param",
    "parse_timedelta",
)

_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_MASK_64 = 0xFFFFFFFFFFFFFFFF
_ISO_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def parse_timedelta(raw: str) -> timedelta:
    """Parse an ISO-8601 duration or float seconds string into a timedelta."""
    match = _ISO_DURATION_RE.match(raw)
    if match and any(match.groupdict().values()):
        days = int(match.group("days") or 0)
        hours = int(match.group("hours") or 0)
        minutes = int(match.group("minutes") or 0)
        seconds = float(match.group("seconds") or 0.0)
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return timedelta(seconds=float(raw))


def parse_path_param(raw: str) -> str:
    """Normalize a greedy path parameter by stripping any leading slash."""
    return raw.lstrip("/")


PARAM_CONVERTERS: dict[str, Callable[[str], Any]] = {
    "str": str,
    "string": str,
    "int": int,
    "float": float,
    "uuid": UUID,
    "decimal": Decimal,
    "date": date.fromisoformat,
    "datetime": datetime.fromisoformat,
    "time": time.fromisoformat,
    "timedelta": parse_timedelta,
    "path": parse_path_param,
    "slug": str,
}


def make_handler_id(method: str, path: str) -> int:
    """Derive a stable uint64 handler ID from method and path using FNV-1a."""
    if _native_handler_id is not None:
        return int(_native_handler_id(method, path))
    h = _FNV_OFFSET
    h = _fnv1a_write(h, method.upper().encode())
    h = _fnv1a_write(h, b"\xff")
    h = _fnv1a_write(h, b" ")
    h = _fnv1a_write(h, path.encode())
    h = _fnv1a_write(h, b"\xff")
    return h


def _fnv1a_write(h: int, data: bytes) -> int:
    """Update FNV-1a hash with each byte of ``data``."""
    for b in data:
        h ^= b
        h = (h * _FNV_PRIME) & _MASK_64
    return h
