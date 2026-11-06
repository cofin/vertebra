"""Rust-accelerated humanize formatting, English noun inflection, and byte/duration/rate unit parsing."""

from __future__ import annotations

import math

from vertebra._vertebra import (
    humanize as _native_humanize,
)
from vertebra._vertebra import (
    humanize_bytes as _native_humanize_bytes,
)
from vertebra._vertebra import (
    humanize_duration as _native_humanize_duration,
)
from vertebra._vertebra import (
    humanize_number as _native_humanize_number,
)
from vertebra._vertebra import (
    humanize_rate as _native_humanize_rate,
)
from vertebra._vertebra import (
    ordinalize as _native_ordinalize,
)
from vertebra._vertebra import (
    parse_bytes as _native_parse_bytes,
)
from vertebra._vertebra import (
    parse_duration as _native_parse_duration,
)
from vertebra._vertebra import (
    pluralize as _native_pluralize,
)
from vertebra._vertebra import (
    singularize as _native_singularize,
)
from vertebra._vertebra import (
    titleize as _native_titleize,
)

__all__ = (
    "humanize",
    "humanize_bytes",
    "humanize_duration",
    "humanize_number",
    "humanize_rate",
    "ordinalize",
    "parse_bytes",
    "parse_duration",
    "pluralize",
    "singularize",
    "titleize",
)


def humanize(value: str, *, capitalize: bool = True) -> str:
    """Convert an identifier (``user_profile_id``, ``getUserById``) into a human-readable phrase."""
    return _native_humanize(value, capitalize=capitalize)


def titleize(value: str) -> str:
    """Convert an identifier or phrase into Title Case (``get_user_by_id`` -> ``Get User By Id``)."""
    return _native_titleize(value)


def pluralize(word: str, count: int | None = None, *, include_count: bool = False) -> str:
    """Inflect an English noun to its plural (or singular when ``count`` is ``1`` or ``-1``)."""
    return _native_pluralize(word, count, include_count=include_count)


def singularize(word: str) -> str:
    """Inflect an English plural noun to its singular form."""
    return _native_singularize(word)


def ordinalize(value: int) -> str:
    """Format an integer as an English ordinal string (``1`` -> ``1st``, ``2`` -> ``2nd``)."""
    return _native_ordinalize(value)


def humanize_bytes(num_bytes: int | float, *, binary: bool = True, precision: int = 1) -> str:
    """Format a byte count using IEC binary (``KiB``, ``MiB``, ``GiB``) or SI decimal (``KB``, ``MB``, ``GB``) units."""
    non_negative = max(0, int(round(num_bytes)))
    return _native_humanize_bytes(non_negative, binary=binary, precision=precision)


def parse_bytes(value: str | int) -> int:
    """Parse a human-formatted byte size string (``64MiB``, ``64 MB``, ``512K``) or int into exact bytes."""
    if isinstance(value, int) and not isinstance(value, bool):
        if value < 0:
            msg = f"Byte size must be non-negative: {value}"
            raise ValueError(msg)
        return value
    return int(_native_parse_bytes(str(value)))


def humanize_duration(seconds: float | int, *, precision: int = 1) -> str:
    """Format a duration in seconds into human-readable units (``ns``, ``µs``, ``ms``, ``s``, ``m``, ``h``, ``d``)."""
    return _native_humanize_duration(float(seconds), precision=precision)


def parse_duration(value: str | float | int) -> float:
    """Parse a single or compound duration string (``500ms``, ``1.5s``, ``1m 30s``) or number into seconds."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        val_f = float(value)
        if not math.isfinite(val_f) or val_f < 0.0:
            msg = f"Duration must be non-negative: {value}"
            raise ValueError(msg)
        return val_f
    return float(_native_parse_duration(str(value)))


def humanize_number(value: int | float, *, precision: int = 1) -> str:
    """Format a number with compact magnitude suffixes (``K``, ``M``, ``B``, ``T``)."""
    return _native_humanize_number(float(value), precision=precision)


def humanize_rate(
    count: int | float,
    seconds: float = 1.0,
    *,
    unit: str = "req",
    precision: int = 1,
) -> str:
    """Format a throughput rate (``count / seconds``) with compact magnitude suffix (``123.8K req/s``)."""
    return _native_humanize_rate(float(count), float(seconds), unit=unit, precision=precision)
