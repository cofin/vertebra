"""Shared decoding utilities for Vertebra serializers."""

import datetime
import decimal
import ipaddress
import pathlib
import uuid
from typing import Any

__all__ = (
    "decode_date",
    "decode_datetime",
    "decode_decimal",
    "decode_ip",
    "decode_path",
    "decode_time",
    "decode_timedelta",
    "decode_uuid",
)


def decode_datetime(value: Any) -> datetime.datetime:
    """Decode a datetime from string."""
    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value)
    return value  # type: ignore[no-any-return]


def decode_date(value: Any) -> datetime.date:
    """Decode a date from string."""
    if isinstance(value, str):
        return datetime.date.fromisoformat(value)
    return value  # type: ignore[no-any-return]


def decode_time(value: Any) -> datetime.time:
    """Decode a time from string."""
    if isinstance(value, str):
        return datetime.time.fromisoformat(value)
    return value  # type: ignore[no-any-return]


def decode_timedelta(value: Any) -> datetime.timedelta:
    """Decode a timedelta from seconds."""
    if isinstance(value, float | int):
        return datetime.timedelta(seconds=value)
    return value  # type: ignore[no-any-return]


def decode_uuid(value: Any) -> uuid.UUID:
    """Decode a UUID from string."""
    if isinstance(value, str):
        return uuid.UUID(value)
    return value  # type: ignore[no-any-return]


def decode_decimal(value: Any) -> decimal.Decimal:
    """Decode a Decimal from string or number."""
    if isinstance(value, str | float | int):
        return decimal.Decimal(str(value))
    return value  # type: ignore[no-any-return]


def decode_path(value: Any) -> pathlib.Path:
    """Decode a Path from string."""
    if isinstance(value, str):
        return pathlib.Path(value)
    return value  # type: ignore[no-any-return]


def decode_ip(value: Any) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    """Decode an IP address from string."""
    if isinstance(value, str):
        return ipaddress.ip_address(value)
    return value  # type: ignore[no-any-return]
