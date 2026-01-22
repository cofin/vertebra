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
    "default_decoder",
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


def default_decoder(type_: type, value: Any) -> Any:
    """Default decoder hook for msgspec."""
    if type_ is pathlib.Path or type_ is pathlib.PurePath:
        return decode_path(value)
    if type_ is ipaddress.IPv4Address:
        return ipaddress.IPv4Address(value)
    if type_ is ipaddress.IPv6Address:
        return ipaddress.IPv6Address(value)
    if type_ in (ipaddress.IPv4Interface, ipaddress.IPv6Interface, ipaddress.IPv4Network, ipaddress.IPv6Network):
        return ipaddress.ip_network(value) if "Network" in str(type_) else ipaddress.ip_interface(value)
        
    raise TypeError(f"Objects of type {type_} are not supported")
