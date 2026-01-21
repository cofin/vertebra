"""Shared encoding utilities for Vertebra serializers."""

from __future__ import annotations

import datetime
import decimal
import enum
import uuid
from typing import Any

__all__ = ("default_encoder",)


def default_encoder(value: Any) -> Any:  # noqa: PLR0911
    """Default encoder for types not natively supported by msgspec.

    Handles datetime, date, time, timedelta, enums, UUIDs, Decimals and other common types.

    Args:
        value: Value to convert.

    Returns:
        Serializable representation of the value.

    Raises:
        TypeError: If value cannot be serialized.
    """
    if isinstance(value, datetime.datetime):
        if not value.tzinfo:
            value = value.replace(tzinfo=datetime.UTC)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, datetime.time):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)
