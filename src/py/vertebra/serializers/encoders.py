"""Shared encoding utilities for Vertebra serializers."""

import collections
import dataclasses
import datetime
import decimal
import enum
import ipaddress
import pathlib
import re
import uuid
from typing import Any

import msgspec

from vertebra.utils.module_loader import dependency_flag

# Dependency flags - avoid circular import by using module_loader directly
PYDANTIC_INSTALLED = dependency_flag("pydantic")
ATTRS_INSTALLED = dependency_flag("attrs")
NUMPY_INSTALLED = dependency_flag("numpy")
PYARROW_INSTALLED = dependency_flag("pyarrow")

__all__ = ("default_encoder",)


def default_encoder(value: Any) -> Any:  # noqa: PLR0911, PLR0912
    """Default encoder for types not natively supported by msgspec.

    Handles datetime, date, time, timedelta, enums, UUIDs, Decimals,
    Pydantic models, Arrow tables, numpy arrays, and other common types.

    Args:
        value: Value to convert.

    Returns:
        Serializable representation of the value.

    Raises:
        TypeError: If value cannot be serialized.
    """
    # Standard types
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
    if isinstance(value, pathlib.PurePath):
        return str(value)
    if isinstance(value, ipaddress.IPv4Address | ipaddress.IPv6Address | ipaddress.IPv4Interface | ipaddress.IPv6Interface | ipaddress.IPv4Network | ipaddress.IPv6Network):
        return str(value)
    if isinstance(value, re.Pattern):
        return value.pattern
    if isinstance(value, set | frozenset | collections.deque):
        return list(value)
    if isinstance(value, bytes):
        return value

    # Optional dependencies - check flags, then import and check type
    if PYDANTIC_INSTALLED:
        from pydantic import BaseModel

        if isinstance(value, BaseModel):
            return value.model_dump()
    if isinstance(value, msgspec.Struct):
        return msgspec.to_builtins(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if ATTRS_INSTALLED:
        import attrs

        if attrs.has(type(value)):
            return attrs.asdict(value)
    if NUMPY_INSTALLED:
        import numpy as np

        if isinstance(value, np.ndarray):
            return value.tolist()
    if PYARROW_INSTALLED:
        import pyarrow as pa

        if isinstance(value, pa.Table):
            table: pa.Table = value
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            return sink.getvalue().to_pybytes()

    # Fallback
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)
