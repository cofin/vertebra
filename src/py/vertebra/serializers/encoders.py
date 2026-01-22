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
from collections.abc import Callable
from typing import Any

import msgspec

from vertebra.utils.module_loader import dependency_flag

# Dependency flags
PYDANTIC_INSTALLED = dependency_flag("pydantic")
ATTRS_INSTALLED = dependency_flag("attrs")
NUMPY_INSTALLED = dependency_flag("numpy")
PYARROW_INSTALLED = dependency_flag("pyarrow")

__all__ = ("default_encoder",)


# Registries
_ENCODERS: dict[type, Callable[[Any], Any]] = {}
_INSTANCE_CHECKS: list[tuple[type | tuple[type, ...], Callable[[Any], Any]]] = []
_INITIALIZED = False


def _encode_datetime(value: datetime.datetime) -> str:
    if not value.tzinfo:
        value = value.replace(tzinfo=datetime.UTC)
    return value.isoformat().replace("+00:00", "Z")


def _encode_arrow_table(value: Any) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, value.schema) as writer:
        writer.write_table(value)
    return sink.getvalue().to_pybytes()  # type: ignore[no-any-return]


def _init_encoders() -> None:
    """Initialize the encoder registry with standard and optional types."""
    global _INITIALIZED  # noqa: PLW0603
    if _INITIALIZED:
        return

    # Standard types (Exact match optimization)
    _ENCODERS[datetime.datetime] = _encode_datetime
    _ENCODERS[datetime.date] = lambda v: v.isoformat()
    _ENCODERS[datetime.time] = lambda v: v.isoformat()
    _ENCODERS[datetime.timedelta] = lambda v: v.total_seconds()
    _ENCODERS[uuid.UUID] = str
    _ENCODERS[decimal.Decimal] = str
    _ENCODERS[bytes] = lambda v: v

    # Instance checks (Inheritance support)
    _INSTANCE_CHECKS.append((enum.Enum, lambda v: v.value))
    _INSTANCE_CHECKS.append((pathlib.PurePath, str))
    _INSTANCE_CHECKS.append((
        (
            ipaddress.IPv4Address,
            ipaddress.IPv6Address,
            ipaddress.IPv4Interface,
            ipaddress.IPv6Interface,
            ipaddress.IPv4Network,
            ipaddress.IPv6Network,
        ),
        str,
    ))
    _INSTANCE_CHECKS.append((re.Pattern, lambda v: v.pattern))
    _INSTANCE_CHECKS.append(((set, frozenset, collections.deque), list))
    _INSTANCE_CHECKS.append((msgspec.Struct, msgspec.to_builtins))

    # Optional dependencies
    if PYDANTIC_INSTALLED:
        from pydantic import BaseModel

        _INSTANCE_CHECKS.append((BaseModel, lambda v: v.model_dump()))

    if dataclasses.is_dataclass:  # Always true in supported python, but for consistency
        # Dataclasses don't have a common base class, so we use a predicate check in default_encoder fallback
        pass

    if ATTRS_INSTALLED:
        # Attrs checks are predicate-based (attrs.has), handled in fallback
        pass

    if NUMPY_INSTALLED:
        import numpy as np

        _INSTANCE_CHECKS.append((np.ndarray, lambda v: v.tolist()))

    if PYARROW_INSTALLED:
        import pyarrow as pa

        _INSTANCE_CHECKS.append((pa.Table, _encode_arrow_table))

    _INITIALIZED = True


def default_encoder(value: Any) -> Any:
    """Default encoder for types not natively supported by msgspec.

    Handles datetime, date, time, timedelta, enums, UUIDs, Decimals,
    Pydantic models, Arrow tables, numpy arrays, and other common types.

    Args:
        value: Value to convert.

    Returns:
        Serializable representation of the value.
    """
    if not _INITIALIZED:
        _init_encoders()

    # Fast path: Exact type match
    encoder = _ENCODERS.get(type(value))
    if encoder:
        return encoder(value)

    # Slow path: Instance checks
    for cls, encoder in _INSTANCE_CHECKS:
        if isinstance(value, cls):
            return encoder(value)

    # Predicate-based checks (dataclasses, attrs)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)

    if ATTRS_INSTALLED:
        import attrs

        if attrs.has(type(value)):
            return attrs.asdict(value)

    # Fallback
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)
