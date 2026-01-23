"""Litestar serialization integration."""

import datetime
import decimal
import ipaddress
import pathlib
import uuid
from typing import Any

import msgspec

from vertebra.serializers.decoders import (
    decode_date,
    decode_datetime,
    decode_decimal,
    decode_ip,
    decode_path,
    decode_time,
    decode_timedelta,
    decode_uuid,
)
from vertebra.serializers.encoders import default_encoder
from vertebra.utils.module_loader import dependency_flag

# Build VERTEBRA_TYPE_ENCODERS
VERTEBRA_TYPE_ENCODERS: dict[type, Any] = {
    datetime.datetime: default_encoder,
    datetime.date: default_encoder,
    datetime.time: default_encoder,
    datetime.timedelta: default_encoder,
    uuid.UUID: default_encoder,
    decimal.Decimal: default_encoder,
    pathlib.Path: default_encoder,
    pathlib.PurePath: default_encoder,
    ipaddress.IPv4Address: default_encoder,
    ipaddress.IPv6Address: default_encoder,
    ipaddress.IPv4Network: default_encoder,
    ipaddress.IPv6Network: default_encoder,
    bytes: default_encoder,
    set: default_encoder,
    frozenset: default_encoder,
    msgspec.Struct: default_encoder,
}

# Optional deps
if dependency_flag("pydantic"):
    from pydantic import BaseModel

    VERTEBRA_TYPE_ENCODERS[BaseModel] = default_encoder

if dependency_flag("pyarrow"):
    import pyarrow as pa

    VERTEBRA_TYPE_ENCODERS[pa.Table] = default_encoder


def _is_type(target: type) -> Any:
    """Create a predicate for type checking."""
    return lambda t: t is target


def _wrap(decoder: Any) -> Any:
    """Wrap a decoder to ignore the type argument passed by Litestar."""
    return lambda _t, v: decoder(v)


# Build VERTEBRA_TYPE_DECODERS
# Must be (predicate, decoder) tuples
VERTEBRA_TYPE_DECODERS: list[tuple[Any, Any]] = [
    (_is_type(datetime.datetime), _wrap(decode_datetime)),
    (_is_type(datetime.date), _wrap(decode_date)),
    (_is_type(datetime.time), _wrap(decode_time)),
    (_is_type(datetime.timedelta), _wrap(decode_timedelta)),
    (_is_type(uuid.UUID), _wrap(decode_uuid)),
    (_is_type(decimal.Decimal), _wrap(decode_decimal)),
    (_is_type(pathlib.Path), _wrap(decode_path)),
    (_is_type(ipaddress.IPv4Address), _wrap(decode_ip)),
    (_is_type(ipaddress.IPv6Address), _wrap(decode_ip)),
]
