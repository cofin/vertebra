"""UUID and NanoID generation utilities backed by native Rust and standard library primitives."""

from __future__ import annotations

from uuid import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500, UUID
from uuid import uuid1 as _stdlib_uuid1
from uuid import uuid3 as _stdlib_uuid3
from uuid import uuid5 as _stdlib_uuid5

from vertebra._vertebra import (
    nanoid_generate as _native_nanoid_generate,
)
from vertebra._vertebra import (
    uuid4_u128 as _native_uuid4_u128,
)
from vertebra._vertebra import (
    uuid7_u128 as _native_uuid7_u128,
)
from vertebra.typing import NANOID_INSTALLED, UUID_UTILS_INSTALLED

__all__ = (
    "NAMESPACE_DNS",
    "NAMESPACE_OID",
    "NAMESPACE_URL",
    "NAMESPACE_X500",
    "NANOID_INSTALLED",
    "UUID_UTILS_INSTALLED",
    "nanoid",
    "uuid3",
    "uuid4",
    "uuid5",
    "uuid6",
    "uuid7",
    "uuid_from_bytes",
    "uuid_from_int",
    "uuid_from_string",
)


def uuid3(name: str, namespace: UUID | None = None) -> UUID:
    """Generate a deterministic RFC 9562 Version 3 UUID using MD5."""
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    return _stdlib_uuid3(namespace_value, name)


def uuid4() -> UUID:
    """Generate a random RFC 9562 Version 4 UUID."""
    return UUID(int=_native_uuid4_u128())


def uuid5(name: str, namespace: UUID | None = None) -> UUID:
    """Generate a deterministic RFC 9562 Version 5 UUID using SHA-1."""
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    return _stdlib_uuid5(namespace_value, name)


def uuid6(node: int | None = None, clock_seq: int | None = None) -> UUID:
    """Generate a field-compatible time-ordered RFC 9562 Version 6 UUID."""
    u1 = _stdlib_uuid1(node=node, clock_seq=clock_seq)
    timestamp = u1.time
    time_high = (timestamp >> 28) & 0xFFFFFFFF
    time_mid = (timestamp >> 12) & 0xFFFF
    time_low_and_version = (timestamp & 0x0FFF) | 0x6000
    clock_seq_val = u1.clock_seq & 0x3FFF
    clock_seq_hi_variant = ((clock_seq_val >> 8) & 0x3F) | 0x80
    clock_seq_low = clock_seq_val & 0xFF
    return UUID(
        fields=(
            time_high,
            time_mid,
            time_low_and_version,
            clock_seq_hi_variant,
            clock_seq_low,
            u1.node,
        )
    )


def uuid7() -> UUID:
    """Generate a monotonically ordered RFC 9562 Version 7 UUID."""
    return UUID(int=_native_uuid7_u128())


def nanoid(size: int = 21) -> str:
    """Generate a URL-safe NanoID string of length ``size`` (default 21)."""
    if size < 1 or size > 1024:
        msg = f"nanoid size must be between 1 and 1024, got {size}"
        raise ValueError(msg)
    return _native_nanoid_generate(size)


def uuid_from_string(value: str) -> UUID:
    """Parse a UUID string into a standard library ``uuid.UUID`` instance."""
    return UUID(value)


def uuid_from_bytes(value: bytes) -> UUID:
    """Construct a standard library ``uuid.UUID`` from 16 raw big-endian bytes."""
    return UUID(bytes=value)


def uuid_from_int(value: int) -> UUID:
    """Construct a standard library ``uuid.UUID`` from a 128-bit unsigned integer."""
    return UUID(int=value)
