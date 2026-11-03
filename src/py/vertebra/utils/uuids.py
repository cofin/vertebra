"""UUID and NanoID generation utilities with native Rust acceleration and stdlib fallback."""

from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Callable
from typing import Any, ClassVar, Final, cast
from uuid import NAMESPACE_DNS, NAMESPACE_OID, NAMESPACE_URL, NAMESPACE_X500, UUID
from uuid import uuid1 as _stdlib_uuid1
from uuid import uuid3 as _stdlib_uuid3
from uuid import uuid4 as _stdlib_uuid4
from uuid import uuid5 as _stdlib_uuid5

from vertebra.typing import NANOID_INSTALLED, UUID_UTILS_INSTALLED
from vertebra.utils.module_loader import import_optional

__all__ = (
    "NAMESPACE_DNS",
    "NAMESPACE_OID",
    "NAMESPACE_URL",
    "NAMESPACE_X500",
    "NANOID_INSTALLED",
    "UUID_UTILS_INSTALLED",
    "_load_uuid_utils",
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

_UNLOADED: Final[object] = object()
_NANOID_ALPHABET: Final[str] = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
_native_vtb: Any = import_optional("vertebra._vertebra")


class _UuidBackend:
    """Class-scoped holder for caching optional uuid-utils and fastnanoid modules."""

    _module: ClassVar[Any] = _UNLOADED
    _nanoid_module: ClassVar[Any] = _UNLOADED
    _uuid7_lock: ClassVar[threading.Lock] = threading.Lock()
    _uuid7_last_ms: ClassVar[int] = 0
    _uuid7_seq: ClassVar[int] = 0

    @classmethod
    def get(cls) -> Any | None:
        """Load and cache uuid-utils when available."""
        if cls._module is not _UNLOADED:
            return cls._module
        if not UUID_UTILS_INSTALLED:
            cls._module = None
            return None
        cls._module = import_optional("uuid_utils")
        return cls._module

    @classmethod
    def get_nanoid(cls) -> Any | None:
        """Load and cache fastnanoid when available."""
        if cls._nanoid_module is not _UNLOADED:
            return cls._nanoid_module
        if not NANOID_INSTALLED:
            cls._nanoid_module = None
            return None
        cls._nanoid_module = import_optional("fastnanoid")
        return cls._nanoid_module


def _load_uuid_utils() -> Any | None:
    """Load and cache uuid-utils when available."""
    return _UuidBackend.get()


def uuid3(name: str, namespace: UUID | None = None) -> UUID:
    """Generate a deterministic RFC 9562 Version 3 UUID using MD5."""
    module = _load_uuid_utils()
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    if module is None:
        return _stdlib_uuid3(namespace_value, name)
    return _coerce_stdlib_uuid(module.uuid3(_convert_namespace(namespace_value, module), name))


def uuid4() -> UUID:
    """Generate a random RFC 9562 Version 4 UUID."""
    if _load_uuid_utils is not _DEFAULT_LOAD_UUID_UTILS:
        patched = _load_uuid_utils()
        if patched is not None:
            return _coerce_stdlib_uuid(patched.uuid4())
    if _native_uuid4_u128 is not None:
        return UUID(int=_native_uuid4_u128())
    module = _load_uuid_utils()
    if module is not None:
        return _coerce_stdlib_uuid(module.uuid4())
    return _stdlib_uuid4()


def uuid5(name: str, namespace: UUID | None = None) -> UUID:
    """Generate a deterministic RFC 9562 Version 5 UUID using SHA-1."""
    module = _load_uuid_utils()
    namespace_value = NAMESPACE_DNS if namespace is None else namespace
    if module is None:
        return _stdlib_uuid5(namespace_value, name)
    return _coerce_stdlib_uuid(module.uuid5(_convert_namespace(namespace_value, module), name))


def uuid6(node: int | None = None, clock_seq: int | None = None) -> UUID:
    """Generate a field-compatible time-ordered RFC 9562 Version 6 UUID."""
    module = _load_uuid_utils()
    if module is not None and hasattr(module, "uuid6"):
        return _coerce_stdlib_uuid(module.uuid6(node=node, clock_seq=clock_seq))
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
    if _load_uuid_utils is not _DEFAULT_LOAD_UUID_UTILS:
        patched = _load_uuid_utils()
        if patched is not None and hasattr(patched, "uuid7"):
            return _coerce_stdlib_uuid(patched.uuid7())
    if _native_uuid7_u128 is not None:
        return UUID(int=_native_uuid7_u128())
    module = _load_uuid_utils()
    if module is not None and hasattr(module, "uuid7"):
        return _coerce_stdlib_uuid(module.uuid7())
    return _py_uuid7()


def nanoid(size: int = 21) -> str:
    """Generate a URL-safe NanoID string of length ``size`` (default 21)."""
    if size < 1 or size > 1024:
        msg = f"nanoid size must be between 1 and 1024, got {size}"
        raise ValueError(msg)
    if _native_nanoid_generate is not None:
        return _native_nanoid_generate(size)
    nanoid_mod = _UuidBackend.get_nanoid()
    if nanoid_mod is not None and hasattr(nanoid_mod, "generate"):
        return str(nanoid_mod.generate(size=size))
    return "".join(_NANOID_ALPHABET[b & 63] for b in secrets.token_bytes(size))


def uuid_from_string(value: str) -> UUID:
    """Parse a UUID string into a standard library ``uuid.UUID`` instance."""
    return UUID(value)


def uuid_from_bytes(value: bytes) -> UUID:
    """Construct a standard library ``uuid.UUID`` from 16 raw big-endian bytes."""
    return UUID(bytes=value)


def uuid_from_int(value: int) -> UUID:
    """Construct a standard library ``uuid.UUID`` from a 128-bit unsigned integer."""
    return UUID(int=value)


def _get_native_fn[R](name: str, _return_type: type[R]) -> Callable[..., R] | None:
    """Return a compiled Rust function from ``vertebra._vertebra`` when present."""
    fn = getattr(_native_vtb, name, None)
    if callable(fn) and not isinstance(fn, type):
        return cast("Callable[..., R]", fn)
    return None


def _coerce_stdlib_uuid(value: Any) -> UUID:
    """Convert a ``uuid_utils.UUID`` or ``uuid.UUID`` object to a standard ``uuid.UUID``."""
    if type(value) is UUID:
        return value
    int_val = getattr(value, "int", None)
    if isinstance(int_val, int):
        return UUID(int=int_val)
    return UUID(str(value))


def _convert_namespace(namespace: Any, module: Any | None) -> Any:
    """Convert namespace to uuid-utils UUID when available."""
    if module is None:
        return namespace
    uuid_cls = module.UUID
    if isinstance(namespace, uuid_cls):
        return namespace
    return uuid_cls(str(namespace))


def _py_uuid7() -> UUID:
    """Pure-Python monotonic RFC 9562 Version 7 UUID fallback."""
    now_ms = int(time.time() * 1000) & 0xFFFFFFFFFFFF
    rand_bits = secrets.randbits(74)
    rand_seq = (rand_bits >> 62) & 0x03FF
    rand_b = rand_bits & 0x3FFFFFFFFFFFFFFF
    with _UuidBackend._uuid7_lock:
        if now_ms > _UuidBackend._uuid7_last_ms:
            _UuidBackend._uuid7_last_ms = now_ms
            _UuidBackend._uuid7_seq = rand_seq
        elif _UuidBackend._uuid7_seq < 0x0FFF:
            _UuidBackend._uuid7_seq += 1
        else:
            _UuidBackend._uuid7_last_ms += 1
            _UuidBackend._uuid7_seq = 0
        ts_ms = _UuidBackend._uuid7_last_ms
        seq = _UuidBackend._uuid7_seq
    high = ((ts_ms & 0xFFFFFFFFFFFF) << 16) | 0x7000 | (seq & 0x0FFF)
    low = (rand_b & 0x3FFFFFFFFFFFFFFF) | 0x8000000000000000
    return UUID(int=(high << 64) | low)


_DEFAULT_LOAD_UUID_UTILS: Final[Callable[[], Any | None]] = _load_uuid_utils
_native_uuid4_u128 = _get_native_fn("uuid4_u128", int)
_native_uuid7_u128 = _get_native_fn("uuid7_u128", int)
_native_nanoid_generate = _get_native_fn("nanoid_generate", str)
