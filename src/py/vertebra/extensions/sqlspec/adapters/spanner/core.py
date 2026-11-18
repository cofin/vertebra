"""Core protocol helpers, exception mapping, connection shim, and typed parameter encoding for Spanner."""

from __future__ import annotations

import base64
import re
from collections.abc import Collection, Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final, Literal, Self

import msgspec
from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig, TypedParameter
from sqlspec.core.cache import get_cache_config
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    DialectNotSupportedError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import to_json

from vertebra._vertebra import (
    ConnectionFailure,
    DatabaseError,
    GoogleCloudAuthError,
    GoogleCloudError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    PoolExhaustedError,
    ProgrammingError,
    QueryCancelledError,
    SerializationFailure,
    SpannerAbortedError,
    SpannerError,
)
from vertebra._vertebra import DataError as NativeDataError
from vertebra._vertebra import IntegrityError as NativeIntegrityError
from vertebra._vertebra import NotFoundError as NativeNotFoundError
from vertebra._vertebra import OperationalError as NativeOperationalError
from vertebra.db.adapters.spanner import (
    Mutation,
    SpannerConfig,
    StalenessBound,
    SyncSpannerDriver,
    SyncSpannerSnapshot,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError

__all__ = (
    "ADAPTER_NAME",
    "DEFAULT_SPANNER_SCOPES",
    "SpannerConnectionBatch",
    "SpannerConnectionShim",
    "default_statement_config",
    "encode_spanner_batch_parameters",
    "encode_spanner_parameters",
    "encode_typed_spanner_value",
    "farm_fingerprint_shard",
    "farmhash_fingerprint64",
    "is_spanner_table_missing",
    "map_native_database_exception",
    "map_spanner_exception",
    "native_statement_config",
    "normalize_spanner_dialect",
    "object_to_artifact_reference",
    "parse_utc_timestamp",
    "positive_int_or_none",
    "resolve_spanner_config",
    "resolve_storage_backend",
    "sqlspec_dialect_name",
)

ADAPTER_NAME: Final[str] = "vertebra_spanner"

DEFAULT_SPANNER_SCOPES: Final[tuple[str, ...]] = (
    "https://www.googleapis.com/auth/spanner.data",
    "https://www.googleapis.com/auth/spanner.admin",
    "https://www.googleapis.com/auth/cloud-platform",
)

_SAFE_TABLE_IDENT_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_SPANNER_SCHEMES: Final[tuple[str, ...]] = ("spanner", "spangres")


def normalize_spanner_dialect(value: Any) -> Literal["googlesql", "spangres"]:
    """Normalize a dialect label into Vertebra's internal ``googlesql`` or ``spangres`` literal."""
    text = str(value or "googlesql").strip().lower()
    if text in {"spangres", "postgres", "postgresql", "pg"}:
        return "spangres"
    return "googlesql"


def sqlspec_dialect_name(value: Any) -> Literal["spanner", "spangres"]:
    """Map a Spanner dialect label to SQLSpec's public dialect name without exposing ``googlesql``."""
    return "spangres" if normalize_spanner_dialect(value) == "spangres" else "spanner"


def native_statement_config(
    dialect: str,
    parameter_style: ParameterStyle,
    **overrides: Any,
) -> StatementConfig:
    """Construct an isolated ``StatementConfig`` configured for the adapter's native bind syntax."""
    default_style = overrides.pop("default_style", ParameterStyle.QMARK)
    supported_styles = overrides.pop("supported_styles", None)
    supported_execution_styles = overrides.pop("supported_execution_styles", None)
    preserve_parameter_format = bool(overrides.pop("preserve_parameter_format", True))
    enable_caching = overrides.pop("enable_caching", None)

    if supported_styles is not None:
        input_styles: Collection[ParameterStyle] = set(supported_styles)
    elif parameter_style == ParameterStyle.NAMED_COLON:
        input_styles = {ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}
    else:
        input_styles = {default_style, parameter_style}

    exec_styles: Collection[ParameterStyle] = (
        set(supported_execution_styles) if supported_execution_styles is not None else {parameter_style}
    )
    caching = get_cache_config().compiled_cache_enabled if enable_caching is None else enable_caching
    parameter_config = overrides.pop(
        "parameter_config",
        ParameterStyleConfig(
            default_parameter_style=default_style,
            supported_parameter_styles=set(input_styles),
            default_execution_parameter_style=parameter_style,
            supported_execution_parameter_styles=set(exec_styles),
            preserve_parameter_format=preserve_parameter_format,
        ),
    )
    return StatementConfig(
        dialect=dialect,
        enable_caching=caching,
        parameter_config=parameter_config,
        **overrides,
    )


def default_statement_config(dialect: str = "googlesql") -> StatementConfig:
    """Construct the SQLSpec ``StatementConfig`` for Cloud Spanner GoogleSQL or Spangres."""
    public_dialect = sqlspec_dialect_name(dialect)
    if public_dialect == "spangres":
        return native_statement_config(
            "spangres",
            ParameterStyle.NUMERIC,
            default_style=ParameterStyle.NUMERIC,
            supported_styles={
                ParameterStyle.NUMERIC,
                ParameterStyle.NAMED_AT,
                ParameterStyle.QMARK,
                ParameterStyle.NAMED_COLON,
            },
            supported_execution_styles={ParameterStyle.NUMERIC},
        )
    return native_statement_config(
        "spanner",
        ParameterStyle.NAMED_AT,
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={
            ParameterStyle.NAMED_AT,
            ParameterStyle.NUMERIC,
            ParameterStyle.QMARK,
            ParameterStyle.NAMED_COLON,
        },
        supported_execution_styles={ParameterStyle.NAMED_AT},
    )


def _format_typed_element(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def encode_typed_spanner_value(value: Any) -> Any:
    """Convert SQLSpec ``TypedParameter`` or native Python values into Rust Spanner JSON descriptors."""
    if isinstance(value, TypedParameter):
        inner = value.value
        semantic = str(value.semantic_name or value.original_type or "").strip().upper()
        if not semantic:
            return encode_typed_spanner_value(inner)
        if semantic.startswith("ARRAY<") and semantic.endswith(">"):
            elem_type = semantic[6:-1].strip() or "STRING"
            items = [_format_typed_element(item) for item in (inner or ())]
            return {"type": "ARRAY", "array_type": elem_type, "value": items}
        if semantic in {"JSON", "JSONB", "PG_JSONB"}:
            payload = inner if isinstance(inner, str) else to_json(inner)
            return {"type": semantic, "value": payload}
        if semantic in {"BYTES", "BYTEA"}:
            if inner is None:
                return {"type": "BYTES", "value": None}
            raw = bytes(inner) if isinstance(inner, (bytes, bytearray, memoryview)) else str(inner).encode("utf-8")
            return {"type": "BYTES", "value": base64.b64encode(raw).decode("ascii")}
        return {"type": semantic, "value": _format_typed_element(inner)}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return {"type": "NUMERIC", "value": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"type": "BYTES", "value": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, datetime):
        return {"type": "TIMESTAMP", "value": _format_typed_element(value)}
    if isinstance(value, date):
        return {"type": "DATE", "value": value.isoformat()}
    if isinstance(value, Mapping):
        if "type" in value and "value" in value:
            return dict(value)
        return {"type": "JSON", "value": to_json(dict(value))}
    if isinstance(value, (list, tuple)):
        return [encode_typed_spanner_value(item) for item in value]
    return value


def _normalize_spangres_key(key: Any) -> str:
    text = str(key).lstrip("@")
    if text.startswith("$") and text[1:].isdigit():
        return f"p{text[1:]}"
    if text.isdigit():
        return f"p{text}"
    return text


def encode_spanner_parameters(
    parameters: Any,
    *,
    dialect: str = "googlesql",
) -> Any:
    """Normalize compiled SQLSpec parameters into the mapping or sequence expected by Spanner drivers."""
    if parameters is None:
        return None
    norm_dialect = normalize_spanner_dialect(dialect)
    if isinstance(parameters, Mapping):
        if not parameters:
            return None
        if norm_dialect == "spangres":
            return {_normalize_spangres_key(key): encode_typed_spanner_value(val) for key, val in parameters.items()}
        return {str(key).lstrip("@$"): encode_typed_spanner_value(val) for key, val in parameters.items()}
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        if not parameters:
            return None
        if norm_dialect == "spangres":
            return {f"p{idx + 1}": encode_typed_spanner_value(val) for idx, val in enumerate(parameters)}
        return [encode_typed_spanner_value(val) for val in parameters]
    return encode_typed_spanner_value(parameters)


def encode_spanner_batch_parameters(
    parameter_sets: Any,
    *,
    dialect: str = "googlesql",
) -> list[Any]:
    """Normalize a sequence of parameter sets for ``ExecuteBatchDml``."""
    if parameter_sets is None:
        return []
    encoded: list[Any] = []
    for item in parameter_sets:
        encoded.append(encode_spanner_parameters(item, dialect=dialect))
    return encoded


def _is_google_api_exception(exc: BaseException) -> bool:
    """Check whether ``exc`` originates from ``google.api_core`` or ``google.cloud`` without importing them."""
    return any(cls.__module__.startswith(("google.api_core", "google.cloud")) for cls in type(exc).__mro__)


def map_native_database_exception(exc: BaseException) -> Exception | None:
    """Map Vertebra native ``DatabaseError`` / ``GoogleCloudError`` hierarchy exceptions to ``sqlspec.exceptions.*``."""
    if isinstance(exc, SQLSpecError):
        return None
    if not isinstance(exc, (DatabaseError, GoogleCloudError, SpannerError)) and not _is_google_api_exception(exc):
        return None

    message = str(exc)
    lower = message.lower()
    sqlstate = str(getattr(exc, "sqlstate", "") or "").upper()
    code_attr = getattr(exc, "code", None)
    code_str = str(code_attr if code_attr is not None else "").upper()

    if (
        isinstance(exc, GoogleCloudAuthError)
        or sqlstate in {"42501", "28000", "28P01"}
        or any(token in lower for token in ("permission denied", "unauthenticated", "forbidden", "access denied"))
    ):
        return PermissionDeniedError(message)

    if isinstance(exc, QueryCancelledError) or any(
        token in lower for token in ("cancelled", "canceled", "query was interrupted", "57014")
    ):
        return OperationCancelledError(message)

    if (
        isinstance(exc, (SpannerAbortedError, SerializationFailure))
        or sqlstate in {"40001", "40P01"}
        or any(token in lower for token in ("deadlock", "serialization", "aborted"))
    ):
        if "deadlock" in lower or sqlstate == "40P01":
            return DeadlockError(message)
        return SerializationConflictError(message)

    if isinstance(exc, NativeIntegrityError) or sqlstate.startswith("23"):
        if "unique" in lower or "duplicate" in lower or sqlstate == "23505":
            return UniqueViolationError(message)
        if "foreign key" in lower or sqlstate == "23503":
            return ForeignKeyViolationError(message)
        if "check constraint" in lower or sqlstate == "23514":
            return CheckViolationError(message)
        if "not null" in lower or "not-null" in lower or sqlstate == "23502":
            return NotNullViolationError(message)
        return IntegrityError(message)

    if isinstance(exc, (ConnectionFailure, PoolExhaustedError)) or sqlstate.startswith("08"):
        if "timeout" in lower or "timed out" in lower:
            return ConnectionTimeoutError(message)
        return DatabaseConnectionError(message)

    if isinstance(exc, (NativeDataError, NativeNotFoundError)) or sqlstate.startswith("22"):
        return DataError(message)

    if isinstance(exc, NotSupportedError):
        return DialectNotSupportedError(message)

    if isinstance(exc, NativeOperationalError) or "timeout" in lower or "deadline" in lower:
        if any(token in lower for token in ("timeout", "timed out", "deadline", "57014")):
            return QueryTimeoutError(message)
        if "deadlock" in lower:
            return DeadlockError(message)
        if any(token in lower for token in ("connection", "refused", "unreachable", "reset by peer")):
            return DatabaseConnectionError(message)
        return OperationalError(message)

    if isinstance(exc, ProgrammingError) or any(
        token in lower for token in ("syntax", "parse error", "no such table", "does not exist", "undefined")
    ):
        return SQLParsingError(message)

    if isinstance(exc, (SpannerError, GoogleCloudError)) or _is_google_api_exception(exc):
        if "ALREADY_EXISTS" in code_str or "already exists" in lower:
            return UniqueViolationError(message)
        if "DEADLINE_EXCEEDED" in code_str or "timeout" in lower:
            return QueryTimeoutError(message)
        if "PERMISSION_DENIED" in code_str or "UNAUTHENTICATED" in code_str:
            return PermissionDeniedError(message)
        if "ABORTED" in code_str:
            return SerializationConflictError(message)
        if "CANCELLED" in code_str:
            return OperationCancelledError(message)
        return OperationalError(message)

    if isinstance(exc, (InterfaceError, InternalError, DatabaseError)):
        return OperationalError(message)

    return OperationalError(message)


def map_spanner_exception(exc: BaseException) -> Exception | None:
    """Translate Vertebra ``SpannerError`` and gRPC status codes into SQLSpec exceptions."""
    if isinstance(exc, SQLSpecError):
        return None
    if not isinstance(exc, (SpannerError, SpannerAbortedError)):
        return map_native_database_exception(exc)
    message = str(exc)
    lower = message.lower()
    code = getattr(exc, "code", None)
    if isinstance(exc, SpannerAbortedError) or code == 10 or "aborted" in lower:
        return SerializationConflictError(message)
    if code == 6 or "already exists" in lower or "duplicate" in lower:
        return UniqueViolationError(message)
    if code == 9:
        return IntegrityError(message)
    if code == 5 or "not found" in lower:
        return DataError(message)
    if code in {7, 16}:
        return PermissionDeniedError(message)
    if code == 4 or "deadline exceeded" in lower:
        return QueryTimeoutError(message)
    if code == 1:
        return OperationCancelledError(message)
    if code == 14:
        return DatabaseConnectionError(message)
    if code == 12:
        return DialectNotSupportedError(message)
    if code == 3:
        if any(token in lower for token in ("syntax", "parse", "unrecognized", "unexpected")):
            return SQLParsingError(message)
        return DataError(message)
    return map_native_database_exception(exc) or OperationalError(message)


_FARMHASH_MASK64: Final[int] = 0xFFFFFFFFFFFFFFFF
_FARMHASH_K0: Final[int] = 0xC3A5C85C97CB3127
_FARMHASH_K1: Final[int] = 0xB492B66FBE98F273
_FARMHASH_K2: Final[int] = 0x9AE16A3B2F90404F


def _farmhash_rot64(val: int, shift: int) -> int:
    val &= _FARMHASH_MASK64
    return val if shift == 0 else (((val >> shift) | (val << (64 - shift))) & _FARMHASH_MASK64)


def _farmhash_shift_mix(val: int) -> int:
    val &= _FARMHASH_MASK64
    return (val ^ (val >> 47)) & _FARMHASH_MASK64


def _farmhash_len_16(u: int, v: int, mul: int) -> int:
    a = ((u ^ v) * mul) & _FARMHASH_MASK64
    a ^= a >> 47
    b = ((v ^ a) * mul) & _FARMHASH_MASK64
    b ^= b >> 47
    return (b * mul) & _FARMHASH_MASK64


def _farmhash_fetch64(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 8], "little", signed=False)


def _farmhash_fetch32(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 4], "little", signed=False)


def _farmhash_weak_len_32(buf: bytes, offset: int, a: int, b: int) -> tuple[int, int]:
    w = _farmhash_fetch64(buf, offset)
    x = _farmhash_fetch64(buf, offset + 8)
    y = _farmhash_fetch64(buf, offset + 16)
    z = _farmhash_fetch64(buf, offset + 24)
    a = (a + w) & _FARMHASH_MASK64
    b = _farmhash_rot64((b + a + z) & _FARMHASH_MASK64, 21)
    c = a
    a = (a + x) & _FARMHASH_MASK64
    a = (a + y) & _FARMHASH_MASK64
    b = (b + _farmhash_rot64(a, 44)) & _FARMHASH_MASK64
    return (a + z) & _FARMHASH_MASK64, (b + c) & _FARMHASH_MASK64


def _farmhash_len_0_to_16(buf: bytes) -> int:
    length = len(buf)
    if length >= 8:
        mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
        a = (_farmhash_fetch64(buf, 0) + _FARMHASH_K2) & _FARMHASH_MASK64
        b = _farmhash_fetch64(buf, length - 8)
        c = (_farmhash_rot64(b, 37) * mul + a) & _FARMHASH_MASK64
        d = ((_farmhash_rot64(a, 25) + b) * mul) & _FARMHASH_MASK64
        return _farmhash_len_16(c, d, mul)
    if length >= 4:
        mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
        a = _farmhash_fetch32(buf, 0)
        return _farmhash_len_16((length + (a << 3)) & _FARMHASH_MASK64, _farmhash_fetch32(buf, length - 4), mul)
    if length > 0:
        a = buf[0]
        b = buf[length >> 1]
        c = buf[length - 1]
        y = (a + (b << 8)) & 0xFFFFFFFF
        z = (length + (c << 2)) & 0xFFFFFFFF
        return (_farmhash_shift_mix(((y * _FARMHASH_K2) ^ (z * _FARMHASH_K0)) & _FARMHASH_MASK64) * _FARMHASH_K2) & (
            _FARMHASH_MASK64
        )
    return _FARMHASH_K2


def _farmhash_len_17_to_32(buf: bytes) -> int:
    length = len(buf)
    mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
    a = (_farmhash_fetch64(buf, 0) * _FARMHASH_K1) & _FARMHASH_MASK64
    b = _farmhash_fetch64(buf, 8)
    c = (_farmhash_fetch64(buf, length - 8) * mul) & _FARMHASH_MASK64
    d = (_farmhash_fetch64(buf, length - 16) * _FARMHASH_K2) & _FARMHASH_MASK64
    return _farmhash_len_16(
        (_farmhash_rot64((a + b) & _FARMHASH_MASK64, 43) + _farmhash_rot64(c, 30) + d) & _FARMHASH_MASK64,
        (a + _farmhash_rot64((b + _FARMHASH_K2) & _FARMHASH_MASK64, 18) + c) & _FARMHASH_MASK64,
        mul,
    )


def _farmhash_len_33_to_64(buf: bytes) -> int:
    length = len(buf)
    mul = (_FARMHASH_K2 + length * 2) & _FARMHASH_MASK64
    a = (_farmhash_fetch64(buf, 0) * _FARMHASH_K2) & _FARMHASH_MASK64
    b = _farmhash_fetch64(buf, 8)
    c = (_farmhash_fetch64(buf, length - 8) * mul) & _FARMHASH_MASK64
    d = (_farmhash_fetch64(buf, length - 16) * _FARMHASH_K2) & _FARMHASH_MASK64
    y = (_farmhash_rot64((a + b) & _FARMHASH_MASK64, 43) + _farmhash_rot64(c, 30) + d) & _FARMHASH_MASK64
    z = _farmhash_len_16(
        y, (a + _farmhash_rot64((b + _FARMHASH_K2) & _FARMHASH_MASK64, 18) + c) & _FARMHASH_MASK64, mul
    )
    e = (_farmhash_fetch64(buf, 16) * mul) & _FARMHASH_MASK64
    f = _farmhash_fetch64(buf, 24)
    g = ((y + _farmhash_fetch64(buf, length - 32)) * mul) & _FARMHASH_MASK64
    h = ((z + _farmhash_fetch64(buf, length - 24)) * mul) & _FARMHASH_MASK64
    return _farmhash_len_16(
        (_farmhash_rot64((e + f) & _FARMHASH_MASK64, 43) + _farmhash_rot64(g, 30) + h) & _FARMHASH_MASK64,
        (e + _farmhash_rot64((f + a) & _FARMHASH_MASK64, 18) + g) & _FARMHASH_MASK64,
        mul,
    )


def farmhash_fingerprint64(value: bytes | str) -> int:
    """Compute Google's FarmHash ``Fingerprint64`` as a signed 64-bit integer matching ``FARM_FINGERPRINT``."""
    buf = value.encode("utf-8") if isinstance(value, str) else bytes(value)
    length = len(buf)
    if length <= 16:
        u64 = _farmhash_len_0_to_16(buf)
    elif length <= 32:
        u64 = _farmhash_len_17_to_32(buf)
    elif length <= 64:
        u64 = _farmhash_len_33_to_64(buf)
    else:
        seed = 81
        x = seed
        y = (seed * _FARMHASH_K1 + 113) & _FARMHASH_MASK64
        z = (_farmhash_shift_mix((y * _FARMHASH_K2 + 113) & _FARMHASH_MASK64) * _FARMHASH_K2) & _FARMHASH_MASK64
        v0, v1 = 0, 0
        w0, w1 = 0, 0
        x = (x * _FARMHASH_K2 + _farmhash_fetch64(buf, 0)) & _FARMHASH_MASK64
        end = ((length - 1) // 64) * 64
        last64 = end + ((length - 1) & 63) - 63
        offset = 0
        while True:
            x = (
                _farmhash_rot64((x + y + v0 + _farmhash_fetch64(buf, offset + 8)) & _FARMHASH_MASK64, 37) * _FARMHASH_K1
            ) & _FARMHASH_MASK64
            y = (
                _farmhash_rot64((y + v1 + _farmhash_fetch64(buf, offset + 48)) & _FARMHASH_MASK64, 42) * _FARMHASH_K1
            ) & _FARMHASH_MASK64
            x ^= w1
            y = (y + v0 + _farmhash_fetch64(buf, offset + 40)) & _FARMHASH_MASK64
            z = (_farmhash_rot64((z + w0) & _FARMHASH_MASK64, 33) * _FARMHASH_K1) & _FARMHASH_MASK64
            v0, v1 = _farmhash_weak_len_32(
                buf,
                offset,
                (v1 * _FARMHASH_K1) & _FARMHASH_MASK64,
                (x + w0) & _FARMHASH_MASK64,
            )
            w0, w1 = _farmhash_weak_len_32(
                buf,
                offset + 32,
                (z + w1) & _FARMHASH_MASK64,
                (y + _farmhash_fetch64(buf, offset + 16)) & _FARMHASH_MASK64,
            )
            z, x = x, z
            offset += 64
            if offset == end:
                break
        mul = (_FARMHASH_K1 + ((z & 0xFF) << 1)) & _FARMHASH_MASK64
        offset = last64
        w0 = (w0 + ((length - 1) & 63)) & _FARMHASH_MASK64
        v0 = (v0 + w0) & _FARMHASH_MASK64
        w0 = (w0 + v0) & _FARMHASH_MASK64
        x = (
            _farmhash_rot64((x + y + v0 + _farmhash_fetch64(buf, offset + 8)) & _FARMHASH_MASK64, 37) * mul
        ) & _FARMHASH_MASK64
        y = (
            _farmhash_rot64((y + v1 + _farmhash_fetch64(buf, offset + 48)) & _FARMHASH_MASK64, 42) * mul
        ) & _FARMHASH_MASK64
        x ^= w1
        y = (y + v0 + _farmhash_fetch64(buf, offset + 40)) & _FARMHASH_MASK64
        z = (_farmhash_rot64((z + w0) & _FARMHASH_MASK64, 33) * mul) & _FARMHASH_MASK64
        v0, v1 = _farmhash_weak_len_32(buf, offset, (v1 * mul) & _FARMHASH_MASK64, (x + w0) & _FARMHASH_MASK64)
        w0, w1 = _farmhash_weak_len_32(
            buf,
            offset + 32,
            (z + w1) & _FARMHASH_MASK64,
            (y + _farmhash_fetch64(buf, offset + 16)) & _FARMHASH_MASK64,
        )
        z, x = x, z
        u64 = _farmhash_len_16(
            (_farmhash_len_16(v0, w0, mul) + _farmhash_shift_mix(y) * _FARMHASH_K0 + z) & _FARMHASH_MASK64,
            (_farmhash_len_16(v1, w1, mul) + x) & _FARMHASH_MASK64,
            mul,
        )
    return u64 - 0x10000000000000000 if u64 >= 0x8000000000000000 else u64


def farm_fingerprint_shard(value: str, shard_count: int = 16) -> int:
    """Compute a deterministic non-negative shard index matching ``MOD(ABS(FARM_FINGERPRINT(...)), N)``."""
    return abs(farmhash_fingerprint64(value)) % max(1, int(shard_count))


def parse_utc_timestamp(value: Any) -> datetime:
    """Normalize a timestamp string or ``datetime`` instance into a timezone-aware UTC ``datetime``."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def positive_int_or_none(value: Any, name: str) -> int | None:
    """Validate optional positive integer settings without silently converting booleans."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        msg = f"{name} must be a positive integer when provided"
        raise ImproperConfigurationError(msg)
    return int(value)


def validate_spanner_identifier(identifier: str, label: str = "table") -> str:
    """Validate and normalize a Cloud Spanner table or index identifier."""
    stripped = str(identifier).strip()
    if not stripped or not _SAFE_TABLE_IDENT_RE.match(stripped):
        msg = f"Invalid Spanner {label} identifier: {identifier!r}"
        raise ImproperConfigurationError(msg)
    return stripped


def is_spanner_table_missing(exc: BaseException) -> bool:
    """Return whether ``exc`` reports a missing Spanner table or entity."""
    lower = str(exc).lower()
    if any(
        token in lower
        for token in (
            "table not found",
            "does not exist",
            "no such table",
            "not_found",
            "relation",
        )
    ):
        return "not found" in lower or "does not exist" in lower or "no such table" in lower
    return getattr(exc, "code", None) == 5


def object_to_artifact_reference(
    uri: str,
    version: int,
    *,
    content_type: str | None = None,
    size: int | None = None,
    updated: datetime | str | None = None,
    signed_url: str | None = None,
) -> ArtifactReference:
    """Construct an immutable ``vertebra.storage.ArtifactReference`` without BigQuery dependencies."""
    parsed_updated: datetime | None
    if updated is None:
        parsed_updated = None
    elif isinstance(updated, datetime):
        parsed_updated = updated if updated.tzinfo is not None else updated.replace(tzinfo=UTC)
    else:
        parsed_updated = parse_utc_timestamp(updated)
    return ArtifactReference(
        uri=str(uri),
        version=int(version),
        content_type=content_type,
        size=int(size) if size is not None else None,
        updated=parsed_updated,
        signed_url=signed_url,
    )


class SpannerConnectionBatch:
    """Buffered mutation context matching Spanner ``database.batch()`` semantics without third-party SDKs."""

    def __init__(self, driver: SyncSpannerDriver) -> None:
        self._driver = driver
        self._mutations: list[Mutation] = []

    def insert(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``insert`` mutation."""
        self._mutations.append(Mutation.insert(table, columns, values))

    def update(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``update`` mutation."""
        self._mutations.append(Mutation.update(table, columns, values))

    def insert_or_update(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``insert_or_update`` mutation."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def upsert(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``insert_or_update`` mutation."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def replace(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer a ``replace`` mutation."""
        self._mutations.append(Mutation.replace(table, columns, values))

    def delete(self, table: str, keys: Sequence[Any]) -> None:
        """Buffer a ``delete`` mutation."""
        self._mutations.append(Mutation.delete(table, keys))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None and self._mutations:
            self._driver.apply_mutations(self._mutations)


class SpannerConnectionShim:
    """Connection protocol shim backed by ``SyncSpannerDriver`` with zero third-party SDK imports."""

    def __init__(self, driver: SyncSpannerDriver) -> None:
        self._driver = driver

    @property
    def driver(self) -> SyncSpannerDriver:
        """Return the underlying ``SyncSpannerDriver``."""
        return self._driver

    @property
    def database_path(self) -> str:
        """Return the canonical ``projects/.../instances/.../databases/...`` resource path."""
        return self._driver.config.database_path

    @property
    def project_id(self) -> str:
        """Return the Google Cloud project ID."""
        return self._driver.config.project_id

    @property
    def instance_id(self) -> str:
        """Return the Cloud Spanner instance ID."""
        return self._driver.config.instance_id

    @property
    def database_id(self) -> str:
        """Return the Cloud Spanner database ID."""
        return self._driver.config.database_id

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the underlying Vertebra Spanner dialect literal."""
        return normalize_spanner_dialect(self._driver.config.dialect)

    @property
    def dialect(self) -> Literal["spanner", "spangres"]:
        """Return SQLSpec's dialect identifier (``spanner`` or ``spangres``)."""
        return sqlspec_dialect_name(self._driver.config.dialect)

    def run_in_transaction(
        self,
        fn: Any,
        *,
        tag: str = "",
        max_commit_delay_ms: int | None = None,
    ) -> Any:
        """Execute ``fn(transaction)`` inside an atomic ReadWrite transaction."""
        return self._driver.run_in_transaction(fn, tag=tag, max_commit_delay_ms=max_commit_delay_ms)

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> SyncSpannerSnapshot:
        """Open a lock-free ReadOnly ``SyncSpannerSnapshot``."""
        return self._driver.snapshot(staleness, multi_use=multi_use)

    def batch(self) -> SpannerConnectionBatch:
        """Return a buffered mutation context manager."""
        return SpannerConnectionBatch(self._driver)

    def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Apply Spanner schema DDL statements."""
        return self._driver.update_ddl(statements)

    def execute_sql(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Execute a SQL query or DML statement over ``SyncSpannerDriver``."""
        _ = kwargs
        stripped = sql.strip().upper()
        if stripped.startswith(("SELECT", "WITH", "GRAPH")):
            return list(self._driver.select(sql, params))
        self._driver.execute(sql, params)
        return []

    def query(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a read-only ``SELECT`` query returning row dictionaries."""
        return list(self._driver.select(sql, params))

    def close(self) -> None:
        """Close the wrapped ``SyncSpannerDriver``."""
        self._driver.close()


def _validate_dsn_scheme(dsn: str) -> None:
    if "://" not in dsn:
        msg = f"Invalid Spanner DSN {dsn!r}: expected spanner:// or spangres://"
        raise ImproperConfigurationError(msg)
    scheme = dsn.partition("://")[0].lower()
    base = scheme.split("+", 1)[0]
    if base not in _SPANNER_SCHEMES:
        msg = f"Unsupported Spanner DSN scheme {scheme!r}; expected 'spanner://' or 'spangres://'"
        raise ImproperConfigurationError(msg)


def resolve_spanner_config(
    config: SpannerConfig | Mapping[str, Any] | str | None = None,
    *,
    dsn: str | None = None,
    project_id: str | None = None,
    instance_id: str | None = None,
    database_id: str | None = None,
    database_role: str | None = None,
    dialect: str = "googlesql",
    endpoint: str | None = None,
    admin_endpoint: str | None = None,
    pool_size: int = 4,
    route_to_leader_enabled: bool = True,
    max_commit_delay_ms: int | None = None,
    data_boost_enabled: bool = True,
    max_partitions: int = 4,
    credentials_path: str | None = None,
    service_account_key: str | dict[str, Any] | None = None,
    quota_project_id: str | None = None,
    impersonate_service_account: str | None = None,
    use_gcloud_cli: bool = False,
    scopes: Sequence[str] | None = None,
    access_token: str | None = None,
    use_shm_cache: bool = True,
    statement_cache_capacity: int = 512,
    ca_pem: bytes | None = None,
    broker: Any = None,
) -> SpannerConfig:
    """Resolve explicit configuration arguments, mappings, or URIs into a canonical ``SpannerConfig``."""
    overrides: dict[str, Any] = {}
    if project_id:
        overrides["project_id"] = project_id
    if instance_id:
        overrides["instance_id"] = instance_id
    if database_id:
        overrides["database_id"] = database_id
    if database_role is not None:
        overrides["database_role"] = database_role
    if endpoint is not None:
        overrides["endpoint"] = endpoint
    if admin_endpoint is not None:
        overrides["admin_endpoint"] = admin_endpoint
    if pool_size != 4:
        overrides["pool_size"] = max(1, int(pool_size))
    if not route_to_leader_enabled:
        overrides["route_to_leader_enabled"] = False
    if max_commit_delay_ms is not None and int(max_commit_delay_ms) > 0:
        overrides["max_commit_delay_ms"] = int(max_commit_delay_ms)
    if not data_boost_enabled:
        overrides["data_boost_enabled"] = False
    if max_partitions != 4:
        overrides["max_partitions"] = max(1, int(max_partitions))
    if credentials_path is not None:
        overrides["credentials_path"] = credentials_path
    if service_account_key is not None:
        overrides["service_account_key"] = service_account_key
    if quota_project_id is not None:
        overrides["quota_project_id"] = quota_project_id
    if impersonate_service_account is not None:
        overrides["impersonate_service_account"] = impersonate_service_account
    if use_gcloud_cli:
        overrides["use_gcloud_cli"] = True
    if scopes is not None:
        overrides["scopes"] = tuple(scopes)
    if access_token is not None:
        overrides["access_token"] = access_token
    if not use_shm_cache:
        overrides["use_shm_cache"] = False
    if statement_cache_capacity != 512:
        overrides["statement_cache_capacity"] = max(16, int(statement_cache_capacity))
    if ca_pem is not None:
        overrides["ca_pem"] = ca_pem
    if broker is not None:
        overrides["broker"] = broker

    if isinstance(config, SpannerConfig):
        return msgspec.structs.replace(config, **overrides) if overrides else config
    if isinstance(config, str):
        _validate_dsn_scheme(config)
        return SpannerConfig.from_dsn(config, **overrides)
    if isinstance(config, Mapping):
        merged = dict(config)
        merged_dsn = merged.pop("dsn", None) or merged.pop("url", None) or dsn
        if "dialect" in merged:
            merged["dialect"] = normalize_spanner_dialect(merged["dialect"])
        elif dialect != "googlesql":
            merged["dialect"] = normalize_spanner_dialect(dialect)
        merged.update(overrides)
        if merged_dsn:
            _validate_dsn_scheme(str(merged_dsn))
            return SpannerConfig.from_dsn(str(merged_dsn), **merged)
        return SpannerConfig(**merged)
    if dsn is not None:
        _validate_dsn_scheme(dsn)
        return SpannerConfig.from_dsn(dsn, **overrides)
    if dialect:
        overrides["dialect"] = normalize_spanner_dialect(dialect)
    return SpannerConfig(**overrides)


def resolve_storage_backend(backend: ObjectStoreBackend | str | None) -> ObjectStoreBackend | None:
    """Resolve a storage backend instance or URI string into an ``ObjectStoreBackend``."""
    if backend is None:
        return None
    if isinstance(backend, ObjectStoreBackend):
        return backend
    if isinstance(backend, str):
        return ObjectStoreBackend(backend)
    msg = f"Unsupported Spanner storage_backend: {backend!r}"
    raise ImproperConfigurationError(msg)
