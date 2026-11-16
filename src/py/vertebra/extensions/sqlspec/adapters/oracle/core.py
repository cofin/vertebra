"""Oracle SQLSpec core helpers, exception mapping, compilation, version caching, and PL/SQL utilities."""

from __future__ import annotations

import array
import re
import threading
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.core.parameters import TypedParameter
from sqlspec.data_dictionary import VersionInfo
from sqlspec.data_dictionary.dialects.oracle import (
    ORACLE_JSON_STORAGE_BLOB_JSON,
    ORACLE_JSON_STORAGE_BLOB_PLAIN,
    ORACLE_JSON_STORAGE_NATIVE,
    parse_oracle_compatible_major,
    resolve_oracle_json_storage,
)
from sqlspec.exceptions import (
    CheckViolationError,
    DeadlockError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SerializationConflictError,
    SQLParsingError,
    UniqueViolationError,
)

from vertebra.db.adapters.oracle.config import OracleDriverFeatures
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.extensions.sqlspec._driver import (
    _normalize_bound_params,
    map_native_database_exception,
    native_statement_config,
    unwrap_typed_parameters,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "ORACLE_ALREADY_EXISTS_ERROR",
    "ORACLE_DUPLICATE_KEY_ERROR",
    "ORACLE_INDEX_NOT_FOUND_ERROR",
    "ORACLE_TABLE_NOT_FOUND_ERROR",
    "OracleVersionCache",
    "build_oracle_statement_config",
    "escape_oracle_literal",
    "is_oracle_already_exists",
    "is_oracle_table_missing",
    "is_oracle_unique_violation",
    "map_oracle_exception",
    "normalize_oracle_many_parameters",
    "normalize_oracle_parameters",
    "oracle_ensure_utc",
    "oracle_identifier",
    "oracle_index_name",
    "oracle_json_column_ddl",
    "oracle_json_dumps",
    "oracle_json_loads",
    "oracle_resolve_json_storage_mode",
    "oracle_row_value",
    "oracle_table_feature_clause",
    "oracle_table_ref",
    "sanitize_oracle_text_query",
    "validate_oracle_table_name",
)

ORACLE_DUPLICATE_KEY_ERROR: Final[int] = 1
ORACLE_TABLE_NOT_FOUND_ERROR: Final[int] = 942
ORACLE_ALREADY_EXISTS_ERROR: Final[int] = 955
ORACLE_INDEX_NOT_FOUND_ERROR: Final[int] = 1418

_ORACLE_IDENT_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")
_ORA_CODE_RE: Final[re.Pattern[str]] = re.compile(r"ORA-(\d{5})")
_ORACLE_DEFAULT_FEATURES: Final[OracleDriverFeatures] = OracleDriverFeatures()


class OracleVersionCache:
    """Thread-safe cache holding detected Oracle server version and compatibility metadata."""

    __slots__ = ("_lock", "compatible_major", "is_autonomous", "resolved", "version")

    def __init__(self) -> None:
        """Initialize an unresolved Oracle server version cache."""
        self._lock = threading.Lock()
        self.resolved: bool = False
        self.version: VersionInfo | None = None
        self.compatible_major: int | None = None
        self.is_autonomous: bool = False

    def set(
        self,
        version: VersionInfo | None,
        *,
        compatible_major: int | None = None,
        is_autonomous: bool = False,
    ) -> None:
        """Populate cached Oracle server metadata once resolution succeeds or gracefully completes."""
        with self._lock:
            self.version = version
            self.compatible_major = compatible_major
            self.is_autonomous = is_autonomous
            self.resolved = True

    def clear(self) -> None:
        """Invalidate cached version metadata."""
        with self._lock:
            self.resolved = False
            self.version = None
            self.compatible_major = None
            self.is_autonomous = False


def oracle_identifier(name: str, label: str = "identifier") -> str:
    """Validate an unquoted Oracle SQL identifier against safe characters."""
    candidate = name.strip()
    if not candidate or not _ORACLE_IDENT_RE.fullmatch(candidate):
        msg = f"Invalid Oracle {label}: {name!r}"
        raise ImproperConfigurationError(msg)
    return candidate


def validate_oracle_table_name(table_name: str, label: str = "table name") -> tuple[str, ...]:
    """Validate an unqualified or schema-qualified Oracle table identifier."""
    parts = [part.strip() for part in table_name.split(".")]
    if not parts or len(parts) > 2 or any(not part for part in parts):
        msg = f"Invalid Oracle {label}: {table_name!r}"
        raise ImproperConfigurationError(msg)
    for part in parts:
        oracle_identifier(part, label)
    return tuple(parts)


def oracle_table_ref(table_name: str, label: str = "table name") -> str:
    """Return a validated Oracle table reference (`schema.table` or `table`)."""
    parts = validate_oracle_table_name(table_name, label)
    return ".".join(parts)


def oracle_index_name(prefix: str, table_name: str, suffix: str) -> str:
    """Construct a bounded 30-character Oracle identifier for compatibility across releases."""
    parts = validate_oracle_table_name(table_name)
    bare = parts[-1].lower().replace(".", "_")
    clean = re.sub(r"[^A-Za-z0-9_]", "_", f"{prefix}_{bare}_{suffix}")
    return clean[:30]


def escape_oracle_literal(value: str) -> str:
    """Escape single quotes for PL/SQL string embedding."""
    return value.replace("'", "''")


def build_oracle_statement_config(statement_config: StatementConfig | None = None) -> StatementConfig:
    """Construct the canonical Oracle `StatementConfig` supporting `:name` and `:1` binds."""
    if statement_config is not None:
        return statement_config
    config = native_statement_config(
        "oracle",
        ParameterStyle.NAMED_COLON,
        default_style=ParameterStyle.NAMED_COLON,
        supported_styles=(
            ParameterStyle.NAMED_COLON,
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
        ),
        supported_execution_styles=(ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON),
        preserve_parameter_format=False,
    )
    param_cfg: ParameterStyleConfig = config.parameter_config
    return config.replace(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=param_cfg.default_parameter_style,
            supported_parameter_styles=param_cfg.supported_parameter_styles,
            default_execution_parameter_style=ParameterStyle.NAMED_COLON,
            supported_execution_parameter_styles={
                ParameterStyle.NAMED_COLON,
                ParameterStyle.POSITIONAL_COLON,
            },
            type_coercion_map=param_cfg.type_coercion_map,
            preserve_parameter_format=False,
            allow_mixed_parameter_styles=True,
        )
    )


def _unwrap_oracle_scalar(value: Any) -> Any:
    """Recursively unwrap `TypedParameter` before `OracleCore` coercion."""
    if isinstance(value, TypedParameter):
        return _unwrap_oracle_scalar(value.value)
    if isinstance(value, array.array):
        return value
    if isinstance(value, list):
        return [_unwrap_oracle_scalar(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_unwrap_oracle_scalar(item) for item in value)
    if isinstance(value, Mapping):
        return {key: _unwrap_oracle_scalar(val) for key, val in value.items()}
    return value


def normalize_oracle_parameters(
    params: Any,
    *,
    features: OracleDriverFeatures | None = None,
) -> Any:
    """Unwrap `TypedParameter` values and apply `OracleCore` JSON/`VECTOR` coercion."""
    normalized = _normalize_bound_params(params)
    if normalized is None:
        return None
    unwrapped = _unwrap_oracle_scalar(unwrap_typed_parameters(normalized))
    effective_features = features if features is not None else _ORACLE_DEFAULT_FEATURES
    if isinstance(unwrapped, Mapping):
        cleaned = {str(k).lstrip(":"): v for k, v in unwrapped.items()}
        return OracleCore.coerce_parameters(cleaned, features=effective_features)
    if isinstance(unwrapped, (tuple, list)):
        coerced = OracleCore.coerce_parameters(list(unwrapped), features=effective_features)
        if coerced is None:
            return None
        return tuple(coerced) if isinstance(unwrapped, tuple) else list(coerced)
    coerced_single = OracleCore.coerce_parameters([unwrapped], features=effective_features)
    return tuple(coerced_single) if coerced_single is not None else None


def normalize_oracle_many_parameters(
    params: Any,
    *,
    features: OracleDriverFeatures | None = None,
) -> list[Any]:
    """Normalize batched bind parameters for Oracle `execute_many`."""
    if params is None:
        return []
    if isinstance(params, Sequence) and not isinstance(params, (str, bytes, bytearray)):
        result: list[Any] = []
        for entry in params:
            norm = normalize_oracle_parameters(entry, features=features)
            if norm is not None:
                result.append(norm)
        return result
    return []


def _extract_ora_code(exc: BaseException) -> int | None:
    """Extract numeric Oracle `ORA-XXXXX` code from exception attributes or text."""
    for candidate in (exc, *(exc.args if isinstance(exc.args, tuple) else ())):
        code = getattr(candidate, "code", None)
        if isinstance(code, int):
            return code
    match = _ORA_CODE_RE.search(str(exc))
    if match:
        return int(match.group(1))
    return None


def map_oracle_exception(exc: BaseException) -> Exception | None:
    """Map Oracle ORA error codes onto Vertebra `DatabaseError` subclasses."""
    code = _extract_ora_code(exc)
    message = str(exc)
    if code == ORACLE_DUPLICATE_KEY_ERROR:
        return UniqueViolationError(message)
    if code in {2291, 2292}:
        return ForeignKeyViolationError(message)
    if code in {1400, 1407}:
        return NotNullViolationError(message)
    if code == 2290:
        return CheckViolationError(message)
    if code == 60:
        return DeadlockError(message)
    if code == 8177:
        return SerializationConflictError(message)
    if code in {942, 955, 904, 933, 936}:
        return SQLParsingError(message)
    return map_native_database_exception(exc)


def is_oracle_table_missing(exc: BaseException) -> bool:
    """Return True when an Oracle exception indicates `ORA-00942` missing table or view."""
    return _extract_ora_code(exc) == ORACLE_TABLE_NOT_FOUND_ERROR or "ORA-00942" in str(exc)


def is_oracle_already_exists(exc: BaseException) -> bool:
    """Return True when an Oracle exception indicates `ORA-00955` name already in use."""
    return _extract_ora_code(exc) == ORACLE_ALREADY_EXISTS_ERROR or "ORA-00955" in str(exc)


def is_oracle_unique_violation(exc: BaseException) -> bool:
    """Return True when an Oracle exception indicates `ORA-00001` unique constraint violation."""
    return isinstance(exc, UniqueViolationError) or _extract_ora_code(exc) == ORACLE_DUPLICATE_KEY_ERROR


def oracle_row_value(row: Any, key: str, index: int = 0) -> Any:
    """Read an Oracle row field case-insensitively by mapping key or positional index."""
    if isinstance(row, Mapping):
        if key in row:
            return row[key]
        lowered = key.lower()
        for col_name, value in row.items():
            if str(col_name).lower() == lowered:
                return value
        return None
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        if 0 <= index < len(row):
            return row[index]
        return None
    return getattr(row, key, None)


def oracle_ensure_utc(value: Any) -> datetime:
    """Normalize Oracle timestamp values or ISO strings into timezone-aware UTC `datetime`."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return datetime.now(UTC)


def oracle_json_dumps(value: Any, *, serializer: Any = None) -> str:
    """Serialize JSON values for Oracle `JSON` / `BLOB CHECK (... IS JSON)` storage."""
    if serializer is not None:
        encoded = serializer(value)
        return encoded.decode("utf-8") if isinstance(encoded, (bytes, bytearray)) else str(encoded)
    encoded = encode_json(value)
    return encoded.decode("utf-8") if isinstance(encoded, (bytes, bytearray)) else str(encoded)


def oracle_json_loads(value: Any, *, deserializer: Any = None) -> Any:
    """Decode Oracle JSON, OSON, CLOB, or BLOB values into Python structures."""
    if value is None:
        return None
    if isinstance(value, (dict, list, bool, int, float)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw_bytes = bytes(value)
        if len(raw_bytes) >= 8 and raw_bytes[:3] == b"\xffJZ":
            return OracleCore.decode_oson(raw_bytes)
        raw_text = raw_bytes.decode("utf-8")
    else:
        raw_text = str(value)
    if deserializer is not None:
        return deserializer(raw_text)
    return decode_json(raw_text)


def oracle_resolve_json_storage_mode(
    version_info: VersionInfo | None,
    *,
    override: str | None = None,
    compatible_major: int | None = None,
) -> str:
    """Resolve Oracle JSON storage strategy (`json` for 21c+, `blob_json` for 12c-19c, `blob_plain` otherwise)."""
    if override in {ORACLE_JSON_STORAGE_NATIVE, ORACLE_JSON_STORAGE_BLOB_JSON, ORACLE_JSON_STORAGE_BLOB_PLAIN}:
        return override
    if version_info is None:
        return ORACLE_JSON_STORAGE_BLOB_JSON
    resolved_compat = (
        compatible_major if compatible_major is not None else parse_oracle_compatible_major(str(version_info.major))
    )
    return resolve_oracle_json_storage(version_info.major, resolved_compat)


def oracle_json_column_ddl(column_name: str, storage_mode: str, *, not_null: bool = True) -> str:
    """Build Oracle DDL column definition for `JSON` (21c+), `BLOB CHECK (col IS JSON)`, or `BLOB`."""
    col = oracle_identifier(column_name, "column name")
    null_clause = " NOT NULL" if not_null else ""
    if storage_mode == ORACLE_JSON_STORAGE_NATIVE:
        return f"{col} JSON{null_clause}"
    if storage_mode == ORACLE_JSON_STORAGE_BLOB_JSON:
        return f"{col} BLOB CHECK ({col} IS JSON){null_clause}"
    return f"{col} BLOB{null_clause}"


def oracle_table_feature_clause(settings: Mapping[str, Any] | None) -> str:
    """Build optional Oracle `INMEMORY` / `COMPRESS` / custom `table_options` DDL suffixes."""
    source = settings if isinstance(settings, Mapping) else {}
    clauses: list[str] = []
    if source.get("in_memory"):
        clauses.append("INMEMORY")
    compression = source.get("compression")
    if isinstance(compression, str) and compression.strip():
        clauses.append(compression.strip())
    table_options = source.get("table_options")
    if isinstance(table_options, str) and table_options.strip():
        clauses.append(table_options.strip())
    return (" " + " ".join(clauses)) if clauses else ""


def sanitize_oracle_text_query(query: str, *, operator: str = "ACCUM") -> str:
    """Sanitize raw user text into safe Oracle Text `CONTAINS` search expressions."""
    tokens = [token for token in re.findall(r"[A-Za-z0-9_]+", query) if token]
    if not tokens:
        return ""
    joiner = f" {operator.upper()} "
    return joiner.join(tokens)
