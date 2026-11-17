"""SQL Server (T-SQL) SQLSpec core statement compilation, coercion, and error helpers."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final, Literal

from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    TypedParameter,
    build_statement_config_from_profile,
)
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.config_tools import parse_odbc_connection_string
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions

from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mssql.core import MSSQLCore
from vertebra.typing import ImproperConfigurationError

__all__ = (
    "MSSQL_TABLE_NOT_FOUND_ERROR",
    "build_connection_config",
    "build_mssql_scope_where",
    "build_statement_config",
    "cosine_similarity",
    "default_statement_config",
    "driver_profile",
    "escape_mssql_like",
    "escape_sql_literal",
    "extract_mssql_error_number",
    "extract_mssql_sqlstate",
    "is_mssql_table_missing",
    "map_mssql_exception",
    "mssql_constraint_ref",
    "mssql_ensure_utc",
    "mssql_json_dict",
    "mssql_row_value",
    "mssql_table_ref",
    "normalize_mssql_many_parameters",
    "normalize_mssql_parameters",
    "quote_bracket_identifier",
    "rewrite_mssql_placeholders",
    "split_schema_and_table",
    "unwrap_typed_parameters",
)

MSSQL_TABLE_NOT_FOUND_ERROR: Final[int] = 208

_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?:\(([-]?\d+)(?:,|\))|\bMsg\s+([-]?\d+)\b)")
_MSSQL_CONSTRAINT_547: Final[int] = 547
_SQLSTATE_LENGTH: Final[int] = 5

_ODBC_SQLSTATE_MAP: Final[dict[str, tuple[type[SQLSpecError], str]]] = {
    "08S01": (DatabaseConnectionError, "communication link failure"),
    "08001": (DatabaseConnectionError, "unable to connect to data source"),
    "HYT00": (QueryTimeoutError, "query timeout expired"),
    "HY008": (QueryTimeoutError, "operation canceled"),
}

_ERROR_CODE_MAP: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    220: (DataError, "arithmetic overflow error"),
    258: (QueryTimeoutError, "timeout waiting for latch"),
    2601: (UniqueViolationError, "unique constraint violation"),
    2627: (UniqueViolationError, "unique constraint violation"),
    3617: (QueryTimeoutError, "query canceled by user or attention event"),
    3930: (TransactionError, "current transaction cannot be committed"),
    515: (NotNullViolationError, "not-null constraint violation"),
    18456: (PermissionDeniedError, "permission denied"),
    229: (PermissionDeniedError, "permission denied"),
    230: (PermissionDeniedError, "permission denied"),
    262: (PermissionDeniedError, "permission denied"),
    4060: (DatabaseConnectionError, "database connection error"),
    53: (DatabaseConnectionError, "database connection error"),
    1205: (DeadlockError, "deadlock detected"),
    3960: (SerializationConflictError, "snapshot isolation conflict"),
    -2: (QueryTimeoutError, "query timeout"),
    1222: (QueryTimeoutError, "lock request time out period exceeded"),
    102: (SQLParsingError, "SQL syntax error"),
    105: (SQLParsingError, "unclosed quotation mark"),
    156: (SQLParsingError, "incorrect syntax near keyword"),
    207: (SQLParsingError, "invalid column name"),
    208: (SQLParsingError, "invalid object name"),
    241: (DataError, "data conversion error"),
    242: (DataError, "datetime conversion out of range"),
    245: (DataError, "data conversion error"),
    2628: (DataError, "string or binary data would be truncated"),
    8114: (DataError, "data conversion error"),
    8115: (DataError, "arithmetic overflow error"),
    8152: (DataError, "string or binary data would be truncated"),
    1105: (OperationalError, "operational error"),
    20003: (QueryTimeoutError, "query timeout expired"),
    20002: (DatabaseConnectionError, "server connection failed"),
    20009: (DatabaseConnectionError, "unable to connect: host unreachable or server down"),
    20047: (DatabaseConnectionError, "connection closed or broken"),
}


def _identity(value: Any) -> Any:
    return value


def driver_profile() -> DriverParameterProfile:
    """Build the SQL Server parameter profile targeting `ParameterStyle.QMARK` for `@P1..@Pn` rewrite."""
    coercions: dict[type, Callable[[Any], Any]] = {
        bool: _identity,
        int: _identity,
        float: _identity,
        bytes: _identity,
        **build_uuid_coercions(native=False),
    }
    return DriverParameterProfile(
        name="vertebra_mssql",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_PYFORMAT},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=False,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=coercions,
        default_dialect="tsql",
        extras={"json_serializer": to_json, "json_deserializer": from_json},
    )


def build_statement_config(
    *,
    json_serializer: Callable[[Any], str] | None = None,
    json_deserializer: Callable[[str], Any] | None = None,
) -> StatementConfig:
    """Build a SQL Server (`tsql`) `StatementConfig` targeting `ParameterStyle.QMARK` `?` placeholders."""
    return build_statement_config_from_profile(
        driver_profile(),
        statement_overrides={"dialect": "tsql"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


def default_statement_config() -> StatementConfig:
    """Return a fresh default SQL Server `StatementConfig`."""
    return build_statement_config()


def rewrite_mssql_placeholders(sql: str) -> tuple[str, int]:
    """Rewrite `?` and `$1..$n` placeholders into T-SQL `@P1..@Pn` via `MSSQLCore`."""
    return MSSQLCore.rewrite_placeholders(sql)


def unwrap_typed_parameters(bound: Any) -> Any:
    """Unwrap `TypedParameter` wrappers recursively across dicts, sequences, and scalars."""
    if isinstance(bound, dict):
        return {key: unwrap_typed_parameters(value) for key, value in bound.items()}
    if isinstance(bound, tuple):
        return tuple(unwrap_typed_parameters(value) for value in bound)
    if isinstance(bound, list):
        return [unwrap_typed_parameters(value) for value in bound]
    if isinstance(bound, TypedParameter):
        return unwrap_typed_parameters(bound.value)
    return bound


def normalize_mssql_parameters(params: Any) -> Any:
    """Unwrap `TypedParameter` nodes and convert positional parameter lists to native tuples."""
    if params is None:
        return None
    unwrapped = unwrap_typed_parameters(params)
    if isinstance(unwrapped, list):
        return tuple(unwrapped) if unwrapped else None
    if isinstance(unwrapped, tuple):
        return unwrapped if unwrapped else None
    if isinstance(unwrapped, Mapping):
        return dict(unwrapped) if unwrapped else None
    return unwrapped


def normalize_mssql_many_parameters(prepared: Any) -> list[tuple[Any, ...]]:
    """Normalize `execute_many` compiled parameter batches into `list[tuple[Any, ...]]`."""
    if not prepared:
        return []
    unwrapped = unwrap_typed_parameters(prepared)
    rows: list[tuple[Any, ...]] = []
    for item in unwrapped:
        if isinstance(item, tuple):
            rows.append(item)
        elif isinstance(item, list):
            rows.append(tuple(item))
        else:
            rows.append((item,))
    return rows


def quote_bracket_identifier(identifier: str) -> str:
    """Quote a T-SQL identifier segment using square brackets."""
    cleaned = identifier.strip()
    if (cleaned.startswith("[") and cleaned.endswith("]")) or (cleaned.startswith('"') and cleaned.endswith('"')):
        cleaned = cleaned[1:-1].replace("]]", "]").replace('""', '"')
    return f"[{cleaned.replace(']', ']]')}]"


def escape_sql_literal(value: str) -> str:
    """Escape single quotes inside a T-SQL string literal."""
    return value.replace("'", "''")


def split_schema_and_table(identifier: str, *, default_schema: str = "dbo") -> tuple[str, str]:
    """Split a T-SQL table name into unquoted `(schema, table)` components."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise ImproperConfigurationError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"', allow_bracket_quotes=True)
    if not parts:
        return default_schema, cleaned
    if len(parts) == 1:
        return default_schema, parts[0]
    return parts[-2], parts[-1]


def mssql_table_ref(table_name: str, *, default_schema: str = "dbo") -> str:
    """Return a bracket-quoted `[schema].[table]` T-SQL reference."""
    schema_name, bare_table = split_schema_and_table(table_name, default_schema=default_schema)
    return f"{quote_bracket_identifier(schema_name)}.{quote_bracket_identifier(bare_table)}"


def mssql_constraint_ref(prefix: str, table_name: str, suffix: str) -> str:
    """Build a deterministic bracket-quoted T-SQL constraint identifier."""
    _, bare_table = split_schema_and_table(table_name)
    return quote_bracket_identifier(f"{prefix}_{bare_table}_{suffix}")


def mssql_row_value(row: Any, key: str, index: int = 0) -> Any:
    """Extract a field from a dict-like or sequence-like row preserving SQL Server casing differences."""
    if isinstance(row, Mapping):
        if key in row:
            return row[key]
        upper_key = key.upper()
        if upper_key in row:
            return row[upper_key]
        lower_key = key.lower()
        for k, v in row.items():
            if str(k).lower() == lower_key:
                return v
        return None
    if isinstance(row, (list, tuple)) and len(row) > index:
        return row[index]
    return getattr(row, key, None)


def mssql_json_dict(value: Any, *, deserializer: Callable[[str], Any] = from_json) -> dict[str, Any]:
    """Decode a JSON string, bytes, or dict payload from SQL Server into a `dict[str, Any]`."""
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        decoded = deserializer(stripped)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def mssql_ensure_utc(value: Any) -> datetime:
    """Normalize a datetime or ISO-8601 value from SQL Server into a UTC-aware `datetime`."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    return datetime.now(UTC)


def extract_mssql_error_number(exc: BaseException | None) -> int | None:
    """Extract numeric SQL Server error code from driver attributes or TDS error text."""
    if exc is None:
        return None
    for attr in ("number", "error_code", "errno"):
        val = getattr(exc, attr, None)
        if isinstance(val, int) and not isinstance(val, bool) and val != 0:
            return val
    if exc.args:
        first_arg = exc.args[0]
        if isinstance(first_arg, int) and not isinstance(first_arg, bool) and first_arg != 0:
            return first_arg
        if isinstance(first_arg, tuple) and first_arg:
            nested = first_arg[0]
            if isinstance(nested, int) and not isinstance(nested, bool) and nested != 0:
                return nested
    matches = _ERROR_NUMBER_PATTERN.findall(str(exc))
    if not matches:
        return None
    last_match = matches[-1]
    return int(last_match[0] or last_match[1])


def extract_mssql_sqlstate(exc: BaseException | None) -> str | None:
    """Extract a 5-character SQLSTATE code from a SQL Server exception."""
    if exc is None:
        return None
    val = getattr(exc, "sqlstate", None)
    if isinstance(val, str) and val.strip():
        return val.strip().upper()
    if hasattr(exc, "args") and len(exc.args) > 1:
        second = exc.args[1]
        if isinstance(second, str) and len(second.strip()) == _SQLSTATE_LENGTH:
            return second.strip().upper()
    msg = str(exc).upper()
    for code in _ODBC_SQLSTATE_MAP:
        if code in msg:
            return code
    return None


def is_mssql_table_missing(exc: BaseException) -> bool:
    """Return `True` when `exc` represents SQL Server error 208 ('Invalid object name')."""
    if extract_mssql_error_number(exc) == MSSQL_TABLE_NOT_FOUND_ERROR:
        return True
    return "invalid object name" in str(exc).lower()


def map_mssql_exception(exc: BaseException) -> SQLSpecError | None:
    """Map SQL Server error numbers (`2601`, `2627`, `547`, `1205`, `208`, etc.) to SQLSpec exceptions."""
    sqlstate = extract_mssql_sqlstate(exc)
    if sqlstate is not None and sqlstate in _ODBC_SQLSTATE_MAP:
        err_cls, desc = _ODBC_SQLSTATE_MAP[sqlstate]
        if sqlstate in {"08S01", "08001"} and "timeout" in str(exc).lower():
            return ConnectionTimeoutError(f"SQL Server SQLSTATE {sqlstate}: {desc}. Original error: {exc}")
        return err_cls(f"SQL Server SQLSTATE {sqlstate}: {desc}. Original error: {exc}")

    err_no = extract_mssql_error_number(exc)
    if err_no == _MSSQL_CONSTRAINT_547:
        msg = str(exc).lower()
        if "check constraint" in msg:
            return CheckViolationError(f"SQL Server error 547: check constraint violation. Original error: {exc}")
        return ForeignKeyViolationError(
            f"SQL Server error 547: foreign key constraint violation. Original error: {exc}"
        )
    if err_no is not None and err_no in _ERROR_CODE_MAP:
        err_cls, desc = _ERROR_CODE_MAP[err_no]
        return err_cls(f"SQL Server error {err_no}: {desc}. Original error: {exc}")

    lower = str(exc).lower()
    if "unique key constraint" in lower or "duplicate key" in lower:
        return UniqueViolationError(f"SQL Server unique constraint violation. Original error: {exc}")
    if "cannot insert the value null" in lower or "does not allow nulls" in lower:
        return NotNullViolationError(f"SQL Server not-null constraint violation. Original error: {exc}")
    if "check constraint" in lower:
        return CheckViolationError(f"SQL Server check constraint violation. Original error: {exc}")
    if "foreign key constraint" in lower:
        return ForeignKeyViolationError(f"SQL Server foreign key constraint violation. Original error: {exc}")
    if "deadlock" in lower:
        return DeadlockError(f"SQL Server deadlock detected. Original error: {exc}")
    if "invalid object name" in lower:
        return SQLParsingError(f"SQL Server invalid object name. Original error: {exc}")
    exc_name = type(exc).__name__
    if exc_name == "IntegrityError":
        return IntegrityError(f"SQL Server integrity error. Original error: {exc}")
    return None


def build_mssql_scope_where(
    app_name: str,
    user_id: str,
    scope_filter: Literal["all", "user", "app"],
) -> tuple[str, tuple[Any, ...]]:
    """Build SQL Server memory scope filter predicate and ordered positional bind values."""
    if scope_filter == "all":
        return "app_name = ? AND ((scope = N'user' AND user_id = ?) OR scope = N'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = ? AND scope = N'user' AND user_id = ?", (app_name, user_id)
    return "app_name = ? AND scope = N'app'", (app_name,)


def escape_mssql_like(value: str) -> str:
    """Escape wildcard characters for T-SQL `LIKE ... ESCAPE N'\\\\'` queries."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("[", "\\[")


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """Compute cosine similarity between two equal-length float vectors."""
    if len(left) != len(right) or not left:
        return 0.0
    dot = 0.0
    norm_left = 0.0
    norm_right = 0.0
    for a, b in zip(left, right, strict=False):
        dot += a * b
        norm_left += a * a
        norm_right += b * b
    denom = math.sqrt(norm_left) * math.sqrt(norm_right)
    return dot / denom if denom > 0.0 else 0.0


def _parse_bool_option(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _parse_server_host_port(server_value: str) -> tuple[str, int | None]:
    raw = server_value.strip()
    if raw.lower().startswith("tcp:"):
        raw = raw[4:]
    if "," in raw:
        host_part, port_part = raw.rsplit(",", 1)
        try:
            return host_part.strip(), int(port_part.strip())
        except ValueError:
            return host_part.strip(), None
    if raw.startswith("[") and "]:" in raw:
        host_part, port_part = raw[1:].split("]:", 1)
        try:
            return host_part.strip(), int(port_part.strip())
        except ValueError:
            return host_part.strip(), None
    if raw.count(":") == 1:
        host_part, port_part = raw.split(":", 1)
        try:
            return host_part.strip(), int(port_part.strip())
        except ValueError:
            return raw, None
    return raw, None


def build_connection_config(
    connection_config: Mapping[str, Any] | None = None,
    driver_features: Mapping[str, Any] | None = None,
    broker: Any = None,
    overrides: Mapping[str, Any] | None = None,
) -> MSSQLConfig:
    """Build an immutable `MSSQLConfig` from SQLSpec connection settings, URLs, and ODBC strings."""
    merged: dict[str, Any] = {}
    if isinstance(connection_config, Mapping):
        merged.update(connection_config)
    if isinstance(overrides, Mapping):
        merged.update(overrides)

    odbc_str = merged.pop("connection_string", None)
    if isinstance(odbc_str, str) and odbc_str.strip():
        odbc_parsed: dict[str, str] = {}
        for raw_key, raw_val in parse_odbc_connection_string(odbc_str):
            odbc_parsed[raw_key.strip().lower()] = raw_val
        server_raw = odbc_parsed.get("server") or odbc_parsed.get("address") or odbc_parsed.get("addr")
        if server_raw:
            parsed_host, parsed_port = _parse_server_host_port(server_raw)
            if parsed_host and "host" not in merged:
                merged["host"] = parsed_host
            if parsed_port is not None and "port" not in merged:
                merged["port"] = parsed_port
        for odbc_key, target in (
            ("database", "database"),
            ("db", "database"),
            ("uid", "user"),
            ("user", "user"),
            ("username", "user"),
            ("pwd", "password"),
            ("password", "password"),
            ("hostnameincertificate", "server_name"),
            ("encrypt", "encryption"),
        ):
            if odbc_key in odbc_parsed and target not in merged:
                merged[target] = odbc_parsed[odbc_key]
        trust_val = odbc_parsed.get("trustservercertificate") or odbc_parsed.get("trust")
        if trust_val is not None and "trust_server_certificate" not in merged:
            parsed_trust = _parse_bool_option(trust_val)
            if parsed_trust is not None:
                merged["trust_server_certificate"] = parsed_trust
        intent_val = odbc_parsed.get("applicationintent")
        if intent_val and intent_val.strip().lower() == "readonly" and "read_only_intent" not in merged:
            merged["read_only_intent"] = True

    url = merged.pop("url", None) or merged.pop("dsn", None)
    server_val = merged.pop("server", None) or merged.pop("address", None) or merged.pop("addr", None)
    if isinstance(server_val, str) and server_val.strip() and "host" not in merged:
        parsed_host, parsed_port = _parse_server_host_port(server_val)
        if parsed_host:
            merged["host"] = parsed_host
        if parsed_port is not None and "port" not in merged:
            merged["port"] = parsed_port

    for alias, target in (
        ("db", "database"),
        ("uid", "user"),
        ("username", "user"),
        ("pwd", "password"),
        ("encrypt", "encryption"),
    ):
        if alias in merged and target not in merged:
            merged[target] = merged.pop(alias)
        else:
            merged.pop(alias, None)

    if "connect_timeout_ms" not in merged:
        for timeout_key in ("connect_timeout", "login_timeout", "connection_timeout"):
            val = merged.pop(timeout_key, None)
            if val is not None:
                merged["connect_timeout_ms"] = max(1, int(float(val) * 1000))
                break
    for stale_timeout in ("connect_timeout", "login_timeout", "connection_timeout"):
        merged.pop(stale_timeout, None)

    if "query_timeout_ms" not in merged:
        for q_key in ("command_timeout", "timeout"):
            val = merged.pop(q_key, None)
            if val is not None:
                merged["query_timeout_ms"] = max(1, int(float(val) * 1000))
                break
    merged.pop("command_timeout", None)
    merged.pop("timeout", None)

    if "cloud_sql_instance" not in merged and isinstance(driver_features, Mapping):
        if driver_features.get("cloud_sql_instance"):
            merged["cloud_sql_instance"] = driver_features["cloud_sql_instance"]
        if driver_features.get("cloudsql_ip_type") and "cloudsql_ip_type" not in merged:
            merged["cloudsql_ip_type"] = str(driver_features["cloudsql_ip_type"])
        if driver_features.get("cloudsql_auth_type") and "cloudsql_auth_type" not in merged:
            merged["cloudsql_auth_type"] = str(driver_features["cloudsql_auth_type"])
        if driver_features.get("enable_iam_auth") and "cloudsql_auth_type" not in merged:
            merged["cloudsql_auth_type"] = "IAM"

    for ignored_key in (
        "pool_recycle_seconds",
        "health_check_interval",
        "connection_validation_sql",
        "driver_features",
        "enable_pooling",
        "json_serializer",
        "json_deserializer",
    ):
        merged.pop(ignored_key, None)

    allowed_fields = set(MSSQLConfig.__struct_fields__)
    cfg_kwargs = {k: v for k, v in merged.items() if k in allowed_fields}
    if broker is not None and "broker" not in cfg_kwargs:
        cfg_kwargs["broker"] = broker

    if isinstance(url, str) and url.strip():
        return MSSQLConfig.from_url(url.strip(), **cfg_kwargs)
    return MSSQLConfig(**cfg_kwargs)
