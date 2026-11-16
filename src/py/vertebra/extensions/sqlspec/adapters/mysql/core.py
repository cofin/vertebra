"""MySQL and MariaDB SQLSpec core statement compilation, coercion, and error helpers."""

import math
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.data_dictionary.dialects.mysql.config import parse_mysql_engine_version
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
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_decimal_converter, build_uuid_coercions

from vertebra.db.adapters.mysql.config import MySQLConfig
from vertebra.extensions.sqlspec._driver import unwrap_typed_parameters
from vertebra.typing import ImproperConfigurationError

__all__ = (
    "MYSQL_SUPPORTS_VECTOR_ERR_HINT",
    "MYSQL_TABLE_NOT_FOUND_ERRNO",
    "build_connection_config",
    "build_mysql_scope_where",
    "build_statement_config",
    "cosine_similarity",
    "default_statement_config",
    "driver_profile",
    "format_mysql_vector_literal",
    "is_mysql_table_missing",
    "map_mysql_exception",
    "mysql_index_options",
    "mysql_json_dict",
    "mysql_json_for_storage",
    "mysql_owner_id_column_parts",
    "mysql_supports_native_vector",
    "mysql_table_options",
    "normalize_mysql_many_parameters",
    "normalize_mysql_parameters",
    "parse_mysql_vector",
)

MYSQL_TABLE_NOT_FOUND_ERRNO = 1146
MYSQL_SUPPORTS_VECTOR_ERR_HINT = "STRING_TO_VECTOR"

_SERVER_ERR_RE = re.compile(r"ERROR\s+(\d+)\s+\(([0-9A-Z]{5})\):")
_REFERENCES_RE = re.compile(r"\s+REFERENCES\s+(.+)", re.IGNORECASE)

_ERRNO_MAP: dict[int, type[SQLSpecError]] = {
    1062: UniqueViolationError,
    1169: UniqueViolationError,
    1586: UniqueViolationError,
    1216: ForeignKeyViolationError,
    1217: ForeignKeyViolationError,
    1451: ForeignKeyViolationError,
    1452: ForeignKeyViolationError,
    1048: NotNullViolationError,
    1364: NotNullViolationError,
    3819: CheckViolationError,
    4025: CheckViolationError,
    1044: PermissionDeniedError,
    1045: PermissionDeniedError,
    1142: PermissionDeniedError,
    1213: DeadlockError,
    1205: QueryTimeoutError,
    1969: QueryTimeoutError,
    3024: QueryTimeoutError,
    1317: OperationCancelledError,
    2013: ConnectionTimeoutError,
    1040: DatabaseConnectionError,
    1042: DatabaseConnectionError,
    1043: DatabaseConnectionError,
    1047: DatabaseConnectionError,
    1053: DatabaseConnectionError,
    1152: DatabaseConnectionError,
    1153: DatabaseConnectionError,
    1154: DatabaseConnectionError,
    1155: DatabaseConnectionError,
    1156: DatabaseConnectionError,
    1157: DatabaseConnectionError,
    1158: DatabaseConnectionError,
    1159: DatabaseConnectionError,
    1160: DatabaseConnectionError,
    1161: DatabaseConnectionError,
    2002: DatabaseConnectionError,
    2003: DatabaseConnectionError,
    2004: DatabaseConnectionError,
    2005: DatabaseConnectionError,
    2006: DatabaseConnectionError,
    2026: DatabaseConnectionError,
    2055: DatabaseConnectionError,
    4031: DatabaseConnectionError,
    1264: DataError,
    1265: DataError,
    1292: DataError,
    1366: DataError,
    1406: DataError,
    1411: DataError,
    1690: DataError,
    3140: DataError,
    3141: DataError,
    3142: DataError,
    3143: DataError,
    1049: SQLParsingError,
    1050: SQLParsingError,
    1051: SQLParsingError,
    1054: SQLParsingError,
    1064: SQLParsingError,
    1146: SQLParsingError,
    1305: SQLParsingError,
    1061: OperationalError,
    1091: OperationalError,
}


def driver_profile() -> DriverParameterProfile:
    """Build the MySQL driver parameter profile using `ParameterStyle.QMARK` (`?`) execution."""
    decimal_converter = build_decimal_converter(mode="string")
    coercions: dict[type, Callable[[Any], Any]] = {
        bool: int,
        **build_uuid_coercions(native=False),
        type(None): lambda value: value,
    }
    return DriverParameterProfile(
        name="vertebra_mysql",
        default_style=ParameterStyle.QMARK,
        supported_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.POSITIONAL_PYFORMAT,
        },
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=False,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=coercions,
        default_dialect="mysql",
        extras={"json_serializer": to_json, "json_deserializer": from_json, "decimal_converter": decimal_converter},
    )


def build_statement_config(
    *,
    json_serializer: Callable[[Any], str] | None = None,
    json_deserializer: Callable[[str], Any] | None = None,
) -> StatementConfig:
    """Build a MySQL `StatementConfig` targeting `ParameterStyle.QMARK` `?` placeholders."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    return build_statement_config_from_profile(
        driver_profile(),
        statement_overrides={"dialect": "mysql"},
        json_serializer=serializer,
        json_deserializer=deserializer,
    )


def default_statement_config() -> StatementConfig:
    """Return the default `ParameterStyle.QMARK` MySQL statement configuration."""
    return build_statement_config()


def _coerce_param(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


def normalize_mysql_parameters(params: Any) -> Any:
    """Normalize compiled positional MySQL bind parameters into a coerced tuple."""
    if params is None:
        return None
    unwrapped = unwrap_typed_parameters(params)
    if isinstance(unwrapped, Mapping):
        return {str(k): _coerce_param(v) for k, v in unwrapped.items()}
    if isinstance(unwrapped, Sequence) and not isinstance(unwrapped, (str, bytes, bytearray)):
        return tuple(_coerce_param(item) for item in unwrapped)
    return (_coerce_param(unwrapped),)


def normalize_mysql_many_parameters(params: Any) -> list[Any]:
    """Normalize batch parameter sets for `execute_many` execution."""
    unwrapped = unwrap_typed_parameters(params)
    if unwrapped is None:
        return []
    if not isinstance(unwrapped, Sequence) or isinstance(unwrapped, (str, bytes, bytearray)):
        return [normalize_mysql_parameters(unwrapped)]
    return [normalize_mysql_parameters(item) for item in unwrapped]


def _extract_errno_and_sqlstate(exc: BaseException) -> tuple[int | None, str | None]:
    errno: int | None = None
    sqlstate: str | None = None
    raw_errno = getattr(exc, "vendor_code", None)
    if raw_errno is None:
        raw_errno = getattr(exc, "errno", None)
    if isinstance(raw_errno, int):
        errno = raw_errno
    args = getattr(exc, "args", ())
    if errno is None and args and isinstance(args[0], int):
        errno = int(args[0])
    raw_state = getattr(exc, "sqlstate", None)
    if isinstance(raw_state, str) and raw_state:
        sqlstate = raw_state
    if errno is None:
        match = _SERVER_ERR_RE.search(str(exc))
        if match is not None:
            errno = int(match.group(1))
            if sqlstate is None:
                sqlstate = match.group(2)
    return errno, sqlstate


def map_mysql_exception(exc: BaseException) -> Exception | None:
    """Map native MySQL or MariaDB errors onto `sqlspec.exceptions.*` preserving causes."""
    if isinstance(exc, SQLSpecError):
        return exc
    errno, sqlstate = _extract_errno_and_sqlstate(exc)
    msg = str(exc)

    target: type[SQLSpecError] | None = None
    if errno is not None:
        target = _ERRNO_MAP.get(errno)
        if target is None and 1064 <= errno < 1100:
            target = SQLParsingError

    if target is None and sqlstate:
        if sqlstate == "23505":
            target = UniqueViolationError
        elif sqlstate == "23503":
            target = ForeignKeyViolationError
        elif sqlstate == "23502":
            target = NotNullViolationError
        elif sqlstate == "23514":
            target = CheckViolationError
        elif sqlstate == "40001":
            target = DeadlockError
        elif sqlstate.startswith("23"):
            target = IntegrityError
        elif sqlstate.startswith("28"):
            target = PermissionDeniedError
        elif sqlstate.startswith("40"):
            target = TransactionError
        elif sqlstate.startswith("42"):
            target = SQLParsingError
        elif sqlstate.startswith("08"):
            target = DatabaseConnectionError
        elif sqlstate.startswith("22"):
            target = DataError

    if target is None:
        cls_name = type(exc).__name__
        if cls_name == "IntegrityError":
            target = IntegrityError
        elif cls_name in {"SerializationFailure", "DeadlockError"}:
            target = DeadlockError
        elif cls_name == "QueryCancelledError":
            target = OperationCancelledError
        elif cls_name == "ConnectionFailure":
            target = DatabaseConnectionError
        elif cls_name == "ProgrammingError":
            target = SQLParsingError
        elif cls_name == "DataError":
            target = DataError
        elif cls_name == "OperationalError":
            target = OperationalError

    if target is None:
        return None
    mapped = target(f"MySQL error [{sqlstate or errno}]: {msg}" if (sqlstate or errno) else msg)
    mapped.__cause__ = exc
    return mapped


def is_mysql_table_missing(exc: BaseException) -> bool:
    """Return whether an exception represents a missing MySQL or MariaDB table (`1146` / `42S02`)."""
    errno, sqlstate = _extract_errno_and_sqlstate(exc)
    if errno == MYSQL_TABLE_NOT_FOUND_ERRNO or sqlstate == "42S02":
        return True
    lower = str(exc).lower()
    return "doesn't exist" in lower or "does not exist" in lower or "unknown table" in lower


def mysql_supports_native_vector(raw_version: str | None) -> bool:
    """Return whether a MySQL/MariaDB version string supports native `VECTOR` columns and distance functions."""
    if not raw_version:
        return False
    parsed = parse_mysql_engine_version(raw_version)
    if parsed is None:
        return False
    if parsed.engine_family == "mariadb":
        return (parsed.version.major, parsed.version.minor) >= (11, 7)
    return parsed.version.major >= 9


def mysql_json_for_storage(value: Any, *, serializer: Any = to_json) -> str:
    """Serialize a Python mapping or JSON string cleanly for MySQL `JSON` columns."""
    if isinstance(value, str):
        return value
    return str(serializer(value))


def mysql_json_dict(value: Any, *, deserializer: Any = from_json) -> dict[str, Any]:
    """Decode a MySQL `JSON` column payload into a dictionary."""
    if value is None:
        return {}
    if isinstance(value, (bytearray, memoryview)):
        value = bytes(value)
    if isinstance(value, (bytes, str)):
        decoded = deserializer(value)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def mysql_owner_id_column_parts(column_ddl: str) -> tuple[str, str]:
    """Split an `owner_id_column` specification into `(column_def, foreign_key_clause)`."""
    match = _REFERENCES_RE.search(column_ddl)
    if match is None:
        return column_ddl.strip(), ""
    col_def = column_ddl[: match.start()].strip()
    fk_clause = match.group(1).strip()
    col_name = col_def.split()[0]
    return col_def, f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"


def mysql_table_options(config_section: Mapping[str, Any], key: str = "table_options") -> str:
    """Extract and validate raw MySQL DDL table options from extension configuration."""
    value = config_section.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        msg = f"MySQL extension config '{key}' must be a string"
        raise ImproperConfigurationError(msg)
    cleaned = value.strip()
    return f" {cleaned}" if cleaned else ""


def mysql_index_options(config_section: Mapping[str, Any], key: str = "index_options") -> str:
    """Extract and validate raw MySQL inline index options from extension configuration."""
    value = config_section.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        msg = f"MySQL extension config '{key}' must be a string"
        raise ImproperConfigurationError(msg)
    cleaned = value.strip()
    return f" {cleaned}" if cleaned else ""


def build_mysql_scope_where(
    app_name: str,
    user_id: str,
    scope_filter: Literal["all", "user", "app"],
) -> tuple[str, tuple[Any, ...]]:
    """Build positional `?` WHERE clause and parameters for MySQL ADK memory scope filtering."""
    if scope_filter == "all":
        return "app_name = ? AND ((scope = 'user' AND user_id = ?) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = ? AND scope = 'user' AND user_id = ?", (app_name, user_id)
    return "app_name = ? AND scope = 'app'", (app_name,)


def format_mysql_vector_literal(embedding: Sequence[float] | None) -> str | None:
    """Format a numeric embedding vector into a MySQL `STRING_TO_VECTOR(?)` JSON array literal."""
    if embedding is None:
        return None
    return to_json([float(x) for x in embedding])


def parse_mysql_vector(value: Any) -> list[float] | None:
    """Decode a MySQL JSON/string vector column value into a Python float list."""
    if value is None:
        return None
    if isinstance(value, (bytearray, memoryview)):
        value = bytes(value)
    if isinstance(value, (bytes, str)):
        raw = value.decode("utf-8") if isinstance(value, bytes) else value
        text = raw.strip()
        if not text:
            return None
        if text.startswith("["):
            decoded = from_json(text)
            if isinstance(decoded, list):
                return [float(x) for x in decoded]
        return None
    if isinstance(value, Sequence):
        return [float(x) for x in value]
    return None


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


def build_connection_config(
    connection_config: Mapping[str, Any] | None = None,
    driver_features: Mapping[str, Any] | None = None,
    broker: Any = None,
    overrides: Mapping[str, Any] | None = None,
) -> MySQLConfig:
    """Build an immutable `MySQLConfig` from SQLSpec `connection_config` and keyword overrides."""
    merged: dict[str, Any] = {}
    if isinstance(connection_config, Mapping):
        merged.update(connection_config)
    if isinstance(overrides, Mapping):
        merged.update(overrides)

    url = merged.pop("url", None) or merged.pop("dsn", None)
    if "cloud_sql_instance" not in merged and isinstance(driver_features, Mapping):
        if driver_features.get("cloud_sql_instance"):
            merged["cloud_sql_instance"] = driver_features["cloud_sql_instance"]
        if "enable_iam_auth" in driver_features and "enable_iam_auth" not in merged:
            merged["enable_iam_auth"] = bool(driver_features["enable_iam_auth"])

    if "connect_timeout" not in merged and "timeout" in merged:
        merged["connect_timeout"] = merged.pop("timeout")
    else:
        merged.pop("timeout", None)

    for ignored_key in (
        "pool_recycle_seconds",
        "health_check_interval",
        "connection_validation_sql",
        "driver_features",
    ):
        merged.pop(ignored_key, None)

    allowed_fields = set(MySQLConfig.__struct_fields__)
    cfg_kwargs = {k: v for k, v in merged.items() if k in allowed_fields}
    if broker is not None and "broker" not in cfg_kwargs:
        cfg_kwargs["broker"] = broker

    if isinstance(url, str) and url.strip():
        return MySQLConfig.from_url(url.strip(), **cfg_kwargs)
    return MySQLConfig(**cfg_kwargs)
