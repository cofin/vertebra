"""DuckDB SQLSpec core statement configuration, parameter, COPY, and error mapping helpers."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final
from uuid import UUID

from sqlglot import exp, parse
from sqlglot.errors import ParseError
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    TypedParameter,
    build_statement_config_from_profile,
)
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_decimal_converter, build_uuid_coercions, time_iso_convert

from vertebra.db.adapters.duckdb.config import DuckDBConfig
from vertebra.typing import ImproperConfigurationError

__all__ = (
    "ARROW_MIN_ROWS",
    "DUCKDB_COPY_FILENAME_FOLLOWS_QUERY",
    "DUCKDB_TABLE_NOT_FOUND_ERROR",
    "build_connection_config",
    "build_duckdb_copy_sql",
    "build_duckdb_scope_where",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "cosine_similarity",
    "default_statement_config",
    "description_has_arrow_divergent_types",
    "driver_profile",
    "duckdb_copy_filename_follows_query",
    "duckdb_json_dict",
    "duckdb_row_value",
    "format_duckdb_identifier",
    "is_duckdb_table_missing",
    "map_duckdb_exception",
    "normalize_duckdb_many_parameters",
    "normalize_duckdb_parameters",
    "order_duckdb_copy_parameters",
    "split_schema_and_table",
    "unwrap_typed_parameters",
)

ARROW_MIN_ROWS: Final[int] = 64
DUCKDB_COPY_FILENAME_FOLLOWS_QUERY: Final[bool] = True
DUCKDB_TABLE_NOT_FOUND_ERROR: Final[str] = "does not exist"

_FIXED_ARRAY_PATTERN: Final[re.Pattern[str]] = re.compile(r"\[\d+\]")
_MEMORY_SUFFIX_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*$")
_MEMORY_UNIT_DIVISORS_MB: Final[dict[str, float]] = {
    "": 1.0 / (1024 * 1024),
    "B": 1.0 / (1024 * 1024),
    "K": 1.0 / 1024,
    "KB": 1000.0 / (1024 * 1024),
    "KIB": 1.0 / 1024,
    "M": 1.0,
    "MB": 1000.0 * 1000.0 / (1024 * 1024),
    "MIB": 1.0,
    "G": 1024.0,
    "GB": 1000.0 * 1000.0 * 1000.0 / (1024 * 1024),
    "GIB": 1024.0,
    "T": 1024.0 * 1024.0,
    "TB": 1000.0 * 1000.0 * 1000.0 * 1000.0 / (1024 * 1024),
    "TIB": 1024.0 * 1024.0,
}

_ARROW_SAFE_SCALAR_TYPES: Final[frozenset[str]] = frozenset(
    {
        "INTEGER",
        "BIGINT",
        "VARCHAR",
        "DOUBLE",
        "BOOLEAN",
        "DATE",
        "TIMESTAMP",
        "SMALLINT",
        "TINYINT",
        "UBIGINT",
        "UINTEGER",
        "USMALLINT",
        "UTINYINT",
        "FLOAT",
        "BLOB",
        "UUID",
        "TIME",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMPTZ",
        "TIMESTAMP_S",
        "TIMESTAMP_MS",
        "JSON",
    }
)

_TIME_TO_ISO = time_iso_convert
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def _identity(value: Any) -> Any:
    return value


def build_profile() -> DriverParameterProfile:
    """Create the DuckDB `DriverParameterProfile` supporting `?` and `$1` placeholders."""
    return DriverParameterProfile(
        name="DuckDB",
        default_style=ParameterStyle.QMARK,
        supported_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NAMED_AT,
        },
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            bool: _identity,
            datetime: _TIME_TO_ISO,
            date: _TIME_TO_ISO,
            Decimal: _DECIMAL_TO_STRING,
            **build_uuid_coercions(),
        },
        default_dialect="duckdb",
    )


driver_profile: DriverParameterProfile = build_profile()


def build_statement_config(*, json_serializer: Callable[[Any], str] | None = None) -> StatementConfig:
    """Construct a DuckDB `StatementConfig` targeting SQLGlot dialect `'duckdb'`."""
    serializer = json_serializer or to_json
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "duckdb"},
        json_serializer=serializer,
    )


def default_statement_config() -> StatementConfig:
    """Return a fresh default DuckDB `StatementConfig`."""
    return build_statement_config()


def duckdb_copy_filename_follows_query() -> bool:
    """Return whether DuckDB expects query parameters before destination in `COPY (...) TO ?`."""
    return DUCKDB_COPY_FILENAME_FOLLOWS_QUERY


def build_duckdb_copy_sql(sql: str, file_format: str = "parquet") -> str | None:
    """Wrap one DuckDB query into `COPY (<query>) TO ? (FORMAT ...)`."""
    normalized = file_format.lower()
    if normalized not in {"parquet", "csv"}:
        return None
    try:
        statements = parse(sql, read="duckdb")
    except ParseError:
        return None
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        return None
    options = [exp.CopyParameter(this=exp.Var(this="FORMAT"), expression=exp.Var(this=normalized.upper()))]
    if normalized == "csv":
        options.append(exp.CopyParameter(this=exp.Var(this="HEADER"), expression=exp.Boolean(this=True)))
    return exp.Copy(
        this=exp.Subquery(this=statements[0]),
        kind=False,
        files=[exp.Placeholder()],
        params=options,
    ).sql(dialect="duckdb", copy=False)


def order_duckdb_copy_parameters(
    query_parameters: Sequence[Any] | None,
    destination: str,
    *,
    filename_follows_query: bool | None = None,
) -> tuple[Any, ...]:
    """Order bound parameters for `COPY (<query>) TO ?` using DuckDB 2.0+ lexical order by default."""
    query_seq = tuple(query_parameters or ())
    follows = DUCKDB_COPY_FILENAME_FOLLOWS_QUERY if filename_follows_query is None else bool(filename_follows_query)
    if follows:
        return (*query_seq, destination)
    return (destination, *query_seq)


def _type_str_is_arrow_divergent(type_str: str) -> bool:
    if (
        "HUGEINT" in type_str
        or "UHUGEINT" in type_str
        or "TIMETZ" in type_str
        or "TIME WITH TIME ZONE" in type_str
        or "TIMESTAMP_NS" in type_str
        or "INTERVAL" in type_str
        or "MAP(" in type_str
        or "BIGNUM" in type_str
        or "VARINT" in type_str
    ):
        return True
    if type_str == "BIT" or "BIT[" in type_str or " BIT" in type_str or type_str.startswith("BIT "):
        return True
    if "UUID" in type_str and type_str != "UUID":
        return True
    return "[" in type_str and _FIXED_ARRAY_PATTERN.search(type_str) is not None


def description_has_arrow_divergent_types(description: Sequence[Any] | None) -> bool:
    """Return whether `description` includes column types where DuckDB Arrow output diverges from `fetchall()`."""
    if not description:
        return False
    for column in description:
        if isinstance(column, str) or not isinstance(column, (tuple, list)):
            continue
        if len(column) <= 1 or column[1] is None:
            continue
        type_str = str(column[1]).upper()
        if type_str in _ARROW_SAFE_SCALAR_TYPES:
            continue
        if _type_str_is_arrow_divergent(type_str):
            return True
    return False


def collect_rows(fetched_data: list[Any] | None, description: Sequence[Any] | None) -> tuple[list[Any], list[str]]:
    """Collect DuckDB rows and column names from cursor description metadata."""
    if not description:
        return [], []
    column_names = [str(col[0]) if isinstance(col, (tuple, list)) else str(col) for col in description]
    if not fetched_data:
        return [], column_names
    return list(fetched_data), column_names


def _coerce_memory_limit_mb(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return max(1, value)
    match = _MEMORY_SUFFIX_PATTERN.match(str(value))
    if match is None:
        return None
    amount = float(match.group(1))
    unit = match.group(2).upper()
    divisor = _MEMORY_UNIT_DIVISORS_MB.get(unit)
    if divisor is None:
        return None
    return max(1, int(amount * divisor))


def build_connection_config(
    *,
    connection_config: Mapping[str, Any] | None = None,
    driver_features: Mapping[str, Any] | None = None,
    broker: Any = None,
    overrides: Mapping[str, Any] | None = None,
) -> DuckDBConfig:
    """Translate SQLSpec connection settings into a Vertebra `DuckDBConfig` preserving `:memory:<name>`."""
    _ = driver_features
    raw: dict[str, Any] = dict(connection_config or {})
    if overrides:
        raw.update({k: v for k, v in overrides.items() if v is not None})

    dsn = raw.pop("dsn", None) or raw.pop("url", None)
    database = raw.pop("database", None) or raw.pop("db", None) or raw.pop("path", None) or raw.pop("file", None)
    memory_id = raw.pop("memory_id", None)
    if memory_id and database is None:
        database = f":memory:{memory_id}"

    duckdb_settings: dict[str, str] = {}
    nested_cfg = raw.pop("config", None)
    if isinstance(nested_cfg, Mapping):
        for k, v in nested_cfg.items():
            if v is not None:
                duckdb_settings[str(k)] = str(v)
    extra = raw.pop("extra", None)
    if isinstance(extra, Mapping):
        for k, v in extra.items():
            if v is not None:
                duckdb_settings[str(k)] = str(v)

    memory_limit_mb = raw.pop("memory_limit_mb", None)
    if memory_limit_mb is None and "memory_limit" in raw:
        memory_limit_mb = _coerce_memory_limit_mb(raw.pop("memory_limit"))
    elif "memory_limit" in duckdb_settings and memory_limit_mb is None:
        memory_limit_mb = _coerce_memory_limit_mb(duckdb_settings.pop("memory_limit"))
    else:
        raw.pop("memory_limit", None)

    threads = raw.pop("threads", None)
    if threads is None and "threads" in duckdb_settings:
        threads = int(duckdb_settings.pop("threads"))

    temp_directory = raw.pop("temp_directory", None)
    if temp_directory is None and "temp_directory" in duckdb_settings:
        temp_directory = duckdb_settings.pop("temp_directory")

    init_stmts = raw.pop("init_statements", ())
    extensions = raw.pop("extensions", ())
    secrets = raw.pop("secrets", ())

    kwargs: dict[str, Any] = {
        "read_only": bool(raw.pop("read_only", False)),
        "pool_min_size": int(raw.pop("pool_min_size", 1)),
        "pool_max_size": int(raw.pop("pool_max_size", 10)),
        "statement_cache_size": int(raw.pop("statement_cache_size", 256)),
        "parquet_metadata_cache": bool(raw.pop("parquet_metadata_cache", True)),
        "httpfs_connection_caching": bool(raw.pop("httpfs_connection_caching", True)),
        "enable_http_metadata_cache": bool(raw.pop("enable_http_metadata_cache", True)),
        "config": duckdb_settings,
        "extensions": tuple(str(ext) for ext in extensions) if extensions else (),
        "init_statements": tuple(str(stmt) for stmt in init_stmts) if init_stmts else (),
        "secrets": tuple(secrets) if secrets else (),
        "broker": raw.pop("broker", broker),
    }
    if memory_limit_mb is not None:
        kwargs["memory_limit_mb"] = int(memory_limit_mb)
    if threads is not None:
        kwargs["threads"] = int(threads)
    if temp_directory is not None:
        kwargs["temp_directory"] = str(temp_directory)

    raw.pop("pool_timeout", None)
    raw.pop("pool_recycle_seconds", None)
    raw.pop("health_check_interval", None)
    raw.pop("memory_ceiling_fraction", None)
    raw.pop("reject_tmpfs_temp_directory", None)

    for key, val in raw.items():
        if val is not None:
            duckdb_settings[str(key)] = str(val)

    if dsn is not None:
        cfg = DuckDBConfig.from_url(str(dsn), **kwargs)
        if database is not None:
            return DuckDBConfig(
                path=str(database),
                database=str(database),
                **{k: v for k, v in kwargs.items() if k not in {"path", "database"}},
            )
        return cfg

    resolved_db = str(database) if database is not None else ":memory:"
    return DuckDBConfig(path=resolved_db, database=resolved_db, **kwargs)


def normalize_duckdb_scalar(value: Any) -> Any:
    """Normalize rich Python scalar types for DuckDB parameter binding."""
    if isinstance(value, UUID):
        return str(value)
    return value


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


def normalize_duckdb_parameters(params: Any) -> Any:
    """Unwrap `TypedParameter` markers and normalize bind shapes for DuckDB execution."""
    if params is None:
        return None
    unwrapped = unwrap_typed_parameters(params)
    if isinstance(unwrapped, Mapping):
        return {str(k): normalize_duckdb_scalar(v) for k, v in unwrapped.items()} if unwrapped else None
    if isinstance(unwrapped, list):
        return tuple(normalize_duckdb_scalar(v) for v in unwrapped) if unwrapped else None
    if isinstance(unwrapped, tuple):
        return tuple(normalize_duckdb_scalar(v) for v in unwrapped) if unwrapped else None
    return normalize_duckdb_scalar(unwrapped)


def normalize_duckdb_many_parameters(prepared: Any) -> list[tuple[Any, ...]]:
    """Normalize `execute_many` parameter batches into `list[tuple[Any, ...]]`."""
    if not prepared:
        return []
    unwrapped = unwrap_typed_parameters(prepared)
    rows: list[tuple[Any, ...]] = []
    for item in unwrapped:
        if isinstance(item, (tuple, list)):
            rows.append(tuple(normalize_duckdb_scalar(v) for v in item))
        else:
            rows.append((normalize_duckdb_scalar(item),))
    return rows


def map_duckdb_exception(exc: BaseException) -> Exception | None:
    """Map native DuckDB and Vertebra errors onto the SQLSpec exception hierarchy."""
    if isinstance(exc, SQLSpecError):
        return exc
    exc_name = type(exc).__name__.lower()
    msg = str(exc)
    lower = msg.lower()

    if "constraint" in exc_name or "constraint error" in lower or "violates" in lower:
        if "unique" in lower or "duplicate" in lower or "primary key" in lower:
            mapped: SQLSpecError = UniqueViolationError(msg)
        elif "foreign key" in lower:
            mapped = ForeignKeyViolationError(msg)
        elif "not null" in lower or "null value" in lower:
            mapped = NotNullViolationError(msg)
        elif "check" in lower:
            mapped = CheckViolationError(msg)
        else:
            mapped = IntegrityError(msg)
        mapped.__cause__ = exc
        return mapped

    if "catalog error" in lower or "catalogexception" in exc_name or DUCKDB_TABLE_NOT_FOUND_ERROR in lower:
        mapped = NotFoundError(msg) if DUCKDB_TABLE_NOT_FOUND_ERROR in lower else SQLParsingError(msg)
        mapped.__cause__ = exc
        return mapped

    if (
        "parser error" in lower
        or "binder error" in lower
        or "syntax error" in lower
        or "parserexception" in exc_name
        or "binderexception" in exc_name
    ):
        mapped = SQLParsingError(msg)
        mapped.__cause__ = exc
        return mapped

    if "permission" in lower or "access denied" in lower or "permissionexception" in exc_name:
        mapped = PermissionDeniedError(msg)
        mapped.__cause__ = exc
        return mapped

    if "interrupt" in lower or "cancel" in lower or "interruptexception" in exc_name:
        mapped = OperationCancelledError(msg)
        mapped.__cause__ = exc
        return mapped

    if "transaction" in exc_name or "conflict" in lower or "serialization" in lower:
        if "conflict" in lower or "serialization" in lower:
            mapped = SerializationConflictError(msg)
        else:
            mapped = OperationalError(msg)
        mapped.__cause__ = exc
        return mapped

    if "conversion" in lower or "out of range" in lower or "type mismatch" in lower or "invalid input" in lower:
        mapped = DataError(msg)
        mapped.__cause__ = exc
        return mapped

    if "connection" in lower or "connectionexception" in exc_name:
        mapped = DatabaseConnectionError(msg)
        mapped.__cause__ = exc
        return mapped

    if "io error" in lower or "out of memory" in lower or "operational" in lower:
        mapped = OperationalError(msg)
        mapped.__cause__ = exc
        return mapped

    return None


def is_duckdb_table_missing(exc: BaseException) -> bool:
    """Return True when `exc` indicates a missing DuckDB table or catalog object."""
    msg = str(exc).lower()
    return DUCKDB_TABLE_NOT_FOUND_ERROR in msg or "catalog error" in msg


def format_duckdb_identifier(identifier: str) -> str:
    """Quote a DuckDB table or schema-qualified identifier safely with double quotes."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise ImproperConfigurationError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"')
    return ".".join(quote_identifier(part) for part in parts)


def split_schema_and_table(table_name: str, *, default_schema: str = "main") -> tuple[str, str]:
    """Split an optionally qualified DuckDB table name into `(schema, bare_table)`."""
    parts = split_qualified_identifier(table_name.strip(), quote_chars='"')
    if len(parts) == 1:
        return default_schema, parts[0]
    if len(parts) > 1:
        return parts[-2], parts[-1]
    msg = "DuckDB table name must not be empty."
    raise ValueError(msg)


def duckdb_row_value(row: Any, key: str, index: int = 0) -> Any:
    """Extract a column from a mapping or tuple/sequence DuckDB row."""
    if isinstance(row, Mapping):
        if key in row:
            return row[key]
        key_lower = key.lower()
        for k, v in row.items():
            if str(k).lower() == key_lower:
                return v
        return None
    if isinstance(row, (tuple, list)):
        return row[index] if index < len(row) else None
    return getattr(row, key, None)


def duckdb_json_dict(raw: Any, *, deserializer: Callable[[Any], Any] = from_json) -> dict[str, Any]:
    """Decode a DuckDB `JSON` column payload into a dictionary."""
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray, memoryview)):
        raw = bytes(raw).decode("utf-8")
    if isinstance(raw, str):
        parsed = deserializer(raw) if raw else {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def build_duckdb_scope_where(
    app_name: str,
    user_id: str,
    scope_filter: str,
    *,
    prefix: str = "",
) -> tuple[str, tuple[Any, ...]]:
    """Build DuckDB `WHERE` clause and positional `?` binds for ADK memory scope filters."""
    col = f"{prefix}." if prefix else ""
    if scope_filter == "user":
        return f"{col}app_name = ? AND {col}scope = 'user' AND {col}user_id = ?", (app_name, user_id)
    if scope_filter == "app":
        return f"{col}app_name = ? AND {col}scope = 'app'", (app_name,)
    return (
        f"{col}app_name = ? AND (({col}scope = 'user' AND {col}user_id = ?) OR {col}scope = 'app')",
        (app_name, user_id),
    )


def cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    """Compute cosine similarity between two equal-length vectors cleanly."""
    if len(vec_a) != len(vec_b) or not vec_a:
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b, strict=True))
    norm_a = math.sqrt(sum(float(a) * float(a) for a in vec_a))
    norm_b = math.sqrt(sum(float(b) * float(b) for b in vec_b))
    denom = norm_a * norm_b
    return dot / denom if denom > 0.0 else 0.0
