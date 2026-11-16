"""PostgreSQL SQLSpec core helpers, exception mapping, compilation, and DDL tuning utilities."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.core.parameters import TypedParameter
from sqlspec.exceptions import (
    CheckViolationError,
    DeadlockError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SerializationConflictError,
    SQLParsingError,
    UniqueViolationError,
)

from vertebra.extensions.sqlspec._driver import (
    _normalize_bound_params,
    map_native_database_exception,
    native_statement_config,
    unwrap_typed_parameters,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "POSTGRES_JSON_FALLBACK",
    "build_postgres_statement_config",
    "compile_postgres_numeric_transport",
    "escape_pg_literal",
    "is_pg_table_missing",
    "is_pg_unique_violation",
    "map_postgres_exception",
    "normalize_pg_many_parameters",
    "normalize_pg_parameters",
    "pg_ensure_utc",
    "pg_identifier",
    "pg_index_name",
    "pg_json_dumps",
    "pg_json_loads",
    "pg_row_value",
    "pg_table_ref",
    "pg_vector_literal",
    "postgres_storage_params_sql",
    "validate_pg_table_name",
)

POSTGRES_JSON_FALLBACK: Final[str] = "JSONB"

_PG_IDENT_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_PG_STORAGE_INT_OPTIONS: Final[tuple[tuple[str, str, int, int], ...]] = (("fillfactor", "fillfactor", 10, 100),)
_PG_STORAGE_FLOAT_OPTIONS: Final[tuple[tuple[str, str, float, float], ...]] = (
    ("autovacuum_vacuum_scale_factor", "autovacuum_vacuum_scale_factor", 0.0, 1.0),
    ("autovacuum_analyze_scale_factor", "autovacuum_analyze_scale_factor", 0.0, 1.0),
)


def pg_identifier(name: str, label: str = "identifier") -> str:
    """Validate and double-quote a single PostgreSQL identifier."""
    candidate = name.strip()
    if not candidate or not _PG_IDENT_RE.fullmatch(candidate):
        msg = f"Invalid PostgreSQL {label}: {name!r}"
        raise ImproperConfigurationError(msg)
    escaped = candidate.replace('"', '""')
    return f'"{escaped}"'


def validate_pg_table_name(table_name: str, label: str = "table name") -> tuple[str, ...]:
    """Validate an unqualified or schema-qualified PostgreSQL table name."""
    parts = [part.strip() for part in table_name.split(".")]
    if not parts or len(parts) > 2 or any(not part for part in parts):
        msg = f"Invalid PostgreSQL {label}: {table_name!r}"
        raise ImproperConfigurationError(msg)
    for part in parts:
        pg_identifier(part, label)
    return tuple(parts)


def pg_table_ref(table_name: str, label: str = "table name") -> str:
    """Format a validated PostgreSQL table identifier as `\"schema\".\"table\"` or `\"table\"`."""
    parts = validate_pg_table_name(table_name, label)
    return ".".join(pg_identifier(part, label) for part in parts)


def pg_index_name(prefix: str, table_name: str, suffix: str) -> str:
    """Build a bounded, valid PostgreSQL index name from table parts."""
    parts = validate_pg_table_name(table_name)
    bare = parts[-1].lower()
    raw = f"{prefix}_{bare}_{suffix}"
    clean = re.sub(r"[^A-Za-z0-9_]", "_", raw)
    return clean[:63]


def escape_pg_literal(value: str) -> str:
    """Escape single quotes inside a PostgreSQL string literal."""
    return value.replace("'", "''")


def build_postgres_statement_config(statement_config: StatementConfig | None = None) -> StatementConfig:
    """Construct the canonical PostgreSQL `StatementConfig` compiling to `$1..$N` numeric binds."""
    if statement_config is not None:
        return statement_config
    config = native_statement_config(
        "postgres",
        ParameterStyle.NUMERIC,
        default_style=ParameterStyle.NUMERIC,
        supported_styles=(
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.QMARK,
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
        ),
        supported_execution_styles=(ParameterStyle.NUMERIC,),
        preserve_parameter_format=False,
    )
    param_cfg: ParameterStyleConfig = config.parameter_config
    return config.replace(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=param_cfg.default_parameter_style,
            supported_parameter_styles=param_cfg.supported_parameter_styles,
            default_execution_parameter_style=ParameterStyle.NUMERIC,
            supported_execution_parameter_styles={ParameterStyle.NUMERIC},
            type_coercion_map=param_cfg.type_coercion_map,
            preserve_parameter_format=False,
            allow_mixed_parameter_styles=True,
        )
    )


def _unwrap_scalar(value: Any) -> Any:
    """Unwrap a single `TypedParameter` wrapper when encountered."""
    if isinstance(value, TypedParameter):
        return value.value
    return value


def normalize_pg_parameters(params: Any) -> Any:
    """Normalize PostgreSQL parameters into a tuple, mapping, or `None` with unwrapped `TypedParameter`s."""
    if params is None:
        return None
    unwrapped = unwrap_typed_parameters(params)
    if unwrapped is None:
        return None
    if isinstance(unwrapped, list):
        return tuple(_unwrap_scalar(item) for item in unwrapped)
    if isinstance(unwrapped, tuple):
        return tuple(_unwrap_scalar(item) for item in unwrapped)
    if isinstance(unwrapped, Mapping):
        return {key: _unwrap_scalar(val) for key, val in unwrapped.items()}
    return (_unwrap_scalar(unwrapped),)


def normalize_pg_many_parameters(params: Any) -> list[Any]:
    """Normalize batch parameter sequences for `execute_many` over PostgreSQL `$1..$N` binds."""
    if params is None:
        return []
    if isinstance(params, Sequence) and not isinstance(params, (str, bytes, bytearray)):
        normalized: list[Any] = []
        for entry in params:
            unwrapped = unwrap_typed_parameters(entry)
            if isinstance(unwrapped, list):
                normalized.append(tuple(unwrapped))
            elif isinstance(unwrapped, (tuple, Mapping)):
                normalized.append(unwrapped)
            else:
                normalized.append((unwrapped,))
        return normalized
    return []


def compile_postgres_numeric_transport(
    driver: Any,
    statement: Any,
    parameters: tuple[Any, ...] | list[Any] = (),
    statement_config: StatementConfig | None = None,
    kwargs: dict[str, Any] | None = None,
) -> tuple[Any, str, Any]:
    """Prepare and compile a SQLSpec statement to PostgreSQL `$N` binds preserving filters and transformers."""
    explicit_cfg = statement_config or getattr(statement, "statement_config", None) or driver.statement_config
    prepared = driver.prepare_statement(
        statement,
        parameters,
        statement_config=explicit_cfg,
        kwargs=kwargs,
    )
    compiled_sql, bound = prepared.compile()
    normalized = normalize_pg_parameters(_normalize_bound_params(bound))
    return prepared, compiled_sql, normalized


def map_postgres_exception(exc: BaseException) -> Exception | None:
    """Map native PostgreSQL SQLSTATE codes and errors onto Vertebra `DatabaseError` subclasses."""
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    message = str(exc)
    if sqlstate == "23505":
        return UniqueViolationError(message)
    if sqlstate == "23503":
        return ForeignKeyViolationError(message)
    if sqlstate == "23502":
        return NotNullViolationError(message)
    if sqlstate == "23514":
        return CheckViolationError(message)
    if sqlstate == "40P01":
        return DeadlockError(message)
    if sqlstate == "40001":
        return SerializationConflictError(message)
    if sqlstate in {"42P01", "42703", "42601", "3F000"}:
        return SQLParsingError(message)
    return map_native_database_exception(exc)


def is_pg_table_missing(exc: BaseException) -> bool:
    """Return True when an exception indicates a missing PostgreSQL relation (`42P01`)."""
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if sqlstate == "42P01":
        return True
    lowered = str(exc).lower()
    return "does not exist" in lowered and ("relation" in lowered or "table" in lowered)


def is_pg_unique_violation(exc: BaseException) -> bool:
    """Return True when an exception represents a PostgreSQL unique constraint violation (`23505`)."""
    if isinstance(exc, UniqueViolationError):
        return True
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    return sqlstate == "23505"


def pg_row_value(row: Any, key: str, index: int = 0) -> Any:
    """Read a column value by case-insensitive mapping key or positional tuple index."""
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


def pg_ensure_utc(value: Any) -> datetime:
    """Normalize a database timestamp or ISO string into a timezone-aware UTC `datetime`."""
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


def pg_json_dumps(value: Any, *, serializer: Any = None) -> str:
    """Serialize Python JSON payloads into UTF-8 text via msgspec or a configured codec."""
    if serializer is not None:
        encoded = serializer(value)
        return encoded.decode("utf-8") if isinstance(encoded, (bytes, bytearray)) else str(encoded)
    encoded = encode_json(value)
    return encoded.decode("utf-8") if isinstance(encoded, (bytes, bytearray)) else str(encoded)


def pg_json_loads(value: Any, *, deserializer: Any = None) -> Any:
    """Decode JSONB column text, bytes, memoryviews, or pre-decoded Python objects."""
    if value is None:
        return None
    if isinstance(value, (dict, list, bool, int, float)):
        return value
    raw = bytes(value).decode("utf-8") if isinstance(value, (bytes, bytearray, memoryview)) else str(value)
    if deserializer is not None:
        return deserializer(raw)
    return decode_json(raw)


def pg_vector_literal(embedding: Sequence[float]) -> str:
    """Format a dense float sequence as a PostgreSQL pgvector text literal (`[v1,v2,...]`)."""
    coords = ",".join(repr(float(value)) for value in embedding)
    return f"[{coords}]"


def postgres_storage_params_sql(
    settings: Mapping[str, Any] | None,
    *,
    default_fillfactor: int | None = None,
    default_vacuum_scale: float | None = None,
    default_analyze_scale: float | None = None,
    include_autovacuum: bool = False,
) -> str:
    """Build a PostgreSQL `WITH (...)` table storage parameter clause from extension config."""
    source = settings if isinstance(settings, Mapping) else {}
    clauses: list[str] = []
    defaults_int = {"fillfactor": default_fillfactor}
    for key, sql_key, low, high in _PG_STORAGE_INT_OPTIONS:
        raw_val = source.get(key, defaults_int.get(key))
        if raw_val is None or isinstance(raw_val, bool):
            continue
        int_val = int(raw_val)
        if int_val < low or int_val > high:
            msg = f"PostgreSQL storage option {key!r} must be between {low} and {high}, got {raw_val!r}"
            raise ImproperConfigurationError(msg)
        clauses.append(f"{sql_key} = {int_val}")

    if include_autovacuum:
        defaults_float = {
            "autovacuum_vacuum_scale_factor": default_vacuum_scale,
            "autovacuum_analyze_scale_factor": default_analyze_scale,
        }
        for key, sql_key, min_val, max_val in _PG_STORAGE_FLOAT_OPTIONS:
            raw_val = source.get(key, defaults_float.get(key))
            if raw_val is None or isinstance(raw_val, bool):
                continue
            float_val = float(raw_val)
            if float_val < min_val or float_val > max_val:
                msg = f"PostgreSQL storage option {key!r} must be between {min_val} and {max_val}, got {raw_val!r}"
                raise ImproperConfigurationError(msg)
            clauses.append(f"{sql_key} = {float_val:g}")

    if not clauses:
        return ""
    return " WITH (" + ", ".join(clauses) + ")"
