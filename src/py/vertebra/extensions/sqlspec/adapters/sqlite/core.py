"""SQLite adapter core helpers for statement config, bind formatting, PRAGMAs, and metadata shaping."""

from __future__ import annotations

import array
import math
import re
from collections.abc import Collection, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Literal

from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    StatementConfig,
    build_statement_config_from_profile,
)
from sqlspec.core.cache import get_cache_config
from sqlspec.data_dictionary.dialects.sqlite import list_sqlite_available_features, resolve_sqlite_json_type
from sqlspec.data_dictionary.dialects.sqlite.config import (
    format_sqlite_schema_prefix,
    normalize_sqlite_schema_name,
    sqlite_check_constraints,
    sqlite_constraint_metadata,
    sqlite_generated_expressions,
    sqlite_object_ddl_result,
    sqlite_schema_ddl_results,
)
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    SQLParsingError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_decimal_converter, build_uuid_coercions

from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.typing import ImproperConfigurationError

__all__ = (
    "JULIAN_EPOCH",
    "SECONDS_PER_DAY",
    "SQLITE_TABLE_NOT_FOUND_TEXT",
    "apply_pragmas_async",
    "apply_pragmas_sync",
    "build_connection_config",
    "build_sqlite_scope_clause",
    "cosine_similarity",
    "datetime_to_julian",
    "default_statement_config",
    "driver_profile",
    "extension_pragma_statements",
    "format_fts_options",
    "format_sqlite_schema_prefix",
    "julian_to_datetime",
    "list_sqlite_available_features",
    "map_sqlite_exception",
    "native_statement_config",
    "normalize_sqlite_schema_name",
    "pack_embedding",
    "render_pragmas",
    "resolve_sqlite_json_type",
    "sqlite_check_constraints",
    "sqlite_constraint_metadata",
    "sqlite_generated_expressions",
    "sqlite_object_ddl_result",
    "sqlite_schema_ddl_results",
    "unpack_embedding",
    "validate_fts_options",
)

JULIAN_EPOCH = 2440587.5
SECONDS_PER_DAY = 86400.0
SQLITE_TABLE_NOT_FOUND_TEXT = "no such table"

_PRAGMA_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PRAGMA_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9_.\-]+$")
_FTS_DETAIL_VALUES = frozenset({"full", "column", "none"})
_FTS_TOKENIZE_PATTERN = re.compile(r"^[A-Za-z0-9_ -]+$")
_EXTENSION_PRAGMA_PROFILE: tuple[tuple[str, str], ...] = (
    ("foreign_keys", "ON"),
    ("cache_size", "-64000"),
    ("mmap_size", "30000000"),
    ("journal_size_limit", "67108864"),
)
_EPOCH_UNIX = datetime(1970, 1, 1, tzinfo=UTC)


def native_statement_config(
    dialect: str,
    parameter_style: ParameterStyle,
    **overrides: Any,
) -> StatementConfig:
    """Construct an isolated `StatementConfig` configured for the adapter's native bind syntax."""
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


def driver_profile() -> DriverParameterProfile:
    """Build the SQLite driver parameter profile accepting `?` and `:name` input and executing as `?`."""
    decimal_converter = build_decimal_converter(mode="string")
    type_handlers = {**build_uuid_coercions(native=False)}
    return DriverParameterProfile(
        name="vertebra_sqlite",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=True,
        json_serializer_strategy="helper",
        custom_type_coercions={
            **type_handlers,
            type(None): lambda value: value,
        },
        extras={"json_serializer": to_json, "json_deserializer": from_json, "decimal_converter": decimal_converter},
    )


def default_statement_config() -> StatementConfig:
    """Return the default SQLite `StatementConfig` accepting `?` and `:name` input and executing as `?`."""
    _ = build_statement_config_from_profile
    return native_statement_config(
        "sqlite",
        ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        supported_execution_styles={ParameterStyle.QMARK},
    )


def datetime_to_julian(dt: datetime | None) -> float | None:
    """Convert an aware or naive UTC `datetime` to SQLite Julian Day float."""
    if dt is None:
        return None
    aware = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    epoch_seconds = (aware - _EPOCH_UNIX).total_seconds()
    return JULIAN_EPOCH + (epoch_seconds / SECONDS_PER_DAY)


def julian_to_datetime(julian: float | int | str | None) -> datetime | None:
    """Convert a SQLite Julian Day number or ISO string back into a UTC `datetime`."""
    if julian is None:
        return None
    if isinstance(julian, str):
        stripped = julian.strip()
        try:
            julian_num = float(stripped)
        except ValueError:
            parsed = datetime.fromisoformat(stripped)
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
        days_since_epoch = julian_num - JULIAN_EPOCH
        return datetime.fromtimestamp(days_since_epoch * SECONDS_PER_DAY, tz=UTC)
    days_since_epoch = float(julian) - JULIAN_EPOCH
    return datetime.fromtimestamp(days_since_epoch * SECONDS_PER_DAY, tz=UTC)


def render_pragmas(pragmas: Mapping[str, Any]) -> list[tuple[str, str]]:
    """Validate and format a mapping of SQLite PRAGMA names to normalized `(name, value)` pairs."""
    rendered: list[tuple[str, str]] = []
    for raw_name, raw_value in pragmas.items():
        name = str(raw_name).strip()
        if not _PRAGMA_NAME_PATTERN.match(name):
            msg = f"Invalid SQLite PRAGMA name: {raw_name!r}"
            raise ImproperConfigurationError(msg)
        if isinstance(raw_value, bool):
            formatted = "ON" if raw_value else "OFF"
        elif isinstance(raw_value, int):
            formatted = str(raw_value)
        elif isinstance(raw_value, str):
            formatted = raw_value.strip()
            if not _PRAGMA_VALUE_PATTERN.match(formatted):
                msg = f"Invalid SQLite PRAGMA value for {name}: {raw_value!r}"
                raise ImproperConfigurationError(msg)
        else:
            msg = f"Unsupported SQLite PRAGMA value type for {name}: {type(raw_value).__name__}"
            raise ImproperConfigurationError(msg)
        rendered.append((name, formatted))
    return rendered


def extension_pragma_statements(config: Any, section: str) -> list[str]:
    """Build SQLite PRAGMA statements from an extension configuration section."""
    ext_cfg = getattr(config, "extension_config", {})
    section_cfg = ext_cfg.get(section, {}) if isinstance(ext_cfg, Mapping) else {}
    if not isinstance(section_cfg, Mapping):
        return []
    use_profile = bool(section_cfg.get("pragma_profile", False))
    overrides = section_cfg.get("pragma_overrides")
    statements: list[str] = []
    seen_names: set[str] = set()
    if overrides is not None:
        if not isinstance(overrides, Mapping):
            msg = f"extension_config['{section}']['pragma_overrides'] must be a mapping of PRAGMA names to values"
            raise ImproperConfigurationError(msg)
        override_pairs = render_pragmas(overrides)
        for name, _val in override_pairs:
            seen_names.add(name.lower())
        if use_profile:
            for prof_name, prof_val in _EXTENSION_PRAGMA_PROFILE:
                if prof_name.lower() not in seen_names:
                    statements.append(f"PRAGMA {prof_name} = {prof_val}")
        for name, val in override_pairs:
            statements.append(f"PRAGMA {name} = {val}")
        return statements
    if use_profile:
        for prof_name, prof_val in _EXTENSION_PRAGMA_PROFILE:
            statements.append(f"PRAGMA {prof_name} = {prof_val}")
    return statements


def apply_pragmas_sync(driver: Any, statements: Sequence[str]) -> None:
    """Execute SQLite PRAGMA statements sequentially via a synchronous driver."""
    for statement in statements:
        driver.execute(statement)


async def apply_pragmas_async(driver: Any, statements: Sequence[str]) -> None:
    """Execute SQLite PRAGMA statements sequentially via an asynchronous driver."""
    for statement in statements:
        await driver.execute(statement)


def validate_fts_options(config: Any) -> dict[str, Any]:
    """Validate optional SQLite FTS5 tokenizer and detail settings in `extension_config['adk']`."""
    ext_cfg = getattr(config, "extension_config", {})
    adk_cfg = ext_cfg.get("adk", {}) if isinstance(ext_cfg, Mapping) else {}
    if not isinstance(adk_cfg, Mapping):
        return {}
    options: dict[str, Any] = {}
    tokenize = adk_cfg.get("fts_tokenize")
    if tokenize is not None:
        tok_str = str(tokenize).strip()
        if not tok_str or not _FTS_TOKENIZE_PATTERN.match(tok_str):
            msg = f"Invalid SQLite FTS5 tokenize specification: {tokenize!r}"
            raise ImproperConfigurationError(msg)
        options["tokenize"] = tok_str
    detail = adk_cfg.get("fts_detail")
    if detail is not None:
        detail_str = str(detail).strip().lower()
        if detail_str not in _FTS_DETAIL_VALUES:
            msg = f"Invalid SQLite FTS5 detail mode: {detail!r}"
            raise ImproperConfigurationError(msg)
        options["detail"] = detail_str
    return options


def format_fts_options(options: Mapping[str, Any]) -> str:
    """Format validated FTS5 virtual-table options into a SQLite `USING fts5(...)` tail clause."""
    parts: list[str] = []
    if "tokenize" in options:
        parts.append(f"tokenize='{options['tokenize']}'")
    if "detail" in options:
        parts.append(f"detail={options['detail']}")
    return f", {', '.join(parts)}" if parts else ""


def build_sqlite_scope_clause(
    table_alias: str,
    app_name: str,
    user_id: str,
    scope_filter: Literal["all", "user", "app"] = "all",
) -> tuple[str, tuple[Any, ...]]:
    """Construct the SQLite WHERE clause and bind values for ADK memory scope filtering."""
    if scope_filter == "user":
        return f"{table_alias}app_name = ? AND {table_alias}user_id = ? AND {table_alias}scope = 'user'", (
            app_name,
            user_id,
        )
    if scope_filter == "app":
        return f"{table_alias}app_name = ? AND {table_alias}scope = 'app'", (app_name,)
    return (
        f"{table_alias}app_name = ? AND (({table_alias}user_id = ? AND {table_alias}scope = 'user') OR {table_alias}scope = 'app')",
        (app_name, user_id),
    )


def pack_embedding(embedding: Sequence[float] | None) -> bytes | None:
    """Serialize a float vector into packed IEEE-754 little-endian `BLOB` bytes."""
    if embedding is None:
        return None
    buf = array.array("f", (float(x) for x in embedding))
    return buf.tobytes()


def unpack_embedding(blob: bytes | bytearray | memoryview | None) -> list[float] | None:
    """Deserialize packed IEEE-754 float `BLOB` bytes back into a list of floats."""
    if not blob:
        return None
    buf = array.array("f")
    buf.frombytes(bytes(blob))
    return list(buf)


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
) -> SQLiteConfig:
    """Construct a native `SQLiteConfig` from SQLSpec connection parameters and shared `:memory:` URIs."""
    merged: dict[str, Any] = {}
    if isinstance(connection_config, Mapping):
        merged.update(connection_config)
    if isinstance(overrides, Mapping):
        merged.update(overrides)

    url = merged.pop("url", None) or merged.pop("dsn", None)
    database = merged.pop("database", None)
    path = merged.pop("path", None)
    uri_flag = bool(merged.pop("uri", False))
    timeout = merged.pop("timeout", None)
    enable_foreign_keys = merged.pop("enable_foreign_keys", None)
    enable_optimizations = merged.pop("enable_optimizations", None)
    pragmas = merged.pop("pragmas", None)
    if isinstance(driver_features, Mapping) and pragmas is None:
        pragmas = driver_features.get("pragmas")
    if isinstance(pragmas, Mapping):
        render_pragmas(pragmas)

    for ignored_key in (
        "detect_types",
        "isolation_level",
        "check_same_thread",
        "factory",
        "cached_statements",
        "autocommit",
        "pool_recycle_seconds",
        "health_check_interval",
        "extra",
    ):
        merged.pop(ignored_key, None)

    if timeout is not None and "busy_timeout_ms" not in merged:
        merged["busy_timeout_ms"] = int(float(timeout) * 1000)
    if enable_foreign_keys is not None and "foreign_keys" not in merged:
        merged["foreign_keys"] = bool(enable_foreign_keys)
    if enable_optimizations is not None and "wal_mode" not in merged:
        merged["wal_mode"] = bool(enable_optimizations)
    if broker is not None and "broker" not in merged:
        merged["broker"] = broker

    if url:
        return SQLiteConfig.from_url(str(url), **merged)

    target = path if path is not None else database
    if target is not None:
        target_str = str(target)
        if uri_flag and not target_str.startswith("file:") and target_str != ":memory:":
            target_str = f"file:{target_str}"
        merged["path"] = target_str
        merged["database"] = target_str
    return SQLiteConfig(**merged)


def map_sqlite_exception(exc: BaseException) -> Exception | None:
    """Map SQLite extended messages and error strings onto SQLSpec exception subclasses."""
    msg = str(exc)
    lower = msg.lower()
    if (
        "unique constraint failed" in lower
        or "sqlite_constraint_unique" in lower
        or "sqlite_constraint_primarykey" in lower
    ):
        return UniqueViolationError(msg)
    if "foreign key constraint failed" in lower or "sqlite_constraint_foreignkey" in lower:
        return ForeignKeyViolationError(msg)
    if "not null constraint failed" in lower or "sqlite_constraint_notnull" in lower:
        return NotNullViolationError(msg)
    if "check constraint failed" in lower or "sqlite_constraint_check" in lower:
        return CheckViolationError(msg)
    if "constraint failed" in lower:
        return IntegrityError(msg)
    if "sqlite_interrupt" in lower or "interrupted" in lower:
        return OperationCancelledError(msg)
    if (
        "database is locked" in lower
        or "database table is locked" in lower
        or "sqlite_busy" in lower
        or "sqlite_locked" in lower
    ):
        return OperationalError(msg)
    if "readonly" in lower or "attempt to write a readonly database" in lower:
        return PermissionDeniedError(msg)
    if "unable to open database" in lower or "sqlite_cantopen" in lower:
        return DatabaseConnectionError(msg)
    if "datatype mismatch" in lower:
        return DataError(msg)
    if (
        "syntax error" in lower
        or "no such table" in lower
        or "no such column" in lower
        or "unknown parameter name" in lower
    ):
        return SQLParsingError(msg)
    if "deadlock" in lower:
        return DeadlockError(msg)
    return None
