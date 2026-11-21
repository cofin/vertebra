"""Oracle SQLSpec core helpers, exception mapping, compilation, version caching, and PL/SQL utilities."""

from __future__ import annotations

import contextlib
import re
import threading
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.core.cache import get_cache_config
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
)
from vertebra._vertebra import DataError as NativeDataError
from vertebra._vertebra import IntegrityError as NativeIntegrityError
from vertebra._vertebra import NotFoundError as NativeNotFoundError
from vertebra._vertebra import OperationalError as NativeOperationalError
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "ORACLE_ALREADY_EXISTS_ERROR",
    "ORACLE_DUPLICATE_KEY_ERROR",
    "ORACLE_INDEX_NOT_FOUND_ERROR",
    "ORACLE_SESSIONLESS_TXN_ID_MAX_BYTES",
    "ORACLE_TABLE_NOT_FOUND_ERROR",
    "OracleSessionContextConfig",
    "OracleVersionCache",
    "apply_oracle_session_attrs",
    "apply_oracle_transaction_priority",
    "build_duality_etag_update_sql",
    "build_oracle_checkout_operations",
    "build_oracle_release_operations",
    "build_oracle_statement_config",
    "escape_oracle_literal",
    "extract_duality_view_etag",
    "inject_duality_view_etag",
    "is_oracle_already_exists",
    "is_oracle_table_missing",
    "is_oracle_unique_violation",
    "map_native_database_exception",
    "map_oracle_exception",
    "native_statement_config",
    "normalize_oracle_transaction_priority",
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
    "validate_sessionless_execution_options",
    "validate_sessionless_transaction_args",
)

ORACLE_DUPLICATE_KEY_ERROR: Final[int] = 1
ORACLE_TABLE_NOT_FOUND_ERROR: Final[int] = 942
ORACLE_ALREADY_EXISTS_ERROR: Final[int] = 955
ORACLE_INDEX_NOT_FOUND_ERROR: Final[int] = 1418
ORACLE_SESSIONLESS_TXN_ID_MAX_BYTES: Final[int] = 64

_ORACLE_IDENT_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")
_ORA_CODE_RE: Final[re.Pattern[str]] = re.compile(r"ORA-(\d{5})")
_VALID_TRANSACTION_PRIORITIES: Final[frozenset[str]] = frozenset({"DEFAULT", "LOW", "MEDIUM", "HIGH"})


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


def _unwrap_typed_parameter(value: TypedParameter) -> Any:
    """Unwrap nested `TypedParameter` wrappers during SQLSpec parameter coercion."""
    unwrapped: Any = value
    while isinstance(unwrapped, TypedParameter):
        unwrapped = unwrapped.value
    return unwrapped


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


def map_native_database_exception(exc: BaseException) -> Exception | None:
    """Map Vertebra native `DatabaseError` hierarchy exceptions to `sqlspec.exceptions.*`."""
    if isinstance(exc, SQLSpecError):
        return None
    if not isinstance(exc, (DatabaseError, GoogleCloudError)):
        return None

    message = str(exc)
    lower = message.lower()
    sqlstate = str(getattr(exc, "sqlstate", "") or "").upper()

    if (
        isinstance(exc, GoogleCloudAuthError)
        or sqlstate in {"42501", "28000", "28P01"}
        or any(
            token in lower
            for token in ("permission denied", "unauthenticated", "forbidden", "access denied", "ora-01031")
        )
    ):
        return PermissionDeniedError(message)

    if isinstance(exc, QueryCancelledError) or any(
        token in lower for token in ("cancelled", "canceled", "query was interrupted", "57014")
    ):
        return OperationCancelledError(message)

    if (
        isinstance(exc, SerializationFailure)
        or sqlstate in {"40001", "40P01"}
        or any(token in lower for token in ("deadlock", "serialization", "ora-00060", "ora-08177", "aborted"))
    ):
        if "deadlock" in lower or sqlstate == "40P01" or "ora-00060" in lower:
            return DeadlockError(message)
        return SerializationConflictError(message)

    if isinstance(exc, NativeIntegrityError) or sqlstate.startswith("23"):
        if "unique" in lower or "duplicate" in lower or sqlstate == "23505" or "ora-00001" in lower:
            return UniqueViolationError(message)
        if "foreign key" in lower or sqlstate == "23503" or "ora-02291" in lower or "ora-02292" in lower:
            return ForeignKeyViolationError(message)
        if "check constraint" in lower or sqlstate == "23514":
            return CheckViolationError(message)
        if "not null" in lower or "not-null" in lower or sqlstate == "23502":
            return NotNullViolationError(message)
        return IntegrityError(message)

    if isinstance(exc, (ConnectionFailure, PoolExhaustedError)) or sqlstate.startswith("08"):
        if "timeout" in lower or "timed out" in lower or "ora-12170" in lower:
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

    if isinstance(exc, (InterfaceError, InternalError, DatabaseError, GoogleCloudError)):
        return OperationalError(message)

    return OperationalError(message)


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
    type_coercion_map = dict(param_cfg.type_coercion_map or {})
    type_coercion_map[TypedParameter] = _unwrap_typed_parameter
    return config.replace(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=param_cfg.default_parameter_style,
            supported_parameter_styles=param_cfg.supported_parameter_styles,
            default_execution_parameter_style=ParameterStyle.NAMED_COLON,
            supported_execution_parameter_styles={
                ParameterStyle.NAMED_COLON,
                ParameterStyle.POSITIONAL_COLON,
            },
            type_coercion_map=type_coercion_map,
            preserve_parameter_format=False,
            allow_mixed_parameter_styles=True,
        )
    )


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


class OracleSessionContextConfig:
    """Declarative Oracle `DBMS_SESSION` / `CLIENT_IDENTIFIER` checkout and release configuration."""

    __slots__ = (
        "action",
        "clear_context_namespaces",
        "clear_identifier",
        "client_identifier",
        "context_attributes",
        "contexts",
        "current_schema",
        "ecid",
        "edition",
        "module",
        "nls_parameters",
        "reset_on_release",
        "reset_package",
        "time_zone",
        "transaction_priority",
    )

    def __init__(
        self,
        *,
        client_identifier: str | Callable[[], str | None] | None = None,
        module: str | Callable[[], str | None] | None = None,
        action: str | Callable[[], str | None] | None = None,
        ecid: str | Callable[[], str | None] | None = None,
        edition: str | Callable[[], str | None] | None = None,
        time_zone: str | Callable[[], str | None] | None = None,
        current_schema: str | None = None,
        transaction_priority: Any = None,
        nls_parameters: Mapping[str, str] | Callable[[], Mapping[str, str] | None] | None = None,
        contexts: Mapping[str, Mapping[str, str | Callable[[], str | None]]]
        | Callable[[], Mapping[str, Mapping[str, str]] | None]
        | None = None,
        context_attributes: Mapping[str, Mapping[str, str | Callable[[], str | None]]]
        | Callable[[], Mapping[str, Mapping[str, str]] | None]
        | None = None,
        reset_package: bool = False,
        clear_identifier: bool = False,
        clear_context_namespaces: Sequence[str] = (),
        reset_on_release: bool = True,
    ) -> None:
        """Validate and store declarative Oracle session context configuration."""
        if isinstance(edition, str):
            oracle_identifier(edition, "edition")
        if isinstance(current_schema, str):
            oracle_identifier(current_schema, "current_schema")
        resolved_contexts = contexts if contexts is not None else context_attributes
        if isinstance(resolved_contexts, Mapping):
            for ns, attrs in resolved_contexts.items():
                oracle_identifier(ns, "context namespace")
                if not isinstance(attrs, Mapping):
                    msg = f"Oracle context attributes for namespace {ns!r} must be a mapping"
                    raise ImproperConfigurationError(msg)
                for attr_name in attrs:
                    oracle_identifier(attr_name, "context attribute")
        for ns in clear_context_namespaces:
            oracle_identifier(ns, "context namespace")
        self.client_identifier = client_identifier
        self.module = module
        self.action = action
        self.ecid = ecid
        self.edition = edition
        self.time_zone = time_zone
        self.current_schema = current_schema
        self.transaction_priority = normalize_oracle_transaction_priority(transaction_priority)
        self.nls_parameters = dict(nls_parameters) if isinstance(nls_parameters, Mapping) else nls_parameters
        normalized_ctx = (
            {ns: dict(attrs) for ns, attrs in resolved_contexts.items()}
            if isinstance(resolved_contexts, Mapping)
            else resolved_contexts
        )
        self.contexts = normalized_ctx
        self.context_attributes = normalized_ctx
        self.reset_package = bool(reset_package)
        self.clear_identifier = bool(clear_identifier)
        self.clear_context_namespaces: tuple[str, ...] = tuple(clear_context_namespaces)
        self.reset_on_release = bool(reset_on_release)

    def __repr__(self) -> str:
        """Return a concise representation of the session context configuration."""
        return (
            f"OracleSessionContextConfig(client_identifier={self.client_identifier!r}, "
            f"module={self.module!r}, action={self.action!r}, ecid={self.ecid!r}, "
            f"reset_package={self.reset_package!r}, reset_on_release={self.reset_on_release!r})"
        )

    def __eq__(self, other: object) -> bool:
        """Compare two `OracleSessionContextConfig` instances by value."""
        if not isinstance(other, OracleSessionContextConfig):
            return NotImplemented
        return (
            self.client_identifier,
            self.module,
            self.action,
            self.ecid,
            self.edition,
            self.time_zone,
            self.current_schema,
            self.transaction_priority,
            self.nls_parameters,
            self.contexts,
            self.reset_package,
            self.clear_identifier,
            self.clear_context_namespaces,
            self.reset_on_release,
        ) == (
            other.client_identifier,
            other.module,
            other.action,
            other.ecid,
            other.edition,
            other.time_zone,
            other.current_schema,
            other.transaction_priority,
            other.nls_parameters,
            other.contexts,
            other.reset_package,
            other.clear_identifier,
            other.clear_context_namespaces,
            other.reset_on_release,
        )

    def __hash__(self) -> int:
        """Compute hash over immutable scalar session context attributes."""
        return hash(
            (
                self.client_identifier,
                self.module,
                self.action,
                self.ecid,
                self.edition,
                self.time_zone,
                self.current_schema,
                self.transaction_priority,
                self.reset_package,
                self.clear_identifier,
                self.clear_context_namespaces,
                self.reset_on_release,
            )
        )


def validate_sessionless_transaction_args(
    transaction_id: str | bytes | None,
    *,
    timeout: int,
    is_resume: bool = False,
    defer_round_trip: bool = True,
) -> bytes | None:
    """Validate optional global transaction ID and timeout for Oracle 26ai sessionless transactions."""
    if (
        not isinstance(defer_round_trip, bool)
        or isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or timeout < (0 if is_resume else 1)
    ):
        msg = "Sessionless timeout must be positive (nonnegative on resume) and defer_round_trip must be bool"
        raise ImproperConfigurationError(msg)
    if transaction_id is None and not is_resume:
        return None
    if not isinstance(transaction_id, (str, bytes)):
        msg = "Sessionless transaction_id must be a string or bytes"
        raise ImproperConfigurationError(msg)
    resolved = transaction_id.encode("utf-8") if isinstance(transaction_id, str) else transaction_id
    if not 1 <= len(resolved) <= ORACLE_SESSIONLESS_TXN_ID_MAX_BYTES:
        msg = "Sessionless transaction_id must contain 1..64 UTF-8 bytes"
        raise ImproperConfigurationError(msg)
    return resolved


def validate_sessionless_execution_options(
    *,
    suspend_on_success: bool = False,
    batch_size: int | None = None,
) -> None:
    """Validate sessionless suspension and batch-size options prior to SQL execution."""
    if (
        not isinstance(suspend_on_success, bool)
        or (
            batch_size is not None
            and (isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0)
        )
        or (suspend_on_success and batch_size is not None)
    ):
        msg = "suspend_on_success must be bool and cannot be combined with a positive batch_size"
        raise ImproperConfigurationError(msg)


def normalize_oracle_transaction_priority(value: Any) -> Any:
    """Validate and normalize Oracle 26ai transaction priority (`DEFAULT`, `LOW`, `MEDIUM`, `HIGH`)."""
    if value is None:
        return None
    if isinstance(value, str):
        upper = value.strip().upper()
        if upper in _VALID_TRANSACTION_PRIORITIES:
            return upper
        msg = "Oracle transaction priority must be DEFAULT, LOW, MEDIUM, or HIGH"
        raise ImproperConfigurationError(msg)
    name = getattr(value, "name", None)
    if isinstance(name, str) and name.upper() in _VALID_TRANSACTION_PRIORITIES:
        return value
    msg = "Oracle transaction priority must be DEFAULT, LOW, MEDIUM, or HIGH"
    raise ImproperConfigurationError(msg)


def apply_oracle_transaction_priority(connection: Any, priority: Any) -> None:
    """Apply Oracle 26ai transaction priority to the underlying connection or driver."""
    resolved = normalize_oracle_transaction_priority(priority)
    if resolved is None:
        return
    setter = getattr(connection, "set_transaction_priority", None)
    if callable(setter):
        try:
            setter(resolved)
        except Exception as exc:
            mapped = map_oracle_exception(exc)
            if mapped is not None:
                raise mapped from exc
            raise
        return
    try:
        connection.transaction_priority = resolved
    except AttributeError as exc:
        msg = "Oracle transaction priority is not supported by the underlying native Oracle connection"
        raise DialectNotSupportedError(msg) from exc
    except Exception as exc:
        mapped = map_oracle_exception(exc)
        if mapped is not None:
            raise mapped from exc
        raise


def extract_duality_view_etag(row_or_doc: Mapping[str, Any] | str | bytes) -> str | None:
    """Extract `_metadata.etag` from an Oracle 26ai JSON Relational Duality View document or row."""
    parsed: Any = row_or_doc
    if isinstance(parsed, (str, bytes)):
        try:
            parsed = decode_json(parsed)
        except (ValueError, TypeError):
            return None
    if not isinstance(parsed, Mapping):
        return None
    metadata = parsed.get("_metadata")
    if isinstance(metadata, Mapping):
        etag = metadata.get("etag")
        if isinstance(etag, str) and etag:
            return etag
    for key in ("data", "DATA"):
        nested = parsed.get(key)
        if isinstance(nested, (str, bytes)):
            try:
                nested = decode_json(nested)
            except (ValueError, TypeError):
                continue
        if isinstance(nested, Mapping):
            nested_meta = nested.get("_metadata")
            if isinstance(nested_meta, Mapping):
                etag = nested_meta.get("etag")
                if isinstance(etag, str) and etag:
                    return etag
    return None


def inject_duality_view_etag(doc: Mapping[str, Any], etag: str) -> dict[str, Any]:
    """Return a copy of a Duality View document with `_metadata.etag` injected."""
    result: dict[str, Any] = dict(doc)
    existing_meta = result.get("_metadata")
    merged_meta: dict[str, Any] = dict(existing_meta) if isinstance(existing_meta, Mapping) else {}
    merged_meta["etag"] = etag
    result["_metadata"] = merged_meta
    return result


def build_duality_etag_update_sql(
    view_name: str,
    id_column: str = "ID",
    data_column: str = "DATA",
    *,
    etag_param: str = "expected_etag",
) -> str:
    """Build an optimistic-concurrency `UPDATE` statement for an Oracle 26ai JSON Relational Duality View."""
    validated_view = oracle_table_ref(view_name, "view_name")
    validated_id = oracle_identifier(id_column, "id_column")
    validated_data = oracle_identifier(data_column, "data_column")
    validated_etag_param = oracle_identifier(etag_param, "etag_param")
    return (
        f"UPDATE {validated_view} SET {validated_data} = :data "
        f"WHERE {validated_id} = :id AND JSON_VALUE({validated_data}, '$._metadata.etag') = :{validated_etag_param}"
    )


def _hook_field(hook: Any, name: str, default: Any = None) -> Any:
    """Read a field from either a mapping or an `OracleSessionContextConfig` instance."""
    if isinstance(hook, Mapping):
        return hook.get(name, default)
    return getattr(hook, name, default)


def _resolve_hook_value(raw: Any) -> Any:
    """Invoke callable hook values lazily while passing static values through unchanged."""
    return raw() if callable(raw) else raw


def build_oracle_checkout_operations(
    hook: Any,
) -> tuple[dict[str, Any], list[tuple[str, dict[str, str] | None]]]:
    """Build zero-RTT attribute updates and `DBMS_SESSION` checkout SQL for `session_context_hook`."""
    if hook is None or callable(hook) or not isinstance(hook, (Mapping, OracleSessionContextConfig)):
        return {}, []
    attrs: dict[str, Any] = {}
    sql_ops: list[tuple[str, dict[str, str] | None]] = []

    client_id = _resolve_hook_value(_hook_field(hook, "client_identifier"))
    if client_id is not None:
        client_id_str = str(client_id)
        attrs["client_identifier"] = client_id_str
        sql_ops.append(("BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;", {"value": client_id_str}))

    for attr_name in ("module", "action", "ecid"):
        val = _resolve_hook_value(_hook_field(hook, attr_name))
        if val is not None:
            attrs[attr_name] = str(val)

    priority = _hook_field(hook, "transaction_priority")
    if priority is not None:
        attrs["transaction_priority"] = normalize_oracle_transaction_priority(priority)

    current_schema = _hook_field(hook, "current_schema")
    if isinstance(current_schema, str) and current_schema.strip():
        validated_schema = oracle_identifier(current_schema, "current_schema")
        sql_ops.append((f"ALTER SESSION SET CURRENT_SCHEMA = {validated_schema}", None))

    contexts = _resolve_hook_value(_hook_field(hook, "contexts"))
    if contexts is None:
        contexts = _resolve_hook_value(_hook_field(hook, "context_attributes"))
    if isinstance(contexts, Mapping):
        for namespace, context_map in contexts.items():
            ns = oracle_identifier(str(namespace), "context namespace")
            if not isinstance(context_map, Mapping):
                continue
            for attr_key, raw_val in context_map.items():
                attr = oracle_identifier(str(attr_key), "context attribute")
                val = _resolve_hook_value(raw_val)
                if val is not None:
                    sql_ops.append(
                        (
                            "BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;",
                            {"namespace": ns, "attribute": attr, "value": str(val)},
                        )
                    )

    return attrs, sql_ops


def build_oracle_release_operations(
    hook: Any,
    *,
    reset_session_on_release: bool,
) -> tuple[bool, list[tuple[str, dict[str, str] | None]]]:
    """Build `DBMS_SESSION` cleanup and `RESET_PACKAGE` SQL for session release."""
    sql_ops: list[tuple[str, dict[str, str] | None]] = []
    clear_identifier_attr = False

    if hook is not None:
        hook_reset = (
            bool(_hook_field(hook, "reset_on_release", True))
            if isinstance(hook, (Mapping, OracleSessionContextConfig))
            else True
        )
        if hook_reset:
            if callable(hook):
                clear_identifier_attr = True
                sql_ops.append(("BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;", None))
            elif isinstance(hook, (Mapping, OracleSessionContextConfig)):
                if _hook_field(hook, "client_identifier") is not None or bool(_hook_field(hook, "clear_identifier")):
                    clear_identifier_attr = True
                    sql_ops.append(("BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;", None))
                contexts = _resolve_hook_value(_hook_field(hook, "contexts"))
                if contexts is None:
                    contexts = _resolve_hook_value(_hook_field(hook, "context_attributes"))
                if isinstance(contexts, Mapping):
                    for namespace, context_map in contexts.items():
                        ns = oracle_identifier(str(namespace), "context namespace")
                        if not isinstance(context_map, Mapping):
                            continue
                        for attr_key in context_map:
                            attr = oracle_identifier(str(attr_key), "context attribute")
                            sql_ops.append(
                                (
                                    "BEGIN DBMS_SESSION.CLEAR_CONTEXT(:namespace, NULL, :attribute); END;",
                                    {"namespace": ns, "attribute": attr},
                                )
                            )
                clear_namespaces = _hook_field(hook, "clear_context_namespaces") or ()
                for namespace in clear_namespaces:
                    ns = oracle_identifier(str(namespace), "context namespace")
                    sql_ops.append(
                        (
                            "BEGIN DBMS_SESSION.CLEAR_CONTEXT(:namespace, NULL, NULL); END;",
                            {"namespace": ns},
                        )
                    )
                if bool(_hook_field(hook, "reset_package")) or reset_session_on_release:
                    sql_ops.append(("BEGIN DBMS_SESSION.RESET_PACKAGE; END;", None))
                return clear_identifier_attr, sql_ops

    if reset_session_on_release:
        sql_ops.append(("BEGIN DBMS_SESSION.RESET_PACKAGE; END;", None))

    return clear_identifier_attr, sql_ops


def apply_oracle_session_attrs(connection: Any, attrs: Mapping[str, Any]) -> None:
    """Apply resolved session attributes and transaction priority to a connection."""
    if not attrs:
        return
    priority = attrs.get("transaction_priority")
    if priority is not None:
        apply_oracle_transaction_priority(connection, priority)
    tracing = {k: v for k, v in attrs.items() if k in ("client_identifier", "module", "action", "ecid")}
    if not tracing:
        return
    set_attrs_fn = getattr(connection, "set_session_attrs", None)
    if callable(set_attrs_fn):
        set_attrs_fn(**tracing)
        return
    for key, val in tracing.items():
        with contextlib.suppress(AttributeError):
            setattr(connection, key, val)
