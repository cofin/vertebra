"""Standalone synchronous PEP 249 DB-API 2.0 connection, cursor, exceptions, and SQL classifier."""

from __future__ import annotations

import contextlib
import datetime
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Final, Protocol, Self, cast
from urllib.parse import urlparse

import msgspec

import vertebra._vertebra as _native
from vertebra._vertebra import classify_sql_statement
from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.db.registry import SCHEME_REGISTRY, AnyDatabaseConfig, DatabaseRegistry
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    UnsupportedBackendError,
    VertebraError,
)

__all__ = (
    "BFILE",
    "BINARY",
    "BLOB",
    "CLOB",
    "DATETIME",
    "FIXED_CHAR",
    "NCLOB",
    "NUMBER",
    "ROWID",
    "STRING",
    "Binary",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "SyncDBAPIConnection",
    "SyncDBAPICursor",
    "SyncDBAPIModule",
    "SyncSQLDriverProtocol",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "is_row_returning_sql",
    "paramstyle",
    "threadsafety",
)

apilevel: Final[str] = "2.0"
threadsafety: Final[int] = 2
paramstyle: Final[str] = "qmark"

type DescriptionColumn = tuple[str, Any, Any, Any, Any, Any, Any]
type DescriptionTuple = tuple[DescriptionColumn, ...]

_POSTGRES_CONNECT_ARGS: Final[frozenset[str]] = frozenset(
    {
        "dsn",
        "direct_dsn",
        "pooler_mode",
        "pgbouncer",
        "pgbouncer_mode",
        "protocol_level_pooler_cache",
        "max_prepared_statements",
        "min_size",
        "max_size",
        "enable_cloud_sql",
        "cloud_sql_instance",
        "cloud_sql_ip_type",
        "enable_alloydb",
        "alloydb_instance_uri",
        "alloydb_ip_type",
        "enable_iam_auth",
        "iam_auth_options",
        "enable_alloydb_columnar_engine",
        "application_name",
        "statement_timeout_ms",
        "idle_in_transaction_session_timeout_ms",
        "session_gucs",
    }
)

_SPANNER_CONNECT_ARGS: Final[frozenset[str]] = frozenset(
    {
        "project_id",
        "instance_id",
        "database_id",
        "dsn",
        "database_role",
        "dialect",
        "endpoint",
        "admin_endpoint",
        "pool_size",
        "route_to_leader_enabled",
        "max_commit_delay_ms",
        "data_boost_enabled",
        "max_partitions",
        "credentials_path",
        "service_account_key",
        "quota_project_id",
        "impersonate_service_account",
        "use_gcloud_cli",
        "scopes",
        "access_token",
        "use_shm_cache",
        "statement_cache_capacity",
        "ca_pem",
    }
)

_SUPPORTED_CONNECT_ARGS_BY_DIALECT: Final[dict[str, frozenset[str]]] = {
    "postgres": _POSTGRES_CONNECT_ARGS,
    "postgresql": _POSTGRES_CONNECT_ARGS,
    "mysql": frozenset(
        {
            "dsn",
            "host",
            "port",
            "user",
            "password",
            "database",
            "unix_socket",
            "charset",
            "autocommit",
            "connect_timeout",
            "read_timeout",
            "write_timeout",
            "ssl_mode",
            "ssl_ca",
            "ssl_cert",
            "ssl_key",
            "init_command",
            "min_size",
            "max_size",
            "statement_cache_size",
            "cloud_sql_instance",
            "cloud_sql_ip_type",
            "enable_iam_auth",
            "iam_auth_options",
            "credentials_path",
            "credentials_json",
            "target_service_account",
            "pool",
            "statement",
        }
    ),
    "sqlite": frozenset(
        {
            "path",
            "database",
            "wal_mode",
            "synchronous",
            "mmap_size",
            "busy_timeout_ms",
            "max_readers",
            "foreign_keys",
            "enable_jsonb",
        }
    ),
    "duckdb": frozenset(
        {
            "path",
            "database",
            "memory_limit_mb",
            "threads",
            "temp_directory",
            "parquet_metadata_cache",
            "httpfs_connection_caching",
            "enable_http_metadata_cache",
            "read_only",
            "secrets",
            "extensions",
            "init_statements",
            "config",
            "pool_min_size",
            "pool_max_size",
            "statement_cache_size",
        }
    ),
    "oracle": frozenset(
        {
            "dsn",
            "host",
            "port",
            "service_name",
            "sid",
            "connect_descriptor_str",
            "user",
            "password",
            "wallet_path",
            "wallet_zip",
            "tns_alias",
            "edition",
            "topology",
            "drcp",
            "auth_mode",
            "access_token",
            "token_auth_options",
            "driver_features",
            "client_identifier",
            "module",
            "action",
            "ecid",
            "context_namespace",
            "context_package_proc",
            "context_clear_proc",
            "pool_min_size",
            "pool_max_size",
            "statement_cache_size",
        }
    ),
    "mssql": frozenset(
        {
            "dsn",
            "host",
            "port",
            "database",
            "user",
            "password",
            "application_name",
            "encryption",
            "trust_server_certificate",
            "ca_pem",
            "client_cert_pem",
            "client_key_pem",
            "server_name",
            "connect_timeout_ms",
            "query_timeout_ms",
            "read_only_intent",
            "cloud_sql_instance",
            "cloudsql_ip_type",
            "cloudsql_auth_type",
            "cloudsql_proxy_addr",
            "pool_min_size",
            "pool_max_size",
            "statement_cache_size",
        }
    ),
    "bigquery": frozenset(
        {
            "project_id",
            "dataset_id",
            "location",
            "dsn",
            "rest_endpoint",
            "storage_endpoint",
            "maximum_bytes_billed",
            "job_timeout_ms",
            "priority",
            "labels",
            "credentials_path",
            "service_account_key",
            "quota_project_id",
            "impersonate_service_account",
            "access_token",
            "use_gcloud_cli",
            "scopes",
            "pool_size",
            "statement_cache_capacity",
            "use_shm_cache",
            "arrow_compression",
            "max_read_streams",
            "ca_pem",
        }
    ),
    "spanner": _SPANNER_CONNECT_ARGS,
    "spangres": _SPANNER_CONNECT_ARGS,
}

_SCHEME_ALIASES: Final[dict[str, str]] = {
    "mariadb": "mysql",
    "mysql+pymysql": "mysql",
    "sqlserver": "mssql",
    "bq": "bigquery",
    "spanner+googlesql": "spanner",
    "spanner+spangres": "spangres",
}

_TSQL_DML_VERBS: Final[frozenset[str]] = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})
_TSQL_OUTPUT_TARGETS: Final[frozenset[str]] = frozenset({"INSERTED", "DELETED"})


class SyncSQLDriverProtocol(Protocol):
    """Protocol describing synchronous Vertebra SQL drivers consumed by SyncDBAPIConnection."""

    dialect: str
    closed: bool

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Any:
        """Execute a statement and return rows with column metadata."""
        ...

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute a statement and return tuple rows."""
        ...

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a statement and return affected row count."""
        ...

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Execute a statement across parameter sets and return affected row count."""
        ...

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[Any]:
        """Execute a query and return hydrated row records."""
        ...

    def begin(self) -> None:
        """Begin a native transaction."""
        ...

    def commit(self) -> None:
        """Commit the active transaction."""
        ...

    def rollback(self) -> None:
        """Roll back the active transaction."""
        ...

    def close(self) -> None:
        """Close the underlying driver."""
        ...


class Error(Exception):
    """Base PEP 249 DBAPI exception."""


class Warning(Exception):
    """PEP 249 DBAPI warning exception."""


class InterfaceError(Error):
    """PEP 249 database interface error."""


class DatabaseError(Error):
    """PEP 249 database operation error."""


class DataError(DatabaseError):
    """PEP 249 data processing error."""


class OperationalError(DatabaseError):
    """PEP 249 database operational error."""


class IntegrityError(DatabaseError):
    """PEP 249 relational integrity constraint error."""


class InternalError(DatabaseError):
    """PEP 249 database internal error."""


class ProgrammingError(DatabaseError):
    """PEP 249 SQL programming error."""


class NotSupportedError(DatabaseError):
    """PEP 249 feature not supported error."""


class _DBAPITypeObject:
    """PEP 249 type singleton comparing equal to matching driver type codes."""

    __slots__ = ("_name", "_values")

    def __init__(self, name: str, *values: Any) -> None:
        self._name = name
        self._values = frozenset(values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _DBAPITypeObject):
            return self._name == other._name
        return other in self._values

    def __hash__(self) -> int:
        return hash(self._name)

    def __repr__(self) -> str:
        return f"<DBAPIType {self._name}>"


class _DBAPIOutBindVar:
    """Mutable PEP 249 output bind variable returned by cursor.var()."""

    __slots__ = ("arraysize", "outconverter", "size", "type_code", "values")

    def __init__(
        self,
        type_code: Any,
        size: int = 0,
        arraysize: int = 1,
        outconverter: Any = None,
    ) -> None:
        self.type_code = type_code
        self.size = size
        self.arraysize = max(int(arraysize), 1)
        self.outconverter = outconverter
        self.values: list[Any] = [None] * self.arraysize

    def getvalue(self, pos: int = 0) -> Any:
        """Return the bound output value at pos, applying outconverter when configured."""
        val = self.values[pos]
        if self.outconverter is not None and val is not None:
            return self.outconverter(val)
        return val

    def setvalue(self, pos: int, value: Any) -> None:
        """Assign the bound output value at pos."""
        if pos >= len(self.values):
            self.values.extend([None] * (pos - len(self.values) + 1))
        self.values[pos] = value


Binary = bytes
STRING: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "STRING",
    str,
    "str",
    "TEXT",
    "VARCHAR",
    "CHAR",
    "STRING",
    25,
    1043,
)
BINARY: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "BINARY",
    bytes,
    bytearray,
    memoryview,
    "BLOB",
    "BYTEA",
    "BINARY",
    "VARBINARY",
    17,
)
NUMBER: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "NUMBER",
    int,
    float,
    "INTEGER",
    "INT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "NUMERIC",
    "DECIMAL",
    "NUMBER",
    20,
    21,
    23,
    700,
    701,
    1700,
)
DATETIME: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "DATETIME",
    datetime.datetime,
    datetime.date,
    datetime.time,
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    1082,
    1083,
    1114,
    1184,
)
ROWID: Final[_DBAPITypeObject] = _DBAPITypeObject("ROWID", "ROWID", "OID", 26)
BLOB: Final[_DBAPITypeObject] = _DBAPITypeObject("BLOB", bytes, bytearray, memoryview, "BLOB")
BFILE: Final[_DBAPITypeObject] = _DBAPITypeObject("BFILE", bytes, bytearray, memoryview, "BFILE")
CLOB: Final[_DBAPITypeObject] = _DBAPITypeObject("CLOB", str, "CLOB")
NCLOB: Final[_DBAPITypeObject] = _DBAPITypeObject("NCLOB", str, "NCLOB")
FIXED_CHAR: Final[_DBAPITypeObject] = _DBAPITypeObject("FIXED_CHAR", str, "CHAR", "NCHAR", "FIXED_CHAR")


def Date(year: int, month: int, day: int) -> datetime.date:
    """Construct a PEP 249 date value."""
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:
    """Construct a PEP 249 time value."""
    return datetime.time(hour, minute, second)


def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> datetime.datetime:
    """Construct a PEP 249 timestamp value."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> datetime.date:
    """Construct a PEP 249 date value from epoch seconds."""
    local = time.localtime(ticks)
    return datetime.date(local.tm_year, local.tm_mon, local.tm_mday)


def TimeFromTicks(ticks: float) -> datetime.time:
    """Construct a PEP 249 time value from epoch seconds."""
    local = time.localtime(ticks)
    return datetime.time(local.tm_hour, local.tm_min, local.tm_sec)


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """Construct a PEP 249 timestamp value from epoch seconds."""
    local = time.localtime(ticks)
    return datetime.datetime(
        local.tm_year,
        local.tm_mon,
        local.tm_mday,
        local.tm_hour,
        local.tm_min,
        local.tm_sec,
    )


def _translate_native_error(exc: Exception) -> Exception:
    """Map a native vertebra._vertebra exception to its corresponding PEP 249 dbapi.*Error."""
    if isinstance(exc, (Error, Warning)):
        return exc
    target_cls: type[Error]
    if isinstance(exc, _native.IntegrityError):
        target_cls = IntegrityError
    elif isinstance(exc, _native.OperationalError):
        target_cls = OperationalError
    elif isinstance(exc, _native.ProgrammingError):
        target_cls = ProgrammingError
    elif isinstance(exc, _native.DataError):
        target_cls = DataError
    elif isinstance(exc, _native.NotSupportedError):
        target_cls = NotSupportedError
    elif isinstance(exc, _native.InterfaceError):
        target_cls = InterfaceError
    elif isinstance(exc, _native.InternalError):
        target_cls = InternalError
    elif isinstance(exc, _native.DatabaseError):
        target_cls = DatabaseError
    else:
        return exc
    new_exc = target_cls(str(exc))
    for attr in ("sqlstate", "code", "is_transient"):
        if hasattr(exc, attr):
            setattr(new_exc, attr, getattr(exc, attr))
    return new_exc


def _translate_vertebra_error(exc: VertebraError) -> Error:
    """Map a Vertebra configuration or runtime exception to a PEP 249 Error subclass."""
    if isinstance(exc, (ImproperConfigurationError, UnsupportedBackendError, MissingDependencyError)):
        return InterfaceError(str(exc))
    return DatabaseError(str(exc))


@contextlib.contextmanager
def _translate_errors() -> Iterator[None]:
    """Context manager catching driver and config exceptions and re-raising PEP 249 errors."""
    try:
        yield
    except (Error, Warning):
        raise
    except _native.DatabaseError as exc:
        raise _translate_native_error(exc) from exc
    except VertebraError as exc:
        raise _translate_vertebra_error(exc) from exc
    except (ValueError, TypeError) as exc:
        raise InterfaceError(str(exc)) from exc


def is_insert_sql(operation: str) -> bool:
    """Return True when the leading SQL verb in operation is INSERT."""
    return classify_sql_statement(operation)[1]


def is_read_only_sql(operation: str) -> bool:
    """Return True when the leading SQL verb is read-only query or introspection."""
    return classify_sql_statement(operation)[2]


def _coerce_column_description(col: Any) -> DescriptionColumn:
    """Normalize a driver column entry into a 7-item PEP 249 description tuple."""
    if isinstance(col, tuple):
        if len(col) >= 7:
            return (str(col[0]), col[1], col[2], col[3], col[4], col[5], col[6])
        if len(col) >= 2:
            return (str(col[0]), col[1], None, None, None, None, None)
        if len(col) == 1:
            return (str(col[0]), None, None, None, None, None, None)
        return ("", None, None, None, None, None, None)
    if isinstance(col, str):
        return (col, None, None, None, None, None, None)
    if hasattr(col, "name"):
        type_code = getattr(col, "type_code", None)
        if type_code is None:
            type_code = getattr(col, "type_oid", None)
        return (
            str(col.name),
            type_code,
            getattr(col, "display_size", None),
            getattr(col, "internal_size", None),
            getattr(col, "precision", None),
            getattr(col, "scale", None),
            getattr(col, "null_ok", None),
        )
    return (str(col), None, None, None, None, None, None)


def coerce_dbapi_rows_and_desc(
    raw_res: Any,
) -> tuple[list[tuple[Any, ...]], DescriptionTuple | None]:
    """Normalize driver query results into PEP 249 tuple rows and column descriptions."""
    if isinstance(raw_res, tuple) and len(raw_res) >= 2:
        raw_rows = raw_res[0]
        col_names = raw_res[1]
        desc = tuple(_coerce_column_description(col) for col in col_names) if col_names else None
        return [tuple(r) if not isinstance(r, tuple) else r for r in raw_rows], desc
    return [], None


def _match_dollar_quote_tag(sql: str, idx: int) -> str | None:
    """Return the PostgreSQL dollar-quote delimiter starting at idx, or None."""
    n = len(sql)
    if idx >= n or sql[idx] != "$":
        return None
    if idx + 1 < n and sql[idx + 1] == "$":
        return "$$"
    end = idx + 1
    if end >= n or not (sql[end].isalpha() or sql[end] == "_"):
        return None
    end += 1
    while end < n and (sql[end].isalnum() or sql[end] == "_"):
        end += 1
    if end < n and sql[end] == "$":
        return sql[idx : end + 1]
    return None


def _scan_unquoted_sql_tokens(sql: str) -> list[tuple[str, int]]:
    """Tokenize unquoted, uncommented SQL tokens with parenthesis depth."""
    tokens: list[tuple[str, int]] = []
    n = len(sql)
    i = 0
    paren_depth = 0
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            newline_pos = sql.find("\n", i + 2)
            i = n if newline_pos == -1 else newline_pos + 1
            continue
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            depth = 1
            i += 2
            while i < n and depth > 0:
                if sql[i] == "/" and i + 1 < n and sql[i + 1] == "*":
                    depth += 1
                    i += 2
                elif sql[i] == "*" and i + 1 < n and sql[i + 1] == "/":
                    depth -= 1
                    i += 2
                else:
                    i += 1
            continue
        if ch in {"'", '"', "`"}:
            quote = ch
            i += 1
            while i < n:
                if sql[i] == quote:
                    if i + 1 < n and sql[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if ch == "[":
            i += 1
            while i < n:
                if sql[i] == "]":
                    if i + 1 < n and sql[i + 1] == "]":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if ch == "$":
            tag = _match_dollar_quote_tag(sql, i)
            if tag is not None:
                close_pos = sql.find(tag, i + len(tag))
                i = n if close_pos == -1 else close_pos + len(tag)
                continue
            start = i
            i += 1
            while i < n and (sql[i].isalnum() or sql[i] == "_"):
                i += 1
            tokens.append((sql[start:i].upper(), paren_depth))
            continue
        if ch in {":", "@"} and i + 1 < n and (sql[i + 1].isalpha() or sql[i + 1] == "_"):
            start = i
            i += 2
            while i < n and (sql[i].isalnum() or sql[i] == "_"):
                i += 1
            tokens.append((sql[start:i].upper(), paren_depth))
            continue
        if ch in {"(", ")"}:
            if ch == "(":
                tokens.append(("(", paren_depth))
                paren_depth += 1
            else:
                paren_depth = max(0, paren_depth - 1)
                tokens.append((")", paren_depth))
            i += 1
            continue
        if ch in {".", ",", ";", "="}:
            tokens.append((ch, paren_depth))
            i += 1
            continue
        if ch.isalpha() or ch == "_":
            start = i
            i += 1
            while i < n and (sql[i].isalnum() or sql[i] == "_"):
                i += 1
            tokens.append((sql[start:i].upper(), paren_depth))
            continue
        i += 1
    return tokens


def _has_tsql_output_clause(operation: str) -> bool:
    """Return True when operation is a DML statement with a T-SQL OUTPUT clause."""
    tokens = _scan_unquoted_sql_tokens(operation)
    if not tokens:
        return False
    in_dml = False
    is_cte = False
    at_statement_start = True
    seen_output = False
    total = len(tokens)
    for idx, (tok, depth) in enumerate(tokens):
        if tok == ";" and depth == 0:
            in_dml = False
            is_cte = False
            at_statement_start = True
            seen_output = False
            continue
        if at_statement_start:
            at_statement_start = False
            if tok in _TSQL_DML_VERBS:
                in_dml = True
            elif tok == "WITH":
                is_cte = True
            continue
        if is_cte and not in_dml:
            if depth == 0 and tok in _TSQL_DML_VERBS:
                in_dml = True
            continue
        if not in_dml:
            continue
        if tok == "OUTPUT":
            prev_tok = tokens[idx - 1][0] if idx > 0 else ""
            next_tok = tokens[idx + 1][0] if idx + 1 < total else ""
            if prev_tok != "." and next_tok != "=":
                seen_output = True
            continue
        if seen_output:
            if tok == "$ACTION":
                return True
            if tok in _TSQL_OUTPUT_TARGETS and idx + 1 < total and tokens[idx + 1][0] == ".":
                return True
    return False


def is_row_returning_sql(operation: str) -> bool:
    """Return True when operation starts with a query keyword, RETURNING, or T-SQL OUTPUT clause."""
    if classify_sql_statement(operation)[0]:
        return True
    return _has_tsql_output_clause(operation)


def _extract_out_bind_vars(parameters: Any) -> dict[Any, _DBAPIOutBindVar]:
    """Return mapping of parameter keys or positions to _DBAPIOutBindVar placeholders."""
    if isinstance(parameters, Mapping):
        return {k: v for k, v in parameters.items() if isinstance(v, _DBAPIOutBindVar)}
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        return {idx: v for idx, v in enumerate(parameters) if isinstance(v, _DBAPIOutBindVar)}
    return {}


def _populate_out_bind_vars(
    out_vars: dict[Any, _DBAPIOutBindVar],
    rows: list[tuple[Any, ...]],
    driver: Any,
) -> None:
    """Populate cursor.var() placeholders from driver out_binds or returned row values."""
    if not out_vars:
        return
    driver_out = getattr(driver, "out_binds", None) if driver is not None else None
    if isinstance(driver_out, Mapping):
        for key, var in out_vars.items():
            if key in driver_out:
                var.setvalue(0, driver_out[key])
            elif isinstance(key, str) and key.lstrip(":") in driver_out:
                var.setvalue(0, driver_out[key.lstrip(":")])
        return
    if isinstance(driver_out, Sequence) and not isinstance(driver_out, (str, bytes, bytearray)):
        for idx, var in enumerate(out_vars.values()):
            if idx < len(driver_out):
                var.setvalue(0, driver_out[idx])
        return
    if rows:
        first_row = rows[0]
        for idx, var in enumerate(out_vars.values()):
            if idx < len(first_row):
                var.setvalue(0, first_row[idx])


def _normalize_connect_args_engine(url: str) -> str:
    """Extract normalized engine identifier from a database URL scheme."""
    raw_scheme, sep, _ = url.partition("://")
    scheme = (raw_scheme if sep else urlparse(url).scheme).lower()
    base_scheme = scheme
    if "+" in scheme:
        prefix, _, driver_tag = scheme.rpartition("+")
        if driver_tag in {"vtb", "vertebra", "vtb_async", "vertebra_async"}:
            base_scheme = prefix
    return _SCHEME_ALIASES.get(base_scheme, base_scheme)


def _validate_connect_kwargs(engine: str, connect_kwargs: dict[str, Any]) -> None:
    """Validate connect_args overrides against the engine's supported configuration fields."""
    if not connect_kwargs:
        return
    normalized_engine = _SCHEME_ALIASES.get(engine.lower(), engine.lower())
    allowed = _SUPPORTED_CONNECT_ARGS_BY_DIALECT.get(normalized_engine)
    if allowed is None:
        unsupported = ", ".join(sorted(connect_kwargs))
        msg = f"Unsupported connect_args for {normalized_engine!r}: {unsupported}"
        raise InterfaceError(msg)
    invalid = sorted(set(connect_kwargs) - allowed)
    if invalid:
        unsupported = ", ".join(invalid)
        msg = f"Unsupported connect_args for {normalized_engine!r}: {unsupported}"
        raise InterfaceError(msg)


def _resolve_dbapi_config(
    url: str | BaseDatabaseConfig | None,
    *,
    driver: Any,
    driver_broker: Any,
    connect_kwargs: dict[str, Any],
) -> tuple[str, AnyDatabaseConfig | BaseDatabaseConfig | None]:
    """Resolve URL, validate connect_args, and build typed config inside PEP 249 error translation."""
    if url is None and driver is None:
        exc = ValueError("Unsupported database URL scheme: ''")
        raise InterfaceError(f"Cannot parse Vertebra database URL: {exc}") from exc

    if isinstance(url, BaseDatabaseConfig):
        resolved_url = getattr(url, "dsn", getattr(url, "url", ""))
        if not resolved_url:
            dialect = getattr(url, "dialect", "sqlite")
            path_val = getattr(url, "path", "")
            resolved_url = f"{dialect}:///{path_val}" if path_val else f"{dialect}://"
        if connect_kwargs:
            _validate_connect_kwargs(_config_dialect(url), connect_kwargs)
            with _translate_errors():
                return str(resolved_url), msgspec.structs.replace(url, **connect_kwargs)
        return str(resolved_url), url

    resolved_url = str(url) if url is not None else str(getattr(driver, "url", getattr(driver, "dsn", "")))
    if not (resolved_url or driver is None):
        if connect_kwargs:
            _validate_connect_kwargs(_config_dialect(driver), connect_kwargs)
        return resolved_url, None

    raw_scheme, sep, _ = resolved_url.partition("://")
    scheme = (raw_scheme if sep else urlparse(resolved_url).scheme).lower()
    base_scheme = scheme
    if "+" in scheme:
        prefix, _, driver_tag = scheme.rpartition("+")
        if driver_tag in {"vtb", "vertebra", "vtb_async", "vertebra_async"}:
            base_scheme = prefix
    known_scheme = (
        SCHEME_REGISTRY.get(scheme)
        or SCHEME_REGISTRY.get(base_scheme)
        or SCHEME_REGISTRY.get(base_scheme.split("+", 1)[0])
    )
    if known_scheme is not None and connect_kwargs:
        engine = _normalize_connect_args_engine(resolved_url)
        _validate_connect_kwargs(engine, connect_kwargs)

    overrides = dict(connect_kwargs)
    if driver_broker is not None:
        overrides["broker"] = driver_broker
    with _translate_errors():
        try:
            parsed_config = DatabaseRegistry.parse_url(resolved_url, **overrides)
        except ValueError as exc:
            msg = f"Cannot parse Vertebra database URL: {exc}"
            raise InterfaceError(msg) from exc
    return resolved_url, parsed_config


def _build_driver(
    config: AnyDatabaseConfig | BaseDatabaseConfig,
    *,
    broker: Any,
    share_pool: bool,
    lease_managed: bool,
) -> tuple[SyncSQLDriverProtocol, bool]:
    """Construct or acquire a synchronous driver for config inside PEP 249 error translation."""
    with _translate_errors():
        has_shared_key = config.pool_key() is not None
        has_custom_broker = broker is not None and broker is not DatabaseRegistry._DEFAULT_BROKER
        if share_pool and not lease_managed and has_shared_key and not has_custom_broker:
            driver = DatabaseRegistry.default().get_shared_sync_driver(cast("Any", config))
            return cast("SyncSQLDriverProtocol", driver), False
        target_registry = DatabaseRegistry(broker=None if lease_managed else broker)
        build_fn = getattr(config, "build_sync_driver", None)
        if not callable(build_fn):
            msg = f"{type(config).__name__} does not support synchronous DBAPI connections"
            raise InterfaceError(msg)
        driver = build_fn(target_registry)
        return cast("SyncSQLDriverProtocol", driver), True


def _apply_dialect_metadata(module_obj: Any, dialect: str | None) -> None:
    """Attach engine-specific PEP 249 version attributes onto a DBAPI module instance."""
    if dialect is not None and dialect.lower() == "sqlite":
        module_obj.sqlite_version = sqlite3.sqlite_version
        module_obj.sqlite_version_info = sqlite3.sqlite_version_info


class SyncDBAPICursor:
    """PEP 249 DBAPI 2.0 cursor returning plain Python tuples."""

    __slots__ = (
        "_closed",
        "_conn",
        "_pos",
        "_raw_cursor",
        "_rows",
        "arraysize",
        "description",
        "lastrowid",
        "rowcount",
    )

    def __init__(self, connection: SyncDBAPIConnection, raw_cursor: Any = None) -> None:
        """Initialize PEP 249 cursor with connection and optional underlying DBAPI cursor."""
        self._conn = connection
        self._raw_cursor = raw_cursor
        self._closed: bool = False
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.description: DescriptionTuple | None = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.arraysize: int = 1

    def execute(self, operation: str, parameters: Sequence[Any] | dict[str, Any] = ()) -> SyncDBAPICursor:
        """Execute a SQL statement and buffer plain-tuple rows."""
        self._require_open()
        self.lastrowid = None
        out_vars = _extract_out_bind_vars(parameters)
        with _translate_errors():
            if not is_row_returning_sql(operation):
                affected, captured_rowid = self._conn.execute_dml_with_lastrowid(operation, parameters)
                self._rows = []
                self.description = None
                self._pos = 0
                self.rowcount = affected
                self.lastrowid = (
                    captured_rowid if captured_rowid is not None else self._resolve_driver_lastrowid(operation)
                )
                _populate_out_bind_vars(out_vars, self._rows, self._conn.driver)
                return self

            self._rows, self.description = self._conn.execute_with_description(operation, parameters)
            self._pos = 0
            self.rowcount = len(self._rows)
            self.lastrowid = self._resolve_driver_lastrowid(operation)
            _populate_out_bind_vars(out_vars, self._rows, self._conn.driver)
            return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> SyncDBAPICursor:
        """Execute a parameterized SQL statement across a sequence of parameter sets."""
        self._require_open()
        self.lastrowid = None
        with _translate_errors():
            if not is_row_returning_sql(operation):
                affected = self._conn.execute_many_dml(operation, seq_of_parameters)
                self._rows = []
                self.description = None
                self._pos = 0
                self.rowcount = affected
                return self

            accumulated_rows: list[tuple[Any, ...]] = []
            last_desc: DescriptionTuple | None = None
            for params in seq_of_parameters:
                rows, desc = self._conn.execute_with_description(operation, params)
                accumulated_rows.extend(rows)
                if desc is not None:
                    last_desc = desc
            self._rows = accumulated_rows
            self.description = last_desc
            self._pos = 0
            self.rowcount = len(accumulated_rows)
            return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next plain-tuple row or None."""
        self._require_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to size plain-tuple rows."""
        self._require_open()
        batch_size = self.arraysize if size is None else size
        if batch_size < 0:
            msg = f"fetchmany size must be non-negative (got {batch_size})"
            raise ProgrammingError(msg)
        batch = self._rows[self._pos : self._pos + batch_size]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining plain-tuple rows."""
        self._require_open()
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def var(
        self,
        type_code: Any,
        size: int = 0,
        arraysize: int = 1,
        outconverter: Any = None,
    ) -> Any:
        """Create an output bind variable for engines supporting out parameters."""
        self._require_open()
        raw_var_fn = getattr(self._raw_cursor, "var", None)
        if callable(raw_var_fn):
            with _translate_errors():
                return raw_var_fn(type_code, size=size, arraysize=arraysize, outconverter=outconverter)
        driver_var_fn = getattr(self._conn.driver, "var", None)
        if callable(driver_var_fn):
            with _translate_errors():
                return driver_var_fn(type_code, size=size, arraysize=arraysize, outconverter=outconverter)
        return _DBAPIOutBindVar(type_code, size=size, arraysize=arraysize, outconverter=outconverter)

    def nextset(self) -> bool | None:
        """Advance to the next result set when supported by the underlying driver."""
        self._require_open()
        nextset_fn = getattr(self._raw_cursor, "nextset", None) or getattr(self._conn.driver, "nextset", None)
        if callable(nextset_fn):
            with _translate_errors():
                result = nextset_fn()
            if isinstance(result, tuple):
                self._rows, self.description = coerce_dbapi_rows_and_desc(result)
                self._pos = 0
                self.rowcount = len(self._rows)
                return True
            if result:
                return True
        self._rows = []
        self.description = None
        self._pos = 0
        self.rowcount = -1
        return None

    def setinputsizes(self, sizes: Any) -> None:
        """Accept PEP 249 input buffer size hints as a no-op."""
        self._require_open()
        _ = sizes

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Accept PEP 249 output column buffer size hints as a no-op."""
        self._require_open()
        _ = (size, column)

    def close(self) -> None:
        """Close the cursor."""
        if not self._closed:
            self._rows = []
            self._pos = 0
            self._closed = True

    def __enter__(self) -> Self:
        """Enter cursor context."""
        self._require_open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit cursor context and close cursor."""
        self.close()

    def __iter__(self) -> SyncDBAPICursor:
        """Return cursor iterator."""
        return self

    def __next__(self) -> tuple[Any, ...]:
        """Fetch next row in iteration."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def _require_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor.")
        if self._conn.closed:
            raise InterfaceError("Cannot operate on a closed connection.")

    def _resolve_driver_lastrowid(self, operation: str) -> int | None:
        if not is_insert_sql(operation):
            return None
        driver = self._conn.driver
        if driver is None:
            return None
        direct = getattr(driver, "lastrowid", None)
        if isinstance(direct, int) and not isinstance(direct, bool):
            return direct
        last_rowid_fn = cast("Callable[[], Any] | None", getattr(driver, "last_insert_rowid", None))
        if callable(last_rowid_fn):
            val: Any = last_rowid_fn()
            return int(val) if val is not None else None
        return None


class SyncDBAPIConnection:
    """Standalone synchronous PEP 249 connection backed by Vertebra native drivers."""

    __slots__ = (
        "_aggregates",
        "_autocommit",
        "_broker",
        "_collations",
        "_config",
        "_driver",
        "_functions",
        "_in_transaction",
        "_owns_driver",
        "_share_pool",
        "_url",
        "closed",
    )

    def __init__(
        self,
        url: str | BaseDatabaseConfig | None = None,
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        lease_managed: bool = False,
        autocommit: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize PEP 249 connection with a non-empty database URL."""
        self._share_pool = bool(share_pool)
        self._broker = (
            broker
            if broker is not None
            else (DatabaseRegistry.default_broker() if self._share_pool and not lease_managed else None)
        )
        self._in_transaction = False
        self.closed: bool = False
        self._functions: dict[str, Any] = {}
        self._aggregates: dict[str, Any] = {}
        self._collations: dict[str, Any] = {}

        driver_broker = None if lease_managed else self._broker
        resolved_url, resolved_config = _resolve_dbapi_config(
            url,
            driver=driver,
            driver_broker=driver_broker,
            connect_kwargs=kwargs,
        )
        self._url: str = resolved_url
        self._config: AnyDatabaseConfig | BaseDatabaseConfig | None = resolved_config
        self._autocommit: bool = bool(getattr(self._config, "requires_statement_autocommit", False))
        self._driver: SyncSQLDriverProtocol | None = driver
        self._owns_driver: bool = driver is not None

        if self._driver is None and self._config is not None:
            self._driver, self._owns_driver = _build_driver(
                self._config,
                broker=broker,
                share_pool=self._share_pool,
                lease_managed=lease_managed,
            )

        if autocommit is not None:
            self.autocommit = bool(autocommit)

    @property
    def url(self) -> str:
        """Return the connection URL."""
        return self._url

    @property
    def config(self) -> AnyDatabaseConfig | BaseDatabaseConfig | None:
        """Return the parsed database configuration."""
        return self._config

    @property
    def broker(self) -> Any:
        """Return the attached shared-memory database broker, if any."""
        return self._broker

    @property
    def driver(self) -> SyncSQLDriverProtocol | None:
        """Return the active underlying driver instance."""
        return self._driver

    @property
    def autocommit(self) -> bool:
        """Return connection autocommit state."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit, rejecting transactional DBAPI mode for autocommit-only backends."""
        if not value and getattr(self._config, "requires_statement_autocommit", False):
            config_name = type(self._config).__name__ if self._config is not None else "Backend"
            msg = f"{config_name} DBAPI connections require autocommit; use the native driver transaction API"
            raise NotSupportedError(msg)
        if value and self._in_transaction:
            self.commit()
        self._autocommit = value

    def cursor(self) -> SyncDBAPICursor:
        """Create a new plain-tuple DBAPI cursor."""
        self._require_open()
        return SyncDBAPICursor(self)

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> SyncDBAPICursor:
        """Create a cursor and execute a statement."""
        cur = self.cursor()
        cur.execute(operation, parameters)
        return cur

    def create_function(
        self,
        name: str,
        narg: int,
        func: Any,
        *,
        deterministic: bool = False,
    ) -> None:
        """Register a custom scalar function on the connection."""
        self._functions[name] = (narg, func, deterministic)
        raw = getattr(self._driver, "_client", None) or getattr(self._driver, "raw_connection", None)
        if raw is not None and hasattr(raw, "create_function"):
            raw.create_function(name, narg, func, deterministic=deterministic)

    def create_aggregate(self, name: str, narg: int, aggregate_class: Any) -> None:
        """Register a custom aggregate function on the connection."""
        self._aggregates[name] = (narg, aggregate_class)
        raw = getattr(self._driver, "_client", None) or getattr(self._driver, "raw_connection", None)
        if raw is not None and hasattr(raw, "create_aggregate"):
            raw.create_aggregate(name, narg, aggregate_class)

    def create_collation(self, name: str, callable: Any) -> None:
        """Register a custom collation on the connection."""
        self._collations[name] = callable
        raw = getattr(self._driver, "_client", None) or getattr(self._driver, "raw_connection", None)
        if raw is not None and hasattr(raw, "create_collation"):
            raw.create_collation(name, callable)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], DescriptionTuple | None]:
        """Execute a query returning rows and column descriptions."""
        self._require_open()
        self.begin()
        rows: list[tuple[Any, ...]] = []
        desc: DescriptionTuple | None = None
        if self._driver is not None:
            should_autocommit = self._autocommit and (self._in_transaction or not is_read_only_sql(sql))
            with _translate_errors():
                if hasattr(self._driver, "execute_with_description"):
                    raw_res = self._driver.execute_with_description(sql, parameters)
                    rows, desc = coerce_dbapi_rows_and_desc(raw_res)
                elif hasattr(self._driver, "execute_tuples"):
                    rows = self._driver.execute_tuples(sql, parameters)
                    desc = (
                        tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(rows[0])))
                        if rows
                        else None
                    )
                elif hasattr(self._driver, "select"):
                    select_params = parameters if parameters else None
                    dict_rows = self._driver.select(sql, select_params)
                    if dict_rows:
                        keys = tuple(dict_rows[0].keys())
                        desc = tuple((str(k), None, None, None, None, None, None) for k in keys)
                        rows = [tuple(r.get(k) for k in keys) for r in dict_rows]
            if should_autocommit:
                self.commit()
        return rows, desc

    def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> int:
        """Execute a non-row-returning SQL statement and return the affected row count."""
        affected, _ = self.execute_dml_with_lastrowid(sql, parameters)
        return affected

    def execute_dml_with_lastrowid(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> tuple[int, int | None]:
        """Execute a non-row-returning SQL statement and return (rowcount, lastrowid)."""
        self._require_open()
        self.begin()
        if self._driver is not None:
            with _translate_errors():
                execute_dml_fn = cast(
                    "Callable[[str, Sequence[Any] | Mapping[str, Any]], tuple[int, int | None]] | None",
                    getattr(self._driver, "execute_dml", None),
                )
                if callable(execute_dml_fn):
                    affected, rowid = execute_dml_fn(sql, parameters)
                    if self._autocommit:
                        self.commit()
                    return int(affected), rowid
                if hasattr(self._driver, "execute"):
                    affected = int(self._driver.execute(sql, parameters))
                    if self._autocommit:
                        self.commit()
                    return affected, None
                rows, _ = self.execute_with_description(sql, parameters)
                return len(rows), None
        return -1, None

    def execute_many_dml(
        self,
        sql: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> int:
        """Execute a non-row-returning SQL statement across multiple parameter sets and return affected rows."""
        self._require_open()
        if not seq_of_parameters:
            return 0
        self.begin()
        if self._driver is not None:
            with _translate_errors():
                if hasattr(self._driver, "execute_many"):
                    affected = int(self._driver.execute_many(sql, seq_of_parameters))
                    if self._autocommit:
                        self.commit()
                    return affected
                if hasattr(self._driver, "execute"):
                    return sum(self.execute_dml(sql, params) for params in seq_of_parameters)
                return sum(len(self.execute_with_description(sql, params)[0]) for params in seq_of_parameters)
        return -1

    def execute_raw_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def begin(self) -> None:
        """Begin a native transaction when implicit commit is disabled."""
        self._require_open()
        if self._autocommit or self._in_transaction:
            return
        if self._driver is not None and hasattr(self._driver, "begin"):
            with _translate_errors():
                self._driver.begin()
                self._in_transaction = True

    def commit(self) -> None:
        """Commit the active transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "commit"):
                with _translate_errors():
                    self._driver.commit()
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "rollback"):
                with _translate_errors():
                    self._driver.rollback()
        finally:
            self._in_transaction = False

    def close(self) -> None:
        """Close the underlying driver connection."""
        if not self.closed:
            try:
                if self._in_transaction:
                    if self._autocommit:
                        self.commit()
                    else:
                        self.rollback()
            finally:
                self._in_transaction = False
                if self._owns_driver and self._driver is not None and hasattr(self._driver, "close"):
                    with _translate_errors():
                        self._driver.close()
                self.closed = True

    def __enter__(self) -> Self:
        """Enter connection context."""
        self._require_open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit connection context, committing or rolling back transaction."""
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

    def _require_open(self) -> None:
        if self.closed:
            raise InterfaceError("Cannot operate on a closed connection.")


class SyncDBAPIModule:
    """Standalone synchronous PEP 249 DBAPI 2.0 module descriptor."""

    apilevel: str = "2.0"
    threadsafety: int = 2
    paramstyle: str = "qmark"

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    Date = staticmethod(Date)
    Time = staticmethod(Time)
    Timestamp = staticmethod(Timestamp)
    DateFromTicks = staticmethod(DateFromTicks)
    TimeFromTicks = staticmethod(TimeFromTicks)
    TimestampFromTicks = staticmethod(TimestampFromTicks)
    Binary = Binary
    STRING = STRING
    BINARY = BINARY
    BLOB = BLOB
    BFILE = BFILE
    CLOB = CLOB
    NCLOB = NCLOB
    FIXED_CHAR = FIXED_CHAR
    NUMBER = NUMBER
    DATETIME = DATETIME
    ROWID = ROWID

    sqlite_version: str
    sqlite_version_info: tuple[int, ...]

    def __init__(self, *, paramstyle: str = "qmark", dialect: str | None = None) -> None:
        """Initialize PEP 249 DBAPI module with target parameter style and optional dialect."""
        self.paramstyle = paramstyle
        self.dialect = dialect
        _apply_dialect_metadata(self, dialect)

    def connect(self, *args: Any, **kwargs: Any) -> SyncDBAPIConnection:
        """Open a synchronous PEP 249 connection with a required URL."""
        url_arg: str | BaseDatabaseConfig | None = None
        if args and isinstance(args[0], (str, BaseDatabaseConfig)):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        if (
            not isinstance(url_arg, BaseDatabaseConfig)
            and (not url_arg or not str(url_arg).strip())
            and "driver" not in kwargs
        ):
            raise InterfaceError("A non-empty database connection URL is required.")
        return SyncDBAPIConnection(url_arg, **kwargs)


def connect(url: str | BaseDatabaseConfig | None = None, **kwargs: Any) -> SyncDBAPIConnection:
    """Open a standalone synchronous PEP 249 DB-API connection."""
    if not isinstance(url, BaseDatabaseConfig) and (not url or not str(url).strip()) and "driver" not in kwargs:
        raise InterfaceError("A non-empty database connection URL is required.")
    return SyncDBAPIConnection(url, **kwargs)


def _config_dialect(config: Any) -> str:
    """Resolve the lowercase dialect identifier exposed by a database configuration object."""
    cls_dialect = getattr(type(config), "dialect", "")
    raw = getattr(config, "dialect", cls_dialect)
    return str(raw).lower()
