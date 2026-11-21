"""MySQL and MariaDB SQL parameter compilation, coercion, and error classification helpers."""

from __future__ import annotations

import datetime
import re
from collections.abc import Mapping, Sequence
from typing import Any

from vertebra.db.driver import (
    ConnectionFailure,
    DatabaseError,
    DataError,
    IntegrityError,
    NotFoundError,
    OperationalError,
    ProgrammingError,
    QueryCancelledError,
    SerializationFailure,
)
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.schema import to_value_type

__all__ = (
    "MYSQL_TRANSIENT_ERRNOS",
    "MySQLCore",
    "build_mysql_connect_kwargs",
    "map_mysql_exception",
    "normalize_mysql_rows",
)

MYSQL_TRANSIENT_ERRNOS: frozenset[int] = frozenset(
    {
        1040,
        1042,
        1043,
        1047,
        1053,
        1152,
        1153,
        1154,
        1155,
        1156,
        1157,
        1158,
        1159,
        1160,
        1161,
        1205,
        1213,
        2002,
        2003,
        2006,
        2013,
    }
)

_INTEGRITY_ERRNOS: frozenset[int] = frozenset({1048, 1062, 1216, 1217, 1364, 1451, 1452, 3819, 4025})
_SERIALIZATION_ERRNOS: frozenset[int] = frozenset({1205, 1213})
_CANCELLED_ERRNOS: frozenset[int] = frozenset({1317, 1969, 3024})
_CONNECTION_ERRNOS: frozenset[int] = frozenset(
    {
        1040,
        1042,
        1043,
        1047,
        1053,
        1152,
        1153,
        1154,
        1155,
        1156,
        1157,
        1158,
        1159,
        1160,
        1161,
        2002,
        2003,
        2006,
        2013,
    }
)
_DATA_ERRNOS: frozenset[int] = frozenset({1264, 1265, 1292, 1366, 1406, 1690})
_PROGRAMMING_ERRNOS: frozenset[int] = frozenset(
    {
        1044,
        1045,
        1049,
        1050,
        1051,
        1054,
        1064,
        1142,
        1146,
        1305,
    }
)
_SERVER_ERR_RE = re.compile(r"ERROR\s+(\d+)\s+\(([0-9A-Z]{5})\):")


class MySQLCore:
    """Core statement compilation, parameter coercion, row normalization, and error mapping for MySQL."""

    @staticmethod
    def compile_for_driver(
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        """Compile named or positional MySQL binds via `vertebra._mysql.compile_binds`."""
        if not parameters:
            return sql, ()
        result: tuple[str, tuple[Any, ...]] = MySQLCore.require_native_module().compile_binds(sql, parameters)
        return result

    @staticmethod
    def require_native_module() -> Any:
        """Import and return `vertebra._mysql`, raising `MissingDependencyError` when absent."""
        return ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")

    @staticmethod
    def coerce_scalar_value(val: Any, value_type: type[Any] | None) -> Any:
        """Coerce a scalar value to `value_type`, normalizing naive `datetime` values to UTC."""
        if value_type is None or val is None:
            return val
        if value_type is datetime.datetime and isinstance(val, datetime.datetime) and val.tzinfo is None:
            return val.replace(tzinfo=datetime.UTC)
        return to_value_type(val, value_type)

    @staticmethod
    def normalize_rows(
        rows: Sequence[Any],
        description: Sequence[Any] | None,
    ) -> list[dict[str, Any]]:
        """Normalize raw cursor rows (`dict` or positional tuples) into `list[dict[str, Any]]`."""
        if not rows:
            return []
        first = rows[0]
        if isinstance(first, dict):
            return [dict(row) for row in rows]
        columns = [col[0] if isinstance(col, (tuple, list)) else str(col) for col in (description or ())]
        return [dict(zip(columns, row, strict=False)) for row in rows]

    @staticmethod
    def build_connect_kwargs(config: Any) -> dict[str, Any]:
        """Build keyword arguments for `SyncMySQLClient` and `AsyncMySQLClient` from `MySQLConfig`."""
        dsn = config.dsn
        parsed = config.parse_dsn_params()
        pool_min = max(1, int(config.min_size if config.min_size != 1 else getattr(config.pool, "min_size", 1)))
        pool_max = max(
            pool_min,
            int(config.max_size if config.max_size != 10 else getattr(config.pool, "max_size", 10)),
        )
        return {
            "dsn": dsn,
            "host": parsed["host"],
            "port": parsed["port"],
            "user": parsed["user"],
            "password": parsed["password"] or None,
            "database": parsed["database"],
            "unix_socket": parsed["unix_socket"],
            "ssl_mode": parsed["ssl_mode"],
            "ssl_ca": config.ssl_ca,
            "ssl_cert": config.ssl_cert,
            "ssl_key": config.ssl_key,
            "connect_timeout_ms": max(1, int(config.connect_timeout * 1000)),
            "read_timeout_ms": max(1, int(config.read_timeout * 1000)),
            "write_timeout_ms": max(1, int(config.write_timeout * 1000)),
            "pool_min_size": pool_min,
            "pool_max_size": pool_max,
            "statement_cache_size": int(config.statement_cache_size),
            "charset": parsed["charset"],
            "autocommit": bool(parsed["autocommit"]),
            "init_command": config.init_command,
            "cloud_sql_instance": config.cloud_sql_instance,
            "cloud_sql_ip_type": config.cloud_sql_ip_type,
            "enable_iam_auth": bool(config.enable_iam_auth),
            "credentials_path": config.credentials_path or config.iam_auth_options.get("credentials_path"),
            "credentials_json": config.credentials_json or config.iam_auth_options.get("credentials_json"),
            "target_service_account": (
                config.target_service_account or config.iam_auth_options.get("target_service_account")
            ),
        }

    @staticmethod
    def map_exception(exc: BaseException) -> DatabaseError:
        """Map an exception from MySQL or MariaDB into the canonical `DatabaseError` hierarchy."""
        if isinstance(exc, DatabaseError):
            return exc
        errno: int | None = None
        sqlstate: str | None = None
        args = getattr(exc, "args", ())
        if args and isinstance(args[0], int):
            errno = int(args[0])
            msg = args[1] if len(args) > 1 and isinstance(args[1], str) else str(exc)
        else:
            msg = str(exc)
            raw_errno = getattr(exc, "errno", None) or getattr(exc, "vendor_code", None)
            if isinstance(raw_errno, int):
                errno = raw_errno

        raw_state = getattr(exc, "sqlstate", None)
        if isinstance(raw_state, str):
            sqlstate = raw_state

        if errno is None:
            match = _SERVER_ERR_RE.search(msg)
            if match is not None:
                errno = int(match.group(1))
                if sqlstate is None:
                    sqlstate = match.group(2)

        if "Query returned no rows" in msg:
            not_found = NotFoundError(msg)
            not_found.sqlstate = sqlstate or "02000"
            not_found.is_transient = False
            return not_found

        target_cls: type[DatabaseError] = OperationalError
        if errno is not None:
            if errno in _INTEGRITY_ERRNOS:
                target_cls = IntegrityError
            elif errno in _SERIALIZATION_ERRNOS:
                target_cls = SerializationFailure
            elif errno in _CANCELLED_ERRNOS:
                target_cls = QueryCancelledError
            elif errno in _CONNECTION_ERRNOS:
                target_cls = ConnectionFailure
            elif errno in _DATA_ERRNOS:
                target_cls = DataError
            elif errno in _PROGRAMMING_ERRNOS:
                target_cls = ProgrammingError
        elif sqlstate:
            if sqlstate == "40001":
                target_cls = SerializationFailure
            elif sqlstate in {"70100", "HY008"}:
                target_cls = QueryCancelledError
            elif sqlstate.startswith("23"):
                target_cls = IntegrityError
            elif sqlstate.startswith("08"):
                target_cls = ConnectionFailure
            elif sqlstate.startswith("22"):
                target_cls = DataError
            elif sqlstate.startswith(("42", "28")):
                target_cls = ProgrammingError

        is_transient = bool(
            (errno is not None and errno in MYSQL_TRANSIENT_ERRNOS)
            or (sqlstate is not None and (sqlstate == "40001" or sqlstate.startswith("08")))
            or target_cls in {ConnectionFailure, SerializationFailure}
        )
        mapped = target_cls(msg)
        mapped.sqlstate = sqlstate
        mapped.vendor_code = errno
        mapped.is_transient = is_transient
        return mapped


def normalize_mysql_rows(
    rows: Sequence[Any],
    description: Sequence[Any] | None,
) -> list[dict[str, Any]]:
    """Normalize raw cursor rows into list[dict[str, Any]]."""
    return MySQLCore.normalize_rows(rows, description)


def build_mysql_connect_kwargs(config: Any) -> dict[str, Any]:
    """Build keyword arguments for MySQL clients from MySQLConfig."""
    return MySQLCore.build_connect_kwargs(config)


def map_mysql_exception(exc: BaseException) -> DatabaseError:
    """Map an exception into the canonical DatabaseError hierarchy."""
    return MySQLCore.map_exception(exc)
