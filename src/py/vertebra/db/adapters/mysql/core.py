"""MySQL and MariaDB SQL parameter compilation, coercion, and error classification helpers."""

from __future__ import annotations

import datetime
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgspec

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
from vertebra.utils.serializers import encode_json

__all__ = (
    "MYSQL_TRANSIENT_ERRNOS",
    "MySQLCore",
    "build_mysql_connect_kwargs",
    "coerce_mysql_param",
    "compile_mysql_statement",
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
    def coerce_param(value: Any) -> Any:
        """Coerce a Python bind value into a MySQL-compatible parameter value."""
        if value is None or isinstance(
            value,
            (
                bool,
                int,
                float,
                str,
                bytes,
                Decimal,
                datetime.datetime,
                datetime.date,
                datetime.time,
                datetime.timedelta,
            ),
        ):
            return value
        if isinstance(value, (bytearray, memoryview)):
            return bytes(value)
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, (msgspec.Struct, dict, list)):
            return encode_json(value)
        return value

    @staticmethod
    def compile_statement(
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        target_style: str = "qmark",
    ) -> tuple[str, tuple[Any, ...]]:
        """Compile a SQL statement with `:name`, `%(name)s`, `%s`, or `?` binds into positional parameters."""
        if not parameters:
            if target_style == "format" and "?" in sql:
                return MySQLCore._rewrite_placeholders_outside_literals(sql, "?", "%s"), ()
            if target_style == "qmark" and "%s" in sql:
                return MySQLCore._rewrite_placeholders_outside_literals(sql, "%s", "?"), ()
            return sql, ()

        placeholder = "%s" if target_style == "format" else "?"
        if isinstance(parameters, Mapping):
            compiled_sql, ordered_values = MySQLCore._compile_named_mysql_binds(sql, parameters, placeholder)
            return compiled_sql, tuple(MySQLCore.coerce_param(v) for v in ordered_values)

        rewritten_sql = (
            MySQLCore._rewrite_placeholders_outside_literals(sql, "?", "%s")
            if target_style == "format"
            else MySQLCore._rewrite_placeholders_outside_literals(sql, "%s", "?")
        )
        return rewritten_sql, tuple(MySQLCore.coerce_param(v) for v in parameters)

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

    @staticmethod
    def _consume_literal_or_comment(sql: str, index: int, length: int) -> int | None:
        ch = sql[index]
        if ch in {"'", '"', "`"}:
            quote = ch
            cursor = index + 1
            while cursor < length:
                c = sql[cursor]
                if c == "\\" and quote != "`" and cursor + 1 < length:
                    cursor += 2
                    continue
                if c == quote:
                    if cursor + 1 < length and sql[cursor + 1] == quote:
                        cursor += 2
                        continue
                    return cursor + 1
                cursor += 1
            return length
        if (ch == "-" and index + 1 < length and sql[index + 1] == "-") or ch == "#":
            newline_idx = sql.find("\n", index + (2 if ch == "-" else 1))
            return length if newline_idx == -1 else newline_idx + 1
        if ch == "/" and index + 1 < length and sql[index + 1] == "*":
            end_idx = sql.find("*/", index + 2)
            return length if end_idx == -1 else end_idx + 2
        return None

    @staticmethod
    def _rewrite_placeholders_outside_literals(sql: str, source_token: str, target_token: str) -> str:
        out: list[str] = []
        n = len(sql)
        i = 0
        src_len = len(source_token)
        while i < n:
            next_i = MySQLCore._consume_literal_or_comment(sql, i, n)
            if next_i is not None:
                out.append(sql[i:next_i])
                i = next_i
                continue
            if sql.startswith(source_token, i):
                out.append(target_token)
                i += src_len
                continue
            out.append(sql[i])
            i += 1
        return "".join(out)

    @staticmethod
    def _compile_named_mysql_binds(
        sql: str,
        params: Mapping[str, Any],
        placeholder: str,
    ) -> tuple[str, list[Any]]:
        out: list[str] = []
        values: list[Any] = []
        n = len(sql)
        i = 0
        while i < n:
            next_i = MySQLCore._consume_literal_or_comment(sql, i, n)
            if next_i is not None:
                out.append(sql[i:next_i])
                i = next_i
                continue
            ch = sql[i]
            if ch == "%" and i + 2 < n and sql[i + 1] == "(":
                close_idx = sql.find(")s", i + 2)
                if close_idx != -1:
                    name = sql[i + 2 : close_idx]
                    if name not in params:
                        msg = f"Missing MySQL bind parameter {name!r}"
                        raise ProgrammingError(msg)
                    values.append(params[name])
                    out.append(placeholder)
                    i = close_idx + 2
                    continue
            if ch == ":" and i + 1 < n and (sql[i + 1].isalpha() or sql[i + 1] == "_"):
                if i > 0 and sql[i - 1] == ":":
                    out.append(ch)
                    i += 1
                    continue
                j = i + 2
                while j < n and (sql[j].isalnum() or sql[j] == "_"):
                    j += 1
                name = sql[i + 1 : j]
                if name not in params:
                    msg = f"Missing MySQL bind parameter {name!r}"
                    raise ProgrammingError(msg)
                values.append(params[name])
                out.append(placeholder)
                i = j
                continue
            out.append(ch)
            i += 1
        return "".join(out), values


def coerce_mysql_param(value: Any) -> Any:
    """Coerce a Python bind value into a MySQL-compatible parameter value."""
    return MySQLCore.coerce_param(value)


def compile_mysql_statement(
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    *,
    target_style: str = "qmark",
) -> tuple[str, tuple[Any, ...]]:
    """Compile a SQL statement into positional parameters."""
    return MySQLCore.compile_statement(sql, parameters, target_style=target_style)


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
