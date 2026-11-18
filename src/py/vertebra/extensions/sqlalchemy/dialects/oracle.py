"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Oracle (+vtb / +vertebra)."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import exc
from sqlalchemy.dialects.oracle.base import OracleDialect as BaseOracleDialect
from sqlalchemy.dialects.oracle.base import OracleExecutionContext
from sqlalchemy.engine import cursor as _cursor
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.types import Boolean

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import _DBAPICursorDescription

from vertebra.extensions.sqlalchemy.dialects._base import (
    _AsyncDialectMixin,
    _extract_sqlstate,
    _extract_vendor_code,
    _is_disconnect_common,
    _SyncDialectMixin,
)
from vertebra.extensions.sqlalchemy.types import MsgspecJSON, OracleVector

__all__ = (
    "AsyncOracleDialect",
    "OracleExecutionContext_vtb",
    "SyncOracleDialect",
)

_ORACLE_VERSION_QUERY = "SELECT version FROM product_component_version WHERE product LIKE 'Oracle%'"
_ORACLE_DISCONNECT_VENDOR_CODES: frozenset[int] = frozenset({28, 1012, 1033, 1089, 3113, 3114, 3135, 12537})
_ORACLE_NON_DISCONNECT_SQLSTATES: frozenset[str] = frozenset({"23505", "40001", "40P01"})
_ORACLE_DISCONNECT_RE = re.compile(r"\bORA-(?:00028|01012|01033|01089|03113|03114|03135|12537)\b", re.IGNORECASE)
_ORACLE_DISCONNECT_PHRASES: tuple[str, ...] = (
    "ora-03113",
    "ora-03114",
    "ora-01012",
    "not connected",
)


def _parse_oracle_version(raw: Any) -> tuple[int, ...] | None:
    """Parse an Oracle version tuple or version string into integer components."""
    if raw is None:
        return None
    if isinstance(raw, (tuple, list)):
        parts: list[int] = []
        for item in raw:
            if isinstance(item, int) and not isinstance(item, bool):
                parts.append(item)
            elif isinstance(item, str) and item.strip().isdigit():
                parts.append(int(item.strip()))
            else:
                return None
        return tuple(parts) if parts else None
    text = str(raw).strip()
    if not text:
        return None
    match = re.match(r"^\s*(\d+(?:\.\d+)*)", text)
    if match is None:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def _extract_out_var_values(var: Any, *, rowcount: int) -> list[Any]:
    """Extract scalar or nested RETURNING values from a cursor.var() bind placeholder."""
    outconverter = getattr(var, "outconverter", None)
    raw_values = getattr(var, "values", None)
    if isinstance(raw_values, (list, tuple)):
        if any(isinstance(entry, (list, tuple)) for entry in raw_values):
            extracted: list[Any] = []
            for stmt_result in raw_values:
                if isinstance(stmt_result, (list, tuple)):
                    for val in stmt_result:
                        extracted.append(outconverter(val) if callable(outconverter) and val is not None else val)
                elif stmt_result is not None or rowcount != 0:
                    extracted.append(
                        outconverter(stmt_result) if callable(outconverter) and stmt_result is not None else stmt_result
                    )
            return extracted
        if rowcount == 0 and all(val is None for val in raw_values):
            return []
        getvalue = getattr(var, "getvalue", None)
        if callable(getvalue):
            return [getvalue(idx) for idx in range(len(raw_values))]
        return [outconverter(val) if callable(outconverter) and val is not None else val for val in raw_values]

    getvalue = getattr(var, "getvalue", None)
    if callable(getvalue):
        if rowcount == 0:
            return []
        arraysize = max(int(getattr(var, "arraysize", 1) or 1), 1)
        return [getvalue(idx) for idx in range(arraysize)]
    return []


class OracleExecutionContext_vtb(OracleExecutionContext):
    """Execution context for Vertebra Oracle dialects supporting RETURNING ... INTO out binds."""

    out_parameters: dict[str, Any] | None = None

    def pre_exec(self) -> None:
        """Prepare cursor.var() placeholders before executing Oracle DML with RETURNING or outparams."""
        super_pre_exec: Callable[[], None] = super().pre_exec
        super_pre_exec()
        if not isinstance(self.compiled, SQLCompiler):
            return
        if not (self.compiled.has_out_parameters or getattr(self.compiled, "_oracle_returning", False)):
            return

        self.out_parameters = {}
        len_params = max(len(self.parameters), 1)
        quoted_bind_names = self.compiled.escaped_bind_names
        dbapi = self.dialect.dbapi

        for bindparam in self.compiled.binds.values():
            if not bindparam.isoutparam:
                continue
            name = self.compiled.bind_names[bindparam]
            type_impl = bindparam.type.dialect_impl(self.dialect)
            dbtype = type_impl.get_dbapi_type(dbapi) if dbapi is not None else None
            if isinstance(type_impl, Boolean) and not self.dialect.supports_native_boolean:
                var = self.cursor.var(
                    getattr(dbapi, "NUMBER", int) if dbapi is not None else int,
                    arraysize=len_params,
                    outconverter=bool,
                )
            else:
                if dbtype is None:
                    dbtype = getattr(dbapi, "STRING", str) if dbapi is not None else str
                var = self.cursor.var(dbtype, arraysize=len_params)
            self.out_parameters[name] = var
            target_key = quoted_bind_names.get(name, name)
            for param in self.parameters:
                if isinstance(param, dict):
                    param[target_key] = var

    def post_exec(self) -> None:
        """Materialize RETURNING ... INTO out-bind values into a buffered fetch strategy."""
        if (
            self.compiled is not None
            and isinstance(self.compiled, SQLCompiler)
            and getattr(self.compiled, "_oracle_returning", False)
            and self.out_parameters
        ):
            initial_buffer = self.fetchall_for_returning(self.cursor, _internal=True)
            alternate_desc = cast(
                "_DBAPICursorDescription",
                [(entry.keyname, None) for entry in self.compiled._result_columns],
            )
            self.cursor_fetch_strategy = _cursor.FullyBufferedCursorFetchStrategy(
                self.cursor,
                alternate_desc,
                initial_buffer=initial_buffer,
            )

    def fetchall_for_returning(self, cursor: Any, *, _internal: bool = False) -> list[tuple[Any, ...]]:
        """Materialize returned rows from RETURNING ... INTO cursor.var() placeholders."""
        _ = cursor
        if not _internal and (
            self.compiled is None
            or not isinstance(self.compiled, SQLCompiler)
            or not getattr(self.compiled, "_oracle_returning", False)
        ):
            msg = "execution context was not prepared for Oracle RETURNING"
            raise NotImplementedError(msg)
        if not self.out_parameters:
            return []

        rowcount = getattr(self.cursor, "rowcount", -1)
        ordered = list(self.out_parameters.values())
        columns = [
            _extract_out_var_values(
                self.out_parameters.get(f"ret_{idx}", ordered[idx]),
                rowcount=rowcount,
            )
            for idx in range(len(self.out_parameters))
        ]
        return list(zip(*columns, strict=True))

    def get_out_parameter_values(self, names: Sequence[str]) -> list[Any]:
        """Return scalar values for explicit outparam() binds after execution."""
        if not self.out_parameters:
            return []
        results: list[Any] = []
        for name in names:
            var = self.out_parameters[name]
            getvalue = getattr(var, "getvalue", None)
            if callable(getvalue):
                results.append(getvalue(0))
            else:
                vals = getattr(var, "values", None)
                results.append(vals[0] if vals else None)
        return results


class _OracleDialectMixin:
    """Shared SQLAlchemy 2.1 dialect behavior for Oracle (+vtb / +vtb_async)."""

    supports_statement_cache = True
    default_paramstyle = "named"
    paramstyle = "named"

    ischema_names = dict(BaseOracleDialect.ischema_names)
    ischema_names.update(
        {
            "JSON": MsgspecJSON,
            "VECTOR": OracleVector,
        }
    )

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Resolve Oracle server_version_info from connection metadata or product_component_version."""
        raw_conn = getattr(getattr(connection, "connection", None), "dbapi_connection", None)
        if raw_conn is not None:
            get_driver_fn = getattr(self, "get_driver_connection", None)
            driver_conn = get_driver_fn(raw_conn) if callable(get_driver_fn) else raw_conn
            candidates = (
                raw_conn,
                driver_conn,
                getattr(driver_conn, "driver", None),
                getattr(driver_conn, "_driver", None),
            )
            for candidate in candidates:
                if candidate is None:
                    continue
                parsed = _parse_oracle_version(getattr(candidate, "server_version_info", None))
                if parsed is not None:
                    return parsed

        raw_version = connection.exec_driver_sql(_ORACLE_VERSION_QUERY).scalar()
        parsed = _parse_oracle_version(raw_version)
        if parsed is None:
            raise exc.DBAPIError(
                _ORACLE_VERSION_QUERY,
                None,
                ValueError(f"Could not determine Oracle server version from {raw_version!r}"),
            )
        return parsed

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True when the exception matches an Oracle disconnect code or SQLSTATE 08*."""
        try:
            sqlstate = _extract_sqlstate(e)
            if sqlstate is not None:
                if sqlstate in _ORACLE_NON_DISCONNECT_SQLSTATES:
                    return False
                if sqlstate.startswith("08"):
                    return True

            vendor_code = _extract_vendor_code(e)
            if vendor_code == 1:
                return False
            if vendor_code in _ORACLE_DISCONNECT_VENDOR_CODES:
                return True

            for candidate in (e, getattr(e, "orig", None), getattr(e, "__cause__", None)):
                if candidate is None:
                    continue
                msg = str(candidate).lower()
                if "ora-00001" in msg:
                    return False
                if any(phrase in msg for phrase in _ORACLE_DISCONNECT_PHRASES) or _ORACLE_DISCONNECT_RE.search(msg):
                    return True

            parent_fn = getattr(super(), "is_disconnect", None)
            return _is_disconnect_common(e, connection, cursor, parent_fn)
        except Exception:
            return False


class SyncOracleDialect(_OracleDialectMixin, _SyncDialectMixin, BaseOracleDialect):
    """SQLAlchemy 2.1 synchronous dialect for Oracle (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    execution_ctx_cls = OracleExecutionContext_vtb


class AsyncOracleDialect(_OracleDialectMixin, _AsyncDialectMixin, BaseOracleDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Oracle (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    execution_ctx_cls = OracleExecutionContext_vtb
