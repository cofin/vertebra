"""Standalone synchronous PEP 249 DB-API 2.0 connection, cursor, exceptions, and SQL classifier."""

from __future__ import annotations

import contextlib
import sqlite3
from typing import TYPE_CHECKING, Any, Final

from vertebra.db.registry import DatabaseRegistry

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "Binary",
    "DataError",
    "DatabaseError",
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


Binary = bytes


def is_row_returning_sql(operation: str) -> bool:
    """Return True when ``operation`` starts with a query keyword or has a standalone ``RETURNING`` clause."""
    length = len(operation)
    idx = 0
    first_keyword: str | None = None
    while idx < length:
        ch = operation[idx]
        if ch.isspace():
            idx += 1
            continue
        if ch == "-" and idx + 1 < length and operation[idx + 1] == "-":
            newline = operation.find("\n", idx + 2)
            idx = length if newline < 0 else newline + 1
            continue
        if ch == "/" and idx + 1 < length and operation[idx + 1] == "*":
            depth = 1
            idx += 2
            while idx < length and depth > 0:
                if operation[idx] == "/" and idx + 1 < length and operation[idx + 1] == "*":
                    depth += 1
                    idx += 2
                elif operation[idx] == "*" and idx + 1 < length and operation[idx + 1] == "/":
                    depth -= 1
                    idx += 2
                else:
                    idx += 1
            continue
        if ch == "$":
            tag = _parse_dollar_tag(operation, idx)
            if tag is not None:
                end_block = operation.find(tag, idx + len(tag))
                idx = length if end_block < 0 else end_block + len(tag)
                continue
        if ch in {"'", '"', "`"}:
            quote = ch
            idx += 1
            while idx < length:
                cur = operation[idx]
                if cur == quote:
                    if idx + 1 < length and operation[idx + 1] == quote:
                        idx += 2
                        continue
                    idx += 1
                    break
                if cur == "\\" and quote == "'":
                    idx += 2
                    continue
                idx += 1
            continue
        if ch.isalpha() or ch == "_":
            start = idx
            idx += 1
            while idx < length and (operation[idx].isalnum() or operation[idx] == "_"):
                idx += 1
            if start > 0 and operation[start - 1] in {":", "@", "$"}:
                continue
            token = operation[start:idx].upper()
            prev_sig_idx = start - 1
            while prev_sig_idx >= 0 and operation[prev_sig_idx].isspace():
                prev_sig_idx -= 1
            is_qualified = prev_sig_idx >= 0 and operation[prev_sig_idx] == "."
            next_sig_idx = idx
            while next_sig_idx < length and operation[next_sig_idx].isspace():
                next_sig_idx += 1
            is_assignment_target = next_sig_idx < length and operation[next_sig_idx] == "="
            if first_keyword is None:
                first_keyword = token
                if first_keyword in _ROW_RETURNING_LEADING_KEYWORDS:
                    return True
            elif token == "RETURNING" and not is_qualified and not is_assignment_target:
                return True
            continue
        idx += 1
    return False


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
        self.description: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.arraysize: int = 1

    def execute(self, operation: str, parameters: Sequence[Any] | dict[str, Any] = ()) -> SyncDBAPICursor:
        """Execute a SQL statement and buffer plain-tuple rows."""
        self._require_open()
        if self._raw_cursor is not None:
            self._raw_cursor.execute(operation, parameters)
            self.description = getattr(self._raw_cursor, "description", None)
            self.rowcount = getattr(self._raw_cursor, "rowcount", -1)
            self.lastrowid = getattr(self._raw_cursor, "lastrowid", None)
            return self

        if not is_row_returning_sql(operation):
            affected = self._conn.execute_dml(operation, parameters)
            self._rows = []
            self.description = None
            self._pos = 0
            self.rowcount = affected
            self.lastrowid = self._resolve_driver_lastrowid(operation)
            return self

        self._rows, self.description = self._conn.execute_with_description(operation, parameters)
        self._pos = 0
        self.rowcount = len(self._rows)
        self.lastrowid = self._resolve_driver_lastrowid(operation)
        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> SyncDBAPICursor:
        """Execute a parameterized SQL statement across a sequence of parameter sets."""
        self._require_open()
        if self._raw_cursor is not None:
            self._raw_cursor.executemany(operation, seq_of_parameters)
            self.description = getattr(self._raw_cursor, "description", None)
            self.rowcount = getattr(self._raw_cursor, "rowcount", -1)
            self.lastrowid = getattr(self._raw_cursor, "lastrowid", None)
            return self

        if not is_row_returning_sql(operation):
            affected = self._conn.execute_many_dml(operation, seq_of_parameters)
            self._rows = []
            self.description = None
            self._pos = 0
            self.rowcount = affected
            return self

        total = 0
        last_rows: list[tuple[Any, ...]] = []
        last_desc: tuple[tuple[str, Any, None, None, None, None, None], ...] | None = None
        for params in seq_of_parameters:
            rows, desc = self._conn.execute_with_description(operation, params)
            total += len(rows)
            last_rows = rows
            last_desc = desc
        self._rows = last_rows
        self.description = last_desc
        self._pos = 0
        self.rowcount = total
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next plain-tuple row or None."""
        self._require_open()
        if self._raw_cursor is not None:
            row = self._raw_cursor.fetchone()
            return tuple(row) if row is not None and not isinstance(row, tuple) else row
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` plain-tuple rows."""
        self._require_open()
        if self._raw_cursor is not None:
            rows = self._raw_cursor.fetchmany(size if size is not None else self.arraysize)
            return [tuple(r) if not isinstance(r, tuple) else r for r in rows]
        batch_size = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + batch_size]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining plain-tuple rows."""
        self._require_open()
        if self._raw_cursor is not None:
            rows = self._raw_cursor.fetchall()
            return [tuple(r) if not isinstance(r, tuple) else r for r in rows]
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

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
            if self._raw_cursor is not None and hasattr(self._raw_cursor, "close"):
                self._raw_cursor.close()
            self._rows = []
            self._pos = 0
            self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor.")
        if self._conn.closed:
            raise InterfaceError("Cannot operate on a closed connection.")

    def _resolve_driver_lastrowid(self, operation: str) -> int | None:
        if not operation.lstrip().upper().startswith("INSERT"):
            return self.lastrowid
        driver = self._conn.driver
        last_rowid_fn = getattr(driver, "last_insert_rowid", None)
        if callable(last_rowid_fn):
            val = last_rowid_fn()
            return int(val) if val is not None else 1
        raw_conn = getattr(driver, "raw_connection", None)
        if raw_conn is not None:
            driver_lock = getattr(driver, "_lock", None)
            ctx = driver_lock if driver_lock is not None else contextlib.nullcontext()
            with ctx, contextlib.closing(raw_conn.cursor()) as cur:
                cur.execute("SELECT last_insert_rowid()")
                row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 1
        return self.lastrowid


class SyncDBAPIConnection:
    """Standalone synchronous PEP 249 connection backed by Vertebra native drivers or sqlite3."""

    __slots__ = (
        "_autocommit",
        "_broker",
        "_config",
        "_driver",
        "_in_transaction",
        "_owns_driver",
        "_share_pool",
        "_sqlite_conn",
        "_url",
        "closed",
    )

    def __init__(
        self,
        url: str = "sqlite+vtb:///:memory:",
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        lease_managed: bool = False,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize PEP 249 connection with URL or database parameters."""
        _ = kwargs

        resolved_url = url
        if database is not None and (url == "sqlite+vtb:///:memory:" or not url):
            resolved_url = f"sqlite+vtb:///{database}"
        self._url = resolved_url
        self._share_pool = bool(share_pool)
        self._broker = (
            broker
            if broker is not None
            else (DatabaseRegistry.default_broker() if self._share_pool and not lease_managed else None)
        )
        self._in_transaction = False
        self.closed: bool = False

        self._config: Any = None
        driver_broker = None if lease_managed else self._broker
        try:
            self._config = (
                DatabaseRegistry.parse_url(resolved_url, broker=driver_broker)
                if driver_broker is not None
                else DatabaseRegistry.parse_url(resolved_url)
            )
        except ValueError as exc:
            msg = f"Cannot parse Vertebra database URL: {exc}"
            raise InterfaceError(msg) from exc

        dialect_name = _config_dialect(self._config)
        self._autocommit = dialect_name in _AUTOCOMMIT_ONLY_DIALECTS
        self._driver = driver
        self._owns_driver = driver is not None
        self._sqlite_conn: sqlite3.Connection | None = None

        if self._driver is None:
            if dialect_name == "sqlite" and self._broker is None:
                db_target = str(getattr(self._config, "database", None) or ":memory:")
                if db_target.startswith("sqlite+"):
                    db_target = db_target.split(":///", 1)[-1] if ":///" in db_target else ":memory:"
                self._sqlite_conn = sqlite3.connect(db_target, check_same_thread=False)
            elif self._share_pool and broker is None and not lease_managed:
                self._driver = DatabaseRegistry.default().get_shared_sync_driver(self._config)
            else:
                target_registry = DatabaseRegistry(broker=None if lease_managed else self._broker)
                self._driver = self._config.build_sync_driver(target_registry)
                self._owns_driver = True

    @property
    def url(self) -> str:
        """Return the connection URL."""
        return self._url

    @property
    def config(self) -> Any:
        """Return the parsed database configuration."""
        return self._config

    @property
    def broker(self) -> Any:
        """Return the attached shared-memory database broker, if any."""
        return self._broker

    @property
    def driver(self) -> Any:
        """Return the active underlying driver instance."""
        return self._driver

    @property
    def autocommit(self) -> bool:
        """Return connection autocommit state."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit, rejecting transactional DBAPI mode for autocommit-only backends."""
        if not value and _config_dialect(self._config) in _AUTOCOMMIT_ONLY_DIALECTS:
            msg = "BigQuery, Spanner, and Spangres DBAPI connections require autocommit; use the native driver transaction API"
            raise NotSupportedError(msg)
        if value and self._in_transaction:
            self.commit()
        self._autocommit = value
        if self._sqlite_conn is not None:
            self._sqlite_conn.autocommit = value

    def cursor(self) -> SyncDBAPICursor:
        """Create a new plain-tuple DBAPI cursor."""
        self._require_open()
        if self._sqlite_conn is not None:
            return SyncDBAPICursor(self, raw_cursor=self._sqlite_conn.cursor())
        return SyncDBAPICursor(self)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a query returning rows and column descriptions."""
        self._require_open()
        if self._sqlite_conn is not None:
            with contextlib.closing(self._sqlite_conn.cursor()) as cur:
                cur.execute(sql, parameters)
                rows = [tuple(r) for r in cur.fetchall()]
                desc = cur.description
            return rows, desc

        self.begin()
        if self._driver is not None:
            if hasattr(self._driver, "execute_with_description"):
                raw_res = self._driver.execute_with_description(sql, parameters)
                if isinstance(raw_res, tuple) and len(raw_res) >= 2:
                    col_names = raw_res[1]
                    desc = (
                        tuple(
                            (col[0] if isinstance(col, tuple) else str(col), None, None, None, None, None, None)
                            for col in col_names
                        )
                        if col_names
                        else None
                    )
                    if self._autocommit:
                        self.commit()
                    return raw_res[0], desc
                return [], None
            if hasattr(self._driver, "execute_tuples"):
                rows = self._driver.execute_tuples(sql, parameters)
                desc = (
                    tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(rows[0])))
                    if rows
                    else None
                )
                if self._autocommit:
                    self.commit()
                return rows, desc
            if hasattr(self._driver, "select"):
                select_params = parameters if parameters else None
                dict_rows = self._driver.select(sql, select_params)
                if not dict_rows:
                    return [], None
                keys = tuple(dict_rows[0].keys())
                desc = tuple((str(k), None, None, None, None, None, None) for k in keys)
                rows = [tuple(r.get(k) for k in keys) for r in dict_rows]
                if self._autocommit:
                    self.commit()
                return rows, desc
        return [], None

    def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> int:
        """Execute a non-row-returning SQL statement and return the affected row count."""
        self._require_open()
        if self._sqlite_conn is not None:
            with contextlib.closing(self._sqlite_conn.cursor()) as cur:
                cur.execute(sql, parameters)
                return cur.rowcount
        self.begin()
        if self._driver is not None:
            if hasattr(self._driver, "execute"):
                affected = int(self._driver.execute(sql, parameters))
                if self._autocommit:
                    self.commit()
                return affected
            rows, _ = self.execute_with_description(sql, parameters)
            return len(rows)
        return -1

    def execute_many_dml(
        self,
        sql: str,
        seq_of_parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> int:
        """Execute a non-row-returning SQL statement across multiple parameter sets and return affected rows."""
        self._require_open()
        if not seq_of_parameters:
            return 0
        if self._sqlite_conn is not None:
            with contextlib.closing(self._sqlite_conn.cursor()) as cur:
                cur.executemany(sql, seq_of_parameters)
                return cur.rowcount
        self.begin()
        if self._driver is not None:
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
        if self._autocommit or self._in_transaction or self._sqlite_conn is not None:
            return
        if self._driver is not None and hasattr(self._driver, "begin"):
            self._driver.begin()
            self._in_transaction = True

    def commit(self) -> None:
        """Commit the active transaction."""
        self._require_open()
        self._in_transaction = False
        if self._sqlite_conn is not None:
            self._sqlite_conn.commit()
        elif self._driver is not None and hasattr(self._driver, "commit"):
            self._driver.commit()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        self._require_open()
        self._in_transaction = False
        if self._sqlite_conn is not None:
            self._sqlite_conn.rollback()
        elif self._driver is not None and hasattr(self._driver, "rollback"):
            self._driver.rollback()

    def close(self) -> None:
        """Close the underlying driver connection."""
        if not self.closed:
            if self._in_transaction:
                if self._autocommit:
                    self.commit()
                else:
                    self.rollback()
            if self._sqlite_conn is not None:
                self._sqlite_conn.close()
            if self._owns_driver and self._driver is not None and hasattr(self._driver, "close"):
                self._driver.close()
            self.closed = True

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
    Binary = Binary

    sqlite_version: str = sqlite3.sqlite_version
    sqlite_version_info: tuple[int, ...] = sqlite3.sqlite_version_info

    def __init__(self, *, paramstyle: str = "qmark") -> None:
        """Initialize PEP 249 DBAPI module with target parameter style."""
        self.paramstyle = paramstyle

    def connect(self, *args: Any, **kwargs: Any) -> SyncDBAPIConnection:
        """Open a synchronous PEP 249 connection."""
        url_arg = "sqlite+vtb:///:memory:"
        if args and isinstance(args[0], str):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        return SyncDBAPIConnection(url_arg, **kwargs)


def connect(url: str = "sqlite+vtb:///:memory:", **kwargs: Any) -> SyncDBAPIConnection:
    """Open a standalone synchronous PEP 249 DB-API connection."""
    return SyncDBAPIConnection(url, **kwargs)


_AUTOCOMMIT_ONLY_DIALECTS: frozenset[str] = frozenset({"bigquery", "googlesql", "spangres", "spanner"})

_ROW_RETURNING_LEADING_KEYWORDS: frozenset[str] = frozenset(
    {
        "SELECT",
        "WITH",
        "PRAGMA",
        "SHOW",
        "DESCRIBE",
        "DESC",
        "EXPLAIN",
        "VALUES",
    }
)


def _config_dialect(config: Any) -> str:
    """Resolve the lowercase dialect identifier exposed by a database configuration object."""
    cls_dialect = getattr(type(config), "dialect", "")
    raw = getattr(config, "dialect", cls_dialect)
    return str(raw).lower()


def _parse_dollar_tag(sql: str, start: int) -> str | None:
    """Parse a PostgreSQL dollar-quote delimiter starting at ``$`` or return None."""
    length = len(sql)
    idx = start + 1
    if idx < length and sql[idx] == "$":
        return "$$"
    while idx < length:
        ch = sql[idx]
        if ch == "$":
            return sql[start : idx + 1] if idx > start + 1 else None
        if not (ch.isalnum() or ch == "_"):
            return None
        if idx == start + 1 and ch.isdigit():
            return None
        idx += 1
    return None
