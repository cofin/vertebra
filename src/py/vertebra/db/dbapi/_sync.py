"""Standalone synchronous PEP 249 DB-API 2.0 connection, cursor, exceptions, and SQL classifier."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Final, Self, cast

import vertebra._vertebra as _native
from vertebra._vertebra import classify_sql_statement
from vertebra.db.driver._common import BaseDatabaseConfig
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


@contextlib.contextmanager
def _translate_errors() -> Iterator[None]:
    """Context manager catching native driver DatabaseError exceptions and re-raising PEP 249 errors."""
    try:
        yield
    except (Error, Warning):
        raise
    except _native.DatabaseError as exc:
        raise _translate_native_error(exc) from exc


def is_insert_sql(operation: str) -> bool:
    """Return True when the leading SQL verb in operation is INSERT."""
    return classify_sql_statement(operation)[1]


def is_read_only_sql(operation: str) -> bool:
    """Return True when the leading SQL verb is read-only query or introspection."""
    return classify_sql_statement(operation)[2]


def coerce_dbapi_rows_and_desc(
    raw_res: Any,
) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
    """Normalize driver query results into PEP 249 tuple rows and column descriptions."""
    if isinstance(raw_res, tuple) and len(raw_res) >= 2:
        raw_rows = raw_res[0]
        col_names = raw_res[1]
        desc = (
            tuple(
                (col[0] if isinstance(col, tuple) else str(col), None, None, None, None, None, None)
                for col in col_names
            )
            if col_names
            else None
        )
        return [tuple(r) if not isinstance(r, tuple) else r for r in raw_rows], desc
    return [], None


def is_row_returning_sql(operation: str) -> bool:
    """Return True when operation starts with a query keyword or has a standalone RETURNING clause."""
    return classify_sql_statement(operation)[0]


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
        self.lastrowid = None
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
        self.lastrowid = None
        with _translate_errors():
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
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to size plain-tuple rows."""
        self._require_open()
        batch_size = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + batch_size]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining plain-tuple rows."""
        self._require_open()
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
        last_rowid_fn = getattr(driver, "last_insert_rowid", None)
        if callable(last_rowid_fn):
            val = last_rowid_fn()
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
        url: str | None = None,
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        lease_managed: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize PEP 249 connection with a non-empty database URL."""
        _ = kwargs

        if url is None and driver is None:
            exc = ValueError("Unsupported database URL scheme: ''")
            raise InterfaceError(f"Cannot parse Vertebra database URL: {exc}") from exc

        self._url: str = ""
        self._config = url if isinstance(url, BaseDatabaseConfig) else None
        if isinstance(url, BaseDatabaseConfig):
            resolved_url = getattr(url, "dsn", getattr(url, "url", ""))
            if not resolved_url:
                dialect = getattr(url, "dialect", "sqlite")
                path_val = getattr(url, "path", "")
                resolved_url = f"{dialect}:///{path_val}" if path_val else f"{dialect}://"
            self._url = str(resolved_url)
        else:
            resolved_url = str(url) if url is not None else getattr(driver, "url", getattr(driver, "dsn", ""))
            self._url = str(resolved_url)

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
        if self._config is None and (resolved_url or driver is None):
            try:
                self._config = (
                    DatabaseRegistry.parse_url(resolved_url, broker=driver_broker)
                    if driver_broker is not None
                    else DatabaseRegistry.parse_url(resolved_url)
                )
            except ValueError as exc:
                msg = f"Cannot parse Vertebra database URL: {exc}"
                raise InterfaceError(msg) from exc

        self._autocommit = bool(getattr(self._config, "requires_statement_autocommit", False))
        self._driver = driver
        self._owns_driver = driver is not None

        if self._driver is None:
            has_shared_key = self._config is not None and self._config.pool_key() is not None
            has_custom_broker = broker is not None and broker is not DatabaseRegistry._DEFAULT_BROKER
            if self._share_pool and not lease_managed and has_shared_key and not has_custom_broker:
                self._driver = DatabaseRegistry.default().get_shared_sync_driver(cast("Any", self._config))
                self._owns_driver = False
            else:
                target_registry = DatabaseRegistry(broker=None if lease_managed else self._broker)
                self._driver = cast("Any", self._config).build_sync_driver(target_registry)
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
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a query returning rows and column descriptions."""
        self._require_open()
        self.begin()
        rows: list[tuple[Any, ...]] = []
        desc = None
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
                if hasattr(self._driver, "execute_dml"):
                    affected, rowid = self._driver.execute_dml(sql, parameters)
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
    Binary = Binary

    sqlite_version: str = "3.45.0"
    sqlite_version_info: tuple[int, ...] = (3, 45, 0)

    def __init__(self, *, paramstyle: str = "qmark") -> None:
        """Initialize PEP 249 DBAPI module with target parameter style."""
        self.paramstyle = paramstyle

    def connect(self, *args: Any, **kwargs: Any) -> SyncDBAPIConnection:
        """Open a synchronous PEP 249 connection with a required URL."""
        url_arg: str | None = None
        if args and isinstance(args[0], str):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        if (not url_arg or not url_arg.strip()) and "driver" not in kwargs:
            raise InterfaceError("A non-empty database connection URL is required.")
        return SyncDBAPIConnection(url_arg, **kwargs)


def connect(url: str | None = None, **kwargs: Any) -> SyncDBAPIConnection:
    """Open a standalone synchronous PEP 249 DB-API connection."""
    if (not url or not str(url).strip()) and "driver" not in kwargs:
        raise InterfaceError("A non-empty database connection URL is required.")
    return SyncDBAPIConnection(url, **kwargs)


def _config_dialect(config: Any) -> str:
    """Resolve the lowercase dialect identifier exposed by a database configuration object."""
    cls_dialect = getattr(type(config), "dialect", "")
    raw = getattr(config, "dialect", cls_dialect)
    return str(raw).lower()
