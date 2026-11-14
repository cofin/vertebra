"""Synchronous native MySQL 8.4/9 and MariaDB driver backed by `vertebra._mysql`."""

from __future__ import annotations

import datetime
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, ClassVar, Self

from vertebra.db.adapters.mysql.config import (
    MySQLConfig,
    MySQLPoolConfig,
    MySQLStatementConfig,
)
from vertebra.db.adapters.mysql.core import (
    build_mysql_connect_kwargs,
    coerce_mysql_param,
    compile_mysql_statement,
    map_mysql_exception,
)
from vertebra.db.adapters.mysql.pipeline import SyncMySQLPipeline
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver import (
    DatabaseError,
    NotFoundError,
    OperationalError,
    SyncDatabaseDriver,
)
from vertebra.db.driver._common import MYSQL_DIALECT_TAG
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.schema import to_value_type

_mysql_ext = import_optional("vertebra._mysql")

__all__ = (
    "MySQLConfig",
    "MySQLPoolConfig",
    "MySQLStatementConfig",
    "SyncMySQLDriver",
    "SyncMySQLPipeline",
)


def _load_mysql_module() -> Any:
    """Resolve `vertebra._mysql` or raise `MissingDependencyError` with install hint."""
    if _mysql_ext is not None:
        return _mysql_ext
    return ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")


def _coerce_scalar_value(val: Any, value_type: type[Any] | None) -> Any:
    """Coerce a scalar value to `value_type`, normalizing naive `datetime` values to UTC."""
    if value_type is None or val is None:
        return val
    if value_type is datetime.datetime and isinstance(val, datetime.datetime) and val.tzinfo is None:
        return val.replace(tzinfo=datetime.UTC)
    return to_value_type(val, value_type)


class SyncMySQLDriver(SyncDatabaseDriver[MySQLConfig]):
    """Synchronous MySQL 8.4/9 and MariaDB driver backed by `vertebra._mysql.SyncMySQLClient`."""

    dialect: ClassVar[str] = "mysql"
    dialect_tag: ClassVar[int] = MYSQL_DIALECT_TAG

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        client: Any = None,
        dialer: Any = None,
        broker: Any = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            config = MySQLConfig(**kwargs) if kwargs else MySQLConfig()
        elif kwargs:
            merged = {f: getattr(config, f) for f in config.__struct_fields__}
            merged.update(kwargs)
            config = MySQLConfig(**merged)

        super().__init__(config, broker=broker if broker is not None else config.broker)
        self._client: Any = client
        self._dialer: Any = dialer
        self._savepoint_seq: int = 0
        self._closed: bool = False

    @property
    def dialer(self) -> Any:
        """Return the optional Cloud SQL dialer attached to this driver."""
        return self._dialer

    @property
    def raw_client(self) -> Any:
        """Return the underlying `vertebra._mysql.SyncMySQLClient` instance."""
        return self._get_client()

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is active on this client."""
        if self._client is None:
            return False
        return bool(getattr(self._client, "in_transaction", False))

    @property
    def connection_id(self) -> int | None:
        """Return the active MySQL server `CONNECTION_ID()` when connected."""
        if self._client is None:
            return None
        cid = getattr(self._client, "connection_id", None)
        return int(cid) if cid is not None else None

    @property
    def lastrowid(self) -> int | None:
        """Return the `AUTO_INCREMENT` ID from the most recent `INSERT` statement."""
        if self._client is None:
            return None
        lid = getattr(self._client, "lastrowid", None)
        return int(lid) if lid is not None else None

    def statement_cache_len(self) -> int:
        """Return the number of binary prepared statements cached in the session LRU."""
        if self._client is None:
            return 0
        fn = getattr(self._client, "statement_cache_len", None)
        return int(fn()) if callable(fn) else 0

    def connect(self) -> Self:
        """Initialize the underlying native MySQL client and return `self`."""
        self._closed = False
        self._get_client()
        return self

    def close(self) -> None:
        """Close the underlying native MySQL pool and connections."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
            close_fn = getattr(self._client, "close", None)
            if callable(close_fn):
                close_fn()

    def begin(
        self,
        isolation_level: str | None = None,
        *,
        readonly: bool = False,
    ) -> None:
        """Start an explicit transaction on a pinned connection."""
        client = self._get_client()
        try:
            client.begin(isolation_level, readonly)
        except TypeError:
            client.begin()
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def commit(self) -> None:
        """Commit the active transaction and return the connection to the pool."""
        client = self._get_client()
        try:
            client.commit()
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def rollback(self) -> None:
        """Roll back the active transaction and return the connection to the pool."""
        client = self._get_client()
        try:
            client.rollback()
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def savepoint(self, name: str | None = None) -> str:
        """Create a `SAVEPOINT` inside the current transaction and return its identifier."""
        client = self._get_client()
        if name is None:
            self._savepoint_seq += 1
            name = f"vtb_sp_{self._savepoint_seq}"
        try:
            return str(client.savepoint(name))
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back changes to savepoint `name`."""
        client = self._get_client()
        try:
            client.rollback_to_savepoint(name)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def release_savepoint(self, name: str) -> None:
        """Release savepoint `name`."""
        client = self._get_client()
        try:
            client.release_savepoint(name)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    @contextmanager
    def transaction(
        self,
        isolation_level: str | None = None,
        *,
        readonly: bool = False,
    ) -> Iterator[Self]:
        """Context manager for transactions with automatic nested `SAVEPOINT` support."""
        self._get_client()
        if self.in_transaction:
            sp_name = self.savepoint()
            try:
                yield self
            except BaseException:
                self.rollback_to_savepoint(sp_name)
                self.release_savepoint(sp_name)
                raise
            else:
                self.release_savepoint(sp_name)
        else:
            self.begin(isolation_level=isolation_level, readonly=readonly)
            try:
                yield self
            except BaseException:
                self.rollback()
                raise
            else:
                self.commit()

    def pipeline(self) -> SyncMySQLPipeline:
        """Return a `SyncMySQLPipeline` context manager for batched transaction execution."""
        self._get_client()
        return SyncMySQLPipeline(self)

    def ping(self) -> bool:
        """Issue a MySQL `COM_PING` health check."""
        if self._closed:
            return False
        client = self._get_client()
        try:
            return bool(client.ping())
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def cancel_query(self, connection_id: int | None = None) -> bool:
        """Issue an out-of-band `KILL QUERY <conn_id>` and return whether a target was signalled."""
        if self._closed or self._client is None:
            return False
        try:
            if connection_id is not None and hasattr(self._client, "cancel_query"):
                return bool(self._client.cancel_query(int(connection_id)))
            return bool(self._client.cancel())
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def cancel(self, connection_id: int | None = None) -> None:
        """Issue an out-of-band `KILL QUERY <conn_id>` on a separate control socket."""
        self.cancel_query(connection_id=connection_id)

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            count = client.execute(compiled_sql, params if params else None)
            return int(count)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement repeatedly across `parameter_sets` and return total affected rows."""
        client = self._get_client()
        if not parameter_sets:
            return 0
        compiled_sets: list[tuple[Any, ...]] = []
        compiled_sql = sql
        for idx, param_set in enumerate(parameter_sets):
            norm_sql, params = self._compile(sql, param_set)
            if idx == 0:
                compiled_sql = norm_sql
            compiled_sets.append(params)
        try:
            count = client.execute_many(compiled_sql, compiled_sets)
            return int(count)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def execute_script(self, sql: str) -> int:
        """Execute a semicolon-delimited SQL script via `COM_QUERY` multi-statement mode."""
        client = self._get_client()
        try:
            if hasattr(client, "execute_script"):
                return int(client.execute_script(sql))
            total = 0
            for stmt in (part.strip() for part in sql.split(";") if part.strip()):
                total += self.execute(stmt)
            return total
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional row tuples."""
        rows, _cols = self.execute_with_description(sql, parameters, **kwargs)
        return rows

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a statement and return `(tuple_rows, column_names)` preserving empty-result columns."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            if hasattr(client, "execute_with_description"):
                raw = client.execute_with_description(compiled_sql, params if params else None)
                dict_rows, raw_cols = raw[0], raw[1]
                col_names = tuple(str(col[0]) if isinstance(col, (tuple, list)) else str(col) for col in raw_cols)
                if not col_names and dict_rows:
                    col_names = tuple(str(k) for k in dict_rows[0])
                tuple_rows = [
                    tuple(row.get(c) for c in col_names) if isinstance(row, Mapping) else tuple(row)
                    for row in dict_rows
                ]
                return tuple_rows, col_names
            rows = client.fetch_all(compiled_sql, params if params else None)
            if rows:
                col_names = tuple(str(k) for k in rows[0])
                tuple_rows = [tuple(row[c] for c in col_names) for row in rows]
                return tuple_rows, col_names
            return [], ()
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as `list[dict[str, Any]]` or hydrated `schema_type`."""
        if schema_type is not None:
            tuple_rows, cols = self.execute_with_description(sql, parameters, **kwargs)
            return self.hydrate_rows(tuple_rows, cols, schema_type)
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            rows = client.fetch_all(compiled_sql, params if params else None)
            return [dict(r) for r in rows]
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first row or raise `NotFoundError`."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            row = client.fetch_one(compiled_sql, params if params else None)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc
        if row is None:
            err = NotFoundError("Expected at least one row, got 0")
            err.sqlstate = "02000"
            raise err
        row_dict = dict(row)
        if schema_type is not None:
            cols = tuple(row_dict.keys())
            vals = (tuple(row_dict.values()),)
            return self.hydrate_rows(vals, cols, schema_type)[0]
        return row_dict

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a `SELECT` query and return the first row or `None` when empty."""
        try:
            return self.select_one(sql, parameters, schema_type=schema_type, **kwargs)
        except NotFoundError:
            return None

    def select_many(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        size: int = 100,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return at most `size` rows."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            if hasattr(client, "fetch_many"):
                rows = [dict(r) for r in client.fetch_many(compiled_sql, params if params else None, size)]
            else:
                rows = self.select(sql, parameters)[:size]
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc
        if schema_type is not None and rows:
            cols = tuple(rows[0].keys())
            tuple_rows = [tuple(r[c] for c in cols) for r in rows]
            return self.hydrate_rows(tuple_rows, cols, schema_type)
        return rows

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first scalar value or raise `NotFoundError`."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            if hasattr(client, "select_value"):
                val = client.select_value(compiled_sql, params if params else None)
            else:
                row = self.select_one(sql, parameters)
                val = next(iter(row.values()), None)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc
        return _coerce_scalar_value(val, value_type)

    def select_value_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first scalar value or `None` when empty."""
        try:
            return self.select_value(sql, parameters, value_type=value_type, **kwargs)
        except NotFoundError:
            return None

    def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute a `SELECT` query and return `(rows, total_count)`."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        cleaned = sql.strip().rstrip(";")
        upper = cleaned.upper()
        if " LIMIT " in upper:
            limit_idx = upper.rfind(" LIMIT ")
            base_sql = cleaned[:limit_idx]
            count_sql = f"SELECT COUNT(*) AS total FROM ({base_sql}) AS _vtb_count_subq"
            total_val = self.select_value(count_sql, parameters)
            return rows, int(total_val or 0)
        return rows, len(rows)

    def fetch_all_sets(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[list[dict[str, Any]]]:
        """Execute a multi-result query or `CALL` procedure and return all result sets."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            sets = client.fetch_all_sets(compiled_sql, params if params else None)
            return [[dict(r) for r in rs] for rs in sets]
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a `SELECT` query and materialize results into a `VTBARROW` `ArrowCursor`."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            native_cursor = client.select_to_arrow(
                compiled_sql,
                params if params else None,
                batch_size=batch_size,
            )
            if isinstance(native_cursor, ArrowCursor):
                return native_cursor
            return ArrowCursor(native_cursor, used_shm=True, shm_cursor=native_cursor)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[ArrowCursor]:
        """Stream a `SELECT` query as bounded `VTBARROW` `ArrowCursor` chunks."""
        client = self._get_client()
        compiled_sql, params = self._compile(sql, parameters)
        try:
            batches = client.stream_arrow_batches(
                compiled_sql,
                params if params else None,
                batch_size=batch_size,
            )
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc
        for batch in batches:
            if isinstance(batch, ArrowCursor):
                yield batch
            else:
                yield ArrowCursor(batch, used_shm=True, shm_cursor=batch)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        batch_size: int = 1000,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an `ArrowCursor`, PyArrow Table/RecordBatch, or `__arrow_c_stream__` into `table_name`."""
        client = self._get_client()
        source = data._shm_cursor if isinstance(data, ArrowCursor) and data._shm_cursor is not None else data
        try:
            inserted = client.load_from_arrow(
                table_name,
                source,
                columns=list(columns) if columns is not None else None,
                batch_size=batch_size,
            )
            return int(inserted)
        except BaseException as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise map_mysql_exception(exc) from exc

    def __repr__(self) -> str:
        """Return a redacted representation of the driver."""
        return (
            f"SyncMySQLDriver(host={self.config.host!r}, port={self.config.port}, "
            f"user={self.config.user!r}, database={self.config.database!r}, "
            f"ssl_mode={self.config.canonical_ssl_mode!r}, password='***')"
        )

    def _get_client(self) -> Any:
        if self._closed or (self._client is not None and bool(getattr(self._client, "is_closed", False))):
            msg = "SyncMySQLDriver is closed"
            raise OperationalError(msg)
        if self._client is None:
            mod = _load_mysql_module()
            connect_kwargs = build_mysql_connect_kwargs(self.config)
            self._client = mod.SyncMySQLClient(**connect_kwargs)
        return self._client

    def _compile(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        if not parameters:
            return sql, ()
        if isinstance(parameters, Mapping):
            return compile_mysql_statement(sql, parameters, target_style="qmark")
        return sql, tuple(coerce_mysql_param(v) for v in parameters)
