"""Synchronous native MySQL 8.4/9 and MariaDB driver backed by `vertebra._mysql`."""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Self

import msgspec

from vertebra.db.adapters.mysql.config import (
    MySQLConfig,
    build_mysql_client_kwargs,
    is_mysql_auth_rejection,
)
from vertebra.db.adapters.mysql.core import MySQLCore
from vertebra.db.adapters.mysql.pipeline import SyncMySQLPipeline
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver import (
    AZURE_MYSQL_SCOPE,
    AzureEntraTokenProvider,
    DatabaseError,
    NotFoundError,
    OperationalError,
    SyncDatabaseDriver,
)
from vertebra.db.driver._common import MYSQL_DIALECT_TAG

if TYPE_CHECKING:
    from vertebra._mysql import SyncMySQLCursor

__all__ = (
    "MySQLConfig",
    "SyncMySQLDriver",
    "SyncMySQLPipeline",
)


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

        if isinstance(config.token_provider, AzureEntraTokenProvider):
            config = msgspec.structs.replace(
                config,
                token_provider=config.token_provider.with_default_scope(AZURE_MYSQL_SCOPE),
            )

        super().__init__(config, broker=broker)
        self._client: Any = client
        self._owns_client: bool = client is None
        self._client_token: str | None = None
        self._dialer: Any = dialer
        self._savepoint_seq: int = 0
        self._lastrowid: int | None = None
        self._has_broker: bool = self._broker is not None

    def __repr__(self) -> str:
        """Return a redacted representation of the driver."""
        provider_name = type(self.config.token_provider).__name__ if self.config.token_provider is not None else None
        return (
            f"SyncMySQLDriver(host={self.config.host!r}, port={self.config.port}, "
            f"user={self.config.user!r}, database={self.config.database!r}, "
            f"ssl_mode={self.config.canonical_ssl_mode!r}, password='***', "
            f"token_provider={provider_name!r})"
        )

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
        return self._lastrowid

    def cursor(self, *, lazy_begin: bool = False) -> SyncMySQLCursor:
        """Return a native `SyncMySQLCursor` bound to this driver's client."""
        self._ensure_open()
        cur: SyncMySQLCursor = self._get_client().cursor(lazy_begin=lazy_begin)
        return cur

    def statement_cache_len(self) -> int:
        """Return the number of binary prepared statements cached in the session LRU."""
        if self._client is None:
            return 0
        fn: Callable[[], Any] | None = getattr(self._client, "statement_cache_len", None)
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
        client = self._client
        if self._owns_client or (client is not None and bool(getattr(client, "is_closed", False))):
            self._client = None
            self._client_token = None
        if client is not None:
            if self._has_broker:
                release_fn: Callable[[], Any] | None = getattr(client, "release_lease", None)
                if callable(release_fn):
                    release_fn()
            close_fn: Callable[[], Any] | None = getattr(client, "close", None)
            if callable(close_fn):
                close_fn()

    def begin(
        self,
        isolation_level: str | None = None,
        *,
        readonly: bool = False,
    ) -> None:
        """Start an explicit transaction on a pinned connection."""

        def _op(client: Any) -> None:
            try:
                client.begin(isolation_level, readonly)
            except TypeError:
                client.begin()

        self._run_with_client(_op)

    def commit(self) -> None:
        """Commit the active transaction and return the connection to the pool."""
        self._run_with_client(lambda client: client.commit())

    def rollback(self) -> None:
        """Roll back the active transaction and return the connection to the pool."""
        self._run_with_client(lambda client: client.rollback())

    def savepoint(self, name: str | None = None) -> str:
        """Create a `SAVEPOINT` inside the current transaction and return its identifier."""
        self._ensure_open()
        if name is None:
            self._savepoint_seq += 1
            name = f"vtb_sp_{self._savepoint_seq}"
        return str(self._run_with_client(lambda client: client.savepoint(name)))

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back changes to savepoint `name`."""
        self._run_with_client(lambda client: client.rollback_to_savepoint(name))

    def release_savepoint(self, name: str) -> None:
        """Release savepoint `name`."""
        self._run_with_client(lambda client: client.release_savepoint(name))

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
        return bool(self._run_with_client(lambda client: client.ping()))

    def cancel_query(self, connection_id: int | None = None) -> bool:
        """Issue an out-of-band `KILL QUERY <conn_id>` and return whether a target was signalled."""
        if self._closed or self._client is None:
            return False
        try:
            if connection_id is not None and hasattr(self._client, "cancel_query"):
                return bool(self._client.cancel_query(int(connection_id)))
            return bool(self._client.cancel())
        except Exception as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise MySQLCore.map_exception(exc) from exc

    def cancel(self, connection_id: int | None = None) -> None:
        """Issue an out-of-band `KILL QUERY <conn_id>` on a separate control socket."""
        self.cancel_query(connection_id=connection_id)

    def _execute_cursor(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Any:
        self._ensure_open()
        bound = parameters or ()
        cur = self._run_with_client(
            lambda client: client.cursor().execute(sql, bound),
            map_errors=False,
        )
        self._lastrowid = cur.lastrowid
        return cur

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        cur = self._execute_cursor(sql, parameters)
        return max(int(cur.rowcount), 0)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement repeatedly across `parameter_sets` and return total affected rows."""
        self._ensure_open()
        if not parameter_sets:
            return 0
        cur = self._run_with_client(
            lambda client: client.cursor().executemany(sql, parameter_sets),
            map_errors=False,
        )
        self._lastrowid = cur.lastrowid
        return max(int(cur.rowcount), 0)

    def execute_script(self, sql: str) -> int:
        """Execute a semicolon-delimited SQL script via `COM_QUERY` multi-statement mode."""
        client = self._get_client()
        if hasattr(client, "execute_script"):
            return int(self._run_with_client(lambda c: c.execute_script(sql)))
        try:
            total = 0
            for stmt in (part.strip() for part in sql.split(";") if part.strip()):
                total += self.execute(stmt)
            return total
        except Exception as exc:
            if isinstance(exc, DatabaseError):
                raise
            raise MySQLCore.map_exception(exc) from exc

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional row tuples."""
        cur = self._execute_cursor(sql, parameters)
        if cur.description is None:
            return []
        rows: list[tuple[Any, ...]] = cur.fetchall()
        return rows

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a statement and return `(tuple_rows, column_names)` preserving empty-result columns."""
        cur = self._execute_cursor(sql, parameters)
        description = cur.description
        if description is None:
            return [], ()
        names = tuple(col[0] for col in description)
        rows: list[tuple[Any, ...]] = cur.fetchall()
        return rows, names

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as `list[dict[str, Any]]` or hydrated `schema_type`."""
        rows, names = self.execute_with_description(sql, parameters, **kwargs)
        return self.hydrate_rows(rows, names, schema_type)

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first row or raise `NotFoundError`."""
        cur = self._execute_cursor(sql, parameters)
        description = cur.description
        row = cur.fetchone() if description is not None else None
        if row is None or description is None:
            err = NotFoundError("Expected at least one row, got 0")
            err.sqlstate = "02000"
            raise err
        names = tuple(col[0] for col in description)
        return self.hydrate_rows((row,), names, schema_type)[0]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a `SELECT` query and return the first row or `None` when empty."""
        cur = self._execute_cursor(sql, parameters)
        description = cur.description
        row = cur.fetchone() if description is not None else None
        if row is None or description is None:
            return None
        names = tuple(col[0] for col in description)
        return self.hydrate_rows((row,), names, schema_type)[0]

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
        cur = self._execute_cursor(sql, parameters)
        description = cur.description
        if description is None:
            return []
        rows = cur.fetchmany(size)
        names = tuple(col[0] for col in description)
        return self.hydrate_rows(rows, names, schema_type)

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first scalar value or raise `NotFoundError`."""
        cur = self._execute_cursor(sql, parameters)
        row = cur.fetchone() if cur.description is not None else None
        if row is None:
            err = NotFoundError("Expected at least one row, got 0")
            err.sqlstate = "02000"
            raise err
        val = row[0] if row else None
        return MySQLCore.coerce_scalar_value(val, value_type)

    def select_value_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a `SELECT` query and return the first scalar value or `None` when empty."""
        cur = self._execute_cursor(sql, parameters)
        row = cur.fetchone() if cur.description is not None else None
        if row is None:
            return None
        val = row[0] if row else None
        return MySQLCore.coerce_scalar_value(val, value_type)

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
        self._ensure_open()
        compiled_sql, params = MySQLCore.compile_for_driver(sql, parameters)
        bound = params if params else None
        sets = self._run_with_client(lambda client: client.fetch_all_sets(compiled_sql, bound))
        return [[dict(r) for r in rs] for rs in sets]

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
        self._ensure_open()
        compiled_sql, params = MySQLCore.compile_for_driver(sql, parameters)
        bound = params if params else None
        native_cursor = self._run_with_client(
            lambda client: client.select_to_arrow(compiled_sql, bound, batch_size=batch_size)
        )
        if isinstance(native_cursor, ArrowCursor):
            return native_cursor
        return ArrowCursor(native_cursor, used_shm=True, shm_cursor=native_cursor)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[ArrowCursor]:
        """Stream a `SELECT` query as bounded `VTBARROW` `ArrowCursor` chunks."""
        self._ensure_open()
        compiled_sql, params = MySQLCore.compile_for_driver(sql, parameters)
        bound = params if params else None
        batches = self._run_with_client(
            lambda client: client.stream_arrow_batches(compiled_sql, bound, batch_size=batch_size)
        )
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
        self._ensure_open()
        source = data._shm_cursor if isinstance(data, ArrowCursor) and data._shm_cursor is not None else data
        col_list = list(columns) if columns is not None else None
        return int(
            self._run_with_client(
                lambda client: client.load_from_arrow(
                    table_name,
                    source,
                    columns=col_list,
                    batch_size=batch_size,
                )
            )
        )

    def _ensure_open(self) -> None:
        if self._closed or (self._client is not None and bool(getattr(self._client, "is_closed", False))):
            msg = "SyncMySQLDriver is closed"
            raise OperationalError(msg)

    def _close_owned_client(self) -> None:
        client = self._client
        if self._owns_client:
            self._client = None
            self._client_token = None
        if client is not None:
            if self._has_broker:
                release_fn: Callable[[], Any] | None = getattr(client, "release_lease", None)
                if callable(release_fn):
                    with contextlib.suppress(Exception):
                        release_fn()
            close_fn: Callable[[], Any] | None = getattr(client, "close", None)
            if callable(close_fn):
                with contextlib.suppress(Exception):
                    close_fn()

    def _get_client(self, *, force_refresh: bool = False) -> Any:
        self._ensure_open()
        token: str | None = None
        if self._client is not None:
            if self._owns_client and self.config.token_provider is not None and not self.in_transaction:
                token = self.config.resolve_iam_password_sync(force_refresh=force_refresh)
                if not force_refresh and token == self._client_token:
                    return self._client
                self._close_owned_client()
            else:
                return self._client
        elif self.config.token_provider is not None:
            token = self.config.resolve_iam_password_sync(force_refresh=force_refresh)

        mod = MySQLCore.require_native_module()
        connect_kwargs = build_mysql_client_kwargs(self.config, token)
        client = mod.SyncMySQLClient(**connect_kwargs)
        if self._has_broker:
            self._attach_checkout_lease(client)
        self._client = client
        self._client_token = token
        self._owns_client = True
        return self._client

    def _refresh_client_after_auth_error(self) -> None:
        invalidate_fn: Callable[[], Any] | None = getattr(self.config.token_provider, "invalidate", None)
        if callable(invalidate_fn):
            invalidate_fn()
        if self._owns_client:
            client = self._client
            if self._has_broker and client is not None:
                release_fn: Callable[[], Any] | None = getattr(client, "release_lease", None)
                if callable(release_fn):
                    with contextlib.suppress(Exception):
                        release_fn()
            self._get_client(force_refresh=True)
        else:
            self.config.resolve_iam_password_sync(force_refresh=True)

    def _run_with_client[T](
        self,
        op: Callable[[Any], T],
        *,
        map_errors: bool = True,
    ) -> T:
        for attempt in (0, 1):
            client = self._get_client()
            try:
                return op(client)
            except Exception as exc:
                if (
                    attempt == 0
                    and self.config.token_provider is not None
                    and not self.in_transaction
                    and is_mysql_auth_rejection(exc)
                ):
                    self._refresh_client_after_auth_error()
                    continue
                if not map_errors or isinstance(exc, DatabaseError):
                    raise
                raise MySQLCore.map_exception(exc) from exc
        msg = "MySQL authentication retry exhausted"
        raise OperationalError(msg)
