"""Synchronous pure-Rust SQL Server (TDS 7.4) driver."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Self

from vertebra._vertebra import NotFoundError, OperationalError
from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mssql.core import (
    MSSQL_DIALECT_TAG,
    MSSQLCore,
)
from vertebra.db.adapters.mssql.pipeline import SyncMSSQLPipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.db.driver._sync import SyncDatabaseDriver
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._mssql import SyncMSSQLClient, SyncMSSQLCursor
    from vertebra.db.dialer.cloudsql import SyncCloudSQLDialer

__all__ = ("SyncMSSQLDriver",)


class SyncMSSQLDriver(SyncDatabaseDriver[MSSQLConfig]):
    """Synchronous SQL Server TDS 7.4 driver backed by `vertebra._mssql.SyncMSSQLClient`."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    __slots__ = (
        "_client",
        "_client_token",
        "_dialer",
        "_in_transaction",
    )

    def __init__(
        self,
        config: MSSQLConfig | str | None = None,
        *,
        dialer: SyncCloudSQLDialer | None = None,
        broker: Any = None,
        client: SyncMSSQLClient | Any | None = None,
        **overrides: Any,
    ) -> None:
        resolved = MSSQLConfig._resolve_driver_config(config, overrides, dialer)
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        if client is not None:
            self._client: SyncMSSQLClient | Any = client
            self._client_token: str | None = None
        else:
            token = resolved.resolve_aad_token_sync()
            self._client = MSSQLCore.create_sync_rust_client(resolved, dialer=dialer, aad_token=token)
            self._client_token = token
        self._in_transaction: bool = False

    @property
    def dialer(self) -> SyncCloudSQLDialer | None:
        """Return the attached Cloud SQL dialer if present."""
        return self._dialer

    @property
    def in_transaction(self) -> bool:
        """Return whether an explicit transaction is currently open."""
        if self._in_transaction:
            return True
        if hasattr(self._client, "in_transaction"):
            return bool(self._client.in_transaction)
        return False

    @property
    def routing_destination(self) -> tuple[str, int] | None:
        """Return the latest `(host, port)` received via TDS `ENVCHANGE` routing."""
        dest = getattr(self._client, "routing_destination", None)
        if dest is not None and isinstance(dest, tuple) and len(dest) == 2:
            return str(dest[0]), int(dest[1])
        return None

    def cursor(self, *, lazy_begin: bool = False) -> SyncMSSQLCursor:
        """Return a native `SyncMSSQLCursor` bound to this driver's client."""
        self._ensure_open()
        cur: SyncMSSQLCursor = self._client.cursor(lazy_begin=lazy_begin)
        return cur

    def pipeline(self) -> SyncMSSQLPipeline:
        """Create a synchronous transactional query pipeline bound to this driver."""
        return SyncMSSQLPipeline(self)

    def connect(self) -> Self:
        """Ensure the underlying native `SyncMSSQLClient` is ready with a fresh Entra token if configured."""
        if self._client is None or getattr(self._client, "closed", False):
            token = self.config.resolve_aad_token_sync()
            self._client = MSSQLCore.create_sync_rust_client(self.config, dialer=self._dialer, aad_token=token)
            self._client_token = token
        else:
            self._refresh_aad_token_sync()
        if self._broker is not None:
            self._attach_checkout_lease(self._client)
        self._closed = False
        return self

    def ping(self) -> bool:
        """Execute a lightweight health check against the SQL Server session."""
        return bool(self._run_with_client(lambda client: client.ping()))

    def begin(self, isolation_level: str | None = None) -> None:
        """Begin an explicit SQL Server transaction."""

        def _op(client: Any) -> None:
            if isolation_level is not None:
                client.begin(isolation_level)
            else:
                client.begin()

        self._run_with_client(_op)
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the active SQL Server transaction."""
        try:
            self._client.commit()
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active SQL Server transaction."""
        try:
            self._client.rollback()
        finally:
            self._in_transaction = False

    def savepoint(self, name: str) -> None:
        """Create a transaction savepoint (`SAVE TRANSACTION`)."""
        validate_identifier(name, "savepoint")
        self._client.savepoint(name)

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a transaction savepoint (`ROLLBACK TRANSACTION <name>`)."""
        validate_identifier(name, "savepoint")
        self._client.rollback_to_savepoint(name)

    def release_savepoint(self, name: str) -> None:
        """Validate a savepoint identifier (no-op on T-SQL)."""
        validate_identifier(name, "savepoint")
        self._client.release_savepoint(name)

    def cancel(self) -> None:
        """Send an out-of-band TDS `ATTENTION` (`0x06`) packet to interrupt an in-flight query."""
        if not self._closed:
            self._client.cancel()

    def close(self) -> None:
        """Close the pooled TDS sessions and release driver resources."""
        self._closed = True
        self._in_transaction = False
        if self._broker is not None:
            release_fn = getattr(self._client, "release_lease", None)
            if callable(release_fn):
                release_fn()
        self._client.close()

    def _ensure_open(self) -> None:
        if self._closed or self._client is None or bool(getattr(self._client, "closed", False)):
            msg = "SyncMSSQLDriver is closed"
            raise OperationalError(msg)

    def _execute_cursor(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> SyncMSSQLCursor:
        self._ensure_open()
        bound = parameters or ()
        cur: SyncMSSQLCursor = self._run_with_client(lambda client: client.cursor().execute(sql, bound))
        return cur

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        _ = kwargs
        cur = self._execute_cursor(sql, parameters)
        return max(int(cur.rowcount), 0)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets and return total affected rows."""
        _ = kwargs
        self._ensure_open()
        if not parameter_sets:
            return 0
        param_list = list(parameter_sets)
        cur = self._run_with_client(lambda client: client.cursor().executemany(sql, param_list))
        return max(int(cur.rowcount), 0)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        _ = kwargs
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
        """Execute SQL and return `(rows, column_names)`."""
        _ = kwargs
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
        """Execute a `SELECT` query and return rows as dictionaries or hydrated `schema_type` instances."""
        _ = kwargs
        cur = self._execute_cursor(sql, parameters)
        description = cur.description
        if description is None:
            return []
        names = tuple(col[0] for col in description)
        rows = cur.fetchall()
        return self.hydrate_rows(rows, names, schema_type)

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        cur = self._execute_cursor(sql, parameters)
        description = cur.description
        row = cur.fetchone() if description is not None else None
        if row is None or description is None:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
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
        """Execute a query and return the first row or `None` when empty."""
        _ = kwargs
        cur = self._execute_cursor(sql, parameters)
        description = cur.description
        row = cur.fetchone() if description is not None else None
        if row is None or description is None:
            return None
        names = tuple(col[0] for col in description)
        return self.hydrate_rows((row,), names, schema_type)[0]

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        cur = self._execute_cursor(sql, parameters)
        row = cur.fetchone() if cur.description is not None else None
        if row is None:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        val = row[0] if row else None
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute a query and return `(rows, total_count)`."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows, len(rows)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy `VTBARROW` `ArrowCursor`."""
        _ = (threshold_rows, kwargs)
        raw_cursor = self._run_with_client(lambda client: client.select_to_arrow(sql, parameters))
        if isinstance(raw_cursor, ArrowCursor):
            return raw_cursor
        return ArrowCursor(raw_cursor)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[ArrowCursor]:
        """Execute a query and yield `VTBARROW` `ArrowCursor` chunks of at most `batch_size` rows."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        raw_batches = self._run_with_client(lambda client: client.stream_arrow_batches(sql, parameters, chunk))
        for item in raw_batches:
            yield item if isinstance(item, ArrowCursor) else ArrowCursor(item)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, IPC stream, or row sequence into `table_name` via TDS `BulkLoad`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        source, col_list = MSSQLCore.coerce_arrow_source(data, columns)
        if source is None:
            return 0
        return int(self._run_with_client(lambda client: client.load_from_arrow(table_name, source, col_list)))

    def lease_stats(self) -> dict[str, int]:
        """Return broker connection lease telemetry counters."""
        return dict(self._client.lease_stats())

    def statement_cache_stats(self) -> dict[str, int] | None:
        """Return broker statement cache telemetry counters."""
        stats = self._client.statement_cache_stats()
        return dict(stats) if stats is not None else None

    def _refresh_aad_token_sync(self, *, force_refresh: bool = False) -> None:
        if self.config.token_provider is None or self._client is None:
            return
        token = self.config.resolve_aad_token_sync(force_refresh=force_refresh)
        if token != self._client_token:
            set_fn = getattr(self._client, "set_aad_token", None)
            if callable(set_fn):
                set_fn(token)
            self._client_token = token

    def _run_with_client[T](self, op: Callable[[Any], T]) -> T:
        for attempt in (0, 1):
            if self.config.token_provider is not None and not self.in_transaction:
                self._refresh_aad_token_sync()
            try:
                return op(self._client)
            except Exception as exc:
                if (
                    attempt == 0
                    and self.config.token_provider is not None
                    and not self.in_transaction
                    and MSSQLCore.is_auth_rejection(exc)
                ):
                    invalidate_fn = getattr(self.config.token_provider, "invalidate", None)
                    if callable(invalidate_fn):
                        invalidate_fn()
                    self._refresh_aad_token_sync(force_refresh=True)
                    continue
                raise
        msg = "MSSQL authentication retry exhausted"
        raise OperationalError(msg)
