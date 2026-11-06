"""Synchronous and asynchronous MySQL and MariaDB drivers."""

from __future__ import annotations

import collections
import inspect
import os
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.adapters.mysql.config import MySQLConfig
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.dialer.cloudsql import AsyncCloudSQLDialer, SyncCloudSQLDialer
from vertebra.db.driver import (
    MYSQL_DIALECT_TAG,
    AsyncDatabaseDriver,
    SyncDatabaseDriver,
)
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

if TYPE_CHECKING:
    import ssl

__all__ = (
    "AsyncMySQLDriver",
    "SyncMySQLDriver",
)


class SyncMySQLDriver(SyncDatabaseDriver[MySQLConfig]):
    """Synchronous MySQL driver facade with bounded connection pooling and SHM broker integration."""

    dialect: ClassVar[str] = "mysql"
    dialect_tag: ClassVar[int] = MYSQL_DIALECT_TAG

    __slots__ = (
        "_connect_factory",
        "_dialer",
        "_idle_conns",
        "_in_memory_rows",
        "_in_transaction",
        "_tx_conn",
        "_tx_snapshot",
    )

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        dialer: SyncCloudSQLDialer | None = None,
        broker: Any = None,
        connect_factory: Any = None,
    ) -> None:
        resolved = config or MySQLConfig()
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        if self._dialer is None:
            csql_cfg = self.config.to_cloud_dialer_config()
            if csql_cfg is not None:
                self._dialer = SyncCloudSQLDialer(csql_cfg)
        self._connect_factory = connect_factory
        self._idle_conns: collections.deque[Any] = collections.deque(maxlen=self.config.max_size)
        self._in_memory_rows: list[tuple[Any, ...]] = []
        self._in_transaction: bool = False
        self._tx_conn: Any = None
        self._tx_snapshot: list[tuple[Any, ...]] | None = None

    @property
    def _config(self) -> MySQLConfig:
        """Return the active MySQLConfig."""
        return self.config

    @property
    def dialer(self) -> SyncCloudSQLDialer | None:
        """Return the attached CloudSQLDialer if configured."""
        return self._dialer

    def begin(self) -> None:
        """Begin an explicit MySQL transaction and pin a pooled connection."""
        if self._closed:
            msg = "SyncMySQLDriver is closed"
            raise RuntimeError(msg)
        self._in_transaction = True
        self._tx_snapshot = list(self._in_memory_rows)
        if self._tx_conn is None and not (self.config.dsn.startswith("memory://") and self._connect_factory is None):
            self._tx_conn = self._acquire()
            if hasattr(self._tx_conn, "begin"):
                self._tx_conn.begin()

    def commit(self) -> None:
        """Commit the active transaction."""
        tx_conn = self._tx_conn
        mru_conn = self._idle_conns.pop() if tx_conn is None and self._idle_conns else None
        target = tx_conn if tx_conn is not None else mru_conn
        try:
            if target is not None:
                if hasattr(target, "commit"):
                    target.commit()
            elif hasattr(self._connect_factory, "commit"):
                self._connect_factory.commit()
        finally:
            self._tx_conn = None
            self._tx_snapshot = None
            self._in_transaction = False
            if target is not None:
                self._release(target)

    def rollback(self) -> None:
        """Roll back the active transaction."""
        tx_conn = self._tx_conn
        mru_conn = self._idle_conns.pop() if tx_conn is None and self._idle_conns else None
        target = tx_conn if tx_conn is not None else mru_conn
        try:
            if self._tx_snapshot is not None:
                self._in_memory_rows = list(self._tx_snapshot)
            if target is not None:
                if hasattr(target, "rollback"):
                    target.rollback()
            elif hasattr(self._connect_factory, "rollback"):
                self._connect_factory.rollback()
        finally:
            self._tx_conn = None
            self._tx_snapshot = None
            self._in_transaction = False
            if target is not None:
                self._release(target)

    def close(self) -> None:
        """Close the driver and release all pooled connections."""
        self._closed = True
        tx_conn = self._tx_conn
        self._tx_conn = None
        self._tx_snapshot = None
        self._in_transaction = False
        if tx_conn is not None and hasattr(tx_conn, "close"):
            tx_conn.close()
        while self._idle_conns:
            conn = self._idle_conns.pop()
            if hasattr(conn, "close"):
                conn.close()
        if hasattr(self._connect_factory, "close"):
            self._connect_factory.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        _ = kwargs
        _, _, rowcount = self._run_sync_query(sql, parameters, auto_commit_dml=True)
        return rowcount

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a SQL statement and return plain tuple rows and column names."""
        _ = kwargs
        rows, cols, _ = self._run_sync_query(sql, parameters, auto_commit_dml=False)
        return rows, cols

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and return an `ArrowCursor` backed by ShmRegion Arrow IPC."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        col_list = list(cols) if cols else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        return ArrowCursor.from_rows(rows, col_list, threshold_rows=threshold_rows)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("%s" for _ in cols)
        quoted_table = quote_dialect_identifier(table_name, self.dialect)
        col_list = ", ".join(quote_dialect_identifier(str(col), self.dialect) for col in cols)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        for row in rows:
            self.execute(sql, row)
        return len(rows)

    def _run_sync_query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        *,
        auto_commit_dml: bool,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        if self._closed:
            msg = "SyncMySQLDriver is closed"
            raise RuntimeError(msg)

        lease: Any = None
        if self._broker is not None:
            if hasattr(self._broker, "get_or_compile_statement"):
                self._broker.get_or_compile_statement("mysql", sql, lambda s: (s, ()))
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                lease = self._broker.lease_table.acquire(os.getpid())
            elif hasattr(self._broker, "acquire"):
                try:
                    lease = self._broker.acquire(bind_key="mysql")
                except TypeError:
                    lease = self._broker.acquire(os.getpid())

        try:
            if self.config.dsn.startswith("memory://") and self._connect_factory is None:
                cols = tuple(f"col_{i}" for i in range(len(self._in_memory_rows[0]))) if self._in_memory_rows else ()
                return list(self._in_memory_rows), cols, len(self._in_memory_rows)

            pinned = self._tx_conn is not None
            conn = self._tx_conn if pinned else self._acquire()
            try:
                if hasattr(conn, "execute_with_description"):
                    raw_rows, raw_desc = conn.execute_with_description(sql, parameters or ())
                    tuple_rows = [tuple(r) for r in raw_rows]
                    return tuple_rows, self._extract_column_names(raw_desc), len(tuple_rows)
                if hasattr(conn, "cursor"):
                    cur = conn.cursor()
                    try:
                        cur.execute(sql, parameters if parameters else None)
                        raw_desc = getattr(cur, "description", None)
                        if raw_desc:
                            rows = [tuple(r) for r in (cur.fetchall() if hasattr(cur, "fetchall") else ())]
                            return rows, self._extract_column_names(raw_desc), len(rows)
                        if auto_commit_dml and not self._in_transaction and hasattr(conn, "commit"):
                            conn.commit()
                        rowcount = getattr(cur, "rowcount", 0)
                        return [], (), max(int(rowcount or 0), 0)
                    finally:
                        if hasattr(cur, "close"):
                            cur.close()
                if hasattr(conn, "execute"):
                    exec_res = conn.execute(sql, parameters if parameters else None)
                    raw_desc = getattr(exec_res, "description", None)
                    raw_rows = exec_res.fetchall() if hasattr(exec_res, "fetchall") else list(exec_res)
                    tuple_rows = [tuple(r) for r in raw_rows]
                    return tuple_rows, self._extract_column_names(raw_desc), len(tuple_rows)
                return [], (), 0
            finally:
                if not pinned:
                    self._release(conn)
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    def _create_connection(self) -> Any:
        """Create a new database connection or raise MissingDependencyError."""
        params = self.config.parse_dsn_params()
        tls_sock: ssl.SSLSocket | None = None
        if self._dialer is not None:
            tls_sock = self._dialer.connect(host=params["host"])

        if self._connect_factory is not None:
            if tls_sock is not None:
                try:
                    return self._connect_factory(sock=tls_sock)
                except TypeError:
                    return self._connect_factory()
            return self._connect_factory()

        pymysql: Any = ensure_module("pymysql", install_package="pymysql", extra="mysql")

        connect_kwargs: dict[str, Any] = {
            "host": params["host"],
            "port": params["port"],
            "user": params["user"],
            "password": params["password"],
            "database": params["database"],
            "charset": params["charset"],
            "autocommit": params["autocommit"],
        }
        if tls_sock is not None:
            connect_kwargs["sock"] = tls_sock
        return pymysql.connect(**connect_kwargs)

    def _acquire(self) -> Any:
        """Acquire an idle connection or create a new connection."""
        if self._closed:
            msg = "SyncMySQLDriver is closed"
            raise RuntimeError(msg)
        if self._idle_conns:
            return self._idle_conns.pop()
        return self._create_connection()

    def _release(self, conn: Any) -> None:
        """Release a connection back to the idle pool or close it."""
        if self._closed or len(self._idle_conns) >= self.config.max_size:
            if hasattr(conn, "close"):
                conn.close()
        else:
            self._idle_conns.append(conn)


class AsyncMySQLDriver(AsyncDatabaseDriver[MySQLConfig]):
    """Asynchronous MySQL driver facade backed by asyncmy and SHM broker leasing."""

    dialect: ClassVar[str] = "mysql"
    dialect_tag: ClassVar[int] = MYSQL_DIALECT_TAG

    __slots__ = (
        "_dialer",
        "_in_memory_rows",
        "_in_transaction",
        "_pool",
        "_tx_conn",
        "_tx_conn_ctx",
        "_tx_snapshot",
    )

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        dialer: AsyncCloudSQLDialer | None = None,
        broker: Any = None,
        pool: Any = None,
    ) -> None:
        resolved = config or MySQLConfig()
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        if self._dialer is None:
            csql_cfg = self.config.to_cloud_dialer_config()
            if csql_cfg is not None:
                self._dialer = AsyncCloudSQLDialer(csql_cfg)
        self._pool = pool
        self._in_memory_rows: list[tuple[Any, ...]] = []
        self._in_transaction: bool = False
        self._tx_conn: Any = None
        self._tx_conn_ctx: Any = None
        self._tx_snapshot: list[tuple[Any, ...]] | None = None

    @property
    def _config(self) -> MySQLConfig:
        """Return the active MySQLConfig."""
        return self.config

    @property
    def dialer(self) -> AsyncCloudSQLDialer | None:
        """Return the attached CloudSQLDialer if configured."""
        return self._dialer

    async def begin(self) -> None:
        """Begin an explicit async MySQL transaction and pin a pooled connection."""
        if self._closed:
            msg = "AsyncMySQLDriver is closed"
            raise RuntimeError(msg)
        self._in_transaction = True
        self._tx_snapshot = list(self._in_memory_rows)
        if self._tx_conn is not None or (self.config.dsn.startswith("memory://") and self._pool is None):
            return
        pool = await self._ensure_pool()
        if hasattr(pool, "acquire"):
            ctx = pool.acquire()
            self._tx_conn_ctx = ctx
            self._tx_conn = await ctx.__aenter__()
        elif hasattr(pool, "connection"):
            conn_obj = pool.connection()
            if hasattr(conn_obj, "__aenter__"):
                self._tx_conn_ctx = conn_obj
                self._tx_conn = await conn_obj.__aenter__()
            else:
                self._tx_conn = conn_obj
        if self._tx_conn is not None and hasattr(self._tx_conn, "begin"):
            res = self._tx_conn.begin()
            if inspect.isawaitable(res):
                await res

    async def commit(self) -> None:
        """Commit the active async MySQL transaction."""
        try:
            if self._tx_conn is not None and hasattr(self._tx_conn, "commit"):
                res = self._tx_conn.commit()
                if inspect.isawaitable(res):
                    await res
            elif self._pool is not None and hasattr(self._pool, "commit"):
                res = self._pool.commit()
                if inspect.isawaitable(res):
                    await res
        finally:
            await self._release_tx_conn()

    async def rollback(self) -> None:
        """Roll back the active async MySQL transaction."""
        try:
            if self._tx_snapshot is not None:
                self._in_memory_rows = list(self._tx_snapshot)
            if self._tx_conn is not None and hasattr(self._tx_conn, "rollback"):
                res = self._tx_conn.rollback()
                if inspect.isawaitable(res):
                    await res
            elif self._pool is not None and hasattr(self._pool, "rollback"):
                res = self._pool.rollback()
                if inspect.isawaitable(res):
                    await res
        finally:
            await self._release_tx_conn()

    async def close(self) -> None:
        """Close the async driver pool."""
        self._closed = True
        await self._release_tx_conn()
        if self._pool is not None:
            if hasattr(self._pool, "close"):
                res = self._pool.close()
                if hasattr(res, "__await__"):
                    await res
            if hasattr(self._pool, "wait_closed"):
                await self._pool.wait_closed()

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously execute a DML or DDL statement and return affected row count."""
        _ = kwargs
        _, _, rowcount = await self._run_async_query(sql, parameters, auto_commit_dml=True)
        return rowcount

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Asynchronously execute a SQL statement and return tuple rows and column names."""
        _ = kwargs
        rows, cols, _ = await self._run_async_query(sql, parameters, auto_commit_dml=False)
        return rows, cols

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Asynchronously execute a query and return an `ArrowCursor` over ShmRegion Arrow IPC."""
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        col_list = list(cols) if cols else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        return ArrowCursor.from_rows(rows, col_list, threshold_rows=threshold_rows)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("%s" for _ in cols)
        quoted_table = quote_dialect_identifier(table_name, self.dialect)
        col_list = ", ".join(quote_dialect_identifier(str(col), self.dialect) for col in cols)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        for row in rows:
            await self.execute(sql, row)
        return len(rows)

    async def _release_tx_conn(self) -> None:
        ctx = self._tx_conn_ctx
        self._tx_conn = None
        self._tx_conn_ctx = None
        self._tx_snapshot = None
        self._in_transaction = False
        if ctx is not None:
            await ctx.__aexit__(None, None, None)

    async def _execute_on_async_connection(
        self,
        active_conn: Any,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        *,
        auto_commit_dml: bool,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        async with active_conn.cursor() as cur:
            await cur.execute(sql, parameters if parameters else None)
            raw_desc = getattr(cur, "description", None)
            if raw_desc:
                fetched = await cur.fetchall() if hasattr(cur, "fetchall") else []
                rows = [tuple(r) for r in fetched]
                return rows, self._extract_column_names(raw_desc), len(rows)
            if auto_commit_dml and not self._in_transaction and hasattr(active_conn, "commit"):
                commit_res = active_conn.commit()
                if inspect.isawaitable(commit_res):
                    await commit_res
            rowcount = getattr(cur, "rowcount", 0)
            return [], (), max(int(rowcount or 0), 0)

    async def _run_async_query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        *,
        auto_commit_dml: bool,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        if self._closed:
            msg = "AsyncMySQLDriver is closed"
            raise RuntimeError(msg)

        lease: Any = None
        if self._broker is not None:
            if hasattr(self._broker, "get_or_compile_statement"):
                self._broker.get_or_compile_statement("mysql", sql, lambda s: (s, ()))
            if hasattr(self._broker, "acquire_async"):
                lease = await self._broker.acquire_async(bind_key="mysql")
            elif hasattr(self._broker, "acquire"):
                lease = self._broker.acquire(bind_key="mysql")

        try:
            if self.config.dsn.startswith("memory://") and self._pool is None:
                cols = tuple(f"col_{i}" for i in range(len(self._in_memory_rows[0]))) if self._in_memory_rows else ()
                return list(self._in_memory_rows), cols, len(self._in_memory_rows)

            if self._tx_conn is not None:
                return await self._execute_on_async_connection(
                    self._tx_conn,
                    sql,
                    parameters,
                    auto_commit_dml=False,
                )

            pool = await self._ensure_pool()
            if hasattr(pool, "execute_with_description"):
                raw_res = pool.execute_with_description(sql, parameters or ())
                if hasattr(raw_res, "__await__"):
                    raw_res = await raw_res
                raw_rows, raw_desc = raw_res
                tuple_rows = [tuple(r) for r in raw_rows]
                return tuple_rows, self._extract_column_names(raw_desc), len(tuple_rows)
            if hasattr(pool, "acquire"):
                async with pool.acquire() as conn:
                    return await self._execute_on_async_connection(
                        conn,
                        sql,
                        parameters,
                        auto_commit_dml=auto_commit_dml,
                    )
            if hasattr(pool, "connection"):
                conn = pool.connection()
                if hasattr(conn, "__aenter__"):
                    async with conn as active_conn:
                        return await self._execute_on_async_connection(
                            active_conn,
                            sql,
                            parameters,
                            auto_commit_dml=auto_commit_dml,
                        )
            return [], (), 0
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    async def _ensure_pool(self) -> Any:
        """Ensure connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        asyncmy: Any = ensure_module("asyncmy", install_package="asyncmy", extra="mysql")
        params = await self.config.parse_dsn_params_async()
        pool_kwargs: dict[str, Any] = {
            "host": params["host"],
            "port": params["port"],
            "user": params["user"],
            "password": params["password"],
            "db": params["database"],
            "charset": params["charset"],
            "autocommit": params["autocommit"],
            "minsize": self.config.min_size,
            "maxsize": self.config.max_size,
        }
        self._pool = await asyncmy.create_pool(**pool_kwargs)
        return self._pool
