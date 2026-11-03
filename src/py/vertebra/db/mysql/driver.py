"""Synchronous and asynchronous MySQL and MariaDB drivers."""

from __future__ import annotations

import collections
import os
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.dialer.cloudsql import CloudSQLDialer
from vertebra.db.driver import (
    AsyncDatabaseDriver,
    SyncDatabaseDriver,
    _normalize_driver_params,
)
from vertebra.db.mysql.config import MySQLConfig
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    import ssl

__all__ = (
    "AsyncMySQLDriver",
    "SyncMySQLDriver",
)


class SyncMySQLDriver(SyncDatabaseDriver[MySQLConfig]):
    """Synchronous MySQL driver facade with bounded connection pooling and SHM broker integration."""

    dialect: ClassVar[str] = "mysql"
    dialect_tag: ClassVar[int] = 2

    __slots__ = ("_connect_factory", "_dialer", "_idle_conns", "_in_memory_rows")

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        dialer: CloudSQLDialer | None = None,
        broker: Any = None,
        connect_factory: Any = None,
    ) -> None:
        resolved = config or MySQLConfig()
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        if self._dialer is None:
            csql_cfg = self.config.to_cloud_dialer_config()
            if csql_cfg is not None:
                self._dialer = CloudSQLDialer(csql_cfg)
        self._connect_factory = connect_factory
        self._idle_conns: collections.deque[Any] = collections.deque(maxlen=self.config.max_size)
        self._in_memory_rows: list[tuple[Any, ...]] = []

    @property
    def _config(self) -> MySQLConfig:
        """Return the active MySQLConfig."""
        return self.config

    @property
    def dialer(self) -> CloudSQLDialer | None:
        """Return the attached CloudSQLDialer if configured."""
        return self._dialer

    def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement and return plain tuples and column description."""
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
                desc = (
                    tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(self._in_memory_rows[0])))
                    if self._in_memory_rows
                    else None
                )
                return list(self._in_memory_rows), desc

            conn = self._acquire()
            try:
                if hasattr(conn, "execute_with_description"):
                    res: tuple[
                        list[tuple[Any, ...]],
                        tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                    ] = conn.execute_with_description(sql, parameters)
                    return res
                if hasattr(conn, "cursor"):
                    cur = conn.cursor()
                    try:
                        cur.execute(sql, parameters if parameters else None)
                        rows = cur.fetchall() if hasattr(cur, "fetchall") else []
                        desc = (
                            tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                            if getattr(cur, "description", None)
                            else None
                        )
                        return [tuple(r) for r in rows], desc
                    finally:
                        if hasattr(cur, "close"):
                            cur.close()
                if hasattr(conn, "execute"):
                    exec_res = conn.execute(sql, parameters if parameters else None)
                    rows = exec_res.fetchall() if hasattr(exec_res, "fetchall") else list(exec_res)
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in exec_res.description)
                        if getattr(exec_res, "description", None)
                        else None
                    )
                    return [tuple(r) for r in rows], desc
                return [], None
            finally:
                self._release(conn)
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    def execute_tuples(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def execute(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement with variadic or sequence parameters."""
        resolved = _normalize_driver_params(params, parameters)
        return self.execute_tuples(sql, resolved)

    def select(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a SELECT statement and return rows as dictionaries or hydrated `schema_type` instances."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        if not rows:
            return []
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]))]
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    def select_one(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a SELECT statement and return the first row or raise `ValueError`."""
        rows = self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a SELECT statement and return the first row or `None`."""
        rows = self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        return rows[0] if rows else None

    def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("%s" for _ in cols)
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        for row in rows:
            self.execute_tuples(sql, row)
        return len(rows)

    def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> Any:
        """Execute a query and return a PyArrow Table backed by ShmRegion Arrow IPC when >= 256 rows."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        cursor = ArrowCursor.from_rows(rows, cols, threshold_rows=ARROW_SHM_THRESHOLD_ROWS)
        return cursor.to_arrow_table()

    def commit(self) -> None:
        """Commit the active transaction across active connections."""
        for conn in list(self._idle_conns):
            if hasattr(conn, "commit"):
                conn.commit()
        if hasattr(self._connect_factory, "commit"):
            self._connect_factory.commit()

    def rollback(self) -> None:
        """Roll back the active transaction across active connections."""
        for conn in list(self._idle_conns):
            if hasattr(conn, "rollback"):
                conn.rollback()
        if hasattr(self._connect_factory, "rollback"):
            self._connect_factory.rollback()

    def close(self) -> None:
        """Close the driver and release all pooled connections."""
        self._closed = True
        while self._idle_conns:
            conn = self._idle_conns.pop()
            if hasattr(conn, "close"):
                conn.close()
        if hasattr(self._connect_factory, "close"):
            self._connect_factory.close()

    def _create_connection(self) -> Any:
        """Create a new database connection or raise MissingDependencyError."""
        params = self.config.parse_dsn_params()
        tls_sock: ssl.SSLSocket | None = None
        if self._dialer is not None:
            tls_sock = self._dialer.connect_sync(host=params["host"])

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
    dialect_tag: ClassVar[int] = 2

    __slots__ = ("_dialer", "_in_memory_rows", "_pool")

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        dialer: CloudSQLDialer | None = None,
        broker: Any = None,
        pool: Any = None,
    ) -> None:
        resolved = config or MySQLConfig()
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        if self._dialer is None:
            csql_cfg = self.config.to_cloud_dialer_config()
            if csql_cfg is not None:
                self._dialer = CloudSQLDialer(csql_cfg)
        self._pool = pool
        self._in_memory_rows: list[tuple[Any, ...]] = []

    @property
    def _config(self) -> MySQLConfig:
        """Return the active MySQLConfig."""
        return self.config

    @property
    def dialer(self) -> CloudSQLDialer | None:
        """Return the attached CloudSQLDialer if configured."""
        return self._dialer

    async def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement and return rows and column descriptions."""
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
                desc = (
                    tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(self._in_memory_rows[0])))
                    if self._in_memory_rows
                    else None
                )
                return list(self._in_memory_rows), desc

            pool = await self._ensure_pool()
            if hasattr(pool, "execute_with_description"):
                raw_res = pool.execute_with_description(sql, parameters)
                if hasattr(raw_res, "__await__"):
                    raw_res = await raw_res
                res: tuple[
                    list[tuple[Any, ...]],
                    tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                ] = raw_res
                return res
            if hasattr(pool, "acquire"):
                async with pool.acquire() as conn, conn.cursor() as cur:
                    await cur.execute(sql, parameters if parameters else None)
                    rows = await cur.fetchall() if hasattr(cur, "fetchall") else []
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                        if getattr(cur, "description", None)
                        else None
                    )
                    return [tuple(r) for r in rows], desc
            if hasattr(pool, "connection"):
                conn = pool.connection()
                if hasattr(conn, "__aenter__"):
                    async with conn as active_conn, active_conn.cursor() as cur:
                        await cur.execute(sql, parameters if parameters else None)
                        rows = await cur.fetchall() if hasattr(cur, "fetchall") else []
                        desc = (
                            tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                            if getattr(cur, "description", None)
                            else None
                        )
                        return [tuple(r) for r in rows], desc
            return [], None
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    async def execute_tuples(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        rows, _ = await self.execute_with_description(sql, parameters)
        return rows

    async def execute(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement."""
        resolved = _normalize_driver_params(params, parameters)
        return await self.execute_tuples(sql, resolved)

    async def select(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Asynchronously execute a SELECT statement and return rows as dictionaries or `schema_type` instances."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = await self.execute_with_description(sql, resolved)
        if not rows:
            return []
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]))]
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    async def select_one(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Asynchronously execute a SELECT statement and return the first row or raise `ValueError`."""
        rows = await self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    async def select_one_or_none(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Asynchronously execute a SELECT statement and return the first row or `None`."""
        rows = await self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        return rows[0] if rows else None

    async def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("%s" for _ in cols)
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        for row in rows:
            await self.execute_tuples(sql, row)
        return len(rows)

    async def select_to_arrow(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> Any:
        """Asynchronously execute a query and return a PyArrow Table over ShmRegion Arrow IPC."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = await self.execute_with_description(sql, resolved)
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        cursor = ArrowCursor.from_rows(rows, cols, threshold_rows=ARROW_SHM_THRESHOLD_ROWS)
        return cursor.to_arrow_table()

    async def close(self) -> None:
        """Close the async driver pool."""
        self._closed = True
        if self._pool is not None:
            if hasattr(self._pool, "close"):
                res = self._pool.close()
                if hasattr(res, "__await__"):
                    await res
            if hasattr(self._pool, "wait_closed"):
                await self._pool.wait_closed()

    async def _ensure_pool(self) -> Any:
        """Ensure connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        asyncmy: Any = ensure_module("asyncmy", install_package="asyncmy", extra="mysql")
        params = self.config.parse_dsn_params()
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
