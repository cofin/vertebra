"""Synchronous and asynchronous PostgreSQL drivers."""

from __future__ import annotations

import inspect
import os
from collections.abc import Awaitable, Buffer, Callable, Iterator, Mapping, Sequence
from typing import Any, ClassVar

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import (
    AsyncDatabaseDriver,
    SyncDatabaseDriver,
    _normalize_driver_params,
)
from vertebra.db.postgres.config import PostgreSQLConfig
from vertebra.db.postgres.core import PostgreSQLCore
from vertebra.utils.module_loader import ensure_module, import_optional

__all__ = (
    "AsyncPostgreSQLDriver",
    "SyncPostgreSQLDriver",
)


class SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]):
    """Synchronous PostgreSQL driver facade supporting plain-tuple, zero-copy JSONB, and COPY streaming."""

    dialect: ClassVar[str] = "postgres"
    dialect_tag: ClassVar[int] = 1

    __slots__ = ("_dialer", "_executor", "_in_memory_rows", "_pool")

    def __init__(
        self,
        config: PostgreSQLConfig | None = None,
        *,
        pool: Any = None,
        dialer: Any = None,
        broker: Any = None,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
    ) -> None:
        resolved = config or PostgreSQLConfig()
        super().__init__(resolved, broker=broker)
        self._pool = pool
        self._dialer = dialer
        self._executor = executor
        self._in_memory_rows: list[tuple[Any, ...]] = []

    @property
    def _config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQLConfig."""
        return self.config

    @property
    def dialer(self) -> Any:
        """Return the attached Cloud SQL or AlloyDB dialer if present."""
        return self._dialer

    @property
    def pool(self) -> Any:
        """Return the underlying connection pool if present."""
        return self._pool

    @staticmethod
    def strip_jsonb_header(payload: Buffer) -> memoryview:
        """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
        return PostgreSQLCore.strip_jsonb_header(payload)

    @staticmethod
    def encode_jsonb(value: Any) -> bytes:
        """Encode a Python value with the 1-byte PostgreSQL `\\x01` JSONB version header."""
        return PostgreSQLCore.encode_jsonb(value)

    @staticmethod
    def encode_pgcopy_binary_rows(
        rows: Sequence[Sequence[bytes | str | None]],
        flags: int = 0,
        ext_bytes: bytes = b"",
    ) -> bytes:
        """Encode rows into PostgreSQL binary COPY format."""
        return PostgreSQLCore.encode_pgcopy_rows(rows, flags=flags, ext_bytes=ext_bytes)

    @staticmethod
    def parse_pgcopy_binary_rows(payload: Buffer) -> list[tuple[bytes | None, ...]]:
        """Parse PostgreSQL binary COPY format stream into row tuples of byte values."""
        return PostgreSQLCore.decode_pgcopy_rows(payload)

    def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement returning rows and column descriptions."""
        lease = self._acquire_pg_broker_lease(sql)
        try:
            if self._executor is not None:
                seq_params = tuple(parameters) if isinstance(parameters, Sequence) else ()
                exec_rows = [tuple(row) for row in self._executor(sql, seq_params)]
                return exec_rows, None
            if self._pool is not None:
                if hasattr(self._pool, "connection"):
                    conn = self._pool.connection()
                    conn_ctx = conn if hasattr(conn, "__enter__") else None
                    try:
                        active_conn = conn.__enter__() if conn_ctx is not None else conn
                        cur = active_conn.cursor()
                        try:
                            cur.execute(sql, parameters)
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
                    finally:
                        if conn_ctx is not None:
                            conn_ctx.__exit__(None, None, None)
                if hasattr(self._pool, "execute_with_description"):
                    res: tuple[
                        list[tuple[Any, ...]],
                        tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                    ] = self._pool.execute_with_description(sql, parameters)
                    return res
                if hasattr(self._pool, "execute"):
                    exec_res = self._pool.execute(sql, parameters)
                    rows = exec_res.fetchall() if hasattr(exec_res, "fetchall") else list(exec_res)
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in exec_res.description)
                        if hasattr(exec_res, "description") and exec_res.description
                        else None
                    )
                    return [tuple(r) for r in rows], desc

            if not self.config.dsn.startswith("memory://"):
                pool = self._ensure_pool()
                if pool is not None and hasattr(pool, "connection"):
                    with pool.connection() as conn, conn.cursor() as cur:
                        cur.execute(sql, parameters)
                        rows = cur.fetchall() if hasattr(cur, "fetchall") else []
                        desc = (
                            tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                            if getattr(cur, "description", None)
                            else None
                        )
                        return [tuple(r) for r in rows], desc

            _ = (sql, parameters)
            desc = (
                tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(self._in_memory_rows[0])))
                if self._in_memory_rows
                else None
            )
            return list(self._in_memory_rows), desc
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
        """Execute a SQL statement with positional or sequence parameters."""
        resolved = _normalize_driver_params(params, parameters)
        return self.execute_tuples(sql, resolved)

    def select(  # type: ignore[override]
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a SELECT query and return rows as dictionaries or hydrated schema_type instances."""
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
        """Execute a SELECT query and return the first row or raise ValueError."""
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
        """Execute a SELECT query and return the first row or None."""
        rows = self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        return rows[0] if rows else None

    def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-load an Arrow Table, RecordBatch, ArrowCursor, or row sequence into table_name."""
        tuple_rows, col_names = _table_to_rows_and_cols(data, columns)
        if not tuple_rows:
            return 0
        placeholders = ", ".join("%s" for _ in col_names)
        cols_sql = ", ".join(col_names)
        sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})"
        for row in tuple_rows:
            self.execute_tuples(sql, row)
        return len(tuple_rows)

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

    def stream_arrow_batches(
        self,
        sql: str,
        columns: Sequence[str],
        *,
        batch_size: int = 1024,
    ) -> Iterator[Any]:
        """Execute a query via binary COPY and yield PyArrow RecordBatch or tuple batch chunks."""
        rows: list[tuple[Any, ...]] = []
        if self._pool is not None:
            if hasattr(self._pool, "copy_binary_stream"):
                stream = self._pool.copy_binary_stream(sql)
                rows = self.parse_pgcopy_binary_rows(stream)
            elif hasattr(self._pool, "connection"):
                with self._pool.connection() as conn, conn.cursor() as cur:
                    buf = bytearray()
                    with cur.copy(f"COPY ({sql}) TO STDOUT (FORMAT BINARY)") as copy:
                        for chunk in copy:
                            buf.extend(chunk)
                    rows = self.parse_pgcopy_binary_rows(buf)
            elif hasattr(self._pool, "execute_tuples"):
                rows = self._pool.execute_tuples(sql)
            else:
                rows = list(self._in_memory_rows)
        elif not self.config.dsn.startswith("memory://"):
            pool = self._ensure_pool()
            if pool is not None and hasattr(pool, "connection"):
                with pool.connection() as conn, conn.cursor() as cur:
                    buf = bytearray()
                    with cur.copy(f"COPY ({sql}) TO STDOUT (FORMAT BINARY)") as copy:
                        for chunk in copy:
                            buf.extend(chunk)
                    rows = self.parse_pgcopy_binary_rows(buf)
            else:
                rows = list(self._in_memory_rows)
        else:
            rows = list(self._in_memory_rows)

        pa = import_optional("pyarrow")
        cols = tuple(columns)
        if not rows:
            if pa is not None:
                pydict_empty: dict[str, list[Any]] = {col: [] for col in cols}
                yield pa.RecordBatch.from_pydict(pydict_empty)
            else:
                yield []
            return

        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            if pa is not None:
                pydict_chunk: dict[str, list[Any]] = {
                    col: [r[j] if j < len(r) else None for r in chunk] for j, col in enumerate(cols)
                }
                yield pa.RecordBatch.from_pydict(pydict_chunk)
            else:
                yield chunk

    def commit(self) -> None:
        """Commit the active transaction."""
        if self._pool is not None and hasattr(self._pool, "commit"):
            self._pool.commit()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        if self._pool is not None and hasattr(self._pool, "rollback"):
            self._pool.rollback()

    def close(self) -> None:
        """Close the driver and release the connection pool."""
        self._closed = True
        if self._pool is not None and hasattr(self._pool, "close"):
            self._pool.close()

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return self.strip_jsonb_header(raw)

    def _format_guc_options(self) -> str:
        """Format session GUC options for psycopg connection parameters."""
        gucs = self.config.build_session_gucs()
        return " ".join(f"-c {k}={v}" for k, v in gucs.items())

    def _ensure_pool(self) -> Any:
        """Ensure connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        if self.config.dsn.startswith("memory://"):
            return None
        psycopg_pool = ensure_module(
            "psycopg_pool",
            install_package="psycopg[binary,pool]",
            extra="postgres",
        )

        kwargs: dict[str, Any] = {
            "autocommit": False,
            "prepare_threshold": None if self.config.effective_statement_cache_size == 0 else 5,
        }
        guc_opts = self._format_guc_options()
        if guc_opts:
            kwargs["options"] = guc_opts
        if self.config.enable_iam_auth and self._dialer is None:
            iam_pwd = self.config.resolve_iam_password_sync()
            if iam_pwd:
                kwargs["password"] = iam_pwd

        self._pool = psycopg_pool.ConnectionPool(
            conninfo=self.config.dsn,
            min_size=self.config.min_size,
            max_size=self.config.max_size,
            kwargs=kwargs,
        )
        return self._pool

    def _acquire_pg_broker_lease(self, sql: str) -> Any:
        """Acquire a lease and statement cache plan from the attached broker if present."""
        if self._broker is None:
            return None
        if hasattr(self._broker, "get_or_compile_statement") and self.config.effective_statement_cache_size > 0:
            self._broker.get_or_compile_statement("postgresql", sql, lambda s: (s, ()))
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(bind_key="postgresql")
            except TypeError:
                return self._broker.acquire(os.getpid())
        return None


class AsyncPostgreSQLDriver(AsyncDatabaseDriver[PostgreSQLConfig]):
    """Asynchronous PostgreSQL driver facade supporting plain-tuple, zero-copy JSONB, and COPY streaming."""

    dialect: ClassVar[str] = "postgres"
    dialect_tag: ClassVar[int] = 1

    __slots__ = ("_async_executor", "_dialer", "_pool", "_sync")

    def __init__(
        self,
        config: PostgreSQLConfig | None = None,
        *,
        pool: Any = None,
        dialer: Any = None,
        broker: Any = None,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
        async_executor: Callable[[str, Sequence[Any]], Awaitable[list[tuple[Any, ...]]]] | None = None,
    ) -> None:
        resolved = config or PostgreSQLConfig()
        super().__init__(resolved, broker=broker)
        self._pool = pool
        self._dialer = dialer
        self._async_executor = async_executor
        self._sync = SyncPostgreSQLDriver(
            self.config,
            pool=pool,
            dialer=dialer,
            broker=self._broker,
            executor=executor,
        )

    @property
    def _config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQLConfig."""
        return self.config

    @property
    def dialer(self) -> Any:
        """Return the attached Cloud SQL or AlloyDB dialer if present."""
        return self._dialer

    @property
    def pool(self) -> Any:
        """Return the underlying connection pool if present."""
        return self._pool

    async def execute_with_description(  # type: ignore[override]
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement returning rows and column descriptions."""
        lease: Any = None
        if self._broker is not None:
            if hasattr(self._broker, "get_or_compile_statement") and self.config.effective_statement_cache_size > 0:
                self._broker.get_or_compile_statement("postgresql", sql, lambda s: (s, ()))
            if hasattr(self._broker, "acquire_async"):
                lease = await self._broker.acquire_async(bind_key="postgresql")
            elif hasattr(self._broker, "acquire"):
                lease = self._broker.acquire(bind_key="postgresql")
        try:
            if self._async_executor is not None:
                seq_params = tuple(parameters) if isinstance(parameters, Sequence) else ()
                exec_rows = await self._async_executor(sql, seq_params)
                return [tuple(r) for r in exec_rows], None
            if self._pool is not None:
                if hasattr(self._pool, "acquire"):
                    async with self._pool.acquire() as conn:
                        if hasattr(conn, "fetch"):
                            params = tuple(parameters) if isinstance(parameters, Sequence) else ()
                            records = await conn.fetch(sql, *params)
                            return [tuple(r.values() if hasattr(r, "values") else r) for r in records], None
                        if hasattr(conn, "cursor"):
                            async with conn.cursor() as cur:
                                await cur.execute(sql, parameters)
                                rows = await cur.fetchall() if hasattr(cur, "fetchall") else []
                                desc = (
                                    tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                                    if getattr(cur, "description", None)
                                    else None
                                )
                                return [tuple(r) for r in rows], desc
                if hasattr(self._pool, "execute_with_description"):
                    res = self._pool.execute_with_description(sql, parameters)
                    if inspect.iscoroutine(res):
                        awaited_res: tuple[
                            list[tuple[Any, ...]],
                            tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                        ] = await res
                        return awaited_res
                    typed_res: tuple[
                        list[tuple[Any, ...]],
                        tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                    ] = res
                    return typed_res
                if hasattr(self._pool, "execute_tuples"):
                    res_tuples = self._pool.execute_tuples(sql, parameters)
                    tuple_rows: list[tuple[Any, ...]]
                    if inspect.iscoroutine(res_tuples):
                        tuple_rows = await res_tuples
                    else:
                        tuple_rows = res_tuples
                    return tuple_rows, None

            if not self.config.dsn.startswith("memory://") and self._sync._executor is None:
                self._ensure_async_pool()

            return self._sync.execute_with_description(sql, parameters)
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
        """Asynchronously execute a SELECT query and return rows as dictionaries or hydrated schema_type instances."""
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
        """Asynchronously execute a SELECT query and return the first row or raise ValueError."""
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
        """Asynchronously execute a SELECT query and return the first row or None."""
        rows = await self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        return rows[0] if rows else None

    async def load_from_arrow(  # type: ignore[override]
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Asynchronously bulk-load an Arrow Table, RecordBatch, ArrowCursor, or row sequence into table_name."""
        tuple_rows, col_names = _table_to_rows_and_cols(data, columns)
        if not tuple_rows:
            return 0
        placeholders = ", ".join("%s" for _ in col_names)
        cols_sql = ", ".join(col_names)
        sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})"
        for row in tuple_rows:
            await self.execute_tuples(sql, row)
        return len(tuple_rows)

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

    async def stream_arrow_batches(
        self,
        sql: str,
        columns: Sequence[str],
        *,
        batch_size: int = 1024,
    ) -> Iterator[Any]:
        """Asynchronously execute a query via binary COPY and yield batch chunks."""
        return self._sync.stream_arrow_batches(sql, columns, batch_size=batch_size)

    async def commit(self) -> None:
        """Commit the active transaction."""
        self._sync.commit()

    async def rollback(self) -> None:
        """Roll back the active transaction."""
        self._sync.rollback()

    async def close(self) -> None:
        """Close the driver and underlying resources."""
        self._closed = True
        if self._pool is not None and hasattr(self._pool, "close"):
            res = self._pool.close()
            if inspect.iscoroutine(res):
                await res
        self._sync.close()

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return SyncPostgreSQLDriver.strip_jsonb_header(raw)

    def _ensure_async_pool(self) -> Any:
        """Ensure async connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        if self.config.dsn.startswith("memory://"):
            return None
        asyncpg_mod = import_optional("asyncpg")
        if asyncpg_mod is not None:
            return asyncpg_mod
        return ensure_module(
            "psycopg_pool",
            package="asyncpg",
            install_package="asyncpg",
            extra="postgres",
        )
