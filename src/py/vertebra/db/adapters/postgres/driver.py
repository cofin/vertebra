"""Synchronous and asynchronous PostgreSQL drivers."""

from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import AsyncIterator, Awaitable, Buffer, Callable, Iterator, Mapping, Sequence
from typing import Any, ClassVar

from anyio import CancelScope

from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import (
    POSTGRES_DIALECT_TAG,
    AsyncDatabaseDriver,
    SyncDatabaseDriver,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

__all__ = (
    "AsyncPostgreSQLDriver",
    "SyncPostgreSQLDriver",
)


class SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]):
    """Synchronous PostgreSQL driver facade supporting plain-tuple, zero-copy JSONB, and COPY streaming."""

    dialect: ClassVar[str] = "postgres"
    dialect_tag: ClassVar[int] = POSTGRES_DIALECT_TAG

    __slots__ = (
        "_dialer",
        "_executor",
        "_in_transaction",
        "_pool",
        "_tx_conn",
        "_tx_conn_ctx",
    )

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
        _reject_memory_dsn(resolved.dsn)
        super().__init__(resolved, broker=broker)
        self._pool = pool
        self._dialer = dialer
        self._executor = executor
        self._in_transaction: bool = False
        self._tx_conn: Any = None
        self._tx_conn_ctx: Any = None

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

    def begin(self) -> None:
        """Begin an explicit transaction and pin a pooled connection when available."""
        self._in_transaction = True
        if self._tx_conn is not None:
            return
        pool = self._pool
        if pool is None and self._executor is None:
            pool = self._ensure_pool()
        if pool is not None and hasattr(pool, "connection"):
            conn_obj = pool.connection()
            if hasattr(conn_obj, "__enter__"):
                self._tx_conn_ctx = conn_obj
                self._tx_conn = conn_obj.__enter__()
            else:
                self._tx_conn = conn_obj

    def commit(self) -> None:
        """Commit the active transaction."""
        try:
            if self._tx_conn is not None and hasattr(self._tx_conn, "commit"):
                self._tx_conn.commit()
            elif self._pool is not None and hasattr(self._pool, "commit"):
                self._pool.commit()
        finally:
            self._release_tx_conn()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        try:
            if self._tx_conn is not None and hasattr(self._tx_conn, "rollback"):
                self._tx_conn.rollback()
            elif self._pool is not None and hasattr(self._pool, "rollback"):
                self._pool.rollback()
        finally:
            self._release_tx_conn()

    def close(self) -> None:
        """Close the driver and release the connection pool."""
        self._closed = True
        self._release_tx_conn()
        if self._pool is not None and hasattr(self._pool, "close"):
            self._pool.close()

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
        """Execute a SQL statement returning tuple rows and column names."""
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

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Execute a query via binary COPY or cursor fallback and yield Arrow RecordBatches."""
        chunk_size = max(1, int(batch_size))
        rows: list[tuple[Any, ...]] = []
        discovered_cols: tuple[str, ...] = ()

        if self._pool is not None and hasattr(self._pool, "copy_binary_stream"):
            stream = self._pool.copy_binary_stream(sql)
            rows = self.parse_pgcopy_binary_rows(stream)
        else:
            pool = self._pool
            if pool is None and self._executor is None:
                pool = self._ensure_pool()
            used_copy = False
            if pool is not None and not parameters and hasattr(pool, "connection"):
                conn_obj = pool.connection()
                conn_ctx = conn_obj if hasattr(conn_obj, "__enter__") else None
                try:
                    active_conn = conn_obj.__enter__() if conn_ctx is not None else conn_obj
                    cur = active_conn.cursor()
                    try:
                        if hasattr(cur, "copy"):
                            used_copy = True
                            buf = bytearray()
                            with cur.copy(f"COPY ({sql}) TO STDOUT (FORMAT BINARY)") as copy_stream:
                                for part in copy_stream:
                                    buf.extend(part)
                            rows = self.parse_pgcopy_binary_rows(buf)
                    finally:
                        if hasattr(cur, "close"):
                            cur.close()
                finally:
                    if conn_ctx is not None:
                        conn_ctx.__exit__(None, None, None)

            if not used_copy:
                rows, discovered_cols = self.execute_with_description(sql, parameters, **kwargs)

        cols = (
            tuple(columns)
            if columns is not None
            else (discovered_cols if discovered_cols else tuple(f"col_{i}" for i in range(len(rows[0]) if rows else 0)))
        )
        pa = import_optional("pyarrow")
        if not rows:
            if columns is not None:
                if pa is not None:
                    pydict_empty: dict[str, list[Any]] = {col: [] for col in cols}
                    yield pa.RecordBatch.from_pydict(pydict_empty)
                else:
                    yield []
            return

        for idx in range(0, len(rows), chunk_size):
            batch_rows = rows[idx : idx + chunk_size]
            if pa is not None:
                pydict_chunk: dict[str, list[Any]] = {
                    col: [row[col_idx] if col_idx < len(row) else None for row in batch_rows]
                    for col_idx, col in enumerate(cols)
                }
                yield pa.RecordBatch.from_pydict(pydict_chunk)
            else:
                yield batch_rows

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an Arrow Table, RecordBatch, ArrowCursor, or row sequence into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        tuple_rows, col_names = _table_to_rows_and_cols(data, columns)
        for col in col_names:
            validate_identifier(str(col), "column", allow_dots=False)
        if not tuple_rows or not col_names:
            return 0
        placeholders = ", ".join("%s" for _ in col_names)
        quoted_table = quote_dialect_identifier(table_name, self.dialect)
        cols_sql = ", ".join(quote_dialect_identifier(str(col), self.dialect) for col in col_names)
        sql = f"INSERT INTO {quoted_table} ({cols_sql}) VALUES ({placeholders})"
        for row in tuple_rows:
            self.execute(sql, row)
        return len(tuple_rows)

    def _release_tx_conn(self) -> None:
        ctx = self._tx_conn_ctx
        self._tx_conn = None
        self._tx_conn_ctx = None
        self._in_transaction = False
        if ctx is not None:
            ctx.__exit__(None, None, None)

    def _execute_on_sync_connection(
        self,
        active_conn: Any,
        sql: str,
        params_arg: Any,
        *,
        auto_commit_dml: bool,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        cur = active_conn.cursor()
        try:
            cur.execute(sql, params_arg)
            raw_desc = getattr(cur, "description", None)
            if raw_desc:
                rows = [tuple(r) for r in (cur.fetchall() if hasattr(cur, "fetchall") else ())]
                cols = self._extract_column_names(raw_desc)
                return rows, cols, len(rows)
            if auto_commit_dml and not self._in_transaction and hasattr(active_conn, "commit"):
                active_conn.commit()
            rowcount = getattr(cur, "rowcount", 0)
            return [], (), max(int(rowcount or 0), 0)
        finally:
            if hasattr(cur, "close"):
                cur.close()

    def _run_sync_query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        *,
        auto_commit_dml: bool,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        params_arg: Any = () if parameters is None else parameters
        lease = self._acquire_pg_broker_lease(sql)
        try:
            if self._executor is not None:
                seq_params = tuple(parameters) if isinstance(parameters, Sequence) else ()
                exec_rows = [tuple(row) for row in self._executor(sql, seq_params)]
                return exec_rows, (), len(exec_rows)

            if self._tx_conn is not None:
                return self._execute_on_sync_connection(
                    self._tx_conn,
                    sql,
                    params_arg,
                    auto_commit_dml=False,
                )

            pool = self._pool
            if pool is None:
                pool = self._ensure_pool()

            if hasattr(pool, "connection"):
                conn = pool.connection()
                conn_ctx = conn if hasattr(conn, "__enter__") else None
                try:
                    active_conn = conn.__enter__() if conn_ctx is not None else conn
                    return self._execute_on_sync_connection(
                        active_conn,
                        sql,
                        params_arg,
                        auto_commit_dml=auto_commit_dml,
                    )
                finally:
                    if conn_ctx is not None:
                        conn_ctx.__exit__(None, None, None)
            if hasattr(pool, "execute_with_description"):
                raw_rows, raw_desc = pool.execute_with_description(sql, params_arg)
                tuple_rows = [tuple(r) for r in raw_rows]
                return tuple_rows, self._extract_column_names(raw_desc), len(tuple_rows)
            if hasattr(pool, "execute"):
                exec_res = pool.execute(sql, params_arg)
                raw_rows = exec_res.fetchall() if hasattr(exec_res, "fetchall") else list(exec_res)
                tuple_rows = [tuple(r) for r in raw_rows]
                raw_desc = getattr(exec_res, "description", None)
                return tuple_rows, self._extract_column_names(raw_desc), len(tuple_rows)

            msg = "PostgreSQL pool must provide connection(), execute_with_description(), or execute()"
            raise ImproperConfigurationError(msg)
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    def _format_guc_options(self) -> str:
        """Format session GUC options for psycopg connection parameters."""
        gucs = self.config.build_session_gucs()
        return " ".join(f"-c {k}={v}" for k, v in gucs.items())

    def _ensure_pool(self) -> Any:
        """Ensure connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
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
    dialect_tag: ClassVar[int] = POSTGRES_DIALECT_TAG

    __slots__ = (
        "_async_executor",
        "_dialer",
        "_in_transaction",
        "_pool",
        "_sync",
        "_tx_conn",
        "_tx_conn_ctx",
        "_tx_transaction",
    )

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
        _reject_memory_dsn(resolved.dsn)
        super().__init__(resolved, broker=broker)
        self._pool = pool
        self._dialer = dialer
        self._async_executor = async_executor
        self._in_transaction: bool = False
        self._tx_conn: Any = None
        self._tx_conn_ctx: Any = None
        self._tx_transaction: Any = None
        self._sync = SyncPostgreSQLDriver(
            self.config,
            pool=None if hasattr(pool, "acquire") else pool,
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

    async def begin(self) -> None:
        """Begin an explicit async transaction and pin a pooled connection when available."""
        self._in_transaction = True
        if self._tx_conn is not None:
            return
        if self._pool is not None and hasattr(self._pool, "acquire"):
            ctx = self._pool.acquire()
            try:
                self._tx_conn = await ctx.__aenter__()
                self._tx_conn_ctx = ctx
                if hasattr(self._tx_conn, "fetch") and hasattr(self._tx_conn, "transaction"):
                    self._tx_transaction = self._tx_conn.transaction()
                    await self._tx_transaction.start()
            except BaseException:
                with CancelScope(shield=True):
                    await self._release_tx_conn()
                raise
        else:
            await asyncio.to_thread(self._sync.begin)

    async def commit(self) -> None:
        """Commit the active transaction."""
        try:
            if self._tx_transaction is not None:
                await self._tx_transaction.commit()
            elif self._tx_conn is not None and hasattr(self._tx_conn, "commit"):
                res = self._tx_conn.commit()
                if inspect.isawaitable(res):
                    await res
            elif self._pool is not None and hasattr(self._pool, "commit"):
                res = self._pool.commit()
                if inspect.isawaitable(res):
                    await res
            else:
                await asyncio.to_thread(self._sync.commit)
        finally:
            await self._release_tx_conn()

    async def rollback(self) -> None:
        """Roll back the active transaction."""
        try:
            if self._tx_transaction is not None:
                await self._tx_transaction.rollback()
            elif self._tx_conn is not None and hasattr(self._tx_conn, "rollback"):
                res = self._tx_conn.rollback()
                if inspect.isawaitable(res):
                    await res
            elif self._pool is not None and hasattr(self._pool, "rollback"):
                res = self._pool.rollback()
                if inspect.isawaitable(res):
                    await res
            else:
                await asyncio.to_thread(self._sync.rollback)
        finally:
            await self._release_tx_conn()

    async def close(self) -> None:
        """Close the driver and underlying resources."""
        self._closed = True
        if self._in_transaction:
            await self.rollback()
        else:
            await self._release_tx_conn()
        if self._pool is not None and hasattr(self._pool, "close"):
            res = self._pool.close()
            if inspect.iscoroutine(res):
                await res
        await asyncio.to_thread(self._sync.close)

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
        """Asynchronously execute a SQL statement returning tuple rows and column names."""
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

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Asynchronously execute a query via binary COPY or cursor fallback and yield batch chunks."""
        if self._pool is not None and hasattr(self._pool, "copy_binary_stream"):
            batches = await asyncio.to_thread(
                lambda: list(
                    self._sync.stream_arrow_batches(
                        sql,
                        parameters,
                        batch_size=batch_size,
                        columns=columns,
                        **kwargs,
                    )
                )
            )
            for batch in batches:
                yield batch
            return

        if self._pool is not None and hasattr(self._pool, "acquire"):
            chunk_size = max(1, int(batch_size))
            rows, discovered_cols = await self.execute_with_description(sql, parameters, **kwargs)
            cols = (
                tuple(columns)
                if columns is not None
                else (
                    discovered_cols
                    if discovered_cols
                    else tuple(f"col_{i}" for i in range(len(rows[0]) if rows else 0))
                )
            )
            pa = import_optional("pyarrow")
            if not rows:
                if columns is not None:
                    if pa is not None:
                        pydict_empty: dict[str, list[Any]] = {col: [] for col in cols}
                        yield pa.RecordBatch.from_pydict(pydict_empty)
                    else:
                        yield []
                return
            for idx in range(0, len(rows), chunk_size):
                batch_rows = rows[idx : idx + chunk_size]
                if pa is not None:
                    pydict_chunk: dict[str, list[Any]] = {
                        col: [row[col_idx] if col_idx < len(row) else None for row in batch_rows]
                        for col_idx, col in enumerate(cols)
                    }
                    yield pa.RecordBatch.from_pydict(pydict_chunk)
                else:
                    yield batch_rows
            return

        fallback_batches = await asyncio.to_thread(
            lambda: list(
                self._sync.stream_arrow_batches(
                    sql,
                    parameters,
                    batch_size=batch_size,
                    columns=columns,
                    **kwargs,
                )
            )
        )
        for batch in fallback_batches:
            yield batch

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-load an Arrow Table, RecordBatch, ArrowCursor, or row sequence into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        tuple_rows, col_names = _table_to_rows_and_cols(data, columns)
        for col in col_names:
            validate_identifier(str(col), "column", allow_dots=False)
        if not tuple_rows or not col_names:
            return 0
        placeholders = ", ".join("%s" for _ in col_names)
        quoted_table = quote_dialect_identifier(table_name, self.dialect)
        cols_sql = ", ".join(quote_dialect_identifier(str(col), self.dialect) for col in col_names)
        sql = f"INSERT INTO {quoted_table} ({cols_sql}) VALUES ({placeholders})"
        for row in tuple_rows:
            await self.execute(sql, row)
        return len(tuple_rows)

    async def _release_tx_conn(self) -> None:
        ctx = self._tx_conn_ctx
        self._tx_conn = None
        self._tx_conn_ctx = None
        self._tx_transaction = None
        self._in_transaction = False
        if ctx is not None:
            await ctx.__aexit__(None, None, None)

    async def _execute_on_async_connection(
        self,
        conn: Any,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        *,
        auto_commit_dml: bool,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        params_arg: Any = () if parameters is None else parameters
        if hasattr(conn, "fetch"):
            seq_params = tuple(parameters) if isinstance(parameters, Sequence) else ()
            if auto_commit_dml and hasattr(conn, "execute"):
                status = await conn.execute(sql, *seq_params)
                count = (status or "").rsplit(" ", 1)[-1]
                return [], (), int(count) if count.isdecimal() else 0
            records = await conn.fetch(sql, *seq_params)
            keys_fn = getattr(records[0], "keys", None) if records else None
            cols = tuple(str(k) for k in keys_fn()) if callable(keys_fn) else ()
            rows = [tuple(r.values() if hasattr(r, "values") else r) for r in records]
            return rows, cols, len(rows)
        if hasattr(conn, "cursor"):
            async with conn.cursor() as cur:
                await cur.execute(sql, params_arg)
                raw_desc = getattr(cur, "description", None)
                if raw_desc:
                    fetched = await cur.fetchall() if hasattr(cur, "fetchall") else []
                    rows = [tuple(r) for r in fetched]
                    return rows, self._extract_column_names(raw_desc), len(rows)
                if auto_commit_dml and not self._in_transaction and hasattr(conn, "commit"):
                    commit_res = conn.commit()
                    if inspect.isawaitable(commit_res):
                        await commit_res
                rowcount = getattr(cur, "rowcount", 0)
                return [], (), max(int(rowcount or 0), 0)
        return [], (), 0

    async def _run_async_query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
        *,
        auto_commit_dml: bool,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        params_arg: Any = () if parameters is None else parameters
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
                tuple_rows = [tuple(r) for r in exec_rows]
                return tuple_rows, (), len(tuple_rows)

            if self._tx_conn is not None:
                return await self._execute_on_async_connection(
                    self._tx_conn,
                    sql,
                    parameters,
                    auto_commit_dml=auto_commit_dml,
                )

            if self._pool is not None:
                if hasattr(self._pool, "acquire"):
                    async with self._pool.acquire() as conn:
                        return await self._execute_on_async_connection(
                            conn,
                            sql,
                            parameters,
                            auto_commit_dml=auto_commit_dml,
                        )
                if hasattr(self._pool, "execute_with_description"):
                    res = self._pool.execute_with_description(sql, params_arg)
                    raw_rows, raw_desc = await res if inspect.iscoroutine(res) else res
                    tuple_rows = [tuple(r) for r in raw_rows]
                    return tuple_rows, self._extract_column_names(raw_desc), len(tuple_rows)
                if hasattr(self._pool, "execute_tuples"):
                    res_tuples = self._pool.execute_tuples(sql, params_arg)
                    tuple_rows = await res_tuples if inspect.iscoroutine(res_tuples) else res_tuples
                    return list(tuple_rows), (), len(tuple_rows)

            if self._sync._executor is None and self._pool is None:
                self._ensure_async_pool()

            return await asyncio.to_thread(
                self._sync._run_sync_query,
                sql,
                parameters,
                auto_commit_dml=auto_commit_dml,
            )
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    def _ensure_async_pool(self) -> Any:
        """Ensure async connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        asyncpg_mod = import_optional("asyncpg")
        if asyncpg_mod is not None:
            return asyncpg_mod
        return ensure_module(
            "psycopg_pool",
            package="asyncpg",
            install_package="asyncpg",
            extra="postgres",
        )


def _reject_memory_dsn(dsn: str) -> None:
    """Reject in-process memory:// DSNs.

    Args:
        dsn: Configured PostgreSQL DSN.

    Raises:
        ImproperConfigurationError: When ``dsn`` starts with ``memory://`` (case-insensitive).
    """
    if dsn.strip().lower().startswith("memory://"):
        msg = "PostgreSQL drivers do not support memory:// DSNs; pass a postgresql:// DSN or inject pool="
        raise ImproperConfigurationError(msg)
