"""PostgreSQL driver, PgBouncer/PgCat/pgdog pooler config, and zero-copy JSONB passthrough."""

from __future__ import annotations

import inspect
import struct
from collections.abc import Awaitable, Buffer, Callable, Iterator, Mapping, Sequence
from typing import Any, Literal

import msgspec

from vertebra.db.dialer.alloydb import AlloyDbConfig
from vertebra.db.dialer.cloudsql import CloudSqlConfig
from vertebra.utils.module_loader import ensure_module, import_optional

__all__ = (
    "PGCOPY_BINARY_SIGNATURE",
    "AsyncPostgresDriver",
    "PoolerMode",
    "PostgresConfig",
    "SyncPostgresDriver",
)

PGCOPY_BINARY_SIGNATURE: bytes = b"PGCOPY\n\xff\r\n\x00"

type PoolerMode = Literal["direct", "transaction", "session", "statement", "auto"]


class PostgresConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """PostgreSQL configuration supporting Cloud SQL, AlloyDB, and PgBouncer/PgCat/pgdog poolers."""

    dsn: str = "postgresql://localhost:5432/postgres"
    direct_dsn: str | None = None
    pooler_mode: PoolerMode = "direct"
    protocol_level_pooler_cache: bool = False
    max_prepared_statements: int | None = None
    min_size: int = 1
    max_size: int = 10
    cloud_sql_instance: str | None = None
    alloydb_instance_uri: str | None = None
    enable_iam_auth: bool = False
    enable_alloydb_columnar_engine: bool = False
    application_name: str = "vertebra"
    statement_timeout_ms: int | None = None
    idle_in_transaction_session_timeout_ms: int | None = None
    session_gucs: Mapping[str, str] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.min_size < 0 or self.max_size < 1 or self.min_size > self.max_size:
            msg = f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ValueError(msg)
        msgspec.structs.force_setattr(self, "session_gucs", dict(self.session_gucs))

    @property
    def effective_statement_cache_size(self) -> int:
        """Return the prepared statement cache capacity safe for the configured pooler mode.

        In transaction-mode or statement-mode PgBouncer/PgCat/pgdog, prepared statements must be disabled (`0`)
        unless protocol-level statement caching (`PgBouncer 1.21+` / `PgCat` / `pgdog`) is enabled.
        """
        if self.pooler_mode in ("transaction", "statement") and not self.protocol_level_pooler_cache:
            return 0
        return 256 if self.max_prepared_statements is None else max(0, self.max_prepared_statements)

    @property
    def effective_direct_dsn(self) -> str:
        """Return the direct primary DSN for LISTEN/NOTIFY, logical replication, and migrations."""
        return self.direct_dsn or self.dsn

    def derive_direct_config(self) -> PostgresConfig:
        """Return a session-mode PostgresConfig targeting ``effective_direct_dsn`` (`sqlspec>=0.68.0`)."""
        if not self.direct_dsn:
            return self
        base = msgspec.structs.asdict(self)
        base["dsn"] = self.direct_dsn
        base["pooler_mode"] = "session"
        base["min_size"] = min(1, self.max_size)
        base["max_size"] = min(2, self.max_size)
        base["session_gucs"] = dict(self.session_gucs)
        return PostgresConfig(**base)

    @staticmethod
    def compute_worker_pool_budget(
        max_connections: int,
        worker_count: int = 1,
        *,
        replica_count: int = 1,
        workers_per_replica: int | None = None,
        reserved_admin_connections: int = 5,
    ) -> int:
        """Compute per-worker pool size from a cluster-wide database connection budget."""
        usable = max(0, max_connections - reserved_admin_connections)
        per_replica = workers_per_replica if workers_per_replica is not None else worker_count
        total_shards = max(1, replica_count) * max(1, per_replica)
        return max(1, usable // total_shards)

    def build_session_gucs(self) -> dict[str, str]:
        """Build the startup/session GUC parameters for the PostgreSQL connection."""
        gucs: dict[str, str] = {"application_name": self.application_name}
        if self.enable_alloydb_columnar_engine or self.alloydb_instance_uri is not None:
            gucs["google_columnar_engine.enabled"] = "on"
            gucs["alloydb.enable_columnar_engine"] = "on"
        if self.statement_timeout_ms is not None:
            gucs["statement_timeout"] = str(self.statement_timeout_ms)
        if self.idle_in_transaction_session_timeout_ms is not None:
            gucs["idle_in_transaction_session_timeout"] = str(self.idle_in_transaction_session_timeout_ms)
        gucs.update(self.session_gucs)
        return gucs

    def to_cloud_dialer_config(self) -> CloudSqlConfig | AlloyDbConfig | None:
        """Return the zero-sidecar CloudSqlConfig or AlloyDbConfig when configured."""
        if self.alloydb_instance_uri:
            return AlloyDbConfig(
                instance_uri=self.alloydb_instance_uri,
                enable_iam_auth=self.enable_iam_auth,
                enable_columnar_engine=self.enable_alloydb_columnar_engine or True,
            )
        if self.cloud_sql_instance:
            return CloudSqlConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=self.enable_iam_auth,
            )
        return None


class SyncPostgresDriver:
    """Synchronous PostgreSQL driver facade supporting plain-tuple, zero-copy JSONB, and COPY streaming."""

    __slots__ = ("_closed", "_config", "_dialer", "_executor", "_in_memory_rows", "_pool")

    def __init__(
        self,
        config: PostgresConfig | None = None,
        *,
        pool: Any = None,
        dialer: Any = None,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self._config = config or PostgresConfig()
        self._pool = pool
        self._dialer = dialer
        self._executor = executor
        self._closed = False
        self._in_memory_rows: list[tuple[Any, ...]] = []

    @property
    def config(self) -> PostgresConfig:
        """Return the active PostgresConfig."""
        return self._config

    @property
    def dialer(self) -> Any:
        """Return the attached Cloud SQL or AlloyDB dialer if present."""
        return self._dialer

    @property
    def pool(self) -> Any:
        """Return the underlying connection pool if present."""
        return self._pool

    @property
    def closed(self) -> bool:
        """Return True if the driver has been closed."""
        return self._closed

    def _format_guc_options(self) -> str:
        """Format session GUC options for psycopg connection parameters."""
        gucs = self._config.build_session_gucs()
        return " ".join(f"-c {k}={v}" for k, v in gucs.items())

    def _ensure_pool(self) -> Any:
        """Ensure connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        if self._config.dsn.startswith("memory://"):
            return None
        psycopg_pool = ensure_module(
            "psycopg_pool",
            install_package="psycopg[binary,pool]",
            extra="postgres",
        )

        kwargs: dict[str, Any] = {
            "autocommit": False,
            "prepare_threshold": None if self._config.effective_statement_cache_size == 0 else 5,
        }
        guc_opts = self._format_guc_options()
        if guc_opts:
            kwargs["options"] = guc_opts

        self._pool = psycopg_pool.ConnectionPool(
            conninfo=self._config.dsn,
            min_size=self._config.min_size,
            max_size=self._config.max_size,
            kwargs=kwargs,
        )
        return self._pool

    @staticmethod
    def strip_jsonb_header(payload: Buffer) -> memoryview:
        """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
        view = memoryview(payload).cast("B")
        if len(view) == 0:
            msg = "Empty PostgreSQL binary JSONB payload"
            raise ValueError(msg)
        if view[0] != 0x01:
            msg = f"Unsupported PostgreSQL binary JSONB version byte: 0x{view[0]:02x}"
            raise ValueError(msg)
        return view[1:]

    @staticmethod
    def parse_pgcopy_binary_rows(payload: Buffer) -> list[tuple[bytes | None, ...]]:
        """Parse PostgreSQL binary COPY format stream into row tuples of byte values.

        The binary format begins with the 11-byte signature b"PGCOPY\\n\\xff\\r\\n\\x00",
        followed by a 32-bit integer flags word, and a 32-bit header extension area length.
        Each row is prefixed with a 16-bit field count. A field count of -1 indicates
        the file trailer (EOF). Each field is prefixed with a 32-bit length (-1 for SQL NULL).
        """
        raw = memoryview(payload)
        sig_len = len(PGCOPY_BINARY_SIGNATURE)
        if len(raw) < sig_len or bytes(raw[:sig_len]) != PGCOPY_BINARY_SIGNATURE:
            msg = "Invalid PGCOPY binary signature"
            raise ValueError(msg)

        if len(raw) < sig_len + 8:
            msg = "Truncated PGCOPY header"
            raise ValueError(msg)

        _flags, ext_len = struct.unpack_from(">II", raw, sig_len)
        pos = sig_len + 8 + ext_len
        if pos > len(raw):
            msg = "Corrupted PGCOPY header extension"
            raise ValueError(msg)

        rows: list[tuple[bytes | None, ...]] = []
        while pos + 2 <= len(raw):
            (field_count,) = struct.unpack_from(">h", raw, pos)
            pos += 2
            if field_count == -1:
                return rows
            if field_count < -1:
                msg = f"Invalid PGCOPY field count: {field_count}"
                raise ValueError(msg)

            fields: list[bytes | None] = []
            for _ in range(field_count):
                if pos + 4 > len(raw):
                    msg = "Truncated PGCOPY field length"
                    raise ValueError(msg)
                (field_len,) = struct.unpack_from(">i", raw, pos)
                pos += 4
                if field_len == -1:
                    fields.append(None)
                elif field_len >= 0:
                    if pos + field_len > len(raw):
                        msg = "Truncated PGCOPY field data"
                        raise ValueError(msg)
                    fields.append(bytes(raw[pos : pos + field_len]))
                    pos += field_len
                else:
                    msg = f"Invalid PGCOPY field length: {field_len}"
                    raise ValueError(msg)
            rows.append(tuple(fields))

        msg = "Unexpected EOF before PGCOPY trailer"
        raise ValueError(msg)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement returning rows and column descriptions."""
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

        if not self._config.dsn.startswith("memory://"):
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

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

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
        elif not self._config.dsn.startswith("memory://"):
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


class AsyncPostgresDriver:
    """Asynchronous PostgreSQL driver facade supporting plain-tuple, zero-copy JSONB, and COPY streaming."""

    __slots__ = ("_async_executor", "_closed", "_config", "_dialer", "_pool", "_sync")

    def __init__(
        self,
        config: PostgresConfig | None = None,
        *,
        pool: Any = None,
        dialer: Any = None,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
        async_executor: Callable[[str, Sequence[Any]], Awaitable[list[tuple[Any, ...]]]] | None = None,
    ) -> None:
        self._config = config or PostgresConfig()
        self._pool = pool
        self._dialer = dialer
        self._async_executor = async_executor
        self._closed = False
        self._sync = SyncPostgresDriver(self._config, pool=pool, dialer=dialer, executor=executor)

    @property
    def config(self) -> PostgresConfig:
        """Return the active PostgresConfig."""
        return self._config

    @property
    def dialer(self) -> Any:
        """Return the attached Cloud SQL or AlloyDB dialer if present."""
        return self._dialer

    @property
    def pool(self) -> Any:
        """Return the underlying connection pool if present."""
        return self._pool

    @property
    def closed(self) -> bool:
        """Return True if the driver has been closed."""
        return self._closed

    def _ensure_async_pool(self) -> Any:
        """Ensure async connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        if self._config.dsn.startswith("memory://"):
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

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement returning rows and column descriptions."""
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

        if not self._config.dsn.startswith("memory://") and self._sync._executor is None:
            self._ensure_async_pool()

        return self._sync.execute_with_description(sql, parameters)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        rows, _ = await self.execute_with_description(sql, parameters)
        return rows

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
        return SyncPostgresDriver.strip_jsonb_header(raw)
