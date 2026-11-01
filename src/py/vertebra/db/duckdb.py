"""Embedded DuckDB OLAP driver with SHM single-writer/multi-reader coordination and Arrow IPC streaming."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import threading
from typing import TYPE_CHECKING, Any

from vertebra.db.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
    read_arrow_shm,
    write_arrow_shm,
)
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "AsyncDuckDBDriver",
    "DUCKDB_DIALECT_TAG",
    "DuckDBConfig",
    "DuckDBSecretConfig",
    "SyncDuckDBDriver",
    "duckdb_bootstrap_statements",
    "read_arrow_shm",
    "write_arrow_shm",
)

DUCKDB_DIALECT_TAG = 6


class DuckDBSecretConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Cloud storage secret configuration (`CREATE SECRET`) for DuckDB `httpfs` / S3 / GCS / R2."""

    name: str = "default_s3"
    """Unique DuckDB secret identifier."""
    secret_type: str = "S3"
    """DuckDB secret type (`"S3"`, `"GCS"`, `"R2"`, or `"AZURE"`)."""
    key_id: str = ""
    """Cloud storage access key ID (`KEY_ID`)."""
    secret: str = ""
    """Cloud storage secret access key (`SECRET`)."""
    region: str = "us-east-1"
    """Cloud storage region (`REGION`)."""
    endpoint: str | None = None
    """Optional custom S3-compatible endpoint (`ENDPOINT`, e.g. RustFS or MinIO)."""

    def to_create_secret_sql(self) -> str:
        """Render a `CREATE OR REPLACE SECRET` SQL statement for DuckDB."""
        parts = [f"TYPE {self.secret_type}"]
        if self.key_id:
            parts.append(f"KEY_ID '{self.key_id}'")
        if self.secret:
            parts.append(f"SECRET '{self.secret}'")
        if self.region:
            parts.append(f"REGION '{self.region}'")
        if self.endpoint:
            parts.append(f"ENDPOINT '{self.endpoint}'")
        inner = ", ".join(parts)
        return f"CREATE OR REPLACE SECRET {self.name} ({inner})"


class DuckDBConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Configuration for the embedded DuckDB analytical engine."""

    path: str = ":memory:"
    """Filesystem path to the DuckDB database file or `":memory:"`."""
    database: str = ":memory:"
    """Fallback database path alias used when `path` is left at `":memory:"`."""
    memory_limit_mb: int = 512
    """Maximum buffer manager memory allowance in MiB (`SET memory_limit`)."""
    threads: int = 4
    """Number of DuckDB execution threads (`SET threads`)."""
    parquet_metadata_cache: bool = True
    """When True, enable `SET parquet_metadata_cache = true` after connection bootstrap."""
    httpfs_connection_caching: bool = True
    """When True, keep remote HTTP/S3 object storage connections warm (`SET http_keep_alive = true`)."""
    enable_http_metadata_cache: bool = True
    """When True, cache remote HTTP/Parquet `HEAD` metadata (`SET enable_http_metadata_cache = true`)."""
    read_only: bool = False
    """When True, open the DuckDB file in read-only mode allowing multi-process readers."""
    secrets: tuple[DuckDBSecretConfig, ...] = ()
    """Optional cloud storage secrets (`CREATE OR REPLACE SECRET`) executed at connection bootstrap."""
    pool_min_size: int = 1
    """Minimum number of pooled DuckDB cursors per worker."""
    pool_max_size: int = 10
    """Maximum number of pooled DuckDB cursors per worker."""
    statement_cache_size: int = 256
    """Maximum number of prepared analytical statements cached per worker."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating single-writer/multi-reader leases."""
    gcs_token_cache: Any = None
    """Optional `GcpTokenCache` used to rotate session-local GCS `BEARER_TOKEN` secrets on token refresh."""

    def resolved_path(self) -> str:
        """Resolve the effective database path from `path` or `database`."""
        if self.path != ":memory:":
            return self.path
        return self.database

    def bootstrap_statements(self) -> list[str]:
        """Return post-initialization `SET` statements (`parquet_metadata_cache` is always post-init SQL)."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "duckdb_bootstrap_statements"):
            raw_stmts = native.duckdb_bootstrap_statements(
                self.resolved_path(),
                self.memory_limit_mb,
                self.threads,
                self.parquet_metadata_cache,
                self.httpfs_connection_caching,
                self.enable_http_metadata_cache,
            )
            stmts = [s.replace("SET httpfs_connection_caching = ", "SET http_keep_alive = ") for s in raw_stmts]
        else:
            parquet_flag = "true" if self.parquet_metadata_cache else "false"
            keep_alive_flag = "true" if self.httpfs_connection_caching else "false"
            http_meta_flag = "true" if self.enable_http_metadata_cache else "false"
            stmts = [
                f"SET memory_limit = '{self.memory_limit_mb}MB'",
                f"SET threads = {self.threads}",
                f"SET parquet_metadata_cache = {parquet_flag}",
                f"SET http_keep_alive = {keep_alive_flag}",
                f"SET enable_http_metadata_cache = {http_meta_flag}",
            ]
        for secret in self.secrets:
            stmts.append(secret.to_create_secret_sql())
        return stmts


def duckdb_bootstrap_statements(config: DuckDBConfig | None = None) -> list[str]:
    """Generate post-init DuckDB `SET` statements for the given `DuckDBConfig`."""
    cfg = config or DuckDBConfig()
    return cfg.bootstrap_statements()


def _is_write_statement(sql: str) -> bool:
    """Return True if the SQL statement mutates state and requires the single-writer lock."""
    head = sql.lstrip().split(None, 1)
    if not head:
        return False
    verb = head[0].upper()
    return verb in {"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "COPY", "MERGE", "TRUNCATE", "ATTACH"}


class SyncDuckDBDriver:
    """Synchronous DuckDB analytical driver with single-writer coordination and SHM Arrow streaming."""

    def __init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or DuckDBConfig()
        self._broker = broker if broker is not None else self.config.broker
        self._writer_lock = threading.RLock()
        self.applied_token_generation: int = 0
        self.last_gcs_secret_sql: str = ""
        self.executed_bootstrap_statements: tuple[str, ...] = tuple(self.config.bootstrap_statements())

        duckdb_mod = import_optional("duckdb")
        self._using_native_duckdb = duckdb_mod is not None
        if duckdb_mod is not None:
            self._conn = duckdb_mod.connect(self.config.resolved_path(), read_only=self.config.read_only)
            for stmt in self.executed_bootstrap_statements:
                with contextlib.suppress(RuntimeError, ValueError, OSError):
                    self._conn.execute(stmt)
        else:
            db_path = self.config.resolved_path()
            if db_path.startswith("duckdb://"):
                db_path = db_path[len("duckdb://") :] or ":memory:"
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DDL or DML statement under single-writer coordination."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                if not self._using_native_duckdb:
                    self._conn.commit()
                rowcount = getattr(cur, "rowcount", 0)
                return max(int(rowcount or 0), 0)
        finally:
            self._release_broker_lease(grant)

    def select(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute an analytical `SELECT` query and return rows as dictionaries or `schema_type` instances."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            if _is_write_statement(sql):
                with self._writer_lock:
                    cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                    rows = cur.fetchall()
                    cols = [col[0] for col in (cur.description or ())]
            else:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                rows = cur.fetchall()
                cols = [col[0] for col in (cur.description or ())]
            if not rows:
                return []
            tuple_rows = [tuple(row) for row in rows]
            return _hydrate_rows_with_schema(tuple_rows, cols, schema_type)
        finally:
            self._release_broker_lease(grant)

    def select_one(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `SELECT` query and return the first row or raise `ValueError`."""
        rows = self.select(sql, params, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `SELECT` query and return the first row or `None`."""
        rows = self.select(sql, params, schema_type=schema_type)
        return rows[0] if rows else None

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        self._sync_gcs_token_secret()
        rows, cols = _table_to_rows_and_cols(data, columns)
        if not rows or not cols:
            return 0
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        grant = self._acquire_broker_lease(sql, len(cols))
        try:
            with self._writer_lock:
                self._conn.executemany(sql, rows)
                if not self._using_native_duckdb:
                    self._conn.commit()
                return len(rows)
        finally:
            self._release_broker_lease(grant)

    def execute_tuples(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                rows = [tuple(row) for row in cur.fetchall()]
                if not self._using_native_duckdb:
                    self._conn.commit()
                return rows
        finally:
            self._release_broker_lease(grant)

    def execute_with_description(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None, int]:
        """Execute SQL and return `(rows, description, rowcount)` for DBAPI compatibility."""
        self._sync_gcs_token_secret()
        bound = tuple(params) if isinstance(params, (list, tuple)) else (params or ())
        grant = self._acquire_broker_lease(sql, len(bound) if isinstance(bound, tuple) else 0)
        try:
            with self._writer_lock:
                cur = self._conn.execute(sql, bound) if bound else self._conn.execute(sql)
                if cur.description is not None:
                    rows = [tuple(row) for row in cur.fetchall()]
                    desc = tuple((col[0], None, None, None, None, None, None) for col in cur.description)
                    return rows, desc, len(rows)
                if not self._using_native_duckdb:
                    self._conn.commit()
                return [], None, max(int(getattr(cur, "rowcount", 0) or 0), 0)
        finally:
            self._release_broker_lease(grant)

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = 1,
    ) -> ArrowCursor:
        """Stream analytical query results into a zero-copy `ShmRegion`-backed `ArrowCursor`."""
        rows = self.select(sql, params)
        return ArrowCursor.from_rows(rows, threshold_rows=shm_threshold_rows)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._conn.close()

    def _sync_gcs_token_secret(self) -> None:
        """Ensure this DuckDB session's GCS `BEARER_TOKEN` secret matches the latest `GcpTokenCache` generation."""
        cache = self.config.gcs_token_cache
        if cache is None:
            return
        if (
            self.applied_token_generation == 0
            or cache.needs_refresh()
            or cache.generation != self.applied_token_generation
        ):
            snapshot = cache.get_token()
            if int(snapshot.generation) != self.applied_token_generation:
                escaped_token = str(snapshot.access_token).replace("'", "''")
                secret_sql = f"CREATE OR REPLACE SECRET vertebra_gcs_adc (TYPE GCS, BEARER_TOKEN '{escaped_token}')"
                self.last_gcs_secret_sql = secret_sql
                self.applied_token_generation = int(snapshot.generation)
                if self._using_native_duckdb:
                    with self._writer_lock, contextlib.suppress(RuntimeError, ValueError, OSError):
                        self._conn.execute(secret_sql)

    def _acquire_broker_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            cached = self._broker.statement_cache.lookup(DUCKDB_DIALECT_TAG, sql)
            if cached is None:
                self._broker.statement_cache.insert(
                    DUCKDB_DIALECT_TAG,
                    sql,
                    param_count,
                    sql.encode("utf-8"),
                    b"",
                )
        if hasattr(self._broker, "lease_table"):
            return self._broker.lease_table.acquire(os.getpid())
        return None

    def _release_broker_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None and hasattr(self._broker, "lease_table"):
            self._broker.lease_table.release(grant.slot_index, grant.generation)


class AsyncDuckDBDriver:
    """Asynchronous DuckDB analytical driver with single-writer coordination and SHM Arrow streaming."""

    def __init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None:
        self.config = config or DuckDBConfig()
        self._sync = SyncDuckDBDriver(self.config, broker=broker)

    @property
    def executed_bootstrap_statements(self) -> tuple[str, ...]:
        """Return the post-initialization `SET` statements executed on startup."""
        return self._sync.executed_bootstrap_statements

    @property
    def applied_token_generation(self) -> int:
        """Return the token generation currently applied to this DuckDB session."""
        return self._sync.applied_token_generation

    @property
    def last_gcs_secret_sql(self) -> str:
        """Return the last `CREATE OR REPLACE SECRET` SQL statement applied for GCS ADC auth."""
        return self._sync.last_gcs_secret_sql

    async def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        """Execute a DDL or DML statement asynchronously."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.execute, sql, params)

    async def select(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute an analytical `SELECT` query asynchronously."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.select, sql, params, schema_type=schema_type)

    async def select_one(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `SELECT` query asynchronously and return the first row or raise `ValueError`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.select_one, sql, params, schema_type=schema_type)

    async def select_one_or_none(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `SELECT` query asynchronously and return the first row or `None`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.select_one_or_none, sql, params, schema_type=schema_type)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(self._sync.load_from_arrow, table_name, data, columns=columns)

    async def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        shm_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        await self._ensure_gcs_token_async()
        return await asyncio.to_thread(
            self._sync.select_to_arrow,
            sql,
            params,
            shm_threshold_rows=shm_threshold_rows,
        )

    async def close(self) -> None:
        """Close the underlying DuckDB connection."""
        await asyncio.to_thread(self._sync.close)

    async def _ensure_gcs_token_async(self) -> None:
        """Pre-refresh the GCS ADC token on the Rust Tokio runtime before dispatching DuckDB work."""
        cache = self.config.gcs_token_cache
        if cache is not None and cache.needs_refresh() and hasattr(cache, "get_token_async"):
            await cache.get_token_async()
