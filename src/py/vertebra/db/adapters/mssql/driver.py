"""Synchronous and asynchronous pure-Rust SQL Server (TDS 7.4) drivers."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Self

import msgspec

from vertebra._vertebra import NotFoundError
from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mssql.core import (
    MSSQL_DIALECT_TAG,
    MSSQLCore,
)
from vertebra.db.adapters.mssql.pipeline import AsyncMSSQLPipeline, SyncMSSQLPipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
)
from vertebra.db.driver._async import AsyncDatabaseDriver
from vertebra.db.driver._sync import SyncDatabaseDriver
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._mssql import AsyncMSSQLClient, SyncMSSQLClient
    from vertebra.db.dialer.cloudsql import AsyncCloudSQLDialer, SyncCloudSQLDialer

__all__ = (
    "AsyncMSSQLDriver",
    "SyncMSSQLDriver",
)


def _resolve_mssql_config(
    config: MSSQLConfig | str | None,
    overrides: dict[str, Any],
    dialer: Any = None,
) -> MSSQLConfig:
    """Resolve `config`, keyword `overrides`, and optional `dialer` into an `MSSQLConfig` instance."""
    if isinstance(config, str):
        resolved = MSSQLConfig.from_url(config, **overrides)
    elif isinstance(config, MSSQLConfig):
        resolved = msgspec.structs.replace(config, **overrides) if overrides else config
    else:
        dsn = overrides.pop("dsn", None)
        if isinstance(dsn, str) and dsn.startswith(("mssql://", "sqlserver://")):
            resolved = MSSQLConfig.from_url(dsn, **overrides)
        else:
            if dsn is not None:
                overrides["dsn"] = dsn
            resolved = MSSQLConfig(**overrides)

    if dialer is not None and not resolved.cloud_sql_instance:
        dialer_cfg = getattr(dialer, "config", None)
        instance_name = getattr(dialer_cfg, "instance_connection_name", None) if dialer_cfg is not None else None
        if instance_name:
            ip_type = str(getattr(dialer_cfg, "ip_type", resolved.cloudsql_ip_type))
            auth_type = "IAM" if getattr(dialer_cfg, "enable_iam_auth", False) else resolved.cloudsql_auth_type
            resolved = msgspec.structs.replace(
                resolved,
                cloud_sql_instance=str(instance_name),
                cloudsql_ip_type=ip_type,
                cloudsql_auth_type=auth_type,
            )
    return resolved


class SyncMSSQLDriver(SyncDatabaseDriver[MSSQLConfig]):
    """Synchronous SQL Server TDS 7.4 driver backed by `vertebra._mssql.SyncMSSQLClient`."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dialer",
        "_in_transaction",
        "_last_rewritten_sql",
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
        resolved = _resolve_mssql_config(config, overrides, dialer)
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        self._client: SyncMSSQLClient | Any = (
            client if client is not None else MSSQLCore.create_sync_rust_client(resolved, dialer=dialer)
        )
        self._in_transaction: bool = False
        self._last_rewritten_sql: str = ""

    @property
    def dialer(self) -> SyncCloudSQLDialer | None:
        """Return the attached Cloud SQL dialer if present."""
        return self._dialer

    @property
    def last_rewritten_sql(self) -> str:
        """Return the most recently rewritten `@P1` T-SQL statement."""
        return self._last_rewritten_sql

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

    def pipeline(self) -> SyncMSSQLPipeline:
        """Create a synchronous transactional query pipeline bound to this driver."""
        return SyncMSSQLPipeline(self)

    def connect(self) -> Self:
        """Ensure the underlying native `SyncMSSQLClient` is ready."""
        if self._client is None:
            self._client = MSSQLCore.create_sync_rust_client(self.config, dialer=self._dialer)
        self._closed = False
        return self

    def ping(self) -> bool:
        """Execute a lightweight health check against the SQL Server session."""
        return bool(self._client.ping())

    def begin(self, isolation_level: str | None = None) -> None:
        """Begin an explicit SQL Server transaction."""
        if isolation_level is not None:
            self._client.begin(isolation_level)
        else:
            self._client.begin()
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
        self._client.close()

    def begin(self) -> None:
        """Begin an explicit SQL Server transaction; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().begin()

    def commit(self) -> None:
        """Commit the active SQL Server transaction; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().commit()

    def rollback(self) -> None:
        """Roll back the active SQL Server transaction; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().rollback()

    def cancel(self) -> None:
        """Cancel an in-flight SQL Server query; raises ``MissingDependencyError`` without ``vertebra._mssql``."""
        _require_mssql_native()
        super().cancel()

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return affected row count."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            return int(self._client.execute(self._last_rewritten_sql, parameters))
        finally:
            self._release_mssql_lease(grant)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets and return total affected rows."""
        _ = kwargs
        if not parameter_sets:
            return 0
        grant = self._prepare_and_lease(sql)
        try:
            return int(self._client.execute_many(self._last_rewritten_sql, list(parameter_sets)))
        finally:
            self._release_mssql_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional tuples."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        return rows

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)`."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            if hasattr(self._client, "execute_with_description"):
                rows, cols = self._client.execute_with_description(
                    self._last_rewritten_sql,
                    parameters,
                )
                return list(rows), tuple(cols)
            raw = self._client.query(self._last_rewritten_sql, parameters)
            cols = tuple(str(c) for c in raw.get("columns", ()))
            rows = [tuple(r) for r in raw.get("rows", ())]
            return rows, cols
        finally:
            self._release_mssql_lease(grant)

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
        if schema_type is None or schema_type is dict:
            grant = self._prepare_and_lease(sql)
            try:
                return list(self._client.fetch_all(self._last_rewritten_sql, parameters))
            finally:
                self._release_mssql_lease(grant)
        rows, cols = self.execute_with_description(sql, parameters)
        return _hydrate_rows_with_schema(rows, cols, schema_type)

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
        if schema_type is None or schema_type is dict:
            grant = self._prepare_and_lease(sql)
            try:
                return self._client.fetch_one(self._last_rewritten_sql, parameters)
            finally:
                self._release_mssql_lease(grant)
        rows = self.select(sql, parameters, schema_type=schema_type)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

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
        if schema_type is None or schema_type is dict:
            grant = self._prepare_and_lease(sql)
            try:
                return self._client.fetch_optional(self._last_rewritten_sql, parameters)
            finally:
                self._release_mssql_lease(grant)
        rows = self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

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
        grant = self._prepare_and_lease(sql)
        try:
            val = self._client.select_value(self._last_rewritten_sql, parameters)
        finally:
            self._release_mssql_lease(grant)
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
        grant = self._prepare_and_lease(sql)
        try:
            raw_cursor = self._client.select_to_arrow(self._last_rewritten_sql, parameters)
            if isinstance(raw_cursor, ArrowCursor):
                return raw_cursor
            return ArrowCursor(raw_cursor)
        finally:
            self._release_mssql_lease(grant)

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
        grant = self._prepare_and_lease(sql)
        try:
            raw_batches = self._client.stream_arrow_batches(
                self._last_rewritten_sql,
                parameters,
                chunk,
            )
        finally:
            self._release_mssql_lease(grant)
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
        grant = self._prepare_and_lease(f"INSERT BULK {table_name}")
        try:
            return int(self._client.load_from_arrow(table_name, source, col_list))
        finally:
            self._release_mssql_lease(grant)

    def lease_stats(self) -> dict[str, int]:
        """Return broker connection lease telemetry counters."""
        return dict(self._client.lease_stats())

    def statement_cache_stats(self) -> dict[str, int] | None:
        """Return broker statement cache telemetry counters."""
        stats = self._client.statement_cache_stats()
        return dict(stats) if stats is not None else None

    def _prepare_and_lease(self, sql: str) -> Any:
        sc_attr = getattr(self._broker, "statement_cache", None) if self._broker is not None else None
        sc = sc_attr() if callable(sc_attr) else sc_attr
        if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
            sql_bytes = sql.encode("utf-8")
            try:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql)
                use_bytes_key = False
            except TypeError:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql_bytes)
                use_bytes_key = True
            if cached is not None and cached.compiled_sql:
                rewritten = cached.compiled_sql.decode("utf-8")
            else:
                rewritten, count = MSSQLCore.rewrite_placeholders(sql)
                sc.insert(
                    MSSQL_DIALECT_TAG,
                    sql_bytes if use_bytes_key else sql,
                    count,
                    rewritten.encode("utf-8"),
                    b"",
                )
        else:
            rewritten, _ = MSSQLCore.rewrite_placeholders(sql)

        self._last_rewritten_sql = rewritten

        if self._broker is not None and hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if self._broker is not None and hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="mssql")
        return None

    def _release_mssql_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)


class AsyncMSSQLDriver(AsyncDatabaseDriver[MSSQLConfig]):
    """Asynchronous SQL Server TDS 7.4 driver backed by `vertebra._mssql.AsyncMSSQLClient`."""

    dialect: ClassVar[str] = "mssql"
    dialect_tag: ClassVar[int] = MSSQL_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dialer",
        "_in_transaction",
        "_last_rewritten_sql",
    )

    def __init__(
        self,
        config: MSSQLConfig | str | None = None,
        *,
        dialer: SyncCloudSQLDialer | None = None,
        broker: Any = None,
        client: AsyncMSSQLClient | Any | None = None,
        **overrides: Any,
    ) -> None:
        resolved = _resolve_mssql_config(config, overrides, dialer)
        super().__init__(resolved, broker=broker)
        self._dialer = dialer
        self._client: AsyncMSSQLClient | Any = (
            client if client is not None else MSSQLCore.create_async_rust_client(resolved, dialer=dialer)
        )
        self._in_transaction: bool = False
        self._last_rewritten_sql: str = ""

    @property
    def dialer(self) -> SyncCloudSQLDialer | None:
        """Return the Cloud SQL dialer attached to the paired sync driver."""
        return self._sync.dialer

    @property
    def last_rewritten_sql(self) -> str:
        """Return the most recently rewritten `@P1` T-SQL statement."""
        return self._last_rewritten_sql

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

    def pipeline(self) -> AsyncMSSQLPipeline:
        """Create an asynchronous transactional query pipeline bound to this driver."""
        return AsyncMSSQLPipeline(self)

    async def connect(self) -> Self:
        """Ensure the underlying native `AsyncMSSQLClient` is ready."""
        if self._client is None:
            self._client = MSSQLCore.create_async_rust_client(self.config, dialer=self._dialer)
        self._closed = False
        return self

    async def ping(self) -> bool:
        """Execute an asynchronous health check against the SQL Server session."""
        return bool(await self._client.ping())

    async def begin(self, isolation_level: str | None = None) -> None:
        """Begin an explicit SQL Server transaction."""
        if isolation_level is not None:
            await self._client.begin(isolation_level)
        else:
            await self._client.begin()
        self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active SQL Server transaction."""
        try:
            await self._client.commit()
        finally:
            self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active SQL Server transaction."""
        try:
            await self._client.rollback()
        finally:
            self._in_transaction = False

    async def savepoint(self, name: str) -> None:
        """Create a transaction savepoint (`SAVE TRANSACTION`)."""
        validate_identifier(name, "savepoint")
        await self._client.savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a transaction savepoint (`ROLLBACK TRANSACTION <name>`)."""
        validate_identifier(name, "savepoint")
        await self._client.rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        """Validate a savepoint identifier (no-op on T-SQL)."""
        validate_identifier(name, "savepoint")
        await self._client.release_savepoint(name)

    async def cancel(self) -> None:
        """Send an out-of-band TDS `ATTENTION` (`0x06`) packet to interrupt an in-flight query."""
        if not self._closed:
            await self._client.cancel()

    async def close(self) -> None:
        """Close the pooled TDS sessions and release driver resources."""
        self._closed = True
        self._in_transaction = False
        await self._client.close()

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement asynchronously and return affected row count."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            return int(await self._client.execute(self._last_rewritten_sql, parameters))
        finally:
            self._release_mssql_lease(grant)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets asynchronously."""
        _ = kwargs
        if not parameter_sets:
            return 0
        grant = self._prepare_and_lease(sql)
        try:
            return int(await self._client.execute_many(self._last_rewritten_sql, list(parameter_sets)))
        finally:
            self._release_mssql_lease(grant)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query asynchronously and return positional tuples."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        return rows

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL asynchronously and return `(rows, column_names)`."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            if hasattr(self._client, "execute_with_description"):
                rows, cols = await self._client.execute_with_description(
                    self._last_rewritten_sql,
                    parameters,
                )
                return list(rows), tuple(cols)
            raw = await self._client.query(self._last_rewritten_sql, parameters)
            cols = tuple(str(c) for c in raw.get("columns", ()))
            rows = [tuple(r) for r in raw.get("rows", ())]
            return rows, cols
        finally:
            self._release_mssql_lease(grant)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query asynchronously and return rows as dictionaries or `schema_type` instances."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            grant = self._prepare_and_lease(sql)
            try:
                return list(await self._client.fetch_all(self._last_rewritten_sql, parameters))
            finally:
                self._release_mssql_lease(grant)
        rows, cols = await self.execute_with_description(sql, parameters)
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first row, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            grant = self._prepare_and_lease(sql)
            try:
                return await self._client.fetch_one(self._last_rewritten_sql, parameters)
            finally:
                self._release_mssql_lease(grant)
        rows = await self.select(sql, parameters, schema_type=schema_type)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query asynchronously and return the first row or `None` when empty."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            grant = self._prepare_and_lease(sql)
            try:
                return await self._client.fetch_optional(self._last_rewritten_sql, parameters)
            finally:
                self._release_mssql_lease(grant)
        rows = await self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        grant = self._prepare_and_lease(sql)
        try:
            val = await self._client.select_value(self._last_rewritten_sql, parameters)
        finally:
            self._release_mssql_lease(grant)
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    async def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute a query asynchronously and return `(rows, total_count)`."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows, len(rows)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and materialize the result set as a zero-copy `VTBARROW` `ArrowCursor`."""
        _ = (threshold_rows, kwargs)
        grant = self._prepare_and_lease(sql)
        try:
            raw_cursor = await self._client.select_to_arrow(self._last_rewritten_sql, parameters)
            if isinstance(raw_cursor, ArrowCursor):
                return raw_cursor
            return ArrowCursor(raw_cursor)
        finally:
            self._release_mssql_lease(grant)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowCursor]:
        """Execute a query asynchronously and yield `VTBARROW` `ArrowCursor` chunks."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        grant = self._prepare_and_lease(sql)
        try:
            raw_batches = await self._client.stream_arrow_batches(
                self._last_rewritten_sql,
                parameters,
                chunk,
            )
        finally:
            self._release_mssql_lease(grant)
        for item in raw_batches:
            yield item if isinstance(item, ArrowCursor) else ArrowCursor(item)

    async def load_from_arrow(
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
        grant = self._prepare_and_lease(f"INSERT BULK {table_name}")
        try:
            return int(await self._client.load_from_arrow(table_name, source, col_list))
        finally:
            self._release_mssql_lease(grant)

    def lease_stats(self) -> dict[str, int]:
        """Return broker connection lease telemetry counters."""
        return dict(self._client.lease_stats())

    def statement_cache_stats(self) -> dict[str, int] | None:
        """Return broker statement cache telemetry counters."""
        stats = self._client.statement_cache_stats()
        return dict(stats) if stats is not None else None

    def _prepare_and_lease(self, sql: str) -> Any:
        sc_attr = getattr(self._broker, "statement_cache", None) if self._broker is not None else None
        sc = sc_attr() if callable(sc_attr) else sc_attr
        if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
            sql_bytes = sql.encode("utf-8")
            try:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql)
                use_bytes_key = False
            except TypeError:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql_bytes)
                use_bytes_key = True
            if cached is not None and cached.compiled_sql:
                rewritten = cached.compiled_sql.decode("utf-8")
            else:
                rewritten, count = MSSQLCore.rewrite_placeholders(sql)
                sc.insert(
                    MSSQL_DIALECT_TAG,
                    sql_bytes if use_bytes_key else sql,
                    count,
                    rewritten.encode("utf-8"),
                    b"",
                )
        else:
            rewritten, _ = MSSQLCore.rewrite_placeholders(sql)

        self._last_rewritten_sql = rewritten

        if self._broker is not None and hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if self._broker is not None and hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="mssql")
        return None

    def _release_mssql_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)
