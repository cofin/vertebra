"""Synchronous and asynchronous pure-Rust Google Cloud Spanner v1 drivers."""

from __future__ import annotations

import os
import time as _time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

import msgspec

from vertebra._vertebra import (
    LeaseBroker,
    SpannerAbortedError,
)
from vertebra._vertebra import (
    StatementCache as _RustStatementCache,
)
from vertebra.db.adapters.spanner.config import (
    SPANGRES_DIALECT_TAG,
    SPANNER_GOOGLESQL_DIALECT_TAG,
    Mutation,
    MutationGroup,
    SpannerConfig,
    StalenessBound,
)
from vertebra.db.adapters.spanner.core import SpannerCore
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver._async import AsyncDatabaseDriver
from vertebra.db.driver._sync import SyncDatabaseDriver
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.schema import to_value_type
from vertebra.utils.serializers import encode_json

if TYPE_CHECKING:
    from vertebra._spanner import AsyncSpannerClient, SyncSpannerClient

__all__ = (
    "AsyncSpannerDriver",
    "AsyncSpannerSnapshot",
    "AsyncSpannerTransaction",
    "SyncSpannerDriver",
    "SyncSpannerSnapshot",
    "SyncSpannerTransaction",
)


class SyncSpannerSnapshot:
    """Synchronous `ReadOnly` snapshot context bound to a `StalenessBound`."""

    __slots__ = ("_driver", "_staleness")

    def __init__(self, driver: SyncSpannerDriver, staleness: StalenessBound) -> None:
        self._driver = driver
        self._staleness = staleness

    @property
    def staleness(self) -> StalenessBound:
        """Return the `StalenessBound` governing this snapshot."""
        return self._staleness

    def __enter__(self) -> Self:
        """Enter the snapshot context."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit the snapshot context."""

    def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `ReadOnly` `SELECT` at this snapshot's staleness bound."""
        return self._driver.select(
            sql,
            parameters,
            schema_type=schema_type,
            staleness=self._staleness,
        )

    def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `ReadOnly` `SELECT` at this snapshot's staleness bound."""
        return self.select(sql, parameters, schema_type=schema_type)

    def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `ReadOnly` query and return the first row or `None`."""
        rows = self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

    def query_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `ReadOnly` query and return the first row or `None`."""
        return self.select_one(sql, parameters, schema_type=schema_type)

    def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `ReadOnly` query and return the first scalar column of the first row."""
        return self._driver.select_value(
            sql,
            parameters,
            value_type=value_type,
            staleness=self._staleness,
        )

    def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
    ) -> ArrowCursor:
        """Read partitioned Arrow batches at this snapshot's staleness bound."""
        return self._driver.fetch_arrow(
            sql_or_table,
            parameters,
            max_partitions=max_partitions,
            data_boost_enabled=data_boost_enabled,
            staleness=self._staleness,
        )


class AsyncSpannerSnapshot:
    """Asynchronous `ReadOnly` snapshot context bound to a `StalenessBound`."""

    __slots__ = ("_driver", "_staleness")

    def __init__(self, driver: AsyncSpannerDriver, staleness: StalenessBound) -> None:
        self._driver = driver
        self._staleness = staleness

    @property
    def staleness(self) -> StalenessBound:
        """Return the `StalenessBound` governing this snapshot."""
        return self._staleness

    async def __aenter__(self) -> Self:
        """Enter the async snapshot context."""
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit the async snapshot context."""

    async def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute an async `ReadOnly` `SELECT` at this snapshot's staleness bound."""
        return await self._driver.select(
            sql,
            parameters,
            schema_type=schema_type,
            staleness=self._staleness,
        )

    async def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute an async `ReadOnly` `SELECT` at this snapshot's staleness bound."""
        return await self.select(sql, parameters, schema_type=schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute an async `ReadOnly` query and return the first row or `None`."""
        rows = await self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

    async def query_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute an async `ReadOnly` query and return the first row or `None`."""
        return await self.select_one(sql, parameters, schema_type=schema_type)

    async def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
    ) -> Any:
        """Execute an async `ReadOnly` query and return the first scalar column."""
        return await self._driver.select_value(
            sql,
            parameters,
            value_type=value_type,
            staleness=self._staleness,
        )

    async def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
    ) -> ArrowCursor:
        """Read partitioned Arrow batches asynchronously at this snapshot's staleness bound."""
        return await self._driver.fetch_arrow(
            sql_or_table,
            parameters,
            max_partitions=max_partitions,
            data_boost_enabled=data_boost_enabled,
            staleness=self._staleness,
        )


class SyncSpannerTransaction:
    """Transactional context passed to `SyncSpannerDriver.run_in_transaction(fn)`."""

    __slots__ = ("_driver", "_mutations", "_transaction_tag")

    def __init__(
        self,
        driver: SyncSpannerDriver,
        *,
        transaction_tag: str | None = None,
    ) -> None:
        self._driver = driver
        self._transaction_tag = transaction_tag
        self._mutations: list[Mutation] = []

    @property
    def mutations(self) -> tuple[Mutation, ...]:
        """Return mutations buffered for commit at the end of this transaction."""
        return tuple(self._mutations)

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement inside this read-write transaction."""
        return self._driver.execute(
            sql,
            parameters,
            request_tag=request_tag,
            transaction_tag=self._transaction_tag,
        )

    def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements inside this read-write transaction."""
        return self._driver.execute_batch_dml(
            statements,
            request_tag=request_tag,
            transaction_tag=self._transaction_tag,
        )

    def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement inside this read-write transaction."""
        return self._driver.select(sql, parameters, schema_type=schema_type)

    def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement inside this read-write transaction."""
        return self.select(sql, parameters, schema_type=schema_type)

    def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None`."""
        return self._driver.select_one(sql, parameters, schema_type=schema_type)

    def query_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None`."""
        return self.select_one(sql, parameters, schema_type=schema_type)

    def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
    ) -> Any:
        """Return the first scalar column of the first row of `sql`."""
        return self._driver.select_value(sql, parameters, value_type=value_type)

    def buffer_mutation(self, mutation: Mutation) -> None:
        """Buffer a `Mutation` to be applied when the transaction commits."""
        self._mutations.append(mutation)

    def insert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `insert` mutation on `table`."""
        self._mutations.append(Mutation.insert(table, columns, values))

    def update(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `update` mutation on `table`."""
        self._mutations.append(Mutation.update(table, columns, values))

    def insert_or_update(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `insert_or_update` mutation on `table`."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def upsert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `insert_or_update` mutation on `table`."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def replace(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer a `replace` mutation on `table`."""
        self._mutations.append(Mutation.replace(table, columns, values))

    def delete(
        self,
        table: str,
        keys: Sequence[Sequence[Any] | Any] = (),
        *,
        all_rows: bool = False,
    ) -> None:
        """Buffer a `delete` mutation on `table`."""
        self._mutations.append(Mutation.delete(table, keys, all_rows=all_rows))


class AsyncSpannerTransaction:
    """Asynchronous transactional context passed to `AsyncSpannerDriver.run_in_transaction(fn)`."""

    __slots__ = ("_driver", "_mutations", "_transaction_tag")

    def __init__(
        self,
        driver: AsyncSpannerDriver,
        *,
        transaction_tag: str | None = None,
    ) -> None:
        self._driver = driver
        self._transaction_tag = transaction_tag
        self._mutations: list[Mutation] = []

    @property
    def mutations(self) -> tuple[Mutation, ...]:
        """Return mutations buffered for commit at the end of this transaction."""
        return tuple(self._mutations)

    async def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement asynchronously inside this read-write transaction."""
        return await self._driver.execute(
            sql,
            parameters,
            request_tag=request_tag,
            transaction_tag=self._transaction_tag,
        )

    async def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements asynchronously inside this read-write transaction."""
        return await self._driver.execute_batch_dml(
            statements,
            request_tag=request_tag,
            transaction_tag=self._transaction_tag,
        )

    async def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement asynchronously inside this read-write transaction."""
        return await self._driver.select(sql, parameters, schema_type=schema_type)

    async def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement asynchronously inside this read-write transaction."""
        return await self.select(sql, parameters, schema_type=schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None`."""
        return await self._driver.select_one(sql, parameters, schema_type=schema_type)

    async def query_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None`."""
        return await self.select_one(sql, parameters, schema_type=schema_type)

    async def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
    ) -> Any:
        """Return the first scalar column of the first row of `sql`."""
        return await self._driver.select_value(sql, parameters, value_type=value_type)

    def buffer_mutation(self, mutation: Mutation) -> None:
        """Buffer a `Mutation` to be applied when the transaction commits."""
        self._mutations.append(mutation)

    def insert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `insert` mutation on `table`."""
        self._mutations.append(Mutation.insert(table, columns, values))

    def update(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `update` mutation on `table`."""
        self._mutations.append(Mutation.update(table, columns, values))

    def insert_or_update(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `insert_or_update` mutation on `table`."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def upsert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer an `insert_or_update` mutation on `table`."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def replace(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> None:
        """Buffer a `replace` mutation on `table`."""
        self._mutations.append(Mutation.replace(table, columns, values))

    def delete(
        self,
        table: str,
        keys: Sequence[Sequence[Any] | Any] = (),
        *,
        all_rows: bool = False,
    ) -> None:
        """Buffer a `delete` mutation on `table`."""
        self._mutations.append(Mutation.delete(table, keys, all_rows=all_rows))


class SyncSpannerDriver(SyncDatabaseDriver[SpannerConfig]):
    """Synchronous pure-Rust Google Cloud Spanner v1 driver."""

    dialect_tag: ClassVar[int] = SPANNER_GOOGLESQL_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dialect_tag",
        "_last_commit_stats",
        "_propagator",
        "_statement_cache",
        "dialect",
    )

    def __init__(
        self,
        config: SpannerConfig | str | None = None,
        *,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        propagator: SqlTracePropagator | None = None,
        client: SyncSpannerClient | None = None,
        **overrides: Any,
    ) -> None:
        if isinstance(config, str):
            resolved = SpannerConfig.from_dsn(config, **overrides)
        elif isinstance(config, SpannerConfig):
            resolved = msgspec.structs.replace(config, **overrides) if overrides else config
        else:
            dsn = overrides.pop("dsn", None)
            if isinstance(dsn, str) and dsn.startswith(("spanner", "spangres")):
                resolved = SpannerConfig.from_dsn(dsn, **overrides)
            else:
                resolved = SpannerConfig(**overrides)

        resolved_broker = broker or resolved.broker or LeaseBroker(max(1, resolved.pool_size * 4))
        super().__init__(resolved, broker=resolved_broker)
        self.dialect: Literal["googlesql", "spangres"] = resolved.dialect
        self._dialect_tag: int = (
            SPANGRES_DIALECT_TAG if resolved.dialect == "spangres" else SPANNER_GOOGLESQL_DIALECT_TAG
        )
        self._statement_cache: _RustStatementCache | None = (
            statement_cache
            if statement_cache is not None
            else (_RustStatementCache(max(16, resolved.statement_cache_capacity)) if resolved.use_shm_cache else None)
        )
        self._propagator: SqlTracePropagator = propagator or SqlTracePropagator()
        self._client: SyncSpannerClient | None = client
        self._last_commit_stats: dict[str, Any] = {}

    @property
    def project_id(self) -> str:
        """Return the effective Google Cloud project ID."""
        return self.config.effective_project_id()

    @property
    def instance_id(self) -> str:
        """Return the effective Cloud Spanner instance ID."""
        return self.config.effective_instance_id()

    @property
    def database_id(self) -> str:
        """Return the effective Cloud Spanner database ID."""
        return self.config.effective_database_id()

    @property
    def database_uri(self) -> str:
        """Return the canonical `projects/{p}/instances/{i}/databases/{d}` resource URI."""
        return self.config.database_uri

    @property
    def last_commit_stats(self) -> dict[str, Any]:
        """Return commit timestamp and mutation count from the last write operation."""
        return dict(self._last_commit_stats)

    def __enter__(self) -> Self:
        """Connect and enter the synchronous driver context."""
        return self.connect()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close the driver upon exiting the context."""
        self.close()

    def connect(self) -> Self:
        """Initialize the underlying Rust `SyncSpannerClient` channel pool."""
        if self._client is None:
            self._client = SpannerCore.create_sync_rust_client(self.config)
        self._closed = False
        return self

    def close(self) -> None:
        """Release the underlying Rust client reference."""
        self._client = None
        self._closed = True

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> SyncSpannerSnapshot:
        """Create a lock-free `ReadOnly` `SyncSpannerSnapshot` bound to `staleness`."""
        _ = multi_use
        return SyncSpannerSnapshot(self, staleness or StalenessBound.strong())

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        _ = kwargs
        stripped = sql.strip()
        if SpannerCore.is_ddl_statement(stripped):
            self.update_ddl([stripped])
            return 0

        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(stripped, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        lease = self._acquire_pid_lease()
        try:
            return int(
                client.execute_dml(
                    annotated_sql,
                    params_json,
                    request_tag,
                    transaction_tag,
                )
            )
        finally:
            self._release_pid_lease(lease)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute `sql` across multiple parameter sets via `ExecuteBatchDml`."""
        _ = kwargs
        if not parameter_sets:
            return 0
        statements = [(sql, params) for params in parameter_sets]
        counts = self.execute_batch_dml(
            statements,
            request_tag=request_tag,
            transaction_tag=transaction_tag,
        )
        return sum(counts)

    def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        tag: str | None = None,
    ) -> list[int]:
        """Execute multiple DML statements in a single `ExecuteBatchDml` RPC."""
        effective_tx_tag = transaction_tag or (tag if tag else None)
        if not statements:
            return []

        client = self._ensure_client()
        batch_json = SpannerCore.encode_batch_dml_json(statements, dialect=self.dialect)
        lease = self._acquire_pid_lease()
        try:
            return [
                int(c)
                for c in client.execute_batch_dml(
                    batch_json,
                    request_tag,
                    effective_tx_tag,
                )
            ]
        finally:
            self._release_pid_lease(lease)

    def execute_partitioned_dml(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute a large-scale Partitioned DML statement and return lower-bound affected rows."""
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        return int(
            client.execute_partitioned_dml(
                annotated_sql,
                params_json,
                exclude_txn_from_change_streams,
            )
        )

    def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        """Execute a semicolon-delimited script of DDL or DML statements."""
        parts = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        if not parts:
            return 0
        ddl_batch = [stmt for stmt in parts if SpannerCore.is_ddl_statement(stmt)]
        dml_batch = [stmt for stmt in parts if not SpannerCore.is_ddl_statement(stmt)]
        if ddl_batch:
            self.update_ddl(ddl_batch)
        total = 0
        for stmt in dml_batch:
            total += self.execute(stmt, parameters)
        return total

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters, staleness=staleness)
        cols = tuple(str(c) for c in raw.get("columns", ()))
        rows = [tuple(r) for r in raw.get("rows", ())]
        return rows, cols

    def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and hydrate rows as dictionaries or `schema_type` instances."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters, staleness=staleness)
        return SpannerCore.hydrate_query_rows(
            raw.get("columns", ()),
            raw.get("rows", ()),
            schema_type=schema_type,
        )

    def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None`."""
        rows = self.select(sql, parameters, schema_type=schema_type, staleness=staleness, **kwargs)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column of the first row."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters, staleness=staleness)
        rows = raw.get("rows", ())
        if not rows or not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    def run_in_transaction(
        self,
        fn: Callable[[SyncSpannerTransaction], Any],
        *,
        max_attempts: int = 5,
        transaction_tag: str | None = None,
        tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> Any:
        """Execute `fn(tx)` inside a read-write transaction with wound-wait retry on `Aborted`."""
        _ = max_commit_delay_ms
        effective_tag = transaction_tag or (tag if tag else None)
        attempts = max(1, int(max_attempts))
        for attempt in range(1, attempts + 1):
            tx = SyncSpannerTransaction(self, transaction_tag=effective_tag)
            try:
                result = fn(tx)
                if tx.mutations:
                    self.apply_mutations(
                        tx.mutations,
                        transaction_tag=effective_tag,
                        exclude_txn_from_change_streams=exclude_txn_from_change_streams,
                    )
                return result
            except SpannerAbortedError:
                if attempt >= attempts:
                    raise
                _time.sleep(0.01 * attempt)
        return None

    def apply_mutations(
        self,
        mutations: Sequence[Mutation | Mapping[str, Any]],
        *,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> dict[str, Any]:
        """Commit a sequence of `Mutation` structs atomically."""
        _ = max_commit_delay_ms
        if not mutations:
            return {"commit_timestamp_micros": 0, "mutation_count": 0, "retry_count": 0}
        normalized = [SpannerCore.coerce_mutation(m) for m in mutations]

        client = self._ensure_client()
        total_stats: dict[str, Any] = {
            "commit_timestamp_micros": None,
            "mutation_count": 0,
            "retry_count": 0,
        }
        for chunk in SpannerCore.chunk_mutations_by_cell_limit(normalized):
            mutations_json = encode_json([m.to_dict() for m in chunk])
            res = client.apply_mutations(
                mutations_json,
                transaction_tag,
                exclude_txn_from_change_streams,
            )
            total_stats["commit_timestamp_micros"] = res.get("commit_timestamp_micros")
            total_stats["mutation_count"] += int(res.get("mutation_count", len(chunk)))
            total_stats["retry_count"] += int(res.get("retry_count", 0))
        self._last_commit_stats = total_stats
        return total_stats

    def write_insert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `insert` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.insert(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_update(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `update` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.update(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_upsert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `insert_or_update` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.insert_or_update(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_replace(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply a `replace` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.replace(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_delete(
        self,
        table: str,
        keys: Sequence[Sequence[Any] | Any] = (),
        *,
        all_rows: bool = False,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply a `delete` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.delete(table, keys, all_rows=all_rows)],
            transaction_tag=transaction_tag,
        )

    def batch_write(
        self,
        mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]],
        *,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Commit independent `MutationGroup` batches via streaming `BatchWrite`."""
        _ = max_commit_delay_ms
        if not mutation_groups:
            return []
        groups = [SpannerCore.coerce_mutation_group(g) for g in mutation_groups]
        client = self._ensure_client()
        groups_json = encode_json([g.to_dict() for g in groups])
        return list(client.batch_write(groups_json, exclude_txn_from_change_streams))

    def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Execute one or more DDL statements via `DatabaseAdmin/UpdateDatabaseDdl`."""
        stmt_list = [statements] if isinstance(statements, str) else [str(s) for s in statements if str(s).strip()]
        if not stmt_list:
            return ""
        client = self._ensure_client()
        return str(client.update_ddl(stmt_list))

    def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
        staleness: StalenessBound | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a parallel `PartitionQuery` + Data Boost read into a `VTBARROW` `ArrowCursor`."""
        _ = kwargs
        sql = SpannerCore.build_select_sql_if_table(sql_or_table)
        bound = staleness or StalenessBound.strong()
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        partitions = max_partitions if max_partitions is not None else self.config.max_partitions
        boost = data_boost_enabled if data_boost_enabled is not None else self.config.data_boost_enabled
        try:
            raw = client.partitioned_query(
                rewritten_sql,
                params_json,
                max(1, int(partitions)),
                bool(boost),
                bound.mode,
                bound.effective_value(),
            )
        except Exception as exc:
            if not SpannerCore.is_partitioned_query_fallback_error(exc):
                raise
            raw = self._run_query_sync(sql, parameters, staleness=bound)
        return ArrowCursor.from_rows(
            raw.get("rows", ()),
            columns=[str(c) for c in raw.get("columns", ())],
            threshold_rows=threshold_rows,
        )

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query or table read and return a zero-copy `ArrowCursor`."""
        return self.fetch_arrow(sql, parameters, threshold_rows=threshold_rows, **kwargs)

    def insert_arrow(
        self,
        table_name: str,
        data: ArrowCursor | bytes | Any,
        *,
        columns: Sequence[str] | None = None,
        upsert: bool = True,
        **kwargs: Any,
    ) -> int:
        """Write an `ArrowCursor`, Arrow IPC buffer, or `pyarrow` table into `table_name` via chunked mutations."""
        _ = (columns, kwargs)
        rows = SpannerCore.coerce_arrow_to_dicts(data)
        if not rows:
            return 0
        cols = list(rows[0].keys())
        values = [[r.get(c) for c in cols] for r in rows]
        mutation = (
            Mutation.insert_or_update(table_name, cols, values) if upsert else Mutation.insert(table_name, cols, values)
        )
        self.apply_mutations([mutation])
        return len(rows)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        upsert: bool = True,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an Arrow payload into `table_name` via chunked mutations."""
        return self.insert_arrow(table_name, data, columns=columns, upsert=upsert, **kwargs)

    def export_to_object_store(
        self,
        sql_or_table: str,
        store_or_uri: Any,
        destination_uri: str | None = None,
        *,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        store: Any | None = None,
        format: Literal["parquet", "arrow", "csv", "json", "jsonl"] = "parquet",
        compression: str = "ZSTD",
        data_boost_enabled: bool = True,
        staleness: StalenessBound | None = None,
    ) -> dict[str, Any]:
        """Export a Spanner table or query to `ObjectStoreBackend` via zero-copy `ArrowCursor`."""
        if destination_uri is None and isinstance(store_or_uri, str):
            target_uri = store_or_uri
            target_store = store
        else:
            target_store = store_or_uri if store is None else store
            target_uri = destination_uri or ""
        cursor = self.fetch_arrow(
            sql_or_table,
            params,
            data_boost_enabled=data_boost_enabled,
            staleness=staleness,
        )
        SpannerCore.write_cursor_to_object_store_sync(
            cursor,
            destination_uri=target_uri,
            store=target_store,
            format=format,
            compression=compression,
        )
        return {
            "rows_exported": cursor.row_count,
            "row_count": cursor.row_count,
            "destination_uri": target_uri,
            "format": format,
            "data_boost_enabled": bool(data_boost_enabled),
        }

    def load_from_object_store(
        self,
        table: str,
        store_or_uris: Any,
        source_uris: str | Sequence[str] | None = None,
        *,
        store: Any | None = None,
        format: Literal["parquet", "arrow", "csv", "json", "jsonl"] = "parquet",
        upsert: bool = True,
    ) -> int:
        """Load Parquet, Arrow IPC, CSV, or JSONL files from `ObjectStoreBackend` into `table`."""
        if source_uris is None and isinstance(store_or_uris, (str, Sequence)):
            uris_arg = store_or_uris
            target_store = store
        else:
            target_store = store_or_uris if store is None else store
            uris_arg = source_uris or ()
        uris = [uris_arg] if isinstance(uris_arg, str) else list(uris_arg)
        total = 0
        for uri in uris:
            cursor = SpannerCore.read_cursor_from_object_store_sync(uri, store=target_store, format=format)
            total += self.insert_arrow(table, cursor, upsert=upsert)
        return total

    def _ensure_client(self) -> SyncSpannerClient:
        if self._client is None:
            ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")
            self._client = SpannerCore.create_sync_rust_client(self.config)
        return self._client

    def _acquire_pid_lease(self) -> Any:
        acquire_fn = getattr(self._broker, "acquire", None)
        if callable(acquire_fn):
            with suppress(Exception):
                return acquire_fn(os.getpid(), timeout_ms=5000)
        return None

    def _release_pid_lease(self, lease: Any) -> None:
        if lease is None or self._broker is None:
            return
        release_fn = getattr(self._broker, "release", None)
        if callable(release_fn):
            with suppress(Exception):
                release_fn(lease.slot_index, lease.generation)

    def _prepare_sql_and_params(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None,
    ) -> tuple[str, str | None]:
        rewritten = sql
        if self._statement_cache is not None:
            raw_bytes = sql.encode("utf-8")
            plan = self._statement_cache.lookup(self._dialect_tag, raw_bytes)
            if plan is not None and plan.compiled_sql:
                rewritten = plan.compiled_sql.decode("utf-8", errors="replace")
            else:
                param_count = len(parameters) if parameters is not None else 0
                self._statement_cache.insert(
                    self._dialect_tag,
                    raw_bytes,
                    param_count,
                    raw_bytes,
                    b"",
                )
        params_json = SpannerCore.encode_parameters_json(parameters, dialect=self.dialect)
        return rewritten, params_json

    def _run_query_sync(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
    ) -> dict[str, Any]:
        client = self._ensure_client()
        bound = staleness or StalenessBound.strong()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        raw = client.query(
            annotated_sql,
            params_json,
            bound.mode,
            bound.effective_value(),
        )
        return dict(raw)


class AsyncSpannerDriver(AsyncDatabaseDriver[SpannerConfig]):
    """Asynchronous pure-Rust Google Cloud Spanner v1 driver."""

    dialect_tag: ClassVar[int] = SPANNER_GOOGLESQL_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dialect_tag",
        "_last_commit_stats",
        "_propagator",
        "_statement_cache",
        "dialect",
    )

    def __init__(
        self,
        config: SpannerConfig | str | None = None,
        *,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        propagator: SqlTracePropagator | None = None,
        client: AsyncSpannerClient | None = None,
        **overrides: Any,
    ) -> None:
        if isinstance(config, str):
            resolved = SpannerConfig.from_dsn(config, **overrides)
        elif isinstance(config, SpannerConfig):
            resolved = msgspec.structs.replace(config, **overrides) if overrides else config
        else:
            dsn = overrides.pop("dsn", None)
            if isinstance(dsn, str) and dsn.startswith(("spanner", "spangres")):
                resolved = SpannerConfig.from_dsn(dsn, **overrides)
            else:
                resolved = SpannerConfig(**overrides)

        resolved_broker = broker or resolved.broker or LeaseBroker(max(1, resolved.pool_size * 4))
        super().__init__(resolved, broker=resolved_broker)
        self.dialect: Literal["googlesql", "spangres"] = resolved.dialect
        self._dialect_tag: int = (
            SPANGRES_DIALECT_TAG if resolved.dialect == "spangres" else SPANNER_GOOGLESQL_DIALECT_TAG
        )
        self._statement_cache: _RustStatementCache | None = (
            statement_cache
            if statement_cache is not None
            else (_RustStatementCache(max(16, resolved.statement_cache_capacity)) if resolved.use_shm_cache else None)
        )
        self._propagator: SqlTracePropagator = propagator or SqlTracePropagator()
        self._client: AsyncSpannerClient | None = client
        self._last_commit_stats: dict[str, Any] = {}

    @property
    def project_id(self) -> str:
        """Return the effective Google Cloud project ID."""
        return self.config.effective_project_id()

    @property
    def instance_id(self) -> str:
        """Return the effective Cloud Spanner instance ID."""
        return self.config.effective_instance_id()

    @property
    def database_id(self) -> str:
        """Return the effective Cloud Spanner database ID."""
        return self.config.effective_database_id()

    @property
    def database_uri(self) -> str:
        """Return the canonical `projects/{p}/instances/{i}/databases/{d}` resource URI."""
        return self.config.database_uri

    @property
    def last_commit_stats(self) -> dict[str, Any]:
        """Return commit timestamp and mutation count from the last write operation."""
        return dict(self._last_commit_stats)

    async def __aenter__(self) -> Self:
        """Connect and enter the asynchronous driver context."""
        return await self.connect()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close the driver upon exiting the context."""
        await self.close()

    async def connect(self) -> Self:
        """Initialize the underlying Rust `AsyncSpannerClient` channel pool."""
        if self._client is None:
            self._client = SpannerCore.create_async_rust_client(self.config)
        self._closed = False
        return self

    async def close(self) -> None:
        """Release the underlying Rust client reference."""
        self._client = None
        self._closed = True

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> AsyncSpannerSnapshot:
        """Create a lock-free `ReadOnly` `AsyncSpannerSnapshot` bound to `staleness`."""
        _ = multi_use
        return AsyncSpannerSnapshot(self, staleness or StalenessBound.strong())

    async def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement asynchronously and return the affected row count."""
        _ = kwargs
        stripped = sql.strip()
        if SpannerCore.is_ddl_statement(stripped):
            await self.update_ddl([stripped])
            return 0

        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(stripped, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        lease = self._acquire_pid_lease()
        try:
            res = await client.execute_dml(
                annotated_sql,
                params_json,
                request_tag,
                transaction_tag,
            )
            return int(res)
        finally:
            self._release_pid_lease(lease)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute `sql` across multiple parameter sets asynchronously via `ExecuteBatchDml`."""
        _ = kwargs
        if not parameter_sets:
            return 0
        statements = [(sql, params) for params in parameter_sets]
        counts = await self.execute_batch_dml(
            statements,
            request_tag=request_tag,
            transaction_tag=transaction_tag,
        )
        return sum(counts)

    async def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        tag: str | None = None,
    ) -> list[int]:
        """Execute multiple DML statements asynchronously in a single `ExecuteBatchDml` RPC."""
        effective_tx_tag = transaction_tag or (tag if tag else None)
        if not statements:
            return []

        client = self._ensure_client()
        batch_json = SpannerCore.encode_batch_dml_json(statements, dialect=self.dialect)
        lease = self._acquire_pid_lease()
        try:
            res = await client.execute_batch_dml(
                batch_json,
                request_tag,
                effective_tx_tag,
            )
            return [int(c) for c in res]
        finally:
            self._release_pid_lease(lease)

    async def execute_partitioned_dml(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute a large-scale Partitioned DML statement asynchronously."""
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        res = await client.execute_partitioned_dml(
            annotated_sql,
            params_json,
            exclude_txn_from_change_streams,
        )
        return int(res)

    async def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        """Execute a semicolon-delimited script of DDL or DML statements asynchronously."""
        parts = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        if not parts:
            return 0
        ddl_batch = [stmt for stmt in parts if SpannerCore.is_ddl_statement(stmt)]
        dml_batch = [stmt for stmt in parts if not SpannerCore.is_ddl_statement(stmt)]
        if ddl_batch:
            await self.update_ddl(ddl_batch)
        total = 0
        for stmt in dml_batch:
            total += await self.execute(stmt, parameters)
        return total

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query asynchronously and return `(tuple_rows, column_names)`."""
        _ = kwargs
        raw = await self._run_query_async(sql, parameters, staleness=staleness)
        cols = tuple(str(c) for c in raw.get("columns", ()))
        rows = [tuple(r) for r in raw.get("rows", ())]
        return rows, cols

    async def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query asynchronously and hydrate rows."""
        _ = kwargs
        raw = await self._run_query_async(sql, parameters, staleness=staleness)
        return SpannerCore.hydrate_query_rows(
            raw.get("columns", ()),
            raw.get("rows", ()),
            schema_type=schema_type,
        )

    async def select_one(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query asynchronously and return the first row or `None`."""
        rows = await self.select(sql, parameters, schema_type=schema_type, staleness=staleness, **kwargs)
        return rows[0] if rows else None

    async def select_value(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first scalar column."""
        _ = kwargs
        raw = await self._run_query_async(sql, parameters, staleness=staleness)
        rows = raw.get("rows", ())
        if not rows or not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    async def run_in_transaction(
        self,
        fn: Callable[[AsyncSpannerTransaction], Any],
        *,
        max_attempts: int = 5,
        transaction_tag: str | None = None,
        tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> Any:
        """Execute an async or sync `fn(tx)` inside a read-write transaction."""
        _ = max_commit_delay_ms
        effective_tag = transaction_tag or (tag if tag else None)
        attempts = max(1, int(max_attempts))
        for attempt in range(1, attempts + 1):
            tx = AsyncSpannerTransaction(self, transaction_tag=effective_tag)
            try:
                maybe_coro = fn(tx)
                result = await maybe_coro if hasattr(maybe_coro, "__await__") else maybe_coro
                if tx.mutations:
                    await self.apply_mutations(
                        tx.mutations,
                        transaction_tag=effective_tag,
                        exclude_txn_from_change_streams=exclude_txn_from_change_streams,
                    )
                return result
            except SpannerAbortedError:
                if attempt >= attempts:
                    raise
        return None

    async def apply_mutations(
        self,
        mutations: Sequence[Mutation | Mapping[str, Any]],
        *,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> dict[str, Any]:
        """Commit a sequence of `Mutation` structs asynchronously."""
        _ = max_commit_delay_ms
        if not mutations:
            return {"commit_timestamp_micros": 0, "mutation_count": 0, "retry_count": 0}
        normalized = [SpannerCore.coerce_mutation(m) for m in mutations]

        client = self._ensure_client()
        total_stats: dict[str, Any] = {
            "commit_timestamp_micros": None,
            "mutation_count": 0,
            "retry_count": 0,
        }
        for chunk in SpannerCore.chunk_mutations_by_cell_limit(normalized):
            mutations_json = encode_json([m.to_dict() for m in chunk])
            res = await client.apply_mutations(
                mutations_json,
                transaction_tag,
                exclude_txn_from_change_streams,
            )
            total_stats["commit_timestamp_micros"] = res.get("commit_timestamp_micros")
            total_stats["mutation_count"] += int(res.get("mutation_count", len(chunk)))
            total_stats["retry_count"] += int(res.get("retry_count", 0))
        self._last_commit_stats = total_stats
        return total_stats

    async def write_insert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `insert` mutation asynchronously."""
        return await self.apply_mutations(
            [Mutation.insert(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    async def write_update(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `update` mutation asynchronously."""
        return await self.apply_mutations(
            [Mutation.update(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    async def write_upsert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `insert_or_update` mutation asynchronously."""
        return await self.apply_mutations(
            [Mutation.insert_or_update(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    async def write_replace(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply a `replace` mutation asynchronously."""
        return await self.apply_mutations(
            [Mutation.replace(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    async def write_delete(
        self,
        table: str,
        keys: Sequence[Sequence[Any] | Any] = (),
        *,
        all_rows: bool = False,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply a `delete` mutation asynchronously."""
        return await self.apply_mutations(
            [Mutation.delete(table, keys, all_rows=all_rows)],
            transaction_tag=transaction_tag,
        )

    async def batch_write(
        self,
        mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]],
        *,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Commit independent `MutationGroup` batches asynchronously via `BatchWrite`."""
        _ = max_commit_delay_ms
        if not mutation_groups:
            return []
        groups = [SpannerCore.coerce_mutation_group(g) for g in mutation_groups]
        client = self._ensure_client()
        groups_json = encode_json([g.to_dict() for g in groups])
        res = await client.batch_write(groups_json, exclude_txn_from_change_streams)
        return list(res)

    async def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Execute one or more DDL statements asynchronously via `UpdateDatabaseDdl`."""
        stmt_list = [statements] if isinstance(statements, str) else [str(s) for s in statements if str(s).strip()]
        if not stmt_list:
            return ""
        client = self._ensure_client()
        res = await client.update_ddl(stmt_list)
        return str(res)

    async def fetch_arrow(
        self,
        sql_or_table: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
        staleness: StalenessBound | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute an async parallel `PartitionQuery` + Data Boost read into an `ArrowCursor`."""
        _ = kwargs
        sql = SpannerCore.build_select_sql_if_table(sql_or_table)
        bound = staleness or StalenessBound.strong()
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        partitions = max_partitions if max_partitions is not None else self.config.max_partitions
        boost = data_boost_enabled if data_boost_enabled is not None else self.config.data_boost_enabled
        try:
            raw = await client.partitioned_query(
                rewritten_sql,
                params_json,
                max(1, int(partitions)),
                bool(boost),
                bound.mode,
                bound.effective_value(),
            )
        except Exception as exc:
            if not SpannerCore.is_partitioned_query_fallback_error(exc):
                raise
            raw = await self._run_query_async(sql, parameters, staleness=bound)
        return ArrowCursor.from_rows(
            raw.get("rows", ()),
            columns=[str(c) for c in raw.get("columns", ())],
            threshold_rows=threshold_rows,
        )

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query or table read asynchronously and return a zero-copy `ArrowCursor`."""
        return await self.fetch_arrow(sql, parameters, threshold_rows=threshold_rows, **kwargs)

    async def insert_arrow(
        self,
        table_name: str,
        data: ArrowCursor | bytes | Any,
        *,
        columns: Sequence[str] | None = None,
        upsert: bool = True,
        **kwargs: Any,
    ) -> int:
        """Write an `ArrowCursor`, Arrow IPC buffer, or `pyarrow` table asynchronously."""
        _ = (columns, kwargs)
        rows = SpannerCore.coerce_arrow_to_dicts(data)
        if not rows:
            return 0
        cols = list(rows[0].keys())
        values = [[r.get(c) for c in cols] for r in rows]
        mutation = (
            Mutation.insert_or_update(table_name, cols, values) if upsert else Mutation.insert(table_name, cols, values)
        )
        await self.apply_mutations([mutation])
        return len(rows)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        upsert: bool = True,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-load an Arrow payload into `table_name`."""
        return await self.insert_arrow(table_name, data, columns=columns, upsert=upsert, **kwargs)

    async def export_to_object_store(
        self,
        sql_or_table: str,
        store_or_uri: Any,
        destination_uri: str | None = None,
        *,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        store: Any | None = None,
        format: Literal["parquet", "arrow", "csv", "json", "jsonl"] = "parquet",
        compression: str = "ZSTD",
        data_boost_enabled: bool = True,
        staleness: StalenessBound | None = None,
    ) -> dict[str, Any]:
        """Export a Spanner table or query asynchronously to `ObjectStoreBackend`."""
        if destination_uri is None and isinstance(store_or_uri, str):
            target_uri = store_or_uri
            target_store = store
        else:
            target_store = store_or_uri if store is None else store
            target_uri = destination_uri or ""
        cursor = await self.fetch_arrow(
            sql_or_table,
            params,
            data_boost_enabled=data_boost_enabled,
            staleness=staleness,
        )
        await SpannerCore.write_cursor_to_object_store_async(
            cursor,
            destination_uri=target_uri,
            store=target_store,
            format=format,
            compression=compression,
        )
        return {
            "rows_exported": cursor.row_count,
            "row_count": cursor.row_count,
            "destination_uri": target_uri,
            "format": format,
            "data_boost_enabled": bool(data_boost_enabled),
        }

    async def load_from_object_store(
        self,
        table: str,
        store_or_uris: Any,
        source_uris: str | Sequence[str] | None = None,
        *,
        store: Any | None = None,
        format: Literal["parquet", "arrow", "csv", "json", "jsonl"] = "parquet",
        upsert: bool = True,
    ) -> int:
        """Load Parquet, Arrow IPC, CSV, or JSONL files asynchronously from `ObjectStoreBackend`."""
        if source_uris is None and isinstance(store_or_uris, (str, Sequence)):
            uris_arg = store_or_uris
            target_store = store
        else:
            target_store = store_or_uris if store is None else store
            uris_arg = source_uris or ()
        uris = [uris_arg] if isinstance(uris_arg, str) else list(uris_arg)
        total = 0
        for uri in uris:
            cursor = await SpannerCore.read_cursor_from_object_store_async(uri, store=target_store, format=format)
            total += await self.insert_arrow(table, cursor, upsert=upsert)
        return total

    def _ensure_client(self) -> AsyncSpannerClient:
        if self._client is None:
            ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")
            self._client = SpannerCore.create_async_rust_client(self.config)
        return self._client

    def _acquire_pid_lease(self) -> Any:
        acquire_fn = getattr(self._broker, "acquire", None)
        if callable(acquire_fn):
            with suppress(Exception):
                return acquire_fn(os.getpid(), timeout_ms=5000)
        return None

    def _release_pid_lease(self, lease: Any) -> None:
        if lease is None or self._broker is None:
            return
        release_fn = getattr(self._broker, "release", None)
        if callable(release_fn):
            with suppress(Exception):
                release_fn(lease.slot_index, lease.generation)

    def _prepare_sql_and_params(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None,
    ) -> tuple[str, str | None]:
        rewritten = sql
        if self._statement_cache is not None:
            raw_bytes = sql.encode("utf-8")
            plan = self._statement_cache.lookup(self._dialect_tag, raw_bytes)
            if plan is not None and plan.compiled_sql:
                rewritten = plan.compiled_sql.decode("utf-8", errors="replace")
            else:
                param_count = len(parameters) if parameters is not None else 0
                self._statement_cache.insert(
                    self._dialect_tag,
                    raw_bytes,
                    param_count,
                    raw_bytes,
                    b"",
                )
        params_json = SpannerCore.encode_parameters_json(parameters, dialect=self.dialect)
        return rewritten, params_json

    async def _run_query_async(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
    ) -> dict[str, Any]:
        client = self._ensure_client()
        bound = staleness or StalenessBound.strong()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        raw = await client.query(
            annotated_sql,
            params_json,
            bound.mode,
            bound.effective_value(),
        )
        return dict(raw)
