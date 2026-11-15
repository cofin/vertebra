"""Synchronous and asynchronous snapshot and transaction contexts for Cloud Spanner."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

from vertebra._vertebra import NotFoundError
from vertebra.db.adapters.spanner.mutations import Mutation, StalenessBound
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor

if TYPE_CHECKING:
    from vertebra.db.adapters.spanner._async import AsyncSpannerDriver
    from vertebra.db.adapters.spanner._sync import SyncSpannerDriver

__all__ = (
    "AsyncSpannerSnapshot",
    "AsyncSpannerTransaction",
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
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
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
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `ReadOnly` `SELECT` at this snapshot's staleness bound."""
        return self.select(sql, parameters, schema_type=schema_type)

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `ReadOnly` query and return the first row, raising `NotFoundError` when empty."""
        rows = self.select(sql, parameters, schema_type=schema_type)
        if not rows:
            msg = "Spanner snapshot query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a `ReadOnly` query and return the first row or `None` when empty."""
        rows = self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

    def query_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a `ReadOnly` query and return the first row, raising `NotFoundError` when empty."""
        return self.select_one(sql, parameters, schema_type=schema_type)

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
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

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Read partitioned Arrow batches at this snapshot's staleness bound."""
        return self._driver.select_to_arrow(
            sql,
            parameters,
            threshold_rows=threshold_rows,
            max_partitions=max_partitions,
            data_boost_enabled=data_boost_enabled,
            staleness=self._staleness,
            **kwargs,
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
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
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
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute an async `ReadOnly` `SELECT` at this snapshot's staleness bound."""
        return await self.select(sql, parameters, schema_type=schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute an async `ReadOnly` query and return the first row, raising `NotFoundError` when empty."""
        rows = await self.select(sql, parameters, schema_type=schema_type)
        if not rows:
            msg = "Spanner snapshot query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute an async `ReadOnly` query and return the first row or `None` when empty."""
        rows = await self.select(sql, parameters, schema_type=schema_type)
        return rows[0] if rows else None

    async def query_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute an async `ReadOnly` query and return the first row, raising `NotFoundError` when empty."""
        return await self.select_one(sql, parameters, schema_type=schema_type)

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
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

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Read partitioned Arrow batches asynchronously at this snapshot's staleness bound."""
        return await self._driver.select_to_arrow(
            sql,
            parameters,
            threshold_rows=threshold_rows,
            max_partitions=max_partitions,
            data_boost_enabled=data_boost_enabled,
            staleness=self._staleness,
            **kwargs,
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
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement inside this read-write transaction."""
        return self._driver.select(sql, parameters, schema_type=schema_type)

    def query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement inside this read-write transaction."""
        return self.select(sql, parameters, schema_type=schema_type)

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        return self._driver.select_one(sql, parameters, schema_type=schema_type)

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None` when empty."""
        return self._driver.select_one_or_none(sql, parameters, schema_type=schema_type)

    def query_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        return self.select_one(sql, parameters, schema_type=schema_type)

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
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
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement asynchronously inside this read-write transaction."""
        return await self._driver.select(sql, parameters, schema_type=schema_type)

    async def query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement asynchronously inside this read-write transaction."""
        return await self.select(sql, parameters, schema_type=schema_type)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        return await self._driver.select_one(sql, parameters, schema_type=schema_type)

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None` when empty."""
        return await self._driver.select_one_or_none(sql, parameters, schema_type=schema_type)

    async def query_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        return await self.select_one(sql, parameters, schema_type=schema_type)

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
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
