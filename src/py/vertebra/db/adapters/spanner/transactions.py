"""Synchronous and asynchronous snapshot and transaction contexts for Cloud Spanner."""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self

from vertebra._vertebra import NotFoundError
from vertebra.db.adapters.spanner.core import SpannerCore
from vertebra.db.adapters.spanner.mutations import Mutation, StalenessBound
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.utils.schema import to_value_type
from vertebra.utils.serializers import encode_json

if TYPE_CHECKING:
    from vertebra.db.adapters.spanner._async import (
        AsyncSpannerCursorProtocol,
        AsyncSpannerDriver,
        AsyncSpannerReadWriteTxProtocol,
    )
    from vertebra.db.adapters.spanner._sync import (
        SyncSpannerCursorProtocol,
        SyncSpannerDriver,
        SyncSpannerReadWriteTxProtocol,
    )

__all__ = (
    "AsyncSpannerSnapshot",
    "AsyncSpannerTransaction",
    "SyncSpannerSnapshot",
    "SyncSpannerTransaction",
    "compute_abort_backoff_seconds",
)


def compute_abort_backoff_seconds(exc: BaseException, attempt: int) -> float:
    """Return delay in seconds honoring server `retry_delay_ms` or bounded jittered exponential backoff."""
    retry_delay_ms = getattr(exc, "retry_delay_ms", None)
    if isinstance(retry_delay_ms, (int, float)) and retry_delay_ms >= 0:
        return float(retry_delay_ms) / 1000.0
    base: float = min(0.01 * float(2 ** max(0, attempt - 1)), 1.0)
    jitter: float = random.uniform(0.0, base * 0.5)
    return float(base + jitter)


def _encode_tx_mutations(mutations: Sequence[Mutation]) -> str | None:
    if not mutations:
        return None
    return encode_json([m.to_dict() for m in mutations])


def _extract_scalar_value(rows: Sequence[Sequence[Any]], value_type: type[Any] | None) -> Any:
    if not rows:
        msg = "Spanner query returned no rows"
        raise NotFoundError(msg)
    if not rows[0]:
        return None
    val = rows[0][0]
    if value_type is not None and val is not None:
        return to_value_type(val, value_type)
    return val


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
    """Interactive synchronous `ReadWrite` transaction bound to a pinned Spanner transaction ID."""

    __slots__ = (
        "_committed",
        "_driver",
        "_handle",
        "_mutations",
        "_rolled_back",
        "_transaction_tag",
    )

    def __init__(
        self,
        driver: SyncSpannerDriver,
        handle: SyncSpannerReadWriteTxProtocol,
        *,
        transaction_tag: str | None = None,
    ) -> None:
        self._driver = driver
        self._handle = handle
        self._transaction_tag = transaction_tag
        self._mutations: list[Mutation] = []
        self._committed = False
        self._rolled_back = False

    @property
    def transaction_id(self) -> bytes:
        """Return the server-assigned Cloud Spanner transaction identifier bytes."""
        return bytes(self._handle.transaction_id)

    @property
    def is_active(self) -> bool:
        """Return whether this transaction has not been committed or rolled back."""
        return not self._committed and not self._rolled_back

    @property
    def mutations(self) -> tuple[Mutation, ...]:
        """Return mutations buffered for atomic commit when this transaction completes."""
        return tuple(self._mutations)

    def cursor(self, *, request_tag: str | None = None) -> SyncSpannerCursorProtocol:
        """Create a PEP 249 cursor bound to this interactive read-write transaction."""
        return self._handle.cursor(
            request_tag=request_tag,
            param_encoder=self._driver._encode_parameters,
        )

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement inside this interactive read-write transaction."""
        cursor = self.cursor(request_tag=request_tag)
        try:
            annotated = SpannerCore.annotate_sql_with_trace(sql, self._driver._propagator)
            cursor.execute(annotated, parameters or ())
            return max(cursor.rowcount, 0)
        finally:
            cursor.close()

    def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements inside this interactive read-write transaction."""
        if not statements:
            return []
        batch_json = SpannerCore.encode_batch_dml_json(statements, dialect=self._driver.dialect)
        lease = self._driver._acquire_pid_lease()
        try:
            return [int(c) for c in self._handle.execute_batch_dml(batch_json, request_tag)]
        finally:
            self._driver._release_pid_lease(lease)

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement inside this interactive read-write transaction."""
        cursor = self.cursor(request_tag=request_tag)
        try:
            annotated = SpannerCore.annotate_sql_with_trace(sql, self._driver._propagator)
            cursor.execute(annotated, parameters or ())
            description = cursor.description
            if description is None:
                return []
            cols = tuple(str(col[0]) for col in description)
            return SpannerCore.hydrate_query_rows(
                cols,
                cursor.fetchall(),
                schema_type=schema_type,
            )
        finally:
            cursor.close()

    def query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement inside this interactive read-write transaction."""
        return self.select(sql, parameters, schema_type=schema_type, request_tag=request_tag)

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        rows = self.select(sql, parameters, schema_type=schema_type, request_tag=request_tag)
        if not rows:
            msg = "Spanner query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None` when empty."""
        rows = self.select(sql, parameters, schema_type=schema_type, request_tag=request_tag)
        return rows[0] if rows else None

    def query_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        return self.select_one(sql, parameters, schema_type=schema_type, request_tag=request_tag)

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any:
        """Return the first scalar column of the first row of `sql`."""
        cursor = self.cursor(request_tag=request_tag)
        try:
            annotated = SpannerCore.annotate_sql_with_trace(sql, self._driver._propagator)
            cursor.execute(annotated, parameters or ())
            return _extract_scalar_value(cursor.fetchall(), value_type)
        finally:
            cursor.close()

    def buffer_mutation(self, mutation: Mutation) -> None:
        """Buffer a `Mutation` to be applied when the transaction commits."""
        self._mutations.append(mutation)

    def buffer_mutations(self, mutations: Sequence[Mutation]) -> None:
        """Buffer multiple `Mutation` items to be applied when the transaction commits."""
        self._mutations.extend(mutations)

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

    def commit(self) -> dict[str, Any]:
        """Commit this interactive transaction along with any buffered mutations."""
        mutations_json = _encode_tx_mutations(self._mutations)
        stats = dict(self._handle.commit(mutations_json))
        self._committed = True
        self._mutations.clear()
        self._driver._last_commit_stats = dict(stats)
        return stats

    def rollback(self) -> None:
        """Explicitly roll back this interactive transaction when still active."""
        if self._committed or self._rolled_back:
            return
        self._rolled_back = True
        self._mutations.clear()
        self._handle.rollback()


class AsyncSpannerTransaction:
    """Interactive asynchronous `ReadWrite` transaction bound to a pinned Spanner transaction ID."""

    __slots__ = (
        "_committed",
        "_driver",
        "_handle",
        "_mutations",
        "_rolled_back",
        "_transaction_tag",
    )

    def __init__(
        self,
        driver: AsyncSpannerDriver,
        handle: AsyncSpannerReadWriteTxProtocol,
        *,
        transaction_tag: str | None = None,
    ) -> None:
        self._driver = driver
        self._handle = handle
        self._transaction_tag = transaction_tag
        self._mutations: list[Mutation] = []
        self._committed = False
        self._rolled_back = False

    @property
    def transaction_id(self) -> bytes:
        """Return the server-assigned Cloud Spanner transaction identifier bytes."""
        return bytes(self._handle.transaction_id)

    @property
    def is_active(self) -> bool:
        """Return whether this transaction has not been committed or rolled back."""
        return not self._committed and not self._rolled_back

    @property
    def mutations(self) -> tuple[Mutation, ...]:
        """Return mutations buffered for atomic commit when this transaction completes."""
        return tuple(self._mutations)

    def cursor(self, *, request_tag: str | None = None) -> AsyncSpannerCursorProtocol:
        """Create an asynchronous PEP 249 cursor bound to this interactive read-write transaction."""
        return self._handle.cursor(
            request_tag=request_tag,
            param_encoder=self._driver._encode_parameters,
        )

    async def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement asynchronously inside this interactive read-write transaction."""
        cursor = self.cursor(request_tag=request_tag)
        try:
            annotated = SpannerCore.annotate_sql_with_trace(sql, self._driver._propagator)
            await cursor.execute(annotated, parameters or ())
            return max(cursor.rowcount, 0)
        finally:
            await cursor.close()

    async def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements asynchronously inside this interactive transaction."""
        if not statements:
            return []
        batch_json = SpannerCore.encode_batch_dml_json(statements, dialect=self._driver.dialect)
        lease = self._driver._acquire_pid_lease()
        try:
            res = await self._handle.execute_batch_dml(batch_json, request_tag)
            return [int(c) for c in res]
        finally:
            self._driver._release_pid_lease(lease)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement asynchronously inside this interactive transaction."""
        cursor = self.cursor(request_tag=request_tag)
        try:
            annotated = SpannerCore.annotate_sql_with_trace(sql, self._driver._propagator)
            await cursor.execute(annotated, parameters or ())
            description = cursor.description
            if description is None:
                return []
            cols = tuple(str(col[0]) for col in description)
            return SpannerCore.hydrate_query_rows(
                cols,
                await cursor.fetchall(),
                schema_type=schema_type,
            )
        finally:
            await cursor.close()

    async def query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> list[Any]:
        """Execute a `SELECT` statement asynchronously inside this interactive transaction."""
        return await self.select(sql, parameters, schema_type=schema_type, request_tag=request_tag)

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        rows = await self.select(sql, parameters, schema_type=schema_type, request_tag=request_tag)
        if not rows:
            msg = "Spanner query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any | None:
        """Return the first row of `sql` or `None` when empty."""
        rows = await self.select(sql, parameters, schema_type=schema_type, request_tag=request_tag)
        return rows[0] if rows else None

    async def query_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any:
        """Return the first row of `sql`, raising `NotFoundError` when empty."""
        return await self.select_one(sql, parameters, schema_type=schema_type, request_tag=request_tag)

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        request_tag: str | None = None,
    ) -> Any:
        """Return the first scalar column of the first row of `sql`."""
        cursor = self.cursor(request_tag=request_tag)
        try:
            annotated = SpannerCore.annotate_sql_with_trace(sql, self._driver._propagator)
            await cursor.execute(annotated, parameters or ())
            return _extract_scalar_value(await cursor.fetchall(), value_type)
        finally:
            await cursor.close()

    def buffer_mutation(self, mutation: Mutation) -> None:
        """Buffer a `Mutation` to be applied when the transaction commits."""
        self._mutations.append(mutation)

    def buffer_mutations(self, mutations: Sequence[Mutation]) -> None:
        """Buffer multiple `Mutation` items to be applied when the transaction commits."""
        self._mutations.extend(mutations)

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

    async def commit(self) -> dict[str, Any]:
        """Commit this interactive transaction asynchronously along with any buffered mutations."""
        mutations_json = _encode_tx_mutations(self._mutations)
        stats = dict(await self._handle.commit(mutations_json))
        self._committed = True
        self._mutations.clear()
        self._driver._last_commit_stats = dict(stats)
        return stats

    async def rollback(self) -> None:
        """Explicitly roll back this interactive transaction asynchronously when still active."""
        if self._committed or self._rolled_back:
            return
        self._rolled_back = True
        self._mutations.clear()
        await self._handle.rollback()
