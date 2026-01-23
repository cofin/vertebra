"""Shared broker-backed driver implementations."""

import asyncio
from collections.abc import Iterable, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, TypedDict, TypeVar, cast

import msgspec

from vertebra.resources import AsyncBrokerClient, AsyncBrokerPool, BrokerClientConfig, SyncBrokerClient, SyncBrokerPool
from vertebra.typing import ArrowTable
from vertebra.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Awaitable, Coroutine


T = TypeVar("T")


class VertebraResponse(TypedDict, total=False):
    """Response payload from broker-backed driver calls."""

    columns: list[str]
    rows: list[list[object]]
    rowcount: int
    arrow: object


ParamValue = object
Params = Sequence[ParamValue] | Mapping[str, ParamValue]
Row = tuple[object, ...]


def _run_sync(coro: "Awaitable[T]") -> T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(cast("Coroutine[Any, Any, T]", coro))
    msg = "Use async broker connection inside an event loop"
    raise RuntimeError(msg)


class VertebraDriverConfig(msgspec.Struct, frozen=True):
    """Configuration for a broker-backed database driver."""

    pool: str
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class VertebraSyncCursor:
    def __init__(self, connection: "VertebraSyncConnection") -> None:
        self._connection = connection
        self._rows: list[Row] = []
        self._columns: list[str] = []
        self._index = 0

    def execute(self, query: str, params: Sequence[ParamValue] | None = None) -> int | None:
        result = self._connection._execute(query, params or [])
        if "arrow" in result:
            table = result["arrow"]
            # pyarrow Table
            self._columns = table.column_names  # type: ignore[attr-defined]
            # Convert to rows. Using to_pylist() returns list of dicts.
            self._rows = [tuple(row[col] for col in self._columns) for row in table.to_pylist()]  # type: ignore[attr-defined]
            rowcount = table.num_rows  # type: ignore[attr-defined]
        else:
            self._columns = list(result.get("columns", []))
            self._rows = [tuple(row) for row in result.get("rows", [])]
            rowcount = result.get("rowcount", 0)

        self._index = 0
        return cast("int | None", rowcount)

    def executemany(self, query: str, params: Iterable[Sequence[ParamValue]]) -> int:
        total = 0
        for param_set in params:
            result = self._connection._execute(query, param_set)
            total += result.get("rowcount", 0)
        return total

    def fetchone(self) -> Row | None:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[Row]:
        count = size or 1
        rows = self._rows[self._index : self._index + count]
        self._index += len(rows)
        return rows

    def fetchall(self) -> list[Row]:
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    def close(self) -> None:
        self._rows = []
        self._columns = []
        self._index = 0


class VertebraArrowCursor:
    def __init__(self, connection: "VertebraSyncConnection") -> None:
        self._connection = connection
        self._table: ArrowTable | None = None

    def execute(self, query: str, params: Sequence[ParamValue] | None = None) -> int | None:
        result = self._connection._execute(query, params or [])
        if "arrow" in result:
            self._table = result["arrow"]
        else:
            rows = result.get("rows", [])
            columns = result.get("columns", [])
            # Ensure pyarrow is available with clear error message
            pa = ensure_pyarrow()

            if not rows and not columns:
                self._table = pa.Table.from_pydict({})
            elif rows:
                data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                self._table = pa.Table.from_pydict(data)
            else:
                self._table = pa.Table.from_pydict({col: [] for col in columns})

        return cast("int | None", self._table.num_rows if self._table else 0)

    def fetchall(self) -> ArrowTable | None:
        """Return the pyarrow Table."""
        return self._table


class VertebraSyncConnection:
    def __init__(self, pool: str, client: SyncBrokerClient, token: str) -> None:
        self._pool = pool
        self._client = client
        self._token = token

    @property
    def token(self) -> str:
        return self._token

    def cursor(self) -> VertebraSyncCursor:
        return VertebraSyncCursor(self)

    def arrow_cursor(self) -> VertebraArrowCursor:
        return VertebraArrowCursor(self)

    def begin(self) -> None:
        self._client.begin(self._pool, self._token)

    def commit(self) -> None:
        self._client.commit(self._pool, self._token)

    def rollback(self) -> None:
        self._client.rollback(self._pool, self._token)

    def savepoint(self, name: str) -> None:
        self._client.savepoint(self._pool, self._token, name)

    def release_savepoint(self, name: str) -> None:
        self._client.release_savepoint(self._pool, self._token, name)

    def rollback_savepoint(self, name: str) -> None:
        self._client.rollback_savepoint(self._pool, self._token, name)

    def close(self) -> None:
        self._client.release(self._pool, self._token)

    def _execute(self, query: str, params: Params, correlation_id: str | None = None) -> VertebraResponse:
        payload: list[ParamValue] | dict[str, ParamValue]
        payload = params if isinstance(params, dict) else list(params)
        return cast("VertebraResponse", self._client.execute(self._pool, self._token, query, payload, correlation_id))

    def _execute_many(
        self, query: str, params: Iterable[Params], correlation_id: str | None = None
    ) -> VertebraResponse:
        payload: list[list[ParamValue] | dict[str, ParamValue]] = []
        for param_set in params:
            if isinstance(param_set, dict):
                payload.append(param_set)
            else:
                payload.append(list(param_set))
        return cast(
            "VertebraResponse", self._client.execute_many(self._pool, self._token, query, payload, correlation_id)
        )


class VertebraAsyncCursor:
    def __init__(self, connection: "VertebraAsyncConnection") -> None:
        self._connection = connection
        self._rows: list[Row] = []
        self._columns: list[str] = []
        self._index = 0

    async def execute(self, query: str, params: Sequence[ParamValue] | None = None) -> int | None:
        result = await self._connection._execute(query, params or [])
        if "arrow" in result:
            table = result["arrow"]
            self._columns = table.column_names  # type: ignore[attr-defined]
            self._rows = [tuple(row[col] for col in self._columns) for row in table.to_pylist()]  # type: ignore[attr-defined]
            rowcount = table.num_rows  # type: ignore[attr-defined]
        else:
            self._columns = list(result.get("columns", []))
            self._rows = [tuple(row) for row in result.get("rows", [])]
            rowcount = result.get("rowcount", 0)
        self._index = 0
        return cast("int | None", rowcount)

    async def executemany(self, query: str, params: Iterable[Sequence[ParamValue]]) -> int:
        total = 0
        for param_set in params:
            result = await self._connection._execute(query, param_set)
            total += result.get("rowcount", 0)
        return total

    async def fetchone(self) -> Row | None:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[Row]:
        count = size or 1
        rows = self._rows[self._index : self._index + count]
        self._index += len(rows)
        return rows

    async def fetchall(self) -> list[Row]:
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    async def close(self) -> None:
        self._rows = []
        self._columns = []
        self._index = 0


class VertebraAsyncArrowCursor:
    def __init__(self, connection: "VertebraAsyncConnection") -> None:
        self._connection = connection
        self._table: ArrowTable | None = None

    async def execute(self, query: str, params: Sequence[ParamValue] | None = None) -> int | None:
        result = await self._connection._execute(query, params or [])
        if "arrow" in result:
            self._table = result["arrow"]
        else:
            rows = result.get("rows", [])
            columns = result.get("columns", [])
            # Ensure pyarrow is available with clear error message
            pa = ensure_pyarrow()

            if not rows and not columns:
                self._table = pa.Table.from_pydict({})
            elif rows:
                data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                self._table = pa.Table.from_pydict(data)
            else:
                self._table = pa.Table.from_pydict({col: [] for col in columns})

        return cast("int | None", self._table.num_rows if self._table else 0)

    async def fetchall(self) -> ArrowTable | None:
        return self._table


class VertebraAsyncConnection:
    def __init__(self, pool: str, client: AsyncBrokerClient, token: str) -> None:
        self._pool = pool
        self._client = client
        self._token = token

    @property
    def token(self) -> str:
        return self._token

    def cursor(self) -> VertebraAsyncCursor:
        return VertebraAsyncCursor(self)

    def arrow_cursor(self) -> VertebraAsyncArrowCursor:
        return VertebraAsyncArrowCursor(self)

    async def begin(self) -> None:
        await self._client.begin(self._pool, self._token)

    async def commit(self) -> None:
        await self._client.commit(self._pool, self._token)

    async def rollback(self) -> None:
        await self._client.rollback(self._pool, self._token)

    async def savepoint(self, name: str) -> None:
        await self._client.savepoint(self._pool, self._token, name)

    async def release_savepoint(self, name: str) -> None:
        await self._client.release_savepoint(self._pool, self._token, name)

    async def rollback_savepoint(self, name: str) -> None:
        await self._client.rollback_savepoint(self._pool, self._token, name)

    def transaction(self) -> "VertebraAsyncTransaction":
        return VertebraAsyncTransaction(self)

    async def fetch(self, query: str, params: Sequence[ParamValue] | None = None) -> list[Row]:
        result = await self._execute(query, params or [])
        if "arrow" in result:
            table = result["arrow"]
            columns = table.column_names  # type: ignore[attr-defined]
            return [tuple(row[col] for col in columns) for row in table.to_pylist()]  # type: ignore[attr-defined]
        return [tuple(row) for row in result.get("rows", [])]

    async def fetchrow(self, query: str, params: Sequence[ParamValue] | None = None) -> Row | None:
        rows = await self.fetch(query, params)
        return rows[0] if rows else None

    async def fetchval(self, query: str, params: Sequence[ParamValue] | None = None) -> object | None:
        row = await self.fetchrow(query, params)
        if not row:
            return None
        return row[0]

    async def execute(self, query: str, params: Sequence[ParamValue] | None = None) -> int | None:
        result = await self._execute(query, params or [])
        return result.get("rowcount")

    async def executemany(self, query: str, params: Iterable[Sequence[ParamValue]]) -> int:
        total = 0
        for param_set in params:
            result = await self._execute(query, param_set)
            total += result.get("rowcount", 0)
        return total

    async def close(self) -> None:
        await self._client.release(self._pool, self._token)

    async def _execute(self, query: str, params: Params, correlation_id: str | None = None) -> VertebraResponse:
        payload: list[ParamValue] | dict[str, ParamValue]
        payload = params if isinstance(params, dict) else list(params)
        return cast(
            "VertebraResponse", await self._client.execute(self._pool, self._token, query, payload, correlation_id)
        )

    async def _execute_many(
        self, query: str, params: Iterable[Params], correlation_id: str | None = None
    ) -> VertebraResponse:
        payload: list[list[ParamValue] | dict[str, ParamValue]] = []
        for param_set in params:
            if isinstance(param_set, dict):
                payload.append(param_set)
            else:
                payload.append(list(param_set))
        return cast(
            "VertebraResponse", await self._client.execute_many(self._pool, self._token, query, payload, correlation_id)
        )


class VertebraAsyncTransaction:
    def __init__(self, connection: VertebraAsyncConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> Self:
        await self._connection._client.begin(self._connection._pool, self._connection._token)
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if exc_type is None:
            await self._connection._client.commit(self._connection._pool, self._connection._token)
        else:
            await self._connection._client.rollback(self._connection._pool, self._connection._token)


class VertebraSyncPool:
    def __init__(self, pool: str, config: BrokerClientConfig, max_clients: int = 5) -> None:
        self._pool = SyncBrokerPool(pool, client_config=config, max_clients=max_clients)

    def acquire(self) -> VertebraSyncConnection:
        lease = self._pool.acquire()
        return VertebraSyncConnection(self._pool._pool_name, lease.client, lease.token)

    def release(self, connection: VertebraSyncConnection) -> None:
        self._pool.release(connection._client, connection._token)

    def close(self) -> None:
        self._pool.close()

    def get_size(self) -> int:
        return 0

    def get_idle_size(self) -> int:
        return 0


class VertebraAsyncPool:
    def __init__(self, pool: str, config: BrokerClientConfig, max_clients: int = 5) -> None:
        self._pool = AsyncBrokerPool(pool, client_config=config, max_clients=max_clients)

    async def acquire(self) -> VertebraAsyncConnection:
        lease = await self._pool.acquire()
        return VertebraAsyncConnection(self._pool._pool_name, lease.client, lease.token)

    async def release(self, connection: VertebraAsyncConnection) -> None:
        await self._pool.release(connection._client, connection._token)

    async def close(self) -> None:
        await self._pool.close()

    def get_size(self) -> int:
        return 0

    def get_idle_size(self) -> int:
        return 0


__all__ = (
    "VertebraArrowCursor",
    "VertebraAsyncArrowCursor",
    "VertebraAsyncConnection",
    "VertebraAsyncCursor",
    "VertebraAsyncPool",
    "VertebraDriverConfig",
    "VertebraResponse",
    "VertebraSyncConnection",
    "VertebraSyncCursor",
    "VertebraSyncPool",
)
