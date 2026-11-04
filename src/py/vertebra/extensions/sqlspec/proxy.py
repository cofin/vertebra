"""Shared-memory proxy drivers and session context managers for SQLSpec configurations."""

from __future__ import annotations

import contextlib
import inspect
import os
from typing import TYPE_CHECKING, Any, cast, overload

import msgspec

from vertebra.extensions.sqlspec.provider import (
    BrokerRpcRequest,
    BrokerRpcResponse,
    SharedArrowBuffer,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
    _BaseSQLSpecProvider,
)
from vertebra.typing import ImproperConfigurationError, VertebraError
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_msgpack,
    encode_msgpack_bytes,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Sequence
    from types import TracebackType

__all__ = (
    "ProxyAsyncDriver",
    "ProxyAsyncSessionContext",
    "ProxySyncDriver",
    "ProxySyncSessionContext",
)


class ProxySyncDriver:
    """Synchronous SQLSpec proxy driver executing queries through SQLSpecSyncProvider and SHM leases."""

    __slots__ = (
        "_broker_lease",
        "_context_entered",
        "_in_tx",
        "_leased",
        "_request_seq",
        "_transport_fn",
        "broker",
        "generation",
        "last_response",
        "pool_name",
        "provider",
        "slot_idx",
        "worker_pid",
    )

    def __init__(
        self,
        provider: SQLSpecSyncProvider | Callable[[BrokerRpcRequest], BrokerRpcResponse] | Callable[[bytes], bytes],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
        slot_idx: int = 0,
        generation: int = 0,
    ) -> None:
        """Initialize ProxySyncDriver with a SQLSpecSyncProvider or RPC transport callable."""
        if isinstance(provider, _BaseSQLSpecProvider):
            self.provider: SQLSpecSyncProvider | None = provider
            self._transport_fn: Callable[[Any], Any] = provider.handle_request
            resolved_broker = broker if broker is not None else provider.broker
        elif callable(provider):
            self.provider = None
            self._transport_fn = provider
            resolved_broker = broker
        else:
            raise ImproperConfigurationError("ProxySyncDriver requires a SQLSpecSyncProvider or callable RPC transport")

        self.broker = resolved_broker
        self.pool_name = pool_name
        self.worker_pid = worker_pid if worker_pid is not None else os.getpid()
        self.slot_idx = slot_idx
        self.generation = generation
        self.last_response: BrokerRpcResponse | None = None
        self._broker_lease: Any = None
        self._leased: bool = generation > 0
        self._context_entered: bool = False
        self._in_tx: bool = False
        self._request_seq: int = 0

    @property
    def in_transaction(self) -> bool:
        """Return True when the driver has an active transaction pinned to its lease."""
        return self._in_tx

    @property
    def is_leased(self) -> bool:
        """Return True when the driver holds an active broker lease."""
        return self._leased

    def acquire_lease(self) -> tuple[int, int]:
        """Acquire a connection lease and pin an upstream session in SQLSpecSyncProvider."""
        if self._leased:
            return self.slot_idx, self.generation

        slot_idx = self.slot_idx
        generation = 0
        if self.broker is not None:
            lease = self._acquire_broker_lease()
            self._broker_lease = lease
            slot_idx, generation = self._extract_lease_coords(lease)

        resp = self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op="acquire",
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=slot_idx,
                generation=generation,
            ),
            record_last=False,
        )
        self._raise_for_error(resp)
        self.slot_idx = resp.slot_idx
        self.generation = resp.generation
        self._leased = True
        self._in_tx = resp.in_tx
        return self.slot_idx, self.generation

    def release_lease(self) -> None:
        """Release the active connection lease and unpin the upstream session."""
        if not self._leased:
            return
        was_in_tx = self._in_tx
        self._in_tx = False
        self._leased = False
        lease = self._broker_lease
        self._broker_lease = None
        try:
            if was_in_tx:
                with contextlib.suppress(Exception):
                    self._send_rpc(
                        BrokerRpcRequest(
                            request_id=self._next_request_id(),
                            op="rollback",
                            pool_name=self.pool_name,
                            worker_pid=self.worker_pid,
                            slot_idx=self.slot_idx,
                            generation=self.generation,
                            in_tx=True,
                        ),
                        record_last=False,
                    )
            with contextlib.suppress(Exception):
                self._send_rpc(
                    BrokerRpcRequest(
                        request_id=self._next_request_id(),
                        op="release",
                        pool_name=self.pool_name,
                        worker_pid=self.worker_pid,
                        slot_idx=self.slot_idx,
                        generation=self.generation,
                        in_tx=False,
                    ),
                    record_last=False,
                )
        finally:
            self._release_broker_lease(lease)

    def close(self) -> None:
        """Close the proxy driver and release its held lease."""
        self._context_entered = False
        self.release_lease()

    def begin(self) -> None:
        """Begin a transaction pinned to the current connection lease."""
        self.acquire_lease()
        resp = self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op="begin",
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=self.slot_idx,
                generation=self.generation,
                in_tx=self._in_tx,
            )
        )
        self._raise_for_error(resp)
        self._in_tx = True

    def commit(self) -> None:
        """Commit the active pinned transaction."""
        try:
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="commit",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            self._in_tx = False
            self._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                self.release_lease()

    def rollback(self) -> None:
        """Roll back the active pinned transaction."""
        try:
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="rollback",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            self._in_tx = False
            self._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                self.release_lease()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[ProxySyncDriver]:
        """Context manager executing statements inside a pinned BEGIN/COMMIT/ROLLBACK block."""
        self.begin()
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

    def execute(self, sql: str, *params: Any) -> int:
        """Execute a SQL statement synchronously and return the affected row count."""
        with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            return resp.rowcount

    def execute_many(
        self,
        sql: str,
        params_seq: Sequence[tuple[Any, ...] | list[Any] | dict[str, Any]],
    ) -> int:
        """Execute a batch SQL statement across params_seq and return affected row count."""
        with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            normalized_many: list[list[Any] | dict[str, Any]] = [
                list(item) if isinstance(item, tuple) else item for item in params_seq
            ]
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute_many",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    many_params=normalized_many,
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            return resp.rowcount

    @overload
    def select(self, sql: str, *params: Any, schema_type: None = None) -> list[dict[str, Any]]: ...

    @overload
    def select[SchemaT](self, sql: str, *params: Any, schema_type: type[SchemaT]) -> list[SchemaT]: ...

    def select(self, sql: str, *params: Any, schema_type: type[Any] | None = None) -> list[Any]:
        """Execute a SELECT statement and return rows, transparently reading spilled Arrow SHM."""
        with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows: list[dict[str, Any]] = table.to_pylist()
                    return self._convert_rows(dict_rows, schema_type)
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)

            if schema_type is not None and schema_type is not dict:
                return self._hydrate_raw_rows(resp.rows, resp.columns, schema_type)
            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            return self._convert_rows(dict_rows, schema_type)

    def fetch(self, sql: str, *params: Any, schema_type: type[Any] | None = None) -> list[Any]:
        """Alias for select matching SQLSpec fetch conventions."""
        return self.select(sql, *params, schema_type=schema_type)

    @overload
    def select_one(self, sql: str, *params: Any, schema_type: None = None) -> dict[str, Any]: ...

    @overload
    def select_one[SchemaT](self, sql: str, *params: Any, schema_type: type[SchemaT]) -> SchemaT: ...

    def select_one(self, sql: str, *params: Any, schema_type: type[Any] | None = None) -> Any:
        """Execute a SELECT statement and return exactly one row."""
        with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        raise VertebraError("ValueError: No row found for select_one")
                    return self._convert_single(dict_rows[0], schema_type)
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                raise VertebraError("ValueError: No row found for select_one")
            return self._convert_single(dict_rows[0], schema_type)

    @overload
    def select_one_or_none(self, sql: str, *params: Any, schema_type: None = None) -> dict[str, Any] | None: ...

    @overload
    def select_one_or_none[SchemaT](
        self,
        sql: str,
        *params: Any,
        schema_type: type[SchemaT],
    ) -> SchemaT | None: ...

    def select_one_or_none(
        self,
        sql: str,
        *params: Any,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a SELECT statement and return at most one row or None."""
        with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        return None
                    return self._convert_single(dict_rows[0], schema_type)
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                return None
            return self._convert_single(dict_rows[0], schema_type)

    def select_value(self, sql: str, *params: Any) -> Any:
        """Execute a SELECT statement and return the first column of the first row."""
        with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            return resp.scalar

    def select_to_arrow(self, sql: str, *params: Any) -> Any:
        """Execute a SELECT statement and return a zero-copy PyArrow Table from SharedArrowBuffer."""
        with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_to_arrow",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=self._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            self._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    return SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                finally:
                    self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = self._rows_to_dicts(resp.rows, resp.columns)
            return SharedArrowBuffer.coerce_table(dict_rows, columns=resp.columns)

    def __enter__(self) -> ProxySyncDriver:
        """Enter context manager and acquire a pinned broker lease."""
        self._context_entered = True
        self.acquire_lease()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager, rolling back any uncommitted transaction and releasing the lease."""
        _ = (exc_type, exc_val, exc_tb)
        self._context_entered = False
        self.release_lease()

    def _next_request_id(self) -> int:
        """Increment and return the next monotonically increasing request ID."""
        self._request_seq += 1
        return self._request_seq

    @contextlib.contextmanager
    def _operation_lease(self) -> Iterator[None]:
        """Ensure a lease is held for an operation, releasing it afterwards when not context-pinned."""
        auto_acquired = not self._leased
        if auto_acquired:
            self.acquire_lease()
        try:
            yield
        finally:
            if auto_acquired and not self._context_entered and not self._in_tx:
                self.release_lease()

    def _compile_statement(self, sql: str) -> str:
        """Consult the broker statement cache when available and return compiled SQL."""
        if self.broker is None:
            return sql
        target = getattr(self.broker, "transport", self.broker)
        compile_fn = getattr(target, "get_or_compile_statement", None)
        if callable(compile_fn):
            plan = compile_fn("sqlspec", sql, lambda raw: (raw, ()))
            compiled = getattr(plan, "compiled_sql", None)
            if isinstance(compiled, str):
                return compiled
        return sql

    def _send_rpc(self, request: BrokerRpcRequest, *, record_last: bool = True) -> BrokerRpcResponse:
        """Dispatch a BrokerRpcRequest over the configured provider or raw transport."""
        try:
            res = self._transport_fn(request)
        except TypeError:
            res = self._transport_fn(encode_msgpack_bytes(request))
        if isinstance(res, (bytes, bytearray, memoryview)):
            resp = cast("BrokerRpcResponse", decode_msgpack(res, type=BrokerRpcResponse))
        elif isinstance(res, BrokerRpcResponse):
            resp = res
        else:
            raw_req = encode_msgpack_bytes(request)
            raw_resp = self._transport_fn(raw_req)
            resp = cast("BrokerRpcResponse", decode_msgpack(raw_resp, type=BrokerRpcResponse))
        if record_last:
            self.last_response = resp
        return resp

    def _release_arrow_shm(self, shm_name: str) -> None:
        """Send arrow_release RPC and unlink the shared-memory Arrow segment."""
        if not shm_name:
            return
        try:
            self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="arrow_release",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                    arrow_shm_name=shm_name,
                ),
                record_last=False,
            )
        finally:
            SharedArrowBuffer.unlink(shm_name)

    def _acquire_broker_lease(self) -> Any:
        """Acquire a lease from self.broker."""
        if self.provider is not None and self.broker is self.provider.broker:
            return None
        acquire_lease_fn = getattr(self.broker, "acquire_lease", None)
        if callable(acquire_lease_fn):
            return _BaseSQLSpecProvider._call_broker_acquire(acquire_lease_fn, self.pool_name, self.worker_pid)
        target = getattr(self.broker, "transport", self.broker)
        acquire_fn = getattr(target, "acquire", None)
        if callable(acquire_fn):
            return _BaseSQLSpecProvider._call_broker_acquire(acquire_fn, self.pool_name, self.worker_pid)
        return None

    def _release_broker_lease(self, lease: Any) -> None:
        """Release a directly acquired broker lease."""
        if lease is None:
            return
        if hasattr(lease, "release") and callable(lease.release):
            with contextlib.suppress(Exception):
                lease.release()
            return
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            with contextlib.suppress(Exception):
                release_lease_fn(self.slot_idx, self.generation, in_tx=False)
            return
        target = getattr(self.broker, "transport", self.broker)
        release_fn = getattr(target, "release", None)
        if callable(release_fn):
            with contextlib.suppress(Exception):
                release_fn(f"{self.slot_idx}:{self.generation}")

    @staticmethod
    def _extract_lease_coords(lease: Any) -> tuple[int, int]:
        """Extract (slot_idx, generation) from a lease object or tuple."""
        if lease is None:
            return 0, 0
        if isinstance(lease, tuple) and len(lease) >= 2:
            return int(lease[0]), int(lease[1])
        slot_idx = int(getattr(lease, "slot_idx", getattr(lease, "slot_index", 0)))
        generation = int(getattr(lease, "generation", 1))
        return slot_idx, max(1, generation)

    @staticmethod
    def _normalize_params(params: tuple[Any, ...]) -> list[Any] | dict[str, Any]:
        """Normalize positional varargs into a flat parameter list or dict."""
        if not params:
            return []
        if len(params) == 1 and isinstance(params[0], (tuple, list, dict)):
            first = params[0]
            if isinstance(first, (tuple, list)):
                return list(first)
            return dict(first)
        return list(params)

    @staticmethod
    def _rows_to_dicts(
        rows: Sequence[dict[str, Any] | tuple[Any, ...] | list[Any]],
        columns: Sequence[str],
    ) -> list[dict[str, Any]]:
        """Convert a sequence of dict or tuple rows into a list of dictionaries."""
        if not rows:
            return []
        first = rows[0]
        col_names = list(columns) if columns else [f"c{idx}" for idx in range(len(first))]
        return [dict(r) if isinstance(r, dict) else dict(zip(col_names, r, strict=False)) for r in rows]

    @staticmethod
    def _hydrate_raw_rows(
        rows: Sequence[dict[str, Any] | tuple[Any, ...] | list[Any]],
        columns: Sequence[str],
        schema_type: type[Any],
    ) -> list[Any]:
        """Hydrate raw tuple/list/dict rows into schema_type via C-API vectorcall without intermediate dicts."""
        if not rows:
            return []
        plan = ZeroCopyCodecRegistry.compile(schema_type)
        first = rows[0]
        col_names = (
            tuple(columns)
            if columns
            else (tuple(first.keys()) if isinstance(first, dict) else tuple(f"c{idx}" for idx in range(len(first))))
        )
        if isinstance(first, dict):
            tuple_rows = [tuple(r.get(c) for c in col_names) for r in rows if isinstance(r, dict)]
            return plan.hydrate_rows(tuple_rows, col_names)
        return plan.hydrate_rows(rows, col_names)

    @staticmethod
    def _convert_rows(rows: list[dict[str, Any]], schema_type: type[Any] | None) -> list[Any]:
        """Convert dictionary rows into schema_type instances when schema_type is provided."""
        if schema_type is None or schema_type is dict:
            return rows
        return [msgspec.convert(row, type=schema_type) for row in rows]

    @staticmethod
    def _convert_single(row: dict[str, Any], schema_type: type[Any] | None) -> Any:
        """Convert a single dictionary row into schema_type when provided."""
        if schema_type is None or schema_type is dict:
            return row
        return msgspec.convert(row, type=schema_type)

    @staticmethod
    def _raise_for_error(response: BrokerRpcResponse) -> None:
        """Raise VertebraError when response.ok is False."""
        if not response.ok:
            err_type = response.error_type or "BrokerRpcError"
            raise VertebraError(f"{err_type}: {response.error_message}")


class ProxyAsyncDriver:
    """Asynchronous SQLSpec proxy driver executing queries through SQLSpecAsyncProvider and SHM leases."""

    __slots__ = (
        "_broker_lease",
        "_context_entered",
        "_in_tx",
        "_leased",
        "_request_seq",
        "_transport_fn",
        "broker",
        "generation",
        "last_response",
        "pool_name",
        "provider",
        "slot_idx",
        "worker_pid",
    )

    def __init__(
        self,
        provider: SQLSpecAsyncProvider | Callable[[BrokerRpcRequest], Any] | Callable[[bytes], Any],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
        slot_idx: int = 0,
        generation: int = 0,
    ) -> None:
        """Initialize ProxyAsyncDriver with a SQLSpecAsyncProvider or async RPC transport callable."""
        if isinstance(provider, _BaseSQLSpecProvider):
            self.provider: SQLSpecAsyncProvider | None = provider
            self._transport_fn: Callable[[Any], Any] = provider.handle_request
            resolved_broker = broker if broker is not None else provider.broker
        elif callable(provider):
            self.provider = None
            self._transport_fn = provider
            resolved_broker = broker
        else:
            raise ImproperConfigurationError(
                "ProxyAsyncDriver requires a SQLSpecAsyncProvider or callable RPC transport"
            )

        self.broker = resolved_broker
        self.pool_name = pool_name
        self.worker_pid = worker_pid if worker_pid is not None else os.getpid()
        self.slot_idx = slot_idx
        self.generation = generation
        self.last_response: BrokerRpcResponse | None = None
        self._broker_lease: Any = None
        self._leased: bool = generation > 0
        self._context_entered: bool = False
        self._in_tx: bool = False
        self._request_seq: int = 0

    @property
    def in_transaction(self) -> bool:
        """Return True when the driver has an active transaction pinned to its lease."""
        return self._in_tx

    @property
    def is_leased(self) -> bool:
        """Return True when the driver holds an active broker lease."""
        return self._leased

    async def acquire_lease(self) -> tuple[int, int]:
        """Acquire a connection lease asynchronously and pin an upstream session."""
        if self._leased:
            return self.slot_idx, self.generation

        slot_idx = self.slot_idx
        generation = 0
        if self.broker is not None:
            lease = await self._acquire_broker_lease()
            self._broker_lease = lease
            slot_idx, generation = ProxySyncDriver._extract_lease_coords(lease)

        resp = await self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op="acquire",
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=slot_idx,
                generation=generation,
            ),
            record_last=False,
        )
        ProxySyncDriver._raise_for_error(resp)
        self.slot_idx = resp.slot_idx
        self.generation = resp.generation
        self._leased = True
        self._in_tx = resp.in_tx
        return self.slot_idx, self.generation

    async def release_lease(self) -> None:
        """Release the active connection lease asynchronously and unpin the upstream session."""
        if not self._leased:
            return
        was_in_tx = self._in_tx
        self._in_tx = False
        self._leased = False
        lease = self._broker_lease
        self._broker_lease = None
        try:
            if was_in_tx:
                with contextlib.suppress(Exception):
                    await self._send_rpc(
                        BrokerRpcRequest(
                            request_id=self._next_request_id(),
                            op="rollback",
                            pool_name=self.pool_name,
                            worker_pid=self.worker_pid,
                            slot_idx=self.slot_idx,
                            generation=self.generation,
                            in_tx=True,
                        ),
                        record_last=False,
                    )
            with contextlib.suppress(Exception):
                await self._send_rpc(
                    BrokerRpcRequest(
                        request_id=self._next_request_id(),
                        op="release",
                        pool_name=self.pool_name,
                        worker_pid=self.worker_pid,
                        slot_idx=self.slot_idx,
                        generation=self.generation,
                        in_tx=False,
                    ),
                    record_last=False,
                )
        finally:
            await self._release_broker_lease_async(lease)

    async def close(self) -> None:
        """Close the async proxy driver and release its held lease."""
        self._context_entered = False
        await self.release_lease()

    async def begin(self) -> None:
        """Begin a transaction pinned to the current connection lease."""
        await self.acquire_lease()
        resp = await self._send_rpc(
            BrokerRpcRequest(
                request_id=self._next_request_id(),
                op="begin",
                pool_name=self.pool_name,
                worker_pid=self.worker_pid,
                slot_idx=self.slot_idx,
                generation=self.generation,
                in_tx=self._in_tx,
            )
        )
        ProxySyncDriver._raise_for_error(resp)
        self._in_tx = True

    async def commit(self) -> None:
        """Commit the active pinned transaction."""
        try:
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="commit",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            self._in_tx = False
            ProxySyncDriver._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                await self.release_lease()

    async def rollback(self) -> None:
        """Roll back the active pinned transaction."""
        try:
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="rollback",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                )
            )
            self._in_tx = False
            ProxySyncDriver._raise_for_error(resp)
        finally:
            if not self._context_entered and not self._in_tx:
                await self.release_lease()

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[ProxyAsyncDriver]:
        """Async context manager executing statements inside a pinned BEGIN/COMMIT/ROLLBACK block."""
        await self.begin()
        try:
            yield self
        except BaseException:
            await self.rollback()
            raise
        else:
            await self.commit()

    async def execute(self, sql: str, *params: Any) -> int:
        """Execute a SQL statement asynchronously and return the affected row count."""
        async with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            return resp.rowcount

    async def execute_many(
        self,
        sql: str,
        params_seq: Sequence[tuple[Any, ...] | list[Any] | dict[str, Any]],
    ) -> int:
        """Execute a batch SQL statement asynchronously across params_seq and return affected row count."""
        async with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            normalized_many: list[list[Any] | dict[str, Any]] = [
                list(item) if isinstance(item, tuple) else item for item in params_seq
            ]
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="execute_many",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    many_params=normalized_many,
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            return resp.rowcount

    @overload
    async def select(self, sql: str, *params: Any, schema_type: None = None) -> list[dict[str, Any]]: ...

    @overload
    async def select[SchemaT](self, sql: str, *params: Any, schema_type: type[SchemaT]) -> list[SchemaT]: ...

    async def select(
        self,
        sql: str,
        *params: Any,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a SELECT statement asynchronously and return rows, transparently reading spilled Arrow SHM."""
        async with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows: list[dict[str, Any]] = table.to_pylist()
                    return ProxySyncDriver._convert_rows(dict_rows, schema_type)
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)

            if schema_type is not None and schema_type is not dict:
                return ProxySyncDriver._hydrate_raw_rows(resp.rows, resp.columns, schema_type)
            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            return ProxySyncDriver._convert_rows(dict_rows, schema_type)

    async def fetch(
        self,
        sql: str,
        *params: Any,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Alias for select matching SQLSpec fetch conventions."""
        return await self.select(sql, *params, schema_type=schema_type)

    @overload
    async def select_one(self, sql: str, *params: Any, schema_type: None = None) -> dict[str, Any]: ...

    @overload
    async def select_one[SchemaT](self, sql: str, *params: Any, schema_type: type[SchemaT]) -> SchemaT: ...

    async def select_one(
        self,
        sql: str,
        *params: Any,
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a SELECT statement asynchronously and return exactly one row."""
        async with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        raise VertebraError("ValueError: No row found for select_one")
                    return ProxySyncDriver._convert_single(dict_rows[0], schema_type)
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                raise VertebraError("ValueError: No row found for select_one")
            return ProxySyncDriver._convert_single(dict_rows[0], schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        *params: Any,
        schema_type: None = None,
    ) -> dict[str, Any] | None: ...

    @overload
    async def select_one_or_none[SchemaT](
        self,
        sql: str,
        *params: Any,
        schema_type: type[SchemaT],
    ) -> SchemaT | None: ...

    async def select_one_or_none(
        self,
        sql: str,
        *params: Any,
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a SELECT statement asynchronously and return at most one row or None."""
        async with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_one_or_none",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    table = SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                    dict_rows = table.to_pylist()
                    if not dict_rows:
                        return None
                    return ProxySyncDriver._convert_single(dict_rows[0], schema_type)
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            if not dict_rows:
                return None
            return ProxySyncDriver._convert_single(dict_rows[0], schema_type)

    async def select_value(self, sql: str, *params: Any) -> Any:
        """Execute a SELECT statement asynchronously and return the first scalar column value."""
        async with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_value",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            return resp.scalar

    async def select_to_arrow(self, sql: str, *params: Any) -> Any:
        """Execute a SELECT statement asynchronously and return a zero-copy PyArrow Table."""
        async with self._operation_lease():
            compiled_sql = self._compile_statement(sql)
            resp = await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="select_to_arrow",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    sql=compiled_sql,
                    params=ProxySyncDriver._normalize_params(params),
                    in_tx=self._in_tx,
                )
            )
            ProxySyncDriver._raise_for_error(resp)
            if resp.is_arrow_shm:
                try:
                    return SharedArrowBuffer.read_table(resp.arrow_shm_name, resp.arrow_byte_len)
                finally:
                    await self._release_arrow_shm(resp.arrow_shm_name)

            dict_rows = ProxySyncDriver._rows_to_dicts(resp.rows, resp.columns)
            return SharedArrowBuffer.coerce_table(dict_rows, columns=resp.columns)

    async def __aenter__(self) -> ProxyAsyncDriver:
        """Enter async context manager and acquire a pinned broker lease."""
        self._context_entered = True
        await self.acquire_lease()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager, rolling back any uncommitted transaction and releasing the lease."""
        _ = (exc_type, exc_val, exc_tb)
        self._context_entered = False
        await self.release_lease()

    def _next_request_id(self) -> int:
        """Increment and return the next monotonically increasing request ID."""
        self._request_seq += 1
        return self._request_seq

    @contextlib.asynccontextmanager
    async def _operation_lease(self) -> AsyncIterator[None]:
        """Ensure a lease is held for an async operation, releasing it afterwards when not context-pinned."""
        auto_acquired = not self._leased
        if auto_acquired:
            await self.acquire_lease()
        try:
            yield
        finally:
            if auto_acquired and not self._context_entered and not self._in_tx:
                await self.release_lease()

    def _compile_statement(self, sql: str) -> str:
        """Consult the broker statement cache when available and return compiled SQL."""
        if self.broker is None:
            return sql
        target = getattr(self.broker, "transport", self.broker)
        compile_fn = getattr(target, "get_or_compile_statement", None)
        if callable(compile_fn):
            plan = compile_fn("sqlspec", sql, lambda raw: (raw, ()))
            compiled = getattr(plan, "compiled_sql", None)
            if isinstance(compiled, str):
                return compiled
        return sql

    async def _send_rpc(self, request: BrokerRpcRequest, *, record_last: bool = True) -> BrokerRpcResponse:
        """Dispatch a BrokerRpcRequest asynchronously over the configured provider or raw transport."""
        try:
            res = self._transport_fn(request)
            if inspect.isawaitable(res):
                res = await res
        except TypeError:
            res = self._transport_fn(encode_msgpack_bytes(request))
            if inspect.isawaitable(res):
                res = await res
        if isinstance(res, (bytes, bytearray, memoryview)):
            resp = cast("BrokerRpcResponse", decode_msgpack(res, type=BrokerRpcResponse))
        elif isinstance(res, BrokerRpcResponse):
            resp = res
        else:
            raw_req = encode_msgpack_bytes(request)
            raw_resp = self._transport_fn(raw_req)
            if inspect.isawaitable(raw_resp):
                raw_resp = await raw_resp
            resp = cast("BrokerRpcResponse", decode_msgpack(raw_resp, type=BrokerRpcResponse))
        if record_last:
            self.last_response = resp
        return resp

    async def _release_arrow_shm(self, shm_name: str) -> None:
        """Send arrow_release RPC asynchronously and unlink the shared-memory Arrow segment."""
        if not shm_name:
            return
        try:
            await self._send_rpc(
                BrokerRpcRequest(
                    request_id=self._next_request_id(),
                    op="arrow_release",
                    pool_name=self.pool_name,
                    worker_pid=self.worker_pid,
                    slot_idx=self.slot_idx,
                    generation=self.generation,
                    in_tx=self._in_tx,
                    arrow_shm_name=shm_name,
                ),
                record_last=False,
            )
        finally:
            SharedArrowBuffer.unlink(shm_name)

    async def _acquire_broker_lease(self) -> Any:
        """Acquire a lease asynchronously from self.broker."""
        if self.provider is not None and self.broker is self.provider.broker:
            return None
        target = getattr(self.broker, "transport", self.broker)
        for attr in ("acquire_lease", "acquire"):
            fn = getattr(self.broker, attr, None) or getattr(target, attr, None)
            if callable(fn):
                res = _BaseSQLSpecProvider._call_broker_acquire(fn, self.pool_name, self.worker_pid)
                if inspect.isawaitable(res):
                    return await res
                return res
        return None

    async def _release_broker_lease_async(self, lease: Any) -> None:
        """Release a directly acquired broker lease asynchronously."""
        if lease is None:
            return
        if hasattr(lease, "release") and callable(lease.release):
            with contextlib.suppress(Exception):
                res = lease.release()
                if inspect.isawaitable(res):
                    await res
            return
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            with contextlib.suppress(Exception):
                res = release_lease_fn(self.slot_idx, self.generation, in_tx=False)
                if inspect.isawaitable(res):
                    await res
            return
        target = getattr(self.broker, "transport", self.broker)
        release_fn = getattr(target, "release", None)
        if callable(release_fn):
            with contextlib.suppress(Exception):
                res = release_fn(f"{self.slot_idx}:{self.generation}")
                if inspect.isawaitable(res):
                    await res


class ProxySyncSessionContext:
    """Synchronous session context manager yielding a lease-pinned ProxySyncDriver."""

    __slots__ = ("_driver",)

    def __init__(
        self,
        provider: SQLSpecSyncProvider | Callable[[BrokerRpcRequest], BrokerRpcResponse],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
    ) -> None:
        """Initialize ProxySyncSessionContext wrapping a ProxySyncDriver."""
        self._driver = ProxySyncDriver(
            provider=provider,
            broker=broker,
            pool_name=pool_name,
            worker_pid=worker_pid,
        )

    @property
    def driver(self) -> ProxySyncDriver:
        """Return the underlying ProxySyncDriver instance."""
        return self._driver

    def __enter__(self) -> ProxySyncDriver:
        """Acquire lease and return the active ProxySyncDriver."""
        return self._driver.__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release the lease on context exit."""
        self._driver.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name: str) -> Any:
        """Forward direct driver method calls to the underlying ProxySyncDriver."""
        return getattr(self._driver, name)


class ProxyAsyncSessionContext:
    """Asynchronous session context manager yielding a lease-pinned ProxyAsyncDriver."""

    __slots__ = ("_driver",)

    def __init__(
        self,
        provider: SQLSpecAsyncProvider | Callable[[BrokerRpcRequest], Any],
        broker: Any = None,
        *,
        pool_name: str = "default",
        worker_pid: int | None = None,
    ) -> None:
        """Initialize ProxyAsyncSessionContext wrapping a ProxyAsyncDriver."""
        self._driver = ProxyAsyncDriver(
            provider=provider,
            broker=broker,
            pool_name=pool_name,
            worker_pid=worker_pid,
        )

    @property
    def driver(self) -> ProxyAsyncDriver:
        """Return the underlying ProxyAsyncDriver instance."""
        return self._driver

    async def __aenter__(self) -> ProxyAsyncDriver:
        """Acquire lease asynchronously and return the active ProxyAsyncDriver."""
        return await self._driver.__aenter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release the lease on async context exit."""
        await self._driver.__aexit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name: str) -> Any:
        """Forward direct driver method calls to the underlying ProxyAsyncDriver."""
        return getattr(self._driver, name)
