"""Shared-memory RPC provider host and zero-copy Arrow IPC buffer for SQLSpec drivers."""

from __future__ import annotations

import contextlib
import os
import threading
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import msgspec
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import CommonDriverMixin, InterfaceError, NotFoundError
from vertebra.extensions.broker import (
    AsyncBrokerLeaseSource,
    AsyncSHMBrokerTransport,
    SyncBrokerLeaseSource,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, VertebraError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import (
    decode_json,
    decode_msgpack,
    encode_json_bytes,
    encode_msgpack_bytes,
)
from vertebra.utils.sync_tools import SyncPool
from vertebra.utils.type_guards import is_coroutine_callable, is_msgspec_struct

__all__ = (
    "ArrowCursor",
    "AsyncBrokerLeaseSource",
    "BrokerRpcRequest",
    "BrokerRpcResponse",
    "SQLSpecAsyncProvider",
    "SQLSpecSyncProvider",
    "SyncBrokerLeaseSource",
)


class BrokerRpcRequest(msgspec.Struct, frozen=True):
    """RPC request frame dispatched from a worker proxy driver to SQLSpecSyncProvider or SQLSpecAsyncProvider."""

    request_id: int = 0
    op: str = "execute"
    pool_name: str = "default"
    worker_pid: int = 0
    slot_idx: int = 0
    generation: int = 0
    sql: str = ""
    params: list[Any] | dict[str, Any] = msgspec.field(default_factory=list)
    many_params: list[list[Any] | dict[str, Any]] = msgspec.field(default_factory=list)
    in_tx: bool = False
    arrow_shm_name: str = ""
    stream_id: str = ""
    chunk_rows: int = 0


class BrokerRpcResponse(msgspec.Struct, frozen=True):
    """RPC response frame returned from SQLSpecSyncProvider or SQLSpecAsyncProvider to a worker proxy driver."""

    request_id: int = 0
    ok: bool = True
    slot_idx: int = 0
    generation: int = 0
    rows: list[dict[str, Any] | list[Any]] = msgspec.field(default_factory=list)
    columns: list[str] = msgspec.field(default_factory=list)
    rowcount: int = 0
    scalar: Any = None
    in_tx: bool = False
    is_arrow_shm: bool = False
    arrow_shm_name: str = ""
    arrow_byte_len: int = 0
    error_type: str = ""
    error_message: str = ""
    stream_id: str = ""
    has_more: bool = False


def _coerce_dict_rows(
    data: Any,
    columns: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Coerce tabular objects or sequences into normalized (dict_rows, column_names)."""
    if isinstance(data, ArrowCursor):
        py_rows = data.to_pylist()
        cols = list(data._columns) if data._columns else list(columns or ())
        if not cols and py_rows:
            cols = list(py_rows[0].keys())
        return py_rows, cols
    to_pylist_fn = getattr(data, "to_pylist", None)
    if callable(to_pylist_fn):
        py_rows = list(cast("Iterable[Any]", to_pylist_fn()))
        raw_cols = getattr(data, "column_names", getattr(data, "columns", columns))
        cols = [str(c) for c in raw_cols] if isinstance(raw_cols, (list, tuple)) else []
        if not cols and py_rows and isinstance(py_rows[0], dict):
            cols = list(py_rows[0].keys())
        return [dict(r) for r in py_rows], cols
    for arrow_attr in ("to_arrow_table", "to_pyarrow", "to_arrow"):
        arrow_fn = getattr(data, arrow_attr, None)
        if callable(arrow_fn):
            converted = arrow_fn()
            if converted is not data:
                return _coerce_dict_rows(converted, columns=columns)
    items = data if isinstance(data, list) else list(data)
    if not items:
        return [], list(columns) if columns else []
    first = items[0]
    if isinstance(first, dict):
        dict_rows = [dict(r) for r in items]
        cols = list(columns) if columns else list(dict_rows[0].keys())
        return dict_rows, cols
    if is_msgspec_struct(first):
        struct_rows = [msgspec.structs.asdict(item) for item in items]
        cols = list(columns) if columns else list(struct_rows[0].keys())
        return struct_rows, cols
    if isinstance(first, (tuple, list)):
        cols = list(columns) if columns else [f"c{idx}" for idx in range(len(first))]
        return [dict(zip(cols, row, strict=False)) for row in items], cols
    return [{"value": item} for item in items], list(columns) if columns else ["value"]


def _coerce_pyarrow_table(pa: Any, data: Any, columns: Sequence[str] | None = None) -> Any:
    """Coerce an Arrow object or row collection into a pyarrow.Table."""
    if hasattr(data, "schema") and hasattr(data, "num_rows"):
        if isinstance(data, pa.RecordBatch):
            return pa.Table.from_batches([data])
        return data
    for arrow_attr in ("to_arrow", "to_arrow_table", "to_pyarrow"):
        arrow_fn = getattr(data, arrow_attr, None)
        if callable(arrow_fn):
            converted = arrow_fn()
            if converted is data:
                continue
            if hasattr(converted, "schema") and hasattr(converted, "num_rows"):
                if isinstance(converted, pa.RecordBatch):
                    return pa.Table.from_batches([converted])
                return converted
            return _coerce_pyarrow_table(pa, converted, columns=columns)
    dict_rows, cols = _coerce_dict_rows(data, columns=columns)
    if not dict_rows:
        if cols:
            return pa.Table.from_pydict({col: [] for col in cols})
        return pa.table({})
    return pa.Table.from_pylist(dict_rows)


def _write_vtbarrow_cursor(
    data: Any,
    *,
    columns: Sequence[str] | None = None,
) -> ArrowCursor:
    """Write a PyArrow table or row sequence into a VTBARROW POSIX SHM ArrowCursor."""
    native_mod = import_optional("vertebra._vertebra")
    native_arrow_cls = getattr(native_mod, "ArrowCursor", None) if native_mod is not None else None
    if native_arrow_cls is None or not hasattr(native_arrow_cls, "write_ipc_to_shm"):
        raise MissingDependencyError("vertebra._vertebra.ArrowCursor", install_package="vertebra-core")
    pa = import_optional("pyarrow")
    if pa is not None:
        table = _coerce_pyarrow_table(pa, data, columns=columns)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        ipc_bytes = sink.getvalue().to_pybytes()
        shm_name = ArrowCursor._next_shm_segment_name()
        row_count = int(table.num_rows)
        native_cur = native_arrow_cls.write_ipc_to_shm(shm_name, ipc_bytes, row_count)
        return ArrowCursor(
            native_cur,
            columns=tuple(str(c) for c in table.column_names),
            used_shm=True,
            row_count=row_count,
        )
    dict_rows, cols = _coerce_dict_rows(data, columns=columns)
    json_bytes = encode_json_bytes(dict_rows)
    shm_name = ArrowCursor._next_shm_segment_name()
    native_cur = native_arrow_cls.write_ipc_to_shm(shm_name, json_bytes, len(dict_rows))
    return ArrowCursor(
        native_cur,
        columns=tuple(cols),
        used_shm=True,
        row_count=len(dict_rows),
    )


def _unlink_arrow_shm(shm_name: str, cursor: ArrowCursor | None = None) -> bool:
    """Release an active VTBARROW ArrowCursor and ensure its POSIX SHM segment is unlinked."""
    if not shm_name:
        return False
    released = False
    if cursor is not None:
        with contextlib.suppress(Exception):
            released = bool(cursor.release())
    native_mod = import_optional("vertebra._vertebra")
    native_cls = getattr(native_mod, "ArrowCursor", None) if native_mod is not None else None
    shm_path = Path("/dev/shm") / shm_name.lstrip("/")
    if native_cls is not None and shm_path.exists():
        with contextlib.suppress(Exception):
            released = bool(native_cls(shm_name, 0).release()) or released
    if shm_path.exists():
        with contextlib.suppress(OSError):
            shm_path.unlink()
            released = True
    return released


class _ProviderStream:
    """Active paged stream state held by SQLSpecSyncProvider or SQLSpecAsyncProvider."""

    __slots__ = (
        "chunk_rows",
        "closed",
        "columns",
        "cursor",
        "iterator",
        "kind",
        "lease_key",
        "rows_buffer",
        "rows_offset",
        "stream_id",
        "stream_obj",
    )

    def __init__(
        self,
        *,
        stream_id: str,
        lease_key: tuple[int, int],
        chunk_rows: int,
        kind: str,
        columns: list[str] | None = None,
        stream_obj: Any = None,
        iterator: Any = None,
        cursor: Any = None,
        rows_buffer: list[dict[str, Any] | list[Any]] | None = None,
    ) -> None:
        self.stream_id = stream_id
        self.lease_key = lease_key
        self.chunk_rows = chunk_rows
        self.kind = kind
        self.columns: list[str] = list(columns) if columns else []
        self.stream_obj = stream_obj
        self.iterator = iterator
        self.cursor = cursor
        self.rows_buffer: list[dict[str, Any] | list[Any]] = rows_buffer if rows_buffer is not None else []
        self.rows_offset: int = 0
        self.closed: bool = False


class _BaseSQLSpecProvider:
    """Host provider base executing SQLSpec driver operations over pinned broker leases and Arrow SHM."""

    __slots__ = (
        "_active_arrow_buffers",
        "_active_streams",
        "_broker_leases",
        "_lease_arrow_buffers",
        "_lease_streams",
        "_leased_contexts",
        "_leased_sessions",
        "_lock",
        "_next_generation",
        "_next_stream_seq",
        "_tx_states",
        "_upstreams",
        "arrow_threshold_bytes",
        "arrow_threshold_rows",
        "broker",
        "default_pool_name",
    )

    def __init__(
        self,
        upstream: Any = None,
        broker: Any = None,
        *,
        pool_name: str = "default",
        arrow_threshold_rows: int = 256,
        arrow_threshold_bytes: int = 65_536,
    ) -> None:
        """Initialize provider with an upstream config/driver/factory and optional broker."""
        self.default_pool_name = pool_name
        self.broker = broker
        self.arrow_threshold_rows = arrow_threshold_rows
        self.arrow_threshold_bytes = arrow_threshold_bytes
        self._upstreams: dict[str, Any] = {}
        self._leased_sessions: dict[tuple[int, int], Any] = {}
        self._leased_contexts: dict[tuple[int, int], Any] = {}
        self._broker_leases: dict[tuple[int, int], Any] = {}
        self._tx_states: dict[tuple[int, int], bool] = {}
        self._active_arrow_buffers: dict[str, ArrowCursor] = {}
        self._lease_arrow_buffers: dict[tuple[int, int], set[str]] = {}
        self._active_streams: dict[str, _ProviderStream] = {}
        self._lease_streams: dict[tuple[int, int], set[str]] = {}
        self._next_generation: int = 1
        self._next_stream_seq: int = 1
        self._lock = threading.RLock()

        if isinstance(upstream, dict):
            self._upstreams.update(upstream)
        elif upstream is not None:
            self._upstreams[pool_name] = upstream
            self._upstreams.setdefault("default", upstream)
            bind_key = getattr(upstream, "bind_key", None)
            if isinstance(bind_key, str) and bind_key:
                self._upstreams[bind_key] = upstream

    @property
    def active_leases(self) -> int:
        """Return the number of currently active leased sessions."""
        with self._lock:
            return len(self._leased_sessions)

    @property
    def active_arrow_buffers(self) -> frozenset[str]:
        """Return the set of currently active shared-memory Arrow buffer names."""
        with self._lock:
            return frozenset(self._active_arrow_buffers)

    @property
    def active_streams(self) -> frozenset[str]:
        """Return the set of currently active stream identifiers."""
        with self._lock:
            return frozenset(self._active_streams)

    def register_pool(self, pool_name: str, upstream: Any) -> None:
        """Register an upstream configuration, driver, or session factory under pool_name."""
        with self._lock:
            self._upstreams[pool_name] = upstream

    def is_in_transaction(self, slot_idx: int, generation: int) -> bool:
        """Return True when the lease identified by (slot_idx, generation) is in an open transaction."""
        with self._lock:
            return self._tx_states.get((slot_idx, generation), False)

    def get_leased_session(self, slot_idx: int, generation: int) -> Any | None:
        """Return the pinned upstream session for (slot_idx, generation) if leased."""
        with self._lock:
            return self._leased_sessions.get((slot_idx, generation))

    def resolve_upstream_optional(self, pool_name: str) -> Any:
        """Return registered upstream config or driver for pool_name when present."""
        with self._lock:
            if pool_name in self._upstreams:
                return self._upstreams[pool_name]
            if self.default_pool_name in self._upstreams:
                return self._upstreams[self.default_pool_name]
            if "default" in self._upstreams:
                return self._upstreams["default"]
            if self._upstreams:
                return next(iter(self._upstreams.values()))
        return None

    def _tx_state_for(self, key: tuple[int, int], default: bool) -> bool:
        with self._lock:
            return self._tx_states.get(key, default)

    @staticmethod
    def _format_error_message(exc: BaseException) -> str:
        msg = str(exc)
        chained = exc.__cause__ or exc.__context__
        if chained is not None:
            detail = f"{type(chained).__name__}: {chained}"
            if detail not in msg and str(chained) not in msg:
                return f"{msg} (caused by {detail})" if msg else detail
        return msg

    def _handle_request_sync(self, request: BrokerRpcRequest) -> BrokerRpcResponse:
        """Execute a synchronous BrokerRpcRequest and return a BrokerRpcResponse."""
        op = request.op
        key = (request.slot_idx, request.generation)
        try:
            if op == "arrow_release":
                if request.arrow_shm_name:
                    self._release_arrow_buffer(request.arrow_shm_name, key=key)
                with self._lock:
                    in_tx = self._tx_states.get(key, request.in_tx)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=in_tx,
                )

            if op == "acquire":
                slot_idx, generation = self._acquire_slot_sync(request)
                lease_key = (slot_idx, generation)
                try:
                    self._ensure_session_sync(lease_key, request.pool_name)
                except BaseException:
                    with self._lock:
                        lease = self._broker_leases.pop(lease_key, None)
                    if lease is not None:
                        self._release_broker_lease(lease, lease_key, in_tx=False)
                    raise
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=slot_idx,
                    generation=generation,
                    in_tx=False,
                )

            if op == "release":
                self._release_session_sync(key, request.in_tx)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "commit":
                session = self.get_leased_session(key[0], key[1])
                if session is None:
                    raise RuntimeError(f"Cannot commit without an active leased session for slot {key}")
                self._exec_commit_sync(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "rollback":
                self._close_lease_streams_sync(key)
                session = self.get_leased_session(key[0], key[1])
                if session is None:
                    with self._lock:
                        self._tx_states[key] = False
                    return BrokerRpcResponse(
                        request_id=request.request_id,
                        ok=True,
                        slot_idx=request.slot_idx,
                        generation=request.generation,
                        in_tx=False,
                    )
                self._exec_rollback_sync(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "stream_close":
                self._close_stream_by_id_sync(key, request.stream_id)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=self._tx_state_for(key, request.in_tx),
                    stream_id=request.stream_id,
                    has_more=False,
                )

            if op == "stream_fetch":
                rows, columns, has_more = self._exec_stream_fetch_sync(key, request.stream_id, request.chunk_rows)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=rows,
                    columns=columns,
                    rowcount=len(rows),
                    in_tx=self._tx_state_for(key, request.in_tx),
                    stream_id=request.stream_id,
                    has_more=has_more,
                )

            if op == "stream_open" and request.chunk_rows <= 0:
                msg = "chunk_rows must be greater than 0"
                raise ValueError(msg)

            session = self._ensure_session_sync(key, request.pool_name)

            if op == "begin":
                self._exec_begin_sync(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=True,
                )

            if op == "execute":
                raw_res = self._invoke_sync(session, ("execute",), request.sql, request.params)
                rows, columns, rowcount = self._extract_result_sync(raw_res)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=rows,
                    columns=columns,
                    rowcount=rowcount,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "execute_many":
                rowcount = self._exec_many_sync(session, request.sql, request.many_params)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rowcount=rowcount,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "select":
                raw_res = self._invoke_sync(session, ("select", "fetch", "execute"), request.sql, request.params)
                rows, columns, _ = self._extract_result_sync(raw_res)
                return self._build_select_response(request, key, rows, columns)

            if op == "select_one":
                row, columns = self._exec_select_one_sync(session, request.sql, request.params, allow_none=False)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=[row] if row is not None else [],
                    columns=columns,
                    rowcount=1 if row is not None else 0,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "select_one_or_none":
                row, columns = self._exec_select_one_sync(session, request.sql, request.params, allow_none=True)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=[row] if row is not None else [],
                    columns=columns,
                    rowcount=1 if row is not None else 0,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op in {"select_value", "select_value_or_none"}:
                allow_none = op == "select_value_or_none"
                scalar = self._exec_select_value_sync(session, request.sql, request.params, allow_none=allow_none)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    scalar=scalar,
                    rowcount=1 if scalar is not None else 0,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "select_to_arrow":
                return self._exec_select_to_arrow_sync(session, request, key)

            if op == "stream_open":
                stream_id, rows, columns, has_more = self._exec_stream_open_sync(
                    session, key, request.sql, request.params, request.chunk_rows
                )
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=rows,
                    columns=columns,
                    rowcount=len(rows),
                    in_tx=self._tx_state_for(key, request.in_tx),
                    stream_id=stream_id,
                    has_more=has_more,
                )

            msg = f"Unsupported BrokerRpcRequest operation: {op!r}"
            raise VertebraError(msg)
        except Exception as exc:
            with self._lock:
                in_tx = self._tx_states.get(key, request.in_tx)
            return BrokerRpcResponse(
                request_id=request.request_id,
                ok=False,
                slot_idx=request.slot_idx,
                generation=request.generation,
                in_tx=in_tx,
                error_type=type(exc).__name__,
                error_message=self._format_error_message(exc),
            )

    async def _handle_request_async(self, request: BrokerRpcRequest) -> BrokerRpcResponse:
        """Execute an asynchronous BrokerRpcRequest and return a BrokerRpcResponse."""
        op = request.op
        key = (request.slot_idx, request.generation)
        try:
            if op == "arrow_release":
                if request.arrow_shm_name:
                    await SyncPool.default().submit_async(self._release_arrow_buffer, request.arrow_shm_name, key)
                with self._lock:
                    in_tx = self._tx_states.get(key, request.in_tx)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=in_tx,
                )

            if op == "acquire":
                slot_idx, generation = await self._acquire_slot_async(request)
                lease_key = (slot_idx, generation)
                try:
                    await self._ensure_session_async(lease_key, request.pool_name)
                except BaseException:
                    with self._lock:
                        lease = self._broker_leases.pop(lease_key, None)
                    if lease is not None:
                        await self._release_broker_lease_async(lease, lease_key, in_tx=False)
                    raise
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=slot_idx,
                    generation=generation,
                    in_tx=False,
                )

            if op == "release":
                await self._release_session_async(key, request.in_tx)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "commit":
                session = self.get_leased_session(key[0], key[1])
                if session is None:
                    raise RuntimeError(f"Cannot commit without an active leased session for slot {key}")
                await self._exec_commit_async(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "rollback":
                await self._close_lease_streams_async(key)
                session = self.get_leased_session(key[0], key[1])
                if session is None:
                    with self._lock:
                        self._tx_states[key] = False
                    return BrokerRpcResponse(
                        request_id=request.request_id,
                        ok=True,
                        slot_idx=request.slot_idx,
                        generation=request.generation,
                        in_tx=False,
                    )
                await self._exec_rollback_async(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "stream_close":
                await self._close_stream_by_id_async(key, request.stream_id)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=self._tx_state_for(key, request.in_tx),
                    stream_id=request.stream_id,
                    has_more=False,
                )

            if op == "stream_fetch":
                rows, columns, has_more = await self._exec_stream_fetch_async(
                    key, request.stream_id, request.chunk_rows
                )
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=rows,
                    columns=columns,
                    rowcount=len(rows),
                    in_tx=self._tx_state_for(key, request.in_tx),
                    stream_id=request.stream_id,
                    has_more=has_more,
                )

            if op == "stream_open" and request.chunk_rows <= 0:
                msg = "chunk_rows must be greater than 0"
                raise ValueError(msg)

            session = await self._ensure_session_async(key, request.pool_name)

            if op == "begin":
                await self._exec_begin_async(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=True,
                )

            if op == "execute":
                raw_res = await self._invoke_async(session, ("execute",), request.sql, request.params)
                rows, columns, rowcount = await self._extract_result_async(raw_res)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=rows,
                    columns=columns,
                    rowcount=rowcount,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "execute_many":
                rowcount = await self._exec_many_async(session, request.sql, request.many_params)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rowcount=rowcount,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "select":
                raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), request.sql, request.params)
                rows, columns, _ = await self._extract_result_async(raw_res)
                return await self._build_select_response_async(request, key, rows, columns)

            if op == "select_one":
                row, columns = await self._exec_select_one_async(session, request.sql, request.params, allow_none=False)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=[row] if row is not None else [],
                    columns=columns,
                    rowcount=1 if row is not None else 0,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "select_one_or_none":
                row, columns = await self._exec_select_one_async(session, request.sql, request.params, allow_none=True)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=[row] if row is not None else [],
                    columns=columns,
                    rowcount=1 if row is not None else 0,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op in {"select_value", "select_value_or_none"}:
                allow_none = op == "select_value_or_none"
                scalar = await self._exec_select_value_async(
                    session, request.sql, request.params, allow_none=allow_none
                )
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    scalar=scalar,
                    rowcount=1 if scalar is not None else 0,
                    in_tx=self._tx_state_for(key, request.in_tx),
                )

            if op == "select_to_arrow":
                return await self._exec_select_to_arrow_async(session, request, key)

            if op == "stream_open":
                stream_id, rows, columns, has_more = await self._exec_stream_open_async(
                    session, key, request.sql, request.params, request.chunk_rows
                )
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rows=rows,
                    columns=columns,
                    rowcount=len(rows),
                    in_tx=self._tx_state_for(key, request.in_tx),
                    stream_id=stream_id,
                    has_more=has_more,
                )

            msg = f"Unsupported BrokerRpcRequest operation: {op!r}"
            raise VertebraError(msg)
        except Exception as exc:
            with self._lock:
                in_tx = self._tx_states.get(key, request.in_tx)
            return BrokerRpcResponse(
                request_id=request.request_id,
                ok=False,
                slot_idx=request.slot_idx,
                generation=request.generation,
                in_tx=in_tx,
                error_type=type(exc).__name__,
                error_message=self._format_error_message(exc),
            )

    def _handle_raw_frame_sync(self, payload: bytes | bytearray | memoryview) -> bytes:
        """Decode a serialized RPC request frame (MessagePack or JSON), execute it, and encode the response."""
        raw_bytes = bytes(payload)
        is_json = raw_bytes.lstrip().startswith(b"{")
        if is_json:
            decoded_dict = decode_json(raw_bytes)
            req = msgspec.convert(decoded_dict, type=BrokerRpcRequest)
            resp = self._handle_request_sync(req)
            return encode_json_bytes(resp)
        req = decode_msgpack(raw_bytes, type=BrokerRpcRequest)
        resp = self._handle_request_sync(req)
        return encode_msgpack_bytes(resp)

    async def _handle_raw_frame_async(self, payload: bytes | bytearray | memoryview) -> bytes:
        """Decode a serialized RPC request frame asynchronously, execute it, and encode the response."""
        raw_bytes = bytes(payload)
        is_json = raw_bytes.lstrip().startswith(b"{")
        if is_json:
            decoded_dict = decode_json(raw_bytes)
            req = msgspec.convert(decoded_dict, type=BrokerRpcRequest)
            resp = await self._handle_request_async(req)
            return encode_json_bytes(resp)
        req = decode_msgpack(raw_bytes, type=BrokerRpcRequest)
        resp = await self._handle_request_async(req)
        return encode_msgpack_bytes(resp)

    def _close_sync(self) -> None:
        """Release all leased sessions, close open streams, and unlink remaining Arrow buffers."""
        with self._lock:
            keys = list(self._leased_sessions.keys())
            arrow_items = list(self._active_arrow_buffers.items())
            self._active_arrow_buffers.clear()
            self._lease_arrow_buffers.clear()
            remaining_streams = list(self._active_streams.values())
            self._active_streams.clear()
            self._lease_streams.clear()
        for stream in remaining_streams:
            self._close_stream_handle_sync(stream)
        for key in keys:
            self._release_session_sync(key, in_tx=self._tx_states.get(key, False))
        for name, cursor in arrow_items:
            _unlink_arrow_shm(name, cursor)

    async def _close_async(self) -> None:
        """Asynchronously release all leased sessions, close open streams, and unlink Arrow buffers."""
        with self._lock:
            keys = list(self._leased_sessions.keys())
            arrow_items = list(self._active_arrow_buffers.items())
            self._active_arrow_buffers.clear()
            self._lease_arrow_buffers.clear()
            remaining_streams = list(self._active_streams.values())
            self._active_streams.clear()
            self._lease_streams.clear()
        for stream in remaining_streams:
            await self._close_stream_handle_async(stream)
        for key in keys:
            await self._release_session_async(key, in_tx=self._tx_state_for(key, False))
        for name, cursor in arrow_items:
            await SyncPool.default().submit_async(_unlink_arrow_shm, name, cursor)

    def _resolve_upstream(self, pool_name: str) -> Any:
        """Resolve the upstream config, driver, or factory for pool_name."""
        upstream = self.resolve_upstream_optional(pool_name)
        if upstream is not None:
            return upstream
        raise ImproperConfigurationError(f"No upstream SQLSpec config or driver registered for pool {pool_name!r}")

    def _acquire_slot_sync(self, request: BrokerRpcRequest) -> tuple[int, int]:
        """Acquire a (slot_idx, generation) pair synchronously from broker or internal counter."""
        if request.generation > 0:
            return request.slot_idx, request.generation
        if self.broker is not None:
            lease = self._acquire_from_broker_sync(request.pool_name, request.worker_pid)
            slot_idx, generation = self._extract_lease_coords(lease)
            with self._lock:
                self._broker_leases[(slot_idx, generation)] = lease
            return slot_idx, generation
        with self._lock:
            gen = self._next_generation
            self._next_generation += 1
            return request.slot_idx, gen

    async def _acquire_slot_async(self, request: BrokerRpcRequest) -> tuple[int, int]:
        """Acquire a (slot_idx, generation) pair asynchronously from broker or internal counter."""
        if request.generation > 0:
            return request.slot_idx, request.generation
        if self.broker is not None:
            lease = await self._acquire_from_broker_async(request.pool_name, request.worker_pid)
            slot_idx, generation = self._extract_lease_coords(lease)
            with self._lock:
                self._broker_leases[(slot_idx, generation)] = lease
            return slot_idx, generation
        with self._lock:
            gen = self._next_generation
            self._next_generation += 1
            return request.slot_idx, gen

    @staticmethod
    async def _invoke_async_or_offload(fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Invoke a coroutine callable directly or offload a synchronous callable onto SyncPool."""
        if is_coroutine_callable(fn):
            return await fn(*args, **kwargs)
        res = await SyncPool.default().submit_async(fn, *args, **kwargs)
        return await res if isinstance(res, Awaitable) else res

    def _acquire_from_broker_sync(self, pool_name: str, worker_pid: int) -> Any:
        """Acquire a lease from self.broker synchronously via typed lease source contracts."""
        pid = worker_pid if worker_pid > 0 else os.getpid()
        target = getattr(self.broker, "transport", self.broker)
        if isinstance(target, SyncSHMBrokerTransport):
            return target.acquire(bind_key=pool_name, worker_id=pid)
        if isinstance(target, AsyncSHMBrokerTransport):
            raise InterfaceError(
                "AsyncSHMBrokerTransport cannot acquire leases synchronously; use SyncSHMBrokerTransport"
            )
        acquire_lease_fn = getattr(self.broker, "acquire_lease", None)
        if callable(acquire_lease_fn):
            return acquire_lease_fn(pool_name=pool_name, worker_pid=pid)
        if isinstance(target, SyncBrokerLeaseSource):
            return target.acquire(bind_key=pool_name, worker_id=pid)
        raise ImproperConfigurationError("Configured broker does not support synchronous lease acquisition")

    async def _acquire_from_broker_async(self, pool_name: str, worker_pid: int) -> Any:
        """Acquire a lease from self.broker asynchronously via typed lease source contracts."""
        pid = worker_pid if worker_pid > 0 else os.getpid()
        target = getattr(self.broker, "transport", self.broker)
        if isinstance(target, AsyncSHMBrokerTransport):
            return await target.acquire(bind_key=pool_name, worker_id=pid)
        if isinstance(target, SyncSHMBrokerTransport):
            return await SyncPool.default().submit_async(target.acquire, bind_key=pool_name, worker_id=pid)
        candidates: tuple[tuple[Any, dict[str, Any]], ...] = (
            (getattr(self.broker, "acquire_lease", None), {"pool_name": pool_name, "worker_pid": pid}),
            (getattr(target, "acquire", None), {"bind_key": pool_name, "worker_id": pid}),
        )
        for fn, call_kwargs in candidates:
            if callable(fn):
                return await self._invoke_async_or_offload(fn, **call_kwargs)
        raise ImproperConfigurationError("Configured broker does not support asynchronous lease acquisition")

    def _extract_lease_coords(self, lease: Any) -> tuple[int, int]:
        """Extract (slot_idx, generation) from a broker lease object or tuple."""
        if isinstance(lease, tuple) and len(lease) >= 2:
            return int(lease[0]), int(lease[1])
        slot_idx = int(getattr(lease, "slot_idx", getattr(lease, "slot_index", 0)))
        generation = int(getattr(lease, "generation", 0))
        if generation <= 0:
            with self._lock:
                generation = self._next_generation
                self._next_generation += 1
        return slot_idx, generation

    def _ensure_session_sync(self, key: tuple[int, int], pool_name: str) -> Any:
        """Return the existing leased session for key or checkout a new session synchronously."""
        with self._lock:
            existing = self._leased_sessions.get(key)
            if existing is not None:
                return existing

        upstream = self._resolve_upstream(pool_name)
        candidate = (
            upstream.provide_session()
            if hasattr(upstream, "provide_session")
            else (upstream() if callable(upstream) else upstream)
        )
        ctx_mgr = None
        enter_fn = getattr(candidate, "__enter__", None)
        if callable(enter_fn) and candidate is not upstream:
            ctx_mgr = candidate
            session = enter_fn()
        else:
            session = candidate

        with self._lock:
            self._leased_sessions[key] = session
            if ctx_mgr is not None:
                self._leased_contexts[key] = ctx_mgr
            self._tx_states.setdefault(key, False)
        return session

    async def _ensure_session_async(self, key: tuple[int, int], pool_name: str) -> Any:
        """Return the existing leased session for key or checkout a new session asynchronously."""
        with self._lock:
            existing = self._leased_sessions.get(key)
            if existing is not None:
                return existing

        upstream = self._resolve_upstream(pool_name)
        if hasattr(upstream, "provide_session"):
            candidate = upstream.provide_session()
        elif callable(upstream):
            candidate = upstream()
        else:
            candidate = upstream

        if isinstance(candidate, Awaitable):
            candidate = await candidate

        ctx_mgr = None
        aenter_fn = getattr(candidate, "__aenter__", None)
        enter_fn = getattr(candidate, "__enter__", None)
        if callable(aenter_fn) and candidate is not upstream:
            ctx_mgr = candidate
            session = await cast("Awaitable[Any]", aenter_fn())
        elif callable(enter_fn) and candidate is not upstream:
            ctx_mgr = candidate
            session = await SyncPool.default().submit_async(cast("Callable[[], Any]", enter_fn))
        else:
            session = candidate

        with self._lock:
            self._leased_sessions[key] = session
            if ctx_mgr is not None:
                self._leased_contexts[key] = ctx_mgr
            self._tx_states.setdefault(key, False)
        return session

    def _quarantine_broker_lease(self, lease: Any, key: tuple[int, int]) -> None:
        """Quarantine a lease or coordinate pair on self.broker without releasing it."""
        if self.broker is None:
            return
        target = getattr(self.broker, "transport", self.broker)
        lease_arg = lease if lease is not None else key
        if isinstance(target, AsyncSHMBrokerTransport):
            raise InterfaceError(
                "AsyncSHMBrokerTransport cannot quarantine leases synchronously; use SyncSHMBrokerTransport"
            )
        quarantine_fn = getattr(self.broker, "quarantine", None) or getattr(target, "quarantine", None)
        if callable(quarantine_fn) and not is_coroutine_callable(quarantine_fn):
            quarantine_fn(lease_arg)

    async def _quarantine_broker_lease_async(self, lease: Any, key: tuple[int, int]) -> None:
        """Quarantine a lease or coordinate pair asynchronously without releasing it."""
        if self.broker is None:
            return
        target = getattr(self.broker, "transport", self.broker)
        lease_arg = lease if lease is not None else key
        quarantine_fn = getattr(self.broker, "quarantine", None) or getattr(target, "quarantine", None)
        if callable(quarantine_fn):
            await self._invoke_async_or_offload(quarantine_fn, lease_arg)

    def _release_arrow_buffer(self, shm_name: str, key: tuple[int, int] | None = None) -> None:
        """Release and unlink an active VTBARROW SHM cursor tracked by this provider."""
        if not shm_name:
            return
        with self._lock:
            cursor = self._active_arrow_buffers.pop(shm_name, None)
            if key is not None:
                lease_names = self._lease_arrow_buffers.get(key)
                if lease_names is not None:
                    lease_names.discard(shm_name)
                    if not lease_names:
                        self._lease_arrow_buffers.pop(key, None)
            else:
                empty_keys: list[tuple[int, int]] = []
                for lease_key, lease_names in self._lease_arrow_buffers.items():
                    lease_names.discard(shm_name)
                    if not lease_names:
                        empty_keys.append(lease_key)
                for empty_key in empty_keys:
                    self._lease_arrow_buffers.pop(empty_key, None)
        _unlink_arrow_shm(shm_name, cursor)

    def _pop_lease_arrow_items(self, key: tuple[int, int]) -> list[tuple[str, ArrowCursor | None]]:
        """Detach and return all VTBARROW SHM buffers associated with a lease key."""
        with self._lock:
            names = self._lease_arrow_buffers.pop(key, None)
            if not names:
                return []
            return [(name, self._active_arrow_buffers.pop(name, None)) for name in names]

    def _release_lease_arrow_buffers_sync(self, key: tuple[int, int]) -> None:
        """Synchronously release and unlink every VTBARROW SHM segment tied to a lease."""
        for shm_name, cursor in self._pop_lease_arrow_items(key):
            _unlink_arrow_shm(shm_name, cursor)

    async def _release_lease_arrow_buffers_async(self, key: tuple[int, int]) -> None:
        """Asynchronously release and unlink every VTBARROW SHM segment tied to a lease."""
        items = self._pop_lease_arrow_items(key)
        for shm_name, cursor in items:
            await SyncPool.default().submit_async(_unlink_arrow_shm, shm_name, cursor)

    def _release_session_sync(self, key: tuple[int, int], in_tx: bool) -> None:
        """Release a pinned session synchronously, quarantining on rollback or context-exit error."""
        self._close_lease_streams_sync(key)
        self._release_lease_arrow_buffers_sync(key)
        with self._lock:
            session = self._leased_sessions.pop(key, None)
            ctx_mgr = self._leased_contexts.pop(key, None)
            was_in_tx = self._tx_states.pop(key, False) or in_tx
            lease = self._broker_leases.pop(key, None)

        cleanup_exc: BaseException | None = None
        try:
            if session is not None and was_in_tx:
                self._exec_rollback_sync(session, key)
        except BaseException as exc:
            cleanup_exc = exc
        finally:
            if ctx_mgr is not None and hasattr(ctx_mgr, "__exit__"):
                try:
                    ctx_mgr.__exit__(None, None, None)
                except BaseException as exit_exc:
                    if cleanup_exc is None:
                        cleanup_exc = exit_exc

        if cleanup_exc is not None:
            try:
                self._quarantine_broker_lease(lease, key)
            except BaseException as quarantine_exc:
                if isinstance(cleanup_exc, Exception) and isinstance(quarantine_exc, Exception):
                    msg = (
                        f"quarantine failed ({type(quarantine_exc).__name__}: {quarantine_exc}) "
                        f"after cleanup error: {cleanup_exc}"
                    )
                    raise RuntimeError(msg) from cleanup_exc
                raise
            if isinstance(cleanup_exc, Exception) and not isinstance(cleanup_exc, LeaseQuarantinedError):
                raise LeaseQuarantinedError(key[0], key[1], str(cleanup_exc)) from cleanup_exc
            raise cleanup_exc

        if lease is not None:
            self._release_broker_lease(lease, key, in_tx=False)

    async def _release_session_async(self, key: tuple[int, int], in_tx: bool) -> None:
        """Release a pinned session asynchronously, quarantining on rollback or context-exit error."""
        await self._close_lease_streams_async(key)
        await self._release_lease_arrow_buffers_async(key)
        with self._lock:
            session = self._leased_sessions.pop(key, None)
            ctx_mgr = self._leased_contexts.pop(key, None)
            was_in_tx = self._tx_states.pop(key, False) or in_tx
            lease = self._broker_leases.pop(key, None)

        cleanup_exc: BaseException | None = None
        try:
            if session is not None and was_in_tx:
                await self._exec_rollback_async(session, key)
        except BaseException as exc:
            cleanup_exc = exc
        finally:
            if ctx_mgr is not None:
                try:
                    if hasattr(ctx_mgr, "__aexit__"):
                        await ctx_mgr.__aexit__(None, None, None)
                    elif hasattr(ctx_mgr, "__exit__"):
                        await SyncPool.default().submit_async(ctx_mgr.__exit__, None, None, None)
                except BaseException as exit_exc:
                    if cleanup_exc is None:
                        cleanup_exc = exit_exc

        if cleanup_exc is not None:
            try:
                await self._quarantine_broker_lease_async(lease, key)
            except BaseException as quarantine_exc:
                if isinstance(cleanup_exc, Exception) and isinstance(quarantine_exc, Exception):
                    msg = (
                        f"quarantine failed ({type(quarantine_exc).__name__}: {quarantine_exc}) "
                        f"after cleanup error: {cleanup_exc}"
                    )
                    raise RuntimeError(msg) from cleanup_exc
                raise
            if isinstance(cleanup_exc, Exception) and not isinstance(cleanup_exc, LeaseQuarantinedError):
                raise LeaseQuarantinedError(key[0], key[1], str(cleanup_exc)) from cleanup_exc
            raise cleanup_exc

        if lease is not None:
            await self._release_broker_lease_async(lease, key, in_tx=False)

    def _release_broker_lease(self, lease: Any, key: tuple[int, int], *, in_tx: bool = False) -> None:
        """Release a broker lease back to self.broker."""
        lease_release_sync = getattr(lease, "release_sync", None)
        if callable(lease_release_sync):
            lease_release_sync()
            return
        if hasattr(lease, "release") and callable(lease.release) and not is_coroutine_callable(lease.release):
            lease.release()
            return
        if self.broker is None:
            return
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            release_lease_fn(key[0], key[1], in_tx=in_tx)
            return
        target = getattr(self.broker, "transport", self.broker)
        if isinstance(target, AsyncSHMBrokerTransport):
            raise InterfaceError(
                "AsyncSHMBrokerTransport cannot release leases synchronously without a lease.release_sync() handle"
            )
        release_fn = getattr(target, "release", None)
        if callable(release_fn) and not is_coroutine_callable(release_fn):
            release_fn(f"{key[0]}:{key[1]}")

    async def _release_broker_lease_async(self, lease: Any, key: tuple[int, int], *, in_tx: bool = False) -> None:
        """Release a broker lease back to self.broker asynchronously."""
        if hasattr(lease, "release") and callable(lease.release):
            await self._invoke_async_or_offload(lease.release)
            return
        if self.broker is None:
            return
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            await self._invoke_async_or_offload(release_lease_fn, key[0], key[1], in_tx=in_tx)
            return
        target = getattr(self.broker, "transport", self.broker)
        release_fn = getattr(target, "release", None)
        if callable(release_fn):
            await self._invoke_async_or_offload(release_fn, f"{key[0]}:{key[1]}")

    def _exec_begin_sync(self, session: Any, key: tuple[int, int]) -> None:
        """Begin a transaction on session synchronously and mark key as in-transaction."""
        begin_fn = getattr(session, "begin", None)
        if callable(begin_fn):
            begin_fn()
        elif hasattr(session, "execute") and not getattr(session, "in_transaction", False):
            session.execute("BEGIN")
        with self._lock:
            self._tx_states[key] = True

    async def _exec_begin_async(self, session: Any, key: tuple[int, int]) -> None:
        """Begin a transaction on session asynchronously and mark key as in-transaction."""
        begin_fn = getattr(session, "begin", None)
        if callable(begin_fn):
            res = begin_fn()
            if isinstance(res, Awaitable):
                await res
        elif hasattr(session, "execute") and not getattr(session, "in_transaction", False):
            res = session.execute("BEGIN")
            if isinstance(res, Awaitable):
                await res
        with self._lock:
            self._tx_states[key] = True

    def _exec_commit_sync(self, session: Any, key: tuple[int, int]) -> None:
        """Commit an active transaction on session synchronously and clear transaction state."""
        commit_fn = getattr(session, "commit", None)
        if callable(commit_fn):
            commit_fn()
        elif hasattr(session, "execute"):
            session.execute("COMMIT")
        with self._lock:
            if key in self._leased_sessions:
                self._tx_states[key] = False

    async def _exec_commit_async(self, session: Any, key: tuple[int, int]) -> None:
        """Commit an active transaction on session asynchronously and clear transaction state."""
        commit_fn = getattr(session, "commit", None)
        if callable(commit_fn):
            res = commit_fn()
            if isinstance(res, Awaitable):
                await res
        elif hasattr(session, "execute"):
            res = session.execute("COMMIT")
            if isinstance(res, Awaitable):
                await res
        with self._lock:
            if key in self._leased_sessions:
                self._tx_states[key] = False

    def _exec_rollback_sync(self, session: Any, key: tuple[int, int]) -> None:
        """Roll back an active transaction on session synchronously and clear transaction state."""
        self._close_lease_streams_sync(key)
        rollback_fn = getattr(session, "rollback", None)
        if callable(rollback_fn):
            rollback_fn()
        elif hasattr(session, "execute"):
            session.execute("ROLLBACK")
        with self._lock:
            if key in self._leased_sessions:
                self._tx_states[key] = False

    async def _exec_rollback_async(self, session: Any, key: tuple[int, int]) -> None:
        """Roll back an active transaction on session asynchronously and clear transaction state."""
        await self._close_lease_streams_async(key)
        rollback_fn = getattr(session, "rollback", None)
        if callable(rollback_fn):
            res = rollback_fn()
            if isinstance(res, Awaitable):
                await res
        elif hasattr(session, "execute"):
            res = session.execute("ROLLBACK")
            if isinstance(res, Awaitable):
                await res
        with self._lock:
            if key in self._leased_sessions:
                self._tx_states[key] = False

    def _exec_many_sync(
        self,
        session: Any,
        sql: str,
        many_params: Sequence[tuple[Any, ...] | list[Any] | dict[str, Any]],
    ) -> int:
        """Execute a batch DML operation synchronously and return the affected row count."""
        for attr in ("execute_many", "executemany"):
            fn = getattr(session, attr, None)
            if callable(fn):
                res = fn(sql, list(many_params))
                if isinstance(res, int):
                    return res
                rowcount = getattr(res, "rowcount", getattr(res, "rows_affected", None))
                if isinstance(rowcount, int) and rowcount >= 0:
                    return rowcount
                return len(many_params)
        total = 0
        for item in many_params:
            res = self._invoke_sync(session, ("execute",), sql, item)
            _, _, rc = self._extract_result_sync(res)
            total += rc if rc > 0 else 1
        return total

    async def _exec_many_async(
        self,
        session: Any,
        sql: str,
        many_params: Sequence[tuple[Any, ...] | list[Any] | dict[str, Any]],
    ) -> int:
        """Execute a batch DML operation asynchronously and return the affected row count."""
        for attr in ("execute_many", "executemany"):
            fn = getattr(session, attr, None)
            if callable(fn):
                res = fn(sql, list(many_params))
                if isinstance(res, Awaitable):
                    res = await res
                if isinstance(res, int):
                    return res
                rowcount = getattr(res, "rowcount", getattr(res, "rows_affected", None))
                if isinstance(rowcount, int) and rowcount >= 0:
                    return rowcount
                return len(many_params)
        total = 0
        for item in many_params:
            res = await self._invoke_async(session, ("execute",), sql, item)
            _, _, rc = await self._extract_result_async(res)
            total += rc if rc > 0 else 1
        return total

    def _exec_select_one_sync(
        self,
        session: Any,
        sql: str,
        params: Any,
        *,
        allow_none: bool,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        """Execute select_one or select_one_or_none synchronously."""
        method_name = "select_one_or_none" if allow_none else "select_one"
        direct_fn = getattr(session, method_name, None)
        if callable(direct_fn):
            res = self._dispatch_sql(session, direct_fn, sql, params)
            if res is None:
                if not allow_none:
                    msg = "No row found for select_one"
                    raise ValueError(msg)
                return None, []
            row_dict = self._coerce_single_row(res)
            return row_dict, list(row_dict.keys())

        raw_res = self._invoke_sync(session, ("select", "fetch", "execute"), sql, params)
        rows, columns, _ = self._extract_result_sync(raw_res)
        if not rows:
            if not allow_none:
                msg = "No row found for select_one"
                raise ValueError(msg)
            return None, columns
        if len(rows) > 1:
            msg = f"Expected at most 1 row, got {len(rows)}"
            raise ValueError(msg)
        row_dict = self._coerce_single_row(rows[0], columns)
        return row_dict, columns or list(row_dict.keys())

    async def _exec_select_one_async(
        self,
        session: Any,
        sql: str,
        params: Any,
        *,
        allow_none: bool,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        """Execute select_one or select_one_or_none asynchronously."""
        method_name = "select_one_or_none" if allow_none else "select_one"
        direct_fn = getattr(session, method_name, None)
        if callable(direct_fn):
            res = self._dispatch_sql(session, direct_fn, sql, params)
            if isinstance(res, Awaitable):
                res = await res
            if res is None:
                if not allow_none:
                    msg = "No row found for select_one"
                    raise ValueError(msg)
                return None, []
            row_dict = self._coerce_single_row(res)
            return row_dict, list(row_dict.keys())

        raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), sql, params)
        rows, columns, _ = await self._extract_result_async(raw_res)
        if not rows:
            if not allow_none:
                msg = "No row found for select_one"
                raise ValueError(msg)
            return None, columns
        if len(rows) > 1:
            msg = f"Expected at most 1 row, got {len(rows)}"
            raise ValueError(msg)
        row_dict = self._coerce_single_row(rows[0], columns)
        return row_dict, columns or list(row_dict.keys())

    def _exec_select_value_sync(
        self,
        session: Any,
        sql: str,
        params: Any,
        *,
        allow_none: bool = False,
    ) -> Any:
        """Execute select_value or select_value_or_none synchronously and return a scalar value."""
        if allow_none:
            direct_or_none = getattr(session, "select_value_or_none", None)
            if callable(direct_or_none):
                return self._dispatch_sql(session, direct_or_none, sql, params)
        else:
            for attr in ("select_value", "fetch_val"):
                fn = getattr(session, attr, None)
                if callable(fn):
                    return self._dispatch_sql(session, fn, sql, params)
        raw_res = self._invoke_sync(session, ("select", "fetch", "execute"), sql, params)
        rows, _, _ = self._extract_result_sync(raw_res)
        if not rows:
            if allow_none:
                return None
            msg = "No row found for select_value"
            raise ValueError(msg)
        if len(rows) > 1:
            msg = f"Expected at most 1 row, got {len(rows)}"
            raise ValueError(msg)
        return self._extract_scalar(rows)

    async def _exec_select_value_async(
        self,
        session: Any,
        sql: str,
        params: Any,
        *,
        allow_none: bool = False,
    ) -> Any:
        """Execute select_value or select_value_or_none asynchronously and return a scalar value."""
        if allow_none:
            direct_or_none = getattr(session, "select_value_or_none", None)
            if callable(direct_or_none):
                res = self._dispatch_sql(session, direct_or_none, sql, params)
                if isinstance(res, Awaitable):
                    return await res
                return res
        else:
            for attr in ("select_value", "fetch_val"):
                fn = getattr(session, attr, None)
                if callable(fn):
                    res = self._dispatch_sql(session, fn, sql, params)
                    if isinstance(res, Awaitable):
                        return await res
                    return res
        raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), sql, params)
        rows, _, _ = await self._extract_result_async(raw_res)
        if not rows:
            if allow_none:
                return None
            msg = "No row found for select_value"
            raise ValueError(msg)
        if len(rows) > 1:
            msg = f"Expected at most 1 row, got {len(rows)}"
            raise ValueError(msg)
        return self._extract_scalar(rows)

    def _exec_select_to_arrow_sync(
        self,
        session: Any,
        request: BrokerRpcRequest,
        key: tuple[int, int],
    ) -> BrokerRpcResponse:
        """Execute select_to_arrow synchronously and spill the result table into a VTBARROW ArrowCursor."""
        arrow_fn = getattr(session, "select_to_arrow", None)
        if callable(arrow_fn):
            raw_arrow = self._dispatch_sql(session, arrow_fn, request.sql, request.params)
            shm_name, byte_len, columns, rowcount = self._spill_arrow_object(raw_arrow, key=key)
        else:
            raw_res = self._invoke_sync(session, ("select", "fetch", "execute"), request.sql, request.params)
            rows, columns, _ = self._extract_result_sync(raw_res)
            cursor = _write_vtbarrow_cursor(rows, columns=columns)
            shm_name, byte_len, tracked_cols, rowcount = self._track_arrow_cursor(cursor, key=key)
            if not columns:
                columns = tracked_cols
        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            columns=columns,
            rowcount=rowcount,
            in_tx=self._tx_state_for(key, request.in_tx),
            is_arrow_shm=True,
            arrow_shm_name=shm_name,
            arrow_byte_len=byte_len,
        )

    async def _exec_select_to_arrow_async(
        self,
        session: Any,
        request: BrokerRpcRequest,
        key: tuple[int, int],
    ) -> BrokerRpcResponse:
        """Execute select_to_arrow asynchronously and spill the result table into a VTBARROW ArrowCursor."""
        arrow_fn = getattr(session, "select_to_arrow", None)
        if callable(arrow_fn):
            raw_arrow = self._dispatch_sql(session, arrow_fn, request.sql, request.params)
            if isinstance(raw_arrow, Awaitable):
                raw_arrow = await raw_arrow
            shm_name, byte_len, columns, rowcount = await SyncPool.default().submit_async(
                self._spill_arrow_object, raw_arrow, key
            )
        else:
            raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), request.sql, request.params)
            rows, columns, _ = await self._extract_result_async(raw_res)
            cursor = await SyncPool.default().submit_async(_write_vtbarrow_cursor, rows, columns=columns)
            shm_name, byte_len, tracked_cols, rowcount = self._track_arrow_cursor(cursor, key=key)
            if not columns:
                columns = tracked_cols
        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            columns=columns,
            rowcount=rowcount,
            in_tx=self._tx_state_for(key, request.in_tx),
            is_arrow_shm=True,
            arrow_shm_name=shm_name,
            arrow_byte_len=byte_len,
        )

    def _track_arrow_cursor(
        self,
        cursor: ArrowCursor,
        key: tuple[int, int] | None = None,
    ) -> tuple[str, int, list[str], int]:
        """Record an active VTBARROW SHM cursor scoped to an optional lease key."""
        shm_name = cursor.shm_name
        cols = list(cursor._columns)
        with self._lock:
            self._active_arrow_buffers[shm_name] = cursor
            if key is not None:
                self._lease_arrow_buffers.setdefault(key, set()).add(shm_name)
        return shm_name, int(cursor.byte_len), cols, int(cursor.row_count)

    def _spill_arrow_object(
        self,
        raw_arrow: Any,
        key: tuple[int, int] | None = None,
    ) -> tuple[str, int, list[str], int]:
        """Write an Arrow Table or row collection into a VTBARROW ArrowCursor and return metadata."""
        cursor = _write_vtbarrow_cursor(raw_arrow)
        return self._track_arrow_cursor(cursor, key=key)

    def _should_spill_select(self, rows: list[dict[str, Any] | list[Any]]) -> bool:
        """Return True when result rows exceed row-count or byte thresholds for SHM Arrow spill."""
        row_count = len(rows)
        if row_count >= self.arrow_threshold_rows:
            return True
        if row_count > 0 and self.arrow_threshold_bytes > 0:
            encoded = encode_msgpack_bytes(rows)
            return len(encoded) >= self.arrow_threshold_bytes
        return False

    def _build_spilled_select_response(
        self,
        request: BrokerRpcRequest,
        in_tx: bool,
        columns: list[str],
        row_count: int,
        cursor: ArrowCursor,
        key: tuple[int, int] | None = None,
    ) -> BrokerRpcResponse:
        """Record an active VTBARROW SHM cursor and return its Arrow SHM BrokerRpcResponse."""
        shm_name, byte_len, tracked_cols, _ = self._track_arrow_cursor(cursor, key=key)
        resolved_cols = list(columns) if columns else tracked_cols
        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            columns=resolved_cols,
            rowcount=row_count,
            in_tx=in_tx,
            is_arrow_shm=True,
            arrow_shm_name=shm_name,
            arrow_byte_len=byte_len,
        )

    def _build_select_response(
        self,
        request: BrokerRpcRequest,
        key: tuple[int, int],
        rows: list[dict[str, Any] | list[Any]],
        columns: list[str],
    ) -> BrokerRpcResponse:
        """Construct a BrokerRpcResponse for select, spilling to a VTBARROW ArrowCursor when thresholds are met."""
        in_tx = self._tx_state_for(key, request.in_tx)
        row_count = len(rows)
        if rows and self._should_spill_select(rows):
            cursor = _write_vtbarrow_cursor(rows, columns=columns)
            return self._build_spilled_select_response(request, in_tx, columns, row_count, cursor, key=key)
        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            rows=rows,
            columns=columns,
            rowcount=row_count,
            in_tx=in_tx,
        )

    async def _build_select_response_async(
        self,
        request: BrokerRpcRequest,
        key: tuple[int, int],
        rows: list[dict[str, Any] | list[Any]],
        columns: list[str],
    ) -> BrokerRpcResponse:
        """Construct a select BrokerRpcResponse inline when small, offloading only VTBARROW Arrow spills."""
        in_tx = self._tx_state_for(key, request.in_tx)
        row_count = len(rows)
        if rows and self._should_spill_select(rows):
            cursor = await SyncPool.default().submit_async(_write_vtbarrow_cursor, rows, columns=columns)
            return self._build_spilled_select_response(request, in_tx, columns, row_count, cursor, key=key)
        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            rows=rows,
            columns=columns,
            rowcount=row_count,
            in_tx=in_tx,
        )

    def _invoke_sync(self, session: Any, method_names: tuple[str, ...], sql: str, params: Any) -> Any:
        """Invoke the first available method in method_names on session synchronously."""
        for name in method_names:
            fn = getattr(session, name, None)
            if callable(fn):
                return self._dispatch_sql(session, fn, sql, params)
        msg = f"Upstream session {type(session).__name__!r} does not implement any of {method_names}"
        raise AttributeError(msg)

    async def _invoke_async(self, session: Any, method_names: tuple[str, ...], sql: str, params: Any) -> Any:
        """Invoke the first available method in method_names on session asynchronously."""
        for name in method_names:
            fn = getattr(session, name, None)
            if callable(fn):
                res = self._dispatch_sql(session, fn, sql, params)
                if isinstance(res, Awaitable):
                    return await res
                return res
        msg = f"Upstream session {type(session).__name__!r} does not implement any of {method_names}"
        raise AttributeError(msg)

    @classmethod
    def _native_sqlite_driver_types(cls) -> tuple[type[Any], ...]:
        """Resolve native SQLite driver classes when available without importing stdlib sqlite3."""
        sqlite_mod = import_optional("vertebra.db.adapters.sqlite")
        if sqlite_mod is None:
            return ()
        return tuple(
            candidate
            for candidate in (
                getattr(sqlite_mod, "SyncSQLiteDriver", None),
                getattr(sqlite_mod, "AsyncSQLiteDriver", None),
            )
            if isinstance(candidate, type)
        )

    @staticmethod
    def _dispatch_sql(session: Any, fn: Any, sql: str, params: Any) -> Any:
        """Dispatch SQL execution across None, empty sequence, Mapping, and Sequence parameter forms."""
        if params is None or params == () or params == []:
            return fn(sql)
        if isinstance(params, Mapping):
            return fn(sql, params)
        if isinstance(params, Sequence) and not isinstance(params, str | bytes | bytearray):
            if isinstance(session, CommonDriverMixin):
                return fn(sql, params)
            sqlite_types = _BaseSQLSpecProvider._native_sqlite_driver_types()
            if sqlite_types and isinstance(session, sqlite_types):
                return fn(sql, params)
            if isinstance(session, (SyncDriverAdapterBase, AsyncDriverAdapterBase)):
                return fn(sql, *params)
            if hasattr(session, "cursor") or hasattr(session, "fetchmany"):
                return fn(sql, params)
            if getattr(type(session), "dialect", None) is not None and not hasattr(session, "execute_many"):
                return fn(sql, params)
            try:
                return fn(sql, *params)
            except TypeError:
                return fn(sql, params)
        return fn(sql, params)

    @classmethod
    def _dispatch_select_stream(
        cls,
        session: Any,
        select_stream_fn: Any,
        sql: str,
        params: Any,
        chunk_rows: int,
    ) -> Any:
        """Dispatch session.select_stream with chunk_size when supported."""

        def _call_with_chunk_size(*call_args: Any) -> Any:
            try:
                return select_stream_fn(*call_args, chunk_size=chunk_rows)
            except TypeError as exc:
                if "chunk_size" in str(exc):
                    return select_stream_fn(*call_args)
                raise

        return cls._dispatch_sql(session, _call_with_chunk_size, sql, params)

    def _allocate_stream_id(self) -> str:
        """Allocate a unique provider stream identifier."""
        with self._lock:
            seq = self._next_stream_seq
            self._next_stream_seq += 1
            return f"stream-{seq}"

    def _register_stream(self, stream: _ProviderStream) -> None:
        """Register an active stream under self._lock scoped to its lease key."""
        with self._lock:
            self._active_streams[stream.stream_id] = stream
            self._lease_streams.setdefault(stream.lease_key, set()).add(stream.stream_id)

    def _get_stream(self, lease_key: tuple[int, int], stream_id: str) -> _ProviderStream:
        """Return an active stream for (lease_key, stream_id) or raise NotFoundError."""
        if not stream_id:
            msg = "Stream identifier is required"
            raise NotFoundError(msg)
        with self._lock:
            stream = self._active_streams.get(stream_id)
            if stream is None or stream.lease_key != lease_key or stream.closed:
                msg = f"Unknown or closed stream {stream_id!r} for slot {lease_key}"
                raise NotFoundError(msg)
            return stream

    def _remove_stream(self, lease_key: tuple[int, int], stream_id: str) -> _ProviderStream | None:
        """Detach and return an active stream for (lease_key, stream_id) if registered."""
        if not stream_id:
            return None
        with self._lock:
            stream = self._active_streams.get(stream_id)
            if stream is None or stream.lease_key != lease_key:
                return None
            self._active_streams.pop(stream_id, None)
            lease_ids = self._lease_streams.get(lease_key)
            if lease_ids is not None:
                lease_ids.discard(stream_id)
                if not lease_ids:
                    self._lease_streams.pop(lease_key, None)
            return stream

    def _pop_lease_streams(self, lease_key: tuple[int, int]) -> list[_ProviderStream]:
        """Detach and return all active streams registered for lease_key."""
        with self._lock:
            stream_ids = self._lease_streams.pop(lease_key, None)
            if not stream_ids:
                return []
            streams: list[_ProviderStream] = []
            for sid in stream_ids:
                stream = self._active_streams.pop(sid, None)
                if stream is not None:
                    streams.append(stream)
            return streams

    @staticmethod
    def _close_stream_handle_sync(stream: _ProviderStream) -> None:
        """Close underlying stream or cursor handles synchronously without raising."""
        stream.closed = True
        for handle in (stream.stream_obj, stream.cursor):
            if handle is None:
                continue
            close_fn = getattr(handle, "close", None)
            if callable(close_fn):
                with contextlib.suppress(Exception):
                    close_fn()

    @staticmethod
    async def _close_stream_handle_async(stream: _ProviderStream) -> None:
        """Close underlying stream or cursor handles asynchronously without raising."""
        stream.closed = True
        for handle in (stream.stream_obj, stream.cursor):
            if handle is None:
                continue
            aclose_fn = getattr(handle, "aclose", None)
            if callable(aclose_fn):
                with contextlib.suppress(Exception):
                    res = aclose_fn()
                    if isinstance(res, Awaitable):
                        await res
                continue
            close_fn = getattr(handle, "close", None)
            if callable(close_fn):
                with contextlib.suppress(Exception):
                    res = close_fn()
                    if isinstance(res, Awaitable):
                        await res

    def _close_stream_by_id_sync(self, lease_key: tuple[int, int], stream_id: str) -> None:
        """Close and remove a specific stream for lease_key synchronously."""
        stream = self._remove_stream(lease_key, stream_id)
        if stream is not None:
            self._close_stream_handle_sync(stream)

    async def _close_stream_by_id_async(self, lease_key: tuple[int, int], stream_id: str) -> None:
        """Close and remove a specific stream for lease_key asynchronously."""
        stream = self._remove_stream(lease_key, stream_id)
        if stream is not None:
            await self._close_stream_handle_async(stream)

    def _close_lease_streams_sync(self, lease_key: tuple[int, int]) -> None:
        """Close and remove every active stream registered for lease_key synchronously."""
        for stream in self._pop_lease_streams(lease_key):
            self._close_stream_handle_sync(stream)

    async def _close_lease_streams_async(self, lease_key: tuple[int, int]) -> None:
        """Close and remove every active stream registered for lease_key asynchronously."""
        for stream in self._pop_lease_streams(lease_key):
            await self._close_stream_handle_async(stream)

    @staticmethod
    def _consume_sync_iterator_chunk(iterator: Any, batch_size: int) -> tuple[list[Any], bool]:
        """Pull up to batch_size items from a synchronous iterator and report exhaustion."""
        raw_rows: list[Any] = []
        exhausted = False
        for _ in range(batch_size):
            try:
                raw_rows.append(next(iterator))
            except StopIteration:
                exhausted = True
                break
        return raw_rows, exhausted

    def _finalize_stream_rows(
        self,
        stream: _ProviderStream,
        raw_rows: list[Any],
        *,
        exhausted: bool,
        batch_size: int,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], bool]:
        """Normalize streamed row objects and compute column names and has_more."""
        if raw_rows:
            rows, columns, _ = self._normalize_collection_result(raw_rows)
            if not stream.columns and columns:
                stream.columns = list(columns)
        else:
            rows = []
            columns = list(stream.columns)
        if not stream.columns:
            col_types = getattr(stream.stream_obj, "column_types", None)
            if isinstance(col_types, Mapping):
                stream.columns = [str(col) for col in col_types]
        resolved_cols = list(stream.columns) if stream.columns else columns
        has_more = (not exhausted) and (len(rows) >= batch_size)
        return rows, resolved_cols, has_more

    def _read_stream_chunk_sync(
        self,
        stream: _ProviderStream,
        batch_size: int,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], bool]:
        """Read the next batch_size chunk from an open synchronous stream."""
        if stream.kind == "buffered":
            start = stream.rows_offset
            end = start + batch_size
            chunk = list(stream.rows_buffer[start:end])
            stream.rows_offset = min(end, len(stream.rows_buffer))
            has_more = stream.rows_offset < len(stream.rows_buffer)
            return chunk, list(stream.columns), has_more

        if stream.kind == "cursor":
            fetched = stream.cursor.fetchmany(batch_size)
            desc = getattr(stream.cursor, "description", None)
            if desc is not None and not stream.columns:
                stream.columns = [str(col[0]) for col in desc]
            rows: list[dict[str, Any] | list[Any]] = [self._coerce_single_row(row, stream.columns) for row in fetched]
            if not stream.columns and rows and isinstance(rows[0], dict):
                stream.columns = list(rows[0].keys())
            has_more = len(rows) >= batch_size
            return rows, list(stream.columns), has_more

        raw_rows, exhausted = self._consume_sync_iterator_chunk(stream.iterator, batch_size)
        return self._finalize_stream_rows(stream, raw_rows, exhausted=exhausted, batch_size=batch_size)

    async def _read_stream_chunk_async(
        self,
        stream: _ProviderStream,
        batch_size: int,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], bool]:
        """Read the next batch_size chunk from an open asynchronous stream."""
        if stream.kind == "buffered":
            start = stream.rows_offset
            end = start + batch_size
            chunk = list(stream.rows_buffer[start:end])
            stream.rows_offset = min(end, len(stream.rows_buffer))
            has_more = stream.rows_offset < len(stream.rows_buffer)
            return chunk, list(stream.columns), has_more

        if stream.kind == "cursor":
            fetched = stream.cursor.fetchmany(batch_size)
            if isinstance(fetched, Awaitable):
                fetched = await fetched
            desc = getattr(stream.cursor, "description", None)
            if desc is not None and not stream.columns:
                stream.columns = [str(col[0]) for col in desc]
            rows: list[dict[str, Any] | list[Any]] = [self._coerce_single_row(row, stream.columns) for row in fetched]
            if not stream.columns and rows and isinstance(rows[0], dict):
                stream.columns = list(rows[0].keys())
            has_more = len(rows) >= batch_size
            return rows, list(stream.columns), has_more

        if hasattr(stream.iterator, "__anext__"):
            raw_rows: list[Any] = []
            exhausted = False
            for _ in range(batch_size):
                try:
                    raw_rows.append(await stream.iterator.__anext__())
                except StopAsyncIteration:
                    exhausted = True
                    break
        else:
            raw_rows, exhausted = await SyncPool.default().submit_async(
                self._consume_sync_iterator_chunk, stream.iterator, batch_size
            )
        return self._finalize_stream_rows(stream, raw_rows, exhausted=exhausted, batch_size=batch_size)

    def _exec_stream_open_sync(
        self,
        session: Any,
        key: tuple[int, int],
        sql: str,
        params: Any,
        chunk_rows: int,
    ) -> tuple[str, list[dict[str, Any] | list[Any]], list[str], bool]:
        """Open a paged synchronous row stream or native cursor and return the first chunk."""
        stream_id = self._allocate_stream_id()
        select_stream_fn = getattr(session, "select_stream", None)
        if callable(select_stream_fn):
            stream_obj = self._dispatch_select_stream(session, select_stream_fn, sql, params, chunk_rows)
            stream = _ProviderStream(
                stream_id=stream_id,
                lease_key=key,
                chunk_rows=chunk_rows,
                kind="stream",
                stream_obj=stream_obj,
                iterator=iter(stream_obj),
            )
            try:
                rows, columns, has_more = self._read_stream_chunk_sync(stream, chunk_rows)
            except BaseException:
                self._close_stream_handle_sync(stream)
                raise
            if has_more:
                self._register_stream(stream)
            else:
                self._close_stream_handle_sync(stream)
            return stream_id, rows, columns, has_more

        cursor_fn = getattr(session, "cursor", None)
        if callable(cursor_fn):
            cur: Any = cursor_fn()
            stream = _ProviderStream(
                stream_id=stream_id,
                lease_key=key,
                chunk_rows=chunk_rows,
                kind="cursor",
                cursor=cur,
            )
            try:
                exec_res = self._dispatch_sql(cur, cur.execute, sql, params)
                if exec_res is not None and hasattr(exec_res, "fetchmany"):
                    stream.cursor = exec_res
                rows, columns, has_more = self._read_stream_chunk_sync(stream, chunk_rows)
            except BaseException:
                self._close_stream_handle_sync(stream)
                raise
            if has_more:
                self._register_stream(stream)
            else:
                self._close_stream_handle_sync(stream)
            return stream_id, rows, columns, has_more

        raw_res = self._invoke_sync(session, ("select", "fetch", "execute"), sql, params)
        if hasattr(raw_res, "fetchmany") and callable(raw_res.fetchmany):
            stream = _ProviderStream(
                stream_id=stream_id,
                lease_key=key,
                chunk_rows=chunk_rows,
                kind="cursor",
                cursor=raw_res,
            )
            try:
                rows, columns, has_more = self._read_stream_chunk_sync(stream, chunk_rows)
            except BaseException:
                self._close_stream_handle_sync(stream)
                raise
            if has_more:
                self._register_stream(stream)
            else:
                self._close_stream_handle_sync(stream)
            return stream_id, rows, columns, has_more

        all_rows, columns, _ = self._extract_result_sync(raw_res)
        stream = _ProviderStream(
            stream_id=stream_id,
            lease_key=key,
            chunk_rows=chunk_rows,
            kind="buffered",
            columns=columns,
            rows_buffer=all_rows,
        )
        rows, resolved_cols, has_more = self._read_stream_chunk_sync(stream, chunk_rows)
        if has_more:
            self._register_stream(stream)
        return stream_id, rows, resolved_cols, has_more

    async def _exec_stream_open_async(
        self,
        session: Any,
        key: tuple[int, int],
        sql: str,
        params: Any,
        chunk_rows: int,
    ) -> tuple[str, list[dict[str, Any] | list[Any]], list[str], bool]:
        """Open a paged asynchronous row stream or native cursor and return the first chunk."""
        stream_id = self._allocate_stream_id()
        select_stream_fn = getattr(session, "select_stream", None)
        if callable(select_stream_fn):
            stream_obj = self._dispatch_select_stream(session, select_stream_fn, sql, params, chunk_rows)
            if isinstance(stream_obj, Awaitable):
                stream_obj = await stream_obj
            iterator = stream_obj.__aiter__() if hasattr(stream_obj, "__aiter__") else iter(stream_obj)
            stream = _ProviderStream(
                stream_id=stream_id,
                lease_key=key,
                chunk_rows=chunk_rows,
                kind="stream",
                stream_obj=stream_obj,
                iterator=iterator,
            )
            try:
                rows, columns, has_more = await self._read_stream_chunk_async(stream, chunk_rows)
            except BaseException:
                await self._close_stream_handle_async(stream)
                raise
            if has_more:
                self._register_stream(stream)
            else:
                await self._close_stream_handle_async(stream)
            return stream_id, rows, columns, has_more

        cursor_fn = getattr(session, "cursor", None)
        if callable(cursor_fn):
            cur: Any = cursor_fn()
            if isinstance(cur, Awaitable):
                cur = await cur
            stream = _ProviderStream(
                stream_id=stream_id,
                lease_key=key,
                chunk_rows=chunk_rows,
                kind="cursor",
                cursor=cur,
            )
            try:
                exec_res = self._dispatch_sql(cur, cur.execute, sql, params)
                if isinstance(exec_res, Awaitable):
                    exec_res = await exec_res
                if exec_res is not None and hasattr(exec_res, "fetchmany"):
                    stream.cursor = exec_res
                rows, columns, has_more = await self._read_stream_chunk_async(stream, chunk_rows)
            except BaseException:
                await self._close_stream_handle_async(stream)
                raise
            if has_more:
                self._register_stream(stream)
            else:
                await self._close_stream_handle_async(stream)
            return stream_id, rows, columns, has_more

        raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), sql, params)
        if hasattr(raw_res, "fetchmany") and callable(raw_res.fetchmany):
            stream = _ProviderStream(
                stream_id=stream_id,
                lease_key=key,
                chunk_rows=chunk_rows,
                kind="cursor",
                cursor=raw_res,
            )
            try:
                rows, columns, has_more = await self._read_stream_chunk_async(stream, chunk_rows)
            except BaseException:
                await self._close_stream_handle_async(stream)
                raise
            if has_more:
                self._register_stream(stream)
            else:
                await self._close_stream_handle_async(stream)
            return stream_id, rows, columns, has_more

        all_rows, columns, _ = await self._extract_result_async(raw_res)
        stream = _ProviderStream(
            stream_id=stream_id,
            lease_key=key,
            chunk_rows=chunk_rows,
            kind="buffered",
            columns=columns,
            rows_buffer=all_rows,
        )
        rows, resolved_cols, has_more = await self._read_stream_chunk_async(stream, chunk_rows)
        if has_more:
            self._register_stream(stream)
        return stream_id, rows, resolved_cols, has_more

    def _exec_stream_fetch_sync(
        self,
        key: tuple[int, int],
        stream_id: str,
        requested_chunk_rows: int,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], bool]:
        """Fetch the next chunk from an active synchronous stream, closing on exhaustion."""
        if requested_chunk_rows < 0:
            msg = "chunk_rows must be greater than 0"
            raise ValueError(msg)
        stream = self._get_stream(key, stream_id)
        batch_size = requested_chunk_rows if requested_chunk_rows > 0 else stream.chunk_rows
        try:
            rows, columns, has_more = self._read_stream_chunk_sync(stream, batch_size)
        except BaseException:
            removed = self._remove_stream(key, stream_id)
            if removed is not None:
                self._close_stream_handle_sync(removed)
            raise
        if not has_more:
            removed = self._remove_stream(key, stream_id)
            if removed is not None:
                self._close_stream_handle_sync(removed)
        return rows, columns, has_more

    async def _exec_stream_fetch_async(
        self,
        key: tuple[int, int],
        stream_id: str,
        requested_chunk_rows: int,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], bool]:
        """Fetch the next chunk from an active asynchronous stream, closing on exhaustion."""
        if requested_chunk_rows < 0:
            msg = "chunk_rows must be greater than 0"
            raise ValueError(msg)
        stream = self._get_stream(key, stream_id)
        batch_size = requested_chunk_rows if requested_chunk_rows > 0 else stream.chunk_rows
        try:
            rows, columns, has_more = await self._read_stream_chunk_async(stream, batch_size)
        except BaseException:
            removed = self._remove_stream(key, stream_id)
            if removed is not None:
                await self._close_stream_handle_async(removed)
            raise
        if not has_more:
            removed = self._remove_stream(key, stream_id)
            if removed is not None:
                await self._close_stream_handle_async(removed)
        return rows, columns, has_more

    def _extract_result_sync(
        self,
        raw_res: Any,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], int]:
        """Extract normalized (rows, columns, rowcount) synchronously from a driver result."""
        if raw_res is None:
            return [], [], 0
        if isinstance(raw_res, int) and not isinstance(raw_res, bool):
            return [], [], raw_res
        fetchall_fn = getattr(raw_res, "fetchall", None)
        if callable(fetchall_fn):
            desc: Any = getattr(raw_res, "description", None)
            if desc is not None:
                columns = [str(col[0]) for col in cast("Iterable[Any]", desc)]
                fetched: Any = fetchall_fn()
                rows: list[dict[str, Any] | list[Any]] = [
                    self._coerce_single_row(row, columns) for row in cast("Iterable[Any]", fetched)
                ]
                return rows, columns, len(rows)
            rc = getattr(raw_res, "rowcount", 0)
            return [], [], max(0, int(rc)) if isinstance(rc, int) else 0
        return self._normalize_collection_result(raw_res)

    async def _extract_result_async(
        self,
        raw_res: Any,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], int]:
        """Extract normalized (rows, columns, rowcount) asynchronously from a driver result."""
        if raw_res is None:
            return [], [], 0
        if isinstance(raw_res, int) and not isinstance(raw_res, bool):
            return [], [], raw_res
        fetchall_fn = getattr(raw_res, "fetchall", None)
        if callable(fetchall_fn):
            desc: Any = getattr(raw_res, "description", None)
            if desc is not None:
                columns = [str(col[0]) for col in cast("Iterable[Any]", desc)]
                fetched: Any = fetchall_fn()
                if isinstance(fetched, Awaitable):
                    fetched = await fetched
                rows: list[dict[str, Any] | list[Any]] = [
                    self._coerce_single_row(row, columns) for row in cast("Iterable[Any]", fetched)
                ]
                return rows, columns, len(rows)
            rc = getattr(raw_res, "rowcount", 0)
            return [], [], max(0, int(rc)) if isinstance(rc, int) else 0
        return self._normalize_collection_result(raw_res)

    def _normalize_collection_result(
        self,
        raw_res: Any,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], int]:
        """Normalize a SQLResult, list of dicts, or list of tuples into (rows, columns, rowcount)."""
        to_dicts_fn = getattr(raw_res, "to_dicts", None)
        if callable(to_dicts_fn):
            items = list(cast("Iterable[Any]", to_dicts_fn()))
        elif hasattr(raw_res, "data") and isinstance(raw_res.data, list):
            items = list(raw_res.data)
        elif hasattr(raw_res, "rows") and isinstance(raw_res.rows, list):
            items = list(raw_res.rows)
        elif isinstance(raw_res, (list, tuple)):
            items = list(raw_res)
        else:
            rc = getattr(raw_res, "rowcount", getattr(raw_res, "rows_affected", 0))
            return [], [], max(0, int(rc)) if isinstance(rc, int) else 0

        raw_cols = getattr(raw_res, "column_names", getattr(raw_res, "columns", None))
        columns: list[str] = [str(c) for c in raw_cols] if isinstance(raw_cols, (list, tuple)) else []
        if not items:
            rc = getattr(raw_res, "rowcount", getattr(raw_res, "rows_affected", 0))
            return [], columns, max(0, int(rc)) if isinstance(rc, int) else 0

        first = items[0]
        if isinstance(first, dict):
            dict_rows: list[dict[str, Any] | list[Any]] = [dict(r) for r in items]
            if not columns and dict_rows and isinstance(dict_rows[0], dict):
                columns = list(dict_rows[0].keys())
            return dict_rows, columns, len(dict_rows)
        if is_msgspec_struct(first):
            struct_rows: list[dict[str, Any] | list[Any]] = [msgspec.structs.asdict(r) for r in items]
            if not columns and struct_rows and isinstance(struct_rows[0], dict):
                columns = list(struct_rows[0].keys())
            return struct_rows, columns, len(struct_rows)
        if isinstance(first, (tuple, list)):
            if not columns:
                columns = [f"c{idx}" for idx in range(len(first))]
            mapped_rows: list[dict[str, Any] | list[Any]] = [dict(zip(columns, r, strict=False)) for r in items]
            return mapped_rows, columns, len(mapped_rows)
        if hasattr(first, "keys"):
            keyed_rows: list[dict[str, Any] | list[Any]] = [dict(r) for r in items]
            if not columns and keyed_rows and isinstance(keyed_rows[0], dict):
                columns = list(keyed_rows[0].keys())
            return keyed_rows, columns, len(keyed_rows)
        scalar_rows: list[dict[str, Any] | list[Any]] = [{"value": r} for r in items]
        return scalar_rows, columns or ["value"], len(scalar_rows)

    @staticmethod
    def _coerce_single_row(row: Any, columns: Sequence[str] | None = None) -> dict[str, Any]:
        """Coerce a single row object (dict, struct, tuple, or mapping) into a dictionary."""
        if isinstance(row, dict):
            return dict(row)
        if is_msgspec_struct(row):
            return msgspec.structs.asdict(row)
        if isinstance(row, (tuple, list)):
            col_names = list(columns) if columns else [f"c{idx}" for idx in range(len(row))]
            return dict(zip(col_names, row, strict=False))
        if hasattr(row, "keys"):
            return dict(row)
        return {"value": row}

    @staticmethod
    def _extract_scalar(rows: Sequence[Any]) -> Any:
        """Extract the first scalar column value from the first row of a result sequence."""
        if not rows:
            return None
        first = rows[0]
        if isinstance(first, dict):
            return next(iter(first.values()), None)
        if isinstance(first, (tuple, list)):
            return first[0] if first else None
        return first


class SQLSpecSyncProvider(_BaseSQLSpecProvider):
    """Synchronous host provider executing SQLSpec driver operations over pinned broker leases and Arrow SHM."""

    __slots__ = ()

    def handle_request(self, request: BrokerRpcRequest) -> BrokerRpcResponse:
        """Execute a synchronous BrokerRpcRequest and return a BrokerRpcResponse."""
        return self._handle_request_sync(request)

    def handle_raw_frame(self, frame: bytes | bytearray | memoryview) -> bytes:
        """Decode a msgpack BrokerRpcRequest frame, execute synchronously, and return encoded response."""
        return self._handle_raw_frame_sync(frame)

    def close(self) -> None:
        """Release all leased sessions and unlink any remaining shared-memory Arrow buffers."""
        self._close_sync()


class SQLSpecAsyncProvider(_BaseSQLSpecProvider):
    """Asynchronous host provider executing SQLSpec driver operations over pinned broker leases and Arrow SHM."""

    __slots__ = ()

    async def handle_request(self, request: BrokerRpcRequest) -> BrokerRpcResponse:
        """Execute an asynchronous BrokerRpcRequest without event-loop thread blocking."""
        return await self._handle_request_async(request)

    async def handle_raw_frame(self, frame: bytes | bytearray | memoryview) -> bytes:
        """Decode a msgpack BrokerRpcRequest frame, execute asynchronously, and return encoded response."""
        return await self._handle_raw_frame_async(frame)

    async def close(self) -> None:
        """Asynchronously release all leased sessions and unlink remaining Arrow buffers."""
        await self._close_async()
