"""Shared-memory RPC provider host and zero-copy Arrow IPC buffer for SQLSpec drivers."""

from __future__ import annotations

import contextlib
import inspect
import mmap
import os
import tempfile
import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import msgspec

from vertebra.typing import ImproperConfigurationError, VertebraError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import (
    decode_json,
    decode_msgpack,
    encode_json_bytes,
    encode_msgpack_bytes,
)
from vertebra.utils.type_guards import is_msgspec_struct

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "BrokerRpcRequest",
    "BrokerRpcResponse",
    "SharedArrowBuffer",
    "SharedArrowTable",
    "SQLSpecProvider",
)


class BrokerRpcRequest(msgspec.Struct, frozen=True, gc=False):
    """RPC request frame dispatched from a worker proxy driver to SQLSpecProvider."""

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


class BrokerRpcResponse(msgspec.Struct, frozen=True, gc=False):
    """RPC response frame returned from SQLSpecProvider to a worker proxy driver."""

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


class SharedArrowTable:
    """Zero-copy mmap-backed Arrow-compatible table when PyArrow is not installed in the environment."""

    __slots__ = ("_column_names", "_mm", "_rows")

    def __init__(
        self,
        rows: list[dict[str, Any]],
        column_names: list[str],
        *,
        mm: mmap.mmap | None = None,
    ) -> None:
        """Initialize SharedArrowTable holding rows, column names, and the underlying mmap view."""
        self._rows = rows
        self._column_names = column_names
        self._mm = mm

    @property
    def num_rows(self) -> int:
        """Return the number of rows in the table."""
        return len(self._rows)

    @property
    def column_names(self) -> list[str]:
        """Return the ordered column names of the table."""
        return list(self._column_names)

    def __len__(self) -> int:
        """Return the number of rows in the table."""
        return len(self._rows)

    def to_pylist(self) -> list[dict[str, Any]]:
        """Convert the table into a list of row dictionaries."""
        return [dict(r) for r in self._rows]

    def to_pydict(self) -> dict[str, list[Any]]:
        """Convert the table into a dictionary of column lists."""
        return {col: [r.get(col) for r in self._rows] for col in self._column_names}


class SharedArrowBuffer:
    """Named POSIX shared-memory manager for zero-copy PyArrow IPC stream buffers."""

    __slots__ = ("_shm_dir",)

    _DEFAULT_SHM_DIR: ClassVar[Path] = Path("/dev/shm") if Path("/dev/shm").is_dir() else Path(tempfile.gettempdir())

    def __init__(self, shm_dir: Path | str | None = None) -> None:
        """Initialize SharedArrowBuffer with an optional backing directory."""
        self._shm_dir = Path(shm_dir) if shm_dir is not None else self._DEFAULT_SHM_DIR

    @property
    def shm_dir(self) -> Path:
        """Return the directory hosting shared-memory Arrow IPC files."""
        return self._shm_dir

    @classmethod
    def write_table(
        cls,
        table: Any,
        *,
        columns: Sequence[str] | None = None,
        shm_dir: Path | None = None,
    ) -> tuple[str, int]:
        """Serialize a PyArrow Table or row sequence into a named shared-memory IPC stream."""
        pa = import_optional("pyarrow")
        if pa is not None:
            arrow_table = cls._coerce_arrow_table(pa, table, columns=columns)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
                writer.write_table(arrow_table)
            ipc_buf = sink.getvalue()
            payload = memoryview(ipc_buf)
        else:
            fallback_rows, fallback_cols = cls._coerce_fallback_rows(table, columns=columns)
            raw_bytes = encode_msgpack_bytes({"columns": fallback_cols, "rows": fallback_rows})
            payload = memoryview(raw_bytes)

        byte_len = len(payload)
        shm_name = f"vtb-arrow-{uuid.uuid4().hex}"
        shm_path = cls._resolve_shm_path(shm_name, shm_dir=shm_dir)
        alloc_len = max(1, byte_len)
        fd = os.open(shm_path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        try:
            os.ftruncate(fd, alloc_len)
            with mmap.mmap(fd, alloc_len, access=mmap.ACCESS_WRITE) as mm:
                if byte_len > 0:
                    mm[:byte_len] = payload
        finally:
            os.close(fd)
        return shm_name, byte_len

    @classmethod
    def read_table(
        cls,
        shm_name: str,
        byte_len: int,
        *,
        shm_dir: Path | None = None,
    ) -> Any:
        """Read a PyArrow Table zero-copy from a named shared-memory IPC stream."""
        pa = import_optional("pyarrow")
        shm_path = cls._resolve_shm_path(shm_name, shm_dir=shm_dir)
        if not shm_path.exists():
            msg = f"Shared Arrow buffer {shm_name!r} does not exist at {shm_path}"
            raise FileNotFoundError(msg)
        if byte_len <= 0:
            return pa.table({}) if pa is not None else SharedArrowTable([], [])

        fd = os.open(shm_path, os.O_RDONLY)
        try:
            mm = mmap.mmap(fd, byte_len, access=mmap.ACCESS_READ)
        finally:
            os.close(fd)

        if pa is not None:
            pa_buf = pa.py_buffer(mm)
            with pa.ipc.open_stream(pa_buf) as reader:
                return reader.read_all()

        decoded = decode_msgpack(memoryview(mm))
        cols = [str(c) for c in decoded.get("columns", [])] if isinstance(decoded, dict) else []
        rows = [dict(r) for r in decoded.get("rows", [])] if isinstance(decoded, dict) else []
        return SharedArrowTable(rows, cols, mm=mm)

    @classmethod
    def unlink(cls, shm_name: str, *, shm_dir: Path | None = None) -> bool:
        """Unlink a named shared-memory Arrow IPC buffer and return True if removed."""
        if not shm_name:
            return False
        shm_path = cls._resolve_shm_path(shm_name, shm_dir=shm_dir)
        try:
            shm_path.unlink()
            return True
        except FileNotFoundError:
            return False

    @classmethod
    def exists(cls, shm_name: str, *, shm_dir: Path | None = None) -> bool:
        """Return True when the named shared-memory Arrow IPC buffer exists on disk."""
        if not shm_name:
            return False
        return cls._resolve_shm_path(shm_name, shm_dir=shm_dir).exists()

    @classmethod
    def _resolve_shm_path(cls, shm_name: str, shm_dir: Path | None = None) -> Path:
        """Resolve the filesystem path for a shared-memory segment name."""
        clean_name = Path(shm_name).name
        if not clean_name or clean_name in {".", ".."}:
            raise ImproperConfigurationError(f"Invalid shared memory segment name: {shm_name!r}")
        base = shm_dir if shm_dir is not None else cls._DEFAULT_SHM_DIR
        return base / clean_name

    @classmethod
    def coerce_table(
        cls,
        table: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> Any:
        """Coerce a PyArrow Table, RecordBatch, or row sequence into an Arrow table."""
        pa = import_optional("pyarrow")
        if pa is not None:
            return cls._coerce_arrow_table(pa, table, columns=columns)
        rows, cols = cls._coerce_fallback_rows(table, columns=columns)
        return SharedArrowTable(rows, cols)

    @classmethod
    def _coerce_fallback_rows(
        cls,
        table: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Coerce a table or row sequence into normalized (dict_rows, column_names)."""
        if isinstance(table, SharedArrowTable):
            return table.to_pylist(), table.column_names
        if hasattr(table, "to_pylist") and callable(table.to_pylist):
            py_rows = list(table.to_pylist())
            raw_cols = getattr(table, "column_names", columns)
            cols = [str(c) for c in raw_cols] if isinstance(raw_cols, (list, tuple)) else []
            if not cols and py_rows and isinstance(py_rows[0], dict):
                cols = list(py_rows[0].keys())
            return [dict(r) for r in py_rows], cols
        if isinstance(table, list):
            if not table:
                return [], list(columns) if columns else []
            first = table[0]
            if isinstance(first, dict):
                dict_rows = [dict(r) for r in table]
                cols = list(columns) if columns else list(dict_rows[0].keys())
                return dict_rows, cols
            if is_msgspec_struct(first):
                struct_rows = [msgspec.structs.asdict(item) for item in table]
                cols = list(columns) if columns else list(struct_rows[0].keys())
                return struct_rows, cols
            if isinstance(first, (tuple, list)):
                cols = list(columns) if columns else [f"c{idx}" for idx in range(len(first))]
                mapped = [dict(zip(cols, row, strict=False)) for row in table]
                return mapped, cols
            return [{"value": item} for item in table], list(columns) if columns else ["value"]
        items = list(table)
        return cls._coerce_fallback_rows(items, columns=columns)

    @classmethod
    def _coerce_arrow_table(
        cls,
        pa: Any,
        table: Any,
        columns: Sequence[str] | None = None,
    ) -> Any:
        """Coerce a PyArrow Table, RecordBatch, or row sequence into a pyarrow.Table."""
        if hasattr(table, "schema") and hasattr(table, "num_rows"):
            if isinstance(table, pa.RecordBatch):
                return pa.Table.from_batches([table])
            return table
        if hasattr(table, "to_arrow") and callable(table.to_arrow):
            converted = table.to_arrow()
            if isinstance(converted, pa.RecordBatch):
                return pa.Table.from_batches([converted])
            return converted
        if isinstance(table, list):
            if not table:
                if columns:
                    return pa.Table.from_pydict({col: [] for col in columns})
                return pa.table({})
            first = table[0]
            if isinstance(first, dict):
                return pa.Table.from_pylist(table)
            if is_msgspec_struct(first):
                return pa.Table.from_pylist([msgspec.structs.asdict(item) for item in table])
            if isinstance(first, (tuple, list)):
                col_names = list(columns) if columns else [f"c{idx}" for idx in range(len(first))]
                col_dict = {
                    col_names[idx]: [row[idx] if idx < len(row) else None for row in table]
                    for idx in range(len(col_names))
                }
                return pa.Table.from_pydict(col_dict)
            return pa.Table.from_pydict({"value": table})
        return pa.Table.from_pylist(list(table))


class SQLSpecProvider:
    """Host provider executing SQLSpec driver operations over pinned broker leases and Arrow SHM."""

    __slots__ = (
        "_active_arrow_buffers",
        "_broker_leases",
        "_leased_contexts",
        "_leased_sessions",
        "_lock",
        "_next_generation",
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
        """Initialize SQLSpecProvider with an upstream config/driver/factory and optional broker."""
        self.default_pool_name = pool_name
        self.broker = broker
        self.arrow_threshold_rows = arrow_threshold_rows
        self.arrow_threshold_bytes = arrow_threshold_bytes
        self._upstreams: dict[str, Any] = {}
        self._leased_sessions: dict[tuple[int, int], Any] = {}
        self._leased_contexts: dict[tuple[int, int], Any] = {}
        self._broker_leases: dict[tuple[int, int], Any] = {}
        self._tx_states: dict[tuple[int, int], bool] = {}
        self._active_arrow_buffers: set[str] = set()
        self._next_generation: int = 1
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

    def handle_request(self, request: BrokerRpcRequest) -> BrokerRpcResponse:
        """Execute a synchronous BrokerRpcRequest and return a BrokerRpcResponse."""
        op = request.op
        key = (request.slot_idx, request.generation)
        try:
            if op in {"arrow_release", "_arrow_release"}:
                SharedArrowBuffer.unlink(request.arrow_shm_name)
                with self._lock:
                    self._active_arrow_buffers.discard(request.arrow_shm_name)
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
                self._ensure_session_sync(lease_key, request.pool_name)
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

            if op == "commit":
                self._exec_commit_sync(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "rollback":
                self._exec_rollback_sync(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
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
                    in_tx=self._tx_states.get(key, request.in_tx),
                )

            if op == "execute_many":
                rowcount = self._exec_many_sync(session, request.sql, request.many_params)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rowcount=rowcount,
                    in_tx=self._tx_states.get(key, request.in_tx),
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
                    in_tx=self._tx_states.get(key, request.in_tx),
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
                    in_tx=self._tx_states.get(key, request.in_tx),
                )

            if op == "select_value":
                scalar = self._exec_select_value_sync(session, request.sql, request.params)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    scalar=scalar,
                    rowcount=1 if scalar is not None else 0,
                    in_tx=self._tx_states.get(key, request.in_tx),
                )

            if op == "select_to_arrow":
                return self._exec_select_to_arrow_sync(session, request, key)

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
                error_message=str(exc),
            )

    async def handle_request_async(self, request: BrokerRpcRequest) -> BrokerRpcResponse:
        """Execute an asynchronous BrokerRpcRequest and return a BrokerRpcResponse."""
        op = request.op
        key = (request.slot_idx, request.generation)
        try:
            if op in {"arrow_release", "_arrow_release"}:
                SharedArrowBuffer.unlink(request.arrow_shm_name)
                with self._lock:
                    self._active_arrow_buffers.discard(request.arrow_shm_name)
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
                await self._ensure_session_async(lease_key, request.pool_name)
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

            if op == "commit":
                await self._exec_commit_async(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
                )

            if op == "rollback":
                await self._exec_rollback_async(session, key)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    in_tx=False,
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
                    in_tx=self._tx_states.get(key, request.in_tx),
                )

            if op == "execute_many":
                rowcount = await self._exec_many_async(session, request.sql, request.many_params)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    rowcount=rowcount,
                    in_tx=self._tx_states.get(key, request.in_tx),
                )

            if op == "select":
                raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), request.sql, request.params)
                rows, columns, _ = await self._extract_result_async(raw_res)
                return self._build_select_response(request, key, rows, columns)

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
                    in_tx=self._tx_states.get(key, request.in_tx),
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
                    in_tx=self._tx_states.get(key, request.in_tx),
                )

            if op == "select_value":
                scalar = await self._exec_select_value_async(session, request.sql, request.params)
                return BrokerRpcResponse(
                    request_id=request.request_id,
                    ok=True,
                    slot_idx=request.slot_idx,
                    generation=request.generation,
                    scalar=scalar,
                    rowcount=1 if scalar is not None else 0,
                    in_tx=self._tx_states.get(key, request.in_tx),
                )

            if op == "select_to_arrow":
                return await self._exec_select_to_arrow_async(session, request, key)

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
                error_message=str(exc),
            )

    def handle_raw_frame(self, payload: bytes | bytearray | memoryview) -> bytes:
        """Decode a serialized RPC request frame (MessagePack or JSON), execute it, and encode the response."""
        raw_bytes = bytes(payload)
        is_json = raw_bytes.lstrip().startswith(b"{")
        if is_json:
            decoded_dict = decode_json(raw_bytes)
            req = msgspec.convert(decoded_dict, type=BrokerRpcRequest)
            resp = self.handle_request(req)
            return encode_json_bytes(resp)
        req = decode_msgpack(raw_bytes, type=BrokerRpcRequest)
        resp = self.handle_request(req)
        return encode_msgpack_bytes(resp)

    async def handle_raw_frame_async(self, payload: bytes | bytearray | memoryview) -> bytes:
        """Decode a serialized RPC request frame asynchronously, execute it, and encode the response."""
        raw_bytes = bytes(payload)
        is_json = raw_bytes.lstrip().startswith(b"{")
        if is_json:
            decoded_dict = decode_json(raw_bytes)
            req = msgspec.convert(decoded_dict, type=BrokerRpcRequest)
            resp = await self.handle_request_async(req)
            return encode_json_bytes(resp)
        req = decode_msgpack(raw_bytes, type=BrokerRpcRequest)
        resp = await self.handle_request_async(req)
        return encode_msgpack_bytes(resp)

    def close(self) -> None:
        """Release all leased sessions and unlink any remaining shared-memory Arrow buffers."""
        with self._lock:
            keys = list(self._leased_sessions.keys())
            arrow_names = list(self._active_arrow_buffers)
            self._active_arrow_buffers.clear()
        for key in keys:
            self._release_session_sync(key, in_tx=self._tx_states.get(key, False))
        for name in arrow_names:
            SharedArrowBuffer.unlink(name)

    async def close_async(self) -> None:
        """Asynchronously release all leased sessions and unlink remaining Arrow buffers."""
        with self._lock:
            keys = list(self._leased_sessions.keys())
            arrow_names = list(self._active_arrow_buffers)
            self._active_arrow_buffers.clear()
        for key in keys:
            await self._release_session_async(key, in_tx=self._tx_states.get(key, False))
        for name in arrow_names:
            SharedArrowBuffer.unlink(name)

    def _resolve_upstream(self, pool_name: str) -> Any:
        """Resolve the upstream config, driver, or factory for pool_name."""
        with self._lock:
            if pool_name in self._upstreams:
                return self._upstreams[pool_name]
            if self.default_pool_name in self._upstreams:
                return self._upstreams[self.default_pool_name]
            if "default" in self._upstreams:
                return self._upstreams["default"]
            if self._upstreams:
                return next(iter(self._upstreams.values()))
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

    def _acquire_from_broker_sync(self, pool_name: str, worker_pid: int) -> Any:
        """Acquire a lease from self.broker synchronously."""
        target = getattr(self.broker, "transport", self.broker)
        acquire_lease_fn = getattr(self.broker, "acquire_lease", None)
        if callable(acquire_lease_fn):
            return self._call_broker_acquire(acquire_lease_fn, pool_name, worker_pid)
        acquire_fn = getattr(target, "acquire", None)
        if callable(acquire_fn):
            return self._call_broker_acquire(acquire_fn, pool_name, worker_pid)
        raise ImproperConfigurationError("Configured broker does not support synchronous lease acquisition")

    async def _acquire_from_broker_async(self, pool_name: str, worker_pid: int) -> Any:
        """Acquire a lease from self.broker asynchronously."""
        target = getattr(self.broker, "transport", self.broker)
        for attr in ("acquire_lease_async", "acquire_async", "acquire_lease", "acquire"):
            fn = getattr(self.broker, attr, None) or getattr(target, attr, None)
            if callable(fn):
                res = self._call_broker_acquire(fn, pool_name, worker_pid)
                if inspect.isawaitable(res):
                    return await res
                return res
        raise ImproperConfigurationError("Configured broker does not support asynchronous lease acquisition")

    @staticmethod
    def _call_broker_acquire(fn: Any, pool_name: str, worker_pid: int) -> Any:
        """Invoke a broker acquire function matching its accepted keyword or positional parameters."""
        pid = worker_pid if worker_pid > 0 else os.getpid()
        try:
            sig = inspect.signature(fn)
            params = sig.parameters
            kwargs: dict[str, Any] = {}
            if "pool_name" in params:
                kwargs["pool_name"] = pool_name
            elif "bind_key" in params:
                kwargs["bind_key"] = pool_name
            if "worker_pid" in params:
                kwargs["worker_pid"] = pid
            elif "worker_id" in params:
                kwargs["worker_id"] = pid
            if kwargs or not params:
                return fn(**kwargs)
        except (ValueError, TypeError):
            pass
        return fn()

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
        if hasattr(candidate, "__enter__") and not hasattr(candidate, "execute"):
            ctx_mgr = candidate
            session = ctx_mgr.__enter__()
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

        if inspect.isawaitable(candidate):
            candidate = await candidate

        ctx_mgr = None
        if hasattr(candidate, "__aenter__") and not hasattr(candidate, "execute"):
            ctx_mgr = candidate
            session = await ctx_mgr.__aenter__()
        elif hasattr(candidate, "__enter__") and not hasattr(candidate, "execute"):
            ctx_mgr = candidate
            session = ctx_mgr.__enter__()
        else:
            session = candidate

        with self._lock:
            self._leased_sessions[key] = session
            if ctx_mgr is not None:
                self._leased_contexts[key] = ctx_mgr
            self._tx_states.setdefault(key, False)
        return session

    def _release_session_sync(self, key: tuple[int, int], in_tx: bool) -> None:
        """Release a pinned session synchronously, rolling back any uncommitted transaction."""
        with self._lock:
            session = self._leased_sessions.pop(key, None)
            ctx_mgr = self._leased_contexts.pop(key, None)
            was_in_tx = self._tx_states.pop(key, False) or in_tx
            lease = self._broker_leases.pop(key, None)

        if session is not None and was_in_tx:
            with contextlib.suppress(Exception):
                self._exec_rollback_sync(session, key)

        if ctx_mgr is not None and hasattr(ctx_mgr, "__exit__"):
            with contextlib.suppress(Exception):
                ctx_mgr.__exit__(None, None, None)

        if lease is not None:
            self._release_broker_lease(lease, key, in_tx=False)

    async def _release_session_async(self, key: tuple[int, int], in_tx: bool) -> None:
        """Release a pinned session asynchronously, rolling back any uncommitted transaction."""
        with self._lock:
            session = self._leased_sessions.pop(key, None)
            ctx_mgr = self._leased_contexts.pop(key, None)
            was_in_tx = self._tx_states.pop(key, False) or in_tx
            lease = self._broker_leases.pop(key, None)

        if session is not None and was_in_tx:
            with contextlib.suppress(Exception):
                await self._exec_rollback_async(session, key)

        if ctx_mgr is not None:
            if hasattr(ctx_mgr, "__aexit__"):
                with contextlib.suppress(Exception):
                    await ctx_mgr.__aexit__(None, None, None)
            elif hasattr(ctx_mgr, "__exit__"):
                with contextlib.suppress(Exception):
                    ctx_mgr.__exit__(None, None, None)

        if lease is not None:
            self._release_broker_lease(lease, key, in_tx=False)

    def _release_broker_lease(self, lease: Any, key: tuple[int, int], *, in_tx: bool = False) -> None:
        """Release a broker lease back to self.broker."""
        if hasattr(lease, "release") and callable(lease.release):
            with contextlib.suppress(Exception):
                lease.release()
            return
        if self.broker is None:
            return
        release_lease_fn = getattr(self.broker, "release_lease", None)
        if callable(release_lease_fn):
            with contextlib.suppress(Exception):
                release_lease_fn(key[0], key[1], in_tx=in_tx)
            return
        target = getattr(self.broker, "transport", self.broker)
        release_fn = getattr(target, "release", None)
        if callable(release_fn):
            with contextlib.suppress(Exception):
                release_fn(f"{key[0]}:{key[1]}")

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
            if inspect.isawaitable(res):
                await res
        elif hasattr(session, "execute"):
            if not getattr(session, "in_transaction", False):
                res = session.execute("BEGIN")
                if inspect.isawaitable(res):
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
            self._tx_states[key] = False

    async def _exec_commit_async(self, session: Any, key: tuple[int, int]) -> None:
        """Commit an active transaction on session asynchronously and clear transaction state."""
        commit_fn = getattr(session, "commit", None)
        if callable(commit_fn):
            res = commit_fn()
            if inspect.isawaitable(res):
                await res
        elif hasattr(session, "execute"):
            res = session.execute("COMMIT")
            if inspect.isawaitable(res):
                await res
        with self._lock:
            self._tx_states[key] = False

    def _exec_rollback_sync(self, session: Any, key: tuple[int, int]) -> None:
        """Roll back an active transaction on session synchronously and clear transaction state."""
        rollback_fn = getattr(session, "rollback", None)
        if callable(rollback_fn):
            rollback_fn()
        elif hasattr(session, "execute"):
            session.execute("ROLLBACK")
        with self._lock:
            self._tx_states[key] = False

    async def _exec_rollback_async(self, session: Any, key: tuple[int, int]) -> None:
        """Roll back an active transaction on session asynchronously and clear transaction state."""
        rollback_fn = getattr(session, "rollback", None)
        if callable(rollback_fn):
            res = rollback_fn()
            if inspect.isawaitable(res):
                await res
        elif hasattr(session, "execute"):
            res = session.execute("ROLLBACK")
            if inspect.isawaitable(res):
                await res
        with self._lock:
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
                if inspect.isawaitable(res):
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
            res = self._call_with_params(direct_fn, sql, params)
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
            res = self._call_with_params(direct_fn, sql, params)
            if inspect.isawaitable(res):
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

    def _exec_select_value_sync(self, session: Any, sql: str, params: Any) -> Any:
        """Execute select_value synchronously and return a scalar value."""
        for attr in ("select_value", "fetch_val"):
            fn = getattr(session, attr, None)
            if callable(fn):
                return self._call_with_params(fn, sql, params)
        raw_res = self._invoke_sync(session, ("select", "fetch", "execute"), sql, params)
        rows, _, _ = self._extract_result_sync(raw_res)
        return self._extract_scalar(rows)

    async def _exec_select_value_async(self, session: Any, sql: str, params: Any) -> Any:
        """Execute select_value asynchronously and return a scalar value."""
        for attr in ("select_value", "fetch_val"):
            fn = getattr(session, attr, None)
            if callable(fn):
                res = self._call_with_params(fn, sql, params)
                if inspect.isawaitable(res):
                    return await res
                return res
        raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), sql, params)
        rows, _, _ = await self._extract_result_async(raw_res)
        return self._extract_scalar(rows)

    def _exec_select_to_arrow_sync(
        self,
        session: Any,
        request: BrokerRpcRequest,
        key: tuple[int, int],
    ) -> BrokerRpcResponse:
        """Execute select_to_arrow synchronously and spill the result table into SharedArrowBuffer."""
        arrow_fn = getattr(session, "select_to_arrow", None)
        if callable(arrow_fn):
            raw_arrow = self._call_with_params(arrow_fn, request.sql, request.params)
            shm_name, byte_len, columns, rowcount = self._spill_arrow_object(raw_arrow)
        else:
            raw_res = self._invoke_sync(session, ("select", "fetch", "execute"), request.sql, request.params)
            rows, columns, _ = self._extract_result_sync(raw_res)
            shm_name, byte_len = SharedArrowBuffer.write_table(rows, columns=columns)
            with self._lock:
                self._active_arrow_buffers.add(shm_name)
            rowcount = len(rows)
        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            columns=columns,
            rowcount=rowcount,
            in_tx=self._tx_states.get(key, request.in_tx),
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
        """Execute select_to_arrow asynchronously and spill the result table into SharedArrowBuffer."""
        arrow_fn = getattr(session, "select_to_arrow", None)
        if callable(arrow_fn):
            raw_arrow = self._call_with_params(arrow_fn, request.sql, request.params)
            if inspect.isawaitable(raw_arrow):
                raw_arrow = await raw_arrow
            shm_name, byte_len, columns, rowcount = self._spill_arrow_object(raw_arrow)
        else:
            raw_res = await self._invoke_async(session, ("select", "fetch", "execute"), request.sql, request.params)
            rows, columns, _ = await self._extract_result_async(raw_res)
            shm_name, byte_len = SharedArrowBuffer.write_table(rows, columns=columns)
            with self._lock:
                self._active_arrow_buffers.add(shm_name)
            rowcount = len(rows)
        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            columns=columns,
            rowcount=rowcount,
            in_tx=self._tx_states.get(key, request.in_tx),
            is_arrow_shm=True,
            arrow_shm_name=shm_name,
            arrow_byte_len=byte_len,
        )

    def _spill_arrow_object(self, raw_arrow: Any) -> tuple[str, int, list[str], int]:
        """Write an Arrow Table or row collection into SharedArrowBuffer and return metadata."""
        table = SharedArrowBuffer.coerce_table(raw_arrow)
        shm_name, byte_len = SharedArrowBuffer.write_table(table)
        with self._lock:
            self._active_arrow_buffers.add(shm_name)
        return shm_name, byte_len, list(table.column_names), int(table.num_rows)

    def _build_select_response(
        self,
        request: BrokerRpcRequest,
        key: tuple[int, int],
        rows: list[dict[str, Any] | list[Any]],
        columns: list[str],
    ) -> BrokerRpcResponse:
        """Construct a BrokerRpcResponse for select, spilling to SharedArrowBuffer when thresholds are met."""
        in_tx = self._tx_states.get(key, request.in_tx)
        row_count = len(rows)
        should_spill = row_count >= self.arrow_threshold_rows
        if not should_spill and row_count > 0 and self.arrow_threshold_bytes > 0:
            encoded = encode_msgpack_bytes(rows)
            if len(encoded) >= self.arrow_threshold_bytes:
                should_spill = True

        if should_spill:
            shm_name, byte_len = SharedArrowBuffer.write_table(rows, columns=columns)
            with self._lock:
                self._active_arrow_buffers.add(shm_name)
            return BrokerRpcResponse(
                request_id=request.request_id,
                ok=True,
                slot_idx=request.slot_idx,
                generation=request.generation,
                columns=columns,
                rowcount=row_count,
                in_tx=in_tx,
                is_arrow_shm=True,
                arrow_shm_name=shm_name,
                arrow_byte_len=byte_len,
            )

        return BrokerRpcResponse(
            request_id=request.request_id,
            ok=True,
            slot_idx=request.slot_idx,
            generation=request.generation,
            rows=rows,
            columns=columns,
            rowcount=row_count,
            in_tx=in_tx,
            is_arrow_shm=False,
        )

    def _invoke_sync(self, session: Any, method_names: tuple[str, ...], sql: str, params: Any) -> Any:
        """Invoke the first available method in method_names on session synchronously."""
        for name in method_names:
            fn = getattr(session, name, None)
            if callable(fn):
                return self._call_with_params(fn, sql, params)
        msg = f"Upstream session {type(session).__name__!r} does not implement any of {method_names}"
        raise AttributeError(msg)

    async def _invoke_async(self, session: Any, method_names: tuple[str, ...], sql: str, params: Any) -> Any:
        """Invoke the first available method in method_names on session asynchronously."""
        for name in method_names:
            fn = getattr(session, name, None)
            if callable(fn):
                res = self._call_with_params(fn, sql, params)
                if inspect.isawaitable(res):
                    return await res
                return res
        msg = f"Upstream session {type(session).__name__!r} does not implement any of {method_names}"
        raise AttributeError(msg)

    @staticmethod
    def _call_with_params(fn: Any, sql: str, params: Any) -> Any:
        """Call a driver method with sql and either unpacked varargs or a parameter collection."""
        if params is None or params == () or params == []:
            return fn(sql)
        if isinstance(params, dict):
            return fn(sql, params)
        if isinstance(params, (tuple, list)):
            has_varargs = False
            with contextlib.suppress(ValueError, TypeError):
                sig = inspect.signature(fn)
                has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
            if has_varargs:
                return fn(sql, *params)
            try:
                return fn(sql, params)
            except TypeError:
                return fn(sql, *params)
        return fn(sql, params)

    def _extract_result_sync(
        self,
        raw_res: Any,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], int]:
        """Extract normalized (rows, columns, rowcount) synchronously from a driver result."""
        if raw_res is None:
            return [], [], 0
        if isinstance(raw_res, int) and not isinstance(raw_res, bool):
            return [], [], raw_res
        if hasattr(raw_res, "fetchall") and callable(raw_res.fetchall):
            desc = getattr(raw_res, "description", None)
            if desc is not None:
                columns = [str(col[0]) for col in desc]
                fetched = raw_res.fetchall()
                rows: list[dict[str, Any] | list[Any]] = [self._coerce_single_row(row, columns) for row in fetched]
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
        if hasattr(raw_res, "fetchall") and callable(raw_res.fetchall):
            desc = getattr(raw_res, "description", None)
            if desc is not None:
                columns = [str(col[0]) for col in desc]
                fetched = raw_res.fetchall()
                if inspect.isawaitable(fetched):
                    fetched = await fetched
                rows: list[dict[str, Any] | list[Any]] = [self._coerce_single_row(row, columns) for row in fetched]
                return rows, columns, len(rows)
            rc = getattr(raw_res, "rowcount", 0)
            return [], [], max(0, int(rc)) if isinstance(rc, int) else 0
        return self._normalize_collection_result(raw_res)

    def _normalize_collection_result(
        self,
        raw_res: Any,
    ) -> tuple[list[dict[str, Any] | list[Any]], list[str], int]:
        """Normalize a SQLResult, list of dicts, or list of tuples into (rows, columns, rowcount)."""
        if hasattr(raw_res, "to_dicts") and callable(raw_res.to_dicts):
            items = list(raw_res.to_dicts())
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
