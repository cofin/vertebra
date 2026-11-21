"""Protocol-faithful in-memory native PostgreSQL test doubles matching ``vertebra._postgres``."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import struct
import threading
from collections import deque
from collections.abc import Buffer, Callable, Mapping, Sequence
from typing import Any, Literal

import pyarrow as pa

from vertebra._vertebra import IntegrityError, InterfaceError, NotFoundError, OperationalError, PoolExhaustedError
from vertebra.db.adapters.postgres.binds import compile_postgres_binds
from vertebra.db.adapters.postgres.types import (
    PgJson,
    PostgresArrowResult,
    PostgresColumn,
    PostgresPipelineOperation,
    PostgresPipelineOutcome,
    PostgresQueryResult,
    PostgresReplicationIdentity,
    PostgresReplicationRecord,
)

__all__ = (
    "FakeAsyncPostgresArrowStream",
    "FakeAsyncPostgresClient",
    "FakeAsyncPostgresCopyIn",
    "FakeAsyncPostgresCopyOut",
    "FakeAsyncPostgresCursor",
    "FakeAsyncPostgresListener",
    "FakeAsyncPostgresPool",
    "FakeAsyncPostgresReplication",
    "FakeDirectClientFactory",
    "FakeNativeArrowCursor",
    "FakePostgresCluster",
    "FakeSyncPostgresArrowStream",
    "FakeSyncPostgresClient",
    "FakeSyncPostgresCopyIn",
    "FakeSyncPostgresCopyOut",
    "FakeSyncPostgresCursor",
    "FakeSyncPostgresPool",
)

_VTBARROW_MAGIC = b"VTBARROW"


def _coerce_sqlite_param(value: Any) -> Any:
    """Adapt PostgreSQL parameter wrappers for in-memory SQLite execution."""
    if isinstance(value, PgJson):
        return None if value.value is None else json.dumps(value.value)
    return value


def _sql_placeholders_to_sqlite(sql: str) -> tuple[str, list[int] | None]:
    """Rewrite PostgreSQL ``$N`` placeholders outside quotes/comments into SQLite ``?`` slots."""
    out: list[str] = []
    positions: list[int] = []
    n = len(sql)
    i = 0
    while i < n:
        ch = sql[i]
        if ch == "'":
            out.append(ch)
            i += 1
            while i < n:
                c = sql[i]
                out.append(c)
                i += 1
                if c == "'" and i < n and sql[i] == "'":
                    out.append("'")
                    i += 1
                elif c == "'":
                    break
            continue
        if ch == '"':
            out.append(ch)
            i += 1
            while i < n:
                c = sql[i]
                out.append(c)
                i += 1
                if c == '"':
                    break
            continue
        if ch == "$" and i + 1 < n and sql[i + 1].isdigit():
            j = i + 1
            while j < n and sql[j].isdigit():
                j += 1
            if j == n or not (sql[j].isalnum() or sql[j] == "_"):
                positions.append(int(sql[i + 1 : j]) - 1)
                out.append("?")
                i = j
                continue
        out.append(ch)
        i += 1
    return "".join(out), positions if positions else None


def _strip_leading_sql_comments(sql: str) -> str:
    """Strip leading SQL whitespace and block/line comments before statement classification."""
    text = sql.lstrip()
    while text:
        if text.startswith("/*"):
            end = text.find("*/", 2)
            if end == -1:
                break
            text = text[end + 2 :].lstrip()
        elif text.startswith("--"):
            nl = text.find("\n")
            text = "" if nl == -1 else text[nl + 1 :].lstrip()
        else:
            break
    return text


def _make_col(name: str, *, type_oid: int = 25, attribute_number: int = 0) -> PostgresColumn:
    """Construct a valid ``PostgresColumn`` descriptor with neutral metadata defaults."""
    return PostgresColumn(
        name=name,
        type_oid=type_oid,
        format=0,
        type_modifier=-1,
        table_oid=0,
        attribute_number=attribute_number,
    )


def _encode_arrow_ipc(
    columns: Sequence[str], rows: Sequence[tuple[Any, ...]], *, use_shm: bool
) -> bytes | FakeNativeArrowCursor:
    """Serialize query rows into Arrow IPC bytes or a ``VTBARROW``-framed SHM cursor."""
    pydict: dict[str, list[Any]] = {
        col: [row[idx] if idx < len(row) else None for row in rows] for idx, col in enumerate(columns)
    }
    table = pa.Table.from_pydict(pydict)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    ipc_bytes: bytes = sink.getvalue().to_pybytes()
    if use_shm:
        return FakeNativeArrowCursor(ipc_bytes=ipc_bytes, columns=tuple(columns), row_count=len(rows))
    return ipc_bytes


class FakeNativeArrowCursor:
    """Protocol-faithful fake native ``VTBARROW`` shared-memory cursor double."""

    def __init__(
        self,
        *,
        ipc_bytes: bytes,
        columns: tuple[str, ...],
        row_count: int,
        shm_name: str = "/vtb-fake-shm-1",
    ) -> None:
        self._ipc_bytes = ipc_bytes
        self._vtbarrow_bytes = _VTBARROW_MAGIC + struct.pack("<Q", len(ipc_bytes)) + ipc_bytes
        self.columns = columns
        self.row_count = row_count
        self.shm_name = shm_name
        self.byte_len = len(self._vtbarrow_bytes)
        self.released = False

    def buffer_view(self) -> memoryview:
        """Return a zero-copy ``memoryview`` over the ``VTBARROW`` frame."""
        return memoryview(self._vtbarrow_bytes)

    def to_arrow_table(self) -> pa.Table:
        """Decode the underlying Arrow IPC bytes into a ``pyarrow.Table``."""
        return pa.ipc.open_stream(pa.py_buffer(self._ipc_bytes)).read_all()

    def release(self) -> bool:
        """Mark the fake shared-memory segment as released."""
        was_released = self.released
        self.released = True
        return not was_released


class FakePostgresCluster:
    """In-memory SQLite-backed PostgreSQL state shared across simulated sessions and streams."""

    def __init__(self, *, system_identifier: str = "sys-pg-1", database: str = "appdb") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.isolation_level = None
        self.system_identifier = system_identifier
        self.database = database
        self.next_pid = 100
        self.listeners: list[FakeAsyncPostgresListener] = []
        self.replication_records: deque[PostgresReplicationRecord] = deque()
        self.replication_identities: dict[tuple[str, str], PostgresReplicationIdentity] = {}
        self.rollback_failures_remaining = 0
        self.set_config_reset_failures_remaining = 0
        self.query_started_event: asyncio.Event | None = None
        self.query_release_event: asyncio.Event | None = None
        self.recorded_queries: list[tuple[str, tuple[Any, ...], float | None]] = []

    def next_connection_pid(self) -> int:
        """Allocate a deterministic simulated backend PID."""
        with self._lock:
            self.next_pid += 1
            return self.next_pid

    def enqueue_replication_records(self, *records: PostgresReplicationRecord) -> None:
        """Push simulated logical replication WAL frames onto the cluster queue."""
        with self._lock:
            self.replication_records.extend(records)

    def set_replication_identity(
        self,
        slot: str,
        publication: str,
        *,
        fingerprint: str = "pub-fp-1",
    ) -> PostgresReplicationIdentity:
        """Register an explicit replication slot and publication fingerprint."""
        identity = PostgresReplicationIdentity(
            system_identifier=self.system_identifier,
            database=self.database,
            slot=slot,
            publication_fingerprint=fingerprint,
        )
        with self._lock:
            self.replication_identities[(slot, publication)] = identity
        return identity

    def get_replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity:
        """Resolve registered replication identity metadata or synthesize a default."""
        with self._lock:
            existing = self.replication_identities.get((slot, publication))
            if existing is not None:
                return existing
            return PostgresReplicationIdentity(
                system_identifier=self.system_identifier,
                database=self.database,
                slot=slot,
                publication_fingerprint=f"fp:{publication}",
            )

    def publish_notification(self, channel: str, payload: str, *, pid: int) -> None:
        """Deliver a ``pg_notify`` message to all subscribed fake listeners."""
        payload_bytes = payload.encode("utf-8")
        for listener in list(self.listeners):
            listener.deliver_if_subscribed(pid, channel, payload_bytes)

    def execute_sql(
        self,
        sql: str,
        parameters: Sequence[Any] | None,
        *,
        session_vars: dict[str, str],
        tx_vars: dict[str, str] | None,
        in_tx: bool,
        pid: int,
        timeout: float | None = None,
    ) -> PostgresQueryResult:
        """Execute a parameterized query or simulated PostgreSQL control statement."""
        params_tuple = tuple(parameters or ())
        cleaned = _strip_leading_sql_comments(sql)
        upper = cleaned.upper()
        with self._lock:
            self.recorded_queries.append((sql, params_tuple, timeout))
            if "PG_NOTIFY" in upper and len(params_tuple) == 2:
                channel, payload = str(params_tuple[0]), str(params_tuple[1])
                self.publish_notification(channel, payload, pid=pid)
                return PostgresQueryResult(
                    columns=(_make_col("pg_notify", type_oid=2278),),
                    rows=(("",),),
                    affected_rows=1,
                    command_tag="SELECT 1",
                    transaction_status="T" if in_tx else "I",
                )
            if "SET_CONFIG" in upper and len(params_tuple) == 3:
                key = str(params_tuple[0])
                val = "" if params_tuple[1] is None else str(params_tuple[1])
                is_local = bool(params_tuple[2])
                if not val and self.set_config_reset_failures_remaining > 0:
                    self.set_config_reset_failures_remaining -= 1
                    raise OperationalError("Simulated session tenant reset failure")
                target = tx_vars if (is_local and tx_vars is not None) else session_vars
                if val == "":
                    target.pop(key, None)
                else:
                    target[key] = val
                return PostgresQueryResult(
                    columns=(_make_col("set_config"),),
                    rows=((val,),),
                    affected_rows=1,
                    command_tag="SELECT 1",
                    transaction_status="T" if in_tx else "I",
                )
            if "CURRENT_SETTING" in upper and len(params_tuple) >= 1:
                key = str(params_tuple[0])
                val = (tx_vars or {}).get(key, session_vars.get(key, ""))
                return PostgresQueryResult(
                    columns=(_make_col("current_setting"),),
                    rows=((val,),),
                    affected_rows=1,
                    command_tag="SELECT 1",
                    transaction_status="T" if in_tx else "I",
                )

            if upper.startswith("SET "):
                return PostgresQueryResult(
                    columns=(),
                    rows=(),
                    affected_rows=0,
                    command_tag="SET",
                    transaction_status="T" if in_tx else "I",
                )

            sqlite_sql, positions = _sql_placeholders_to_sqlite(cleaned)
            ordered_params = (
                tuple(_coerce_sqlite_param(params_tuple[pos]) for pos in positions)
                if positions is not None
                else tuple(_coerce_sqlite_param(p) for p in params_tuple)
            )
            cur = self._conn.cursor()
            try:
                try:
                    cur.execute(sqlite_sql, ordered_params)
                except sqlite3.IntegrityError as exc:
                    raise IntegrityError(str(exc)) from exc
                if cur.description:
                    col_objs = tuple(
                        _make_col(str(desc[0]), attribute_number=idx + 1) for idx, desc in enumerate(cur.description)
                    )
                    fetched = tuple(tuple(row) for row in cur.fetchall())
                    if len(params_tuple) == 1 and isinstance(params_tuple[0], PgJson) and params_tuple[0].value is None:
                        fetched = ((PgJson(None),),)
                    return PostgresQueryResult(
                        columns=col_objs,
                        rows=fetched,
                        affected_rows=len(fetched),
                        command_tag=f"SELECT {len(fetched)}",
                        transaction_status="T" if in_tx else "I",
                    )
                affected = max(int(cur.rowcount or 0), 0)
                tag = upper.split(None, 1)[0] if upper else "OK"
                return PostgresQueryResult(
                    columns=(),
                    rows=(),
                    affected_rows=affected,
                    command_tag=f"{tag} {affected}",
                    transaction_status="T" if in_tx else "I",
                )
            finally:
                cur.close()


class _BaseFakeSession:
    """Shared transactional, Arrow, COPY, and pipeline state for sync and async doubles."""

    def __init__(self, cluster: FakePostgresCluster, *, release_hook: Callable[[Any], None] | None = None) -> None:
        self.cluster = cluster
        self.pid = cluster.next_connection_pid()
        self._release_hook = release_hook
        self.closed = False
        self.evicted = False
        self.in_transaction = False
        self.session_vars: dict[str, str] = {}
        self.tx_vars: dict[str, str] | None = None
        self.parameters: dict[str, str] = {"server_version": "16.4"}
        self.cancel_calls = 0
        self.active_pipeline_scopes: dict[int, bool] = {}
        self._next_pipe_scope = 0
        self._lease_releaser: Callable[[], None] | None = None

    def reset_for_pool_reuse(self) -> None:
        """Reset transient session state upon clean pool check-in."""
        if self.in_transaction:
            self._rollback_sync()
        self.session_vars.clear()
        self.tx_vars = None

    def _ensure_not_closed(self) -> None:
        if self.closed:
            msg = "Fake PostgreSQL session is closed"
            raise InterfaceError(msg)

    def _begin_sync(self) -> None:
        self._ensure_not_closed()
        if not self.in_transaction:
            with self.cluster._lock:
                self.cluster._conn.execute("BEGIN")
            self.in_transaction = True
            self.tx_vars = {}

    def _commit_sync(self) -> None:
        self._ensure_not_closed()
        if self.in_transaction:
            with self.cluster._lock:
                self.cluster._conn.execute("COMMIT")
            self.in_transaction = False
            self.tx_vars = None

    def _rollback_sync(self) -> None:
        if self.cluster.rollback_failures_remaining > 0:
            self.cluster.rollback_failures_remaining -= 1
            raise OperationalError("Simulated rollback transport failure")
        if self.in_transaction:
            with self.cluster._lock:
                self.cluster._conn.execute("ROLLBACK")
            self.in_transaction = False
            self.tx_vars = None

    def _savepoint_sync(self, token: int) -> None:
        self._ensure_not_closed()
        with self.cluster._lock:
            self.cluster._conn.execute(f"SAVEPOINT vtb_sp_{token}")

    def _release_savepoint_sync(self, token: int) -> None:
        self._ensure_not_closed()
        with self.cluster._lock:
            self.cluster._conn.execute(f"RELEASE SAVEPOINT vtb_sp_{token}")

    def _rollback_to_savepoint_sync(self, token: int) -> None:
        self._ensure_not_closed()
        with self.cluster._lock:
            self.cluster._conn.execute(f"ROLLBACK TO SAVEPOINT vtb_sp_{token}")

    def _query_sync(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> PostgresQueryResult:
        _ = parameter_oids
        self._ensure_not_closed()
        return self.cluster.execute_sql(
            sql,
            parameters,
            session_vars=self.session_vars,
            tx_vars=self.tx_vars,
            in_tx=self.in_transaction,
            pid=self.pid,
            timeout=timeout,
        )

    def _select_to_arrow_sync(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        threshold_rows: int = 256,
        timeout: float | None = None,
    ) -> PostgresArrowResult:
        res = self._query_sync(sql, parameters, timeout=timeout)
        col_names = tuple(c.name for c in res.columns)
        use_shm = len(res.rows) >= threshold_rows
        payload = _encode_arrow_ipc(col_names, res.rows, use_shm=use_shm)
        return PostgresArrowResult(
            buffer=payload,
            row_count=len(res.rows),
            columns=col_names,
            used_shm=isinstance(payload, FakeNativeArrowCursor),
        )

    def _pipeline_begin_sync(self) -> int:
        self._ensure_not_closed()
        self._next_pipe_scope += 1
        scope_id = self._next_pipe_scope
        nested = self.in_transaction
        if nested:
            with self.cluster._lock:
                self.cluster._conn.execute(f"SAVEPOINT vtb_pipe_{scope_id}")
        else:
            self._begin_sync()
        self.active_pipeline_scopes[scope_id] = nested
        return scope_id

    def _pipeline_results_sync(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> tuple[PostgresPipelineOutcome, ...]:
        self._ensure_not_closed()
        if scope_id not in self.active_pipeline_scopes:
            raise InterfaceError(f"Unknown pipeline scope_id={scope_id}")
        outcomes: list[PostgresPipelineOutcome] = []
        for idx, op in enumerate(operations):
            res = self._query_sync(op.sql, op.parameters)
            outcomes.append(PostgresPipelineOutcome(index=idx, status="result", result=res))
        return tuple(outcomes)

    def _pipeline_finish_sync(self, scope_id: int, success: bool) -> None:
        self._ensure_not_closed()
        nested = self.active_pipeline_scopes.pop(scope_id, None)
        if nested is None:
            return
        if nested:
            stmt = f"RELEASE SAVEPOINT vtb_pipe_{scope_id}" if success else f"ROLLBACK TO SAVEPOINT vtb_pipe_{scope_id}"
            with self.cluster._lock:
                self.cluster._conn.execute(stmt)
        elif success:
            self._commit_sync()
        else:
            self._rollback_sync()


class FakeSyncPostgresCopyOut:
    """Synchronous binary COPY TO STDOUT iterator over simulated rows."""

    def __init__(self, rows: Sequence[tuple[Any, ...]]) -> None:
        self._encoded = [tuple(None if cell is None else str(cell).encode("utf-8") for cell in row) for row in rows]
        self._idx = 0
        self.closed = False

    def __iter__(self) -> FakeSyncPostgresCopyOut:
        return self

    def __next__(self) -> tuple[bytes | None, ...]:
        if self.closed or self._idx >= len(self._encoded):
            raise StopIteration
        item = self._encoded[self._idx]
        self._idx += 1
        return item

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeSyncPostgresCopyOut:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        _ = (exc_type, exc_val, exc_tb)
        self.close()
        return False


class FakeSyncPostgresCopyIn:
    """Synchronous binary COPY FROM STDIN writer supporting row tuples and Arrow IPC."""

    def __init__(
        self,
        session: _BaseFakeSession,
        sql: str,
        *,
        table_name: str | None = None,
        columns: Sequence[str] | None = None,
    ) -> None:
        self._session = session
        self._sql = sql
        self._table_name = table_name
        self._explicit_columns = tuple(columns) if columns is not None else None
        self._pending_rows: list[tuple[Any, ...]] = []
        self._inferred_columns: tuple[str, ...] | None = self._explicit_columns
        self.closed = False
        self.finished = False

    def write(self, rows: Sequence[Sequence[Any]], column_oids: Sequence[int]) -> None:
        _ = column_oids
        if self.closed or self.finished:
            raise InterfaceError("CopyIn stream is closed")
        for row in rows:
            self._pending_rows.append(tuple(row))

    def write_arrow_ipc(self, batch_ipc: Buffer) -> None:
        if self.closed or self.finished:
            raise InterfaceError("CopyIn stream is closed")
        table = pa.ipc.open_stream(pa.py_buffer(batch_ipc)).read_all()
        if self._inferred_columns is None:
            self._inferred_columns = tuple(table.column_names)
        col_data = [table.column(idx).to_pylist() for idx in range(table.num_columns)]
        for row_idx in range(table.num_rows):
            self._pending_rows.append(tuple(col_data[c][row_idx] for c in range(table.num_columns)))

    def finish(self) -> int:
        if self.closed or self.finished:
            raise InterfaceError("CopyIn stream already finalized")
        self.finished = True
        self.closed = True
        parsed_table, parsed_cols = self._parse_table_and_columns_from_copy_sql(self._sql)
        target = self._table_name or parsed_table
        cols = self._inferred_columns or parsed_cols
        if not self._pending_rows:
            return 0
        if cols is None:
            cols_sql = ""
            width = len(self._pending_rows[0])
        else:
            cols_sql = f" ({', '.join(cols)})"
            width = len(cols)
        placeholders = ", ".join("?" for _ in range(width))
        insert_sql = f"INSERT INTO {target}{cols_sql} VALUES ({placeholders})"
        with self._session.cluster._lock:
            cur = self._session.cluster._conn.cursor()
            try:
                cur.executemany(insert_sql, self._pending_rows)
            finally:
                cur.close()
        return len(self._pending_rows)

    @staticmethod
    def _parse_table_and_columns_from_copy_sql(sql: str) -> tuple[str, tuple[str, ...] | None]:
        upper = sql.upper()
        copy_idx = upper.find("COPY ")
        from_idx = upper.find(" FROM ")
        if copy_idx == -1 or from_idx == -1:
            return "copy_target", None
        target_part = sql[copy_idx + 5 : from_idx].strip()
        if "(" in target_part and target_part.endswith(")"):
            table_name, cols_section = target_part.split("(", 1)
            cols = tuple(item.strip() for item in cols_section[:-1].split(",") if item.strip())
            return table_name.strip(), cols or None
        return target_part, None

    def close(self) -> None:
        if not self.finished:
            self._pending_rows.clear()
        self.closed = True

    def __enter__(self) -> FakeSyncPostgresCopyIn:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        _ = (exc_type, exc_val, exc_tb)
        self.close()
        return False


class FakeSyncPostgresArrowStream:
    """Synchronous chunked SHM Arrow cursor portal iterator."""

    def __init__(self, columns: tuple[str, ...], rows: Sequence[tuple[Any, ...]], batch_size: int) -> None:
        self._chunks: list[FakeNativeArrowCursor] = []
        step = max(1, int(batch_size))
        for start in range(0, len(rows), step):
            chunk = rows[start : start + step]
            cursor = _encode_arrow_ipc(columns, chunk, use_shm=True)
            assert isinstance(cursor, FakeNativeArrowCursor)
            self._chunks.append(cursor)
        self._idx = 0
        self.closed = False

    def __iter__(self) -> FakeSyncPostgresArrowStream:
        return self

    def __next__(self) -> FakeNativeArrowCursor:
        if self.closed or self._idx >= len(self._chunks):
            raise StopIteration
        item = self._chunks[self._idx]
        self._idx += 1
        return item

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeSyncPostgresArrowStream:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        _ = (exc_type, exc_val, exc_tb)
        self.close()
        return False


class FakeSyncPostgresCursor:
    """Protocol-faithful synchronous ``SyncPostgresCursor`` double backed by a client or pool."""

    def __init__(
        self,
        *,
        client: FakeSyncPostgresClient | None = None,
        pool: FakeSyncPostgresPool | None = None,
        lazy_begin: bool = False,
    ) -> None:
        self._client = client
        self._pool = pool
        self._lazy_begin = lazy_begin
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0
        self._description: tuple[tuple[str, int, None, None, None, None, None], ...] | None = None
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "Cursor is closed"
            raise InterfaceError(msg)

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> FakeSyncPostgresCursor:
        self._ensure_open()
        compiled = compile_postgres_binds(sql, params)
        compiled_params = compiled.params if compiled.params else None
        if self._client is not None:
            if self._lazy_begin and not self._client.in_transaction:
                self._client.begin()
            res = self._client.query(
                compiled.sql,
                compiled_params,
                parameter_oids=parameter_oids,
                timeout=timeout,
            )
        else:
            assert self._pool is not None
            client = self._pool.acquire()
            try:
                res = client.query(
                    compiled.sql,
                    compiled_params,
                    parameter_oids=parameter_oids,
                    timeout=timeout,
                )
            except BaseException:
                self._pool.evict(client)
                raise
            else:
                client.close()
        self._rows = [
            tuple(None if isinstance(cell, PgJson) and cell.value is None else cell for cell in row) for row in res.rows
        ]
        self._pos = 0
        self._description = (
            tuple((col.name, col.type_oid, None, None, None, None, None) for col in res.columns)
            if res.columns
            else None
        )
        self._rowcount = len(self._rows) if res.columns else int(res.affected_rows)
        return self

    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> FakeSyncPostgresCursor:
        self._ensure_open()
        total = 0
        for item in seq_of_params:
            self.execute(sql, item)
            total += max(self._rowcount, 0)
        self._rows = []
        self._pos = 0
        self._description = None
        self._rowcount = total
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        self._ensure_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        self._ensure_open()
        count = self._arraysize if size is None else size
        if count <= 0:
            return []
        end = min(self._pos + count, len(self._rows))
        batch = self._rows[self._pos : end]
        self._pos = end
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        self._ensure_open()
        remaining = self._rows[self._pos :]
        self._pos = len(self._rows)
        return remaining

    def close(self) -> None:
        self._closed = True
        self._rows.clear()
        self._pos = 0

    @property
    def description(self) -> tuple[tuple[str, int, None, None, None, None, None], ...] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> Any | None:
        return None

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value < 1:
            msg = "arraysize must be >= 1"
            raise ValueError(msg)
        self._arraysize = value


class FakeSyncPostgresClient(_BaseFakeSession):
    """Protocol-faithful synchronous ``SyncPostgresClient`` double."""

    def cursor(self, *, lazy_begin: bool = False) -> FakeSyncPostgresCursor:
        return FakeSyncPostgresCursor(client=self, lazy_begin=lazy_begin)

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run one statement and return its rows and column names."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return ([], ())
            return (cur.fetchall(), tuple(col[0] for col in cur.description))
        finally:
            cur.close()

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Run one statement and return one dictionary per row."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return []
            names = tuple(col[0] for col in cur.description)
            return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]
        finally:
            cur.close()

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run one statement and return the first column of the first row."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            row = cur.fetchone()
            if row is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            return row[0] if row else None
        finally:
            cur.close()

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Run one statement and return its row count and no last row id."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            return (max(cur.rowcount, 0), None)
        finally:
            cur.close()

    def attach_lease(self, release: Callable[[], None]) -> None:
        self.release_lease()
        self._lease_releaser = release

    def release_lease(self) -> None:
        releaser = self._lease_releaser
        self._lease_releaser = None
        if releaser is not None:
            releaser()

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self.release_lease()

    def connect(self) -> FakeSyncPostgresClient:
        self.closed = False
        return self

    def close(self) -> None:
        if self._release_hook is not None:
            self._release_hook(self)
            return
        self.closed = True

    def cancel(self) -> None:
        self.cancel_calls += 1

    def begin(self) -> None:
        self._begin_sync()

    def commit(self) -> None:
        self._commit_sync()

    def rollback(self) -> None:
        self._rollback_sync()

    def savepoint(self, token: int) -> None:
        self._savepoint_sync(token)

    def release_savepoint(self, token: int) -> None:
        self._release_savepoint_sync(token)

    def rollback_to_savepoint(self, token: int) -> None:
        self._rollback_to_savepoint_sync(token)

    def query(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> PostgresQueryResult:
        return self._query_sync(sql, parameters, parameter_oids=parameter_oids, timeout=timeout)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
        threshold_rows: int = 256,
        max_result_bytes: int = 268_435_456,
    ) -> PostgresArrowResult:
        _ = (parameter_oids, max_result_bytes)
        return self._select_to_arrow_sync(sql, parameters, threshold_rows=threshold_rows, timeout=timeout)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        batch_size: int = 65_536,
        columns: Sequence[str] | None = None,
    ) -> FakeSyncPostgresArrowStream:
        _ = parameter_oids
        res = self._query_sync(sql, parameters)
        cols = tuple(columns) if columns is not None else tuple(c.name for c in res.columns)
        return FakeSyncPostgresArrowStream(cols, res.rows, batch_size)

    def copy_to(self, sql: str) -> FakeSyncPostgresCopyOut:
        select_sql = sql.strip()
        if select_sql.upper().startswith("COPY (") and ") TO STDOUT" in select_sql.upper():
            inner_end = select_sql.upper().rfind(") TO STDOUT")
            select_sql = select_sql[6:inner_end]
        res = self._query_sync(select_sql)
        return FakeSyncPostgresCopyOut(res.rows)

    def copy_from(self, sql: str) -> FakeSyncPostgresCopyIn:
        self._ensure_not_closed()
        return FakeSyncPostgresCopyIn(self, sql)

    def copy_from_arrow(
        self,
        table_name: str,
        first_batch_ipc: Buffer,
        *,
        columns: Sequence[str] | None = None,
        schema_name: str | None = None,
    ) -> FakeSyncPostgresCopyIn:
        self._ensure_not_closed()
        target = f"{schema_name}.{table_name}" if schema_name else table_name
        copy_in = FakeSyncPostgresCopyIn(self, "", table_name=target, columns=columns)
        copy_in.write_arrow_ipc(first_batch_ipc)
        return copy_in

    def pipeline_begin(self) -> int:
        return self._pipeline_begin_sync()

    def pipeline_results(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> tuple[PostgresPipelineOutcome, ...]:
        return self._pipeline_results_sync(scope_id, operations)

    def pipeline_finish(self, scope_id: int, success: bool) -> None:
        self._pipeline_finish_sync(scope_id, success)


class FakeSyncPostgresPool:
    """Bounded synchronous ``SyncPostgresPool`` double tracking leases, idle state, and eviction."""

    def __init__(self, cluster: FakePostgresCluster | None = None, *, max_size: int = 4) -> None:
        self.cluster = cluster or FakePostgresCluster()
        self.max_size = max_size
        self.idle: list[FakeSyncPostgresClient] = []
        self.active: list[FakeSyncPostgresClient] = []
        self.evicted: list[FakeSyncPostgresClient] = []
        self.closed = False
        self.reset_count = 0

    def cursor(self, *, lazy_begin: bool = False) -> FakeSyncPostgresCursor:
        if self.closed:
            raise InterfaceError("FakeSyncPostgresPool is closed")
        if lazy_begin:
            raise InterfaceError("lazy_begin is not supported on a pooled cursor")
        return FakeSyncPostgresCursor(pool=self)

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run one statement and return its rows and column names."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return ([], ())
            return (cur.fetchall(), tuple(col[0] for col in cur.description))
        finally:
            cur.close()

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Run one statement and return one dictionary per row."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return []
            names = tuple(col[0] for col in cur.description)
            return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]
        finally:
            cur.close()

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run one statement and return the first column of the first row."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            row = cur.fetchone()
            if row is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            return row[0] if row else None
        finally:
            cur.close()

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Run one statement and return its row count and no last row id."""
        cur = self.cursor()
        try:
            cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            return (max(cur.rowcount, 0), None)
        finally:
            cur.close()

    def acquire(self) -> FakeSyncPostgresClient:
        if self.closed:
            raise InterfaceError("FakeSyncPostgresPool is closed")
        if self.idle:
            client = self.idle.pop()
            client.closed = False
            self.active.append(client)
            return client
        if len(self.active) >= self.max_size:
            raise PoolExhaustedError("FakeSyncPostgresPool capacity exhausted")
        client = FakeSyncPostgresClient(self.cluster, release_hook=self._release_client)
        self.active.append(client)
        return client

    def _release_client(self, client: FakeSyncPostgresClient) -> None:
        if client in self.active:
            self.active.remove(client)
        if self.closed or client.evicted:
            client.closed = True
            return
        client.reset_for_pool_reuse()
        self.reset_count += 1
        self.idle.append(client)

    def evict(self, client: FakeSyncPostgresClient) -> None:
        """Permanently discard a tainted connection from the pool."""
        client.evicted = True
        client.closed = True
        if client in self.active:
            self.active.remove(client)
        if client in self.idle:
            self.idle.remove(client)
        self.evicted.append(client)

    def close(self) -> None:
        self.closed = True
        for client in [*self.active, *self.idle]:
            client.closed = True
        self.active.clear()
        self.idle.clear()


class FakeAsyncPostgresCopyOut(FakeSyncPostgresCopyOut):
    """Asynchronous wrapper over ``FakeSyncPostgresCopyOut``."""

    def __aiter__(self) -> FakeAsyncPostgresCopyOut:
        return self

    async def __anext__(self) -> tuple[bytes | None, ...]:
        try:
            return self.__next__()
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self) -> None:
        self.close()

    async def __aenter__(self) -> FakeAsyncPostgresCopyOut:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = (exc_type, exc_val, exc_tb)
        await self.aclose()
        return False


class FakeAsyncPostgresCopyIn:
    """Asynchronous wrapper over ``FakeSyncPostgresCopyIn``."""

    def __init__(
        self,
        session: _BaseFakeSession,
        sql: str,
        *,
        table_name: str | None = None,
        columns: Sequence[str] | None = None,
    ) -> None:
        self._inner = FakeSyncPostgresCopyIn(session, sql, table_name=table_name, columns=columns)

    @property
    def closed(self) -> bool:
        return self._inner.closed

    @property
    def finished(self) -> bool:
        return self._inner.finished

    async def write(self, rows: Sequence[Sequence[Any]], column_oids: Sequence[int]) -> None:
        self._inner.write(rows, column_oids)

    async def write_arrow_ipc(self, batch_ipc: Buffer) -> None:
        self._inner.write_arrow_ipc(batch_ipc)

    async def finish(self) -> int:
        return self._inner.finish()

    async def aclose(self) -> None:
        self._inner.close()

    async def __aenter__(self) -> FakeAsyncPostgresCopyIn:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        _ = (exc_type, exc_val, exc_tb)
        await self.aclose()
        return False


class FakeAsyncPostgresArrowStream(FakeSyncPostgresArrowStream):
    """Asynchronous wrapper over ``FakeSyncPostgresArrowStream``."""

    def __aiter__(self) -> FakeAsyncPostgresArrowStream:
        return self

    async def __anext__(self) -> FakeNativeArrowCursor:
        try:
            return self.__next__()
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self) -> None:
        self.close()

    async def __aenter__(self) -> FakeAsyncPostgresArrowStream:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = (exc_type, exc_val, exc_tb)
        await self.aclose()
        return False


class FakeAsyncPostgresListener:
    """Dedicated fake ``AsyncPostgresListener`` matching ``_postgres.pyi`` signatures."""

    def __init__(
        self,
        cluster: FakePostgresCluster,
        channels: Sequence[str],
        *,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> None:
        self._cluster = cluster
        self.subscribed_channels: set[str] = set(channels)
        self.queue_capacity = queue_capacity
        self.reconnect = reconnect
        self._queue: asyncio.Queue[tuple[int, str, bytes, bool] | None] = asyncio.Queue(maxsize=max(1, queue_capacity))
        self.closed = False
        cluster.listeners.append(self)

    def deliver_if_subscribed(self, pid: int, channel: str, payload: bytes, *, reconnected_gap: bool = False) -> None:
        """Push notification frame into local async queue if channel matches active subscriptions."""
        if not self.closed and channel in self.subscribed_channels:
            with contextlib.suppress(asyncio.QueueFull):
                self._queue.put_nowait((pid, channel, payload, reconnected_gap))

    async def next(self) -> tuple[int, str, bytes, bool] | None:
        if self.closed and self._queue.empty():
            return None
        return await self._queue.get()

    async def subscribe(self, channels: Sequence[str]) -> None:
        if self.closed:
            raise InterfaceError("Fake listener is closed")
        self.subscribed_channels.update(channels)

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        if self.closed:
            raise InterfaceError("Fake listener is closed")
        self.subscribed_channels.difference_update(channels)

    async def aclose(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self in self._cluster.listeners:
            self._cluster.listeners.remove(self)
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(None)

    async def __aenter__(self) -> FakeAsyncPostgresListener:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        await self.aclose()


class FakeAsyncPostgresReplication:
    """Dedicated fake ``AsyncPostgresReplication`` stream over simulated cluster WAL frames."""

    def __init__(
        self,
        cluster: FakePostgresCluster,
        slot: str,
        publication: str,
        *,
        start_lsn: int | None = None,
        queue_capacity: int = 1024,
        reconnect: bool = True,
    ) -> None:
        self._cluster = cluster
        self.slot = slot
        self.publication = publication
        self.start_lsn = start_lsn
        self.queue_capacity = queue_capacity
        self.reconnect = reconnect
        self.closed = False
        self.acknowledged_lsns: list[int] = []
        self._last_acked = 0

    async def next(self) -> PostgresReplicationRecord | None:
        if self.closed:
            return None
        while not self.closed:
            with self._cluster._lock:
                if self._cluster.replication_records:
                    return self._cluster.replication_records.popleft()
            await asyncio.sleep(0)
            return None
        return None

    def __aiter__(self) -> FakeAsyncPostgresReplication:
        return self

    async def __anext__(self) -> PostgresReplicationRecord:
        item = await self.next()
        if item is None:
            raise StopAsyncIteration
        return item

    def acknowledge(self, commit_end_lsn: int) -> int:
        self._last_acked = max(self._last_acked, int(commit_end_lsn))
        self.acknowledged_lsns.append(self._last_acked)
        return self._last_acked

    async def aclose(self) -> None:
        self.closed = True

    async def __aenter__(self) -> FakeAsyncPostgresReplication:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        await self.aclose()


class FakeAsyncPostgresCursor:
    """Protocol-faithful asynchronous ``AsyncPostgresCursor`` double backed by a client or pool."""

    def __init__(
        self,
        *,
        client: FakeAsyncPostgresClient | None = None,
        pool: FakeAsyncPostgresPool | None = None,
        lazy_begin: bool = False,
    ) -> None:
        self._client = client
        self._pool = pool
        self._lazy_begin = lazy_begin
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0
        self._description: tuple[tuple[str, int, None, None, None, None, None], ...] | None = None
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "Cursor is closed"
            raise InterfaceError(msg)

    async def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> FakeAsyncPostgresCursor:
        self._ensure_open()
        compiled = compile_postgres_binds(sql, params)
        compiled_params = compiled.params if compiled.params else None
        if self._client is not None:
            if self._lazy_begin and not self._client.in_transaction:
                await self._client.begin()
            res = await self._client.query(
                compiled.sql,
                compiled_params,
                parameter_oids=parameter_oids,
                timeout=timeout,
            )
        else:
            assert self._pool is not None
            client = await self._pool.acquire()
            try:
                res = await client.query(
                    compiled.sql,
                    compiled_params,
                    parameter_oids=parameter_oids,
                    timeout=timeout,
                )
            except BaseException:
                await self._pool.evict(client)
                raise
            else:
                await client.close()
        self._rows = [
            tuple(None if isinstance(cell, PgJson) and cell.value is None else cell for cell in row) for row in res.rows
        ]
        self._pos = 0
        self._description = (
            tuple((col.name, col.type_oid, None, None, None, None, None) for col in res.columns)
            if res.columns
            else None
        )
        self._rowcount = len(self._rows) if res.columns else int(res.affected_rows)
        return self

    async def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> FakeAsyncPostgresCursor:
        self._ensure_open()
        total = 0
        for item in seq_of_params:
            await self.execute(sql, item)
            total += max(self._rowcount, 0)
        self._rows = []
        self._pos = 0
        self._description = None
        self._rowcount = total
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        self._ensure_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        self._ensure_open()
        count = self._arraysize if size is None else size
        if count <= 0:
            return []
        end = min(self._pos + count, len(self._rows))
        batch = self._rows[self._pos : end]
        self._pos = end
        return batch

    async def fetchall(self) -> list[tuple[Any, ...]]:
        self._ensure_open()
        remaining = self._rows[self._pos :]
        self._pos = len(self._rows)
        return remaining

    async def close(self) -> None:
        self._closed = True
        self._rows.clear()
        self._pos = 0

    @property
    def description(self) -> tuple[tuple[str, int, None, None, None, None, None], ...] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> Any | None:
        return None

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value < 1:
            msg = "arraysize must be >= 1"
            raise ValueError(msg)
        self._arraysize = value


class FakeAsyncPostgresClient(_BaseFakeSession):
    """Protocol-faithful asynchronous ``AsyncPostgresClient`` double."""

    def __init__(
        self,
        cluster: FakePostgresCluster,
        *,
        release_hook: Callable[[Any], None] | None = None,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(cluster, release_hook=release_hook)
        self.client_kwargs = dict(client_kwargs or {})

    def cursor(self, *, lazy_begin: bool = False) -> FakeAsyncPostgresCursor:
        return FakeAsyncPostgresCursor(client=self, lazy_begin=lazy_begin)

    async def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run one statement and return its rows and column names."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return ([], ())
            return (await cur.fetchall(), tuple(col[0] for col in cur.description))
        finally:
            await cur.close()

    async def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Run one statement and return one dictionary per row."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return []
            names = tuple(col[0] for col in cur.description)
            return [dict(zip(names, row, strict=False)) for row in await cur.fetchall()]
        finally:
            await cur.close()

    async def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run one statement and return the first column of the first row."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            row = await cur.fetchone()
            if row is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            return row[0] if row else None
        finally:
            await cur.close()

    async def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Run one statement and return its row count and no last row id."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            return (max(cur.rowcount, 0), None)
        finally:
            await cur.close()

    def attach_lease(self, release: Callable[[], None]) -> None:
        self.release_lease()
        self._lease_releaser = release

    def release_lease(self) -> None:
        releaser = self._lease_releaser
        self._lease_releaser = None
        if releaser is not None:
            releaser()

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self.release_lease()

    async def connect(self) -> FakeAsyncPostgresClient:
        self.closed = False
        return self

    async def close(self) -> None:
        if self._release_hook is not None:
            self._release_hook(self)
            return
        self.closed = True

    async def cancel(self) -> None:
        self.cancel_calls += 1

    async def begin(self) -> None:
        self._begin_sync()

    async def commit(self) -> None:
        self._commit_sync()

    async def rollback(self) -> None:
        self._rollback_sync()

    async def savepoint(self, token: int) -> None:
        self._savepoint_sync(token)

    async def release_savepoint(self, token: int) -> None:
        self._release_savepoint_sync(token)

    async def rollback_to_savepoint(self, token: int) -> None:
        self._rollback_to_savepoint_sync(token)

    async def query(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> PostgresQueryResult:
        if self.cluster.query_started_event is not None and self.cluster.query_release_event is not None:
            self.cluster.query_started_event.set()
            try:
                await self.cluster.query_release_event.wait()
            except asyncio.CancelledError:
                self.cancel_calls += 1
                raise
        return self._query_sync(sql, parameters, parameter_oids=parameter_oids, timeout=timeout)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
        threshold_rows: int = 256,
        max_result_bytes: int = 268_435_456,
    ) -> PostgresArrowResult:
        _ = (parameter_oids, max_result_bytes)
        return self._select_to_arrow_sync(sql, parameters, threshold_rows=threshold_rows, timeout=timeout)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        parameter_oids: Sequence[int] | None = None,
        batch_size: int = 65_536,
        columns: Sequence[str] | None = None,
    ) -> FakeAsyncPostgresArrowStream:
        _ = parameter_oids
        res = self._query_sync(sql, parameters)
        cols = tuple(columns) if columns is not None else tuple(c.name for c in res.columns)
        return FakeAsyncPostgresArrowStream(cols, res.rows, batch_size)

    async def copy_to(self, sql: str) -> FakeAsyncPostgresCopyOut:
        select_sql = sql.strip()
        if select_sql.upper().startswith("COPY (") and ") TO STDOUT" in select_sql.upper():
            inner_end = select_sql.upper().rfind(") TO STDOUT")
            select_sql = select_sql[6:inner_end]
        res = self._query_sync(select_sql)
        return FakeAsyncPostgresCopyOut(res.rows)

    async def copy_from(self, sql: str) -> FakeAsyncPostgresCopyIn:
        self._ensure_not_closed()
        return FakeAsyncPostgresCopyIn(self, sql)

    async def copy_from_arrow(
        self,
        table_name: str,
        first_batch_ipc: Buffer,
        *,
        columns: Sequence[str] | None = None,
        schema_name: str | None = None,
    ) -> FakeAsyncPostgresCopyIn:
        self._ensure_not_closed()
        target = f"{schema_name}.{table_name}" if schema_name else table_name
        copy_in = FakeAsyncPostgresCopyIn(self, "", table_name=target, columns=columns)
        await copy_in.write_arrow_ipc(first_batch_ipc)
        return copy_in

    async def pipeline_begin(self) -> int:
        return self._pipeline_begin_sync()

    async def pipeline_results(
        self,
        scope_id: int,
        operations: Sequence[PostgresPipelineOperation],
    ) -> tuple[PostgresPipelineOutcome, ...]:
        return self._pipeline_results_sync(scope_id, operations)

    async def pipeline_finish(self, scope_id: int, success: bool) -> None:
        self._pipeline_finish_sync(scope_id, success)

    async def open_listener(
        self,
        channels: Sequence[str],
        *,
        queue_capacity: int = 1024,
        reconnect: bool = False,
    ) -> FakeAsyncPostgresListener:
        self._ensure_not_closed()
        return FakeAsyncPostgresListener(
            self.cluster,
            channels,
            queue_capacity=queue_capacity,
            reconnect=reconnect,
        )

    async def replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity:
        self._ensure_not_closed()
        return self.cluster.get_replication_identity(slot, publication)

    async def open_replication(
        self,
        slot: str,
        publication: str,
        *,
        start_lsn: int | None = None,
        queue_capacity: int = 1024,
        reconnect: bool = True,
    ) -> FakeAsyncPostgresReplication:
        self._ensure_not_closed()
        return FakeAsyncPostgresReplication(
            self.cluster,
            slot,
            publication,
            start_lsn=start_lsn,
            queue_capacity=queue_capacity,
            reconnect=reconnect,
        )


class FakeAsyncPostgresPool:
    """Bounded asynchronous ``AsyncPostgresPool`` double tracking leases and eviction."""

    def __init__(self, cluster: FakePostgresCluster | None = None, *, max_size: int = 4) -> None:
        self.cluster = cluster or FakePostgresCluster()
        self.max_size = max_size
        self.idle: list[FakeAsyncPostgresClient] = []
        self.active: list[FakeAsyncPostgresClient] = []
        self.evicted: list[FakeAsyncPostgresClient] = []
        self.closed = False
        self.acquire_calls = 0
        self.reset_count = 0

    def cursor(self, *, lazy_begin: bool = False) -> FakeAsyncPostgresCursor:
        if self.closed:
            raise InterfaceError("FakeAsyncPostgresPool is closed")
        if lazy_begin:
            raise InterfaceError("lazy_begin is not supported on a pooled cursor")
        return FakeAsyncPostgresCursor(pool=self)

    async def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run one statement and return its rows and column names."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return ([], ())
            return (await cur.fetchall(), tuple(col[0] for col in cur.description))
        finally:
            await cur.close()

    async def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Run one statement and return one dictionary per row."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                return []
            names = tuple(col[0] for col in cur.description)
            return [dict(zip(names, row, strict=False)) for row in await cur.fetchall()]
        finally:
            await cur.close()

    async def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run one statement and return the first column of the first row."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            if cur.description is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            row = await cur.fetchone()
            if row is None:
                msg = "Query returned no rows"
                raise NotFoundError(msg)
            return row[0] if row else None
        finally:
            await cur.close()

    async def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Run one statement and return its row count and no last row id."""
        cur = self.cursor()
        try:
            await cur.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)
            return (max(cur.rowcount, 0), None)
        finally:
            await cur.close()

    async def acquire(self) -> FakeAsyncPostgresClient:
        if self.closed:
            raise InterfaceError("FakeAsyncPostgresPool is closed")
        self.acquire_calls += 1
        if self.idle:
            client = self.idle.pop()
            client.closed = False
            self.active.append(client)
            return client
        if len(self.active) >= self.max_size:
            raise PoolExhaustedError("FakeAsyncPostgresPool capacity exhausted")
        client = FakeAsyncPostgresClient(self.cluster, release_hook=self._release_client)
        self.active.append(client)
        return client

    def _release_client(self, client: FakeAsyncPostgresClient) -> None:
        if client in self.active:
            self.active.remove(client)
        if self.closed or client.evicted:
            client.closed = True
            return
        client.reset_for_pool_reuse()
        self.reset_count += 1
        self.idle.append(client)

    async def evict(self, client: FakeAsyncPostgresClient) -> None:
        """Permanently discard a tainted connection from the async pool."""
        client.evicted = True
        client.closed = True
        if client in self.active:
            self.active.remove(client)
        if client in self.idle:
            self.idle.remove(client)
        self.evicted.append(client)

    async def close(self) -> None:
        self.closed = True
        for client in [*self.active, *self.idle]:
            client.closed = True
        self.active.clear()
        self.idle.clear()


class FakeDirectClientFactory:
    """Recorder factory dialing dedicated non-pooled ``FakeAsyncPostgresClient`` instances."""

    def __init__(self, cluster: FakePostgresCluster) -> None:
        self.cluster = cluster
        self.opened_clients: list[FakeAsyncPostgresClient] = []
        self.dialed_kwargs: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> FakeAsyncPostgresClient:
        self.dialed_kwargs.append(dict(kwargs))
        client = FakeAsyncPostgresClient(self.cluster, release_hook=None, client_kwargs=kwargs)
        self.opened_clients.append(client)
        return client
