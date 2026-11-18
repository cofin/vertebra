"""Shared offline Oracle client, pool, driver, and notification test doubles."""

from __future__ import annotations

import asyncio
import contextlib
import io
import socket
import zipfile
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any, Final

import msgspec

import vertebra._oracle as _oracle
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import (
    InterfaceError,
    OperationalError,
    PoolExhaustedError,
)
from vertebra.db.rls import TenantScopeConfig

_MOCK_OAUTH_TOKEN: Final[str] = "mock-oauth-bearer-token"


class _EmployeeRow(msgspec.Struct, frozen=True, gc=False):
    """Typed row projection target for lowercase column hydration tests."""

    employee_id: int
    first_name: str


def _build_wallet_zip_bytes() -> bytes:
    """Build an in-memory Oracle Autonomous Database Cloud Wallet zip archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "tnsnames.ora",
            (
                "mydb_high = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(PORT=1522)"
                "(HOST=adb.us-ashburn-1.oraclecloud.com))"
                "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=g1_mydb_high.adb.oraclecloud.com))"
                '(SECURITY=(SSL_SERVER_CERT_DN="CN=adb.us-ashburn-1.oraclecloud.com")))\n'
            ),
        )
        zf.writestr("sqlnet.ora", "WALLET_LOCATION = (SOURCE = (METHOD = file))")
        zf.writestr(
            "ewallet.pem",
            b"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n",
        )
    return buf.getvalue()


def _reserve_closed_local_port() -> int:
    """Bind and close an ephemeral loopback socket so connect attempts fail deterministically."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def _make_arrow_cursor(
    rows: Sequence[tuple[Any, ...]],
    columns: Sequence[str],
    *,
    force_shm: bool | None,
) -> ArrowCursor:
    """Materialize a real inline or POSIX SHM `ArrowCursor` over tabular rows."""
    use_shm = bool(force_shm)
    threshold = 1 if use_shm else max(len(rows) + 10, 256)
    return ArrowCursor.from_rows(rows, columns, threshold_rows=threshold)


class _StubAsyncListener:
    """Protocol double matching `_oracle.AsyncOracleListener` for offline notification tests."""

    def __init__(
        self,
        queue_name: str,
        queue_store: dict[str, deque[_oracle.OracleNotification]],
        *,
        consumer_name: str | None = None,
        fail_mode: str | None = None,
    ) -> None:
        self.queue_name = queue_name
        self.consumer_name = consumer_name
        self.is_closed = False
        self.dropped_count = 0
        self.close_calls = 0
        self._queue_store = queue_store
        self._fail_mode = fail_mode

    async def next_notification(self, timeout: float | None = None) -> _oracle.OracleNotification | None:
        """Pop the next queued `OracleNotification` or inject a transient disconnect."""
        _ = timeout
        if self.is_closed:
            return None
        if self._fail_mode == "transient":
            err = OperationalError("ORA-03113: end-of-file on communication channel")
            err.sqlstate = "08006"
            err.vendor_code = 3113
            err.is_transient = True
            raise err
        bucket = self._queue_store.setdefault(self.queue_name, deque())
        if bucket:
            return bucket.popleft()
        await asyncio.sleep(0)
        return None

    async def close(self) -> None:
        """Mark the listener closed idempotently."""
        self.is_closed = True
        self.close_calls += 1


class _StubSyncOracleClient:
    """Protocol double matching `_oracle.SyncOracleClient` at the Python adapter boundary."""

    def __init__(
        self,
        *,
        cclass: str | None = None,
        purity: str = "SELF",
        connection_tag: str | None = None,
    ) -> None:
        self.closed: bool = False
        self.in_transaction: bool = False
        self.cclass: str | None = cclass
        self.purity: str = purity
        self.connection_tag: str | None = connection_tag
        self.tag_matched: bool = False
        self.calls: list[tuple[str, Any]] = []
        self.queued_results: deque[_oracle.OracleQueryResult | BaseException] = deque()
        self.queued_many_counts: deque[int] = deque()
        self.queued_arrow: deque[tuple[Sequence[tuple[Any, ...]], Sequence[str]]] = deque()
        self.queued_pipeline: deque[list[_oracle.OraclePipelineOutcome]] = deque()
        self.aq_queues: dict[str, deque[_oracle.OracleNotification]] = {}
        self._pending_attrs: dict[str, str | None] | None = None
        self._active_attrs: dict[str, str | None] = {
            "client_identifier": None,
            "module": None,
            "action": None,
            "ecid": None,
        }
        self._last_piggyback_frame: bytes | None = None
        self._msg_seq: int = 0

    @property
    def pending_session_attrs(self) -> dict[str, str | None] | None:
        """Return staged TTC session attributes awaiting the next round trip."""
        return dict(self._pending_attrs) if self._pending_attrs is not None else None

    @property
    def active_session_attrs(self) -> dict[str, str | None]:
        """Return active TTC session attributes flushed to the session."""
        return dict(self._active_attrs)

    @property
    def last_piggyback_frame(self) -> bytes | None:
        """Return the most recently encoded `[0x11, 0x6e]` piggyback frame."""
        return self._last_piggyback_frame

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Coalesce session attributes for the next statement flush."""
        base = dict(
            self._pending_attrs
            or {
                "client_identifier": None,
                "module": None,
                "action": None,
                "ecid": None,
            }
        )
        if client_identifier is not None:
            base["client_identifier"] = client_identifier
        if module is not None:
            base["module"] = module
        if action is not None:
            base["action"] = action
        if ecid is not None:
            base["ecid"] = ecid
        self._pending_attrs = base

    def connect(self) -> None:
        """Record explicit connection validation."""
        self._ensure_open()
        self.calls.append(("connect", None))

    def ping(self) -> None:
        """Record session liveness check."""
        self._ensure_open()
        self._flush_piggyback()
        self.calls.append(("ping", None))

    def cancel(self) -> None:
        """Record out-of-band cancellation request."""
        self.calls.append(("cancel", None))

    def close(self) -> None:
        """Close the client session idempotently."""
        self.closed = True
        self.in_transaction = False
        self.calls.append(("close", None))

    def begin(self) -> None:
        """Open a local transaction marker."""
        self._ensure_open()
        self.in_transaction = True
        self.calls.append(("begin", None))

    def commit(self) -> None:
        """Commit the active transaction."""
        self._ensure_open()
        self.in_transaction = False
        self.calls.append(("commit", None))

    def rollback(self) -> None:
        """Roll back the active transaction."""
        self._ensure_open()
        self.in_transaction = False
        self.calls.append(("rollback", None))

    def savepoint(self, name: str) -> None:
        """Record creation of savepoint `name`."""
        self._ensure_open()
        self._flush_piggyback()
        self.in_transaction = True
        self.calls.append(("savepoint", name))

    def rollback_to_savepoint(self, name: str) -> None:
        """Record rollback to savepoint `name`."""
        self._ensure_open()
        self.in_transaction = True
        self.calls.append(("rollback_to_savepoint", name))

    def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Any = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> _oracle.OracleQueryResult:
        """Record a `query()` dispatch and pop the next configured `OracleQueryResult`."""
        self._ensure_open()
        self._flush_piggyback()
        self.calls.append(
            (
                "query",
                {
                    "sql": sql,
                    "params": params,
                    "out_binds": out_binds,
                    "fetch_decimals": fetch_decimals,
                    "enable_lowercase_column_names": enable_lowercase_column_names,
                },
            )
        )
        return self._pop_result(enable_lowercase_column_names)

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Any = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> _oracle.OracleQueryResult:
        """Record an `execute()` dispatch and pop the next configured `OracleQueryResult`."""
        self._ensure_open()
        self._flush_piggyback()
        self.calls.append(
            (
                "execute",
                {
                    "sql": sql,
                    "params": params,
                    "out_binds": out_binds,
                    "fetch_decimals": fetch_decimals,
                    "enable_lowercase_column_names": enable_lowercase_column_names,
                },
            )
        )
        return self._pop_result(enable_lowercase_column_names)

    def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Record a batched `execute_many()` call and return affected row count."""
        self._ensure_open()
        self._flush_piggyback()
        rows_list = list(params_seq)
        self.calls.append(("execute_many", {"sql": sql, "params_seq": rows_list}))
        if self.queued_many_counts:
            return self.queued_many_counts.popleft()
        return len(rows_list)

    def query_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        force_shm: bool | None = None,
        enable_lowercase_column_names: bool | None = None,
    ) -> ArrowCursor:
        """Record a `query_arrow()` call and build a real inline or SHM `ArrowCursor`."""
        self._ensure_open()
        self._flush_piggyback()
        self.calls.append(
            (
                "query_arrow",
                {
                    "sql": sql,
                    "params": params,
                    "batch_size": batch_size,
                    "force_shm": force_shm,
                    "enable_lowercase_column_names": enable_lowercase_column_names,
                },
            )
        )
        rows, raw_cols = self.queued_arrow.popleft() if self.queued_arrow else ((), ())
        lowercase = True if enable_lowercase_column_names is None else enable_lowercase_column_names
        cols = [c.lower() if lowercase else c.upper() for c in raw_cols]
        return _make_arrow_cursor(rows, cols, force_shm=force_shm)

    def execute_pipeline_batch(
        self,
        operations: Sequence[tuple[Any, ...]],
        *,
        continue_on_error: bool = False,
    ) -> list[_oracle.OraclePipelineOutcome]:
        """Record a single worker-batch pipeline flush and return configured step outcomes."""
        self._ensure_open()
        self._flush_piggyback()
        ops = list(operations)
        self.calls.append(("execute_pipeline_batch", {"operations": ops, "continue_on_error": continue_on_error}))
        if self.queued_pipeline:
            return self.queued_pipeline.popleft()
        return [_oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)) for _ in ops]

    def enqueue_notification(
        self,
        *,
        queue_name: str,
        payload: bytes,
        consumer_name: str | None = None,
        correlation_id: str | None = None,
    ) -> bytes:
        """Record an AQ notification enqueue and push to the in-process test queue map."""
        self._ensure_open()
        self._msg_seq += 1
        msg_id = f"MSG{self._msg_seq:04d}".encode("ascii")
        self.calls.append(
            (
                "enqueue_notification",
                {
                    "queue_name": queue_name,
                    "payload": bytes(payload),
                    "consumer_name": consumer_name,
                    "correlation_id": correlation_id,
                },
            )
        )
        frame = _oracle.OracleNotification(queue_name, msg_id, bytes(payload), consumer_name, 1700000000000)
        self.aq_queues.setdefault(queue_name, deque()).append(frame)
        return msg_id

    def _ensure_open(self) -> None:
        if self.closed:
            raise InterfaceError("Oracle client is closed")

    def _flush_piggyback(self) -> None:
        if self._pending_attrs is None:
            return
        staged = self._pending_attrs
        self._pending_attrs = None
        if any(staged.values()):
            frame = OracleCore.encode_ttc_session_attrs(
                client_identifier=staged["client_identifier"],
                module=staged["module"],
                action=staged["action"],
                ecid=staged["ecid"],
            )
            self._last_piggyback_frame = frame
            for k, v in staged.items():
                if v is not None:
                    self._active_attrs[k] = v

    def _pop_result(self, enable_lowercase: bool | None) -> _oracle.OracleQueryResult:
        if not self.queued_results:
            return _oracle.OracleQueryResult([], [], rowcount=1)
        nxt = self.queued_results.popleft()
        if isinstance(nxt, BaseException):
            raise nxt
        if enable_lowercase is None:
            return nxt
        cols = [
            _oracle.OracleColumn(
                c.name.lower() if enable_lowercase else c.name.upper(),
                c.type_name,
                c.type_code,
                c.db_size,
                c.precision,
                c.scale,
                c.nullable,
            )
            for c in nxt.columns
        ]
        return _oracle.OracleQueryResult(
            cols,
            nxt.rows,
            rowcount=nxt.rowcount,
            out_binds=nxt.out_binds,
            lastrowid=nxt.lastrowid,
        )


class _StubAsyncOracleClient:
    """Protocol double matching `_oracle.AsyncOracleClient` via composition around `_StubSyncOracleClient`."""

    def __init__(
        self,
        *,
        cclass: str | None = None,
        purity: str = "SELF",
        connection_tag: str | None = None,
        listener_fail_modes: Sequence[str | None] = (),
    ) -> None:
        self._sync = _StubSyncOracleClient(
            cclass=cclass,
            purity=purity,
            connection_tag=connection_tag,
        )
        self._listener_fail_modes: deque[str | None] = deque(listener_fail_modes)
        self.created_listeners: list[_StubAsyncListener] = []

    @property
    def closed(self) -> bool:
        """Return `True` when the session is closed."""
        return self._sync.closed

    @property
    def in_transaction(self) -> bool:
        """Return `True` when a transaction is open."""
        return self._sync.in_transaction

    @property
    def cclass(self) -> str | None:
        """Return the configured DRCP connection class."""
        return self._sync.cclass

    @property
    def purity(self) -> str:
        """Return the session DRCP purity."""
        return self._sync.purity

    @purity.setter
    def purity(self, value: str) -> None:
        self._sync.purity = value

    @property
    def connection_tag(self) -> str | None:
        """Return the session DRCP connection tag."""
        return self._sync.connection_tag

    @connection_tag.setter
    def connection_tag(self, value: str | None) -> None:
        self._sync.connection_tag = value

    @property
    def tag_matched(self) -> bool:
        """Return whether checkout matched a pooled tag."""
        return self._sync.tag_matched

    @tag_matched.setter
    def tag_matched(self, value: bool) -> None:
        self._sync.tag_matched = value

    @property
    def calls(self) -> list[tuple[str, Any]]:
        """Return recorded client operations."""
        return self._sync.calls

    @property
    def queued_results(self) -> deque[_oracle.OracleQueryResult | BaseException]:
        """Return queued `OracleQueryResult` fixtures."""
        return self._sync.queued_results

    @property
    def queued_arrow(self) -> deque[tuple[Sequence[tuple[Any, ...]], Sequence[str]]]:
        """Return queued `ArrowCursor` row/column fixtures."""
        return self._sync.queued_arrow

    @property
    def queued_pipeline(self) -> deque[list[_oracle.OraclePipelineOutcome]]:
        """Return queued pipeline outcome lists."""
        return self._sync.queued_pipeline

    @property
    def aq_queues(self) -> dict[str, deque[_oracle.OracleNotification]]:
        """Return the in-process AQ notification queue store."""
        return self._sync.aq_queues

    @property
    def pending_session_attrs(self) -> dict[str, str | None] | None:
        """Return staged TTC session attributes."""
        return self._sync.pending_session_attrs

    @property
    def active_session_attrs(self) -> dict[str, str | None]:
        """Return active TTC session attributes."""
        return self._sync.active_session_attrs

    @property
    def last_piggyback_frame(self) -> bytes | None:
        """Return the last encoded TTC piggyback frame."""
        return self._sync.last_piggyback_frame

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Coalesce session attributes onto the underlying sync recorder."""
        self._sync.set_session_attrs(
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    def cancel(self) -> None:
        """Record an out-of-band cancellation signal."""
        self._sync.cancel()

    async def connect(self) -> None:
        """Awaitable `connect()` forwarding to sync recorder."""
        self._sync.connect()

    async def ping(self) -> None:
        """Awaitable `ping()` forwarding to sync recorder."""
        self._sync.ping()

    async def close(self) -> None:
        """Awaitable `close()` forwarding to sync recorder."""
        self._sync.close()

    async def begin(self) -> None:
        """Awaitable `begin()` forwarding to sync recorder."""
        self._sync.begin()

    async def commit(self) -> None:
        """Awaitable `commit()` forwarding to sync recorder."""
        self._sync.commit()

    async def rollback(self) -> None:
        """Awaitable `rollback()` forwarding to sync recorder."""
        self._sync.rollback()

    async def savepoint(self, name: str) -> None:
        """Awaitable `savepoint()` forwarding to sync recorder."""
        self._sync.savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Awaitable `rollback_to_savepoint()` forwarding to sync recorder."""
        self._sync.rollback_to_savepoint(name)

    async def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Any = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> _oracle.OracleQueryResult:
        """Awaitable `query()` yielding once to exercise task scheduling."""
        await asyncio.sleep(0)
        return self._sync.query(
            sql,
            params,
            out_binds=out_binds,
            fetch_decimals=fetch_decimals,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )

    async def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Any = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> _oracle.OracleQueryResult:
        """Awaitable `execute()` yielding once to exercise task scheduling."""
        await asyncio.sleep(0)
        return self._sync.execute(
            sql,
            params,
            out_binds=out_binds,
            fetch_decimals=fetch_decimals,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )

    async def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Awaitable `execute_many()` forwarding to sync recorder."""
        await asyncio.sleep(0)
        return self._sync.execute_many(sql, params_seq)

    async def query_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        force_shm: bool | None = None,
        enable_lowercase_column_names: bool | None = None,
    ) -> ArrowCursor:
        """Awaitable `query_arrow()` forwarding to sync recorder."""
        await asyncio.sleep(0)
        return self._sync.query_arrow(
            sql,
            params,
            batch_size=batch_size,
            force_shm=force_shm,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )

    async def execute_pipeline_batch(
        self,
        operations: Sequence[tuple[Any, ...]],
        *,
        continue_on_error: bool = False,
    ) -> list[_oracle.OraclePipelineOutcome]:
        """Awaitable `execute_pipeline_batch()` forwarding to sync recorder."""
        await asyncio.sleep(0)
        return self._sync.execute_pipeline_batch(operations, continue_on_error=continue_on_error)

    async def enqueue_notification(
        self,
        *,
        queue_name: str,
        payload: bytes,
        consumer_name: str | None = None,
        correlation_id: str | None = None,
    ) -> bytes:
        """Awaitable `enqueue_notification()` forwarding to sync recorder."""
        await asyncio.sleep(0)
        return self._sync.enqueue_notification(
            queue_name=queue_name,
            payload=payload,
            consumer_name=consumer_name,
            correlation_id=correlation_id,
        )

    async def open_listener(
        self,
        *,
        queue_name: str,
        consumer_name: str | None = None,
        wait_timeout_seconds: int = 5,
        max_buffered: int = 1024,
    ) -> _StubAsyncListener:
        """Open a `_StubAsyncListener` attached to `self.aq_queues`."""
        _ = (wait_timeout_seconds, max_buffered)
        if self._sync.closed:
            raise InterfaceError("Oracle client is closed")
        mode = self._listener_fail_modes.popleft() if self._listener_fail_modes else None
        listener = _StubAsyncListener(
            queue_name,
            self._sync.aq_queues,
            consumer_name=consumer_name,
            fail_mode=mode,
        )
        self.created_listeners.append(listener)
        return listener


class _StubSyncOraclePool:
    """Protocol double matching `_oracle.SyncOraclePool` for offline facade and DRCP tests."""

    def __init__(
        self,
        *,
        min_size: int = 0,
        max_size: int = 2,
        cclass: str | None = None,
        purity: str = "SELF",
    ) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self.closed = False
        self._cclass = cclass
        self._default_purity = purity
        self._idle: list[_StubSyncOracleClient] = []
        self._borrowed: list[_StubSyncOracleClient] = []
        self.created_clients: list[_StubSyncOracleClient] = []

    @property
    def size(self) -> int:
        """Total open sessions tracked by the pool."""
        return len(self._idle) + len(self._borrowed)

    @property
    def idle_size(self) -> int:
        """Idle sessions available for checkout."""
        return len(self._idle)

    @property
    def borrowed_size(self) -> int:
        """Borrowed sessions checked out by callers."""
        return len(self._borrowed)

    def stats(self) -> dict[str, int]:
        """Return pool gauge snapshot."""
        return {
            "min_size": self.min_size,
            "max_size": self.max_size,
            "size": self.size,
            "idle_size": self.idle_size,
            "borrowed_size": self.borrowed_size,
        }

    def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> _StubSyncOracleClient:
        """Acquire an idle or newly created `_StubSyncOracleClient`."""
        _ = timeout
        if self.closed:
            raise InterfaceError("Oracle pool is closed")
        eff_purity = (purity or self._default_purity).upper()
        if eff_purity not in ("SELF", "NEW"):
            raise InterfaceError(f"Unsupported DRCP purity {purity!r}")

        chosen: _StubSyncOracleClient | None = None
        if eff_purity == "NEW":
            if self._idle:
                evicted = self._idle.pop(0)
                evicted.close()
            elif self.size >= self.max_size:
                self._raise_exhausted()
            chosen = _StubSyncOracleClient(cclass=self._cclass, purity="NEW", connection_tag=None)
            chosen.tag_matched = False
            self.created_clients.append(chosen)
        else:
            if connection_tag is not None:
                for idx, item in enumerate(self._idle):
                    if item.connection_tag == connection_tag:
                        chosen = self._idle.pop(idx)
                        chosen.purity = "SELF"
                        chosen.tag_matched = True
                        break
            if chosen is None and self._idle:
                chosen = self._idle.pop()
                chosen.purity = "SELF"
                chosen.tag_matched = False
            if chosen is None:
                if self.size >= self.max_size:
                    self._raise_exhausted()
                chosen = _StubSyncOracleClient(
                    cclass=self._cclass,
                    purity="SELF",
                    connection_tag=connection_tag,
                )
                chosen.tag_matched = False
                self.created_clients.append(chosen)

        if any((client_identifier, module, action, ecid)):
            chosen.set_session_attrs(
                client_identifier=client_identifier,
                module=module,
                action=action,
                ecid=ecid,
            )
        self._borrowed.append(chosen)
        return chosen

    def acquire_from_pool(self, **kwargs: Any) -> _StubSyncOracleClient:
        """Alias matching `_oracle.SyncOraclePool.acquire_from_pool`."""
        return self.acquire(**kwargs)

    def release(
        self,
        client: _StubSyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None:
        """Roll back dirty transactions and return or discard `client`."""
        if client in self._borrowed:
            self._borrowed.remove(client)
        if discard or client.closed or self.closed:
            client.close()
            return
        if client.in_transaction:
            client.rollback()
        if connection_tag is not None:
            client.connection_tag = connection_tag
        self._idle.append(client)

    def release_to_pool(self, client: _StubSyncOracleClient, **kwargs: Any) -> None:
        """Alias matching `_oracle.SyncOraclePool.release_to_pool`."""
        self.release(client, **kwargs)

    def evict(self, client: _StubSyncOracleClient) -> None:
        """Permanently close and remove `client` from the pool."""
        if client in self._borrowed:
            self._borrowed.remove(client)
        if client in self._idle:
            self._idle.remove(client)
        client.close()

    def close(self) -> None:
        """Close the pool and all tracked idle sessions."""
        self.closed = True
        while self._idle:
            self._idle.pop().close()

    @staticmethod
    def _raise_exhausted() -> None:
        err = PoolExhaustedError("Oracle connection pool exhausted")
        err.sqlstate = "53300"
        err.is_transient = True
        raise err


class _StubAsyncOraclePool:
    """Protocol double matching `_oracle.AsyncOraclePool` around `_StubAsyncOracleClient`."""

    def __init__(
        self,
        *,
        min_size: int = 0,
        max_size: int = 2,
        cclass: str | None = None,
        purity: str = "SELF",
    ) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self.closed = False
        self._cclass = cclass
        self._default_purity = purity
        self._idle: list[_StubAsyncOracleClient] = []
        self._borrowed: list[_StubAsyncOracleClient] = []
        self.created_clients: list[_StubAsyncOracleClient] = []

    @property
    def size(self) -> int:
        """Total open sessions tracked by the async pool."""
        return len(self._idle) + len(self._borrowed)

    @property
    def idle_size(self) -> int:
        """Idle async sessions available for checkout."""
        return len(self._idle)

    @property
    def borrowed_size(self) -> int:
        """Borrowed async sessions checked out by callers."""
        return len(self._borrowed)

    def stats(self) -> dict[str, int]:
        """Return pool gauge snapshot."""
        return {
            "min_size": self.min_size,
            "max_size": self.max_size,
            "size": self.size,
            "idle_size": self.idle_size,
            "borrowed_size": self.borrowed_size,
        }

    async def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> _StubAsyncOracleClient:
        """Acquire an idle or newly created `_StubAsyncOracleClient`."""
        _ = timeout
        await asyncio.sleep(0)
        if self.closed:
            raise InterfaceError("Oracle pool is closed")
        eff_purity = (purity or self._default_purity).upper()
        if eff_purity not in ("SELF", "NEW"):
            raise InterfaceError(f"Unsupported DRCP purity {purity!r}")

        chosen: _StubAsyncOracleClient | None = None
        if eff_purity == "NEW":
            if self._idle:
                await self._idle.pop(0).close()
            elif self.size >= self.max_size:
                _StubSyncOraclePool._raise_exhausted()
            chosen = _StubAsyncOracleClient(cclass=self._cclass, purity="NEW", connection_tag=None)
            chosen.tag_matched = False
            self.created_clients.append(chosen)
        else:
            if connection_tag is not None:
                for idx, item in enumerate(self._idle):
                    if item.connection_tag == connection_tag:
                        chosen = self._idle.pop(idx)
                        chosen.purity = "SELF"
                        chosen.tag_matched = True
                        break
            if chosen is None and self._idle:
                chosen = self._idle.pop()
                chosen.purity = "SELF"
                chosen.tag_matched = False
            if chosen is None:
                if self.size >= self.max_size:
                    _StubSyncOraclePool._raise_exhausted()
                chosen = _StubAsyncOracleClient(
                    cclass=self._cclass,
                    purity="SELF",
                    connection_tag=connection_tag,
                )
                chosen.tag_matched = False
                self.created_clients.append(chosen)

        if any((client_identifier, module, action, ecid)):
            chosen.set_session_attrs(
                client_identifier=client_identifier,
                module=module,
                action=action,
                ecid=ecid,
            )
        self._borrowed.append(chosen)
        return chosen

    async def acquire_from_pool(self, **kwargs: Any) -> _StubAsyncOracleClient:
        """Alias matching `_oracle.AsyncOraclePool.acquire_from_pool`."""
        return await self.acquire(**kwargs)

    async def release(
        self,
        client: _StubAsyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None:
        """Roll back dirty transactions and return or discard `client`."""
        await asyncio.sleep(0)
        if client in self._borrowed:
            self._borrowed.remove(client)
        if discard or client.closed or self.closed:
            await client.close()
            return
        if client.in_transaction:
            await client.rollback()
        if connection_tag is not None:
            client.connection_tag = connection_tag
        self._idle.append(client)

    async def release_to_pool(self, client: _StubAsyncOracleClient, **kwargs: Any) -> None:
        """Alias matching `_oracle.AsyncOraclePool.release_to_pool`."""
        await self.release(client, **kwargs)

    async def evict(self, client: _StubAsyncOracleClient) -> None:
        """Permanently close and remove `client` from the async pool."""
        if client in self._borrowed:
            self._borrowed.remove(client)
        if client in self._idle:
            self._idle.remove(client)
        await client.close()

    async def close(self) -> None:
        """Close the async pool and all tracked idle sessions."""
        self.closed = True
        while self._idle:
            await self._idle.pop().close()


class _SyncDriverDouble:
    """Minimal synchronous driver double for `TenantConnectionScope` reset/evict verification."""

    def __init__(self, *, fail_reset: bool = False) -> None:
        self.fail_reset = fail_reset
        self.events: list[tuple[str, Any]] = []
        self.evicted = False

    def enter_tenant_scope(self, tenant_value: str, config: TenantScopeConfig) -> None:
        sql, params = OracleCore.build_session_context_sql(tenant_value, config)
        self.events.append(("enter", (sql, params)))

    def exit_tenant_scope(self, config: TenantScopeConfig) -> None:
        sql, params = OracleCore.build_clear_context_sql(config)
        self.events.append(("exit", (sql, params)))
        if self.fail_reset:
            err = OperationalError("ORA-03113: end-of-file on communication channel")
            err.sqlstate = "08006"
            err.vendor_code = 3113
            raise err

    def evict_connection(self) -> None:
        self.evicted = True
        self.events.append(("evict", None))


class _AsyncDriverDouble:
    """Minimal asynchronous driver double for `AsyncTenantConnectionScope` reset/evict verification."""

    def __init__(self, *, fail_reset: bool = False) -> None:
        self.fail_reset = fail_reset
        self.events: list[tuple[str, Any]] = []
        self.evicted = False

    async def enter_tenant_scope(self, tenant_value: str, config: TenantScopeConfig) -> None:
        sql, params = OracleCore.build_session_context_sql(tenant_value, config)
        self.events.append(("enter", (sql, params)))

    async def exit_tenant_scope(self, config: TenantScopeConfig) -> None:
        sql, params = OracleCore.build_clear_context_sql(config)
        self.events.append(("exit", (sql, params)))
        if self.fail_reset:
            err = OperationalError("ORA-03113: end-of-file on communication channel")
            err.sqlstate = "08006"
            err.vendor_code = 3113
            raise err

    async def evict_connection(self) -> None:
        self.evicted = True
        self.events.append(("evict", None))


class _FakeSyncOracleDriver:
    """Synchronous Oracle driver fake for queue store testing."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.select_rows: list[dict[str, Any]] = []
        self.affected_rows: int = 1

    def execute(self, sql: str, parameters: Mapping[str, Any] | None = None) -> int:
        self.executed.append((sql, parameters))
        return self.affected_rows

    def select(self, sql: str, parameters: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        self.executed.append((sql, parameters))
        return list(self.select_rows)

    def select_one_or_none(self, sql: str, parameters: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
        self.executed.append((sql, parameters))
        return self.select_rows[0] if self.select_rows else None

    def in_transaction(self) -> bool:
        return False

    @contextlib.contextmanager
    def transaction(self) -> Any:
        yield self


class _FakeAsyncOracleDriver:
    """Asynchronous Oracle driver fake for queue store testing."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.select_rows: list[dict[str, Any]] = []
        self.affected_rows: int = 1

    async def execute(self, sql: str, parameters: Mapping[str, Any] | None = None) -> int:
        self.executed.append((sql, parameters))
        return self.affected_rows

    async def select(self, sql: str, parameters: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        self.executed.append((sql, parameters))
        return list(self.select_rows)

    async def select_one_or_none(self, sql: str, parameters: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
        self.executed.append((sql, parameters))
        return self.select_rows[0] if self.select_rows else None

    def in_transaction(self) -> bool:
        return False

    @contextlib.asynccontextmanager
    async def transaction(self) -> Any:
        yield self


class _FakeSyncOracleNotifySource:
    """Fake synchronous Oracle notification source capturing published events."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    def publish(self, channel: str, payload: str) -> None:
        self.published.append((channel, payload))


class _FakeAsyncOracleNotifySource:
    """Fake asynchronous Oracle notification source capturing published events."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    async def publish(self, channel: str, payload: str) -> None:
        self.published.append((channel, payload))
