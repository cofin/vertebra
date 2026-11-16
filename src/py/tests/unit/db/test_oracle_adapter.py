"""Offline unit tests for the native Oracle adapter, dialer, codecs, pools, pipelines, notifications, and tenancy."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import io
import socket
import sys
import time
import uuid
import zipfile
from collections import deque
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final

import msgspec
import pyarrow as pa
import pytest
from sqlspec import StatementStack
from sqlspec.core.parameters import ParameterStyle

import vertebra._oracle as _oracle
import vertebra.db.adapters.oracle as oracle_pkg
import vertebra.db.adapters.oracle.core as oracle_core_mod
from vertebra._vertebra import VectorBatchView, VectorView
from vertebra.db.adapters.oracle import (
    AsyncOracleDriver,
    AsyncOraclePipeline,
    AsyncOraclePool,
    OracleEmbeddingPushdown,
    SyncOracleDriver,
    SyncOraclePipeline,
    SyncOraclePool,
)
from vertebra.db.adapters.oracle.config import (
    DEFAULT_ORACLE_DSN,
    OracleConfig,
    OracleConnectDescriptor,
    OracleDrcpConfig,
    OracleDriverFeatures,
    OracleSessionAttrs,
    OracleTopologyConfig,
    OracleWallet,
)
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.adapters.oracle.notify import (
    AsyncOracleNotificationSource,
    OracleNotifyConfig,
    validate_oracle_channel_name,
)
from vertebra.db.arrow import ArrowCursor
from vertebra.db.cdc import ChangeFeed
from vertebra.db.driver import (
    ConnectionFailure,
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    PoolExhaustedError,
    ProgrammingError,
    QueryCancelledError,
)
from vertebra.db.driver.capabilities import (
    AsyncNotificationSource,
    AsyncPipelineCapable,
    SyncPipelineCapable,
)
from vertebra.db.rls import AsyncTenantConnectionScope, TenantConnectionScope, TenantPredicate, TenantScopeConfig
from vertebra.extensions.sqlspec.adapters import OracleAsyncDriver, OracleSyncDriver
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from pathlib import Path

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


@pytest.mark.parametrize(
    ("kwargs", "expected_host", "expected_port", "expected_service", "expected_sid", "expected_protocol"),
    [
        (
            {},
            "localhost",
            1521,
            "FREEPDB1",
            None,
            "TCP",
        ),
        (
            {"host": "db.internal", "port": 1530, "service_name": "ORCLPDB1"},
            "db.internal",
            1530,
            "ORCLPDB1",
            None,
            "TCP",
        ),
        (
            {"host": "legacy.internal", "port": 1521, "sid": "ORCLSID"},
            "legacy.internal",
            1521,
            None,
            "ORCLSID",
            "TCP",
        ),
        (
            {"dsn": "oracle://rac.internal:1525/PROD"},
            "rac.internal",
            1525,
            "PROD",
            None,
            "TCP",
        ),
        (
            {
                "connect_descriptor_str": (
                    "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=explicit.internal)(PORT=1522))"
                    "(CONNECT_DATA=(SERVICE_NAME=EXPLICIT_SVC)))"
                ),
                "dsn": "oracle://rac.internal:1525/PROD",
                "host": "db.internal",
            },
            "explicit.internal",
            1522,
            "EXPLICIT_SVC",
            None,
            "TCPS",
        ),
    ],
)
def test_oracle_config_connect_descriptor_precedence(
    kwargs: dict[str, Any],
    expected_host: str,
    expected_port: int,
    expected_service: str | None,
    expected_sid: str | None,
    expected_protocol: str,
) -> None:
    """Verify descriptor precedence in `OracleConfig.connect_descriptor()`."""
    cfg = OracleConfig(**kwargs)
    assert DEFAULT_ORACLE_DSN == "oracle://localhost:1521/FREEPDB1"
    desc = cfg.connect_descriptor()
    assert isinstance(desc, OracleConnectDescriptor)
    assert desc.host == expected_host
    assert desc.port == expected_port
    assert desc.service_name == expected_service
    assert desc.sid == expected_sid
    assert desc.protocol == expected_protocol


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"host": "memory"},
        {"host": ":memory:"},
        {"dsn": ":memory:"},
        {"dsn": "memory://"},
        {"dsn": "oracle://:memory:"},
        {"dsn": "oracle://memory/FREEPDB1"},
        {"connect_descriptor_str": "memory"},
    ],
)
def test_oracle_rejects_synthetic_memory_endpoints_across_config_and_native_types(
    bad_kwargs: dict[str, Any],
) -> None:
    """Verify Python `OracleConfig` and Rust `_oracle` types reject synthetic memory endpoints."""
    with pytest.raises(ImproperConfigurationError):
        OracleConfig(**bad_kwargs)

    native_kwargs = dict(bad_kwargs)
    if "connect_descriptor_str" in native_kwargs:
        native_kwargs["connect_descriptor"] = native_kwargs.pop("connect_descriptor_str")
    with pytest.raises(InterfaceError):
        _oracle.SyncOracleClient(**native_kwargs)
    with pytest.raises(InterfaceError):
        _oracle.AsyncOracleClient(**native_kwargs)
    with pytest.raises(InterfaceError):
        _oracle.SyncOraclePool(min_size=0, max_size=1, **native_kwargs)
    with pytest.raises(InterfaceError):
        _oracle.AsyncOraclePool(min_size=0, max_size=1, **native_kwargs)


def test_oracle_config_topology_and_wallet_resolution(tmp_path: Path) -> None:
    """Verify `OracleTopologyConfig.to_tns_descriptor()` and `OracleWallet` alias resolution."""
    topo_cfg = OracleConfig(
        topology=OracleTopologyConfig(
            hosts=("rac1.internal", "rac2.internal"),
            port=1522,
            service_name="RAC_SVC",
            protocol="TCPS",
        ),
        host="ignored.internal",
    )
    topo_desc = topo_cfg.connect_descriptor()
    assert topo_desc.host == "rac1.internal"
    assert topo_desc.port == 1522
    assert topo_desc.service_name == "RAC_SVC"
    assert topo_desc.protocol == "TCPS"

    wallet_bytes = _build_wallet_zip_bytes()
    wallet_cfg = OracleConfig(wallet_zip=wallet_bytes, tns_alias="mydb_high")
    wallet_desc = wallet_cfg.connect_descriptor()
    assert wallet_desc.host == "adb.us-ashburn-1.oraclecloud.com"
    assert wallet_desc.port == 1522
    assert wallet_desc.service_name == "g1_mydb_high.adb.oraclecloud.com"
    assert wallet_desc.protocol == "TCPS"

    zip_file = tmp_path / "Wallet_MYDB.zip"
    zip_file.write_bytes(wallet_bytes)
    path_cfg = OracleConfig(
        wallet_path=str(zip_file),
        tns_alias="mydb_high",
        drcp=OracleDrcpConfig(cclass="APP_POOL", purity="SELF", connection_tag="ROLE=ADMIN"),
        auth_mode="token",
        access_token=_MOCK_OAUTH_TOKEN,
    )
    path_desc = path_cfg.connect_descriptor()
    assert path_desc.host == "adb.us-ashburn-1.oraclecloud.com"
    assert path_cfg.drcp.cclass == "APP_POOL"
    assert path_cfg.drcp.purity == "SELF"
    assert path_cfg.drcp.connection_tag == "ROLE=ADMIN"
    assert path_cfg.auth_mode == "token"
    assert path_cfg.access_token == _MOCK_OAUTH_TOKEN
    wallet = OracleWallet.from_path(zip_file)
    assert "MYDB_HIGH" in wallet.aliases


@pytest.mark.parametrize(
    ("bad_kwargs", "match_substr"),
    [
        ({"port": 0}, "port"),
        ({"purity": "SHARED"}, "purity"),
        ({"auth_mode": "kerberos"}, "auth_mode"),
        ({"connect_timeout": -5.0}, "connect_timeout"),
    ],
)
def test_oracle_native_client_constructor_validation_and_immutability(
    bad_kwargs: dict[str, Any],
    match_substr: str,
) -> None:
    """Verify zero-I/O constructor validation, GIL_USED=False, and immutable PyO3 types."""
    assert _oracle.GIL_USED is False

    sync_client = _oracle.SyncOracleClient(
        host="nonexistent.invalid",
        port=1521,
        service_name="FREEPDB1",
        user="hr",
        password="pw",
    )
    assert sync_client.closed is False
    assert sync_client.in_transaction is False
    sync_client.close()
    assert sync_client.closed is True

    attr_name = "_custom_attr"
    with pytest.raises(TypeError):
        setattr(_oracle.SyncOracleClient, attr_name, 1)
    with pytest.raises(TypeError):
        setattr(_oracle.AsyncOracleClient, attr_name, 1)

    with pytest.raises(InterfaceError, match=match_substr):
        _oracle.SyncOracleClient(**bad_kwargs)
    with pytest.raises(InterfaceError, match=match_substr):
        _oracle.AsyncOracleClient(**bad_kwargs)


def test_sync_oracle_client_fail_loud_connect_local_state_and_post_close() -> None:
    """Verify SyncOracleClient fails loud on unreachable ports and preserves local state invariants."""
    closed_port = _reserve_closed_local_port()
    client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=closed_port,
        service_name="FREEPDB1",
        user="hr",
        password="pw",
        connect_timeout=0.5,
    )
    client.cancel()

    with pytest.raises(ConnectionFailure) as conn_exc:
        client.connect()
    assert issubclass(ConnectionFailure, OperationalError)
    assert conn_exc.value.sqlstate == "08006"
    assert conn_exc.value.is_transient is True

    assert client.in_transaction is False
    client.begin()
    assert client.in_transaction is True
    client.commit()
    assert client.in_transaction is False

    client.begin()
    assert client.in_transaction is True
    client.rollback()
    assert client.in_transaction is False

    with pytest.raises(ProgrammingError) as sp_exc:
        client.savepoint("sp;drop")
    assert sp_exc.value.sqlstate == "42000"

    with pytest.raises(OperationalError) as missing_sp_exc:
        client.rollback_to_savepoint("sp_missing")
    assert missing_sp_exc.value.vendor_code == 1086

    assert client.execute_many("INSERT INTO items (id) VALUES (:1)", []) == 0

    client.close()
    assert client.closed is True
    client.close()
    client.cancel()
    with pytest.raises(InterfaceError, match="closed"):
        client.execute("SELECT 1 FROM DUAL")


@pytest.mark.anyio
async def test_async_oracle_client_drainer_fail_loud_connect_and_lifecycle() -> None:
    """Verify AsyncOracleClient completion drainer, fail-loud connect(), and post-close checks."""
    closed_port = _reserve_closed_local_port()
    client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=closed_port,
        service_name="FREEPDB1",
        user="hr",
        password="pw",
        connect_timeout=0.5,
    )
    await client.begin()
    assert client.in_transaction is True

    loop = asyncio.get_running_loop()
    assert hasattr(loop, "_vertebra_oracle_completion_drainer")

    await client.commit()
    assert client.in_transaction is False
    await client.begin()
    await client.rollback()
    assert client.in_transaction is False
    assert await client.execute_many("INSERT INTO items (id) VALUES (:1)", []) == 0

    with pytest.raises(ProgrammingError) as sp_exc:
        await client.savepoint("sp;drop")
    assert sp_exc.value.sqlstate == "42000"

    with pytest.raises(ConnectionFailure) as conn_exc:
        await client.connect()
    assert conn_exc.value.sqlstate == "08006"
    assert conn_exc.value.is_transient is True

    client.cancel()
    await client.close()
    assert client.closed is True
    with pytest.raises(InterfaceError, match="closed"):
        await client.execute("SELECT 1 FROM DUAL")


def test_oracle_column_query_result_and_pipeline_outcome_pyclasses() -> None:
    """Verify OracleColumn, OracleQueryResult, and OraclePipelineOutcome PyO3 classes and immutability."""
    col = _oracle.OracleColumn("id", "NUMBER", 2, 22, 10, 0, False)
    assert col.name == "id"
    assert col.type_code == 2
    assert col.type_name == "NUMBER"
    assert col.oracle_type == "NUMBER"
    assert col.db_size == 22
    assert col.precision == 10
    assert col.scale == 0
    assert col.nullable is False

    res = _oracle.OracleQueryResult([col], [(1,)], rowcount=1, out_binds=(42,), lastrowid="AAAR3sAAEAAAACXAAA")
    assert len(res) == 1
    assert res.is_empty() is False
    assert res.rowcount == 1
    assert res.lastrowid == "AAAR3sAAEAAAACXAAA"
    assert res.out_binds == (42,)
    assert len(res.columns) == 1
    assert res.columns[0].name == "id"
    assert res.rows == [(1,)]
    assert res.to_dicts() == [{"id": 1}]

    outcome = _oracle.OraclePipelineOutcome("completed", res)
    assert outcome.status == "completed"
    assert outcome.result is not None
    assert outcome.error is None

    empty_res = _oracle.OracleQueryResult([col], [], rowcount=0)
    assert len(empty_res) == 0
    assert empty_res.is_empty() is True

    attr_name = "name"
    with pytest.raises(AttributeError):
        setattr(col, attr_name, "x")


@pytest.mark.parametrize(
    ("raw_val", "col_type", "expected_val"),
    [
        (42, "NUMBER", 42),
        (Decimal("12345.67"), "NUMBER", Decimal("12345.67")),
        (3.14159, "BINARY_DOUBLE", 3.14159),
        ("hello UTF-8 \u2603", "VARCHAR2", "hello UTF-8 \u2603"),
        (b"\x00\x01\x02\xff", "RAW", b"\x00\x01\x02\xff"),
        (True, "BOOLEAN", True),
        (False, "BOOLEAN", False),
        (datetime.date(2026, 11, 9), "DATE", datetime.date(2026, 11, 9)),
        (
            datetime.datetime(2026, 11, 9, 14, 30, 45, 123456),
            "TIMESTAMP",
            datetime.datetime(2026, 11, 9, 14, 30, 45, 123456),
        ),
        (
            datetime.datetime(
                2026,
                11,
                9,
                14,
                30,
                45,
                123000,
                tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
            ),
            "TIMESTAMP WITH TIME ZONE",
            datetime.datetime(
                2026,
                11,
                9,
                14,
                30,
                45,
                123000,
                tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
            ),
        ),
        ("inline clob payload", "CLOB", "inline clob payload"),
        (b"inline blob payload", "BLOB", b"inline blob payload"),
    ],
)
def test_oracle_scalar_and_temporal_codecs_roundtrip(
    raw_val: Any,
    col_type: str,
    expected_val: Any,
) -> None:
    """Verify scalar, temporal, boolean, and inline LOB round-trip coercion."""
    features = OracleDriverFeatures(fetch_decimals=(col_type == "NUMBER" and isinstance(raw_val, Decimal)))
    coerced_bind = OracleCore.coerce_bind_parameter(raw_val, features=features)
    decoded_cell = OracleCore.coerce_row_cell(coerced_bind, col_type, features=features)
    assert decoded_cell == expected_val
    assert type(decoded_cell) is type(expected_val)
    if isinstance(expected_val, datetime.datetime) and expected_val.tzinfo is not None:
        assert decoded_cell.utcoffset() == datetime.timedelta(hours=5, minutes=30)


def test_oracle_oson_codec_roundtrip_and_invalid_magic() -> None:
    """Verify OSON 0xFF 'J' 'Z' 0x01 round-trip and DataError on corrupted header."""
    payload = {"tenant": "acme", "scores": [1, 2, 3]}
    encoded = OracleCore.encode_oson(payload)
    assert encoded.startswith(b"\xffJZ\x01")
    decoded = OracleCore.decode_oson(encoded)
    assert decoded == payload

    with pytest.raises((DataError, ValueError)):
        OracleCore.decode_oson(b"\xffJZ\x99corrupted")


@pytest.mark.parametrize(
    ("dtype", "values", "expected_dtype_name", "expected_dims", "expected_list"),
    [
        ("FLOAT32", [1.5, -2.25, 3.0], "float32", 3, [1.5, -2.25, 3.0]),
        ("FLOAT64", [1.125, -0.5], "float64", 2, [1.125, -0.5]),
        ("INT8", [-128, 0, 127], "int8", 3, [-128, 0, 127]),
        ("BINARY", b"\xaa\x55", "binary", 16, [170, 85]),
    ],
)
def test_oracle_vector_codec_all_dtypes_and_return_formats(
    dtype: Any,
    values: Any,
    expected_dtype_name: str,
    expected_dims: int,
    expected_list: list[Any],
) -> None:
    """Verify Oracle 26ai 0xDB 0x01 VECTOR codec across all dtypes and return formats."""
    encoded = OracleCore.encode_vector(values, dtype=dtype)
    assert encoded.startswith(OracleCore.ORACLE_VECTOR_MAGIC)
    assert OracleCore.ORACLE_VECTOR_MAGIC == b"\xdb\x01"

    vv = OracleCore.decode_vector(encoded, return_format="vector_view")
    assert isinstance(vv, VectorView)
    assert vv.dtype == expected_dtype_name
    assert vv.dimensions == expected_dims
    if expected_dtype_name in ("float32", "int8"):
        assert vv.to_list() == expected_list

    as_list = OracleCore.decode_vector(encoded, return_format="list")
    assert as_list == expected_list

    as_bytes = OracleCore.decode_vector(encoded, return_format="bytes")
    assert isinstance(as_bytes, bytes)
    assert len(as_bytes) > 0


def test_oracle_uuid_raw16_feature_toggle() -> None:
    """Verify RAW(16) <-> uuid.UUID conversion governed by enable_uuid_binary."""
    u = uuid.uuid4()
    enabled = OracleDriverFeatures(enable_uuid_binary=True)
    disabled = OracleDriverFeatures(enable_uuid_binary=False)

    bound_bytes = OracleCore.coerce_bind_parameter(u, features=enabled)
    assert isinstance(bound_bytes, bytes)
    assert len(bound_bytes) == 16
    assert bound_bytes == u.bytes

    hydrated = OracleCore.coerce_row_cell(u.bytes, "RAW", features=enabled)
    assert isinstance(hydrated, uuid.UUID)
    assert hydrated == u

    raw_passthrough = OracleCore.coerce_row_cell(u.bytes, "RAW", features=disabled)
    assert isinstance(raw_passthrough, bytes)
    assert raw_passthrough == u.bytes


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sql", "params", "is_mapping"),
    [
        (
            "SELECT :1 AS id, :2 AS name, :3 AS uid_raw, :4 AS doc, :5 AS emb FROM DUAL",
            (
                42,
                "Ada",
                uuid.UUID("12345678-1234-5678-1234-567812345678"),
                {"role": "admin"},
                VectorView.from_f32([1.0, 2.0], False),
            ),
            False,
        ),
        (
            "SELECT :id AS id, :name AS name, :uid_raw AS uid_raw, :doc AS doc, :emb AS emb FROM DUAL",
            {
                "id": 42,
                ":name": "Ada",
                ":uid_raw": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                "doc": {"role": "admin"},
                "emb": VectorView.from_f32([1.0, 2.0], False),
            },
            True,
        ),
    ],
)
async def test_oracle_positional_and_named_bind_lowering(
    sql: str,
    params: Any,
    is_mapping: bool,
) -> None:
    """Verify positional (:1..:n) and named (:name / id) bind lowering and cell hydration."""
    expected_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    oson_bytes = OracleCore.encode_oson({"role": "admin"})
    vec_bytes = OracleCore.encode_vector([1.0, 2.0], dtype="FLOAT32")
    cols = [
        _oracle.OracleColumn("ID", "NUMBER"),
        _oracle.OracleColumn("NAME", "VARCHAR2"),
        _oracle.OracleColumn("UID_RAW", "RAW", db_size=16),
        _oracle.OracleColumn("DOC", "JSON"),
        _oracle.OracleColumn("EMB", "VECTOR"),
    ]
    canned = _oracle.OracleQueryResult(cols, [(42, "Ada", expected_uuid.bytes, oson_bytes, vec_bytes)], rowcount=1)
    vv_cfg = OracleConfig(driver_features=OracleDriverFeatures(vector_return_format="vector_view"))

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_results.append(canned)
    sync_driver = SyncOracleDriver(vv_cfg, client=sync_stub)
    rows = sync_driver.select(sql, params)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == 42
    assert row["name"] == "Ada"
    assert row["uid_raw"] == expected_uuid
    assert row["doc"] == {"role": "admin"}
    assert isinstance(row["emb"], VectorView)
    assert row["emb"].to_list() == [1.0, 2.0]

    method, payload = sync_stub.calls[0]
    assert method == "query"
    coerced = payload["params"]
    if is_mapping:
        assert set(coerced.keys()) == {"id", "name", "uid_raw", "doc", "emb"}
        assert coerced["uid_raw"] == expected_uuid.bytes
        assert coerced["doc"].startswith(b"\xffJZ\x01")
        assert coerced["emb"].startswith(b"\xdb\x01")
    else:
        assert coerced[2] == expected_uuid.bytes
        assert coerced[3].startswith(b"\xffJZ\x01")
        assert coerced[4].startswith(b"\xdb\x01")
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.append(canned)
    async_driver = AsyncOracleDriver(vv_cfg, client=async_stub)
    async_rows = await async_driver.select(sql, params)
    assert len(async_rows) == 1
    assert async_rows[0]["uid_raw"] == expected_uuid
    assert async_rows[0]["doc"] == {"role": "admin"}
    assert isinstance(async_rows[0]["emb"], VectorView)
    assert async_rows[0]["emb"].to_list() == [1.0, 2.0]
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_dml_returning_into_single_round_trip() -> None:
    """Verify DML ... RETURNING ... INTO routes via execute() once and projects out_binds."""
    dml_sql = "INSERT INTO items (name) VALUES (:name) RETURNING id, name INTO :id, :name"
    cols = [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("NAME", "VARCHAR2")]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds={"id": 101, "name": "widget"}),
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds=(102, "gadget")),
        ]
    )
    sync_driver = SyncOracleDriver(client=sync_stub)
    first = sync_driver.select_one(dml_sql, {"name": "widget"})
    assert first == {"id": 101, "name": "widget"}
    tuples = sync_driver.execute_tuples(dml_sql, {"name": "gadget"})
    assert tuples == [(102, "gadget")]
    assert [c[0] for c in sync_stub.calls] == ["execute", "execute"]
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds={"id": 101, "name": "widget"}),
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds={"id": 102, "name": "gadget"}),
        ]
    )
    async_driver = AsyncOracleDriver(client=async_stub)
    assert await async_driver.select_one(dml_sql, {"name": "widget"}) == {"id": 101, "name": "widget"}
    assert await async_driver.exec_returning_async(dml_sql, {"name": "gadget"}) == [{"id": 102, "name": "gadget"}]
    assert [c[0] for c in async_stub.calls] == ["execute", "execute"]
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_execute_many_batches_and_empty_short_circuit() -> None:
    """Verify execute_many dispatches a single batch call and short-circuits empty inputs."""
    batch = [(1, "a"), (2, "b"), (3, "c")]
    sync_stub = _StubSyncOracleClient()
    sync_driver = SyncOracleDriver(client=sync_stub)
    assert sync_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", []) == 0
    assert sync_stub.calls == []
    assert sync_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", batch) == 3
    assert len(sync_stub.calls) == 1
    assert sync_stub.calls[0][0] == "execute_many"
    assert sync_stub.calls[0][1]["params_seq"] == [[1, "a"], [2, "b"], [3, "c"]]
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_driver = AsyncOracleDriver(client=async_stub)
    assert await async_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", []) == 0
    assert async_stub.calls == []
    assert await async_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", batch) == 3
    assert len(async_stub.calls) == 1
    assert async_stub.calls[0][0] == "execute_many"
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_lowercase_column_names_toggle() -> None:
    """Verify enable_lowercase_column_names=True (default) vs False across description, dicts, and structs."""
    query = "SELECT 1 AS EMPLOYEE_ID, 'Ada' AS FIRST_NAME FROM DUAL"
    raw_res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("EMPLOYEE_ID", "NUMBER"), _oracle.OracleColumn("FIRST_NAME", "VARCHAR2")],
        [(1, "Ada")],
        rowcount=1,
    )

    lower_stub = _StubSyncOracleClient()
    lower_stub.queued_results.extend([raw_res, raw_res, raw_res])
    lower_driver = SyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=True)),
        client=lower_stub,
    )
    rows, cols = lower_driver.execute_with_description(query)
    assert cols == ("employee_id", "first_name")
    assert rows == [(1, "Ada")]
    assert lower_driver.select(query) == [{"employee_id": 1, "first_name": "Ada"}]
    assert lower_driver.select(query, schema_type=_EmployeeRow) == [_EmployeeRow(employee_id=1, first_name="Ada")]
    lower_driver.close()

    upper_stub = _StubSyncOracleClient()
    upper_stub.queued_results.extend([raw_res, raw_res])
    upper_driver = SyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=False)),
        client=upper_stub,
    )
    rows_u, cols_u = upper_driver.execute_with_description(query)
    assert cols_u == ("EMPLOYEE_ID", "FIRST_NAME")
    assert rows_u == [(1, "Ada")]
    assert upper_driver.select(query) == [{"EMPLOYEE_ID": 1, "FIRST_NAME": "Ada"}]
    upper_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.extend([raw_res, raw_res])
    async_upper = AsyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=False)),
        client=async_stub,
    )
    _, acols_u = await async_upper.execute_with_description(query)
    assert acols_u == ("EMPLOYEE_ID", "FIRST_NAME")
    assert await async_upper.select(query) == [{"EMPLOYEE_ID": 1, "FIRST_NAME": "Ada"}]
    await async_upper.close()


@pytest.mark.anyio
async def test_oracle_arrow59_inline_and_shm_escalation() -> None:
    """Verify inline vs VTBARROW SHM escalation across SyncOracleDriver and AsyncOracleDriver."""
    query = "SELECT LEVEL AS ID, 'item_' AS NAME FROM DUAL CONNECT BY LEVEL <= 5"
    sample_rows = [(idx, f"item_{idx}") for idx in range(1, 6)]
    sample_cols = ("ID", "NAME")

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_arrow.extend([(sample_rows, sample_cols), (sample_rows, sample_cols)])
    sync_driver = SyncOracleDriver(client=sync_stub)
    with sync_driver.select_to_arrow(query, force_shm=False) as inline_cur:
        assert inline_cur.used_shm is False
        assert len(inline_cur) == 5
        inline_tbl = pa.ipc.open_stream(pa.py_buffer(inline_cur.buffer_view())).read_all()
        assert inline_tbl.column_names == ["id", "name"]
        assert inline_tbl.num_rows == 5

    with sync_driver.select_to_arrow(query, force_shm=True) as shm_cur:
        assert shm_cur.used_shm is True
        assert len(shm_cur) == 5
        shm_tbl = pa.ipc.open_stream(pa.py_buffer(shm_cur.buffer_view())).read_all()
        assert shm_tbl.column_names == ["id", "name"]
        assert shm_tbl.to_pydict()["id"] == [1, 2, 3, 4, 5]
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_arrow.extend([(sample_rows, sample_cols), (sample_rows, sample_cols)])
    async_driver = AsyncOracleDriver(client=async_stub)
    with await async_driver.select_to_arrow(query, force_shm=False) as a_inline:
        assert a_inline.used_shm is False
        assert len(a_inline) == 5

    with await async_driver.select_to_arrow(query, force_shm=True) as a_shm:
        assert a_shm.used_shm is True
        assert len(a_shm) == 5
        ashm_tbl = pa.ipc.open_stream(pa.py_buffer(a_shm.buffer_view())).read_all()
        assert ashm_tbl.column_names == ["id", "name"]
        assert ashm_tbl.num_rows == 5
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_stream_arrow_batches_sync_and_async() -> None:
    """Verify stream_arrow_batches yields chunked ArrowCursor batches with matching IPC streams."""
    query = "SELECT LEVEL AS ID, 'item' AS NAME FROM DUAL CONNECT BY LEVEL <= 5"
    res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("NAME", "VARCHAR2")],
        [(idx, "item") for idx in range(1, 6)],
        rowcount=5,
    )

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_results.append(res)
    sync_driver = SyncOracleDriver(client=sync_stub)
    sync_batches = list(sync_driver.stream_arrow_batches(query, batch_size=2))
    assert [len(b) for b in sync_batches] == [2, 2, 1]
    assert sum(len(b.buffer_view()) for b in sync_batches) > 0
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.append(res)
    async_driver = AsyncOracleDriver(client=async_stub)
    async_lengths: list[int] = []
    async for batch in async_driver.stream_arrow_batches(query, batch_size=2):
        with batch:
            async_lengths.append(len(batch))
            assert len(batch.buffer_view()) > 0
    assert async_lengths == [2, 2, 1]
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_load_from_arrow_direct_path_and_identifier_validation() -> None:
    """Verify load_from_arrow APPEND_VALUES hint precedence, empty short-circuit, and identifier validation."""
    table = pa.Table.from_pydict({"id": [1, 2], "name": ["a", "b"]})
    empty_table = pa.Table.from_pydict({"id": [], "name": []})

    sync_stub = _StubSyncOracleClient()
    sync_driver = SyncOracleDriver(client=sync_stub)
    assert sync_driver.load_from_arrow("items", empty_table) == 0
    assert sync_stub.calls == []
    assert sync_driver.load_from_arrow("hr.items", table, direct_path=True) == 2
    assert sync_stub.calls[0][1]["sql"] == "INSERT /*+ APPEND_VALUES */ INTO hr.items (id, name) VALUES (:1, :2)"
    assert sync_driver.load_from_arrow("items", [{"id": 3, "name": "c"}], direct_path=False) == 1
    assert sync_stub.calls[1][1]["sql"] == "INSERT INTO items (id, name) VALUES (:1, :2)"

    with pytest.raises((ValueError, ProgrammingError)):
        sync_driver.load_from_arrow("bad;table", table)
    with pytest.raises((ValueError, ProgrammingError)):
        sync_driver.load_from_arrow("items", table, columns=("id", "bad;col"))
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_driver = AsyncOracleDriver(client=async_stub)
    assert await async_driver.load_from_arrow("items", empty_table) == 0
    assert await async_driver.load_from_arrow("items", table, direct_path=True) == 2
    assert async_stub.calls[0][1]["sql"] == "INSERT /*+ APPEND_VALUES */ INTO items (id, name) VALUES (:1, :2)"
    with pytest.raises((ValueError, ProgrammingError)):
        await async_driver.load_from_arrow("bad;table", table)
    await async_driver.close()


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"max_size": 0},
        {"min_size": -1, "max_size": 4},
        {"min_size": 5, "max_size": 2},
        {"acquire_timeout": 0.0},
        {"reset_timeout": -1.0},
        {"close_timeout": 0.0},
    ],
)
def test_oracle_pool_zero_io_init_and_bounds_validation(bad_kwargs: dict[str, Any]) -> None:
    """Verify native `_oracle` SyncOraclePool and AsyncOraclePool construct with zero I/O and validate bounds."""
    start = time.perf_counter()
    pool = _oracle.SyncOraclePool(host="nonexistent.invalid", min_size=1, max_size=4)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    assert elapsed_ms < 25.0
    assert pool.min_size == 1
    assert pool.max_size == 4
    assert pool.size == 0
    assert pool.idle_size == 0
    assert pool.borrowed_size == 0
    assert pool.closed is False
    stats = pool.stats()
    assert stats == {
        "min_size": 1,
        "max_size": 4,
        "size": 0,
        "idle": 0,
        "idle_size": 0,
        "borrowed": 0,
        "borrowed_size": 0,
        "opening": 0,
        "closed": 0,
    }
    pool.close()
    assert pool.closed is True
    assert pool.stats()["closed"] == 1

    with pytest.raises((InterfaceError, ValueError)):
        _oracle.SyncOraclePool(host="nonexistent.invalid", **bad_kwargs)
    with pytest.raises((InterfaceError, ValueError)):
        _oracle.AsyncOraclePool(host="nonexistent.invalid", **bad_kwargs)


@pytest.mark.anyio
async def test_native_oracle_pool_fail_loud_connect_rolls_back_opening_slot() -> None:
    """Verify native `_oracle` pools raise ConnectionFailure on dead ports and release reserved opening slots."""
    closed_port = _reserve_closed_local_port()
    pool = _oracle.SyncOraclePool(
        host="127.0.0.1",
        port=closed_port,
        min_size=0,
        max_size=1,
        connect_timeout=0.5,
    )
    with pytest.raises((InterfaceError, ValueError)):
        pool.acquire(purity="BOGUS")
    with pytest.raises(ConnectionFailure) as exc_info:
        pool.acquire()
    assert exc_info.value.sqlstate == "08006"
    assert exc_info.value.is_transient is True
    assert pool.size == 0
    assert pool.borrowed_size == 0
    assert pool.idle_size == 0
    pool.close()
    with pytest.raises(InterfaceError):
        pool.acquire()

    apool = _oracle.AsyncOraclePool(
        host="127.0.0.1",
        port=closed_port,
        min_size=0,
        max_size=1,
        connect_timeout=0.5,
    )
    with pytest.raises((InterfaceError, ValueError)):
        await apool.acquire(purity="BOGUS")
    with pytest.raises(ConnectionFailure) as aexc:
        await apool.acquire()
    assert aexc.value.sqlstate == "08006"
    assert aexc.value.is_transient is True
    assert apool.size == 0
    assert apool.borrowed_size == 0
    assert apool.idle_size == 0
    await apool.close()


@pytest.mark.anyio
async def test_oracle_pool_acquire_release_exhaustion_rollback_and_evict() -> None:
    """Verify Python pool facades propagate PoolExhaustedError (53300), rollback dirty leases, and evict."""
    pool = SyncOraclePool(native_pool=_StubSyncOraclePool(min_size=0, max_size=1))
    c1 = pool.acquire()
    assert pool.size == 1
    assert pool.borrowed_size == 1
    assert pool.idle_size == 0
    c1.begin()
    assert c1.in_transaction is True

    with pytest.raises(PoolExhaustedError) as exc_info:
        pool.acquire(timeout=0.03)
    assert getattr(exc_info.value, "sqlstate", None) == "53300"
    assert getattr(exc_info.value, "is_transient", None) is True

    pool.release(c1)
    assert c1.in_transaction is False
    assert ("rollback", None) in c1.calls
    assert pool.borrowed_size == 0
    assert pool.idle_size == 1

    reused = pool.acquire()
    assert reused is c1
    pool.release(reused, discard=True)
    assert pool.size == 0
    assert reused.closed is True

    c2 = pool.acquire()
    pool.evict(c2)
    assert pool.size == 0
    assert c2.closed is True
    pool.close()

    apool = AsyncOraclePool(native_pool=_StubAsyncOraclePool(min_size=0, max_size=1))
    ac1 = await apool.acquire()
    await ac1.begin()
    with pytest.raises(PoolExhaustedError) as aexc:
        await apool.acquire(timeout=0.03)
    assert getattr(aexc.value, "sqlstate", None) == "53300"
    await apool.release(ac1)
    assert ac1.in_transaction is False
    assert apool.idle_size == 1
    await apool.evict(ac1)
    assert apool.size == 0
    await apool.close()


@pytest.mark.anyio
async def test_oracle_drcp_purity_validation_and_tag_matching() -> None:
    """Verify DRCP cclass, purity (SELF vs NEW), and connection_tag matching on pool checkout."""
    pool = SyncOraclePool(native_pool=_StubSyncOraclePool(min_size=0, max_size=2, cclass="VTB_POOL", purity="SELF"))
    c_a = pool.acquire_from_pool()
    c_b = pool.acquire_from_pool()
    assert c_a.cclass == "VTB_POOL"
    assert c_a.purity == "SELF"
    assert c_a.tag_matched is False
    pool.release_to_pool(c_a, connection_tag="TAG_A")
    pool.release_to_pool(c_b, connection_tag="TAG_B")

    matched = pool.acquire_from_pool(connection_tag="TAG_B", purity="SELF")
    assert matched.connection_tag == "TAG_B"
    assert matched.tag_matched is True
    pool.release_to_pool(matched, connection_tag="TAG_B")

    fresh = pool.acquire_from_pool(connection_tag="TAG_B", purity="NEW")
    assert fresh.purity == "NEW"
    assert fresh.tag_matched is False
    assert fresh.connection_tag is None
    pool.release_to_pool(fresh)

    with pytest.raises((InterfaceError, ValueError)):
        pool.acquire(purity="BOGUS")
    pool.close()

    apool = AsyncOraclePool(
        native_pool=_StubAsyncOraclePool(min_size=0, max_size=2, cclass="VTB_ASYNC_POOL", purity="SELF")
    )
    ac1 = await apool.acquire_from_pool()
    await apool.release_to_pool(ac1, connection_tag="ASYNC_TAG")
    ac_matched = await apool.acquire_from_pool(connection_tag="ASYNC_TAG", purity="SELF")
    assert ac_matched.tag_matched is True
    assert ac_matched.connection_tag == "ASYNC_TAG"
    await apool.release_to_pool(ac_matched)
    await apool.close()


def test_oracle_native_set_session_attrs_coalescing_and_ttc_frame_encoding() -> None:
    """Verify native `_oracle.SyncOracleClient.set_session_attrs` coalesces zero-I/O and encodes `[0x11, 0x6e]`."""
    client = _oracle.SyncOracleClient(host="nonexistent.invalid", service_name="FREEPDB1")
    client.set_session_attrs(client_identifier="t1", module="m1")
    client.set_session_attrs(action="a1")
    assert client.pending_session_attrs == {
        "client_identifier": "t1",
        "module": "m1",
        "action": "a1",
        "ecid": None,
    }
    assert client.last_piggyback_frame is None
    client.close()

    attrs = OracleSessionAttrs(client_identifier="t1", module="m1", action="a1")
    frame = attrs.encode_ttc_piggyback()
    assert frame.startswith(b"\x11\x6e\x03")


@pytest.mark.anyio
async def test_python_oracle_pool_lease_and_driver_piggyback() -> None:
    """Verify SyncOraclePool and AsyncOraclePool lease() checkout and driver last_piggyback_frame."""
    one_row = _oracle.OracleQueryResult([_oracle.OracleColumn("VAL", "NUMBER")], [(1,)], rowcount=1)

    sync_native = _StubSyncOraclePool(min_size=0, max_size=2)
    with SyncOraclePool(native_pool=sync_native) as pool:
        assert pool.min_size == 0
        assert pool.max_size == 2
        assert pool.closed is False
        assert pool.is_closed is False
        with pool.lease(client_identifier="t-0") as drv:
            assert pool.borrowed_size == 1
            drv.client.queued_results.append(one_row)
            drv.set_session_attrs(client_identifier="t-1", module="api")
            assert drv.select_value("SELECT 1 FROM DUAL") == 1
            frame = drv.last_piggyback_frame
            assert frame is not None
            assert frame.startswith(b"\x11\x6e\x02")
        assert pool.borrowed_size == 0
        assert pool.idle_size == 1
    assert pool.closed is True
    assert pool.is_closed is True

    async_native = _StubAsyncOraclePool(min_size=0, max_size=2)
    async with AsyncOraclePool(native_pool=async_native) as apool:
        async with apool.lease() as adrv:
            assert apool.borrowed_size == 1
            adrv.client.queued_results.append(one_row)
            adrv.set_session_attrs(client_identifier="t-async", module="worker")
            assert await adrv.select_value("SELECT 1 FROM DUAL") == 1
            aframe = adrv.last_piggyback_frame
            assert aframe is not None
            assert aframe.startswith(b"\x11\x6e\x02")
        assert apool.borrowed_size == 0
        assert apool.idle_size == 1
    assert apool.closed is True


@pytest.mark.anyio
async def test_oracle_driver_transaction_pins_lease_and_guards_child_tasks() -> None:
    """Verify AsyncOracleDriver transaction() pins a dedicated lease and isolates spawned child tasks."""
    ok_res = _oracle.OracleQueryResult([_oracle.OracleColumn("OK", "NUMBER")], [(1,)], rowcount=1)
    async_native = _StubAsyncOraclePool(min_size=0, max_size=2)
    async with AsyncOraclePool(native_pool=async_native) as apool:
        driver = AsyncOracleDriver(pool=apool)
        assert driver.in_transaction is False

        async with driver.transaction():
            assert driver.in_transaction is True
            assert apool.borrowed_size == 1
            pinned_client = driver.client
            await driver.execute("INSERT INTO tx_guard (id, name) VALUES (1, 'parent')")
            assert driver.client is pinned_client

            async def _child_query() -> tuple[bool, int, Any, list[dict[str, Any]]]:
                in_tx = driver.in_transaction
                rows = await driver.select("SELECT 1 AS ok FROM DUAL")
                return in_tx, apool.size, driver.client, rows

            child_client = _StubAsyncOracleClient()
            child_client.queued_results.append(ok_res)
            async_native._idle.append(child_client)

            child_in_tx, total_size, child_default_client, child_rows = await asyncio.create_task(_child_query())
            assert child_in_tx is False
            assert child_rows == [{"ok": 1}]
            assert total_size == 2
            assert child_default_client is not pinned_client
            assert apool.borrowed_size == 1
            assert apool.idle_size == 1

            pinned_client.queued_results.append(ok_res)
            assert await driver.select_value("SELECT COUNT(*) FROM tx_guard") == 1

        assert driver.in_transaction is False
        assert apool.borrowed_size == 0
        assert apool.idle_size == 2
        await driver.close()


@pytest.mark.anyio
async def test_oracle_driver_savepoint_and_rollback_to_savepoint() -> None:
    """Verify savepoint, rollback_to_savepoint, release_savepoint, and identifier validation."""
    sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(client=sync_stub) as sync_driver, sync_driver.transaction():
        sync_driver.savepoint("sp_1")
        sync_driver.rollback_to_savepoint("sp_1")
        sync_driver.release_savepoint("sp_1")
        assert list(sync_driver.transaction_events) == [
            "BEGIN",
            "SAVEPOINT sp_1",
            "ROLLBACK TO SAVEPOINT sp_1",
        ]

        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.rollback_to_savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.release_savepoint("bad;name")

    async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(client=async_stub) as async_driver, async_driver.transaction():
        await async_driver.savepoint("sp_1")
        await async_driver.rollback_to_savepoint("sp_1")
        await async_driver.release_savepoint("sp_1")
        assert list(async_driver.transaction_events) == [
            "BEGIN",
            "SAVEPOINT sp_1",
            "ROLLBACK TO SAVEPOINT sp_1",
        ]

        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.rollback_to_savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.release_savepoint("bad;name")


@pytest.mark.anyio
async def test_sync_and_async_pipeline_delayed_commit_on_clean_exit() -> None:
    """Verify SyncOraclePipeline and AsyncOraclePipeline hold transaction across results() and commit on exit."""
    row_res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("LABEL", "VARCHAR2")],
        [(1, "a")],
        rowcount=1,
    )
    outcomes = [
        _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
        _oracle.OraclePipelineOutcome("completed", row_res),
    ]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(outcomes)
    with SyncOracleDriver(client=sync_stub) as driver:
        assert isinstance(driver, SyncPipelineCapable)
        with driver.pipeline() as pipe:
            assert isinstance(pipe, SyncOraclePipeline)
            assert driver.in_transaction is True
            with pytest.raises(InterfaceError, match="already active"):
                driver.pipeline()
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "a"))
            pipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
            res = pipe.results()
            assert res == [1, [{"id": 1, "label": "a"}]]
            assert driver.in_transaction is True
            assert driver.transaction_events == ("BEGIN",)
        assert driver.in_transaction is False
        assert driver.transaction_events == ("BEGIN", "COMMIT")

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(outcomes)
    async with AsyncOracleDriver(client=async_stub) as async_driver:
        assert isinstance(async_driver, AsyncPipelineCapable)
        async with async_driver.pipeline() as apipe:
            assert isinstance(apipe, AsyncOraclePipeline)
            assert async_driver.in_transaction is True
            with pytest.raises(InterfaceError, match="already active"):
                async_driver.pipeline()
            apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "a"))
            apipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
            ares = await apipe.results()
            assert ares == [1, [{"id": 1, "label": "a"}]]
            assert async_driver.in_transaction is True
            assert async_driver.transaction_events == ("BEGIN",)
        assert async_driver.in_transaction is False
        assert async_driver.transaction_events == ("BEGIN", "COMMIT")


@pytest.mark.anyio
async def test_pipeline_rolls_back_when_caller_raises_after_results() -> None:
    """Verify raising after pipe.results() or exiting with unexecuted ops rolls back the batch."""
    row_res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("LABEL", "VARCHAR2")],
        [(1, "transient")],
        rowcount=1,
    )
    outcomes = [
        _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
        _oracle.OraclePipelineOutcome("completed", row_res),
    ]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(outcomes)
    with SyncOracleDriver(client=sync_stub) as driver:
        with pytest.raises(ValueError, match="hydration failed"), driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "transient"))
            pipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
            assert pipe.results() == [1, [{"id": 1, "label": "transient"}]]
            raise ValueError("hydration failed")
        assert driver.in_transaction is False
        assert driver.transaction_events == ("BEGIN", "ROLLBACK")

        with pytest.raises(InterfaceError, match="unexecuted operations"), driver.pipeline() as pipe2:
            pipe2.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "unflushed"))
        assert driver.transaction_events == ("BEGIN", "ROLLBACK", "BEGIN", "ROLLBACK")

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(outcomes)
    async with AsyncOracleDriver(client=async_stub) as async_driver:
        with pytest.raises(ValueError, match="hydration failed"):
            async with async_driver.pipeline() as apipe:
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "transient"))
                apipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
                assert await apipe.results() == [1, [{"id": 1, "label": "transient"}]]
                raise ValueError("hydration failed")
        assert async_driver.in_transaction is False
        assert async_driver.transaction_events == ("BEGIN", "ROLLBACK")


@pytest.mark.anyio
async def test_pipeline_nested_inside_transaction_uses_oracle_savepoint_without_release() -> None:
    """Verify nested pipeline inside transaction() uses SAVEPOINT _vtb_pipe_<n> without RELEASE SAVEPOINT."""
    sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(client=sync_stub) as driver:
        with driver.transaction():
            with driver.pipeline() as pipe1:
                pipe1.execute("INSERT INTO items (id, label) VALUES (2, 'inner_ok')")
                assert pipe1.results() == [1]
            assert driver.in_transaction is True
            assert list(driver.transaction_events) == ["BEGIN", "SAVEPOINT _vtb_pipe_1"]

            with pytest.raises(RuntimeError, match="inner abort"), driver.pipeline() as pipe2:
                pipe2.execute("INSERT INTO items (id, label) VALUES (3, 'inner_bad')")
                assert pipe2.results() == [1]
                raise RuntimeError("inner abort")
            assert driver.in_transaction is True
            assert list(driver.transaction_events) == [
                "BEGIN",
                "SAVEPOINT _vtb_pipe_1",
                "SAVEPOINT _vtb_pipe_2",
                "ROLLBACK TO SAVEPOINT _vtb_pipe_2",
            ]
        assert driver.in_transaction is False
        assert list(driver.transaction_events) == [
            "BEGIN",
            "SAVEPOINT _vtb_pipe_1",
            "SAVEPOINT _vtb_pipe_2",
            "ROLLBACK TO SAVEPOINT _vtb_pipe_2",
            "COMMIT",
        ]

    async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(client=async_stub) as async_driver, async_driver.transaction():
        async with async_driver.pipeline() as apipe1:
            apipe1.execute("INSERT INTO items (id, label) VALUES (2, 'inner_ok')")
            assert await apipe1.results() == [1]
        with pytest.raises(RuntimeError, match="inner abort"):
            async with async_driver.pipeline() as apipe2:
                apipe2.execute("INSERT INTO items (id, label) VALUES (3, 'inner_bad')")
                assert await apipe2.results() == [1]
                raise RuntimeError("inner abort")
        assert async_driver.transaction_events == (
            "BEGIN",
            "SAVEPOINT _vtb_pipe_1",
            "SAVEPOINT _vtb_pipe_2",
            "ROLLBACK TO SAVEPOINT _vtb_pipe_2",
        )


@pytest.mark.anyio
async def test_pipeline_mid_batch_error_marks_subsequent_skipped_and_rolls_back() -> None:
    """Verify mid-batch ORA-00001 failure marks later steps 'skipped', raises IntegrityError, and rolls back."""
    dup_err = IntegrityError("ORA-00001: unique constraint violated")
    dup_err.sqlstate = "23505"
    dup_err.vendor_code = 1
    mid_batch = [
        _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
        _oracle.OraclePipelineOutcome("error", None, dup_err),
        _oracle.OraclePipelineOutcome("skipped", None, None),
    ]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(mid_batch)
    with SyncOracleDriver(client=sync_stub) as driver:
        with pytest.raises(IntegrityError) as exc_info, driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "duplicate"))
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "never_run"))
            pipe.results()
        assert exc_info.value.sqlstate == "23505"
        assert exc_info.value.vendor_code == 1
        assert tuple(o.status for o in driver.last_pipeline_outcomes) == ("completed", "error", "skipped")
        assert driver.transaction_events == ("BEGIN", "ROLLBACK")

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(mid_batch)
    async with AsyncOracleDriver(client=async_stub) as async_driver:
        with pytest.raises(IntegrityError) as aexc_info:
            async with async_driver.pipeline() as apipe:
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "duplicate"))
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "never_run"))
                await apipe.results()
        assert aexc_info.value.sqlstate == "23505"
        assert tuple(o.status for o in async_driver.last_pipeline_outcomes) == ("completed", "error", "skipped")
        assert async_driver.transaction_events == ("BEGIN", "ROLLBACK")


def test_oracledb_crate_ttc_pipeline_capability_probe() -> None:
    """Verify ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED and QueryCancelledError availability."""
    assert _oracle.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False
    assert issubclass(QueryCancelledError, OperationalError)


def test_oracle_core_build_session_and_clear_context_set_identifier_default() -> None:
    """Verify default USERENV namespace builds DBMS_SESSION.SET_IDENTIFIER and CLEAR_IDENTIFIER."""
    cfg = OracleConfig()
    assert cfg.context_namespace == "USERENV"
    assert cfg.context_package_proc is None
    assert cfg.context_clear_proc is None

    set_sql, set_params = OracleCore.build_session_context_sql("acme-42")
    assert set_sql == "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;"
    assert set_params == {"value": "acme-42"}

    clear_sql, clear_params = OracleCore.build_clear_context_sql()
    assert clear_sql == "BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;"
    assert clear_params == {}


def test_oracle_core_build_session_and_clear_context_trusted_package() -> None:
    """Verify trusted context package procedures and custom namespaces build parameterized PL/SQL blocks."""
    scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")
    set_sql, set_params = OracleCore.build_session_context_sql(
        "acme-42",
        scope_cfg,
        namespace="VERTEBRA_CTX",
        context_package_proc="APP_SEC.CTX_PKG.SET_ATTR",
    )
    assert set_sql == "BEGIN APP_SEC.CTX_PKG.SET_ATTR(:namespace, :attribute, :value); END;"
    assert set_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "acme-42"}

    clear_sql, clear_params = OracleCore.build_clear_context_sql(
        scope_cfg,
        namespace="VERTEBRA_CTX",
        context_clear_proc="APP_SEC.CTX_PKG.CLEAR_ATTR",
    )
    assert clear_sql == "BEGIN APP_SEC.CTX_PKG.CLEAR_ATTR(:namespace, :attribute); END;"
    assert clear_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id"}

    direct_sql, direct_params = OracleCore.build_session_context_sql(
        "acme-42",
        scope_cfg,
        namespace="VERTEBRA_CTX",
    )
    assert direct_sql == "BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;"
    assert direct_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "acme-42"}

    direct_clear_sql, direct_clear_params = OracleCore.build_clear_context_sql(
        scope_cfg,
        namespace="VERTEBRA_CTX",
    )
    assert direct_clear_sql == "BEGIN DBMS_SESSION.CLEAR_CONTEXT(:namespace, NULL, :attribute); END;"
    assert direct_clear_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id"}


@pytest.mark.parametrize(
    "bad_proc",
    [
        "PKG.PROC; DROP USER system",
        "PKG.PROC()",
        "PKG.PROC -- comment",
        "123PKG.PROC",
    ],
)
def test_oracle_core_rejects_malicious_context_package_proc(bad_proc: str) -> None:
    """Verify PL/SQL injection in context_package_proc or context_clear_proc raises ValueError."""
    with pytest.raises(ValueError, match="Invalid"):
        OracleCore.build_session_context_sql("acme-42", context_package_proc=bad_proc)
    with pytest.raises(ValueError, match="Invalid"):
        OracleCore.build_clear_context_sql(context_clear_proc=bad_proc)


@pytest.mark.anyio
async def test_tenant_connection_scope_resets_on_rollback_and_evicts_on_reset_error() -> None:
    """Verify TenantConnectionScope and AsyncTenantConnectionScope reset on rollback and evict on reset failure."""
    sync_ok = _SyncDriverDouble(fail_reset=False)
    with pytest.raises(RuntimeError, match="rolled back body"), TenantConnectionScope(sync_ok, "tenant-a"):
        raise RuntimeError("rolled back body")
    assert sync_ok.evicted is False
    assert [e[0] for e in sync_ok.events] == ["enter", "exit"]
    assert sync_ok.events[1][1] == ("BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;", {})

    sync_bad = _SyncDriverDouble(fail_reset=True)
    with pytest.raises(OperationalError, match="ORA-03113"), TenantConnectionScope(sync_bad, "tenant-a"):
        pass
    assert sync_bad.evicted is True
    assert [e[0] for e in sync_bad.events] == ["enter", "exit", "evict"]

    async_ok = _AsyncDriverDouble(fail_reset=False)
    with pytest.raises(RuntimeError, match="async body failure"):
        async with AsyncTenantConnectionScope(async_ok, "tenant-a"):
            raise RuntimeError("async body failure")
    assert async_ok.evicted is False
    assert [e[0] for e in async_ok.events] == ["enter", "exit"]

    async_bad = _AsyncDriverDouble(fail_reset=True)
    with pytest.raises(OperationalError, match="ORA-03113"):
        async with AsyncTenantConnectionScope(async_bad, "tenant-a"):
            pass
    assert async_bad.evicted is True
    assert [e[0] for e in async_bad.events] == ["enter", "exit", "evict"]


@pytest.mark.anyio
async def test_sqlspec_oracle_drivers_declare_named_colon_and_compile_stack() -> None:
    """Verify OracleSyncDriver and OracleAsyncDriver declare NAMED_COLON and execute_stack uses pipeline()."""
    assert OracleSyncDriver._execution_parameter_style == ParameterStyle.NAMED_COLON
    assert OracleAsyncDriver._execution_parameter_style == ParameterStyle.NAMED_COLON

    cols = [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("LABEL", "VARCHAR2")]
    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(
        [
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult(cols, [(7, "seven")], rowcount=1)),
        ]
    )
    with SyncOracleDriver(client=sync_stub) as native_sync:
        sync_adapter = OracleSyncDriver(native_sync)
        stmt = sync_adapter.prepare_statement("SELECT id, label FROM items WHERE id = ?", (7,))
        compiled_sql, compiled_params = stmt.compile()
        assert "?" not in compiled_sql
        assert ":" in compiled_sql
        assert compiled_params

        stack = (
            StatementStack()
            .push_execute("INSERT INTO items (id, label) VALUES (?, ?)", 7, "seven")
            .push_execute("SELECT id, label FROM items WHERE id = ?", 7)
        )
        stack_results = sync_adapter.execute_stack(stack)
        assert len(stack_results) == 2
        assert stack_results[0].rows_affected == 1
        assert stack_results[1].result is not None
        assert stack_results[1].result.data == [{"id": 7, "label": "seven"}]

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(
        [
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult(cols, [(9, "nine")], rowcount=1)),
        ]
    )
    async with AsyncOracleDriver(client=async_stub) as native_async:
        async_adapter = OracleAsyncDriver(native_async)
        astack = (
            StatementStack()
            .push_execute("INSERT INTO items (id, label) VALUES (?, ?)", 9, "nine")
            .push_execute("SELECT id, label FROM items WHERE id = ?", 9)
        )
        astack_results = await async_adapter.execute_stack(astack)
        assert len(astack_results) == 2
        assert astack_results[0].rows_affected == 1
        assert astack_results[1].result is not None
        assert astack_results[1].result.data == [{"id": 9, "label": "nine"}]


@pytest.mark.anyio
async def test_sqlspec_oracle_apply_tenant_predicate_and_driver_scope_dispatches() -> None:
    """Verify apply_tenant_predicate and driver tenant scope hooks issue SET_IDENTIFIER or trusted PL/SQL."""
    sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(client=sync_stub) as sync_default:
        sync_adapter_default = OracleSyncDriver(sync_default)
        pred_sql = sync_adapter_default.apply_tenant_predicate(TenantPredicate(tenant_value="t-99"))
        assert pred_sql == "tenant_id = :tenant_id"
        default_execs = [payload for kind, payload in sync_stub.calls if kind == "execute"]
        assert default_execs[0]["sql"] == "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;"
        assert default_execs[0]["params"] == {"value": "t-99"}

        with TenantConnectionScope(sync_default, "scoped-sync"):
            assert sync_default._session_context[("USERENV", "tenant_id")] == "scoped-sync"
        assert ("USERENV", "tenant_id") not in sync_default._session_context
        default_execs = [payload for kind, payload in sync_stub.calls if kind == "execute"]
        assert default_execs[2]["sql"] == "BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;"

    trusted_cfg = OracleConfig(
        context_namespace="APP_CTX",
        context_package_proc="SEC_PKG.SET_CTX",
        context_clear_proc="SEC_PKG.CLEAR_CTX",
    )
    trusted_sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(trusted_cfg, client=trusted_sync_stub) as sync_trusted:
        sync_adapter_trusted = OracleSyncDriver(sync_trusted)
        assert (
            sync_adapter_trusted.apply_tenant_predicate(TenantPredicate(tenant_value="t-tr"))
            == "tenant_id = :tenant_id"
        )
        trusted_execs = [payload for kind, payload in trusted_sync_stub.calls if kind == "execute"]
        assert trusted_execs[0]["sql"] == "BEGIN SEC_PKG.SET_CTX(:namespace, :attribute, :value); END;"
        with TenantConnectionScope(sync_trusted, "scoped-trusted"):
            assert sync_trusted._session_context[("APP_CTX", "tenant_id")] == "scoped-trusted"
        trusted_execs = [payload for kind, payload in trusted_sync_stub.calls if kind == "execute"]
        assert trusted_execs[2]["sql"] == "BEGIN SEC_PKG.CLEAR_CTX(:namespace, :attribute); END;"

    trusted_async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(trusted_cfg, client=trusted_async_stub) as async_trusted:
        async_adapter_trusted = OracleAsyncDriver(async_trusted)
        apred = await async_adapter_trusted.apply_tenant_predicate(TenantPredicate(tenant_value="async-tr"))
        assert apred == "tenant_id = :tenant_id"
        async with AsyncTenantConnectionScope(async_trusted, "async-scoped"):
            assert async_trusted._session_context[("APP_CTX", "tenant_id")] == "async-scoped"
        assert ("APP_CTX", "tenant_id") not in async_trusted._session_context
        async_execs = [payload for kind, payload in trusted_async_stub.calls if kind == "execute"]
        assert async_execs[2]["sql"] == "BEGIN SEC_PKG.CLEAR_CTX(:namespace, :attribute); END;"


@pytest.mark.anyio
async def test_oracle_notification_and_listener_exports_validation_and_fail_loud_worker() -> None:
    """Verify OracleNotification attributes, AQ validation, and fail-loud AsyncOracleListener connect error."""
    notification = _oracle.OracleNotification(
        "VTB_AQ_ORDERS",
        b"msg-001",
        b'{"event":"created"}',
        "ORDER_WORKER",
        1700000000000,
    )
    assert notification.queue_name == "VTB_AQ_ORDERS"
    assert notification.consumer_name == "ORDER_WORKER"
    assert notification.msg_id == b"msg-001"
    assert notification.payload == b'{"event":"created"}'
    assert notification.enqueued_at_epoch_ms == 1700000000000

    closed_port = _reserve_closed_local_port()
    client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=closed_port,
        user="app",
        password="secret",
        connect_timeout=0.5,
    )
    try:
        with pytest.raises(ProgrammingError) as exc_info:
            await client.open_listener(queue_name="BAD;QUEUE")
        assert exc_info.value.sqlstate == "42602"

        with pytest.raises(ProgrammingError) as c_exc:
            await client.open_listener(queue_name="VTB_AQ_ORDERS", consumer_name="BAD;CONSUMER")
        assert c_exc.value.sqlstate == "42602"

        with pytest.raises(InterfaceError):
            await client.open_listener(queue_name="VTB_AQ_ORDERS", max_buffered=0)

        with pytest.raises(ProgrammingError) as enq_exc:
            await client.enqueue_notification(queue_name="BAD;QUEUE", payload=b"x")
        assert enq_exc.value.sqlstate == "42602"

        listener = await client.open_listener(
            queue_name="VTB_AQ_ORDERS",
            consumer_name="ORDER_WORKER",
            wait_timeout_seconds=1,
            max_buffered=16,
        )
        assert isinstance(listener, _oracle.AsyncOracleListener)
        assert listener.queue_name == "VTB_AQ_ORDERS"
        assert listener.consumer_name == "ORDER_WORKER"
        assert listener.dropped_count == 0

        with pytest.raises(ConnectionFailure) as listen_exc:
            await listener.next_notification(timeout=5.0)
        assert listen_exc.value.sqlstate == "08006"
        assert listen_exc.value.is_transient is True

        await listener.close()
        assert listener.is_closed is True
        await listener.close()
        assert await listener.next_notification(timeout=0.05) is None
    finally:
        await client.close()


@pytest.mark.parametrize(
    ("raw_channel", "expected_queue"),
    [
        ("orders", "VTB_AQ_ORDERS"),
        ("  VTB_AQ_BILLING  ", "VTB_AQ_BILLING"),
        ("_events$1#", "VTB_AQ__EVENTS$1#"),
    ],
)
def test_validate_oracle_channel_name_resolves_valid_identifiers(
    raw_channel: str,
    expected_queue: str,
) -> None:
    """Verify valid channel names resolve to uppercase prefixed AQ identifiers."""
    assert validate_oracle_channel_name(raw_channel) == expected_queue


@pytest.mark.parametrize(
    "invalid_channel",
    [
        "",
        "   ",
        "1leading_digit",
        "orders; DROP TABLE users",
        "orders with spaces",
        "schema.orders",
        "A" * 129,
    ],
)
def test_validate_oracle_channel_name_rejects_invalid_identifiers(
    invalid_channel: str,
) -> None:
    """Verify invalid channel names raise ProgrammingError with sqlstate 42602."""
    with pytest.raises(ProgrammingError) as exc_info:
        validate_oracle_channel_name(invalid_channel)
    assert exc_info.value.sqlstate == "42602"


@pytest.mark.anyio
async def test_oracle_notification_source_protocol_publish_subscribe_listen_and_change_feed() -> None:
    """Verify notification source protocol compliance, publish/subscribe/listen, and ChangeFeed envelopes."""
    async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(client=async_stub) as driver:
        source = driver.notification_source(
            OracleNotifyConfig(queue_prefix="VTB_AQ_", poll_timeout=0.05),
        )
        assert isinstance(source, AsyncNotificationSource)

        with pytest.raises(ProgrammingError) as exc_info:
            async for _ in source.listen():
                break
        assert exc_info.value.sqlstate == "42602"

        await source.subscribe(["orders"])
        await source.publish("orders", b"raw-order-payload")
        received_events = []
        async for evt in source.listen():
            received_events.append(evt)
            break

        assert len(received_events) == 1
        assert received_events[0].channel == "orders"
        assert received_events[0].payload == b"raw-order-payload"
        assert received_events[0].metadata["queue_name"] == "VTB_AQ_ORDERS"

        feed = ChangeFeed()
        cf_source = driver.notification_source(
            OracleNotifyConfig(queue_prefix="VTB_AQ_", poll_timeout=0.05),
            change_feed=feed,
        )
        assert cf_source.change_feed is feed
        await cf_source.subscribe(["invoices"])
        await cf_source.publish("invoices", "invoice-99")
        cf_events = []
        async for evt in cf_source.listen():
            cf_events.append(evt)
            break

        assert len(cf_events) == 1
        assert cf_events[0].channel == "invoices"
        assert cf_events[0].payload == b"invoice-99"
        assert cf_events[0].metadata["change_event_id"] == 1
        assert cf_events[0].metadata["spilled"] is False
        assert len(feed.replay_since(0)) == 1


@pytest.mark.anyio
async def test_oracle_notification_source_reconnect_backoff_and_cancellation_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify exponential reconnect backoff on transient OperationalError and listener cleanup on exit."""
    recorded_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        recorded_delays.append(delay)

    monkeypatch.setattr("vertebra.db.adapters.oracle.notify.asyncio.sleep", fake_sleep)

    async_stub = _StubAsyncOracleClient(listener_fail_modes=("transient", "transient", None))
    async_stub.aq_queues["VTB_AQ_RECOVER"] = deque(
        [
            _oracle.OracleNotification(
                "VTB_AQ_RECOVER",
                b"msg-recovered",
                b"recovered-ok",
                None,
                1700000000123,
            )
        ]
    )
    async with AsyncOracleDriver(client=async_stub) as driver:
        source = AsyncOracleNotificationSource(
            driver,
            OracleNotifyConfig(
                reconnect_initial_delay=0.25,
                reconnect_max_delay=15.0,
                max_reconnect_attempts=5,
                poll_timeout=0.05,
            ),
        )
        await source.subscribe(["recover"])
        async for evt in source.listen():
            assert evt.channel == "recover"
            assert evt.payload == b"recovered-ok"
            await source.close()
            break

        assert recorded_delays == [0.25, 0.5]
        assert len(async_stub.created_listeners) == 3
        assert all(listener.is_closed and listener.close_calls >= 1 for listener in async_stub.created_listeners)


def test_oracle_embedding_pushdown_extracts_native_vector_view_bytes_and_sequence() -> None:
    """Verify _extract_vector unwraps VectorView, ORACLE_VECTOR_MAGIC bytes, and tuple rows."""
    vec_view = VectorBatchView.from_f32_rows([[0.25, -0.5, 1.0]], normalize=False)[0]
    raw_bytes = OracleCore.encode_vector_f32([0.25, -0.5, 1.0])
    expected = [0.25, -0.5, 1.0]

    assert OracleEmbeddingPushdown._extract_vector(vec_view, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector({"vec": vec_view}, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector({"embed_vector": vec_view}, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector({"EMBED_VECTOR": raw_bytes}, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector((raw_bytes,), 3) == expected
    assert OracleEmbeddingPushdown._extract_vector(([0.25, -0.5, 1.0],), 3) == expected

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": [0.25, -0.5]}, 3)

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": [0.25, -0.5, 1.0, 2.0]}, 3)

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": [True, False, 1.0]}, 3)

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": "not-json"}, 3)


@pytest.mark.anyio
async def test_oracle_embedding_pushdown_batch_vectors_and_similarity_search() -> None:
    """Verify embed_documents and similarity_search unwrap and bind native VectorView over protocol doubles."""
    v1 = VectorBatchView.from_f32_rows([[0.25, -0.5, 1.0]], normalize=False)[0]
    v2 = VectorBatchView.from_f32_rows([[0.5, 0.25, -0.75]], normalize=False)[0]

    assert OracleEmbeddingPushdown._extract_batch_vectors(
        [{"utl_to_embeddings": [v1, v2]}],
        2,
        3,
    ) == [[0.25, -0.5, 1.0], [0.5, 0.25, -0.75]]
    assert OracleEmbeddingPushdown._extract_batch_vectors(
        [
            {"embed_data": {"embed_vector": [0.25, -0.5, 1.0]}},
            {"EMBED_DATA": {"EMBED_VECTOR": "[0.5, 0.25, -0.75]"}},
        ],
        2,
        3,
    ) == [[0.25, -0.5, 1.0], [0.5, 0.25, -0.75]]

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(
                [_oracle.OracleColumn("UTL_TO_EMBEDDINGS", "VECTOR")],
                [([v1, v2],)],
                rowcount=1,
            ),
            _oracle.OracleQueryResult(
                [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("DISTANCE", "BINARY_DOUBLE")],
                [(1, 0.05)],
                rowcount=1,
            ),
        ]
    )
    async with AsyncOracleDriver(client=async_stub) as driver:
        pushdown = OracleEmbeddingPushdown(
            driver,
            dimensions=3,
            provider="custom",
            normalize=False,
        )
        batch = await pushdown.embed_documents(["alpha", "beta"], normalize=False)
        assert isinstance(batch, VectorBatchView)
        assert len(batch) == 2
        assert batch[0].to_list() == [0.25, -0.5, 1.0]
        assert batch[1].to_list() == [0.5, 0.25, -0.75]

        rows = await pushdown.similarity_search(
            v1,
            table="docs",
            embedding_column="embedding",
            top_k=5,
            metric="cosine",
        )
        assert rows == [{"id": 1, "distance": 0.05}]
        query_calls = [payload for kind, payload in async_stub.calls if kind == "query"]
        assert "DBMS_VECTOR.UTL_TO_EMBEDDINGS" in query_calls[0]["sql"]
        search_call = query_calls[1]
        assert "VECTOR_DISTANCE(embedding, TO_VECTOR(:query_vector), COSINE)" in search_call["sql"]
        assert search_call["params"]["top_k"] == 5
        assert msgspec.json.decode(search_call["params"]["query_vector"]) == [0.25, -0.5, 1.0]


@pytest.mark.parametrize(
    ("wait_timeout_seconds", "valid"),
    [
        (1, True),
        (5, True),
        (30, True),
        (0, False),
        (-3, False),
    ],
)
def test_oracle_notify_config_validates_wait_timeout_seconds(
    wait_timeout_seconds: int,
    valid: bool,
) -> None:
    """Verify OracleNotifyConfig requires wait_timeout_seconds >= 1."""
    if valid:
        cfg = OracleNotifyConfig(wait_timeout_seconds=wait_timeout_seconds)
        assert cfg.wait_timeout_seconds == wait_timeout_seconds
    else:
        with pytest.raises(ValueError, match="wait_timeout_seconds"):
            OracleNotifyConfig(wait_timeout_seconds=wait_timeout_seconds)


@pytest.mark.parametrize(
    ("symbol_name", "target_module", "expected_module"),
    [
        ("SyncOracleDriver", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._sync"),
        ("SyncOracleDriver", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._sync"),
        ("SyncOracleDriver", "vertebra.db.adapters.oracle._sync", "vertebra.db.adapters.oracle._sync"),
        ("AsyncOracleDriver", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._async"),
        ("AsyncOracleDriver", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._async"),
        ("AsyncOracleDriver", "vertebra.db.adapters.oracle._async", "vertebra.db.adapters.oracle._async"),
        ("SyncOraclePipeline", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle.pipeline"),
        ("SyncOraclePipeline", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle.pipeline"),
        ("SyncOraclePipeline", "vertebra.db.adapters.oracle.pipeline", "vertebra.db.adapters.oracle.pipeline"),
        ("AsyncOraclePipeline", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle.pipeline"),
        ("AsyncOraclePipeline", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle.pipeline"),
        ("AsyncOraclePipeline", "vertebra.db.adapters.oracle.pipeline", "vertebra.db.adapters.oracle.pipeline"),
        ("SyncOraclePool", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._sync"),
        ("SyncOraclePool", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._sync"),
        ("SyncOraclePool", "vertebra.db.adapters.oracle._sync", "vertebra.db.adapters.oracle._sync"),
        ("AsyncOraclePool", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._async"),
        ("AsyncOraclePool", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._async"),
        ("AsyncOraclePool", "vertebra.db.adapters.oracle._async", "vertebra.db.adapters.oracle._async"),
    ],
)
def test_oracle_driver_and_pipeline_resolution_identity(
    symbol_name: str,
    target_module: str,
    expected_module: str,
) -> None:
    """Verify Oracle sync/async driver, pool, and pipeline classes resolve identically across import paths."""
    mod = importlib.import_module(target_module)
    exp_mod = importlib.import_module(expected_module)
    assert getattr(mod, symbol_name) is getattr(exp_mod, symbol_name)


@pytest.mark.parametrize(
    "removed_alias",
    [
        "decode_oson",
        "decode_oson_payload",
        "decode_tns_packet",
        "decode_ttc_session_attrs",
        "decode_vector_binary",
        "decode_vector_f32",
        "decode_vector_f64",
        "decode_vector_i8",
        "encode_oson",
        "encode_oson_payload",
        "encode_tns_connect_packet",
        "encode_tns_data_packet",
        "encode_ttc_session_attrs",
        "encode_vector_binary",
        "encode_vector_f32",
        "encode_vector_f64",
        "encode_vector_i8",
    ],
)
def test_oracle_removed_alias_shims_raise_attribute_error(removed_alias: str) -> None:
    """Verify removed module-level Oracle codec alias shims are absent from oracle and oracle.core."""
    with pytest.raises(AttributeError):
        getattr(oracle_core_mod, removed_alias)
    with pytest.raises(AttributeError):
        getattr(oracle_pkg, removed_alias)


def test_oracle_driver_defers_native_extension_import() -> None:
    """Verify importing vertebra.db.adapters.oracle.driver does not eagerly import vertebra._oracle."""
    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra._oracle"
        or name == "vertebra.db.adapters.oracle.driver"
        or name == "vertebra.db.adapters.oracle._sync"
        or name == "vertebra.db.adapters.oracle._async"
    }
    try:
        drv_mod = importlib.import_module("vertebra.db.adapters.oracle.driver")
        assert "vertebra._oracle" not in sys.modules
        flag = drv_mod.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED
        assert isinstance(flag, bool)
    finally:
        sys.modules.update(saved)
