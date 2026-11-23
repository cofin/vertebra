"""Shared offline Oracle client, pool, driver, and notification test doubles."""

from __future__ import annotations

import asyncio
import contextlib
import io
import socket
import zipfile
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from typing import Any, Final

import msgspec

import vertebra._oracle as _oracle
from vertebra.db.adapters.oracle.config import OracleDriverFeatures
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import (
    InterfaceError,
    NotFoundError,
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


def _stub_is_write_sql(sql: str) -> bool:
    trimmed = sql.lstrip()
    while trimmed.startswith("/*"):
        end = trimmed.find("*/")
        if end == -1:
            break
        trimmed = trimmed[end + 2 :].lstrip()
    upper = trimmed.upper()
    return not (upper.startswith("SELECT") or upper.startswith("WITH"))


def _stub_is_var_placeholder(value: Any) -> bool:
    return callable(getattr(value, "setvalue", None)) and hasattr(value, "type_code")


def _stub_var_type_hint(var: Any) -> str:
    type_code = getattr(var, "type_code", None)
    raw_name = getattr(type_code, "_name", None)
    if raw_name is None:
        raw_name = type_code.__name__ if isinstance(type_code, type) else str(type_code or "")
    upper = str(raw_name).upper()
    if upper == "BLOB":
        return "BLOB"
    if upper in {"CLOB", "NCLOB"}:
        return "CLOB"
    if upper in {"BINARY", "BFILE", "BYTES", "RAW"}:
        return "RAW"
    if upper in {"NUMBER", "INT", "FLOAT", "DECIMAL"}:
        return "NUMBER"
    if upper in {"DATETIME", "DATE", "TIMESTAMP"}:
        return "TIMESTAMP"
    if upper in {"BOOL", "BOOLEAN"}:
        return "BOOLEAN"
    return "VARCHAR2"


def _stub_partition_bind_vars(
    parameters: Sequence[Any] | Mapping[str, Any] | None,
    out_binds: Any = None,
) -> tuple[
    Sequence[Any] | Mapping[str, Any] | None,
    Any,
    dict[str, tuple[Any, str]] | list[tuple[Any, str]],
]:
    if parameters is None:
        return None, out_binds, {}
    if isinstance(parameters, Mapping):
        if not any(_stub_is_var_placeholder(v) for v in parameters.values()):
            return parameters, out_binds, {}
        in_params: dict[str, Any] = {}
        out_vars: dict[str, tuple[Any, str]] = {}
        derived_out: dict[str, str] = dict(out_binds) if isinstance(out_binds, Mapping) else {}
        for k, v in parameters.items():
            clean_k = str(k).lstrip(":")
            if _stub_is_var_placeholder(v):
                hint = str(derived_out.get(clean_k) or _stub_var_type_hint(v))
                derived_out[clean_k] = hint
                out_vars[clean_k] = (v, hint)
            else:
                in_params[str(k)] = v
        return in_params, derived_out, out_vars
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        if not any(_stub_is_var_placeholder(v) for v in parameters):
            return parameters, out_binds, []
        in_seq: list[Any] = []
        out_seq: list[tuple[Any, str]] = []
        derived_seq: list[str] = (
            [str(item) for item in out_binds]
            if isinstance(out_binds, Sequence) and not isinstance(out_binds, (str, bytes, bytearray))
            else []
        )
        for v in parameters:
            if _stub_is_var_placeholder(v):
                hint = _stub_var_type_hint(v)
                derived_seq.append(hint)
                out_seq.append((v, hint))
            else:
                in_seq.append(v)
        return in_seq, derived_seq, out_seq
    return parameters, out_binds, {}


def _stub_fill_bind_vars(
    out_vars: dict[str, tuple[Any, str]] | list[tuple[Any, str]],
    result: Any,
    *,
    features: OracleDriverFeatures,
) -> Any:
    raw_out = getattr(result, "out_binds", None)
    if raw_out is None:
        return None
    if isinstance(raw_out, Mapping):
        lower_map = {str(k).lstrip(":").lower(): v for k, v in raw_out.items()}
        coerced_map: dict[str, Any] = {}
        if isinstance(out_vars, dict) and out_vars:
            for clean_k, (var, hint) in out_vars.items():
                raw_val = raw_out.get(clean_k, lower_map.get(clean_k.lower()))
                coerced_val = OracleCore.coerce_row_cell(raw_val, hint, features=features)
                var.setvalue(0, coerced_val)
                coerced_map[clean_k] = coerced_val
            for k, v in raw_out.items():
                norm_k = str(k).lstrip(":")
                if norm_k not in coerced_map:
                    coerced_map[norm_k] = OracleCore.coerce_row_cell(v, None, features=features)
            return coerced_map
        return {str(k).lstrip(":"): OracleCore.coerce_row_cell(v, None, features=features) for k, v in raw_out.items()}
    if isinstance(raw_out, Sequence) and not isinstance(raw_out, (str, bytes, bytearray)):
        var_list = out_vars if isinstance(out_vars, list) else list(out_vars.values())
        coerced_list: list[Any] = []
        for idx, raw_val in enumerate(raw_out):
            seq_hint: str | None = var_list[idx][1] if idx < len(var_list) else None
            coerced_val = OracleCore.coerce_row_cell(raw_val, seq_hint, features=features)
            if idx < len(var_list):
                var_list[idx][0].setvalue(0, coerced_val)
            coerced_list.append(coerced_val)
        return tuple(coerced_list)
    return raw_out


class _StubSyncOracleCursor:
    """Protocol double matching `_oracle.SyncOracleCursor` for offline driver tests."""

    def __init__(
        self,
        client: _StubSyncOracleClient,
        *,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
        lazy_begin: bool = False,
    ) -> None:
        self._client = client
        self.arraysize: int = 256
        self.description: tuple[tuple[str, str, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: str | None = None
        self.touches_session_context: bool = False
        self.out_binds: dict[str, Any] | tuple[Any, ...] | None = None
        self.closed: bool = False
        self.lazy_begin: bool = lazy_begin
        self.options: dict[str, Any] = {
            "enable_uuid_binary": enable_uuid_binary,
            "fetch_decimals": fetch_decimals,
            "vector_return_format": vector_return_format,
            "enable_lowercase_column_names": enable_lowercase_column_names,
        }
        self._features = OracleDriverFeatures(
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0

    def var(
        self,
        type_code: Any,
        size: int = 0,
        arraysize: int = 1,
        outconverter: Callable[[Any], Any] | None = None,
    ) -> _oracle.OracleOutBindVar:
        """Create an `OracleOutBindVar` placeholder."""
        return _oracle.OracleOutBindVar(type_code, size=size, arraysize=arraysize, outconverter=outconverter)

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
    ) -> _StubSyncOracleCursor:
        """Execute SQL against the owning stub client and populate cursor state."""
        if self.closed:
            raise InterfaceError("Oracle cursor is closed")
        raw_params = None if params == () else params
        in_params, resolved_out_binds, out_vars = _stub_partition_bind_vars(raw_params, out_binds)
        coerced = OracleCore.coerce_parameters(in_params, features=self._features)
        if _stub_is_write_sql(sql):
            result = self._client.execute(
                sql,
                coerced,
                out_binds=resolved_out_binds,
                fetch_decimals=self._features.fetch_decimals,
                enable_lowercase_column_names=self._features.enable_lowercase_column_names,
            )
        else:
            result = self._client.query(
                sql,
                coerced,
                out_binds=resolved_out_binds,
                fetch_decimals=self._features.fetch_decimals,
                enable_lowercase_column_names=self._features.enable_lowercase_column_names,
            )
        self.out_binds = _stub_fill_bind_vars(out_vars, result, features=self._features)
        rows, cols = OracleCore.coerce_result_rows_and_cols(
            result,
            features=self._features,
            out_hints=resolved_out_binds,
        )
        if cols:
            self.description = tuple((name, "UNKNOWN", None, None, None, None, None) for name in cols)
        else:
            self.description = None
        self._rows = list(rows)
        self._pos = 0
        self.rowcount = int(result.rowcount)
        self.lastrowid = result.lastrowid
        upper = sql.upper()
        self.touches_session_context = (
            "DBMS_SESSION" in upper or "SET_IDENTIFIER" in upper or "CLEAR_IDENTIFIER" in upper
        )
        return self

    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> _StubSyncOracleCursor:
        """Execute a DML batch against the owning stub client."""
        if self.closed:
            raise InterfaceError("Oracle cursor is closed")
        coerced_seq = [OracleCore.coerce_parameters(p, features=self._features) or () for p in seq_of_params]
        count = self._client.execute_many(sql, coerced_seq)
        self.description = None
        self._rows = []
        self._pos = 0
        self.rowcount = int(count)
        self.lastrowid = None
        self.out_binds = None
        self.touches_session_context = False
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next buffered row or `None`."""
        if self.closed:
            raise InterfaceError("Oracle cursor is closed")
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` buffered rows."""
        if self.closed:
            raise InterfaceError("Oracle cursor is closed")
        count = self.arraysize if size is None else max(0, int(size))
        chunk = self._rows[self._pos : self._pos + count]
        self._pos += len(chunk)
        return chunk

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining buffered rows."""
        if self.closed:
            raise InterfaceError("Oracle cursor is closed")
        chunk = self._rows[self._pos :]
        self._pos = len(self._rows)
        return chunk

    def close(self) -> None:
        """Close the cursor idempotently."""
        self.closed = True
        self._rows = []

    def __enter__(self) -> _StubSyncOracleCursor:
        """Enter the cursor context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the cursor on context exit."""
        self.close()


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
        self.calls: list[tuple[Any, ...]] = []
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
        self._lease_releaser: Callable[[], None] | None = None
        self.attach_lease_calls: int = 0
        self.release_lease_calls: int = 0
        self.cursor_calls: list[dict[str, Any]] = []

    def attach_lease(self, release: Callable[[], None]) -> None:
        """Attach a checkout lease releaser to this stub client."""
        self.attach_lease_calls += 1
        self._lease_releaser = release

    def release_lease(self) -> None:
        """Release any attached checkout lease idempotently."""
        self.release_lease_calls += 1
        releaser = self._lease_releaser
        self._lease_releaser = None
        if releaser is not None:
            releaser()

    def cursor(
        self,
        *,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
        lazy_begin: bool = False,
    ) -> _StubSyncOracleCursor:
        """Create a `_StubSyncOracleCursor` bound to this stub client."""
        self._ensure_open()
        opts: dict[str, Any] = {
            "enable_uuid_binary": enable_uuid_binary,
            "fetch_decimals": fetch_decimals,
            "vector_return_format": vector_return_format,
            "enable_lowercase_column_names": enable_lowercase_column_names,
            "lazy_begin": lazy_begin,
        }
        self.cursor_calls.append(opts)
        return _StubSyncOracleCursor(self, **opts)

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Run `sql` directly via a cursor helper without recording a `cursor()` call."""
        self._ensure_open()
        self.calls.append(
            (
                "fetch_rows",
                {
                    "sql": sql,
                    "params": params,
                    "out_binds": out_binds,
                    "fetch_decimals": fetch_decimals,
                    "enable_lowercase_column_names": enable_lowercase_column_names,
                },
            )
        )
        cur = _StubSyncOracleCursor(
            self,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        try:
            cur.execute(sql, params, out_binds=out_binds)
            if cur.description is None:
                return ([], ())
            names = tuple(col[0] for col in cur.description)
            return (cur.fetchall(), names)
        finally:
            cur.close()

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> list[dict[str, Any]]:
        """Run `sql` directly via a cursor helper and return row dictionaries."""
        self._ensure_open()
        self.calls.append(
            (
                "fetch_dicts",
                {
                    "sql": sql,
                    "params": params,
                    "out_binds": out_binds,
                    "fetch_decimals": fetch_decimals,
                    "enable_lowercase_column_names": enable_lowercase_column_names,
                },
            )
        )
        cur = _StubSyncOracleCursor(
            self,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        try:
            cur.execute(sql, params, out_binds=out_binds)
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
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Run `sql` directly via a cursor helper and return the first scalar cell."""
        self._ensure_open()
        self.calls.append(
            (
                "fetch_value",
                {
                    "sql": sql,
                    "params": params,
                    "out_binds": out_binds,
                    "fetch_decimals": fetch_decimals,
                    "enable_lowercase_column_names": enable_lowercase_column_names,
                },
            )
        )
        cur = _StubSyncOracleCursor(
            self,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        try:
            cur.execute(sql, params, out_binds=out_binds)
            row = cur.fetchone()
            if row is None:
                raise NotFoundError("Query returned no rows")
            if not row:
                return None
            return row[0]
        finally:
            cur.close()

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool]:
        """Run DML/PLSQL `sql` directly via a cursor helper and return the four-item tuple."""
        self._ensure_open()
        self.calls.append(
            (
                "execute_dml",
                {
                    "sql": sql,
                    "params": params,
                    "out_binds": out_binds,
                    "fetch_decimals": fetch_decimals,
                    "enable_lowercase_column_names": enable_lowercase_column_names,
                },
            )
        )
        cur = _StubSyncOracleCursor(
            self,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        try:
            cur.execute(sql, params, out_binds=out_binds)
            return (max(cur.rowcount, 0), cur.lastrowid, cur.out_binds, cur.touches_session_context)
        finally:
            cur.close()

    def __del__(self) -> None:
        releaser = getattr(self, "_lease_releaser", None)
        if releaser is not None:
            self._lease_releaser = None
            with contextlib.suppress(Exception):
                releaser()

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
        self.release_lease()
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


def _proxy_attr(name: str, *, writable: bool = False) -> property:
    """Return a property forwarding reads (and optionally writes) to ``self._sync.<name>``."""

    def _get(self: Any) -> Any:
        return getattr(self._sync, name)

    if writable:

        def _set(self: Any, value: Any) -> None:
            setattr(self._sync, name, value)

        return property(_get, _set)
    return property(_get)


def _proxy_async(name: str, *, yield_loop: bool = True, return_self: bool = False) -> Any:
    """Return an async method forwarding calls to ``self._sync.<name>(*args, **kwargs)``."""

    async def _method(self: Any, *args: Any, **kwargs: Any) -> Any:
        if yield_loop:
            await asyncio.sleep(0)
        result = getattr(self._sync, name)(*args, **kwargs)
        return self if return_self else result

    _method.__name__ = name
    return _method


def _proxy_sync(name: str) -> Any:
    """Return a sync method forwarding calls to ``self._sync.<name>(*args, **kwargs)``."""

    def _method(self: Any, *args: Any, **kwargs: Any) -> Any:
        return getattr(self._sync, name)(*args, **kwargs)

    _method.__name__ = name
    return _method


class _StubAsyncOracleCursor:
    """Protocol double matching `_oracle.AsyncOracleCursor` for offline async driver tests."""

    def __init__(self, sync_cursor: _StubSyncOracleCursor) -> None:
        self._sync = sync_cursor

    arraysize: Any = _proxy_attr("arraysize", writable=True)
    description: Any = _proxy_attr("description")
    rowcount: Any = _proxy_attr("rowcount")
    lastrowid: Any = _proxy_attr("lastrowid")
    touches_session_context: Any = _proxy_attr("touches_session_context")
    out_binds: Any = _proxy_attr("out_binds")
    closed: Any = _proxy_attr("closed")
    lazy_begin: Any = _proxy_attr("lazy_begin")
    options: Any = _proxy_attr("options")
    var: Any = _proxy_sync("var")
    execute: Any = _proxy_async("execute", return_self=True)
    executemany: Any = _proxy_async("executemany", return_self=True)
    fetchone: Any = _proxy_async("fetchone")
    fetchmany: Any = _proxy_async("fetchmany")
    fetchall: Any = _proxy_async("fetchall")
    close: Any = _proxy_async("close", yield_loop=False)


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
        self._sync = _StubSyncOracleClient(cclass=cclass, purity=purity, connection_tag=connection_tag)
        self._listener_fail_modes: deque[str | None] = deque(listener_fail_modes)
        self.created_listeners: list[_StubAsyncListener] = []

    closed: Any = _proxy_attr("closed")
    in_transaction: Any = _proxy_attr("in_transaction")
    cclass: Any = _proxy_attr("cclass")
    purity: Any = _proxy_attr("purity", writable=True)
    connection_tag: Any = _proxy_attr("connection_tag", writable=True)
    tag_matched: Any = _proxy_attr("tag_matched", writable=True)
    calls: Any = _proxy_attr("calls")
    queued_results: Any = _proxy_attr("queued_results")
    queued_arrow: Any = _proxy_attr("queued_arrow")
    queued_pipeline: Any = _proxy_attr("queued_pipeline")
    aq_queues: Any = _proxy_attr("aq_queues")
    pending_session_attrs: Any = _proxy_attr("pending_session_attrs")
    active_session_attrs: Any = _proxy_attr("active_session_attrs")
    last_piggyback_frame: Any = _proxy_attr("last_piggyback_frame")
    attach_lease_calls: Any = _proxy_attr("attach_lease_calls")
    release_lease_calls: Any = _proxy_attr("release_lease_calls")
    cursor_calls: Any = _proxy_attr("cursor_calls")

    attach_lease: Any = _proxy_sync("attach_lease")
    release_lease: Any = _proxy_sync("release_lease")
    set_session_attrs: Any = _proxy_sync("set_session_attrs")
    cancel: Any = _proxy_sync("cancel")

    connect: Any = _proxy_async("connect", yield_loop=False)
    ping: Any = _proxy_async("ping", yield_loop=False)
    close: Any = _proxy_async("close", yield_loop=False)
    begin: Any = _proxy_async("begin", yield_loop=False)
    commit: Any = _proxy_async("commit", yield_loop=False)
    rollback: Any = _proxy_async("rollback", yield_loop=False)
    savepoint: Any = _proxy_async("savepoint", yield_loop=False)
    rollback_to_savepoint: Any = _proxy_async("rollback_to_savepoint", yield_loop=False)

    fetch_rows: Any = _proxy_async("fetch_rows")
    fetch_dicts: Any = _proxy_async("fetch_dicts")
    fetch_value: Any = _proxy_async("fetch_value")
    execute_dml: Any = _proxy_async("execute_dml")
    query: Any = _proxy_async("query")
    execute: Any = _proxy_async("execute")
    execute_many: Any = _proxy_async("execute_many")
    query_arrow: Any = _proxy_async("query_arrow")
    execute_pipeline_batch: Any = _proxy_async("execute_pipeline_batch")
    enqueue_notification: Any = _proxy_async("enqueue_notification")

    def cursor(
        self,
        *,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
        lazy_begin: bool = False,
    ) -> _StubAsyncOracleCursor:
        """Create a `_StubAsyncOracleCursor` bound to this stub client."""
        return _StubAsyncOracleCursor(
            self._sync.cursor(
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
                lazy_begin=lazy_begin,
            )
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
        self.single_call_results: deque[_oracle.OracleQueryResult | BaseException] = deque()
        self.single_calls: list[tuple[str, str, Any]] = []

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

    def _run_single_call(
        self,
        method_name: str,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None,
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None,
        enable_uuid_binary: bool,
        fetch_decimals: bool,
        vector_return_format: str,
        enable_lowercase_column_names: bool,
    ) -> Any:
        self.single_calls.append((method_name, sql, params))
        client = self.acquire()
        if self.single_call_results:
            client.queued_results.append(self.single_call_results.popleft())
        discard = False
        try:
            method = getattr(client, method_name)
            return method(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            )
        except BaseException as exc:
            discard = OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            self.release(client, discard=discard)

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Check out a stub client, run `fetch_rows`, and release it."""
        result: tuple[list[tuple[Any, ...]], tuple[str, ...]] = self._run_single_call(
            "fetch_rows",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        return result

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> list[dict[str, Any]]:
        """Check out a stub client, run `fetch_dicts`, and release it."""
        result: list[dict[str, Any]] = self._run_single_call(
            "fetch_dicts",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        return result

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Check out a stub client, run `fetch_value`, and release it."""
        return self._run_single_call(
            "fetch_value",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool]:
        """Check out a stub client, run `execute_dml`, and release it."""
        result: tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool] = self._run_single_call(
            "execute_dml",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        return result

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
        self.single_call_results: deque[_oracle.OracleQueryResult | BaseException] = deque()
        self.single_calls: list[tuple[str, str, Any]] = []

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

    async def _run_single_call(
        self,
        method_name: str,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None,
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None,
        enable_uuid_binary: bool,
        fetch_decimals: bool,
        vector_return_format: str,
        enable_lowercase_column_names: bool,
    ) -> Any:
        self.single_calls.append((method_name, sql, params))
        client = await self.acquire()
        if self.single_call_results:
            client.queued_results.append(self.single_call_results.popleft())
        discard = False
        try:
            method = getattr(client, method_name)
            return await method(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            )
        except BaseException as exc:
            discard = OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            await self.release(client, discard=discard)

    async def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Check out an async stub client, await `fetch_rows`, and release it."""
        result: tuple[list[tuple[Any, ...]], tuple[str, ...]] = await self._run_single_call(
            "fetch_rows",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        return result

    async def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> list[dict[str, Any]]:
        """Check out an async stub client, await `fetch_dicts`, and release it."""
        result: list[dict[str, Any]] = await self._run_single_call(
            "fetch_dicts",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        return result

    async def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Check out an async stub client, await `fetch_value`, and release it."""
        return await self._run_single_call(
            "fetch_value",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )

    async def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool]:
        """Check out an async stub client, await `execute_dml`, and release it."""
        result: tuple[int, str | None, dict[str, Any] | tuple[Any, ...] | None, bool] = await self._run_single_call(
            "execute_dml",
            sql,
            params,
            out_binds=out_binds,
            enable_uuid_binary=enable_uuid_binary,
            fetch_decimals=fetch_decimals,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        return result


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
    def transaction(self) -> Iterator[_FakeSyncOracleDriver]:
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
    async def transaction(self) -> AsyncIterator[_FakeAsyncOracleDriver]:
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
