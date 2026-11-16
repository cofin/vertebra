"""Unit and live-wire loopback tests for SyncPostgresPipeline and AsyncPostgresPipeline."""

from __future__ import annotations

import asyncio
import socket
import struct
import threading
from datetime import UTC, datetime
from typing import Any

import pytest

from vertebra._postgres import AsyncPostgresClient, SyncPostgresClient
from vertebra._vertebra import (
    DataError,
    IntegrityError,
    InterfaceError,
    NotSupportedError,
)
from vertebra.db.adapters.postgres.pipeline import (
    AsyncPipelineClientProtocol,
    AsyncPostgresPipeline,
    SyncPipelineClientProtocol,
    SyncPostgresPipeline,
)
from vertebra.db.adapters.postgres.types import (
    PgArray,
    PgComposite,
    PgJson,
    PgParameter,
    PostgresColumn,
    PostgresPipelineOperation,
    PostgresPipelineOutcome,
    PostgresQueryResult,
)
from vertebra.db.driver import AsyncPipeline, SyncPipeline


class _RecordingSyncClient:
    """Deterministic structural double satisfying SyncPipelineClientProtocol without _postgres I/O."""

    def __init__(self, outcomes: tuple[PostgresPipelineOutcome, ...] = ()) -> None:
        """Initialize recorded event log and canned outcomes."""
        self.events: list[str] = []
        self.captured_ops: tuple[PostgresPipelineOperation, ...] = ()
        self.finish_args: list[tuple[int, bool]] = []
        self._outcomes = outcomes

    def pipeline_begin(self) -> int:
        """Record pipeline_begin and return scope identifier 17."""
        self.events.append("begin")
        return 17

    def pipeline_results(
        self,
        scope_id: int,
        operations: Any,
    ) -> tuple[PostgresPipelineOutcome, ...]:
        """Record submitted operations and return canned outcomes."""
        assert scope_id == 17
        self.events.append("results")
        self.captured_ops = tuple(operations)
        return self._outcomes

    def pipeline_finish(self, scope_id: int, success: bool) -> None:
        """Record pipeline_finish invocation order and commit flag."""
        self.events.append(f"finish:{success}")
        self.finish_args.append((scope_id, success))


class _RecordingAsyncClient:
    """Deterministic structural double satisfying AsyncPipelineClientProtocol."""

    def __init__(self, outcomes: tuple[PostgresPipelineOutcome, ...] = ()) -> None:
        """Initialize recorded event log and canned outcomes."""
        self.events: list[str] = []
        self.captured_ops: tuple[PostgresPipelineOperation, ...] = ()
        self.finish_args: list[tuple[int, bool]] = []
        self._outcomes = outcomes

    async def pipeline_begin(self) -> int:
        """Record async pipeline_begin and return scope identifier 23."""
        self.events.append("begin")
        return 23

    async def pipeline_results(
        self,
        scope_id: int,
        operations: Any,
    ) -> tuple[PostgresPipelineOutcome, ...]:
        """Record async submitted operations and return canned outcomes."""
        assert scope_id == 23
        self.events.append("results")
        self.captured_ops = tuple(operations)
        return self._outcomes

    async def pipeline_finish(self, scope_id: int, success: bool) -> None:
        """Record async pipeline_finish invocation order and commit flag."""
        self.events.append(f"finish:{success}")
        self.finish_args.append((scope_id, success))


def test_pipeline_protocol_satisfaction_and_empty_batch_skips_results_call() -> None:
    """Verify runtime protocol checks, empty results() == [], and release ordering after finish."""
    client = _RecordingSyncClient()
    assert isinstance(client, SyncPipelineClientProtocol)

    pipe = SyncPostgresPipeline(client, release=lambda: client.events.append("release"))
    assert isinstance(pipe, SyncPipeline)

    with pipe as p:
        assert p.results() == []

    assert client.events == ["begin", "finish:True", "release"]
    assert client.finish_args == [(17, True)]


def test_enqueue_deep_snapshots_nested_mutable_containers_and_projects_pgjson_null() -> None:
    """Verify post-enqueue mutations to nested dicts/lists/bytearray/memoryview/wrappers do not leak."""
    dup_cols = (
        PostgresColumn(name="dup", type_oid=23, format=0, type_modifier=-1, table_oid=0, attribute_number=0),
        PostgresColumn(name="dup", type_oid=23, format=0, type_modifier=-1, table_oid=0, attribute_number=0),
        PostgresColumn(name="payload", type_oid=3802, format=1, type_modifier=-1, table_oid=0, attribute_number=0),
        PostgresColumn(name="nested_arr", type_oid=3807, format=1, type_modifier=-1, table_oid=0, attribute_number=0),
    )
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    canned = (
        PostgresPipelineOutcome(
            index=0,
            status="result",
            result=PostgresQueryResult(
                columns=(),
                rows=(),
                command_tag="INSERT 0 2",
                affected_rows=2,
                transaction_status="T",
            ),
        ),
        PostgresPipelineOutcome(
            index=1,
            status="result",
            result=PostgresQueryResult(
                columns=dup_cols,
                rows=((1, 9, PgJson(None), [PgJson(None), ts]),),
                command_tag="SELECT 1",
                affected_rows=1,
                transaction_status="T",
            ),
        ),
    )
    client = _RecordingSyncClient(outcomes=canned)

    mutable_dict: dict[str, Any] = {"tags": ["a", "b"]}
    mutable_buf = bytearray(b"hello")
    mutable_view_backing = bytearray(b"world")
    mem_view = memoryview(mutable_view_backing)
    mutable_json_inner: dict[str, Any] = {"nested": [1, 2]}
    arr_inner_list: list[Any] = [bytearray(b"x")]
    comp_inner = bytearray(b"c")

    params: list[Any] = [
        mutable_dict,
        mutable_buf,
        mem_view,
        PgJson(mutable_json_inner),
        PgArray(values=(arr_inner_list,), dimensions=(1,), lower_bounds=(1,), element_oid=17),
        PgComposite(type_oid=99001, field_names=("f",), field_oids=(17,), values=(comp_inner,)),
        PgParameter(value=bytearray(b"typed"), type_oid=17),
    ]

    with SyncPostgresPipeline(client) as pipe:
        pipe.execute("INSERT INTO t VALUES ($1,$2,$3,$4,$5,$6,$7)", params)
        pipe.select("SELECT 1")

        params.clear()
        mutable_dict["tags"].append("mutated")
        mutable_buf[0] = ord("X")
        mutable_view_backing[0] = ord("Y")
        mutable_json_inner["nested"].append(999)
        arr_inner_list.append(bytearray(b"z"))
        comp_inner[0] = ord("Z")

        projected = pipe.results()

    assert projected == [
        2,
        [{"dup": 9, "payload": None, "nested_arr": [PgJson(None), ts]}],
    ]
    captured = client.captured_ops[0].parameters
    assert captured[0] == {"tags": ["a", "b"]}
    assert captured[1] == b"hello"
    assert captured[2] == b"world"
    assert captured[3] == PgJson({"nested": [1, 2]})
    assert captured[4] == PgArray(values=([b"x"],), dimensions=(1,), lower_bounds=(1,), element_oid=17)
    assert captured[5] == PgComposite(type_oid=99001, field_names=("f",), field_oids=(17,), values=(b"c",))
    assert captured[6] == PgParameter(value=b"typed", type_oid=17)


def test_caught_prepare_failure_and_cycle_rejection_force_sticky_rollback() -> None:
    """Verify caught Mapping prepare error and cyclic lists force rollback and run release once."""
    client = _RecordingSyncClient()
    with SyncPostgresPipeline(client, release=lambda: client.events.append("release")) as pipe:
        with pytest.raises(InterfaceError):
            pipe.execute("INSERT INTO t VALUES (:x)", {"x": 1})
        with pytest.raises(InterfaceError):
            pipe.results()

    assert client.events == ["begin", "finish:False", "release"]

    cyc_client = _RecordingSyncClient()
    cyclic: list[Any] = []
    cyclic.append(cyclic)

    async def _async_cycle() -> None:
        ac = _RecordingAsyncClient()
        assert isinstance(ac, AsyncPipelineClientProtocol)
        ap = AsyncPostgresPipeline(ac, release=lambda: _async_mark(ac.events, "release"))
        assert isinstance(ap, AsyncPipeline)
        with pytest.raises(ValueError, match="caller boom"):
            async with ap as p:
                with pytest.raises(DataError, match="cycle"):
                    p.execute("SELECT $1", [cyclic])
                raise ValueError("caller boom")
        assert ac.events == ["begin", "finish:False", "release"]

    with SyncPostgresPipeline(cyc_client) as p2, pytest.raises(DataError, match="cycle"):
        p2.select("SELECT $1", [{"self": cyclic}])
    assert cyc_client.finish_args == [(17, False)]

    asyncio.run(_async_cycle())


async def _async_mark(log: list[str], entry: str) -> None:
    """Append an event entry asynchronously."""
    log.append(entry)


def _frame(tag: bytes, body: bytes) -> bytes:
    """Encode a 5-byte PostgreSQL Protocol 3.0 wire frame."""
    return tag + struct.pack("!i", len(body) + 4) + body


def _auth_ok_and_ready() -> bytes:
    """Build AuthenticationOk + baseline ParameterStatus + BackendKeyData + ReadyForQuery('I')."""
    return b"".join(
        (
            _frame(b"R", struct.pack("!i", 0)),
            _frame(b"S", b"application_name\x00vertebra\x00"),
            _frame(b"S", b"client_encoding\x00UTF8\x00"),
            _frame(b"S", b"DateStyle\x00ISO, MDY\x00"),
            _frame(b"S", b"TimeZone\x00UTC\x00"),
            _frame(b"K", struct.pack("!ii", 7100, 12345)),
            _frame(b"Z", b"I"),
        )
    )


def _error_frame(sqlstate: str, message: str) -> bytes:
    """Build an ErrorResponse wire frame."""
    body = b"SERROR\x00VERROR\x00C" + sqlstate.encode("ascii") + b"\x00M" + message.encode("utf-8") + b"\x00\x00"
    return _frame(b"E", body)


def _read_exact(conn: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from a socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def _read_group(conn: socket.socket) -> list[tuple[bytes, bytes]]:
    """Read frontend frames until Sync ('S'), Query ('Q'), or Terminate ('X')."""
    out: list[tuple[bytes, bytes]] = []
    while True:
        hdr = _read_exact(conn, 5)
        tag = hdr[:1]
        (length,) = struct.unpack("!i", hdr[1:5])
        body = _read_exact(conn, length - 4)
        out.append((tag, body))
        if tag in (b"S", b"Q", b"X"):
            return out


class _ScriptedPipelineServer:
    """Loopback wire server recording simple-query boundaries and single-Sync pipeline batches."""

    def __init__(self, *, mode: str) -> None:
        """Bind ephemeral port and serve a single client session according to mode."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(2)
        self._sock.settimeout(3.0)
        self.port: int = self._sock.getsockname()[1]
        self.mode = mode
        self.simple_queries: list[str] = []
        self.sync_batches: list[list[bytes]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        """Serve authentication and scripted extended pipeline frames."""
        with self._sock:
            conn, _ = self._sock.accept()
            with conn:
                conn.settimeout(3.0)
                (slen,) = struct.unpack("!i", _read_exact(conn, 4))
                _read_exact(conn, slen - 4)
                conn.sendall(_auth_ok_and_ready())
                tx = b"I"
                while True:
                    try:
                        grp = _read_group(conn)
                    except (EOFError, OSError):
                        return
                    last_tag, last_body = grp[-1]
                    if last_tag == b"X":
                        return
                    if last_tag == b"Q":
                        sql = last_body.rstrip(b"\x00").decode("utf-8")
                        self.simple_queries.append(sql)
                        up = sql.upper()
                        if up == "BEGIN":
                            tx = b"T"
                            conn.sendall(_frame(b"C", b"BEGIN\x00") + _frame(b"Z", tx))
                        elif up == "COMMIT":
                            tx = b"I"
                            conn.sendall(_frame(b"C", b"COMMIT\x00") + _frame(b"Z", tx))
                        elif up == "ROLLBACK":
                            tx = b"I"
                            conn.sendall(_frame(b"C", b"ROLLBACK\x00") + _frame(b"Z", tx))
                        continue

                    tags = [t for t, _ in grp]
                    self.sync_batches.append(tags)
                    if self.mode == "commit_3op":
                        col = struct.pack("!h", 1) + b"name\x00" + struct.pack("!ihihih", 0, 0, 25, -1, -1, 0)
                        val = b"updated"
                        row = struct.pack("!hi", 1, len(val)) + val
                        resp = b"".join(
                            (
                                _frame(b"1", b""),
                                _frame(b"t", struct.pack("!h", 1) + struct.pack("!I", 25)),
                                _frame(b"n", b""),
                                _frame(b"2", b""),
                                _frame(b"n", b""),
                                _frame(b"C", b"INSERT 0 1\x00"),
                                _frame(b"1", b""),
                                _frame(b"t", struct.pack("!h", 1) + struct.pack("!I", 25)),
                                _frame(b"n", b""),
                                _frame(b"2", b""),
                                _frame(b"n", b""),
                                _frame(b"C", b"UPDATE 1\x00"),
                                _frame(b"1", b""),
                                _frame(b"t", struct.pack("!h", 0)),
                                _frame(b"T", col),
                                _frame(b"2", b""),
                                _frame(b"T", col),
                                _frame(b"D", row),
                                _frame(b"C", b"SELECT 1\x00"),
                                _frame(b"Z", tx),
                            )
                        )
                        conn.sendall(resp)
                    elif self.mode == "fail_op1_23505":
                        tx = b"E"
                        resp = b"".join(
                            (
                                _frame(b"1", b""),
                                _frame(b"t", struct.pack("!h", 0)),
                                _frame(b"n", b""),
                                _frame(b"2", b""),
                                _frame(b"n", b""),
                                _frame(b"C", b"INSERT 0 1\x00"),
                                _frame(b"1", b""),
                                _frame(b"t", struct.pack("!h", 0)),
                                _frame(b"n", b""),
                                _frame(b"2", b""),
                                _frame(b"n", b""),
                                _error_frame("23505", "duplicate key value violates unique constraint"),
                                _frame(b"Z", tx),
                            )
                        )
                        conn.sendall(resp)

    def join(self) -> None:
        """Wait for server thread exit."""
        self._thread.join(timeout=3.0)


def test_sync_live_pipeline_commits_3op_single_sync_and_blocks_raw_client_calls() -> None:
    """Verify SyncPostgresPipeline executes 3 ops in 1 Sync, blocks raw client calls, and commits."""
    srv = _ScriptedPipelineServer(mode="commit_3op")
    client = SyncPostgresClient(
        user="postgres",
        database="appdb",
        host="127.0.0.1",
        port=srv.port,
        sslmode="disable",
        channel_binding="disable",
    )
    client.connect()
    try:
        with SyncPostgresPipeline(client) as pipe:
            pipe.execute("INSERT INTO items(name) VALUES ($1)", ["init"])
            pipe.execute("UPDATE items SET name = $1", ["updated"])
            pipe.select("SELECT name FROM items")

            with pytest.raises(InterfaceError):
                client.query("SELECT 1")
            with pytest.raises(InterfaceError):
                client.commit()

            out = pipe.results()
            assert out == [1, 1, [{"name": "updated"}]]

            with pytest.raises(InterfaceError):
                client.commit()
    finally:
        client.close()
        srv.join()

    assert srv.simple_queries == ["BEGIN", "COMMIT"]
    assert len(srv.sync_batches) == 1
    assert srv.sync_batches[0].count(b"S") == 1


def test_async_live_pipeline_mid_batch_23505_attaches_outcomes_and_rolls_back() -> None:
    """Verify AsyncPostgresPipeline raises IntegrityError(23505, failing_index=1) and rolls back."""
    srv = _ScriptedPipelineServer(mode="fail_op1_23505")
    events: list[str] = []

    async def _release() -> None:
        events.append("released")

    async def _run() -> None:
        client = AsyncPostgresClient(
            user="postgres",
            database="appdb",
            host="127.0.0.1",
            port=srv.port,
            sslmode="disable",
            channel_binding="disable",
        )
        await client.connect()
        try:
            with pytest.raises(IntegrityError) as exc_info:
                async with AsyncPostgresPipeline(client, release=_release) as pipe:
                    pipe.execute("INSERT INTO items VALUES (1)")
                    pipe.execute("INSERT INTO items VALUES (1)")
                    pipe.select("SELECT * FROM items")
                    await pipe.results()

            err: Any = exc_info.value
            assert err.sqlstate == "23505"
            assert err.failing_index == 1
            outcomes = err.outcomes
            assert tuple(o.status for o in outcomes) == ("result", "error", "skipped")
            assert events == ["released"]
        finally:
            await client.close()

    asyncio.run(_run())
    srv.join()
    assert srv.simple_queries == ["BEGIN", "ROLLBACK"]
    assert len(srv.sync_batches) == 1


@pytest.mark.parametrize(
    ("forbidden_sql", "expected_exc"),
    [
        ("; /* comment */ COMMIT", InterfaceError),
        ("PREPARE TRANSACTION 'gid_1'", InterfaceError),
        ("COPY items TO STDOUT", NotSupportedError),
    ],
)
def test_caught_sql_head_preflight_guard_sends_zero_batch_frames_and_rolls_back(
    forbidden_sql: str,
    expected_exc: type[Exception],
) -> None:
    """Verify caught SQL-head guards populate failing_index=1, outcomes=(), send 0 Syncs, and roll back."""
    srv = _ScriptedPipelineServer(mode="commit_3op")
    client = SyncPostgresClient(
        user="postgres",
        database="appdb",
        host="127.0.0.1",
        port=srv.port,
        sslmode="disable",
        channel_binding="disable",
    )
    client.connect()
    try:
        with SyncPostgresPipeline(client) as pipe:
            pipe.execute("INSERT INTO items(name) VALUES ($1)", ["ok"])
            pipe.execute(forbidden_sql)
            with pytest.raises(expected_exc) as exc_info:
                pipe.results()
            assert getattr(exc_info.value, "failing_index", None) == 1
            assert getattr(exc_info.value, "outcomes", None) == ()
    finally:
        client.close()
        srv.join()

    assert srv.sync_batches == []
    assert srv.simple_queries == ["BEGIN", "ROLLBACK"]


def test_post_results_caller_exception_rolls_back_before_release_and_preserves_error() -> None:
    """Verify an exception raised after successful results() rolls back before running release once."""
    srv = _ScriptedPipelineServer(mode="commit_3op")
    order: list[str] = []
    client = SyncPostgresClient(
        user="postgres",
        database="appdb",
        host="127.0.0.1",
        port=srv.port,
        sslmode="disable",
        channel_binding="disable",
    )
    client.connect()

    def _release() -> None:
        order.append(f"release_saw_queries:{len(srv.simple_queries)}")

    try:
        with (
            pytest.raises(ValueError, match="post-results validation failed"),
            SyncPostgresPipeline(
                client,
                release=_release,
            ) as pipe,
        ):
            pipe.execute("INSERT INTO items(name) VALUES ($1)", ["a"])
            pipe.execute("UPDATE items SET name = $1", ["updated"])
            pipe.select("SELECT name FROM items")
            res = pipe.results()
            assert res[2] == [{"name": "updated"}]
            raise ValueError("post-results validation failed")
    finally:
        client.close()
        srv.join()

    assert srv.simple_queries == ["BEGIN", "ROLLBACK"]
    assert order == ["release_saw_queries:2"]
