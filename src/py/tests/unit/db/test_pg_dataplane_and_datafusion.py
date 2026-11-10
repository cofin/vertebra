"""Unit tests for PostgreSQL RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM."""

from __future__ import annotations

import os
import re
import socket
import sys
import threading
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from vertebra.db import (
    ArrowCursor,
    CDCConfig,
    ChangeFeed,
    TenantPredicate,
    TenantScopeConfig,
)
from vertebra.db import arrow as arrow_module
from vertebra.db.adapters.datafusion import (
    ARROW_STREAM_MEDIA_TYPE,
    DataFusionConfig,
    DataFusionEngine,
    TableRegistration,
)
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgresEmbeddingPushdown, SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.adapters.postgres.notify import PostgresNotificationSource, PostgresNotifyConfig
from vertebra.db.driver import NotificationSource, NotSupportedError
from vertebra.extensions.sqlspec.adapters import PostgreSQLSyncDriver
from vertebra.server.platform import SHMAllocationPolicy
from vertebra.typing import MissingDependencyError, reset_dependency_cache
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json

datafusion = import_optional("datafusion")
psycopg_queries = import_optional("psycopg._queries")
psycopg_adapt = import_optional("psycopg.adapt")
requires_psycopg = pytest.mark.skipif(
    psycopg_queries is None or psycopg_adapt is None, reason="psycopg is not installed"
)


def test_rls_planner_unfenced_predicate_and_session_guc() -> None:
    """Tenant fragments use ANSI casts while PostgreSQL owns session SQL."""
    scope = TenantScopeConfig(setting_name="app.tenant_id", tenant_column="org_id", parameter_cast="UUID")
    predicate = TenantPredicate(table_alias="o", tenant_value="tenant-123", config=scope)
    assert predicate.column_reference() == "o.org_id"
    assert predicate.to_sql_fragment() == "o.org_id = CAST(? AS UUID)"
    assert PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=2) == "o.org_id = $2::UUID"
    assert PostgreSQLCore.build_session_guc_sql("tenant-123", scope) == (
        "SELECT set_config($1, $2, $3)",
        ("app.tenant_id", "tenant-123", True),
    )
    plain = TenantPredicate(tenant_value="t1")
    assert plain.column_reference() == "tenant_id"
    assert plain.to_sql_fragment(":tenant_id") == "tenant_id = :tenant_id"
    assert PostgreSQLCore.build_tenant_predicate_sql(plain) == "tenant_id = $1"
    assert not hasattr(TenantPredicate, "build_session_guc_sql")
    scope_cls: Any = TenantScopeConfig
    with pytest.raises(TypeError):
        scope_cls(guc_name="app.tenant_id")


@pytest.mark.parametrize("bad", ["o; DROP", "a.b", ""])
def test_tenant_scope_config_rejects_invalid_identifiers(bad: str) -> None:
    """Injected, qualified, and empty identifiers are rejected for tenant columns and parameter casts."""
    with pytest.raises(ValueError):
        TenantScopeConfig(tenant_column=bad)
    with pytest.raises(ValueError):
        TenantScopeConfig(parameter_cast=bad)


@pytest.mark.parametrize("bad", ["o; DROP", "a.b"])
def test_tenant_predicate_rejects_invalid_table_alias(bad: str) -> None:
    """Injected and qualified table aliases are rejected before SQL fragments are built."""
    with pytest.raises(ValueError):
        TenantPredicate(table_alias=bad, tenant_value="t1")


def test_cdc_listen_notify_spill_and_logical_emit() -> None:
    """Verify ChangeFeed >7500-byte spill hydration, replay_since, bounded eviction, and logical emit SQL."""
    sql, params = PostgresNotificationSource.build_emit_message_sql("orders", '{"id":1}')
    assert "pg_logical_emit_message" in sql
    assert params == (True, "orders", b'{"id":1}')

    bridge = ChangeFeed(CDCConfig(max_history_size=2))
    small_wire, small_evt = bridge.publish_event("INSERT", {"id": 1})
    assert small_evt.spilled is False
    assert bridge.hydrate_notification(small_wire).payload == {"id": 1}

    large_payload = {"blob": "x" * 8000}
    large_wire, large_evt = bridge.publish_event("UPDATE", large_payload)
    assert large_evt.spilled is True
    assert len(large_wire) < 200
    hydrated = bridge.hydrate_notification(large_wire)
    assert hydrated.spilled is True
    assert hydrated.payload == large_payload
    assert len(bridge.replay_since(1)) == 1


def test_embedded_datafusion_and_memfd_scm_rights(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify DataFusionEngine fail-closed execution, ctx injection, and anonymous memfd + SCM_RIGHTS."""
    ctx = None if datafusion is not None else _DeterministicSessionContext()
    engine = DataFusionEngine(DataFusionConfig(mode="in_process"), ctx=ctx)
    assert engine.supports_cloud_run_ingress is True
    engine.register_table(TableRegistration(name="events_archive", uri="gs://bucket/events.parquet"))
    plan = engine.explain_plan("SELECT * FROM events_archive")
    assert plan["media_type"] == ARROW_STREAM_MEDIA_TYPE
    assert plan["referenced_tables"] == ["events_archive"]
    assert engine.execute_sql("SELECT 1 AS ok") == [{"ok": 1}]

    cursor = ArrowCursor.from_rows([{"id": 10, "label": "ok"}])
    engine.register_arrow_table("cursor_tbl", cursor)
    assert engine.execute_dicts("SELECT * FROM cursor_tbl") == [{"id": 10, "label": "ok"}]
    with pytest.raises(Exception, match="missing_datafusion_table"):
        engine.execute_dicts("SELECT * FROM missing_datafusion_table")

    monkeypatch.setitem(sys.modules, "datafusion", None)
    reset_dependency_cache("datafusion")
    try:
        with pytest.raises(MissingDependencyError):
            DataFusionEngine().execute_dicts("SELECT 1")
        with pytest.raises(MissingDependencyError):
            DataFusionEngine().execute_arrow("SELECT 1")
    finally:
        reset_dependency_cache("datafusion")

    fd, _backend = SHMAllocationPolicy.allocate_fd("ring-test", 4096)
    try:
        os.write(fd, b"VERTEBRA_MEMFD")
        s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            SHMAllocationPolicy.send_fd(s1, fd, b"HELLO")
            recv_fd = SHMAllocationPolicy.recv_fd(s2)
            try:
                os.lseek(recv_fd, 0, os.SEEK_SET)
                assert os.read(recv_fd, 14) == b"VERTEBRA_MEMFD"
            finally:
                os.close(recv_fd)
        finally:
            s1.close()
            s2.close()
    finally:
        os.close(fd)


@pytest.mark.parametrize("transactional", [True, False])
@pytest.mark.parametrize("payload", ['{"id":1}', b'{"id":1}'])
def test_postgres_notify_sql_builders(payload: str | bytes, transactional: bool) -> None:
    """PostgreSQL SQL builders retain payloads as bound values."""
    assert PostgresNotificationSource.build_emit_message_sql("orders", payload, transactional=transactional) == (
        "SELECT pg_logical_emit_message($1, $2, $3)",
        (transactional, "orders", b'{"id":1}'),
    )
    assert PostgresNotificationSource.build_notify_sql("orders", "'quoted'") == (
        "SELECT pg_notify($1, $2)",
        ("orders", "'quoted'"),
    )


@pytest.mark.parametrize("field", ["channel", "spill_table", "replication_slot_name", "publication_name"])
@pytest.mark.parametrize("invalid", ["bad; DROP TABLE orders", "bad.name", "", "has space"])
def test_postgres_notify_config_rejects_invalid_identifiers(field: str, invalid: str) -> None:
    """PostgreSQL configuration accepts single SQL identifiers only."""
    kwargs: dict[str, Any] = {field: invalid}
    with pytest.raises(ValueError, match=field):
        PostgresNotifyConfig(**kwargs)


@pytest.mark.parametrize("driver_type", [SyncPostgreSQLDriver, AsyncPostgreSQLDriver])
@pytest.mark.parametrize("payload,expected", [("hello", "hello"), (b"\x00\xff", "b64:AP8=")])
@pytest.mark.parametrize("with_feed", [False, True])
@pytest.mark.anyio
async def test_postgres_notify_publish(
    driver_type: type[SyncPostgreSQLDriver] | type[AsyncPostgreSQLDriver],
    payload: str | bytes,
    expected: str,
    with_feed: bool,
) -> None:
    """Both driver variants publish raw payloads or hydratable canonical CDC envelopes."""
    loop_thread_id = threading.get_ident()
    executed_threads: list[int] = []

    def _record_sync_thread(*_args: Any, **_kwargs: Any) -> int:
        executed_threads.append(threading.get_ident())
        return 1

    driver = MagicMock(spec=driver_type)
    driver._publish_notification.side_effect = driver_type._publish_notification.__get__(driver, driver_type)
    if driver_type is SyncPostgreSQLDriver:
        driver.execute.side_effect = _record_sync_thread
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=16)) if with_feed else None
    source = PostgresNotificationSource(driver, change_feed=feed)
    assert isinstance(source, NotificationSource)
    assert source.change_feed is feed
    assert source.subscribed_channels == []
    await source.publish("orders", payload)
    driver._publish_notification.assert_awaited_once()
    sql, parameters = driver.execute.call_args.args
    assert sql == ("SELECT pg_notify($1, $2)" if driver_type is AsyncPostgreSQLDriver else "SELECT pg_notify(%s, %s)")
    assert parameters[0] == "orders"
    if feed is None:
        assert parameters[1] == expected
    else:
        event = feed.hydrate_notification(parameters[1])
        assert event.operation == "PUBLISH"
        assert event.payload == {"channel": "orders", "data_b64": "aGVsbG8=" if payload == "hello" else "AP8="}
        assert decode_json(parameters[1])["spilled"] is True
        assert feed.replay_since(0) == [event]
    if driver_type is AsyncPostgreSQLDriver:
        driver.execute.assert_awaited_once()
    else:
        assert len(executed_threads) == 1
        assert executed_threads[0] != loop_thread_id


@pytest.mark.parametrize("action", ["publish", "subscribe", "unsubscribe"])
@pytest.mark.parametrize("channel", ["bad;sql", "bad.name", "", "a" * 64, "\u00e9" * 32])
@pytest.mark.anyio
async def test_postgres_notify_rejects_invalid_channels(action: str, channel: str) -> None:
    """Every notification entry point rejects invalid or >63-UTF-8-byte channels before driver I/O."""
    driver = MagicMock(spec=SyncPostgreSQLDriver)
    source = PostgresNotificationSource(driver)
    with pytest.raises(ValueError, match="channel"):
        if action == "publish":
            await source.publish(channel, "data")
        else:
            await getattr(source, action)([channel])
    driver._publish_notification.assert_not_called()
    driver.execute.assert_not_called()


@pytest.mark.parametrize("use_real_driver", [False, True])
@pytest.mark.anyio
async def test_postgres_notify_listen_without_native_stream(use_real_driver: bool) -> None:
    """Legacy PostgreSQL drivers without _open_notification_listener fail explicitly on iteration."""
    driver: Any = (
        SyncPostgreSQLDriver(executor=lambda _s, _p: []) if use_real_driver else MagicMock(spec=SyncPostgreSQLDriver)
    )
    assert not hasattr(SyncPostgreSQLDriver, "_open_notification_listener")
    assert not hasattr(AsyncPostgreSQLDriver, "_open_notification_listener")
    source = PostgresNotificationSource(driver)
    with pytest.raises(NotSupportedError, match="Live PostgreSQL LISTEN streaming requires"):
        await anext(source.listen())


@requires_psycopg
def test_postgres_sync_rls_predicate_binds_with_real_psycopg_converter() -> None:
    """The SQLSpec RLS predicate binds its session setter through the installed psycopg converter."""
    connection = _PsycopgConnectionPort.with_installed_converter(("embedding",), [([3.0, 4.0],)])
    driver = SyncPostgreSQLDriver(pool=connection)
    try:
        adapter = PostgreSQLSyncDriver(driver)
        assert adapter.apply_tenant_predicate(TenantPredicate(tenant_value="tenant-1")) == "tenant_id = $1"
        sql, parameters = connection.calls[0]
        assert sql.casefold() == "select set_config(%s, %s, %s)"
        assert tuple(parameters) == ("app.tenant_id", "tenant-1", True)
    finally:
        driver.close()


@requires_psycopg
def test_postgres_sync_rls_predicate_propagates_session_setter_failure() -> None:
    """A refused RLS session setter propagates the original driver error after binding the statement."""
    connection = _PsycopgConnectionPort.with_installed_converter(("embedding",), [([3.0, 4.0],)])
    connection.failure = RuntimeError("external session setter refused")
    driver = SyncPostgreSQLDriver(pool=connection)
    try:
        adapter = PostgreSQLSyncDriver(driver)
        with pytest.raises(RuntimeError) as raised:
            adapter.apply_tenant_predicate(TenantPredicate(tenant_value="tenant-1"))
        assert raised.value is connection.failure
        sql, parameters = connection.calls[0]
        assert sql.casefold() == "select set_config(%s, %s, %s)"
        assert tuple(parameters) == ("app.tenant_id", "tenant-1", True)
    finally:
        driver.close()


@requires_psycopg
@pytest.mark.anyio
async def test_postgres_sync_notify_binds_with_real_psycopg_converter() -> None:
    """NOTIFY publication keeps quotes and percent signs in bound parameters accepted by psycopg."""
    connection = _PsycopgConnectionPort.with_installed_converter(("embedding",), [([3.0, 4.0],)])
    driver = SyncPostgreSQLDriver(pool=connection)
    try:
        legacy_driver: Any = driver
        await PostgresNotificationSource(legacy_driver).publish("orders", "quote ' and % payload")
        assert connection.calls == [("SELECT pg_notify(%s, %s)", ("orders", "quote ' and % payload"))]
    finally:
        driver.close()


@requires_psycopg
@pytest.mark.anyio
async def test_postgres_sync_embedding_pushdown_binds_with_real_psycopg_converter() -> None:
    """Embedding pushdown binds the model and documents through psycopg's two-placeholder SQL."""
    connection = _PsycopgConnectionPort.with_installed_converter(("embedding",), [([3.0, 4.0],)])
    driver = SyncPostgreSQLDriver(pool=connection)
    try:
        pushdown = PostgresEmbeddingPushdown(driver, dimensions=2, normalize=False)
        assert (await pushdown.embed_query("document")).to_list() == [3.0, 4.0]
        sql, parameters = connection.calls[0]
        assert parameters == ("gemini-embedding-001", ["document"])
        assert sql.count("%s") == 2
    finally:
        driver.close()


@requires_psycopg
@pytest.mark.anyio
async def test_postgres_sync_similarity_search_binds_with_real_psycopg_converter() -> None:
    """Similarity search binds the query vector and limit through psycopg's two-placeholder SQL."""
    connection = _PsycopgConnectionPort.with_installed_converter(("id", "distance"), [(7, 0.25)])
    driver = SyncPostgreSQLDriver(pool=connection)
    try:
        pushdown = PostgresEmbeddingPushdown(driver, dimensions=2, normalize=False)
        assert await pushdown.similarity_search([3.0, 4.0], table="docs", top_k=2) == [{"id": 7, "distance": 0.25}]
        sql, parameters = connection.calls[0]
        assert parameters == ([3.0, 4.0], 2)
        assert sql.count("%s") == 2
    finally:
        driver.close()


@pytest.mark.parametrize("threshold_rows", [1, 256])
@pytest.mark.parametrize(
    "rows,columns,expected",
    [
        ([(1, "a")], ["id", "name"], [{"id": 1, "name": "a"}]),
        ([(1, "a")], None, [{"col_0": 1, "col_1": "a"}]),
        ([{"id": 1}], None, [{"id": 1}]),
        ([], None, []),
        ([], ["id"], []),
    ],
)
def test_arrow_cursor_rows_are_real_ipc(
    rows: list[Any], columns: list[str] | None, expected: list[dict[str, Any]], threshold_rows: int
) -> None:
    """Threshold selection preserves real IPC, values, schema and retained views."""
    cursor = ArrowCursor.from_rows(rows, columns, threshold_rows=threshold_rows)
    view = cursor.buffer_view()
    try:
        table = pa.ipc.open_stream(view).read_all()
        assert table.to_pylist() == expected
        assert cursor.to_pylist() == expected
        assert cursor.to_arrow_table().equals(table)
        assert cursor.num_rows == len(rows)
        assert cursor.used_shm is (len(rows) >= threshold_rows)
        if columns is not None:
            assert table.column_names == columns
        cursor.release()
        assert pa.ipc.open_stream(view).read_all().equals(table)
        assert cursor.release() is False
        del table
    finally:
        cursor.close()
        view.release()


@pytest.mark.skipif(os.name != "posix", reason="POSIX named allocation contract")
def test_arrow_cursor_propagates_real_named_allocation_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Required SHM cannot silently become an anonymous mapping after native failure."""
    monkeypatch.setattr(ArrowCursor, "_next_shm_segment_name", staticmethod(lambda: "/bad/nested/seg"))
    with pytest.raises(RuntimeError, match="/bad/nested/seg"):
        ArrowCursor.from_rows([{"id": 1}], threshold_rows=1)


@pytest.mark.parametrize("missing", ["pyarrow", "vertebra._vertebra"])
def test_arrow_cursor_requires_optional_dependencies(missing: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """IPC requires its codec and threshold-selected SHM requires the native cursor."""
    arrow_mod_any: Any = arrow_module
    original = arrow_mod_any.import_optional
    monkeypatch.setattr(arrow_module, "import_optional", lambda name: None if name == missing else original(name))
    with pytest.raises(MissingDependencyError, match=missing):
        ArrowCursor.from_rows([{"id": 1}], threshold_rows=1)


class _DeterministicDataFrame:
    """In-memory Arrow DataFrame stand-in for SessionContext contract tests."""

    __slots__ = ("_table",)

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_arrow_table(self) -> pa.Table:
        """Return the underlying normalized PyArrow table."""
        return self._table


class _DeterministicSessionContext:
    """Deterministic PyArrow-backed SessionContext double that fails closed on invalid SQL."""

    __slots__ = ("tables",)

    def __init__(self) -> None:
        self.tables: dict[str, pa.Table] = {}

    def register_table(self, name: str, table: Any) -> None:
        """Require a normalized pyarrow.Table during registration."""
        if not isinstance(table, pa.Table):
            msg = f"Expected pyarrow.Table, got {type(table)!r}"
            raise TypeError(msg)
        self.tables[name] = table

    def sql(self, query: str) -> _DeterministicDataFrame:
        """Evaluate supported test projections against registered PyArrow tables or raise."""
        normalized = " ".join(query.strip().split())
        if normalized.upper() == "SELECT 1 AS OK":
            return _DeterministicDataFrame(pa.Table.from_pylist([{"ok": 1}]))
        select_all = re.fullmatch(r"SELECT \* FROM ([A-Za-z_][A-Za-z0-9_]*)", normalized, flags=re.IGNORECASE)
        if select_all is not None:
            tbl_name = select_all.group(1)
            if tbl_name not in self.tables:
                msg = f"Table {tbl_name!r} not found in SessionContext"
                raise RuntimeError(msg)
            return _DeterministicDataFrame(self.tables[tbl_name])
        msg = f"Invalid or unsupported DataFusion SQL: {query}"
        raise RuntimeError(msg)


class _PsycopgConnectionPort:
    """External connection port using psycopg's real SQL parameter converter."""

    __slots__ = ("calls", "description", "failure", "query_type", "rows", "transformer_type")

    def __init__(
        self,
        query_type: Any,
        transformer_type: Any,
        columns: tuple[str, ...],
        rows: list[tuple[Any, ...]],
    ) -> None:
        self.query_type = query_type
        self.transformer_type = transformer_type
        self.description = tuple((column,) for column in columns)
        self.rows = rows
        self.calls: list[tuple[str, Any]] = []
        self.failure: RuntimeError | None = None

    @classmethod
    def with_installed_converter(cls, columns: tuple[str, ...], rows: list[tuple[Any, ...]]) -> _PsycopgConnectionPort:
        """Build a port that validates SQL through the installed psycopg query converter."""
        assert psycopg_queries is not None
        assert psycopg_adapt is not None
        return cls(psycopg_queries.PostgresQuery, psycopg_adapt.Transformer, columns, rows)

    def connection(self) -> _PsycopgConnectionPort:
        """Return self as an active connection object."""
        return self

    def cursor(self) -> _PsycopgConnectionPort:
        """Return self as an active DB-API cursor."""
        return self

    def execute(self, sql: str, parameters: Any = None) -> None:
        """Validate SQL parameter conversion through psycopg and record the call."""
        query = self.query_type(self.transformer_type())
        query.convert(sql, parameters)
        self.calls.append((sql, parameters))
        if self.failure is not None:
            raise self.failure

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return the configured result rows."""
        return self.rows

    def close(self) -> None:
        """Keep the externally owned connection port available for assertions."""
