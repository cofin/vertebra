"""Unit tests for PostgreSQL RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM."""

from __future__ import annotations

import asyncio
import os
import re
import socket
import sys
import threading
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from tests.unit.db.postgres_ports import FakeDirectClientFactory, FakePostgresCluster, FakeSyncPostgresPool
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
from vertebra.db.adapters.postgres.notify import (
    AsyncPostgresNotificationSource,
    PostgresNotifyConfig,
)
from vertebra.db.adapters.postgres.types import PostgresColumn, PostgresQueryResult
from vertebra.db.driver import AsyncNotificationSource, DataError, NotSupportedError
from vertebra.extensions.sqlspec.adapters.postgres import PostgresSyncDriver
from vertebra.server.platform import SHMAllocationPolicy
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, reset_dependency_cache
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json

datafusion = import_optional("datafusion")


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
    sql, params = AsyncPostgresNotificationSource.build_emit_message_sql("orders", '{"id":1}')
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
    assert AsyncPostgresNotificationSource.build_emit_message_sql("orders", payload, transactional=transactional) == (
        "SELECT pg_logical_emit_message($1, $2, $3)",
        (transactional, "orders", b'{"id":1}'),
    )
    assert AsyncPostgresNotificationSource.build_notify_sql("orders", "'quoted'") == (
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
    source = AsyncPostgresNotificationSource(driver, change_feed=feed)
    assert isinstance(source, AsyncNotificationSource)
    assert source.change_feed is feed
    assert source.subscribed_channels == []
    await source.publish("orders", payload)
    driver._publish_notification.assert_awaited_once()
    sql, parameters = driver.execute.call_args.args
    assert sql == "SELECT pg_notify($1, $2)"
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
    source = AsyncPostgresNotificationSource(driver)
    with pytest.raises(ValueError, match="channel"):
        if action == "publish":
            await source.publish(channel, "data")
        else:
            await getattr(source, action)([channel])
    driver._publish_notification.assert_not_called()
    driver.execute.assert_not_called()


@pytest.mark.parametrize("with_stream_support", [False, True])
@pytest.mark.anyio
async def test_postgres_notify_listen_native_and_unsupported_stream(with_stream_support: bool) -> None:
    """Drivers without _open_notification_listener raise NotSupportedError; native drivers stream frames."""
    if not with_stream_support:

        class _NoListenerDriver:
            async def _publish_notification(self, channel: str, payload: str) -> None:
                _ = (channel, payload)

        no_listener: Any = _NoListenerDriver()
        source = AsyncPostgresNotificationSource(no_listener)
        with pytest.raises(NotSupportedError, match="Live PostgreSQL LISTEN streaming requires"):
            await anext(source.listen())
        return

    cluster = FakePostgresCluster()
    driver = SyncPostgreSQLDriver(
        pool=FakeSyncPostgresPool(cluster),
        client_factory=FakeDirectClientFactory(cluster),
    )
    source = AsyncPostgresNotificationSource(driver)
    await source.subscribe(["orders"])
    stream = source.listen()

    async def _next_event() -> Any:
        return await anext(stream)

    next_task = asyncio.create_task(_next_event())
    while not cluster.listeners:
        await asyncio.sleep(0)
    cluster.publish_notification("orders", "hello-native", pid=42)
    event = await next_task
    assert event.channel == "orders"
    assert event.payload == b"hello-native"
    await source.aclose()
    driver.close()


def test_postgres_sync_rls_predicate_binds_with_native_dollar_pool() -> None:
    """The SQLSpec RLS predicate binds its session setter with $1..$3 placeholders."""
    cluster = FakePostgresCluster()
    driver = SyncPostgreSQLDriver(
        pool=FakeSyncPostgresPool(cluster),
        client_factory=FakeDirectClientFactory(cluster),
    )
    try:
        adapter = PostgresSyncDriver(driver)
        assert adapter.apply_tenant_predicate(TenantPredicate(tenant_value="tenant-1")) == "tenant_id = $1"
        sql, parameters, _ = cluster.recorded_queries[0]
        assert sql.casefold() == "select set_config($1, $2, $3)"
        assert parameters == ("app.tenant_id", "tenant-1", True)
    finally:
        driver.close()


def test_postgres_sync_rls_predicate_propagates_session_setter_failure() -> None:
    """A refused RLS session setter propagates the original driver error after binding the statement."""
    cluster = FakePostgresCluster()
    failure = RuntimeError("external session setter refused")
    orig_execute_sql = cluster.execute_sql

    def _failing_execute(sql: str, parameters: Any, **kwargs: Any) -> Any:
        cluster.recorded_queries.append((sql, tuple(parameters or ()), kwargs.get("timeout")))
        raise failure

    _ = orig_execute_sql
    cluster.__dict__["execute_sql"] = _failing_execute
    driver = SyncPostgreSQLDriver(pool=FakeSyncPostgresPool(cluster))
    try:
        adapter = PostgresSyncDriver(driver)
        with pytest.raises(RuntimeError) as raised:
            adapter.apply_tenant_predicate(TenantPredicate(tenant_value="tenant-1"))
        assert raised.value is failure
        sql, parameters, _ = cluster.recorded_queries[0]
        assert sql.casefold() == "select set_config($1, $2, $3)"
        assert parameters == ("app.tenant_id", "tenant-1", True)
    finally:
        driver.close()


@pytest.mark.anyio
async def test_postgres_sync_notify_binds_with_native_dollar_pool() -> None:
    """NOTIFY publication keeps quotes and percent signs cleanly bound with $1/$2 placeholders."""
    cluster = FakePostgresCluster()
    driver = SyncPostgreSQLDriver(
        pool=FakeSyncPostgresPool(cluster),
        client_factory=FakeDirectClientFactory(cluster),
    )
    try:
        await AsyncPostgresNotificationSource(driver).publish("orders", "quote ' and % payload")
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries] == [
            ("SELECT pg_notify($1, $2)", ("orders", "quote ' and % payload"))
        ]
    finally:
        driver.close()


@pytest.mark.anyio
async def test_postgres_sync_embedding_pushdown_binds_with_native_dollar_pool() -> None:
    """Embedding pushdown binds the model and documents through native $1/$2 SQL."""
    port = _NativeEmbeddingQueryPort(("embedding",), [([3.0, 4.0],)])
    driver = SyncPostgreSQLDriver(pool=port)
    try:
        pushdown = PostgresEmbeddingPushdown(driver, dimensions=2, normalize=False)
        assert (await pushdown.embed_query("document")).to_list() == [3.0, 4.0]
        sql, parameters = port.calls[0]
        assert parameters == ("gemini-embedding-001", ["document"])
        assert "$1" in sql and "$2" in sql
    finally:
        driver.close()


@pytest.mark.anyio
async def test_postgres_sync_similarity_search_binds_with_native_dollar_pool() -> None:
    """Similarity search binds the query vector and limit through native $1/$2 SQL."""
    port = _NativeEmbeddingQueryPort(("id", "distance"), [(7, 0.25)])
    driver = SyncPostgreSQLDriver(pool=port)
    try:
        pushdown = PostgresEmbeddingPushdown(driver, dimensions=2, normalize=False)
        assert await pushdown.similarity_search([3.0, 4.0], table="docs", top_k=2) == [{"id": 7, "distance": 0.25}]
        sql, parameters = port.calls[0]
        assert parameters == ([3.0, 4.0], 2)
        assert "$1" in sql and "$2" in sql
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


class _NativeEmbeddingQueryPort:
    """Native pool port returning structured PostgresQueryResult rows for embedding pushdown tests."""

    __slots__ = ("_columns", "_rows", "calls")

    def __init__(self, columns: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
        self._columns = tuple(
            PostgresColumn(
                name=col,
                type_oid=25,
                format=0,
                type_modifier=-1,
                table_oid=0,
                attribute_number=idx + 1,
            )
            for idx, col in enumerate(columns)
        )
        self._rows = tuple(tuple(r) for r in rows)
        self.calls: list[tuple[str, Any]] = []

    def acquire(self) -> _NativeEmbeddingQueryPort:
        return self

    def query(
        self,
        sql: str,
        parameters: Any = None,
        *,
        parameter_oids: Any = None,
        timeout: float | None = None,
    ) -> Any:
        _ = (parameter_oids, timeout)
        self.calls.append((sql, tuple(parameters) if parameters is not None else ()))
        return PostgresQueryResult(
            columns=self._columns,
            rows=self._rows,
            affected_rows=len(self._rows),
            command_tag=f"SELECT {len(self._rows)}",
            transaction_status="I",
        )

    def close(self) -> None:
        """Keep the externally owned connection port available for assertions."""
        return None


class _PgEmbeddingRowPort:
    """Observable row port for PostgreSQL embedding pushdown unit tests."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, Any]] = []

    def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        self.calls.append((sql, parameters))
        return list(self.rows)


@pytest.mark.anyio
async def test_postgres_pushdown_embed_batch() -> None:
    """PostgresEmbeddingPushdown exposes embed_batch delegating to embed_documents."""
    port = _PgEmbeddingRowPort([{"embedding": [3.0, 4.0]}, {"embedding": [0.0, 2.0]}])
    pushdown = PostgresEmbeddingPushdown(port, dimensions=2, normalize=False)
    batch = await pushdown.embed_batch(["alpha", "beta"])
    assert batch.to_list() == [[3.0, 4.0], [0.0, 2.0]]


@pytest.mark.anyio
@pytest.mark.parametrize("bad_top_k", [0, -4])
async def test_postgres_pushdown_rejects_non_positive_top_k(bad_top_k: int) -> None:
    """PostgresEmbeddingPushdown.similarity_search rejects non-positive top_k."""
    pushdown = PostgresEmbeddingPushdown(_PgEmbeddingRowPort([]), dimensions=2, normalize=False)
    with pytest.raises(ValueError, match="top_k must be positive"):
        await pushdown.similarity_search([1.0, 0.0], table="docs", top_k=bad_top_k)


@pytest.mark.anyio
@pytest.mark.parametrize("bad_vec", [[True, False], [1.0], [1.0, 2.0, 3.0]])
async def test_postgres_pushdown_rejects_invalid_vector_rows(bad_vec: list[Any]) -> None:
    """PostgresEmbeddingPushdown rejects boolean elements and dimension drift with DataError."""
    bad_pushdown = PostgresEmbeddingPushdown(
        _PgEmbeddingRowPort([{"embedding": bad_vec}]),
        dimensions=2,
        normalize=False,
    )
    with pytest.raises(DataError, match="PostgreSQL embedding row"):
        await bad_pushdown.embed_documents(["alpha"])


@pytest.mark.parametrize(
    ("url", "expected_partitions", "expected_memory_mb"),
    [
        ("datafusion://in_process", 4, 512),
        ("datafusion://in_process?target_partitions=8&memory_limit_mb=1024", 8, 1024),
        ("datafusion://?target_partitions=2&memory_limit_mb=256", 2, 256),
    ],
)
def test_datafusion_config_from_url_valid(
    url: str,
    expected_partitions: int,
    expected_memory_mb: int,
) -> None:
    """Verify DataFusionConfig.from_url succeeds for supported in-process configurations."""
    cfg = DataFusionConfig.from_url(url)
    assert cfg.mode == "in_process"
    assert cfg.target_partitions == expected_partitions
    assert cfg.memory_limit_mb == expected_memory_mb
    assert cfg.pool_key() == ("datafusion", "in_process", expected_partitions, expected_memory_mb)


@pytest.mark.parametrize(
    "bad_url",
    [
        "datafusion://pgwire_fdw",
        "datafusion://localhost:5432",
        "datafusion://127.0.0.1:5434",
        "datafusion://in_process?mode=pgwire_fdw",
        "datafusion://in_process?pgwire_host=127.0.0.1",
        "datafusion://in_process?pgwire_host=",
        "datafusion://in_process?pgwire_port=5434",
        "datafusion://in_process?require_mtls_for_pgwire=true",
        "datafusion://in_process?require_mtls_for_pgwire=",
        "datafusion:///pgwire_fdw",
        "postgresql://in_process",
        "datafusion://in_process?target_partitions=0",
        "datafusion://in_process?target_partitions=-1",
        "datafusion://in_process?memory_limit_mb=0",
        "datafusion://in_process?memory_limit_mb=-512",
    ],
)
def test_datafusion_config_from_url_rejects_unsupported_params(bad_url: str) -> None:
    """Verify DataFusionConfig.from_url raises ImproperConfigurationError on unsupported URLs and params."""
    with pytest.raises(ImproperConfigurationError):
        DataFusionConfig.from_url(bad_url)


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"mode": "pgwire_fdw"},
        {"target_partitions": 0},
        {"target_partitions": False},
        {"target_partitions": True},
        {"memory_limit_mb": -1},
        {"memory_limit_mb": False},
        {"memory_limit_mb": True},
    ],
)
def test_datafusion_config_post_init_validation(bad_kwargs: dict[str, Any]) -> None:
    """Verify DataFusionConfig.__post_init__ rejects invalid modes, non-positive bounds, and booleans."""
    with pytest.raises(ImproperConfigurationError):
        DataFusionConfig(**bad_kwargs)


def test_datafusion_removed_pgwire_and_vendor_wrappers_absent() -> None:
    """Verify removed pgwire fields and vendor-specific registration wrappers are absent."""
    cfg = DataFusionConfig()
    assert not hasattr(cfg, "pgwire_host")
    assert not hasattr(cfg, "pgwire_port")
    assert not hasattr(cfg, "require_mtls_for_pgwire")
    for removed in (
        "register_bigquery_table",
        "register_bigquery_query",
        "register_spanner_table",
        "register_spanner_query",
    ):
        assert not hasattr(DataFusionEngine, removed)
