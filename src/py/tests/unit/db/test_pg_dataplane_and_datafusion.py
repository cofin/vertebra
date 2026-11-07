"""Unit tests for PostgreSQL RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM."""

from __future__ import annotations

import os
import re
import socket
import sys
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
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
from vertebra.db._staging import ObjectStoreStaging
from vertebra.db.adapters.datafusion import (
    ARROW_STREAM_MEDIA_TYPE,
    DataFusionConfig,
    DataFusionEngine,
    TableRegistration,
)
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgresEmbeddingPushdown, SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.notify import PostgresNotificationSource, PostgresNotifyConfig
from vertebra.db.driver import NotificationEvent, NotificationSource, NotSupportedError
from vertebra.server.platform import SHMAllocationPolicy
from vertebra.storage import AsyncObjectStore, MemoryConfig, SyncObjectStore
from vertebra.typing import MissingDependencyError, reset_dependency_cache
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json


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


def test_rls_planner_unfenced_predicate_and_session_guc() -> None:
    """Tenant fragments use ANSI casts while PostgreSQL owns session SQL."""
    from vertebra.db.adapters.postgres.core import PostgreSQLCore

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
    with pytest.raises(TypeError):
        TenantScopeConfig(guc_name="app.tenant_id")
    for bad in ("o; DROP", "a.b", ""):
        if bad:
            with pytest.raises(ValueError):
                TenantPredicate(table_alias=bad, tenant_value="t1")
        with pytest.raises(ValueError):
            TenantScopeConfig(tenant_column=bad)
        with pytest.raises(ValueError):
            TenantScopeConfig(parameter_cast=bad)


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
    ctx = None if import_optional("datafusion") is not None else _DeterministicSessionContext()
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
    with pytest.raises(ValueError, match=field):
        PostgresNotifyConfig(**{field: invalid})


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
    driver = MagicMock(spec=driver_type)
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=16)) if with_feed else None
    source = PostgresNotificationSource(driver, change_feed=feed)
    assert isinstance(source, NotificationSource)
    assert source.change_feed is feed
    await source.publish("orders", payload)
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


@pytest.mark.parametrize("action", ["publish", "subscribe", "unsubscribe"])
@pytest.mark.parametrize("channel", ["bad;sql", "bad.name", ""])
@pytest.mark.anyio
async def test_postgres_notify_rejects_invalid_channels(action: str, channel: str) -> None:
    """Every notification entry point rejects invalid channels before driver I/O."""
    driver = MagicMock(spec=SyncPostgreSQLDriver)
    source = PostgresNotificationSource(driver)
    with pytest.raises(ValueError, match="channel"):
        if action == "publish":
            await source.publish(channel, "data")
        else:
            await getattr(source, action)([channel])
    driver.execute.assert_not_called()


@pytest.mark.anyio
async def test_postgres_notify_subscription_stream_delegates() -> None:
    """Subscriptions are idempotent and listener frames pass through the driver boundary."""
    driver = MagicMock(spec=SyncPostgreSQLDriver)
    event = NotificationEvent(channel="orders", payload=b"hello")

    async def frames(channels: Sequence[str]) -> AsyncIterator[NotificationEvent]:
        assert tuple(channels) == ("orders",)
        yield event

    driver.listen_notifications = MagicMock(side_effect=frames)
    source = PostgresNotificationSource(driver, PostgresNotifyConfig(channel="orders"))
    await source.subscribe(["orders", "orders", "unused"])
    await source.unsubscribe(["unused", "unknown"])
    assert [frame async for frame in source.listen()] == [event]
    driver.listen_notifications.assert_called_once_with(("orders",))


@pytest.mark.anyio
async def test_postgres_notify_listen_without_native_stream() -> None:
    """Unimplemented native streaming fails explicitly on iteration."""
    source = PostgresNotificationSource(MagicMock(spec=SyncPostgreSQLDriver))
    with pytest.raises(NotSupportedError, match="Live PostgreSQL LISTEN streaming requires"):
        await anext(source.listen())


class _PsycopgConnectionPort:
    """External connection port using psycopg's real SQL parameter converter."""

    def __init__(
        self, query_type: Any, transformer_type: Any, columns: tuple[str, ...], rows: list[tuple[Any, ...]]
    ) -> None:
        self.query_type = query_type
        self.transformer_type = transformer_type
        self.description = tuple((column,) for column in columns)
        self.rows = rows
        self.calls: list[tuple[str, Any]] = []
        self.failure: RuntimeError | None = None

    def connection(self) -> _PsycopgConnectionPort:
        return self

    def cursor(self) -> _PsycopgConnectionPort:
        return self

    def execute(self, sql: str, parameters: Any = None) -> None:
        query = self.query_type(self.transformer_type())
        query.convert(sql, parameters)
        self.calls.append((sql, parameters))
        if self.failure is not None:
            raise self.failure

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def close(self) -> None:
        """Keep the externally owned connection port available for assertions."""


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["notify", "embedding", "search", "rls", "rls-failure"])
async def test_postgres_sync_primitives_bind_with_real_psycopg_converter(operation: str) -> None:
    """Public sync primitives generate SQL accepted by the installed psycopg converter."""
    queries = pytest.importorskip("psycopg._queries")
    adapt = pytest.importorskip("psycopg.adapt")
    columns = ("id", "distance") if operation == "search" else ("embedding",)
    rows: list[tuple[Any, ...]] = [(7, 0.25)] if operation == "search" else [([3.0, 4.0],)]
    connection = _PsycopgConnectionPort(queries.PostgresQuery, adapt.Transformer, columns, rows)
    driver = SyncPostgreSQLDriver(pool=connection)
    try:
        if operation in {"rls", "rls-failure"}:
            from vertebra.extensions.sqlspec.adapters import PostgreSQLSyncDriver

            adapter = PostgreSQLSyncDriver(driver)
            predicate = TenantPredicate(tenant_value="tenant-1")
            if operation == "rls-failure":
                connection.failure = RuntimeError("external session setter refused")
                with pytest.raises(RuntimeError) as raised:
                    adapter.apply_tenant_predicate(predicate)
                assert raised.value is connection.failure
            else:
                assert adapter.apply_tenant_predicate(predicate) == "tenant_id = $1"
            sql, parameters = connection.calls[0]
            assert sql.casefold() == "select set_config(%s, %s, %s)"
            assert tuple(parameters) == ("app.tenant_id", "tenant-1", True)
        elif operation == "notify":
            await PostgresNotificationSource(driver).publish("orders", "quote ' and % payload")
            assert connection.calls == [("SELECT pg_notify(%s, %s)", ("orders", "quote ' and % payload"))]
        else:
            pushdown = PostgresEmbeddingPushdown(driver, dimensions=2, normalize=False)
            if operation == "embedding":
                assert (await pushdown.embed_query("document")).to_list() == [3.0, 4.0]
                assert connection.calls[0][1] == ("gemini-embedding-001", ["document"])
            else:
                assert await pushdown.similarity_search([3.0, 4.0], table="docs", top_k=2) == [
                    {"id": 7, "distance": 0.25}
                ]
                assert connection.calls[0][1] == ([3.0, 4.0], 2)
            assert connection.calls[0][0].count("%s") == 2
    finally:
        driver.close()


@pytest.mark.parametrize("resolver", ["resolve_sync_store", "resolve_async_store"])
def test_staging_rejects_implicit_r2_uri(resolver: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Removed storage schemes fail without creating a local staging directory."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="(?i)unsupported.*scheme.*r2"):
        getattr(ObjectStoreStaging, resolver)(None, "r2://bucket/data.parquet")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
@pytest.mark.parametrize("store_type", [SyncObjectStore, AsyncObjectStore])
@pytest.mark.parametrize("operation", ["validate", "read", "write"])
async def test_staging_rejects_explicit_store_r2_uri(
    store_type: type[SyncObjectStore] | type[AsyncObjectStore], operation: str
) -> None:
    """Public staging operations reject R2 qualification before native object effects."""
    store = store_type(MemoryConfig())
    sync_store = store if isinstance(store, SyncObjectStore) else store.to_sync()
    sync_store.put("data.json", b'{"id":1}\n')
    cursor = ArrowCursor.from_rows([{"id": 2}])
    with pytest.raises(ValueError, match="(?i)unsupported.*scheme.*r2"):
        if operation == "validate":
            ObjectStoreStaging.validate_staging_target(store, "r2://bucket/data.json")
        elif isinstance(store, SyncObjectStore):
            if operation == "read":
                ObjectStoreStaging.read_cursor_sync("r2://bucket/data.json", store=store, format="json")
            else:
                ObjectStoreStaging.write_cursor_sync(
                    cursor, destination_uri="r2://bucket/data.json", store=store, format="json"
                )
        elif operation == "read":
            await ObjectStoreStaging.read_cursor_async("r2://bucket/data.json", store=store, format="json")
        else:
            await ObjectStoreStaging.write_cursor_async(
                cursor, destination_uri="r2://bucket/data.json", store=store, format="json"
            )
    assert sync_store.get("data.json") == b'{"id":1}\n'
    assert [item.path for item in sync_store.list()] == ["data.json"]


@pytest.mark.anyio
async def test_staging_preserves_local_paths_and_file_uris(tmp_path: Path) -> None:
    """Explicit paths and implicit file URIs round-trip through the native local store."""
    cursor = ArrowCursor.from_rows([{"id": 7}])
    store = SyncObjectStore.from_local(tmp_path)
    ObjectStoreStaging.validate_staging_target(store, "plain.json")
    ObjectStoreStaging.write_cursor_sync(cursor, destination_uri="plain.json", store=store, format="json")
    assert ObjectStoreStaging.read_cursor_sync("plain.json", store=store, format="json").to_pylist() == [{"id": 7}]
    uri = (tmp_path / "implicit.json").as_uri()
    ObjectStoreStaging.validate_staging_target(None, uri)
    await ObjectStoreStaging.write_cursor_async(cursor, destination_uri=uri, store=None, format="json")
    assert (await ObjectStoreStaging.read_cursor_async(uri, store=None, format="json")).to_pylist() == [{"id": 7}]


@pytest.mark.anyio
@pytest.mark.parametrize("store_type", [SyncObjectStore, AsyncObjectStore])
async def test_staging_preserves_native_memory_namespace_keys(
    store_type: type[SyncObjectStore] | type[AsyncObjectStore],
) -> None:
    """Native Memory authorities remain logical key namespaces for both staging modes."""
    store = store_type("memory://")
    uri = "memory://namespace/key.json"
    sync_store, sync_key = ObjectStoreStaging.resolve_sync_store(store, uri)
    _, async_key = ObjectStoreStaging.resolve_async_store(store, uri)
    assert sync_key == async_key == "namespace/key.json"
    ObjectStoreStaging.validate_staging_target(store, uri)
    cursor = ArrowCursor.from_rows([{"id": 7}])
    ObjectStoreStaging.write_cursor_sync(cursor, destination_uri=uri, store=store, format="json")
    assert (await ObjectStoreStaging.read_cursor_async(uri, store=store, format="json")).to_pylist() == [{"id": 7}]
    await ObjectStoreStaging.write_cursor_async(
        cursor, destination_uri="memory://other/second.json", store=store, format="json"
    )
    assert ObjectStoreStaging.read_cursor_sync(
        "memory://other/second.json", store=store, format="json"
    ).to_pylist() == [{"id": 7}]
    assert sync_store.get("namespace/key.json") == sync_store.get("other/second.json") == b'{"id":7}\n'
    assert [item.path for item in sync_store.list()] == ["namespace/key.json", "other/second.json"]


@pytest.mark.anyio
@pytest.mark.parametrize("store_type", [SyncObjectStore, AsyncObjectStore])
async def test_staging_preserves_native_file_authority_resolution(
    store_type: type[SyncObjectStore] | type[AsyncObjectStore], tmp_path: Path
) -> None:
    """Native local stores retain their existing file-URI path resolution semantics."""
    store = store_type.from_local(tmp_path)
    uri = "file://localhost/key.json"
    _, sync_key = ObjectStoreStaging.resolve_sync_store(store, uri)
    _, async_key = ObjectStoreStaging.resolve_async_store(store, uri)
    assert sync_key == async_key == "key.json"
    ObjectStoreStaging.validate_staging_target(store, uri)
    cursor = ArrowCursor.from_rows([{"id": 7}])
    ObjectStoreStaging.write_cursor_sync(cursor, destination_uri=uri, store=store, format="json")
    assert (await ObjectStoreStaging.read_cursor_async(uri, store=store, format="json")).to_pylist() == [{"id": 7}]
    assert (tmp_path / "key.json").read_bytes() == b'{"id":7}\n'
