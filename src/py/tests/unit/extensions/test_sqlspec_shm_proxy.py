"""Behavioral and unit tests for SQLSpecProvider, ProxySyncDriver, ProxyAsyncDriver, SharedArrowBuffer, and AlchemyPlugin SHM session brokering."""

from __future__ import annotations

import sqlite3
import warnings
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest
from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin

from vertebra.extensions.alchemy import (
    AsyncSHMBrokerTransport,
    AsyncSHMSessionBroker,
    BrokerConfig,
    BrokerPlugin,
    SyncSHMBrokerTransport,
    SyncSHMSessionBroker,
    create_alchemy_plugin,
    create_sync_alchemy_config,
)
from vertebra.extensions.sqlspec.provider import (
    BrokerRpcRequest,
    BrokerRpcResponse,
    SharedArrowBuffer,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
)
from vertebra.extensions.sqlspec.proxy import (
    ProxyAsyncDriver,
    ProxyAsyncSessionContext,
    ProxySyncDriver,
    ProxySyncSessionContext,
)
from vertebra.typing import VertebraError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import encode_json_bytes


class ItemRow(msgspec.Struct, frozen=True, gc=False):
    """Typed row schema for testing msgspec conversion across inline and Arrow SHM paths."""

    id: int
    name: str


class StrictPositiveRow(msgspec.Struct, frozen=True, gc=False):
    """Row schema that fails conversion when name is not an integer to test exception cleanup."""

    id: int
    name: int


class _SQLiteSyncUpstreamDriver:
    """In-memory SQLite-backed synchronous upstream driver tracking connection and transaction state."""

    __slots__ = ("_conn", "begins", "commits", "executed_sql", "rollbacks")

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:", isolation_level=None)
        self._conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        self.begins: int = 0
        self.commits: int = 0
        self.rollbacks: int = 0
        self.executed_sql: list[str] = []

    def begin(self) -> None:
        """Start an explicit SQLite transaction."""
        self.begins += 1
        self._conn.execute("BEGIN")

    def commit(self) -> None:
        """Commit the active SQLite transaction."""
        self.commits += 1
        self._conn.execute("COMMIT")

    def rollback(self) -> None:
        """Roll back the active SQLite transaction."""
        self.rollbacks += 1
        self._conn.execute("ROLLBACK")

    def execute(self, sql: str, *params: Any) -> int:
        """Execute a DML or DDL statement and return affected rowcount."""
        self.executed_sql.append(sql)
        cur = self._conn.execute(sql, params)
        return max(0, cur.rowcount)

    def execute_many(self, sql: str, params_seq: list[Any]) -> int:
        """Execute a batch DML statement and return affected rowcount."""
        self.executed_sql.append(sql)
        cur = self._conn.executemany(sql, params_seq)
        return max(0, cur.rowcount)

    def select(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        """Execute a SELECT query and return rows as dictionaries."""
        self.executed_sql.append(sql)
        cur = self._conn.execute(sql, params)
        cols = [col[0] for col in (cur.description or ())]
        return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


class _SQLiteAsyncUpstreamDriver:
    """Asynchronous wrapper around _SQLiteSyncUpstreamDriver for testing ProxyAsyncDriver."""

    __slots__ = ("_sync",)

    def __init__(self, sync_driver: _SQLiteSyncUpstreamDriver | None = None) -> None:
        self._sync = sync_driver or _SQLiteSyncUpstreamDriver()

    @property
    def sync_driver(self) -> _SQLiteSyncUpstreamDriver:
        """Return the underlying synchronous tracking driver."""
        return self._sync

    async def begin(self) -> None:
        """Begin an async transaction."""
        self._sync.begin()

    async def commit(self) -> None:
        """Commit an async transaction."""
        self._sync.commit()

    async def rollback(self) -> None:
        """Roll back an async transaction."""
        self._sync.rollback()

    async def execute(self, sql: str, *params: Any) -> int:
        """Execute a statement asynchronously."""
        return self._sync.execute(sql, *params)

    async def execute_many(self, sql: str, params_seq: list[Any]) -> int:
        """Execute a batch statement asynchronously."""
        return self._sync.execute_many(sql, params_seq)

    async def select(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        """Execute a SELECT statement asynchronously."""
        return self._sync.select(sql, *params)


def test_proxy_sync_driver_small_inline_results() -> None:
    """Verify small result sets (< 256 rows) return inline via ProxySyncDriver."""
    upstream = _SQLiteSyncUpstreamDriver()
    cfg = SimpleNamespace(
        bind_key="primary",
        driver_instance=upstream,
        provide_session=lambda: upstream,
    )
    broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=4, max_overflow=2))
    provider = SQLSpecSyncProvider(cfg, broker=broker, pool_name="primary", arrow_threshold_rows=256)

    with ProxySyncSessionContext(provider, broker=broker, pool_name="primary") as driver:
        inserted = driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", 1, "alpha")
        assert inserted == 1

        batch_count = driver.execute_many(
            "INSERT INTO items (id, name) VALUES (?, ?)",
            [(2, "beta"), (3, "gamma")],
        )
        assert batch_count == 2

        rows = driver.select("SELECT id, name FROM items ORDER BY id")
        assert driver.last_response is not None
        assert driver.last_response.is_arrow_shm is False
        assert rows == [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]

        typed_rows = driver.fetch("SELECT id, name FROM items ORDER BY id", schema_type=ItemRow)
        assert typed_rows == [
            ItemRow(id=1, name="alpha"),
            ItemRow(id=2, name="beta"),
            ItemRow(id=3, name="gamma"),
        ]

        one = driver.select_one("SELECT id, name FROM items WHERE id = ?", 2, schema_type=ItemRow)
        assert one == ItemRow(id=2, name="beta")

        maybe_row = driver.select_one_or_none("SELECT id, name FROM items WHERE id = ?", 999)
        assert maybe_row is None

        existing_row = driver.select_one_or_none(
            "SELECT id, name FROM items WHERE id = ?",
            3,
            schema_type=ItemRow,
        )
        assert existing_row == ItemRow(id=3, name="gamma")

        total = driver.select_value("SELECT COUNT(*) FROM items")
        assert total == 3

        with pytest.raises(VertebraError, match="ValueError"):
            driver.select_one("SELECT id, name FROM items WHERE id = ?", 999)

    metrics = broker.metrics()
    assert metrics.active_leases == 0
    assert metrics.total_acquired == 1
    assert metrics.total_released == 1


@pytest.mark.anyio
async def test_proxy_async_driver_small_inline_results() -> None:
    """Verify small result sets (< 256 rows) return inline via ProxyAsyncDriver."""
    upstream = _SQLiteAsyncUpstreamDriver()
    cfg = SimpleNamespace(
        bind_key="async_pool",
        driver_instance=upstream,
        provide_session=lambda: upstream,
    )
    broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=4, max_overflow=2))
    provider = SQLSpecAsyncProvider(cfg, broker=broker, pool_name="async_pool", arrow_threshold_rows=256)

    async with ProxyAsyncSessionContext(provider, broker=broker, pool_name="async_pool") as driver:
        rc = await driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", 10, "ten")
        assert rc == 1

        many_rc = await driver.execute_many(
            "INSERT INTO items (id, name) VALUES (?, ?)",
            [(11, "eleven"), (12, "twelve")],
        )
        assert many_rc == 2

        rows = await driver.select("SELECT id, name FROM items ORDER BY id", schema_type=ItemRow)
        assert driver.last_response is not None
        assert driver.last_response.is_arrow_shm is False
        assert rows == [
            ItemRow(id=10, name="ten"),
            ItemRow(id=11, name="eleven"),
            ItemRow(id=12, name="twelve"),
        ]

        single = await driver.select_one("SELECT id, name FROM items WHERE id = ?", 11)
        assert single == {"id": 11, "name": "eleven"}

        missing = await driver.select_one_or_none("SELECT id, name FROM items WHERE id = ?", 404)
        assert missing is None

        val = await driver.select_value("SELECT name FROM items WHERE id = ?", 12)
        assert val == "twelve"

    metrics = broker.metrics()
    assert metrics.active_leases == 0
    assert metrics.total_acquired >= 1
    assert metrics.total_released >= 1


@pytest.mark.anyio
async def test_large_result_sets_and_select_to_arrow_zero_copy_shm_spill_and_cleanup() -> None:
    """Verify >= 256 row results and select_to_arrow spill to SharedArrowBuffer and always unlink."""
    upstream = _SQLiteSyncUpstreamDriver()
    batch = [(idx, f"item-{idx}") for idx in range(1, 301)]
    upstream.execute_many("INSERT INTO items (id, name) VALUES (?, ?)", batch)

    broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=4, max_overflow=1))
    provider = SQLSpecSyncProvider(upstream, broker=broker, arrow_threshold_rows=256)

    with ProxySyncDriver(provider, broker=broker) as sync_driver:
        rows = sync_driver.select("SELECT id, name FROM items ORDER BY id")
        assert len(rows) == 300
        assert rows[0] == {"id": 1, "name": "item-1"}
        assert rows[-1] == {"id": 300, "name": "item-300"}
        assert sync_driver.last_response is not None
        assert sync_driver.last_response.is_arrow_shm is True
        spilled_shm_name = sync_driver.last_response.arrow_shm_name
        assert spilled_shm_name.startswith("vtb-arrow-")
        assert sync_driver.last_response.arrow_byte_len > 0
        assert not SharedArrowBuffer.exists(spilled_shm_name)
        assert len(provider.active_arrow_buffers) == 0

        arrow_result = sync_driver.select_to_arrow("SELECT id, name FROM items WHERE id <= 5 ORDER BY id")
        assert arrow_result.num_rows == 5
        assert arrow_result.column_names == ["id", "name"]
        assert arrow_result.to_dict() == [
            {"id": 1, "name": "item-1"},
            {"id": 2, "name": "item-2"},
            {"id": 3, "name": "item-3"},
            {"id": 4, "name": "item-4"},
            {"id": 5, "name": "item-5"},
        ]
        assert sync_driver.last_response is not None
        assert sync_driver.last_response.is_arrow_shm is True
        arrow_shm_name = sync_driver.last_response.arrow_shm_name
        assert not SharedArrowBuffer.exists(arrow_shm_name)
        assert len(provider.active_arrow_buffers) == 0

        with pytest.raises(msgspec.ValidationError):
            sync_driver.select(
                "SELECT id, name FROM items ORDER BY id",
                schema_type=StrictPositiveRow,
            )
        assert sync_driver.last_response is not None
        failed_shm_name = sync_driver.last_response.arrow_shm_name
        assert failed_shm_name.startswith("vtb-arrow-")
        assert not SharedArrowBuffer.exists(failed_shm_name)
        assert len(provider.active_arrow_buffers) == 0

    async_upstream = _SQLiteAsyncUpstreamDriver(upstream)
    async_provider = SQLSpecAsyncProvider(async_upstream, broker=broker, arrow_threshold_rows=256)
    async with ProxyAsyncDriver(async_provider, broker=broker) as async_driver:
        async_result = await async_driver.select_to_arrow("SELECT id, name FROM items ORDER BY id")
        assert async_result.num_rows == 300
        assert async_result.column_names == ["id", "name"]
        async_rows = async_result.to_dict()
        assert len(async_rows) == 300
        assert async_rows[0] == {"id": 1, "name": "item-1"}
        assert async_rows[-1] == {"id": 300, "name": "item-300"}
        assert async_result.get_data() is not None
        assert async_driver.last_response is not None
        assert async_driver.last_response.is_arrow_shm is True
        assert not SharedArrowBuffer.exists(async_driver.last_response.arrow_shm_name)
        assert len(async_provider.active_arrow_buffers) == 0

        with pytest.raises(msgspec.ValidationError):
            await async_driver.select(
                "SELECT id, name FROM items ORDER BY id",
                schema_type=StrictPositiveRow,
            )
        assert async_driver.last_response is not None
        assert not SharedArrowBuffer.exists(async_driver.last_response.arrow_shm_name)
        assert len(async_provider.active_arrow_buffers) == 0


@pytest.mark.anyio
async def test_multi_statement_transaction_pinning_sync_and_async() -> None:
    """Verify begin -> execute -> commit/rollback stays pinned to the exact (slot_idx, generation) lease."""
    session_instances: list[_SQLiteSyncUpstreamDriver] = []
    shared_db = _SQLiteSyncUpstreamDriver()

    def session_factory() -> _SQLiteSyncUpstreamDriver:
        session_instances.append(shared_db)
        return shared_db

    broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=1))
    provider = SQLSpecSyncProvider(session_factory, broker=broker)

    with ProxySyncDriver(provider, broker=broker) as driver:
        initial_slot = driver.slot_idx
        initial_gen = driver.generation
        assert initial_gen > 0

        driver.begin()
        assert driver.in_transaction is True
        assert provider.is_in_transaction(initial_slot, initial_gen) is True

        driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", 1, "tx-committed")
        assert driver.slot_idx == initial_slot
        assert driver.generation == initial_gen

        driver.commit()
        assert driver.in_transaction is False
        assert provider.is_in_transaction(initial_slot, initial_gen) is False
        assert driver.select_value("SELECT name FROM items WHERE id = ?", 1) == "tx-committed"

        driver.begin()
        driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", 2, "tx-rolled-back")
        assert driver.select_value("SELECT COUNT(*) FROM items") == 2
        assert driver.slot_idx == initial_slot
        assert driver.generation == initial_gen
        driver.rollback()
        assert driver.in_transaction is False
        assert driver.select_value("SELECT COUNT(*) FROM items") == 1

        with pytest.raises(RuntimeError, match="abort tx"), driver.transaction():
            driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", 3, "tx-failed")
            raise RuntimeError("abort tx")

        assert driver.in_transaction is False
        assert driver.select_value("SELECT COUNT(*) FROM items") == 1

    async_upstream = _SQLiteAsyncUpstreamDriver(shared_db)
    async_provider = SQLSpecAsyncProvider(async_upstream, broker=broker)
    async with ProxyAsyncDriver(async_provider, broker=broker) as async_driver:
        slot = async_driver.slot_idx
        gen = async_driver.generation
        async with async_driver.transaction():
            await async_driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", 4, "async-committed")
            assert async_driver.slot_idx == slot
            assert async_driver.generation == gen
        assert await async_driver.select_value("SELECT COUNT(*) FROM items") == 2


def test_raw_frame_serialization_and_error_handling() -> None:
    """Verify handle_raw_frame supports both MessagePack and JSON envelopes and propagates errors."""
    upstream = _SQLiteSyncUpstreamDriver()
    provider = SQLSpecSyncProvider(upstream)

    acq_req = BrokerRpcRequest(request_id=1, op="acquire", pool_name="default", worker_pid=1234)
    raw_resp = provider.handle_raw_frame(encode_json_bytes(acq_req))
    parsed_resp = msgspec.json.decode(raw_resp, type=BrokerRpcResponse)
    assert parsed_resp.ok is True
    assert parsed_resp.generation >= 1

    bad_driver = ProxySyncDriver(provider.handle_raw_frame)
    with pytest.raises(VertebraError, match="OperationalError"):
        bad_driver.execute("SELECT * FROM nonexistent_table")


@pytest.mark.anyio
async def test_alchemy_plugin_and_shm_session_broker_proxied_sessions() -> None:
    """Verify AlchemyPlugin and SHMSessionBroker acquire leases, roll back on error, and release cleanly."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=3, max_overflow=1))
    session_broker = SyncSHMSessionBroker(transport=transport, capacity=3, bind_key="primary")
    async_session_broker = AsyncSHMSessionBroker(transport=transport, capacity=3, bind_key="primary_async")

    class FakeSession:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.rolled_back: bool = False
            self.closed: bool = False

        def execute(self, sql: str) -> str:
            self.executed.append(sql)
            return sql

        def rollback(self) -> None:
            self.rolled_back = True

        def close(self) -> None:
            self.closed = True

    class FakeAsyncSession:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.rolled_back: bool = False
            self.closed: bool = False

        async def execute(self, sql: str) -> str:
            self.executed.append(sql)
            return sql

        async def rollback(self) -> None:
            self.rolled_back = True

        async def close(self) -> None:
            self.closed = True

    created_sync: list[FakeSession] = []
    created_async: list[FakeAsyncSession] = []

    def make_sync() -> FakeSession:
        s = FakeSession()
        created_sync.append(s)
        return s

    def make_async() -> FakeAsyncSession:
        s = FakeAsyncSession()
        created_async.append(s)
        return s

    sync_cfg = SimpleNamespace(
        bind_key="primary",
        session_maker_instance=make_sync,
        provide_session=make_sync,
    )
    async_cfg = SimpleNamespace(
        bind_key="primary_async",
        session_maker_instance=make_async,
        provide_session=make_async,
    )

    with session_broker.provide_session(sync_cfg) as sess:
        assert session_broker.in_use == 1
        assert transport.metrics().active_leases == 1
        sess.execute("SELECT 1")

    assert session_broker.in_use == 0
    assert transport.metrics().active_leases == 0
    assert created_sync[-1].rolled_back is False
    assert created_sync[-1].closed is True

    with pytest.raises(ValueError, match="sync failure"), session_broker.provide_session(sync_cfg) as sess:
        assert transport.metrics().active_leases == 1
        sess.execute("INSERT INTO t VALUES (1)")
        raise ValueError("sync failure")

    assert session_broker.in_use == 0
    assert transport.metrics().active_leases == 0
    assert created_sync[-1].rolled_back is True
    assert created_sync[-1].closed is True

    async with async_session_broker.provide_session(async_cfg) as async_sess:
        assert async_session_broker.in_use == 1
        assert transport.metrics().active_leases == 1
        await async_sess.execute("SELECT 2")

    assert async_session_broker.in_use == 0
    assert transport.metrics().active_leases == 0
    assert created_async[-1].rolled_back is False
    assert created_async[-1].closed is True

    with pytest.raises(RuntimeError, match="async failure"):
        async with async_session_broker.provide_session(async_cfg) as async_sess:
            await async_sess.execute("INSERT INTO t VALUES (2)")
            raise RuntimeError("async failure")

    assert async_session_broker.in_use == 0
    assert transport.metrics().active_leases == 0
    assert created_async[-1].rolled_back is True
    assert created_async[-1].closed is True

    sa_mod = import_optional("sqlalchemy")
    if sa_mod is not None:
        engine = sa_mod.create_engine("sqlite:///:memory:")
        broker_plugin = BrokerPlugin(BrokerConfig(pool_size=4, max_overflow=1))
        sa_session_broker = SyncSHMSessionBroker(transport=broker_plugin.transport, bind_key="sa_engine")
        sa_session_broker.attach_to_engine(engine)

        sa_sync_config = create_sync_alchemy_config(
            connection_string="sqlite:///:memory:",
            bind_key="sa_engine",
        )
        sa_plugin = create_alchemy_plugin(sa_sync_config)
        assert isinstance(sa_plugin, SQLAlchemyPlugin)

        before_acquired = broker_plugin.transport.metrics().total_acquired
        with engine.connect() as conn:
            assert broker_plugin.transport.metrics().active_leases >= 1
            res = conn.execute(sa_mod.text("SELECT 42")).scalar()
            assert res == 42
        assert broker_plugin.transport.metrics().total_acquired > before_acquired
        assert broker_plugin.transport.metrics().active_leases == 0
        engine.dispose()


@pytest.mark.anyio
async def test_proxy_async_driver_and_provider_reclaim_async_broker_leases() -> None:
    """Verify ProxyAsyncDriver and SQLSpecAsyncProvider await async lease releases on AsyncSHMBrokerTransport."""
    upstream = _SQLiteAsyncUpstreamDriver()
    cfg = SimpleNamespace(
        bind_key="single_slot_async",
        driver_instance=upstream,
        provide_session=lambda: upstream,
    )
    async_broker = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout=0.05))
    provider = SQLSpecAsyncProvider(cfg, broker=async_broker, pool_name="single_slot_async")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        async with ProxyAsyncSessionContext(provider, broker=async_broker, pool_name="single_slot_async") as d1:
            assert await d1.execute("INSERT INTO items (id, name) VALUES (?, ?)", 101, "slot-1") == 1
        assert async_broker.metrics().active_leases == 0

        async with ProxyAsyncSessionContext(provider, broker=async_broker, pool_name="single_slot_async") as d2:
            assert await d2.select_value("SELECT name FROM items WHERE id = ?", 101) == "slot-1"
        assert async_broker.metrics().active_leases == 0
        assert async_broker.metrics().total_released == async_broker.metrics().total_acquired

        await provider.close()

    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []


@pytest.mark.anyio
async def test_proxy_sync_driver_releases_and_quarantines_async_shm_transport_synchronously() -> None:
    """Verify ProxySyncDriver routes direct lease release and quarantine via sync helpers."""
    async_broker = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout=0.1))
    try:
        upstream = _SQLiteSyncUpstreamDriver()
        provider = SQLSpecSyncProvider(upstream)
        driver = ProxySyncDriver(provider, broker=async_broker)
        lease = async_broker.acquire_sync(bind_key="sync_fallback")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", RuntimeWarning)
            driver._release_broker_lease(lease)
            assert async_broker.metrics().active_leases == 0

            bad_lease = async_broker.acquire_sync(bind_key="sync_quarantine")
            driver._quarantine_broker_lease(bad_lease)
            assert bad_lease.quarantined is True
            assert async_broker.quarantined_count() == 1
            await async_broker.dispose_quarantined(bad_lease)
            assert async_broker.quarantined_count() == 0

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert runtime_warnings == []
    finally:
        await async_broker.close()
