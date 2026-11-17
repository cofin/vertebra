"""Behavioral and unit tests for SQLSpecProvider, ProxySyncDriver, ProxyAsyncDriver, VTBARROW ArrowCursor SHM, and AlchemyPlugin session brokering."""

from __future__ import annotations

import ast
import inspect
import sqlite3
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest
from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin
from sqlspec.core import SQLResult
from sqlspec.exceptions import (
    DataError,
    IntegrityError,
    NotFoundError,
    OperationalError,
    ReplicaUnavailableError,
    SQLSpecError,
    StackExecutionError,
    UniqueViolationError,
)

from vertebra.db.driver import InterfaceError
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
from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.extensions.sqlspec.plugin import ProxyAsyncConfig, ProxySyncConfig
from vertebra.extensions.sqlspec.provider import (
    BrokerRpcRequest,
    BrokerRpcResponse,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
    _BaseSQLSpecProvider,
)
from vertebra.extensions.sqlspec.proxy import (
    ProxyAsyncDriver,
    ProxyAsyncSessionContext,
    ProxySyncDriver,
    ProxySyncSessionContext,
    _raise_for_error,
)
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
        assert isinstance(inserted, SQLResult)
        assert inserted.rows_affected == 1

        batch_result = driver.execute_many(
            "INSERT INTO items (id, name) VALUES (?, ?)",
            [(2, "beta"), (3, "gamma")],
        )
        assert isinstance(batch_result, SQLResult)
        assert batch_result.rows_affected == 2

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

        with pytest.raises(NotFoundError):
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
        assert isinstance(rc, SQLResult)
        assert rc.rows_affected == 1

        many_rc = await driver.execute_many(
            "INSERT INTO items (id, name) VALUES (?, ?)",
            [(11, "eleven"), (12, "twelve")],
        )
        assert isinstance(many_rc, SQLResult)
        assert many_rc.rows_affected == 2

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


def _shm_exists(shm_name: str) -> bool:
    """Return True when a POSIX shared-memory segment file is still present under /dev/shm."""
    return (Path("/dev/shm") / shm_name.lstrip("/")).exists()


@pytest.mark.anyio
async def test_large_result_sets_and_select_to_arrow_zero_copy_shm_spill_and_cleanup() -> None:
    """Verify >= 256 row results and select_to_arrow spill to VTBARROW ArrowCursor and always unlink."""
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
        assert spilled_shm_name.lstrip("/").startswith("vtb-arrow-")
        assert sync_driver.last_response.arrow_byte_len > 0
        assert not _shm_exists(spilled_shm_name)
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
        assert not _shm_exists(arrow_shm_name)
        assert len(provider.active_arrow_buffers) == 0

        with pytest.raises(msgspec.ValidationError):
            sync_driver.select(
                "SELECT id, name FROM items ORDER BY id",
                schema_type=StrictPositiveRow,
            )
        assert sync_driver.last_response is not None
        failed_shm_name = sync_driver.last_response.arrow_shm_name
        assert failed_shm_name.lstrip("/").startswith("vtb-arrow-")
        assert not _shm_exists(failed_shm_name)
        assert len(provider.active_arrow_buffers) == 0

    direct_resp = provider.handle_request(
        BrokerRpcRequest(
            request_id=700,
            op="select_to_arrow",
            slot_idx=5,
            generation=5,
            sql="SELECT id, name FROM items WHERE id <= 2 ORDER BY id",
        )
    )
    assert direct_resp.is_arrow_shm is True
    assert _shm_exists(direct_resp.arrow_shm_name)
    assert (Path("/dev/shm") / direct_resp.arrow_shm_name.lstrip("/")).read_bytes()[:8] == b"VTBARROW"
    provider.handle_request(
        BrokerRpcRequest(
            request_id=701,
            op="release",
            slot_idx=5,
            generation=5,
            in_tx=False,
        )
    )
    assert not _shm_exists(direct_resp.arrow_shm_name)
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
        assert not _shm_exists(async_driver.last_response.arrow_shm_name)
        assert len(async_provider.active_arrow_buffers) == 0

        with pytest.raises(msgspec.ValidationError):
            await async_driver.select(
                "SELECT id, name FROM items ORDER BY id",
                schema_type=StrictPositiveRow,
            )
        assert async_driver.last_response is not None
        assert not _shm_exists(async_driver.last_response.arrow_shm_name)
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
    with pytest.raises(OperationalError, match="no such table"):
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
    async_broker = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=50))
    provider = SQLSpecAsyncProvider(cfg, broker=async_broker, pool_name="single_slot_async")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        async with ProxyAsyncSessionContext(provider, broker=async_broker, pool_name="single_slot_async") as d1:
            res = await d1.execute("INSERT INTO items (id, name) VALUES (?, ?)", 101, "slot-1")
            assert isinstance(res, SQLResult)
            assert res.rows_affected == 1
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
    """Verify ProxySyncDriver releases via lease.release_sync() and rejects sync transport quarantine."""
    async_broker = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=100))
    try:
        upstream = _SQLiteSyncUpstreamDriver()
        provider = SQLSpecSyncProvider(upstream)
        driver = ProxySyncDriver(provider, broker=async_broker)
        lease = await async_broker.acquire(bind_key="sync_fallback")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", RuntimeWarning)
            driver._release_broker_lease(lease)
            assert async_broker.metrics().active_leases == 0

            bad_lease = await async_broker.acquire(bind_key="sync_quarantine")
            with pytest.raises(InterfaceError):
                driver._quarantine_broker_lease(bad_lease)
            await async_broker.quarantine(bad_lease)
            assert bad_lease.quarantined is True
            assert async_broker.quarantined_count() == 1
            await async_broker.dispose_quarantined(bad_lease)
            assert async_broker.quarantined_count() == 0

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert runtime_warnings == []
    finally:
        await async_broker.close()


def test_provider_has_no_sqlite3_imports_and_declares_slots() -> None:
    """Verify provider.py AST contains zero sqlite3 imports and provider classes declare __slots__."""
    provider_path = Path(inspect.getfile(SQLSpecSyncProvider))
    tree = ast.parse(provider_path.read_text(encoding="utf-8"), filename=str(provider_path))
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_modules.append(node.module)

    assert not any(mod == "sqlite3" or mod.startswith("sqlite3.") for mod in imported_modules)

    for cls in (_BaseSQLSpecProvider, SQLSpecSyncProvider, SQLSpecAsyncProvider):
        assert "__slots__" in cls.__dict__

    sync_provider = SQLSpecSyncProvider()
    async_provider = SQLSpecAsyncProvider()
    assert not hasattr(sync_provider, "__dict__")
    assert not hasattr(async_provider, "__dict__")


@pytest.mark.parametrize(
    "params",
    [
        [1, "alpha"],
        (2, "beta"),
    ],
)
@pytest.mark.anyio
async def test_provider_dispatches_native_sqlite_sequence_params_without_sqlite3(
    params: list[Any] | tuple[Any, ...],
) -> None:
    """Verify _dispatch_sql handles native SyncSQLiteDriver and AsyncSQLiteDriver sequence params cleanly."""
    sqlite_mod = import_optional("vertebra.db.adapters.sqlite")
    if sqlite_mod is None:
        pytest.skip("vertebra.db.adapters.sqlite is not installed")
    sync_driver_cls = getattr(sqlite_mod, "SyncSQLiteDriver", None)
    async_driver_cls = getattr(sqlite_mod, "AsyncSQLiteDriver", None)
    cfg_cls = getattr(sqlite_mod, "SQLiteConfig", None)
    if sync_driver_cls is None or async_driver_cls is None or cfg_cls is None:
        pytest.skip("Native SQLite driver classes unavailable")

    sync_driver = sync_driver_cls(cfg_cls(path=":memory:"))
    try:
        sync_driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        sync_provider = SQLSpecSyncProvider(sync_driver)
        acq = sync_provider.handle_request(BrokerRpcRequest(request_id=1, op="acquire", slot_idx=0))
        assert acq.ok is True

        ins = sync_provider.handle_request(
            BrokerRpcRequest(
                request_id=2,
                op="execute",
                slot_idx=acq.slot_idx,
                generation=acq.generation,
                sql="INSERT INTO items (id, name) VALUES (?, ?)",
                params=list(params),
            )
        )
        assert ins.ok is True
        assert ins.rowcount == 1

        sel = sync_provider.handle_request(
            BrokerRpcRequest(
                request_id=3,
                op="select",
                slot_idx=acq.slot_idx,
                generation=acq.generation,
                sql="SELECT id, name FROM items WHERE id = ?",
                params=[params[0]],
            )
        )
        assert sel.ok is True
        assert sel.rows == [{"id": params[0], "name": params[1]}]
        sync_provider.close()
    finally:
        sync_driver.close()

    async_driver = async_driver_cls(cfg_cls(path=":memory:"))
    try:
        await async_driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        async_provider = SQLSpecAsyncProvider(async_driver)
        acq_async = await async_provider.handle_request(BrokerRpcRequest(request_id=10, op="acquire", slot_idx=0))
        assert acq_async.ok is True

        ins_async = await async_provider.handle_request(
            BrokerRpcRequest(
                request_id=11,
                op="execute",
                slot_idx=acq_async.slot_idx,
                generation=acq_async.generation,
                sql="INSERT INTO items (id, name) VALUES (?, ?)",
                params=list(params),
            )
        )
        assert ins_async.ok is True
        assert ins_async.rowcount == 1

        sel_async = await async_provider.handle_request(
            BrokerRpcRequest(
                request_id=12,
                op="select",
                slot_idx=acq_async.slot_idx,
                generation=acq_async.generation,
                sql="SELECT id, name FROM items WHERE id = ?",
                params=[params[0]],
            )
        )
        assert sel_async.ok is True
        assert sel_async.rows == [{"id": params[0], "name": params[1]}]
        await async_provider.close()
    finally:
        await async_driver.close()


class _TrackingCursor:
    """Minimal DB-API cursor tracking fetchmany sizes, fetchall calls, and close state."""

    __slots__ = ("_offset", "_raise_on_close", "_rows", "closed", "description", "fetchall_calls", "fetchmany_sizes")

    def __init__(
        self,
        rows: list[tuple[Any, ...]],
        columns: tuple[str, ...],
        *,
        raise_on_close: bool = False,
    ) -> None:
        self._rows = rows
        self._offset = 0
        self._raise_on_close = raise_on_close
        self.description = tuple((col, None, None, None, None, None, None) for col in columns)
        self.fetchmany_sizes: list[int] = []
        self.fetchall_calls: int = 0
        self.closed: bool = False

    def execute(self, sql: str, params: Any = None) -> _TrackingCursor:
        _ = (sql, params)
        return self

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        self.fetchmany_sizes.append(size)
        end = min(len(self._rows), self._offset + size)
        batch = self._rows[self._offset : end]
        self._offset = end
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        self.fetchall_calls += 1
        return list(self._rows[self._offset :])

    def close(self) -> None:
        self.closed = True
        if self._raise_on_close:
            raise RuntimeError("close failure")


class _CursorSession:
    """Sync session exposing cursor() backed by _TrackingCursor."""

    __slots__ = ("created_cursors", "raise_on_close", "rollbacks")

    def __init__(self, *, raise_on_close: bool = False) -> None:
        self.created_cursors: list[_TrackingCursor] = []
        self.raise_on_close = raise_on_close
        self.rollbacks = 0

    def cursor(self) -> _TrackingCursor:
        rows = [(idx, f"row-{idx}") for idx in range(1, 6)]
        cur = _TrackingCursor(rows, ("id", "name"), raise_on_close=self.raise_on_close)
        self.created_cursors.append(cur)
        return cur

    def begin(self) -> None:
        return None

    def rollback(self) -> None:
        self.rollbacks += 1


class _AsyncCursorWrapper:
    """Async wrapper over _TrackingCursor."""

    __slots__ = ("_sync",)

    def __init__(self, sync_cursor: _TrackingCursor) -> None:
        self._sync = sync_cursor

    @property
    def description(self) -> tuple[tuple[Any, ...], ...]:
        return self._sync.description

    @property
    def closed(self) -> bool:
        return self._sync.closed

    @property
    def fetchmany_sizes(self) -> list[int]:
        return self._sync.fetchmany_sizes

    @property
    def fetchall_calls(self) -> int:
        return self._sync.fetchall_calls

    async def execute(self, sql: str, params: Any = None) -> _AsyncCursorWrapper:
        self._sync.execute(sql, params)
        return self

    async def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        return self._sync.fetchmany(size)

    async def aclose(self) -> None:
        self._sync.close()


class _AsyncCursorSession:
    """Async session exposing async cursor() backed by _AsyncCursorWrapper."""

    __slots__ = ("created_cursors", "rollbacks")

    def __init__(self) -> None:
        self.created_cursors: list[_AsyncCursorWrapper] = []
        self.rollbacks = 0

    async def cursor(self) -> _AsyncCursorWrapper:
        cur = _AsyncCursorWrapper(_TrackingCursor([(idx, f"arow-{idx}") for idx in range(1, 6)], ("id", "name")))
        self.created_cursors.append(cur)
        return cur

    async def begin(self) -> None:
        return None

    async def rollback(self) -> None:
        self.rollbacks += 1


class _LazyStreamSession:
    """Sync session implementing select_stream with lazy row production tracking."""

    __slots__ = ("closed_count", "yielded_count")

    def __init__(self) -> None:
        self.yielded_count = 0
        self.closed_count = 0

    def select_stream(self, sql: str, *params: Any, chunk_size: int = 1000) -> Any:
        _ = (sql, params, chunk_size)
        session = self

        class _LazyIter:
            def __iter__(self) -> _LazyIter:
                return self

            def __next__(self) -> dict[str, Any]:
                if session.yielded_count >= 5:
                    raise StopIteration
                session.yielded_count += 1
                return {"id": session.yielded_count, "name": f"s-{session.yielded_count}"}

            def close(self) -> None:
                session.closed_count += 1

        return _LazyIter()


class _AsyncLazyStreamSession:
    """Async session implementing async select_stream with lazy row production tracking."""

    __slots__ = ("closed_count", "yielded_count")

    def __init__(self) -> None:
        self.yielded_count = 0
        self.closed_count = 0

    def select_stream(self, sql: str, *params: Any, chunk_size: int = 1000) -> Any:
        _ = (sql, params, chunk_size)
        session = self

        class _AsyncLazyIter:
            def __aiter__(self) -> _AsyncLazyIter:
                return self

            async def __anext__(self) -> dict[str, Any]:
                if session.yielded_count >= 5:
                    raise StopAsyncIteration
                session.yielded_count += 1
                return {"id": session.yielded_count, "name": f"as-{session.yielded_count}"}

            async def aclose(self) -> None:
                session.closed_count += 1

        return _AsyncLazyIter()


def test_sync_provider_cursor_stream_paging_and_error_cleanup() -> None:
    """Verify sync stream_open -> stream_fetch -> stream_close pages lazily and cleans up cursors."""
    session = _CursorSession()
    provider = SQLSpecSyncProvider(session)

    acq = provider.handle_request(BrokerRpcRequest(request_id=1, op="acquire", slot_idx=0))
    assert acq.ok is True

    open_resp = provider.handle_request(
        BrokerRpcRequest(
            request_id=2,
            op="stream_open",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            sql="SELECT id, name FROM items",
            chunk_rows=2,
        )
    )
    assert open_resp.ok is True
    assert open_resp.stream_id
    assert open_resp.has_more is True
    assert open_resp.columns == ["id", "name"]
    assert open_resp.rows == [{"id": 1, "name": "row-1"}, {"id": 2, "name": "row-2"}]
    assert len(session.created_cursors) == 1
    cur = session.created_cursors[0]
    assert cur.fetchmany_sizes == [2]
    assert cur.fetchall_calls == 0
    assert cur.closed is False
    assert open_resp.stream_id in provider.active_streams

    fetch_1 = provider.handle_request(
        BrokerRpcRequest(
            request_id=3,
            op="stream_fetch",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            stream_id=open_resp.stream_id,
            chunk_rows=2,
        )
    )
    assert fetch_1.ok is True
    assert fetch_1.has_more is True
    assert fetch_1.rows == [{"id": 3, "name": "row-3"}, {"id": 4, "name": "row-4"}]
    assert cur.fetchmany_sizes == [2, 2]

    fetch_2 = provider.handle_request(
        BrokerRpcRequest(
            request_id=4,
            op="stream_fetch",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            stream_id=open_resp.stream_id,
            chunk_rows=2,
        )
    )
    assert fetch_2.ok is True
    assert fetch_2.has_more is False
    assert fetch_2.rows == [{"id": 5, "name": "row-5"}]
    assert cur.closed is True
    assert open_resp.stream_id not in provider.active_streams

    fetch_after_close = provider.handle_request(
        BrokerRpcRequest(
            request_id=5,
            op="stream_fetch",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            stream_id=open_resp.stream_id,
            chunk_rows=2,
        )
    )
    assert fetch_after_close.ok is False
    assert fetch_after_close.error_type == "NotFoundError"

    lazy_session = _LazyStreamSession()
    lazy_provider = SQLSpecSyncProvider(lazy_session)
    lazy_acq = lazy_provider.handle_request(BrokerRpcRequest(request_id=10, op="acquire", slot_idx=0))
    lazy_open = lazy_provider.handle_request(
        BrokerRpcRequest(
            request_id=11,
            op="stream_open",
            slot_idx=lazy_acq.slot_idx,
            generation=lazy_acq.generation,
            sql="SELECT id, name FROM items",
            chunk_rows=2,
        )
    )
    assert lazy_open.ok is True
    assert lazy_open.has_more is True
    assert lazy_session.yielded_count == 2
    assert lazy_open.rows == [{"id": 1, "name": "s-1"}, {"id": 2, "name": "s-2"}]

    lazy_close = lazy_provider.handle_request(
        BrokerRpcRequest(
            request_id=12,
            op="stream_close",
            slot_idx=lazy_acq.slot_idx,
            generation=lazy_acq.generation,
            stream_id=lazy_open.stream_id,
        )
    )
    assert lazy_close.ok is True
    assert lazy_close.has_more is False
    assert lazy_session.closed_count == 1
    assert lazy_open.stream_id not in lazy_provider.active_streams

    lazy_fetch_closed = lazy_provider.handle_request(
        BrokerRpcRequest(
            request_id=13,
            op="stream_fetch",
            slot_idx=lazy_acq.slot_idx,
            generation=lazy_acq.generation,
            stream_id=lazy_open.stream_id,
            chunk_rows=2,
        )
    )
    assert lazy_fetch_closed.ok is False
    assert lazy_fetch_closed.error_type == "NotFoundError"
    lazy_provider.close()
    provider.close()


@pytest.mark.parametrize("bad_chunk_rows", [0, -1, -10])
def test_sync_provider_stream_open_rejects_nonpositive_chunk_rows(bad_chunk_rows: int) -> None:
    """Verify stream_open with chunk_rows <= 0 fails with ValueError response."""
    provider = SQLSpecSyncProvider(_CursorSession())
    resp = provider.handle_request(
        BrokerRpcRequest(
            request_id=1,
            op="stream_open",
            slot_idx=0,
            generation=1,
            sql="SELECT 1",
            chunk_rows=bad_chunk_rows,
        )
    )
    assert resp.ok is False
    assert resp.error_type == "ValueError"
    provider.close()


@pytest.mark.parametrize("cleanup_op", ["rollback", "release", "provider_close"])
def test_sync_provider_closes_active_streams_on_lifecycle_cleanup(cleanup_op: str) -> None:
    """Verify active stream cursors are closed under suppress(Exception) on rollback, release, and close."""
    session = _CursorSession(raise_on_close=True)
    provider = SQLSpecSyncProvider(session)
    acq = provider.handle_request(BrokerRpcRequest(request_id=1, op="acquire", slot_idx=0))
    if cleanup_op == "rollback":
        provider.handle_request(
            BrokerRpcRequest(request_id=2, op="begin", slot_idx=acq.slot_idx, generation=acq.generation)
        )
    open_resp = provider.handle_request(
        BrokerRpcRequest(
            request_id=3,
            op="stream_open",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            sql="SELECT id, name FROM items",
            chunk_rows=2,
        )
    )
    assert open_resp.has_more is True
    cur = session.created_cursors[0]
    assert cur.closed is False

    if cleanup_op == "provider_close":
        provider.close()
    else:
        cleanup_resp = provider.handle_request(
            BrokerRpcRequest(
                request_id=4,
                op=cleanup_op,
                slot_idx=acq.slot_idx,
                generation=acq.generation,
                in_tx=(cleanup_op == "rollback"),
            )
        )
        assert cleanup_resp.ok is True

    assert cur.closed is True
    assert len(provider.active_streams) == 0
    provider.close()


@pytest.mark.anyio
async def test_async_provider_stream_paging_and_lifecycle_cleanup() -> None:
    """Verify async stream_open, stream_fetch, stream_close, error paths, and release cleanup."""
    session = _AsyncCursorSession()
    provider = SQLSpecAsyncProvider(session)
    acq = await provider.handle_request(BrokerRpcRequest(request_id=1, op="acquire", slot_idx=0))
    assert acq.ok is True

    bad_open = await provider.handle_request(
        BrokerRpcRequest(
            request_id=2,
            op="stream_open",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            sql="SELECT id, name FROM items",
            chunk_rows=0,
        )
    )
    assert bad_open.ok is False
    assert bad_open.error_type == "ValueError"

    open_resp = await provider.handle_request(
        BrokerRpcRequest(
            request_id=3,
            op="stream_open",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            sql="SELECT id, name FROM items",
            chunk_rows=2,
        )
    )
    assert open_resp.ok is True
    assert open_resp.has_more is True
    assert open_resp.rows == [{"id": 1, "name": "arow-1"}, {"id": 2, "name": "arow-2"}]
    cur = session.created_cursors[0]
    assert cur.fetchmany_sizes == [2]
    assert cur.fetchall_calls == 0

    f1 = await provider.handle_request(
        BrokerRpcRequest(
            request_id=4,
            op="stream_fetch",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            stream_id=open_resp.stream_id,
            chunk_rows=2,
        )
    )
    assert f1.ok is True
    assert f1.has_more is True
    assert f1.rows == [{"id": 3, "name": "arow-3"}, {"id": 4, "name": "arow-4"}]

    f2 = await provider.handle_request(
        BrokerRpcRequest(
            request_id=5,
            op="stream_fetch",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            stream_id=open_resp.stream_id,
            chunk_rows=2,
        )
    )
    assert f2.ok is True
    assert f2.has_more is False
    assert f2.rows == [{"id": 5, "name": "arow-5"}]
    assert cur.closed is True

    missing_fetch = await provider.handle_request(
        BrokerRpcRequest(
            request_id=6,
            op="stream_fetch",
            slot_idx=acq.slot_idx,
            generation=acq.generation,
            stream_id="unknown-stream",
            chunk_rows=2,
        )
    )
    assert missing_fetch.ok is False
    assert missing_fetch.error_type == "NotFoundError"

    lazy_async = _AsyncLazyStreamSession()
    lazy_provider = SQLSpecAsyncProvider(lazy_async)
    lazy_acq = await lazy_provider.handle_request(BrokerRpcRequest(request_id=20, op="acquire", slot_idx=0))
    lazy_open = await lazy_provider.handle_request(
        BrokerRpcRequest(
            request_id=21,
            op="stream_open",
            slot_idx=lazy_acq.slot_idx,
            generation=lazy_acq.generation,
            sql="SELECT id, name FROM items",
            chunk_rows=2,
        )
    )
    assert lazy_open.ok is True
    assert lazy_open.has_more is True
    assert lazy_async.yielded_count == 2

    rel = await lazy_provider.handle_request(
        BrokerRpcRequest(
            request_id=22,
            op="release",
            slot_idx=lazy_acq.slot_idx,
            generation=lazy_acq.generation,
        )
    )
    assert rel.ok is True
    assert lazy_async.closed_count == 1
    assert len(lazy_provider.active_streams) == 0

    await lazy_provider.close()
    await provider.close()


def test_proxy_drivers_inherit_dispatch_hooks_declare_slots_and_drop_driver_transition() -> None:
    """Verify proxy drivers inherit execute/execute_many/select* from SQLSpec bases and declare __slots__."""
    for cls in (ProxySyncDriver, ProxyAsyncDriver):
        assert "__slots__" in cls.__dict__
        for forbidden_override in (
            "execute",
            "execute_many",
            "select",
            "select_one",
            "select_one_or_none",
            "select_value",
            "select_value_or_none",
            "select_with_total",
            "select_stream",
        ):
            assert forbidden_override not in cls.__dict__

    proxy_path = Path(inspect.getfile(ProxySyncDriver))
    tree = ast.parse(proxy_path.read_text(encoding="utf-8"), filename=str(proxy_path))
    imported_modules = [
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    ]
    assert "vertebra.extensions.sqlspec._driver" not in imported_modules

    sync_driver = ProxySyncDriver(SQLSpecSyncProvider(_SQLiteSyncUpstreamDriver()))
    async_driver = ProxyAsyncDriver(SQLSpecAsyncProvider(_SQLiteAsyncUpstreamDriver()))
    assert not hasattr(sync_driver, "__dict__")
    assert not hasattr(async_driver, "__dict__")


@pytest.mark.parametrize(
    ("error_type", "expected_cls"),
    [
        ("LeaseQuarantinedError", LeaseQuarantinedError),
        ("NotFoundError", NotFoundError),
        ("IntegrityError", IntegrityError),
        ("UniqueViolationError", UniqueViolationError),
        ("OperationalError", OperationalError),
        ("StackExecutionError", StackExecutionError),
        ("ReplicaUnavailableError", ReplicaUnavailableError),
        ("CustomRemoteError", SQLSpecError),
        ("", SQLSpecError),
    ],
)
def test_proxy_raise_for_error_mapping(error_type: str, expected_cls: type[Exception]) -> None:
    """Verify _raise_for_error separates broker quarantine from typed sqlspec exceptions and falls back cleanly."""
    resp = BrokerRpcResponse(
        request_id=1,
        ok=False,
        slot_idx=2,
        generation=3,
        error_type=error_type,
        error_message="remote failure",
    )
    for raise_fn in (_raise_for_error, ProxySyncDriver._raise_for_error, ProxyAsyncDriver._raise_for_error):
        with pytest.raises(expected_cls) as exc_info:
            raise_fn(resp)
        if isinstance(exc_info.value, LeaseQuarantinedError):
            assert exc_info.value.slot_index == 2
            assert exc_info.value.generation == 3


@pytest.mark.anyio
async def test_proxy_sync_and_async_chunked_streaming_and_early_close() -> None:
    """Verify select_stream uses paged stream_open/stream_fetch RPCs and cleans up on early close or error."""
    sync_session = _CursorSession()
    sync_provider = SQLSpecSyncProvider(sync_session)
    sync_ops: list[str] = []

    def sync_spy(req: BrokerRpcRequest) -> BrokerRpcResponse:
        sync_ops.append(req.op)
        return sync_provider.handle_request(req)

    with ProxySyncDriver(sync_spy) as sync_driver:
        with sync_driver.select_stream("SELECT id, name FROM items", chunk_size=2, native_only=True) as stream:
            first_row = next(stream)
            assert first_row == {"id": 1, "name": "row-1"}
            assert sync_ops == ["acquire", "stream_open"]
            remaining_rows = list(stream)
            assert [r["id"] for r in remaining_rows] == [2, 3, 4, 5]
        cur = sync_session.created_cursors[0]
        assert cur.fetchall_calls == 0
        assert cur.fetchmany_sizes == [2, 2, 2]
        assert cur.closed is True
        assert sync_ops == ["acquire", "stream_open", "stream_fetch", "stream_fetch"]
        assert len(sync_provider.active_streams) == 0

    sync_ops.clear()
    with pytest.raises(RuntimeError, match="abort stream"), ProxySyncDriver(sync_spy) as sync_driver:
        open_stream = sync_driver.select_stream("SELECT id, name FROM items", chunk_size=2)
        assert next(open_stream) == {"id": 1, "name": "row-1"}
        assert len(sync_provider.active_streams) == 1
        raise RuntimeError("abort stream")
    assert "stream_close" in sync_ops
    assert sync_session.created_cursors[1].closed is True
    assert len(sync_provider.active_streams) == 0
    sync_provider.close()

    async_session = _AsyncCursorSession()
    async_provider = SQLSpecAsyncProvider(async_session)
    async_ops: list[str] = []

    async def async_spy(req: BrokerRpcRequest) -> BrokerRpcResponse:
        async_ops.append(req.op)
        return await async_provider.handle_request(req)

    async with ProxyAsyncDriver(async_spy) as async_driver:
        async with async_driver.select_stream("SELECT id, name FROM items", chunk_size=2, native_only=True) as astream:
            collected = [row async for row in astream]
        assert [r["id"] for r in collected] == [1, 2, 3, 4, 5]
        acur = async_session.created_cursors[0]
        assert acur.fetchall_calls == 0
        assert acur.fetchmany_sizes == [2, 2, 2]
        assert acur.closed is True
        assert async_ops == ["acquire", "stream_open", "stream_fetch", "stream_fetch"]
        assert len(async_provider.active_streams) == 0

    async_ops.clear()
    with pytest.raises(RuntimeError, match="abort async stream"):
        async with ProxyAsyncDriver(async_spy) as async_driver:
            early_stream = async_driver.select_stream("SELECT id, name FROM items", chunk_size=2)
            first_async = await early_stream.__anext__()
            assert first_async == {"id": 1, "name": "arow-1"}
            assert len(async_provider.active_streams) == 1
            raise RuntimeError("abort async stream")
    assert "stream_close" in async_ops
    assert async_session.created_cursors[1].closed is True
    assert len(async_provider.active_streams) == 0
    await async_provider.close()


@pytest.mark.anyio
async def test_proxy_malformed_rpc_frame_raises_data_error() -> None:
    """Verify malformed msgpack broker RPC frames fail loudly with DataError / SQLSpecError."""
    sync_driver = ProxySyncDriver(lambda _payload: b"\xc1\xff\x00invalid-msgpack")
    with pytest.raises(DataError, match="Malformed broker RPC response frame"):
        sync_driver.execute("SELECT 1")

    async def bad_async_transport(_payload: bytes) -> bytes:
        return b"\xc1\xff\x00invalid-msgpack"

    async_driver = ProxyAsyncDriver(bad_async_transport)
    with pytest.raises(DataError, match="Malformed broker RPC response frame"):
        await async_driver.execute("SELECT 1")


@pytest.mark.anyio
async def test_proxy_configs_provide_connection_leasing_arg_forwarding_and_savepoints() -> None:
    """Verify ProxySyncConfig/ProxyAsyncConfig provide_connection leases via broker, forwards args, and honors savepoints."""
    broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    upstream = _SQLiteSyncUpstreamDriver()
    wrapped = SimpleNamespace(
        dialect="sqlite",
        bind_key="primary",
        driver_instance=upstream,
        provide_session=lambda *args, **kwargs: (args, kwargs),
        provide_connection=lambda *args, **kwargs: (args, kwargs),
    )

    delegating_cfg = ProxySyncConfig(wrapped)
    assert delegating_cfg.provide_session("arg1", opt=True) == (("arg1",), {"opt": True})
    assert delegating_cfg.provide_connection("arg2", opt=False) == (("arg2",), {"opt": False})

    proxied_wrapped = SimpleNamespace(
        dialect="sqlite",
        bind_key="primary",
        driver_instance=upstream,
        provide_session=lambda: upstream,
    )
    proxied_cfg = ProxySyncConfig(proxied_wrapped, broker=broker)
    with proxied_cfg.provide_connection("ignored_pool", worker_pid=777) as conn:
        assert broker.metrics().active_leases == 1
        assert conn.worker_pid == 777
        conn.begin()
        res = conn.execute("INSERT INTO items (id, name) VALUES (?, ?)", 1, "outer")
        assert isinstance(res, SQLResult)
        with pytest.raises(RuntimeError, match="sp rollback"), conn.transaction():
            conn.execute("INSERT INTO items (id, name) VALUES (?, ?)", 2, "inner-rolled-back")
            raise RuntimeError("sp rollback")
        with conn.transaction():
            conn.execute("INSERT INTO items (id, name) VALUES (?, ?)", 3, "inner-committed")
        conn.commit()
        assert conn.select("SELECT id, name FROM items ORDER BY id") == [
            {"id": 1, "name": "outer"},
            {"id": 3, "name": "inner-committed"},
        ]
    assert broker.metrics().active_leases == 0

    async_upstream = _SQLiteAsyncUpstreamDriver()
    async_wrapped = SimpleNamespace(
        dialect="sqlite",
        bind_key="async_primary",
        driver_instance=async_upstream,
        provide_session=lambda *args, **kwargs: (args, kwargs),
        provide_connection=lambda *args, **kwargs: (args, kwargs),
    )
    async_delegating_cfg = ProxyAsyncConfig(async_wrapped)
    assert async_delegating_cfg.provide_session("a1", opt=True) == (("a1",), {"opt": True})
    assert async_delegating_cfg.provide_connection("a2", opt=False) == (("a2",), {"opt": False})

    async_broker = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    async_proxied_wrapped = SimpleNamespace(
        dialect="sqlite",
        bind_key="async_primary",
        driver_instance=async_upstream,
        provide_session=lambda: async_upstream,
    )
    async_proxied_cfg = ProxyAsyncConfig(async_proxied_wrapped, broker=async_broker)
    async with async_proxied_cfg.provide_connection("ignored_pool", worker_pid=888) as aconn:
        assert async_broker.metrics().active_leases == 1
        assert aconn.worker_pid == 888
        await aconn.begin()
        ares = await aconn.execute("INSERT INTO items (id, name) VALUES (?, ?)", 10, "a-outer")
        assert isinstance(ares, SQLResult)
        with pytest.raises(RuntimeError, match="async sp rollback"):
            async with aconn.transaction():
                await aconn.execute("INSERT INTO items (id, name) VALUES (?, ?)", 11, "a-rolled-back")
                raise RuntimeError("async sp rollback")
        await aconn.commit()
        assert await aconn.select("SELECT id, name FROM items ORDER BY id") == [{"id": 10, "name": "a-outer"}]
        with pytest.raises(NotFoundError):
            await aconn.select_one("SELECT id, name FROM items WHERE id = ?", 999)
    assert async_broker.metrics().active_leases == 0
    await async_broker.close()


def test_proxy_config_deep_merges_extension_config_and_exposes_replica_hooks() -> None:
    """Verify caller extension_config wins without mutating inputs, and replica routing hooks & slots work."""
    wrapped_ext: dict[str, dict[str, Any]] = {
        "litestar": {
            "session_table": "wrapped_sessions",
            "session_key": "wrapped_session",
            "nested": {"from_wrapped": True},
        },
        "custom": {"flag": 1},
    }
    caller_ext: dict[str, dict[str, Any]] = {
        "litestar": {
            "session_key": "caller_session",
            "pool_key": "caller_pool",
            "connection_key": "caller_connection",
            "session_table": "caller_sessions",
            "nested": {"from_caller": True},
        }
    }
    wrapped_snapshot = {"litestar": dict(wrapped_ext["litestar"]), "custom": dict(wrapped_ext["custom"])}
    caller_snapshot = {"litestar": dict(caller_ext["litestar"])}

    primary_inner = SimpleNamespace(
        dialect="sqlite",
        bind_key="analytics",
        extension_config=wrapped_ext,
        provide_session=lambda: _SQLiteSyncUpstreamDriver(),
    )
    replica_broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    replica_upstream = _SQLiteSyncUpstreamDriver()
    replica_upstream.execute("INSERT INTO items (id, name) VALUES (?, ?)", 42, "from-replica")
    replica_inner = SimpleNamespace(
        dialect="sqlite",
        bind_key="analytics_ro",
        provide_session=lambda: replica_upstream,
    )
    replica_proxy = ProxySyncConfig(replica_inner, broker=replica_broker, is_replica=True)
    primary_proxy = ProxySyncConfig(
        primary_inner,
        broker=replica_broker,
        extension_config=caller_ext,
        replicas=[replica_proxy],
    )

    assert wrapped_ext["litestar"] == wrapped_snapshot["litestar"]
    assert wrapped_ext["custom"] == wrapped_snapshot["custom"]
    assert caller_ext["litestar"] == caller_snapshot["litestar"]

    assert primary_proxy.session_key == "caller_session"
    assert primary_proxy.pool_key == "caller_pool"
    assert primary_proxy.connection_key == "caller_connection"
    assert primary_proxy.extension_config["litestar"]["session_table"] == "caller_sessions"
    assert primary_proxy.extension_config["litestar"]["nested"] == {"from_wrapped": True, "from_caller": True}
    assert primary_proxy.extension_config["custom"] == {"flag": 1}

    assert "__slots__" in ProxySyncConfig.__dict__
    assert "__slots__" in ProxyAsyncConfig.__dict__
    assert replica_proxy.is_replica is True
    assert primary_proxy.is_replica is False
    assert primary_proxy.has_replicas is True
    assert primary_proxy.supports_read_routing is True
    assert "default" in primary_proxy.replica_groups

    routing = primary_proxy.provide_routing_session()
    assert routing is not None
    with primary_proxy.provide_replica_session() as replica_sess:
        assert isinstance(replica_sess, ProxySyncDriver)
        assert replica_sess.select_value("SELECT name FROM items WHERE id = ?", 42) == "from-replica"
    with primary_proxy.provide_replica_connection() as replica_conn:
        assert isinstance(replica_conn, ProxySyncDriver)
    assert replica_broker.metrics().active_leases == 0

    async_replica = ProxyAsyncConfig(replica_inner, is_replica=True)
    async_primary = ProxyAsyncConfig(primary_inner, extension_config=caller_ext, replicas=[async_replica])
    assert async_primary.session_key == "caller_session"
    assert async_primary.pool_key == "caller_pool"
    assert async_primary.connection_key == "caller_connection"
    assert async_replica.is_replica is True
    assert async_primary.supports_read_routing is True
    assert async_primary.provide_routing_session() is not None
