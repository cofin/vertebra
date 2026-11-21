"""Unit tests for ``SyncPostgreSQLDriver`` and ``AsyncPostgreSQLDriver`` native execution and lifecycle."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Generator, Mapping, Sequence
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest

from tests.unit.db.postgres_ports import (
    FakeAsyncPostgresClient,
    FakeAsyncPostgresPool,
    FakePostgresCluster,
    FakeSyncPostgresClient,
    FakeSyncPostgresPool,
)
from vertebra._vertebra import IntegrityError, InterfaceError, NotFoundError, OperationalError, PoolExhaustedError
from vertebra.db.adapters.postgres import _async as async_pg_mod
from vertebra.db.adapters.postgres._async import _FETCH_VALUE, AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.adapters.postgres.types import PgJson
from vertebra.db.rls import AsyncTenantConnectionScope, TenantConnectionScope, TenantScopeConfig
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import ImproperConfigurationError


@pytest.fixture
def anyio_backend() -> str:
    """Restrict async adapter tests cleanly to asyncio."""
    return "asyncio"


def _seed_items_table(cluster: FakePostgresCluster) -> None:
    """Create the sample transactional items table inside the simulated cluster."""
    cluster._conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)")


@pytest.mark.parametrize("dsn", ["memory://", "MEMORY://local", "memory://test"])
def test_drivers_reject_memory_dsn(dsn: str) -> None:
    """Both native drivers must fail fast on unsupported ``memory://`` URIs."""
    cfg = PostgreSQLConfig(dsn=dsn)
    with pytest.raises(ImproperConfigurationError):
        SyncPostgreSQLDriver(cfg)
    with pytest.raises(ImproperConfigurationError):
        AsyncPostgreSQLDriver(cfg)


def test_drivers_reject_memory_direct_dsn() -> None:
    """Reject ``memory://`` configured via ``direct_dsn`` on construction."""
    cfg = PostgreSQLConfig(dsn="postgresql://localhost:5432/app", direct_dsn="memory://direct")
    with pytest.raises(ImproperConfigurationError):
        SyncPostgreSQLDriver(cfg)
    with pytest.raises(ImproperConfigurationError):
        AsyncPostgreSQLDriver(cfg)


def test_sync_query_compilation_projection_and_pool_reuse() -> None:
    """Sync driver compiles binds, unwraps ``PgJson(None)``, forwards timeouts, and reuses pooled leases."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)

    with SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool) as drv:
        count = drv.execute_many(
            "INSERT INTO items (id, name) VALUES (%s, %s)",
            [(1, "alpha"), (2, "beta")],
            timeout=2.5,
        )
        assert count == 2
        assert len(pool.active) == 0
        assert len(pool.idle) == 1

        rows, cols = drv.execute_with_description(
            "SELECT id, name FROM items WHERE name = %(label)s ORDER BY id",
            {"label": "beta"},
            timeout=1.25,
        )
        assert cols == ("id", "name")
        assert rows == [(2, "beta")]
        assert cluster.recorded_queries[-1][2] == 1.25

        null_json_rows = drv.execute_tuples("SELECT $1 AS j", (PgJson(None),))
        assert null_json_rows == [(None,)]

    assert pool.closed is True
    assert drv.closed is True
    with pytest.raises(InterfaceError):
        drv.execute("SELECT 1")


def test_sync_transaction_savepoints_and_rollback_eviction() -> None:
    """Sync transactions support savepoints, atomic rollbacks, and evict clients on failed rollback."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    with drv.transaction():
        drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "kept"))
        sp = drv.savepoint()
        drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (2, "discarded"))
        drv.rollback_to_savepoint(sp)
        drv.release_savepoint(sp)

    assert drv.execute_tuples("SELECT id, name FROM items ORDER BY id") == [(1, "kept")]
    assert len(pool.active) == 0
    assert len(pool.idle) == 1

    with pytest.raises(RuntimeError), drv.transaction():
        drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (3, "aborted"))
        raise RuntimeError("boom")
    assert drv.execute_tuples("SELECT COUNT(*) FROM items") == [(1,)]

    cluster.rollback_failures_remaining = 1
    drv.begin()
    drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (4, "evict-me"))
    with pytest.raises(OperationalError):
        drv.rollback()
    assert len(pool.evicted) == 1
    assert len(pool.active) == 0
    assert drv.in_transaction is False
    drv.close()


def test_sync_tenant_scope_and_reset_failure_eviction() -> None:
    """``TenantConnectionScope`` pins a session GUC and evicts the physical connection if reset fails."""
    cluster = FakePostgresCluster()
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")

    with TenantConnectionScope(drv, "tenant-a", scope_cfg):
        assert drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-a",)]
        assert len(pool.active) == 1
    assert len(pool.active) == 0
    assert drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("",)]

    cluster.set_config_reset_failures_remaining = 1
    with pytest.raises(OperationalError), TenantConnectionScope(drv, "tenant-b", scope_cfg):
        assert drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-b",)]
    assert len(pool.evicted) == 1
    assert len(pool.active) == 0
    drv.close()


def test_sync_copy_and_cancel() -> None:
    """Sync driver executes binary COPY FROM / COPY TO and forwards ``cancel()``."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    inserted = drv.copy_from(
        "COPY items (id, name) FROM STDIN (FORMAT BINARY)",
        [(10, "row-a"), (11, "row-b")],
        column_oids=(23, 25),
    )
    assert inserted == 2
    with drv.copy_to("COPY (SELECT id, name FROM items ORDER BY id) TO STDOUT (FORMAT BINARY)") as out:
        copied = list(out)
    assert copied == [(b"10", b"row-a"), (b"11", b"row-b")]

    drv.begin()
    drv.cancel()
    assert pool.active[0].cancel_calls == 1
    drv.rollback()
    drv.close()


def test_async_driver_has_zero_thread_offload() -> None:
    """``_async.py`` must execute natively with zero ``asyncio.to_thread`` wrappers."""
    source = inspect.getsource(async_pg_mod)
    assert "to_thread" not in source


@pytest.mark.anyio
async def test_async_query_transaction_savepoints_and_pool_lifecycle() -> None:
    """Async driver executes parameterized queries, transactions, savepoints, and cleanup cleanly."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)

    async with AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool) as drv:
        count = await drv.execute_many(
            "INSERT INTO items (id, name) VALUES (%(id)s, %(name)s)",
            [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}],
            timeout=3.0,
        )
        assert count == 2
        assert len(pool.active) == 0

        async with drv.transaction():
            sp = await drv.savepoint(7)
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (3, "temp"))
            await drv.rollback_to_savepoint(sp)
            await drv.release_savepoint(sp)
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (4, "kept"))

        rows, cols = await drv.execute_with_description("SELECT id, name FROM items ORDER BY id")
        assert cols == ("id", "name")
        assert rows == [(1, "one"), (2, "two"), (4, "kept")]

    assert pool.closed is True
    assert drv.closed is True
    with pytest.raises(InterfaceError):
        await drv.execute("SELECT 1")


@pytest.mark.anyio
async def test_async_cancellation_shields_rollback_and_evicts_tainted_clients() -> None:
    """Cancelling an active async query or transaction shields cleanup and evicts tainted sessions."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    cluster.query_started_event = asyncio.Event()
    cluster.query_release_event = asyncio.Event()

    async def _run_tx() -> None:
        async with drv.transaction():
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (99, "cancelled"))

    task = asyncio.create_task(_run_tx())
    await cluster.query_started_event.wait()
    await drv.cancel()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert drv.in_transaction is False
    assert len(pool.active) == 0
    cluster.query_started_event = None
    cluster.query_release_event = None
    assert await drv.execute_tuples("SELECT COUNT(*) FROM items") == [(0,)]

    cluster.rollback_failures_remaining = 1
    await drv.begin()
    await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (50, "dirty"))
    with pytest.raises(OperationalError):
        await drv.rollback()
    assert len(pool.evicted) == 1
    assert len(pool.active) == 0
    await drv.close()


@pytest.mark.anyio
async def test_async_tenant_scope_copy_and_close_rollback() -> None:
    """Async driver supports ``AsyncTenantConnectionScope``, binary COPY, and rollback-on-close."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")

    async with AsyncTenantConnectionScope(drv, "tenant-async", scope_cfg):
        assert await drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-async",)]
    assert len(pool.active) == 0

    cluster.set_config_reset_failures_remaining = 1
    with pytest.raises(OperationalError):
        async with AsyncTenantConnectionScope(drv, "tenant-broken", scope_cfg):
            assert await drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-broken",)]
    assert len(pool.evicted) == 1

    inserted = await drv.copy_from(
        "COPY items (id, name) FROM STDIN (FORMAT BINARY)",
        [(20, "async-a"), (21, "async-b")],
        column_oids=(23, 25),
    )
    assert inserted == 2
    async with drv.copy_to("COPY (SELECT id, name FROM items ORDER BY id) TO STDOUT (FORMAT BINARY)") as out:
        copied = [row async for row in out]
    assert copied == [(b"20", b"async-a"), (b"21", b"async-b")]

    await drv.begin()
    await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (22, "uncommitted"))
    await drv.close()
    assert pool.closed is True
    assert cluster._conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 2


class _RecordingLease:
    """Minimal lease handle carrying slot and generation identifiers for recording broker tests."""

    def __init__(self, slot_index: int, generation: int) -> None:
        self.slot_index = slot_index
        self.generation = generation


class _RecordingBroker:
    """Recording broker double asserting checkout-level lease acquire and release counts."""

    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.acquire_calls: list[int] = []
        self.release_calls: list[tuple[int, int]] = []
        self._next_slot = 0

    def acquire(self, pid: int, timeout_ms: int = 5000) -> _RecordingLease:
        _ = timeout_ms
        self.acquire_calls.append(pid)
        if self.refuse:
            msg = "broker refused lease"
            raise RuntimeError(msg)
        self._next_slot += 1
        return _RecordingLease(self._next_slot, 1)

    def release(self, slot_index: int, generation: int) -> None:
        self.release_calls.append((slot_index, generation))


class _StubTokenProvider:
    """Stub IAM token provider tracking invalidations and forced refreshes."""

    def __init__(self) -> None:
        self.invalidate_calls = 0
        self.resolve_calls: list[bool] = []

    def invalidate(self) -> None:
        self.invalidate_calls += 1

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        self.resolve_calls.append(force_refresh)
        return "test-token-placeholder-refreshed" if force_refresh else "test-token-placeholder"

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        self.resolve_calls.append(force_refresh)
        return "test-token-placeholder-refreshed" if force_refresh else "test-token-placeholder"


class _AuthRejectingOncePool(FakeSyncPostgresPool):
    """Sync pool double whose single-call methods raise a 28P01 auth rejection while configured."""

    def __init__(self, cluster: FakePostgresCluster) -> None:
        super().__init__(cluster, max_size=2)
        self.auth_failures_remaining = 1
        self.execute_attempts = 0

    def _check_auth(self) -> None:
        self.execute_attempts += 1
        if self.auth_failures_remaining > 0:
            self.auth_failures_remaining -= 1
            msg = "FATAL: password authentication failed for user (28P01)"
            raise OperationalError(msg)

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return super().fetch_rows(sql, params, parameter_oids=parameter_oids, timeout=timeout)

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return super().fetch_dicts(sql, params, parameter_oids=parameter_oids, timeout=timeout)

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return super().fetch_value(sql, params, parameter_oids=parameter_oids, timeout=timeout)

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return super().execute_dml(sql, params, parameter_oids=parameter_oids, timeout=timeout)


class _CountingSyncClient(FakeSyncPostgresClient):
    """Sync client double recording single-call and cursor invocations."""

    def __init__(self, cluster: FakePostgresCluster, *, release_hook: Any = None) -> None:
        super().__init__(cluster, release_hook=release_hook)
        self.calls: list[str] = []
        self._in_single_call = False

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        """Record external cursor creation and delegate to the parent client."""
        if not self._in_single_call:
            self.calls.append("cursor")
        return super().cursor(lazy_begin=lazy_begin)

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Record a fetch_rows call and delegate to the parent client."""
        self.calls.append("fetch_rows")
        self._in_single_call = True
        try:
            return super().fetch_rows(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Record a fetch_dicts call and delegate to the parent client."""
        self.calls.append("fetch_dicts")
        self._in_single_call = True
        try:
            return super().fetch_dicts(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a fetch_value call and delegate to the parent client."""
        self.calls.append("fetch_value")
        self._in_single_call = True
        try:
            return super().fetch_value(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Record an execute_dml call and delegate to the parent client."""
        self.calls.append("execute_dml")
        self._in_single_call = True
        try:
            return super().execute_dml(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False


class _CountingSyncPool(FakeSyncPostgresPool):
    """Sync pool double recording direct single-call, cursor, and acquire invocations."""

    def __init__(self, cluster: FakePostgresCluster, *, max_size: int = 4) -> None:
        super().__init__(cluster, max_size=max_size)
        self.calls: list[str] = []
        self.recorded_calls: list[tuple[str, str, Any, Sequence[int] | None, float | None]] = []
        self.clients: list[_CountingSyncClient] = []
        self._in_single_call = False

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        """Record external cursor creation and delegate to the parent pool."""
        if not self._in_single_call:
            self.calls.append("cursor")
        return super().cursor(lazy_begin=lazy_begin)

    def acquire(self) -> _CountingSyncClient:
        """Record external client checkout and return a counting sync client."""
        if not self._in_single_call:
            self.calls.append("acquire")
        if self.closed:
            msg = "FakeSyncPostgresPool is closed"
            raise InterfaceError(msg)
        if self.idle:
            client = self.idle.pop()
            assert isinstance(client, _CountingSyncClient)
            client.closed = False
            self.active.append(client)
            return client
        if len(self.active) >= self.max_size:
            msg = "FakeSyncPostgresPool capacity exhausted"
            raise PoolExhaustedError(msg)
        client = _CountingSyncClient(self.cluster, release_hook=self._release_client)
        self.active.append(client)
        self.clients.append(client)
        return client

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Record a pool fetch_rows call and delegate to the parent pool."""
        self.calls.append("fetch_rows")
        self.recorded_calls.append(("fetch_rows", sql, params, parameter_oids, timeout))
        self._in_single_call = True
        try:
            return super().fetch_rows(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Record a pool fetch_dicts call and delegate to the parent pool."""
        self.calls.append("fetch_dicts")
        self.recorded_calls.append(("fetch_dicts", sql, params, parameter_oids, timeout))
        self._in_single_call = True
        try:
            return super().fetch_dicts(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a pool fetch_value call and delegate to the parent pool."""
        self.calls.append("fetch_value")
        self.recorded_calls.append(("fetch_value", sql, params, parameter_oids, timeout))
        self._in_single_call = True
        try:
            return super().fetch_value(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Record a pool execute_dml call and delegate to the parent pool."""
        self.calls.append("execute_dml")
        self.recorded_calls.append(("execute_dml", sql, params, parameter_oids, timeout))
        self._in_single_call = True
        try:
            return super().execute_dml(sql, params, parameter_oids=parameter_oids, timeout=timeout)
        finally:
            self._in_single_call = False


class _ItemRow(msgspec.Struct, frozen=True):
    """Typed row schema used to verify typed select hydration."""

    id: int
    name: str


def test_sync_cursor_query_matrix_and_error_classes() -> None:
    """Sync driver executes select, tuples, command, update rowcount, named params, execute_many, and errors."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    try:
        assert drv.select("SELECT 1 AS a") == [{"a": 1}]
        assert drv.execute_with_description("SELECT 1 AS a, 2 AS b") == ([(1, 2)], ("a", "b"))
        assert drv.execute_with_description("SET search_path TO public") == ([], ())

        inserted = drv.execute_many(
            "INSERT INTO items (id, name) VALUES (%s, %s)",
            [(1, "one"), (2, "two"), (3, "three")],
        )
        assert inserted == 3

        updated = drv.execute("UPDATE items SET name = 'updated' WHERE id IN (1, 2)")
        assert updated == 2

        named = drv.select("SELECT id, name FROM items WHERE id = %(x)s", {"x": 3})
        assert named == [{"id": 3, "name": "three"}]

        with pytest.raises(IntegrityError):
            drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "duplicate"))
    finally:
        drv.close()

    with pytest.raises(InterfaceError):
        drv.select("SELECT 1 AS a")


def test_sync_cursor_method_session_and_lazy_begin_contracts() -> None:
    """``driver.cursor()`` selects pinned session or pool without taking a lease and rejects pooled lazy_begin."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    broker = _RecordingBroker()
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, broker=broker)

    try:
        pool_cur = drv.cursor()
        pool_cur.execute("SELECT 1 AS a")
        assert pool_cur.fetchall() == [(1,)]
        assert len(broker.acquire_calls) == 0

        with pytest.raises(InterfaceError, match="lazy_begin needs a session; this driver is pooled"):
            drv.cursor(lazy_begin=True)
        assert len(broker.acquire_calls) == 0

        with drv.transaction():
            assert len(broker.acquire_calls) == 1
            tx_cur = drv.cursor(lazy_begin=True)
            tx_cur.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (10, "tx"))
            assert len(broker.acquire_calls) == 1
        assert len(broker.release_calls) == 1

        scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")
        with TenantConnectionScope(drv, "tenant-cur", scope_cfg):
            assert len(broker.acquire_calls) == 2
            tenant_cur = drv.cursor(lazy_begin=True)
            tenant_cur.execute("SELECT current_setting($1)", ("app.tenant_id",))
            assert tenant_cur.fetchall() == [("tenant-cur",)]
            assert len(broker.acquire_calls) == 2
        assert len(broker.release_calls) == 2
    finally:
        drv.close()

    with pytest.raises(InterfaceError):
        drv.cursor()


@pytest.mark.parametrize("mode", ["no_broker", "transaction", "autocommit", "error", "refused"])
def test_sync_broker_checkout_lease_counts(mode: str) -> None:
    """Broker leases are acquired once per checkout, released on error, and skipped when absent or refused."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)

    if mode == "no_broker":
        unused_broker = _RecordingBroker()
        drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, broker=None)
        try:
            for idx in range(10):
                assert drv.select("SELECT $1 AS n", (idx,)) == [{"n": idx}]
            assert len(unused_broker.acquire_calls) == 0
            assert len(unused_broker.release_calls) == 0
        finally:
            drv.close()
        return

    broker = _RecordingBroker(refuse=mode == "refused")
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, broker=broker)
    try:
        if mode == "transaction":
            with drv.transaction():
                for idx in range(5):
                    drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (idx + 1, f"row-{idx}"))
            assert len(broker.acquire_calls) == 1
            assert len(broker.release_calls) == 1
        elif mode == "autocommit":
            for idx in range(3):
                assert drv.select("SELECT $1 AS n", (idx,)) == [{"n": idx}]
            assert len(broker.acquire_calls) == 3
            assert len(broker.release_calls) == 3
        elif mode == "error":
            drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "seed"))
            with pytest.raises(IntegrityError):
                drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "dup"))
            assert len(broker.acquire_calls) == 2
            assert len(broker.release_calls) == 2
        else:
            assert drv.select("SELECT 1 AS a") == [{"a": 1}]
            assert len(broker.acquire_calls) == 1
            assert len(broker.release_calls) == 0
    finally:
        drv.close()


def test_sync_pool_auth_rejection_retries_once() -> None:
    """A 28P01 authentication rejection on the pool refreshes the token provider and retries once."""
    cluster = FakePostgresCluster()
    pool = _AuthRejectingOncePool(cluster)
    token_provider = _StubTokenProvider()
    cfg = PostgreSQLConfig(token_provider=token_provider)
    drv = SyncPostgreSQLDriver(cfg, pool=pool)
    try:
        assert drv.select("SELECT 1 AS a") == [{"a": 1}]
        assert pool.execute_attempts == 2
        assert token_provider.invalidate_calls == 1
        assert token_provider.resolve_calls == [True]
    finally:
        drv.close()


def test_sync_pool_auth_rejection_without_token_provider_propagates_once() -> None:
    """A 28P01 authentication rejection without a token provider propagates after one attempt."""
    cluster = FakePostgresCluster()
    pool = _AuthRejectingOncePool(cluster)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    try:
        with pytest.raises(OperationalError, match="28P01"):
            drv.select("SELECT 1 AS a")
        assert pool.execute_attempts == 1
    finally:
        drv.close()


def test_sync_single_call_pool_route_counts_one_call_per_method() -> None:
    """Each of the eight sync query methods makes exactly one native pool call with no cursor or acquire."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = _CountingSyncPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool).connect()

    try:
        assert drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "alpha")) == 1
        assert pool.calls == ["execute_dml"]

        assert drv.execute_dml("INSERT INTO items (id, name) VALUES ($1, $2)", (2, "beta")) == (1, None)
        assert pool.calls == ["execute_dml", "execute_dml"]

        assert drv.execute_dml("UPDATE items SET name = 'updated' WHERE id IN (1, 2)") == (2, None)
        assert pool.calls == ["execute_dml", "execute_dml", "execute_dml"]

        pool.calls.clear()

        assert drv.select("SELECT id, name FROM items ORDER BY id") == [
            {"id": 1, "name": "updated"},
            {"id": 2, "name": "updated"},
        ]
        assert drv.select_one("SELECT id, name FROM items WHERE id = $1", (1,)) == {"id": 1, "name": "updated"}
        assert drv.select_one_or_none("SELECT id, name FROM items WHERE id = $1", (99,)) is None
        assert drv.select_value("SELECT name FROM items WHERE id = $1", (2,)) == "updated"
        assert drv.execute_with_description("SELECT id, name FROM items WHERE id = $1", (1,)) == (
            [(1, "updated")],
            ("id", "name"),
        )
        assert drv.execute_tuples("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
        assert drv.execute("UPDATE items SET name = $1 WHERE id = $2", ("one", 1)) == 1
        assert drv.execute_dml("UPDATE items SET name = $1 WHERE id = $2", ("two", 2)) == (1, None)

        assert pool.calls == [
            "fetch_dicts",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_value",
            "fetch_rows",
            "fetch_rows",
            "execute_dml",
            "execute_dml",
        ]
        assert "cursor" not in pool.calls
        assert "acquire" not in pool.calls

        pool.calls.clear()
        typed = drv.select("SELECT id, name FROM items ORDER BY id", schema_type=_ItemRow)
        assert typed == [_ItemRow(id=1, name="one"), _ItemRow(id=2, name="two")]
        assert pool.calls == ["fetch_rows"]
    finally:
        drv.close()


def test_sync_single_call_pinned_transaction_and_tenant_routes() -> None:
    """Pinned transaction and tenant scopes run statements directly on the pinned client with zero pool single-calls."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = _CountingSyncPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool).connect()

    try:
        drv.begin()
        pinned_client = pool.active[0]
        assert isinstance(pinned_client, _CountingSyncClient)
        assert drv.execute_dml("INSERT INTO items (id, name) VALUES ($1, $2)", (10, "tx-a")) == (1, None)
        assert drv.select("SELECT id, name FROM items WHERE id = $1", (10,)) == [{"id": 10, "name": "tx-a"}]
        assert drv.select_value("SELECT name FROM items WHERE id = $1", (10,)) == "tx-a"
        drv.commit()

        assert pool.calls == ["acquire"]
        assert pinned_client.calls == ["execute_dml", "fetch_dicts", "fetch_value"]
        assert "cursor" not in pinned_client.calls

        pool.calls.clear()
        pinned_client.calls.clear()
        drv.begin()
        tx_all_client = pool.active[0]
        assert isinstance(tx_all_client, _CountingSyncClient)
        assert drv.select("SELECT id, name FROM items WHERE id = $1", (10,)) == [{"id": 10, "name": "tx-a"}]
        assert drv.select_one("SELECT id, name FROM items WHERE id = $1", (10,)) == {"id": 10, "name": "tx-a"}
        assert drv.select_one_or_none("SELECT id, name FROM items WHERE id = $1", (99,)) is None
        assert drv.select_value("SELECT name FROM items WHERE id = $1", (10,)) == "tx-a"
        assert drv.execute_with_description("SELECT id FROM items WHERE id = $1", (10,)) == ([(10,)], ("id",))
        assert drv.execute_tuples("SELECT id FROM items WHERE id = $1", (10,)) == [(10,)]
        assert drv.execute("UPDATE items SET name = $1 WHERE id = $2", ("tx-b", 10)) == 1
        assert drv.execute_dml("UPDATE items SET name = $1 WHERE id = $2", ("tx-c", 10)) == (1, None)
        drv.commit()

        assert pool.calls == ["acquire"]
        assert tx_all_client.calls == [
            "fetch_dicts",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_value",
            "fetch_rows",
            "fetch_rows",
            "execute_dml",
            "execute_dml",
        ]
        assert "cursor" not in tx_all_client.calls

        pool.calls.clear()
        tx_all_client.calls.clear()
        scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")
        with TenantConnectionScope(drv, "tenant-z", scope_cfg):
            tenant_client = pool.active[0]
            assert isinstance(tenant_client, _CountingSyncClient)
            assert drv.select("SELECT id FROM items WHERE id = $1", (10,)) == [{"id": 10}]
            assert drv.select_one("SELECT id FROM items WHERE id = $1", (10,)) == {"id": 10}
            assert drv.select_one_or_none("SELECT id FROM items WHERE id = $1", (99,)) is None
            assert drv.select_value("SELECT current_setting($1)", ("app.tenant_id",)) == "tenant-z"
            assert drv.execute_with_description("SELECT id FROM items WHERE id = $1", (10,)) == ([(10,)], ("id",))
            assert drv.execute_tuples("SELECT id FROM items WHERE id = $1", (10,)) == [(10,)]
            assert drv.execute("UPDATE items SET name = $1 WHERE id = $2", ("tenant-1", 10)) == 1
            assert drv.execute_dml("UPDATE items SET name = $1 WHERE id = $2", ("tenant-2", 10)) == (1, None)

        assert pool.calls == ["acquire"]
        assert tenant_client.calls == [
            "execute_dml",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_value",
            "fetch_rows",
            "fetch_rows",
            "execute_dml",
            "execute_dml",
            "execute_dml",
        ]
        assert "cursor" not in tenant_client.calls
    finally:
        drv.close()


def test_sync_single_call_value_options_not_found_and_trace_comment() -> None:
    """``select_value`` handles NotFoundError, value_type coercion, timeout/parameter_oids, and traceparent comments."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = _CountingSyncPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool).connect()

    try:
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            drv.select_value("SELECT id FROM items WHERE id = $1", (999,))

        assert drv.select_value("SELECT '5'", value_type=int) == 5

        assert drv.select_value("SELECT $1", (42,), timeout=1.5, parameter_oids=(23,)) == 42
        last_method, _, _, last_oids, last_timeout = pool.recorded_calls[-1]
        assert last_method == "fetch_value"
        assert last_oids == (23,)
        assert last_timeout == 1.5

        ctx = TraceContext.generate()
        prev_ctx = TraceContext.current()
        TraceContext.set_current(ctx)
        try:
            assert drv.select_value("SELECT 7") == 7
        finally:
            TraceContext.set_current(prev_ctx)
        traced_sql = pool.recorded_calls[-1][1]
        assert traced_sql.endswith(f"/*traceparent='{ctx.format()}'*/")
    finally:
        drv.close()


def test_sync_single_call_closed_and_reopen_uses_pool_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """Closing clears ``_fast_pool`` so queries raise InterfaceError, and ``connect()`` restores the pool route."""
    cluster = FakePostgresCluster()
    created_pools: list[_CountingSyncPool] = []

    def _make_pool(**kwargs: Any) -> _CountingSyncPool:
        _ = kwargs
        pool = _CountingSyncPool(cluster, max_size=2)
        created_pools.append(pool)
        return pool

    monkeypatch.setattr(
        PostgreSQLCore,
        "resolve_native_module",
        staticmethod(lambda: SimpleNamespace(SyncPostgresPool=_make_pool)),
    )

    drv = SyncPostgreSQLDriver(PostgreSQLConfig()).connect()
    assert drv.select("SELECT 1 AS a") == [{"a": 1}]
    assert created_pools[0].calls == ["fetch_dicts"]

    drv.close()
    with pytest.raises(InterfaceError):
        drv.select("SELECT 1 AS a")

    drv.connect()
    try:
        assert drv.select("SELECT 2 AS b") == [{"b": 2}]
        assert len(created_pools) == 2
        assert created_pools[1].calls == ["fetch_dicts"]
    finally:
        drv.close()


class _AsyncRecordingBroker:
    """Async recording broker double whose ``acquire`` is a coroutine."""

    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.acquire_calls: list[int] = []
        self.release_calls: list[tuple[int, int]] = []
        self._next_slot = 0

    async def acquire(self, pid: int, timeout_ms: int = 5000) -> _RecordingLease:
        _ = timeout_ms
        await asyncio.sleep(0)
        self.acquire_calls.append(pid)
        if self.refuse:
            msg = "async broker refused lease"
            raise RuntimeError(msg)
        self._next_slot += 1
        return _RecordingLease(self._next_slot, 1)

    def release(self, slot_index: int, generation: int) -> None:
        self.release_calls.append((slot_index, generation))


class _AsyncAuthRejectingOncePool(FakeAsyncPostgresPool):
    """Async pool double whose single-call methods raise a 28P01 auth rejection while configured."""

    def __init__(self, cluster: FakePostgresCluster) -> None:
        super().__init__(cluster, max_size=2)
        self.auth_failures_remaining = 1
        self.execute_attempts = 0

    def _check_auth(self) -> None:
        self.execute_attempts += 1
        if self.auth_failures_remaining > 0:
            self.auth_failures_remaining -= 1
            msg = "FATAL: password authentication failed for user (28P01)"
            raise OperationalError(msg)

    async def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return await super().fetch_rows(sql, params, parameter_oids=parameter_oids, timeout=timeout)

    async def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return await super().fetch_dicts(sql, params, parameter_oids=parameter_oids, timeout=timeout)

    async def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return await super().fetch_value(sql, params, parameter_oids=parameter_oids, timeout=timeout)

    async def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, None]:
        """Fail with 28P01 while configured, otherwise delegate to the parent pool."""
        self._check_auth()
        return await super().execute_dml(sql, params, parameter_oids=parameter_oids, timeout=timeout)


class _CountingAwaitable:
    """Awaitable wrapper incrementing a shared counter on ``__await__`` and delegating."""

    def __init__(
        self,
        inner: Awaitable[Any],
        counter: list[int],
        *,
        on_start: Callable[[], None] | None = None,
        on_end: Callable[[], None] | None = None,
    ) -> None:
        self._inner = inner
        self._counter = counter
        self._on_start = on_start
        self._on_end = on_end

    def __await__(self) -> Generator[Any, None, Any]:
        self._counter[0] += 1
        if self._on_start is not None:
            self._on_start()
        try:
            return (yield from self._inner.__await__())
        finally:
            if self._on_end is not None:
                self._on_end()


class _CountingAsyncClient(FakeAsyncPostgresClient):
    """Async client double recording single-call invocations and counting awaits."""

    def __init__(
        self,
        cluster: FakePostgresCluster,
        *,
        await_counter: list[int],
        release_hook: Any = None,
    ) -> None:
        super().__init__(cluster, release_hook=release_hook)
        self.calls: list[str] = []
        self.await_counter = await_counter
        self.last_awaitable: _CountingAwaitable | None = None
        self._single_call_depth = 0

    def _enter_single_call(self) -> None:
        self._single_call_depth += 1

    def _exit_single_call(self) -> None:
        self._single_call_depth -= 1

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        """Record external cursor creation and delegate to the parent client."""
        if self._single_call_depth == 0:
            self.calls.append("cursor")
        return super().cursor(lazy_begin=lazy_begin)

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a fetch_rows call and return a counting awaitable."""
        self.calls.append("fetch_rows")
        awaitable = _CountingAwaitable(
            super().fetch_rows(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a fetch_dicts call and return a counting awaitable."""
        self.calls.append("fetch_dicts")
        awaitable = _CountingAwaitable(
            super().fetch_dicts(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a fetch_value call and return a counting awaitable."""
        self.calls.append("fetch_value")
        awaitable = _CountingAwaitable(
            super().fetch_value(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record an execute_dml call and return a counting awaitable."""
        self.calls.append("execute_dml")
        awaitable = _CountingAwaitable(
            super().execute_dml(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable


class _CountingAsyncPool(FakeAsyncPostgresPool):
    """Async pool double recording direct single-call, cursor, acquire, and await counts."""

    def __init__(self, cluster: FakePostgresCluster, *, max_size: int = 4) -> None:
        super().__init__(cluster, max_size=max_size)
        self.calls: list[str] = []
        self.recorded_calls: list[tuple[str, str, Any, Sequence[int] | None, float | None]] = []
        self.await_counter: list[int] = [0]
        self.clients: list[_CountingAsyncClient] = []
        self.last_awaitable: _CountingAwaitable | None = None
        self._single_call_depth = 0

    @property
    def await_count(self) -> int:
        """Return the number of counted awaits executed through this pool or its clients."""
        return self.await_counter[0]

    def reset_counts(self) -> None:
        """Clear recorded call names and reset the shared await counter."""
        self.calls.clear()
        self.await_counter[0] = 0

    def _enter_single_call(self) -> None:
        self._single_call_depth += 1

    def _exit_single_call(self) -> None:
        self._single_call_depth -= 1

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        """Record external cursor creation and delegate to the parent pool."""
        if self._single_call_depth == 0:
            self.calls.append("cursor")
        return super().cursor(lazy_begin=lazy_begin)

    async def acquire(self) -> _CountingAsyncClient:
        """Record external client checkout and return a counting async client."""
        if self._single_call_depth == 0:
            self.calls.append("acquire")
        if self.closed:
            msg = "FakeAsyncPostgresPool is closed"
            raise InterfaceError(msg)
        self.acquire_calls += 1
        if self.idle:
            client = self.idle.pop()
            assert isinstance(client, _CountingAsyncClient)
            client.closed = False
            self.active.append(client)
            return client
        if len(self.active) >= self.max_size:
            msg = "FakeAsyncPostgresPool capacity exhausted"
            raise PoolExhaustedError(msg)
        client = _CountingAsyncClient(
            self.cluster,
            await_counter=self.await_counter,
            release_hook=self._release_client,
        )
        self.active.append(client)
        self.clients.append(client)
        return client

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a pool fetch_rows call and return a counting awaitable."""
        self.calls.append("fetch_rows")
        self.recorded_calls.append(("fetch_rows", sql, params, parameter_oids, timeout))
        awaitable = _CountingAwaitable(
            super().fetch_rows(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a pool fetch_dicts call and return a counting awaitable."""
        self.calls.append("fetch_dicts")
        self.recorded_calls.append(("fetch_dicts", sql, params, parameter_oids, timeout))
        awaitable = _CountingAwaitable(
            super().fetch_dicts(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a pool fetch_value call and return a counting awaitable."""
        self.calls.append("fetch_value")
        self.recorded_calls.append(("fetch_value", sql, params, parameter_oids, timeout))
        awaitable = _CountingAwaitable(
            super().fetch_value(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        parameter_oids: Sequence[int] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Record a pool execute_dml call and return a counting awaitable."""
        self.calls.append("execute_dml")
        self.recorded_calls.append(("execute_dml", sql, params, parameter_oids, timeout))
        awaitable = _CountingAwaitable(
            super().execute_dml(sql, params, parameter_oids=parameter_oids, timeout=timeout),
            self.await_counter,
            on_start=self._enter_single_call,
            on_end=self._exit_single_call,
        )
        self.last_awaitable = awaitable
        return awaitable


@pytest.mark.anyio
async def test_async_cursor_query_matrix_and_error_classes() -> None:
    """Async driver executes select, tuples, command, update rowcount, named params, execute_many, and errors."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    try:
        assert await drv.select("SELECT 1 AS a") == [{"a": 1}]
        assert await drv.execute_with_description("SELECT 1 AS a, 2 AS b") == ([(1, 2)], ("a", "b"))
        assert await drv.execute_with_description("SET search_path TO public") == ([], ())

        inserted = await drv.execute_many(
            "INSERT INTO items (id, name) VALUES (%s, %s)",
            [(1, "one"), (2, "two"), (3, "three")],
        )
        assert inserted == 3

        updated = await drv.execute("UPDATE items SET name = 'updated' WHERE id IN (1, 2)")
        assert updated == 2

        named = await drv.select("SELECT id, name FROM items WHERE id = %(x)s", {"x": 3})
        assert named == [{"id": 3, "name": "three"}]

        with pytest.raises(IntegrityError):
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "duplicate"))
    finally:
        await drv.close()

    with pytest.raises(InterfaceError):
        await drv.select("SELECT 1 AS a")


@pytest.mark.anyio
async def test_async_cursor_method_session_and_lazy_begin_contracts() -> None:
    """Async ``driver.cursor()`` selects pinned session or pool without taking a lease and rejects pooled lazy_begin."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    broker = _AsyncRecordingBroker()
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, broker=broker)

    try:
        pool_cur = drv.cursor()
        await pool_cur.execute("SELECT 1 AS a")
        assert await pool_cur.fetchall() == [(1,)]
        assert len(broker.acquire_calls) == 0

        with pytest.raises(InterfaceError, match="lazy_begin needs a session; this driver is pooled"):
            drv.cursor(lazy_begin=True)
        assert len(broker.acquire_calls) == 0

        async with drv.transaction():
            assert len(broker.acquire_calls) == 1
            tx_cur = drv.cursor(lazy_begin=True)
            await tx_cur.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (10, "tx"))
            assert len(broker.acquire_calls) == 1
        assert len(broker.release_calls) == 1

        scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")
        async with AsyncTenantConnectionScope(drv, "tenant-cur", scope_cfg):
            assert len(broker.acquire_calls) == 2
            tenant_cur = drv.cursor(lazy_begin=True)
            await tenant_cur.execute("SELECT current_setting($1)", ("app.tenant_id",))
            assert await tenant_cur.fetchall() == [("tenant-cur",)]
            assert len(broker.acquire_calls) == 2
        assert len(broker.release_calls) == 2
    finally:
        await drv.close()

    with pytest.raises(InterfaceError):
        drv.cursor()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["no_broker", "transaction", "autocommit", "error", "refused", "cancel"])
async def test_async_broker_checkout_lease_counts(mode: str) -> None:
    """Async broker leases are acquired once per checkout, released on cancel/error, and skipped when absent or refused."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)

    if mode == "no_broker":
        unused_broker = _AsyncRecordingBroker()
        drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, broker=None)
        try:
            for idx in range(10):
                assert await drv.select("SELECT $1 AS n", (idx,)) == [{"n": idx}]
            assert len(unused_broker.acquire_calls) == 0
            assert len(unused_broker.release_calls) == 0
        finally:
            await drv.close()
        return

    broker = _AsyncRecordingBroker(refuse=mode == "refused")
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, broker=broker)
    try:
        if mode == "transaction":
            async with drv.transaction():
                for idx in range(5):
                    await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (idx + 1, f"row-{idx}"))
            assert len(broker.acquire_calls) == 1
            assert len(broker.release_calls) == 1
        elif mode == "autocommit":
            for idx in range(3):
                assert await drv.select("SELECT $1 AS n", (idx,)) == [{"n": idx}]
            assert len(broker.acquire_calls) == 3
            assert len(broker.release_calls) == 3
        elif mode == "error":
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "seed"))
            with pytest.raises(IntegrityError):
                await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "dup"))
            assert len(broker.acquire_calls) == 2
            assert len(broker.release_calls) == 2
        elif mode == "cancel":
            cluster.query_started_event = asyncio.Event()
            cluster.query_release_event = asyncio.Event()

            async def _blocked_query() -> None:
                await drv.select("SELECT 1 AS a")

            task = asyncio.create_task(_blocked_query())
            await cluster.query_started_event.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert len(broker.acquire_calls) == 1
            assert len(broker.release_calls) == 1
        else:
            assert await drv.select("SELECT 1 AS a") == [{"a": 1}]
            assert len(broker.acquire_calls) == 1
            assert len(broker.release_calls) == 0
    finally:
        cluster.query_started_event = None
        cluster.query_release_event = None
        await drv.close()


@pytest.mark.anyio
async def test_async_concurrent_select_tasks() -> None:
    """Twenty concurrent tasks calling ``select`` on one async driver all succeed with their own rows."""
    cluster = FakePostgresCluster()
    pool = FakeAsyncPostgresPool(cluster, max_size=20)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    try:
        results = await asyncio.gather(*(drv.select("SELECT $1 AS n", (idx,)) for idx in range(20)))
        assert results == [[{"n": idx}] for idx in range(20)]
    finally:
        await drv.close()


@pytest.mark.anyio
async def test_async_pool_auth_rejection_retries_once() -> None:
    """A 28P01 authentication rejection on the async pool refreshes the token provider and retries once."""
    cluster = FakePostgresCluster()
    pool = _AsyncAuthRejectingOncePool(cluster)
    token_provider = _StubTokenProvider()
    cfg = PostgreSQLConfig(token_provider=token_provider)
    drv = AsyncPostgreSQLDriver(cfg, pool=pool)
    try:
        assert await drv.select("SELECT 1 AS a") == [{"a": 1}]
        assert pool.execute_attempts == 2
        assert token_provider.invalidate_calls == 1
        assert token_provider.resolve_calls == [True]
    finally:
        await drv.close()


@pytest.mark.anyio
async def test_async_pool_auth_rejection_without_token_provider_propagates_once() -> None:
    """A 28P01 authentication rejection without a token provider propagates after one attempt."""
    cluster = FakePostgresCluster()
    pool = _AsyncAuthRejectingOncePool(cluster)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    try:
        with pytest.raises(OperationalError, match="28P01"):
            await drv.select("SELECT 1 AS a")
        assert pool.execute_attempts == 1
    finally:
        await drv.close()


@pytest.mark.anyio
async def test_async_single_call_pool_route_counts_one_await_per_method() -> None:
    """Each of the eight async query methods awaits exactly one native pool awaitable with no cursor or acquire."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = _CountingAsyncPool(cluster, max_size=2)
    drv = await AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool).connect()

    try:
        assert await drv.select_value("SELECT 1") == 1
        assert pool.calls == ["fetch_value"]
        assert pool.await_count == 1

        pool.reset_counts()
        assert await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "alpha")) == 1
        assert await drv.execute_dml("INSERT INTO items (id, name) VALUES ($1, $2)", (2, "beta")) == (1, None)
        assert await drv.execute_dml("UPDATE items SET name = 'updated' WHERE id IN (1, 2)") == (2, None)
        assert pool.calls == ["execute_dml", "execute_dml", "execute_dml"]
        assert pool.await_count == 3

        pool.reset_counts()
        assert await drv.select("SELECT id, name FROM items ORDER BY id") == [
            {"id": 1, "name": "updated"},
            {"id": 2, "name": "updated"},
        ]
        assert await drv.select_one("SELECT id, name FROM items WHERE id = $1", (1,)) == {"id": 1, "name": "updated"}
        assert await drv.select_one_or_none("SELECT id, name FROM items WHERE id = $1", (99,)) is None
        assert await drv.select_value("SELECT name FROM items WHERE id = $1", (2,)) == "updated"
        assert await drv.execute_with_description("SELECT id, name FROM items WHERE id = $1", (1,)) == (
            [(1, "updated")],
            ("id", "name"),
        )
        assert await drv.execute_tuples("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
        assert await drv.execute("UPDATE items SET name = $1 WHERE id = $2", ("one", 1)) == 1
        assert await drv.execute_dml("UPDATE items SET name = $1 WHERE id = $2", ("two", 2)) == (1, None)

        assert pool.calls == [
            "fetch_dicts",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_value",
            "fetch_rows",
            "fetch_rows",
            "execute_dml",
            "execute_dml",
        ]
        assert pool.await_count == 8
        assert "cursor" not in pool.calls
        assert "acquire" not in pool.calls

        pool.reset_counts()
        typed = await drv.select("SELECT id, name FROM items ORDER BY id", schema_type=_ItemRow)
        assert typed == [_ItemRow(id=1, name="one"), _ItemRow(id=2, name="two")]
        assert pool.calls == ["fetch_rows"]
        assert pool.await_count == 1
    finally:
        await drv.close()


@pytest.mark.anyio
async def test_async_single_call_direct_route_skips_helper_coroutines_and_returns_native_awaitable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connected driver without token provider or broker bypasses helper coroutines and returns the pool awaitable."""
    cluster = FakePostgresCluster()
    pool = _CountingAsyncPool(cluster, max_size=2)
    drv = await AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool).connect()

    helper_calls: list[str] = []

    async def _ensure_spy(*args: Any, **kwargs: Any) -> Any:
        _ = (args, kwargs)
        helper_calls.append("_ensure_pool_async")
        return pool

    async def _checked_spy(*args: Any, **kwargs: Any) -> Any:
        _ = (args, kwargs)
        helper_calls.append("_single_call_checked")
        return None

    monkeypatch.setattr(AsyncPostgreSQLDriver, "_ensure_pool_async", _ensure_spy)
    monkeypatch.setattr(AsyncPostgreSQLDriver, "_single_call_checked", _checked_spy)

    try:
        assert await drv.select_value("SELECT 1") == 1
        assert helper_calls == []
        assert pool.calls == ["fetch_value"]
        assert pool.await_count == 1

        pool.reset_counts()
        returned = drv._single_call(_FETCH_VALUE, "SELECT 1", None, {})
        assert returned is pool.last_awaitable
        assert helper_calls == []
        assert await returned == 1
        assert pool.await_count == 1
    finally:
        await drv.close()


@pytest.mark.anyio
async def test_async_single_call_pinned_transaction_and_tenant_routes() -> None:
    """Pinned transaction and tenant scopes await one client awaitable per statement with zero pool single-calls."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = _CountingAsyncPool(cluster, max_size=2)
    drv = await AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool).connect()

    try:
        await drv.begin()
        pinned_client = pool.active[0]
        assert isinstance(pinned_client, _CountingAsyncClient)
        assert await drv.execute_dml("INSERT INTO items (id, name) VALUES ($1, $2)", (10, "tx-a")) == (1, None)
        assert await drv.select("SELECT id, name FROM items WHERE id = $1", (10,)) == [{"id": 10, "name": "tx-a"}]
        assert await drv.select_value("SELECT name FROM items WHERE id = $1", (10,)) == "tx-a"
        await drv.commit()

        assert pool.calls == ["acquire"]
        assert pinned_client.calls == ["execute_dml", "fetch_dicts", "fetch_value"]
        assert pool.await_count == 3
        assert "cursor" not in pinned_client.calls

        pool.reset_counts()
        pinned_client.calls.clear()
        await drv.begin()
        tx_all_client = pool.active[0]
        assert isinstance(tx_all_client, _CountingAsyncClient)
        assert await drv.select("SELECT id, name FROM items WHERE id = $1", (10,)) == [{"id": 10, "name": "tx-a"}]
        assert await drv.select_one("SELECT id, name FROM items WHERE id = $1", (10,)) == {"id": 10, "name": "tx-a"}
        assert await drv.select_one_or_none("SELECT id, name FROM items WHERE id = $1", (99,)) is None
        assert await drv.select_value("SELECT name FROM items WHERE id = $1", (10,)) == "tx-a"
        assert await drv.execute_with_description("SELECT id FROM items WHERE id = $1", (10,)) == ([(10,)], ("id",))
        assert await drv.execute_tuples("SELECT id FROM items WHERE id = $1", (10,)) == [(10,)]
        assert await drv.execute("UPDATE items SET name = $1 WHERE id = $2", ("tx-b", 10)) == 1
        assert await drv.execute_dml("UPDATE items SET name = $1 WHERE id = $2", ("tx-c", 10)) == (1, None)
        await drv.commit()

        assert pool.calls == ["acquire"]
        assert tx_all_client.calls == [
            "fetch_dicts",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_value",
            "fetch_rows",
            "fetch_rows",
            "execute_dml",
            "execute_dml",
        ]
        assert pool.await_count == 8
        assert "cursor" not in tx_all_client.calls

        pool.reset_counts()
        tx_all_client.calls.clear()
        scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")
        async with AsyncTenantConnectionScope(drv, "tenant-z", scope_cfg):
            tenant_client = pool.active[0]
            assert isinstance(tenant_client, _CountingAsyncClient)
            assert await drv.select("SELECT id FROM items WHERE id = $1", (10,)) == [{"id": 10}]
            assert await drv.select_one("SELECT id FROM items WHERE id = $1", (10,)) == {"id": 10}
            assert await drv.select_one_or_none("SELECT id FROM items WHERE id = $1", (99,)) is None
            assert await drv.select_value("SELECT current_setting($1)", ("app.tenant_id",)) == "tenant-z"
            assert await drv.execute_with_description("SELECT id FROM items WHERE id = $1", (10,)) == ([(10,)], ("id",))
            assert await drv.execute_tuples("SELECT id FROM items WHERE id = $1", (10,)) == [(10,)]
            assert await drv.execute("UPDATE items SET name = $1 WHERE id = $2", ("tenant-1", 10)) == 1
            assert await drv.execute_dml("UPDATE items SET name = $1 WHERE id = $2", ("tenant-2", 10)) == (1, None)

        assert pool.calls == ["acquire"]
        assert tenant_client.calls == [
            "execute_dml",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_dicts",
            "fetch_value",
            "fetch_rows",
            "fetch_rows",
            "execute_dml",
            "execute_dml",
            "execute_dml",
        ]
        assert pool.await_count == 10
        assert "cursor" not in tenant_client.calls
    finally:
        await drv.close()


@pytest.mark.anyio
async def test_async_single_call_value_options_not_found_and_trace_comment() -> None:
    """Async ``select_value`` handles NotFoundError, value_type coercion, options forwarding, and trace comments."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = _CountingAsyncPool(cluster, max_size=2)
    drv = await AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool).connect()

    try:
        with pytest.raises(NotFoundError, match="Query returned no rows"):
            await drv.select_value("SELECT id FROM items WHERE id = $1", (999,))

        assert await drv.select_value("SELECT '5'", value_type=int) == 5

        assert await drv.select_value("SELECT $1", (42,), timeout=1.5, parameter_oids=(23,)) == 42
        last_method, _, _, last_oids, last_timeout = pool.recorded_calls[-1]
        assert last_method == "fetch_value"
        assert last_oids == (23,)
        assert last_timeout == 1.5

        ctx = TraceContext.generate()
        prev_ctx = TraceContext.current()
        TraceContext.set_current(ctx)
        try:
            assert await drv.select_value("SELECT 7") == 7
        finally:
            TraceContext.set_current(prev_ctx)
        traced_sql = pool.recorded_calls[-1][1]
        assert traced_sql.endswith(f"/*traceparent='{ctx.format()}'*/")
    finally:
        await drv.close()


@pytest.mark.anyio
async def test_async_single_call_cancel_on_leased_route_evicts_client_and_releases_lease() -> None:
    """Cancelling a statement on the leased route releases the broker lease and evicts the tainted client."""
    cluster = FakePostgresCluster()
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    broker = _AsyncRecordingBroker()
    drv = await AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool, broker=broker).connect()

    cluster.query_started_event = asyncio.Event()
    cluster.query_release_event = asyncio.Event()
    try:

        async def _blocked() -> None:
            await drv.select_value("SELECT 1")

        task = asyncio.create_task(_blocked())
        await cluster.query_started_event.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(broker.acquire_calls) == 1
        assert len(broker.release_calls) == 1
        assert len(pool.evicted) == 1
        assert len(pool.active) == 0
    finally:
        cluster.query_started_event = None
        cluster.query_release_event = None
        await drv.close()


@pytest.mark.anyio
async def test_async_single_call_closed_and_reopen_uses_pool_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """Closing clears ``_fast_pool`` so queries raise InterfaceError, and ``connect()`` restores the pool route."""
    cluster = FakePostgresCluster()
    created_pools: list[_CountingAsyncPool] = []

    def _make_pool(**kwargs: Any) -> _CountingAsyncPool:
        _ = kwargs
        pool = _CountingAsyncPool(cluster, max_size=2)
        created_pools.append(pool)
        return pool

    monkeypatch.setattr(
        PostgreSQLCore,
        "resolve_native_module",
        staticmethod(lambda: SimpleNamespace(AsyncPostgresPool=_make_pool)),
    )

    drv = await AsyncPostgreSQLDriver(PostgreSQLConfig()).connect()
    assert await drv.select("SELECT 1 AS a") == [{"a": 1}]
    assert created_pools[0].calls == ["fetch_dicts"]
    assert created_pools[0].await_count == 1

    await drv.close()
    with pytest.raises(InterfaceError):
        await drv.select("SELECT 1 AS a")

    await drv.connect()
    try:
        assert await drv.select("SELECT 2 AS b") == [{"b": 2}]
        assert len(created_pools) == 2
        assert created_pools[1].calls == ["fetch_dicts"]
        assert created_pools[1].await_count == 1
    finally:
        await drv.close()
