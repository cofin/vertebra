"""Unit tests for free-threaded Python 3.14t GIL declaration and Headers key interning."""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from vertebra import _vertebra
from vertebra._vertebra import (
    ArrowCursor,
    AsgiScope,
    CloudSQLDialer,
    Headers,
    LeaseBroker,
    Request,
    RequestBatch,
    Response,
    RsgiHttpProtocol,
    RsgiScope,
    WsgiEnviron,
)
from vertebra.db.driver import InterfaceError

NATIVE_MODULES: tuple[str, ...] = (
    "vertebra._vertebra",
    "vertebra._bundler",
    "vertebra._bigquery",
    "vertebra._oracle",
    "vertebra._spanner",
    "vertebra._storage",
)


def test_native_modules_declare_gil_used_false() -> None:
    """Verify native PyO3 extension modules declare gil_used = false and do not re-enable the GIL."""
    checker = getattr(sys, "_is_gil_enabled", None)
    was_disabled = bool(checker is not None and not checker())

    assert getattr(_vertebra, "GIL_USED", None) is False
    for mod_name in (
        "vertebra._bundler",
        "vertebra._mysql",
        "vertebra._bigquery",
        "vertebra._mssql",
        "vertebra._postgres",
        "vertebra._spanner",
        "vertebra._duckdb",
    ):
        ext_mod = pytest.importorskip(mod_name)
        if mod_name == "vertebra._storage":
            assert hasattr(ext_mod, "SyncObjectStore")
            if hasattr(ext_mod, "GIL_USED"):
                assert ext_mod.GIL_USED is False
        else:
            assert getattr(ext_mod, "GIL_USED", None) is False
        if mod_name == "vertebra._postgres":
            assert isinstance(getattr(ext_mod, "FREE_THREADED", None), bool)
            for pg_export in (
                "SyncPostgresClient",
                "AsyncPostgresClient",
                "SyncPostgresPool",
                "AsyncPostgresPool",
                "SyncPostgresPortal",
                "AsyncPostgresPortal",
                "SyncPostgresArrowStream",
                "AsyncPostgresArrowStream",
                "SyncPostgresCopyOut",
                "SyncPostgresCopyIn",
                "AsyncPostgresCopyOut",
                "AsyncPostgresCopyIn",
                "AsyncPostgresListener",
                "AsyncPostgresReplication",
            ):
                assert isinstance(getattr(ext_mod, pg_export, None), type), (
                    f"Missing native PostgreSQL type {pg_export}"
                )

    oracle_mod = pytest.importorskip("vertebra._oracle")
    assert oracle_mod.GIL_USED is False
    assert isinstance(oracle_mod.FREE_THREADED, bool)
    assert isinstance(oracle_mod.__version__, str)
    assert oracle_mod.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False
    for export_name in (
        "SyncOracleClient",
        "AsyncOracleClient",
        "SyncOraclePool",
        "AsyncOraclePool",
        "OracleColumn",
        "OracleQueryResult",
        "OraclePipelineOutcome",
        "OracleNotification",
        "AsyncOracleListener",
    ):
        cls = getattr(oracle_mod, export_name, None)
        assert isinstance(cls, type), f"Missing native Oracle type {export_name}"

    if was_disabled and checker is not None:
        assert checker() is False
        assert oracle_mod.FREE_THREADED is True


def test_oracle_native_clients_concurrent_construction() -> None:
    """Verify vertebra._oracle constructs clients concurrently across threads without re-enabling the GIL."""
    oracle_mod = pytest.importorskip("vertebra._oracle")
    checker = getattr(sys, "_is_gil_enabled", None)
    was_disabled = bool(checker is not None and not checker())
    barrier = threading.Barrier(8)

    def _build(worker_id: int) -> tuple[bool, bool, bool]:
        barrier.wait(timeout=5.0)
        rejected_memory = False
        try:
            oracle_mod.SyncOracleClient(host="memory", service_name=f"ft_{worker_id}")
        except InterfaceError:
            rejected_memory = True

        sync_client = oracle_mod.SyncOracleClient(
            host="127.0.0.1",
            port=1521,
            service_name=f"ft_{worker_id}",
        )
        async_client = oracle_mod.AsyncOracleClient(
            host="127.0.0.1",
            port=1521,
            service_name=f"ft_async_{worker_id}",
        )
        sync_pool = oracle_mod.SyncOraclePool(
            host="127.0.0.1",
            port=1521,
            service_name=f"ft_pool_{worker_id}",
            min_size=0,
            max_size=2,
        )
        try:
            sync_client.begin()
            sync_client.set_session_attrs(module=f"ft_mod_{worker_id}")
            sync_client.rollback()
            ok_sync = (
                sync_client.in_transaction is False
                and sync_client.execute_many("INSERT INTO t VALUES (:1)", []) == 0
                and sync_pool.stats()["max_size"] == 2
            )
            ok_async = not async_client.closed
            return (rejected_memory, ok_sync, ok_async)
        finally:
            sync_pool.close()
            sync_client.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(_build, range(8)))

    assert outcomes == [(True, True, True)] * 8
    if was_disabled and checker is not None:
        assert checker() is False


def test_postgres_native_clients_and_pools_concurrent_construction() -> None:
    """Verify vertebra._postgres constructs clients and pools across 8 threads without enabling GIL."""
    postgres_mod = pytest.importorskip("vertebra._postgres")
    checker = getattr(sys, "_is_gil_enabled", None)
    was_disabled = bool(checker is not None and not checker())
    barrier = threading.Barrier(8)

    def _build(worker_id: int) -> tuple[bool, bool, bool, bool]:
        barrier.wait(timeout=5.0)
        rejected_bad_tls = False
        try:
            postgres_mod.SyncPostgresClient(
                user=f"ft_{worker_id}",
                database="postgres",
                unix_socket="/tmp/.s.PGSQL.5432",
                sslmode="require",
            )
        except ValueError:
            rejected_bad_tls = True

        sync_client = postgres_mod.SyncPostgresClient(
            user=f"ft_{worker_id}",
            database="postgres",
            host="127.0.0.1",
            port=5432,
            sslmode="disable",
        )
        async_client = postgres_mod.AsyncPostgresClient(
            user=f"ft_async_{worker_id}",
            database="postgres",
            host="127.0.0.1",
            port=5432,
            sslmode="disable",
        )
        sync_pool = postgres_mod.SyncPostgresPool(
            user=f"ft_pool_{worker_id}",
            database="postgres",
            host="127.0.0.1",
            port=5432,
            sslmode="disable",
            min_size=0,
            max_size=2,
        )
        async_pool = postgres_mod.AsyncPostgresPool(
            user=f"ft_apool_{worker_id}",
            database="postgres",
            host="127.0.0.1",
            port=5432,
            sslmode="disable",
            min_size=0,
            max_size=2,
        )
        try:
            sync_pool.validate_configuration(
                user=f"ft_pool_{worker_id}",
                database="postgres",
                host="127.0.0.1",
                port=5432,
                sslmode="disable",
                min_size=0,
                max_size=4,
            )
            async_pool.validate_configuration(
                user=f"ft_apool_{worker_id}",
                database="postgres",
                host="127.0.0.1",
                port=5432,
                sslmode="disable",
                min_size=0,
                max_size=4,
            )
            ok_pools = sync_pool.closed is False and async_pool.closed is False
            ok_clients = sync_client.closed is True and async_client.closed is True
            sync_pool.close()
            sync_client.close()
            ok_closed = sync_pool.closed is True and sync_client.closed is True
            return (rejected_bad_tls, ok_pools, ok_clients, ok_closed)
        finally:
            if not sync_pool.closed:
                sync_pool.close()
            if not sync_client.closed:
                sync_client.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(_build, range(8)))

    assert outcomes == [(True, True, True, True)] * 8
    if was_disabled and checker is not None:
        assert checker() is False


def test_headers_interned_keys_and_concurrent_thread_access() -> None:
    """Verify Headers interns standard lowercase HTTP header keys across concurrent threads."""
    pairs = [
        ("Content-Type", "application/json"),
        ("Content-Length", "128"),
        ("Authorization", "Bearer token"),
        ("Accept", "*/*"),
        ("Host", "localhost:8000"),
        ("User-Agent", "vertebra-test/1.0"),
        ("Cookie", "sid=abc"),
        ("Upgrade", "h2c"),
        ("Connection", "Upgrade"),
        ("X-Custom-Trace", "trace-99"),
    ]
    headers = Headers.from_pairs(pairs)

    keys_first = headers.keys()
    keys_second = headers.keys()
    assert keys_first == [
        "content-type",
        "content-length",
        "authorization",
        "accept",
        "host",
        "user-agent",
        "cookie",
        "upgrade",
        "connection",
        "x-custom-trace",
    ]
    for idx in range(9):
        assert keys_first[idx] is keys_second[idx]

    def _worker(iteration: int) -> tuple[str, str, int]:
        ctype = headers["content-type"]
        custom = headers["x-custom-trace"]
        return (ctype, custom, len(headers) + iteration - iteration)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_worker, range(64)))

    assert all(r == ("application/json", "trace-99", 10) for r in results)


def test_native_pyclasses_enforce_immutable_type() -> None:
    """Verify non-subclassed native PyO3 types enforce Py_TPFLAGS_IMMUTABLETYPE."""
    bundler_mod = pytest.importorskip("vertebra._bundler")
    storage_mod = pytest.importorskip("vertebra._storage")
    mysql_mod = pytest.importorskip("vertebra._mysql")
    bigquery_mod = pytest.importorskip("vertebra._bigquery")
    mssql_mod = pytest.importorskip("vertebra._mssql")
    oracle_mod = pytest.importorskip("vertebra._oracle")
    postgres_mod = pytest.importorskip("vertebra._postgres")
    spanner_mod = pytest.importorskip("vertebra._spanner")
    duckdb_mod = pytest.importorskip("vertebra._duckdb")

    raw_classes: list[object] = [
        Request,
        Headers,
        Response,
        RequestBatch,
        AsgiScope,
        WsgiEnviron,
        RsgiScope,
        RsgiHttpProtocol,
        LeaseBroker,
        CloudSQLDialer,
        ArrowCursor,
        bundler_mod.Bundler,
        bundler_mod.BundleConfig,
        getattr(bundler_mod, "SSRPool", None),
        storage_mod.SyncObjectStore,
        storage_mod.AsyncObjectStore,
        storage_mod.StorageBuffer,
        storage_mod.ObjectMeta,
        mysql_mod.SyncMySQLClient,
        mysql_mod.AsyncMySQLClient,
        mysql_mod.MySQLWireTestServer,
        bigquery_mod.SyncBigQueryClient,
        bigquery_mod.AsyncBigQueryClient,
        mssql_mod.SyncMSSQLClient,
        mssql_mod.AsyncMSSQLClient,
        oracle_mod.SyncOracleClient,
        oracle_mod.AsyncOracleClient,
        oracle_mod.SyncOraclePool,
        oracle_mod.AsyncOraclePool,
        oracle_mod.OracleColumn,
        oracle_mod.OracleQueryResult,
        oracle_mod.OraclePipelineOutcome,
        oracle_mod.OracleNotification,
        oracle_mod.AsyncOracleListener,
        postgres_mod.SyncPostgresClient,
        postgres_mod.AsyncPostgresClient,
        postgres_mod.SyncPostgresPool,
        postgres_mod.AsyncPostgresPool,
        postgres_mod.SyncPostgresPortal,
        postgres_mod.AsyncPostgresPortal,
        postgres_mod.SyncPostgresArrowStream,
        postgres_mod.AsyncPostgresArrowStream,
        postgres_mod.SyncPostgresCopyOut,
        postgres_mod.SyncPostgresCopyIn,
        postgres_mod.AsyncPostgresCopyOut,
        postgres_mod.AsyncPostgresCopyIn,
        postgres_mod.AsyncPostgresListener,
        postgres_mod.AsyncPostgresReplication,
        spanner_mod.SyncSpannerClient,
        spanner_mod.AsyncSpannerClient,
        duckdb_mod.SyncDuckDBClient,
        duckdb_mod.AsyncDuckDBClient,
    ]
    classes: list[type[object]] = [cls for cls in raw_classes if isinstance(cls, type)]
    attr_name = "_mutated_type_attr"
    for cls in classes:
        with pytest.raises(TypeError):
            setattr(cls, attr_name, 1)


def test_arrow_cursor_from_shm_capsule_concurrent_single_winner() -> None:
    """Verify ArrowCursor._from_shm_capsule atomically consumes a single capsule across 8 concurrent threads."""
    seg_hint = f"/vtb-cap-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    seg_name, byte_len, capsule = ArrowCursor._allocate_shm_capsule(seg_hint, b"VTBARROW-CAPSULE-TEST")
    barrier = threading.Barrier(8)

    def _consume(_worker_id: int) -> str:
        barrier.wait(timeout=5.0)
        try:
            cursor = ArrowCursor._from_shm_capsule(seg_name, byte_len, 1, capsule)
        except ValueError:
            return "value_error"
        cursor.release()
        return "winner"

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(_consume, range(8)))

    assert outcomes.count("winner") == 1
    assert outcomes.count("value_error") == 7


def test_postgres_import_preserves_disabled_gil_in_child() -> None:
    """Verify importing vertebra._postgres in an isolated free-threaded child preserves disabled GIL."""
    if not bool(sysconfig.get_config_var("Py_GIL_DISABLED")):
        pytest.skip("Free-threaded CPython build required (Py_GIL_DISABLED != 1)")

    env = dict(os.environ)
    env.pop("PYTHON_GIL", None)
    script = (
        "import sys\n"
        "assert hasattr(sys, '_is_gil_enabled')\n"
        "assert sys._is_gil_enabled() is False\n"
        "import vertebra._postgres as pg\n"
        "assert sys._is_gil_enabled() is False\n"
        "assert pg.FREE_THREADED is True\n"
        "assert pg.GIL_USED is False\n"
        "assert pg.SyncPostgresClient(user='u', database='d').closed is True\n"
    )
    subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
