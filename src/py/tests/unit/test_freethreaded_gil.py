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

from vertebra._vertebra import ArrowCursor, Headers
from vertebra.db.driver import InterfaceError

_GIL_EXEMPT_MODULES: tuple[str, ...] = (
    "vertebra._vertebra",
    "vertebra._bundler",
    "vertebra._storage",
    "vertebra._mysql",
    "vertebra._bigquery",
    "vertebra._mssql",
    "vertebra._oracle",
    "vertebra._postgres",
    "vertebra._spanner",
    "vertebra._duckdb",
    "vertebra._sqlite",
    "vertebra._redis",
)
_REDIS_CLASSES: tuple[str, ...] = (
    "SyncRedisClient",
    "AsyncRedisClient",
    "SyncRedisPipeline",
    "AsyncRedisPipeline",
    "SyncRedisListener",
    "AsyncRedisListener",
)
_POSTGRES_CLASSES: tuple[str, ...] = (
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
    "SyncPostgresListener",
    "AsyncPostgresListener",
    "AsyncPostgresReplication",
)
_ORACLE_CLASSES: tuple[str, ...] = (
    "SyncOracleClient",
    "AsyncOracleClient",
    "SyncOraclePool",
    "AsyncOraclePool",
    "OracleColumn",
    "OracleQueryResult",
    "OraclePipelineOutcome",
    "OracleNotification",
    "SyncOracleListener",
    "AsyncOracleListener",
)
_IMMUTABLE_PYCLASSES: tuple[tuple[str, str], ...] = (
    *(
        ("vertebra._vertebra", cls_name)
        for cls_name in (
            "Request",
            "Headers",
            "Response",
            "RequestBatch",
            "AsgiScope",
            "WsgiEnviron",
            "RsgiScope",
            "RsgiHttpProtocol",
            "LeaseBroker",
            "ReclaimedLease",
            "PartitionStats",
            "CloudSQLDialer",
            "ArrowCursor",
        )
    ),
    *(("vertebra._bundler", cls_name) for cls_name in ("Bundler", "BundleConfig", "SSRPool")),
    *(
        ("vertebra._storage", cls_name)
        for cls_name in ("SyncObjectStore", "AsyncObjectStore", "StorageBuffer", "ObjectMeta")
    ),
    *(("vertebra._mysql", cls_name) for cls_name in ("SyncMySQLClient", "AsyncMySQLClient", "MySQLWireTestServer")),
    *(("vertebra._bigquery", cls_name) for cls_name in ("SyncBigQueryClient", "AsyncBigQueryClient")),
    *(("vertebra._mssql", cls_name) for cls_name in ("SyncMSSQLClient", "AsyncMSSQLClient")),
    *(("vertebra._oracle", cls_name) for cls_name in _ORACLE_CLASSES),
    *(("vertebra._postgres", cls_name) for cls_name in _POSTGRES_CLASSES),
    *(("vertebra._spanner", cls_name) for cls_name in ("SyncSpannerClient", "AsyncSpannerClient")),
    *(("vertebra._duckdb", cls_name) for cls_name in ("SyncDuckDBClient", "AsyncDuckDBClient")),
    *(("vertebra._sqlite", cls_name) for cls_name in ("SyncSQLiteClient", "AsyncSQLiteClient")),
    *(("vertebra._redis", cls_name) for cls_name in _REDIS_CLASSES),
)


@pytest.mark.parametrize("mod_name", _GIL_EXEMPT_MODULES)
def test_native_modules_declare_gil_used_false(mod_name: str) -> None:
    """Verify native PyO3 extension modules declare gil_used = false and do not re-enable the GIL."""
    checker = getattr(sys, "_is_gil_enabled", None)
    was_disabled = bool(checker is not None and not checker())

    ext_mod = pytest.importorskip(mod_name)
    assert getattr(ext_mod, "GIL_USED", None) is False
    assert isinstance(getattr(ext_mod, "FREE_THREADED", None), bool)

    if was_disabled and checker is not None:
        assert checker() is False
        assert getattr(ext_mod, "FREE_THREADED", True) is True


@pytest.mark.parametrize("export_name", _POSTGRES_CLASSES)
def test_postgres_native_module_exports_classes(export_name: str) -> None:
    """Verify vertebra._postgres declares GIL_USED = False and exports each native class."""
    pg_mod = pytest.importorskip("vertebra._postgres")
    assert pg_mod.GIL_USED is False
    assert isinstance(pg_mod.FREE_THREADED, bool)
    assert isinstance(pg_mod.__version__, str) and pg_mod.__version__
    cls = getattr(pg_mod, export_name, None)
    assert isinstance(cls, type), f"Missing native PostgreSQL type {export_name}"


@pytest.mark.parametrize("export_name", _ORACLE_CLASSES)
def test_oracle_native_module_exports_classes(export_name: str) -> None:
    """Verify vertebra._oracle declares GIL_USED = False and exports each native class."""
    oracle_mod = pytest.importorskip("vertebra._oracle")
    assert oracle_mod.GIL_USED is False
    assert isinstance(oracle_mod.FREE_THREADED, bool)
    assert isinstance(oracle_mod.__version__, str)
    assert oracle_mod.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False
    cls = getattr(oracle_mod, export_name, None)
    assert isinstance(cls, type), f"Missing native Oracle type {export_name}"


@pytest.mark.parametrize("export_name", _REDIS_CLASSES)
def test_redis_native_module_exports_classes(export_name: str) -> None:
    """Verify vertebra._redis declares GIL_USED = False and exports each native class."""
    redis_mod = pytest.importorskip("vertebra._redis")
    assert redis_mod.GIL_USED is False
    assert isinstance(redis_mod.FREE_THREADED, bool)
    assert isinstance(redis_mod.__version__, str) and redis_mod.__version__
    cls = getattr(redis_mod, export_name, None)
    assert isinstance(cls, type), f"Missing native Redis type {export_name}"


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


@pytest.mark.parametrize(("mod_name", "cls_name"), _IMMUTABLE_PYCLASSES)
def test_native_pyclasses_enforce_immutable_type(mod_name: str, cls_name: str) -> None:
    """Verify each non-subclassed native PyO3 type enforces Py_TPFLAGS_IMMUTABLETYPE."""
    ext_mod = pytest.importorskip(mod_name)
    if mod_name == "vertebra._bundler" and cls_name == "SSRPool" and not hasattr(ext_mod, cls_name):
        pytest.skip("Optional bundler SSRPool built without SSR feature")
    cls = getattr(ext_mod, cls_name)
    assert isinstance(cls, type)
    attr_name = "_mutated_type_attr"
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
