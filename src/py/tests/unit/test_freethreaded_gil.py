"""Unit tests for free-threaded Python 3.14t GIL declaration and Headers key interning."""

from __future__ import annotations

import os
import sys
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


def test_native_modules_declare_gil_used_false() -> None:
    """Verify native PyO3 extension modules declare gil_used = false and do not re-enable the GIL."""
    checker = getattr(sys, "_is_gil_enabled", None)
    was_disabled = bool(checker is not None and not checker())

    assert getattr(_vertebra, "GIL_USED", None) is False
    for mod_name in (
        "vertebra._bundler",
        "vertebra._mysql",
        "vertebra._bigquery",
        "vertebra._spanner",
        "vertebra._duckdb",
    ):
        ext_mod = pytest.importorskip(mod_name)
        assert getattr(ext_mod, "GIL_USED", None) is False

    storage_mod = pytest.importorskip("vertebra._storage")
    assert hasattr(storage_mod, "SyncObjectStore")
    if hasattr(storage_mod, "GIL_USED"):
        assert storage_mod.GIL_USED is False

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
