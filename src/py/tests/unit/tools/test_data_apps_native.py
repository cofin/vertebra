"""Unit tests for the shared benchmark DataBackend and Vertebra native data app."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest
from tools.benchmarks.apps._data_backend import (
    FORBIDDEN_STORAGE_BACKENDS,
    JSON_HELLO_PAYLOAD,
    WORLD_ROW_COUNT,
    DataBackend,
    DataBackendConfig,
    WorldRow,
    assert_free_threaded_runtime,
    clamp_query_count,
    get_default_backend,
    load_backend_config,
    resolve_pool_budget,
)
from tools.benchmarks.apps.data_native import create_native_data_app, native_data_app

from vertebra.storage import (
    AzureConfig,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
)
from vertebra.utils.serializers import decode_json


def test_load_backend_config_defaults() -> None:
    """Verify default configuration values when no benchmark env vars are set."""
    cfg = load_backend_config({})
    assert cfg.db_backend == "memory"
    assert cfg.db_dsn == ""
    assert cfg.cache_backend == "memory"
    assert cfg.cache_dsn == ""
    assert cfg.storage_backend == "memory"
    assert cfg.storage_endpoint == ""
    assert cfg.storage_bucket == "vertebra-bench"
    assert cfg.storage_root == ""
    assert cfg.workers == 1
    assert cfg.broker_mode == "none"
    assert cfg.total_pool_budget == 40
    assert cfg.strict_free_threading is False


def test_load_backend_config_from_env_and_rejects_minio() -> None:
    """Verify explicit environment parsing and fail-closed rejection of MinIO."""
    cfg = load_backend_config(
        {
            "VTB_BENCH_DB_BACKEND": "sqlite",
            "VTB_BENCH_DB_DSN": ":memory:",
            "VTB_BENCH_CACHE_BACKEND": "memory",
            "VTB_BENCH_STORAGE_BACKEND": "rustfs",
            "VTB_BENCH_STORAGE_ENDPOINT": "http://127.0.0.1:9000",
            "VTB_BENCH_STORAGE_BUCKET": "bench-bucket",
            "VTB_BENCH_WORKERS": "4",
            "VTB_BENCH_BROKER_MODE": "shm_broker",
            "VTB_BENCH_TOTAL_POOL_BUDGET": "64",
            "VERTEBRA_STRICT_FREE_THREADING": "true",
        }
    )
    assert cfg.db_backend == "sqlite"
    assert cfg.db_dsn == ":memory:"
    assert cfg.storage_backend == "rustfs"
    assert cfg.storage_endpoint == "http://127.0.0.1:9000"
    assert cfg.storage_bucket == "bench-bucket"
    assert cfg.workers == 4
    assert cfg.broker_mode == "shm_broker"
    assert cfg.total_pool_budget == 64
    assert cfg.strict_free_threading is True

    assert "minio" in FORBIDDEN_STORAGE_BACKENDS
    with pytest.raises(ValueError, match="MinIO is forbidden"):
        load_backend_config({"VTB_BENCH_STORAGE_BACKEND": "minio"})
    with pytest.raises(ValueError, match="MinIO is forbidden"):
        load_backend_config({"VTB_BENCH_STORAGE_BACKEND": "MINIO"})
    with pytest.raises(ValueError, match="MinIO is forbidden"):
        DataBackend(DataBackendConfig(storage_backend="minio"))


@pytest.mark.parametrize(
    ("workers", "broker_mode", "total_budget", "expected"),
    [
        (1, "per_worker", 40, 40),
        (4, "per_worker", 40, 10),
        (8, "per_worker", 40, 5),
        (1, "shm_broker", 40, 40),
        (4, "shm_broker", 40, 40),
        (8, "shm_broker", 40, 40),
        (4, "none", 40, 10),
        (64, "per_worker", 40, 1),
    ],
)
def test_resolve_pool_budget_parity(
    workers: int,
    broker_mode: str,
    total_budget: int,
    expected: int,
) -> None:
    """Verify matched pool-budget parity across worker counts and broker modes."""
    assert resolve_pool_budget(workers=workers, broker_mode=broker_mode, total_budget=total_budget) == expected
    backend = DataBackend(
        DataBackendConfig(
            workers=workers,
            broker_mode=broker_mode,
            total_pool_budget=total_budget,
        )
    )
    try:
        assert backend.pool_size == expected
    finally:
        backend.close()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, 1),
        ("foo", 1),
        ("-5", 1),
        ("0", 1),
        ("1", 1),
        ("20", 20),
        ("500", 500),
        ("999", 500),
        (-3, 1),
        (0, 1),
        (42, 42),
        (777, 500),
    ],
)
def test_clamp_query_count(raw: str | int | None, expected: int) -> None:
    """Verify TechEmpower query count parsing and clamping semantics."""
    assert clamp_query_count(raw) == expected


def test_assert_free_threaded_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify strict free-threading enforcement raises when GIL is enabled and succeeds when disabled."""
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
    assert assert_free_threaded_runtime(strict=False) is False
    with pytest.raises(RuntimeError, match="GIL"):
        assert_free_threaded_runtime(strict=True)

    monkeypatch.setenv("VERTEBRA_STRICT_FREE_THREADING", "1")
    with pytest.raises(RuntimeError, match="GIL"):
        assert_free_threaded_runtime(strict=None)

    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
    assert assert_free_threaded_runtime(strict=True) is True
    assert assert_free_threaded_runtime(strict=None) is True


@pytest.mark.parametrize(
    ("storage_backend", "expected_type"),
    [
        ("memory", MemoryConfig),
        ("local", LocalConfig),
        ("rustfs", S3Config),
        ("floci_s3", S3Config),
        ("floci_aws", S3Config),
        ("floci_gcs", GCSConfig),
        ("floci_oci", OCIConfig),
        ("r2", R2Config),
        ("floci_r2", R2Config),
        ("azurite", AzureConfig),
    ],
)
def test_build_storage_config_providers(
    storage_backend: str,
    expected_type: type[Any],
    tmp_path: Path,
) -> None:
    """Verify storage configuration mapping for all non-MinIO providers."""
    backend = DataBackend(
        DataBackendConfig(
            storage_backend=storage_backend,
            storage_root=str(tmp_path / "local-store"),
            storage_bucket="test-bucket",
        )
    )
    try:
        storage_cfg = backend.build_storage_config()
        assert isinstance(storage_cfg, expected_type)
    finally:
        backend.close()


@pytest.mark.parametrize(
    ("db_backend", "db_dsn"),
    [
        ("memory", ""),
        ("sqlite", ":memory:"),
    ],
)
def test_data_backend_offline_operations(db_backend: str, db_dsn: str) -> None:
    """Verify offline DB, cache, and object-storage operations on memory and SQLite backends."""

    async def _run() -> None:
        backend = DataBackend(DataBackendConfig(db_backend=db_backend, db_dsn=db_dsn, storage_backend="memory"))
        try:
            single = await backend.get_world()
            assert isinstance(single, WorldRow)
            assert 1 <= single.id <= WORLD_ROW_COUNT
            assert 1 <= single.randomNumber <= WORLD_ROW_COUNT

            many = await backend.get_worlds(12)
            assert len(many) == 12
            assert all(isinstance(row, WorldRow) for row in many)

            updated = await backend.update_worlds(7)
            assert len(updated) == 7
            assert all(1 <= row.randomNumber <= WORLD_ROW_COUNT for row in updated)

            cached = await backend.get_cached_worlds(15)
            assert len(cached) == 15
            assert all(isinstance(row, WorldRow) for row in cached)

            sync_row = backend.get_world_sync()
            assert isinstance(sync_row, WorldRow)
            assert len(backend.get_worlds_sync(4)) == 4
            assert len(backend.update_worlds_sync(3)) == 3
            assert len(backend.get_cached_worlds_sync(5)) == 5

            assert await backend.get_object("missing-key") is None
            payload = b"vertebra-benchmark-payload-12345"
            written = await backend.put_object("bench/key-1", payload)
            assert written == len(payload)
            assert await backend.get_object("bench/key-1") == payload
            assert backend.get_object_sync("bench/key-1") == payload

            sync_payload = b"sync-payload"
            assert backend.put_object_sync("bench/key-2", sync_payload) == len(sync_payload)
            assert await backend.get_object("bench/key-2") == sync_payload
        finally:
            backend.close()

    asyncio.run(_run())


def test_sqlite_in_memory_isolation() -> None:
    """Verify two SQLite :memory: backends keep independent state through the driver API."""
    first = DataBackend(DataBackendConfig(db_backend="sqlite", db_dsn=":memory:"))
    second = DataBackend(DataBackendConfig(db_backend="sqlite", db_dsn=":memory:"))
    try:
        assert first._driver is not None
        assert second._driver is not None
        first._driver.update_worlds_sync([(1, 99999)])
        row_first = first._driver.fetch_world_sync(1)
        row_second = second._driver.fetch_world_sync(1)
        assert row_first.randomNumber == 99999
        assert row_second.randomNumber != 99999
    finally:
        first.close()
        second.close()


def test_get_default_backend_singleton() -> None:
    """Verify get_default_backend returns a stable process-local DataBackend."""
    first = get_default_backend()
    second = get_default_backend()
    assert first is second
    assert native_data_app is not None


def test_create_native_data_app_routes_and_handlers() -> None:
    """Verify route registration, startup hook, and direct handler responses for native data app."""

    async def _run() -> None:
        backend = DataBackend(DataBackendConfig(db_backend="memory", storage_backend="memory"))
        try:
            app = create_native_data_app(backend)
            route_map = {(route.method, route.path): route.handler for route in app.routes}
            expected_routes = {
                ("GET", "/json"),
                ("GET", "/db"),
                ("GET", "/queries"),
                ("GET", "/updates"),
                ("GET", "/cached-queries"),
                ("GET", "/object/{key}"),
                ("PUT", "/object/{key}"),
                ("GET", "/health"),
            }
            assert expected_routes.issubset(set(route_map.keys()))

            await app.startup()

            json_resp = route_map[("GET", "/json")]()
            assert json_resp.status == 200
            assert json_resp.body == JSON_HELLO_PAYLOAD

            health_resp = route_map[("GET", "/health")]()
            assert health_resp.status == 200
            assert health_resp.body == b"ok"

            db_resp = await route_map[("GET", "/db")]()
            assert db_resp.status == 200
            db_data = decode_json(db_resp.body)
            assert isinstance(db_data, dict)
            assert 1 <= db_data["id"] <= WORLD_ROW_COUNT
            assert 1 <= db_data["randomNumber"] <= WORLD_ROW_COUNT

            queries_resp = await route_map[("GET", "/queries")](queries="5")
            assert queries_resp.status == 200
            queries_data = decode_json(queries_resp.body)
            assert isinstance(queries_data, list)
            assert len(queries_data) == 5

            clamped_resp = await route_map[("GET", "/queries")](queries="999")
            assert len(decode_json(clamped_resp.body)) == 500

            updates_resp = await route_map[("GET", "/updates")](queries="4")
            assert updates_resp.status == 200
            updates_data = decode_json(updates_resp.body)
            assert isinstance(updates_data, list)
            assert len(updates_data) == 4

            cached_resp = await route_map[("GET", "/cached-queries")](count="6")
            assert cached_resp.status == 200
            cached_data = decode_json(cached_resp.body)
            assert isinstance(cached_data, list)
            assert len(cached_data) == 6

            missing_resp = await route_map[("GET", "/object/{key}")](key="unknown-obj")
            assert missing_resp.status == 404
            assert missing_resp.body == b"Not Found"

            blob = b"hello-object-store"
            put_resp = await route_map[("PUT", "/object/{key}")](key="obj-1", body=blob)
            assert put_resp.status == 200
            put_data = decode_json(put_resp.body)
            assert put_data == {"key": "obj-1", "size": len(blob)}

            get_resp = await route_map[("GET", "/object/{key}")](key="obj-1")
            assert get_resp.status == 200
            assert get_resp.body == blob
        finally:
            backend.close()

    asyncio.run(_run())
