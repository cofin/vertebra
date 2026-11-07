"""Unit tests for active GCP ADC credential caching, proactive refresh, and DuckDB secret rotation."""

from __future__ import annotations

import ast
import asyncio
import base64
import json
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import pytest

import vertebra._storage as native_storage_mod
import vertebra._vertebra as native_core_mod
import vertebra.embeddings._google as google_embeddings_mod
import vertebra.storage._store as storage_store_mod
from vertebra._vertebra import (
    AsyncGcpTokenCache as CoreAsyncGcpTokenCache,
)
from vertebra._vertebra import (
    GcpTokenSnapshot as CoreGcpTokenSnapshot,
)
from vertebra._vertebra import (
    SyncGcpTokenCache as CoreSyncGcpTokenCache,
)
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.embeddings import VertexEmbedder
from vertebra.storage import (
    AsyncGcpTokenCache,
    AsyncObjectStore,
    GcpTokenSnapshot,
    GCSConfig,
    SigningConfig,
    SyncGcpTokenCache,
    SyncObjectStore,
)


class _MockTokenServer:
    """Local HTTP server simulating OAuth2 token, Metadata Server, IAM, and GCS endpoints."""

    def __init__(self) -> None:
        self.oauth_calls = 0
        self.metadata_calls = 0
        self.iam_calls = 0
        self.gcs_calls = 0
        self.last_headers: dict[str, str] = {}
        self.last_post_body: bytes = b""
        self.expires_in: int = 3600
        self.expire_time_iso: str = "2099-01-01T00:00:00Z"
        self._lock = threading.Lock()

        parent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_GET(self) -> None:
                with parent._lock:
                    parent.last_headers = {k.lower(): v for k, v in self.headers.items()}
                    if self.path.startswith("/computeMetadata/v1/instance/service-accounts/"):
                        parent.metadata_calls += 1
                        idx = parent.metadata_calls
                        payload = {
                            "access_token": f"mock-oauth-token-meta-{idx}",
                            "expires_in": parent.expires_in,
                            "token_type": "Bearer",
                        }
                        raw = json.dumps(payload).encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return

                    if (
                        self.path.startswith("/storage/v1/b/")
                        or self.path.startswith("/download/storage/v1/b/")
                        or "model.bin" in self.path
                        or "model%2Ebin" in self.path
                    ):
                        parent.gcs_calls += 1
                        raw = b"gcs-object-payload"
                        self.send_response(200)
                        self.send_header("Content-Type", "application/octet-stream")
                        self.send_header("Content-Length", str(len(raw)))
                        self.send_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                        self.send_header("ETag", '"etag-1"')
                        self.end_headers()
                        self.wfile.write(raw)
                        return

                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length) if length > 0 else b""
                with parent._lock:
                    parent.last_headers = {k.lower(): v for k, v in self.headers.items()}
                    parent.last_post_body = body
                    if self.path == "/token":
                        parent.oauth_calls += 1
                        idx = parent.oauth_calls
                        payload = {
                            "access_token": f"mock-oauth-token-user-{idx}",
                            "expires_in": parent.expires_in,
                            "token_type": "Bearer",
                        }
                        raw = json.dumps(payload).encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return

                    if ":generateAccessToken" in self.path:
                        parent.iam_calls += 1
                        idx = parent.iam_calls
                        payload = {
                            "accessToken": f"mock-oauth-token-impersonated-{idx}",
                            "expireTime": parent.expire_time_iso,
                        }
                        raw = json.dumps(payload).encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return

                    if ":signBlob" in self.path:
                        parent.iam_calls += 1
                        raw = b'{"keyId":"k1","signedBlob":"ZGVhZGJlZWZjYWZl"}'
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return

                self.send_response(404)
                self.end_headers()

        class _HighConcurrencyHTTPServer(ThreadingHTTPServer):
            request_queue_size = 256

        self._server = _HighConcurrencyHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_port
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2.0)


@pytest.fixture
def token_server() -> Any:
    server = _MockTokenServer()
    try:
        yield server
    finally:
        server.close()


def test_gcp_token_cache_authorized_user_and_proactive_skew(
    token_server: _MockTokenServer,
    tmp_path: Path,
) -> None:
    """Verify GcpTokenCache caches tokens, sends quota project header, and refreshes within 300s skew."""
    adc_path = tmp_path / "application_default_credentials.json"
    adc_path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "quota_project_id": "test-quota-proj",
                "token_uri": f"{token_server.base_url}/token",
            }
        ),
        encoding="utf-8",
    )

    cfg = GCSConfig(
        bucket="test-bucket",
        application_credentials=str(adc_path),
        refresh_skew_seconds=300,
    )
    cache = cfg.sync_token_cache()

    snap1 = cache.get_token()
    assert isinstance(snap1, GcpTokenSnapshot)
    assert snap1.access_token == "mock-oauth-token-user-1"
    assert snap1.generation == 1
    assert snap1.quota_project_id == "test-quota-proj"
    assert token_server.oauth_calls == 1
    assert token_server.last_headers.get("x-goog-user-project") == "test-quota-proj"

    snap1_again = cache.get_token()
    assert snap1_again.access_token == "mock-oauth-token-user-1"
    assert snap1_again.generation == 1
    assert token_server.oauth_calls == 1

    near_expiry_ms = snap1.expires_at_unix_ms - 240_000
    assert cache.needs_refresh(now_unix_ms=near_expiry_ms) is True

    token_server.expires_in = 200
    snap2 = cache.get_token(force_refresh=True)
    assert snap2.access_token == "mock-oauth-token-user-2"
    assert snap2.generation == 2
    assert token_server.oauth_calls == 2

    snap3 = cache.get_token()
    assert snap3.access_token == "mock-oauth-token-user-3"
    assert snap3.generation == 3
    assert token_server.oauth_calls == 3


def test_gcp_token_cache_single_flight_deduplication_and_invalidation(
    token_server: _MockTokenServer,
    tmp_path: Path,
) -> None:
    """Verify concurrent callers share a single refresh flight and stale invalidations are ignored."""
    adc_path = tmp_path / "adc.json"
    adc_path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "cid",
                "client_secret": "csec",
                "refresh_token": "rtok",
                "token_uri": f"{token_server.base_url}/token",
            }
        ),
        encoding="utf-8",
    )

    cfg = GCSConfig(bucket="test-bucket", application_credentials=str(adc_path))
    cache = cfg.sync_token_cache()

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: cache.get_token(), range(16)))

    assert all(r.access_token == "mock-oauth-token-user-1" for r in results)
    assert all(r.generation == 1 for r in results)
    assert token_server.oauth_calls == 1

    cache.invalidate(1)
    cache.invalidate(1)
    snap2 = cache.get_token()
    assert snap2.access_token == "mock-oauth-token-user-2"
    assert snap2.generation == 2
    assert token_server.oauth_calls == 2

    cache.invalidate(1)
    snap2_still = cache.get_token()
    assert snap2_still.generation == 2
    assert token_server.oauth_calls == 2


def test_gcp_token_cache_impersonation_and_metadata_server(
    token_server: _MockTokenServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify metadata server base token + target_service_account IAM generateAccessToken refresh."""
    monkeypatch.setenv("GCE_METADATA_HOST", f"127.0.0.1:{token_server.port}")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    cfg = GCSConfig(
        bucket="test-bucket",
        target_service_account="runner@test-proj.iam.gserviceaccount.com",
        iam_endpoint=token_server.base_url,
    )
    cache = cfg.sync_token_cache(allow_ambient_adc=False)

    snap = cache.get_token()
    assert snap.access_token == "mock-oauth-token-impersonated-1"
    assert snap.generation == 1
    assert token_server.metadata_calls == 1
    assert token_server.iam_calls == 1


def test_rust_object_store_refreshing_gcp_credential_provider(
    token_server: _MockTokenServer,
    tmp_path: Path,
) -> None:
    """Verify ObjectStore('gs://...') uses RefreshingGcpCredentialProvider in Rust when token expires."""
    adc_path = tmp_path / "adc.json"
    token_server.expires_in = 120
    adc_path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "cid",
                "client_secret": "csec",
                "refresh_token": "rtok",
                "token_uri": f"{token_server.base_url}/token",
            }
        ),
        encoding="utf-8",
    )

    cfg = GCSConfig(
        bucket="test-bucket",
        application_credentials=str(adc_path),
        endpoint=token_server.base_url,
        allow_http=True,
        refresh_skew_seconds=300,
    )
    store = SyncObjectStore(cfg)

    data1 = store.get("model.bin")
    assert data1 == b"gcs-object-payload"
    assert token_server.oauth_calls >= 1
    calls_after_first = token_server.oauth_calls
    assert token_server.last_headers.get("authorization") == f"Bearer mock-oauth-token-user-{calls_after_first}"

    data2 = store.get("model.bin")
    assert data2 == b"gcs-object-payload"
    assert token_server.oauth_calls == calls_after_first + 1
    assert token_server.last_headers.get("authorization") == f"Bearer mock-oauth-token-user-{calls_after_first + 1}"


def test_duckdb_multi_session_token_generation_secret_rotation(
    token_server: _MockTokenServer,
    tmp_path: Path,
) -> None:
    """Verify multiple DuckDB sessions track applied_token_generation and all rotate secrets on refresh."""
    adc_path = tmp_path / "adc.json"
    adc_path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "cid",
                "client_secret": "csec",
                "refresh_token": "rtok",
                "token_uri": f"{token_server.base_url}/token",
            }
        ),
        encoding="utf-8",
    )
    gcs_cfg = GCSConfig(bucket="test-bucket", application_credentials=str(adc_path))
    cache = gcs_cfg.sync_token_cache()

    db_cfg = DuckDBConfig(gcs_token_cache=cache)
    driver_a = SyncDuckDBDriver(db_cfg)
    driver_b = SyncDuckDBDriver(db_cfg)
    try:
        rows_a1 = driver_a.select("SELECT 1 AS v")
        rows_b1 = driver_b.select("SELECT 1 AS v")
        assert rows_a1 == [{"v": 1}]
        assert rows_b1 == [{"v": 1}]
        assert driver_a.applied_token_generation == 1
        assert driver_b.applied_token_generation == 1
        assert "mock-oauth-token-user-1" in driver_a.last_gcs_secret_sql
        assert "mock-oauth-token-user-1" in driver_b.last_gcs_secret_sql

        cache.invalidate(1)

        driver_a.select("SELECT 2 AS v")
        assert driver_a.applied_token_generation == 2
        assert "mock-oauth-token-user-2" in driver_a.last_gcs_secret_sql

        driver_b.select("SELECT 2 AS v")
        assert driver_b.applied_token_generation == 2
        assert "mock-oauth-token-user-2" in driver_b.last_gcs_secret_sql
    finally:
        driver_a.close()
        driver_b.close()


def test_async_duckdb_driver_token_generation_rotation(
    token_server: _MockTokenServer,
    tmp_path: Path,
) -> None:
    """Verify AsyncDuckDBDriver rotates GCS secrets when SyncGcpTokenCache generation advances."""
    adc_path = tmp_path / "adc.json"
    adc_path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "cid",
                "client_secret": "csec",
                "refresh_token": "rtok",
                "token_uri": f"{token_server.base_url}/token",
            }
        ),
        encoding="utf-8",
    )
    cache = GCSConfig(bucket="b", application_credentials=str(adc_path)).sync_token_cache()
    async_cache = GCSConfig(bucket="b", application_credentials=str(adc_path)).async_token_cache()

    async def _run() -> None:
        drv = AsyncDuckDBDriver(DuckDBConfig(gcs_token_cache=cache))
        try:
            res = await drv.select("SELECT 1 AS ok")
            assert res == [{"ok": 1}]
            assert drv.applied_token_generation == 1
            cache.get_token(force_refresh=True)
            await drv.select("SELECT 1 AS ok")
            assert drv.applied_token_generation == 2
        finally:
            await drv.close()

        async_drv = AsyncDuckDBDriver(DuckDBConfig(gcs_token_cache=async_cache))
        try:
            res_async = await async_drv.select("SELECT 1 AS ok")
            assert res_async == [{"ok": 1}]
            assert async_drv.applied_token_generation == 1
            await async_cache.get_token(force_refresh=True)
            await async_drv.select("SELECT 1 AS ok")
            assert async_drv.applied_token_generation == 2
        finally:
            await async_drv.close()

    asyncio.run(_run())


def test_high_concurrency_async_storage_and_token_refresh_no_gil_blocking(
    token_server: _MockTokenServer,
    tmp_path: Path,
) -> None:
    """Verify 200+ concurrent async storage & token refresh coroutines do not block the GIL or event loop."""
    adc_path = tmp_path / "adc.json"
    adc_path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "cid",
                "client_secret": "csec",
                "refresh_token": "rtok",
                "token_uri": f"{token_server.base_url}/token",
            }
        ),
        encoding="utf-8",
    )
    cfg = GCSConfig(bucket="test-bucket", application_credentials=str(adc_path))
    cache = cfg.async_token_cache()
    local_store = AsyncObjectStore(
        f"file://{tmp_path / 'bucket'}",
        signing=SigningConfig(
            provider="gcs",
            bucket="test-bucket",
            service_account_email="signer@test-proj.iam.gserviceaccount.com",
            iam_endpoint=token_server.base_url,
            access_token="mock-oauth-token-signer",
        ),
    )

    async def _run() -> None:
        max_loop_lag_s = 0.0
        stop_heartbeat = False

        async def _heartbeat() -> None:
            nonlocal max_loop_lag_s
            loop = asyncio.get_running_loop()
            interval = 0.005
            expected = loop.time() + interval
            while not stop_heartbeat:
                await asyncio.sleep(interval)
                now = loop.time()
                lag = max(0.0, now - expected)
                if lag > max_loop_lag_s:
                    max_loop_lag_s = lag
                expected = now + interval

        hb_task = asyncio.create_task(_heartbeat())
        try:
            payload = b"x" * 65536

            async def _worker(idx: int) -> None:
                key = f"shard/obj-{idx}.bin"
                snap = await cache.get_token(force_refresh=(idx == 0))
                assert snap.access_token.startswith("mock-oauth-token-user-")
                meta = await local_store.put(key, payload)
                assert meta.size == len(payload)
                raw = await local_store.get(key)
                assert len(raw) == len(payload)
                view = await local_store.get_view(key)
                assert len(memoryview(view)) == len(payload)
                presigned = await local_store.presign_get(key, expires_in_secs=300)
                assert key in presigned

            await asyncio.gather(*(_worker(i) for i in range(200)))
        finally:
            stop_heartbeat = True
            await hb_task

        assert max_loop_lag_s < 0.25

    asyncio.run(_run())


def test_storage_completion_ring_eventfd_coalesced_wakeup(tmp_path: Path) -> None:
    """Verify AsyncObjectStore async operations use per-loop Notifier (eventfd/kqueue) + completion ring."""
    store = AsyncObjectStore(f"file://{tmp_path / 'ring_bucket'}")
    payload = b"completion-ring-payload" * 64

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        orig_csts = loop.call_soon_threadsafe
        csts_calls = 0

        def tracked_csts(callback: Any, *args: Any, **kwargs: Any) -> Any:
            nonlocal csts_calls
            csts_calls += 1
            return orig_csts(callback, *args, **kwargs)

        csts_attr = "call_soon_threadsafe"
        setattr(loop, csts_attr, tracked_csts)
        try:
            metas = await asyncio.gather(*(store.put(f"k-{i}.bin", payload) for i in range(64)))
            assert len(metas) == 64
            assert all(m.size == len(payload) for m in metas)

            blobs = await asyncio.gather(*(store.get(f"k-{i}.bin") for i in range(64)))
            assert len(blobs) == 64
            assert all(b == payload for b in blobs)
        finally:
            setattr(loop, csts_attr, orig_csts)

        assert csts_calls == 0

    asyncio.run(_run())


def test_vertebra_core_gcp_token_cache_service_account_and_audience_jwt(
    token_server: _MockTokenServer,
) -> None:
    """Verify vertebra._vertebra.SyncGcpTokenCache and AsyncGcpTokenCache exchange Service Account RS256 assertions."""
    pem_key = subprocess.check_output(
        ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048"],
        stderr=subprocess.DEVNULL,
        text=True,
    )
    sa_json = json.dumps(
        {
            "type": "service_account",
            "project_id": "test-gcp-proj",
            "private_key_id": "key-id-99",
            "private_key": pem_key,
            "client_email": "bq-worker@test-gcp-proj.iam.gserviceaccount.com",
            "token_uri": f"{token_server.base_url}/token",
        }
    )
    sync_cache = CoreSyncGcpTokenCache(
        {
            "service_account_key": sa_json,
            "scopes": "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/spanner.data",
            "quota_project_id": "custom-quota-proj",
        }
    )
    assert sync_cache.has_service_account_key is True
    assert sync_cache.service_account_email == "bq-worker@test-gcp-proj.iam.gserviceaccount.com"

    snap1 = sync_cache.get_token()
    assert snap1.access_token == "mock-oauth-token-user-1"
    assert snap1.generation == 1
    assert snap1.quota_project_id == "custom-quota-proj"
    assert token_server.oauth_calls == 1

    form = parse_qs(token_server.last_post_body.decode("utf-8"))
    assert form["grant_type"] == ["urn:ietf:params:oauth:grant-type:jwt-bearer"]
    assertion_parts = form["assertion"][0].split(".")
    assert len(assertion_parts) == 3

    aud_jwt = sync_cache.sign_jwt_for_audience("https://bigquery.googleapis.com/")
    jwt_parts = aud_jwt.split(".")
    assert len(jwt_parts) == 3
    claims_padded = jwt_parts[1] + "=" * ((4 - len(jwt_parts[1]) % 4) % 4)
    claims = json.loads(base64.urlsafe_b64decode(claims_padded))
    assert claims["iss"] == "bq-worker@test-gcp-proj.iam.gserviceaccount.com"
    assert claims["sub"] == "bq-worker@test-gcp-proj.iam.gserviceaccount.com"
    assert claims["aud"] == "https://bigquery.googleapis.com/"

    sync_cache.invalidate(1)
    snap2 = sync_cache.get_token()
    assert snap2.access_token == "mock-oauth-token-user-2"
    assert snap2.generation == 2
    assert token_server.oauth_calls == 2

    async_cache = CoreAsyncGcpTokenCache(
        {
            "service_account_key": sa_json,
            "scopes": "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/spanner.data",
            "quota_project_id": "custom-quota-proj",
        }
    )

    async def _run_async() -> None:
        snap3 = await async_cache.get_token()
        assert snap3.access_token == "mock-oauth-token-user-3"
        assert snap3.generation == 1

        async_cache.invalidate(1)
        snap4 = await async_cache.get_token()
        assert snap4.access_token == "mock-oauth-token-user-4"
        assert snap4.generation == 2

    asyncio.run(_run_async())
    assert token_server.oauth_calls == 4


def test_google_embedder_uses_vertebra_gcp_token_cache_without_storage_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vertebra.embeddings._google imports AsyncGcpTokenCache from vertebra._vertebra and handles TypeError stub."""
    source_path = Path(google_embeddings_mod.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported_modules = [
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    ]
    assert "vertebra.storage" not in imported_modules
    assert "vertebra._vertebra" in imported_modules

    dummy_stub = type("AsyncGcpTokenCache", (), {})
    monkeypatch.setattr(google_embeddings_mod, "AsyncGcpTokenCache", dummy_stub)
    embedder = VertexEmbedder("gemini-embedding-001", project="test-proj")
    resolved_cache = embedder._ensure_token_cache(async_mode=True)
    assert isinstance(resolved_cache, AsyncGcpTokenCache)


def test_gcp_token_cache_unified_on_vertebra_core_and_removed_from_storage_ext(
    token_server: _MockTokenServer,
    tmp_path: Path,
) -> None:
    """Verify vertebra.storage re-exports vertebra._vertebra token classes and removes duplicate _storage/urllib classes."""
    assert SyncGcpTokenCache is CoreSyncGcpTokenCache
    assert AsyncGcpTokenCache is CoreAsyncGcpTokenCache
    assert GcpTokenSnapshot is CoreGcpTokenSnapshot
    assert SyncGcpTokenCache is native_core_mod.SyncGcpTokenCache
    assert AsyncGcpTokenCache is native_core_mod.AsyncGcpTokenCache
    assert GcpTokenSnapshot is native_core_mod.GcpTokenSnapshot

    assert not hasattr(native_storage_mod, "SyncGcpTokenCache")
    assert not hasattr(native_storage_mod, "AsyncGcpTokenCache")
    assert not hasattr(native_storage_mod, "GcpTokenSnapshot")

    assert not hasattr(GCSConfig, "_fetch_adc_token_with_expiry")
    assert not hasattr(GCSConfig, "_exchange_authorized_user_token_with_expiry")
    assert not hasattr(GCSConfig, "_impersonate_service_account_token_with_expiry")
    assert not hasattr(GCSConfig, "_open_http_json")
    assert not hasattr(GCSConfig, "_fetch_metadata_server_token_with_expiry")

    store_source = Path(storage_store_mod.__file__).read_text(encoding="utf-8")
    tree = ast.parse(store_source)
    urllib_imports = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.alias) and node.name in {"urllib.request", "urllib.error"}
    ]
    assert urllib_imports == []

    adc_path = tmp_path / "unified_adc.json"
    adc_path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "cid",
                "client_secret": "csec",
                "refresh_token": "rtok",
                "token_uri": f"{token_server.base_url}/token",
            }
        ),
        encoding="utf-8",
    )
    cfg = GCSConfig(bucket="unified-bucket", application_credentials=str(adc_path))
    assert cfg.resolve_adc_token() == "mock-oauth-token-user-1"
    assert token_server.oauth_calls == 1
