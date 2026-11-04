"""Unit tests for default UTC timestamp handling across storage URL signing functions."""

from __future__ import annotations

import asyncio
import base64
import calendar
import json
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from vertebra.storage import AsyncObjectStore, SigningConfig, SyncObjectStore


def test_signing_functions_default_to_current_utc_timestamp() -> None:
    """Verify signing functions default to current UTC timestamp rather than a static epoch."""
    before_ts = int(time.time())
    native_mod = SyncObjectStore._get_native_module()

    if native_mod is not None:
        hmac_url = native_mod.py_sign_hmac_url(
            "https://cdn.example.com/assets/file.bin",
            "test-hmac-secret-key-32-bytes-ok",
            "GET",
            600,
        )
        hmac_params = parse_qs(urlparse(hmac_url).query)
        hmac_expires = int(hmac_params["X-Vtb-Expires"][0])
        assert hmac_expires >= before_ts + 600
        assert native_mod.py_verify_hmac_url(
            hmac_url,
            "test-hmac-secret-key-32-bytes-ok",
            "GET",
            before_ts + 10,
        )

        s3_url = native_mod.py_sign_s3_v4_url(
            "artifacts",
            "us-east-1",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "GET",
            "models/weights.bin",
            900,
        )
        s3_params = parse_qs(urlparse(s3_url).query)
        amz_date = s3_params["X-Amz-Date"][0]
        s3_signed_ts = int(calendar.timegm(time.strptime(amz_date, "%Y%m%dT%H%M%SZ")))
        assert s3_signed_ts >= before_ts - 2

        azure_key = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("ascii")
        sas_url = native_mod.py_sign_azure_sas_url(
            "devstoreaccount1",
            "datasets",
            azure_key,
            "GET",
            "table.parquet",
            1200,
        )
        sas_params = parse_qs(urlparse(sas_url).query)
        sas_expiry_ts = int(calendar.timegm(time.strptime(sas_params["se"][0], "%Y-%m-%dT%H:%M:%SZ")))
        assert sas_expiry_ts >= before_ts + 1198

    store = SyncObjectStore(
        "memory://",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret="test-hmac-secret-key-32-bytes-ok",
            endpoint="https://cdn.example.com",
        ),
    )
    presigned = store.presign_get("assets/file.bin", expires_in_secs=600)
    presigned_params = parse_qs(urlparse(presigned).query)
    assert int(presigned_params["X-Vtb-Expires"][0]) >= before_ts + 600
    assert store.verify_presigned_url(presigned, method="GET")


def test_gcs_from_gcs_ambient_service_account_adc_presigns_zero_rtt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SyncObjectStore.from_gcs and AsyncObjectStore.from_gcs auto-extract ambient service_account ADC keys."""
    pem_key = subprocess.check_output(
        ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048"],
        stderr=subprocess.DEVNULL,
        text=True,
    )
    sa_file = tmp_path / "ambient_sa.json"
    sa_file.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": "test-gcp-proj",
                "private_key_id": "key-1",
                "private_key": pem_key,
                "client_email": "ambient-sa@test-gcp-proj.iam.gserviceaccount.com",
                "token_uri": "http://127.0.0.1:9/token",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_file))
    store = SyncObjectStore.from_gcs("test-bucket", iam_endpoint="http://127.0.0.1:9")
    assert store.supports_signing is True

    sync_url = store.presign_get("dir/file.parquet", expires_in_secs=600, timestamp=1_700_000_000)
    assert "X-Goog-Algorithm=GOOG4-RSA-SHA256" in sync_url
    assert "ambient-sa%40test-gcp-proj.iam.gserviceaccount.com" in sync_url

    async_store = AsyncObjectStore.from_gcs("test-bucket", iam_endpoint="http://127.0.0.1:9")

    async def _run_async() -> str:
        return await async_store.presign_get("dir/file.parquet", expires_in_secs=600, timestamp=1_700_000_000)

    async_url = asyncio.run(_run_async())
    assert async_url == sync_url


def test_gcs_keyless_presign_reuses_object_store_token_cache(
    tmp_path: Path,
) -> None:
    """Verify keyless SyncObjectStore and AsyncObjectStore reuse token cache and retry IAM signBlob on HTTP 401."""
    oauth_calls = 0
    sign_blob_calls = 0
    seen_auth_headers: list[str] = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            _ = (format, args)

        def do_POST(self) -> None:
            nonlocal oauth_calls, sign_blob_calls
            length = int(self.headers.get("Content-Length", "0"))
            _ = self.rfile.read(length) if length > 0 else b""
            with lock:
                if self.path == "/token":
                    oauth_calls += 1
                    body = json.dumps(
                        {
                            "access_token": f"test-access-token-{oauth_calls}",
                            "expires_in": 3600,
                            "token_type": "Bearer",
                        }
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if ":signBlob" in self.path:
                    sign_blob_calls += 1
                    auth_hdr = self.headers.get("Authorization", "")
                    seen_auth_headers.append(auth_hdr)
                    if sign_blob_calls == 2:
                        err = b'{"error":{"code":401,"message":"UNAUTHENTICATED"}}'
                        self.send_response(401)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(err)))
                        self.end_headers()
                        self.wfile.write(err)
                        return
                    sig_b64 = base64.b64encode(f"sig-{sign_blob_calls}".encode()).decode("ascii")
                    body = json.dumps({"keyId": "k1", "signedBlob": sig_b64}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

            self.send_response(404)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    base_url = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        adc_file = tmp_path / "adc.json"
        adc_file.write_text(
            json.dumps(
                {
                    "type": "authorized_user",
                    "client_id": "cid",
                    "client_secret": "csec",
                    "refresh_token": "rtok",
                    "token_uri": f"{base_url}/token",
                }
            ),
            encoding="utf-8",
        )
        store = SyncObjectStore.from_gcs(
            "test-bucket",
            application_credentials=str(adc_file),
            target_service_account="signer@proj.iam.gserviceaccount.com",
            iam_endpoint=base_url,
        )
        url1 = store.presign_get("dir/a.parquet", expires_in_secs=600, timestamp=1_700_000_000)
        assert "X-Goog-Signature=" in url1
        assert oauth_calls == 1
        assert sign_blob_calls == 1

        url2 = store.presign_get("dir/b.parquet", expires_in_secs=600, timestamp=1_700_000_000)
        assert "X-Goog-Signature=" in url2
        assert oauth_calls == 2
        assert sign_blob_calls == 3

        url3 = store.presign_get("dir/c.parquet", expires_in_secs=600, timestamp=1_700_000_000)
        assert "X-Goog-Signature=" in url3
        assert oauth_calls == 2
        assert sign_blob_calls == 4
        assert seen_auth_headers == [
            "Bearer test-access-token-1",
            "Bearer test-access-token-1",
            "Bearer test-access-token-2",
            "Bearer test-access-token-2",
        ]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
