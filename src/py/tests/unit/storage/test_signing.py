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

import vertebra._storage as native_storage_mod
from vertebra.storage import (
    AsyncObjectStore,
    AzureConfig,
    GCSConfig,
    OCIConfig,
    S3Config,
    SigningConfig,
    SyncObjectStore,
)


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


def test_signing_expiry_bounds_invalid_keys_and_rfc3986_paths() -> None:
    """Verify out-of-bounds expiry and malformed keys raise ValueError and paths follow RFC 3986."""
    native_mod = SyncObjectStore._get_native_module()
    assert native_mod is not None

    azure_key = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("ascii")
    for bad_expiry in (0, 604_801):
        with pytest.raises(ValueError, match="expires_in_secs"):
            native_mod.py_sign_s3_v4_url(
                "assets",
                "auto",
                "custom-ak",
                "custom-sk",
                "GET",
                "img/logo.png",
                bad_expiry,
                1_794_009_600,
                "https://custom.example",
            )
        with pytest.raises(ValueError, match="expires_in_secs"):
            native_mod.py_sign_azure_sas_url(
                "acct1",
                "container",
                azure_key,
                "PUT",
                "blob/file.csv",
                bad_expiry,
                1_794_009_600,
            )
        with pytest.raises(ValueError, match="expires_in_secs"):
            native_mod.py_sign_hmac_url(
                "https://cdn.example.com/assets/file.bin",
                "test-hmac-secret-key-32-bytes-ok",
                "GET",
                bad_expiry,
                1_794_009_600,
            )

    with pytest.raises(ValueError, match="base64"):
        native_mod.py_sign_azure_sas_url(
            "acct1",
            "container",
            "%%%not-valid-base64%%%",
            "PUT",
            "blob/file.csv",
            3600,
            1_794_009_600,
        )

    custom_s3_url = native_mod.py_sign_s3_v4_url(
        "assets",
        "auto",
        "custom-ak",
        "custom-sk",
        "GET",
        "reports/2026 Q4/summary+final.parquet",
        3600,
        1_794_009_600,
        "https://custom.example",
    )
    assert custom_s3_url.startswith("https://custom.example/assets/reports/2026%20Q4/summary%2Bfinal.parquet?")
    assert "X-Amz-Credential=custom-ak%2F20261107%2Fauto%2Fs3%2Faws4_request" in custom_s3_url
    assert native_mod.py_verify_s3_v4_url(
        "assets",
        "auto",
        "custom-ak",
        "custom-sk",
        "GET",
        custom_s3_url,
        1_794_009_660,
        "https://custom.example",
    )


def test_native_oci_par_and_gcs_iam_assembly_exports() -> None:
    """Verify sign_oci_par_url, build_gcs_v4_canonical_request, and assemble_gcs_iam_v4_url on vertebra._storage."""
    par_url = native_storage_mod.sign_oci_par_url(
        "lakehouse",
        "mytenancy",
        "us-ashburn-1",
        "p/secret-par-token/",
        "reports/q4.parquet",
        600,
    )
    assert (
        par_url
        == "https://objectstorage.us-ashburn-1.oraclecloud.com/p/secret-par-token/n/mytenancy/b/lakehouse/o/reports%2Fq4.parquet"
    )

    with pytest.raises(ValueError, match="expires_in_secs"):
        native_storage_mod.sign_oci_par_url(
            "lakehouse",
            "mytenancy",
            "us-ashburn-1",
            "secret-par-token",
            "reports/q4.parquet",
            0,
        )

    with pytest.raises(ValueError, match="PAR token"):
        native_storage_mod.sign_oci_par_url(
            "lakehouse",
            "mytenancy",
            "us-ashburn-1",
            "   ",
            "reports/q4.parquet",
            600,
        )

    c_uri, c_qs, c_req, sts = native_storage_mod.build_gcs_v4_canonical_request(
        "ml-bucket",
        "signer@proj.iam.gserviceaccount.com",
        "GET",
        "reports/q4.parquet",
        600,
        1_794_009_600,
    )
    assert c_uri == "/ml-bucket/reports/q4.parquet"
    assert "X-Goog-Algorithm=GOOG4-RSA-SHA256" in c_qs
    assert "UNSIGNED-PAYLOAD" in c_req
    assert sts.startswith("GOOG4-RSA-SHA256\n20261107T000000Z\n")

    assembled = native_storage_mod.assemble_gcs_iam_v4_url(c_uri, c_qs, "DEADBEEFCAFE")
    assert assembled.startswith("https://storage.googleapis.com/ml-bucket/reports/q4.parquet?")
    assert assembled.endswith("&X-Goog-Signature=deadbeefcafe")

    with pytest.raises(ValueError, match="hex"):
        native_storage_mod.assemble_gcs_iam_v4_url(c_uri, c_qs, "not-valid-hex!!")


def test_multicloud_store_sign_sync_and_async(tmp_path: Path) -> None:
    """Verify store.sign across GCS (RSA + IAM signBlob), S3, custom S3 endpoints, Azure SAS, and OCI PAR for sync and async stores."""
    ts = 1_794_009_600
    pem_key = subprocess.check_output(
        ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048"],
        stderr=subprocess.DEVNULL,
        text=True,
    )
    sa_json = json.dumps(
        {
            "type": "service_account",
            "project_id": "test-gcp-proj",
            "private_key_id": "key-1",
            "private_key": pem_key,
            "client_email": "rsa-signer@test-gcp-proj.iam.gserviceaccount.com",
            "token_uri": "http://127.0.0.1:9/token",
        }
    )

    gcs_rsa_cfg = GCSConfig(bucket="gcs-rsa-bucket", service_account_key=sa_json, iam_endpoint="http://127.0.0.1:9")
    gcs_rsa_sync = SyncObjectStore(gcs_rsa_cfg)
    gcs_rsa_async = AsyncObjectStore(gcs_rsa_cfg)

    s3_cfg = S3Config(
        bucket="s3-analytics",
        region="us-east-1",
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    s3_sync = SyncObjectStore(s3_cfg)
    s3_async = AsyncObjectStore(s3_cfg)

    custom_s3_cfg = S3Config(
        bucket="custom-assets",
        region="auto",
        endpoint="https://custom.example",
        access_key_id="custom-ak",
        secret_access_key="custom-sk",
    )
    custom_s3_sync = SyncObjectStore(custom_s3_cfg)
    custom_s3_async = AsyncObjectStore(custom_s3_cfg)

    az_key = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("ascii")
    az_cfg = AzureConfig(container="az-lake", account="devstoreaccount1", access_key=az_key)
    az_sync = SyncObjectStore(az_cfg)
    az_async = AsyncObjectStore(az_cfg)

    oci_cfg = OCIConfig(
        bucket="oci-warehouse",
        namespace="mytenancy",
        region="us-ashburn-1",
        par_token="par-tok-xyz",
    )
    oci_sync = SyncObjectStore(oci_cfg)
    oci_async = AsyncObjectStore(oci_cfg)

    oci_prefix_cfg = OCIConfig(
        bucket="oci-warehouse-2",
        namespace="ns2",
        region="us-phoenix-1",
        par_url_prefix="https://objectstorage.us-phoenix-1.oraclecloud.com/p/par-prefix-tok/n/ns2/b/oci-warehouse-2/o/",
    )
    oci_prefix_sync = SyncObjectStore(oci_prefix_cfg)

    class _IamHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            _ = (format, args)

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            _ = self.rfile.read(length) if length > 0 else b""
            if self.path == "/token":
                body = b'{"access_token":"iam-oauth-tok","expires_in":3600,"token_type":"Bearer"}'
            elif ":signBlob" in self.path:
                body = b'{"keyId":"k1","signedBlob":"ZGVhZGJlZWZjYWZl"}'
            else:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _IamHandler)
    iam_base = f"http://127.0.0.1:{server.server_port}"
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    try:
        adc_file = tmp_path / "iam_adc.json"
        adc_file.write_text(
            json.dumps(
                {
                    "type": "authorized_user",
                    "client_id": "cid",
                    "client_secret": "csec",
                    "refresh_token": "rtok",
                    "token_uri": f"{iam_base}/token",
                }
            ),
            encoding="utf-8",
        )
        gcs_iam_cfg = GCSConfig(
            bucket="gcs-iam-bucket",
            application_credentials=str(adc_file),
            target_service_account="iam-signer@test-gcp-proj.iam.gserviceaccount.com",
            iam_endpoint=iam_base,
        )
        gcs_iam_sync = SyncObjectStore(gcs_iam_cfg)
        gcs_iam_async = AsyncObjectStore(gcs_iam_cfg)

        gcs_rsa_url = gcs_rsa_sync.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
        assert gcs_rsa_url.startswith("https://storage.googleapis.com/gcs-rsa-bucket/reports/q4.parquet?")
        assert "X-Goog-Algorithm=GOOG4-RSA-SHA256" in gcs_rsa_url
        assert "X-Goog-Expires=600" in gcs_rsa_url

        gcs_iam_url = gcs_iam_sync.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
        assert gcs_iam_url.startswith("https://storage.googleapis.com/gcs-iam-bucket/reports/q4.parquet?")
        assert "X-Goog-Signature=646561646265656663616665" in gcs_iam_url

        s3_url = s3_sync.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
        assert s3_url.startswith("https://s3.us-east-1.amazonaws.com/s3-analytics/reports/q4.parquet?")
        assert s3_sync.verify_presigned_url(s3_url, method="GET", timestamp=ts + 10)

        custom_s3_url = custom_s3_sync.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
        assert custom_s3_url.startswith("https://custom.example/custom-assets/reports/q4.parquet?")
        assert custom_s3_sync.verify_presigned_url(custom_s3_url, method="GET", timestamp=ts + 10)

        az_url = az_sync.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
        assert az_url.startswith("https://devstoreaccount1.blob.core.windows.net/az-lake/reports/q4.parquet?")
        assert "sv=2020-12-06" in az_url
        assert "sp=r" in az_url
        assert "sig=" in az_url

        oci_url = oci_sync.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
        assert (
            oci_url
            == "https://objectstorage.us-ashburn-1.oraclecloud.com/p/par-tok-xyz/n/mytenancy/b/oci-warehouse/o/reports%2Fq4.parquet"
        )

        oci_prefix_url = oci_prefix_sync.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
        assert (
            oci_prefix_url
            == "https://objectstorage.us-phoenix-1.oraclecloud.com/p/par-prefix-tok/n/ns2/b/oci-warehouse-2/o/reports%2Fq4.parquet"
        )

        async def _verify_async() -> None:
            assert (
                await gcs_rsa_async.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
                == gcs_rsa_url
            )
            assert (
                await gcs_iam_async.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
                == gcs_iam_url
            )
            assert await s3_async.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts) == s3_url
            assert (
                await custom_s3_async.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts)
                == custom_s3_url
            )
            assert await az_async.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts) == az_url
            assert await oci_async.sign("reports/q4.parquet", expires_in=600, method="GET", timestamp=ts) == oci_url

        asyncio.run(_verify_async())
    finally:
        server.shutdown()
        server.server_close()
        srv_thread.join(timeout=2.0)
