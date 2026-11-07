"""Unit tests for multi-cloud StorageConfig providers, GCP ADC auth/signing, OCI/Azure/R2 signing, and SyncReadableFile."""

from __future__ import annotations

import base64
import subprocess
import sys
import threading
from collections.abc import Buffer, Callable
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

import vertebra.storage as storage
from vertebra.storage import (
    AsyncObjectStore,
    AzureConfig,
    GCSConfig,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
    SigningConfig,
    SyncObjectStore,
    SyncReadableFile,
)
from vertebra.utils.serializers import encode_json_bytes


def test_storage_provider_configs_to_url_and_options(tmp_path: Path) -> None:
    """All 8 provider configurations emit canonical URLs and option dictionaries."""
    local_cfg = LocalConfig(root=tmp_path)
    local_url, local_opts = local_cfg.to_url_and_options()
    assert local_url.startswith("file://")
    assert local_opts == {}

    s3_cfg = S3Config(
        bucket="analytics",
        region="us-west-2",
        endpoint="http://127.0.0.1:4566",
        access_key_id="ak",
        secret_access_key="sk",
        allow_http=True,
    )
    s3_url, s3_opts = s3_cfg.to_url_and_options()
    assert s3_url == "s3://analytics"
    assert s3_opts["region"] == "us-west-2"
    assert s3_opts["allow_http"] == "true"

    gcs_cfg = GCSConfig(
        bucket="ml-artifacts",
        service_account_path="/tmp/sa.json",
        application_credentials="/tmp/adc.json",
    )
    gcs_url, gcs_opts = gcs_cfg.to_url_and_options()
    assert gcs_url == "gs://ml-artifacts"
    assert gcs_opts["service_account_path"] == "/tmp/sa.json"
    assert gcs_opts["application_credentials_path"] == "/tmp/adc.json"

    az_cfg = AzureConfig(container="Lake", account="devstoreaccount1", use_emulator=True)
    az_url, az_opts = az_cfg.to_url_and_options()
    assert az_url == "az://Lake"
    assert az_opts["endpoint"] == "http://127.0.0.1:4577/devstoreaccount1"

    oci_cfg = OCIConfig(
        bucket="models",
        namespace="mytenancy",
        region="us-ashburn-1",
        tenancy_ocid="ocid1.tenancy.oc1..aaaa",
        user_ocid="ocid1.user.oc1..bbbb",
        fingerprint="12:34:56",
    )
    oci_url, oci_opts = oci_cfg.to_url_and_options()
    assert oci_url == "oci://models@mytenancy"
    assert oci_opts["namespace"] == "mytenancy"
    assert oci_opts["region"] == "us-ashburn-1"

    r2_cfg = R2Config(bucket="assets", account_id="acct123", jurisdiction="eu")
    r2_url, r2_opts = r2_cfg.to_url_and_options()
    assert r2_url == "r2://assets"
    assert r2_opts["endpoint"] == "https://acct123.eu.r2.cloudflarestorage.com"

    http_cfg = HTTPConfig(url="https://cdn.example.com/files", headers={"X-Token": "abc"})
    http_url, http_opts = http_cfg.to_url_and_options()
    assert http_url == "https://cdn.example.com/files"
    assert http_opts == {"X-Token": "abc"}

    mem_cfg = MemoryConfig(namespace="unit")
    mem_url, mem_opts = mem_cfg.to_url_and_options()
    assert mem_url == "memory://"
    assert mem_opts == {"namespace": "unit"}


def test_duplicated_storage_facades_and_trackers_removed() -> None:
    """Duplicated SyncStore, AsyncStore, UploadTracker, StagedUpload, and StorageRegistry are deleted."""
    assert not hasattr(storage, "SyncStore")
    assert not hasattr(storage, "AsyncStore")
    assert not hasattr(storage, "UploadTracker")
    assert not hasattr(storage, "StagedUpload")
    assert not hasattr(storage, "StorageRegistry")
    assert not hasattr(storage, "get_default_storage_registry")


def test_gcs_emulator_env_and_service_account_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """GCSConfig honors STORAGE_EMULATOR_HOST and extracts service account identity vs impersonation."""
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:4443")
    emu_cfg = GCSConfig(bucket="local-dev")
    url, opts = emu_cfg.to_url_and_options()
    assert url == "gs://local-dev"
    assert opts["endpoint"] == "http://127.0.0.1:4443"
    assert opts["allow_http"] == "true"
    assert opts["skip_signature"] == "true"

    sa_file = tmp_path / "sa.json"
    sa_file.write_bytes(
        bytes(
            encode_json_bytes(
                {
                    "type": "service_account",
                    "client_email": "worker@proj.iam.gserviceaccount.com",
                    "private_key": "fake-pem-material",
                }
            )
        )
    )
    direct_cfg = GCSConfig(bucket="prod-bucket", service_account_path=str(sa_file))
    email, pem = direct_cfg.extract_service_account_identity()
    assert email == "worker@proj.iam.gserviceaccount.com"
    assert pem == "fake-pem-material"

    impersonated_cfg = GCSConfig(
        bucket="prod-bucket",
        service_account_path=str(sa_file),
        target_service_account="impersonated@proj.iam.gserviceaccount.com",
    )
    imp_email, imp_pem = impersonated_cfg.extract_service_account_identity()
    assert imp_email == "impersonated@proj.iam.gserviceaccount.com"
    assert imp_pem is None


class _MockAdcAndIamHandler(BaseHTTPRequestHandler):
    """Mock HTTP handler serving OAuth2 token refresh and IAM generateAccessToken endpoints."""

    seen_paths: list[str] = []
    seen_headers: list[dict[str, str]] = []

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        _ = self.rfile.read(length)
        self.seen_paths.append(self.path)
        self.seen_headers.append({k.lower(): v for k, v in self.headers.items()})
        if self.path == "/token":
            body = b'{"access_token":"mock-source-adc-token","expires_in":3600}'
        elif self.path.endswith(":generateAccessToken"):
            body = b'{"accessToken":"mock-impersonated-sa-token","expireTime":"2030-01-01T00:00:00Z"}'
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        _ = (format, args)


def test_gcs_adc_authorized_user_and_impersonation_flow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GCSConfig exchanges authorized_user ADC credentials and impersonates target_service_account."""
    _MockAdcAndIamHandler.seen_paths = []
    _MockAdcAndIamHandler.seen_headers = []
    server = HTTPServer(("127.0.0.1", 0), _MockAdcAndIamHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        adc_path = tmp_path / "application_default_credentials.json"
        adc_path.write_bytes(
            bytes(
                encode_json_bytes(
                    {
                        "type": "authorized_user",
                        "client_id": "test-client-id",
                        "client_secret": "test-client-secret",
                        "refresh_token": "test-refresh-token",
                        "quota_project_id": "analytics-quota-proj",
                        "token_uri": f"http://127.0.0.1:{port}/token",
                    }
                )
            )
        )
        monkeypatch.setitem(sys.modules, "google.auth", None)
        cfg = GCSConfig(
            bucket="lakehouse-bucket",
            application_credentials=str(adc_path),
            target_service_account="lake-reader@analytics-proj.iam.gserviceaccount.com",
            iam_endpoint=f"http://127.0.0.1:{port}",
        )
        token = cfg.resolve_adc_token()
        assert token == "mock-impersonated-sa-token"
        assert _MockAdcAndIamHandler.seen_paths == [
            "/token",
            "/v1/projects/-/serviceAccounts/lake-reader@analytics-proj.iam.gserviceaccount.com:generateAccessToken",
        ]
        assert _MockAdcAndIamHandler.seen_headers[0].get("x-goog-user-project") == "analytics-quota-proj"
        assert _MockAdcAndIamHandler.seen_headers[1].get("authorization") == "Bearer mock-source-adc-token"

        _, opts = cfg.to_url_and_options()
        assert opts["access_token"] == "mock-impersonated-sa-token"
        assert opts["target_service_account"] == "lake-reader@analytics-proj.iam.gserviceaccount.com"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gcs_v4_local_rsa_presigning_without_network() -> None:
    """SyncObjectStore signs GCS V4 URLs locally via RSA-SHA256 when private_key_pem is provided."""
    pem_key = subprocess.check_output(
        ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048"],
        stderr=subprocess.DEVNULL,
        text=True,
    )
    store = SyncObjectStore(
        "gs://ml-artifacts",
        signing=SigningConfig(
            provider="gcs",
            bucket="ml-artifacts",
            service_account_email="local-signer@project.iam.gserviceaccount.com",
            private_key_pem=pem_key,
            iam_endpoint="http://127.0.0.1:9",
        ),
    )
    signed_get = store.presign_get("models/weights.safetensors", expires_in_secs=900, timestamp=1_700_000_000)
    signed_put = store.presign_put("models/weights.safetensors", expires_in_secs=900, timestamp=1_700_000_000)
    assert signed_get.startswith("https://storage.googleapis.com/ml-artifacts/models/weights.safetensors?")
    assert "X-Goog-Algorithm=GOOG4-RSA-SHA256" in signed_get
    assert "X-Goog-Credential=local-signer%40project.iam.gserviceaccount.com" in signed_get
    assert "X-Goog-Signature=" in signed_get
    assert signed_get != signed_put


def test_verify_presigned_url_gcs_raises_not_implemented() -> None:
    """Verifying a GCS presigned URL offline raises NotImplementedError."""
    store = SyncObjectStore(
        "gs://ml-artifacts",
        signing=SigningConfig(
            provider="gcs",
            bucket="ml-artifacts",
            service_account_email="signer@project.iam.gserviceaccount.com",
            iam_endpoint="http://127.0.0.1:8000",
            access_token="mock-token",
        ),
    )
    with pytest.raises(
        NotImplementedError,
        match="GCS presigned URL verification is not supported offline",
    ):
        store.verify_presigned_url(
            "https://storage.googleapis.com/ml-artifacts/test.bin?X-Goog-Algorithm=GOOG4-RSA-SHA256"
        )


def test_azure_sas_and_r2_uniform_chunking() -> None:
    """Azure SAS URL signing and Cloudflare R2 uniform multipart chunking are deterministic."""
    key = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("ascii")
    signed_url = SigningConfig.sign_azure_sas(
        "devstoreaccount1",
        key,
        "mycontainer",
        "dir/blob.parquet",
        method="PUT",
        expires_in_secs=1800,
        now_ts=1_700_000_000,
    )
    assert signed_url.startswith("https://devstoreaccount1.blob.core.windows.net/mycontainer/dir/blob.parquet?")
    assert "sv=2020-12-06" in signed_url
    assert "sr=b" in signed_url
    assert "sp=cw" in signed_url
    assert "sig=" in signed_url

    az_store = SyncObjectStore(
        "az://mycontainer",
        signing=SigningConfig(
            provider="azure",
            account="devstoreaccount1",
            container="mycontainer",
            azure_access_key=key,
        ),
    )
    presigned_az = az_store.presign_put("dir/blob.parquet", expires_in_secs=1800, timestamp=1_700_000_000)
    assert presigned_az == signed_url

    payload = b"x" * (12 * 1024 * 1024)
    chunks = R2Config.chunk_uniform_parts(payload, part_size=5 * 1024 * 1024)
    assert [len(c) for c in chunks] == [5 * 1024 * 1024, 5 * 1024 * 1024, 2 * 1024 * 1024]


def test_vertebra_readable_file_seek_slice_and_readinto() -> None:
    """SyncReadableFile supports random-access range reads and zero-copy readinto buffers."""
    store = SyncObjectStore.from_memory("arrow-test")
    data = b"PAR1-column-chunk-0123456789-footer-PAR1"
    store.put("table.parquet", data)

    with SyncReadableFile(store, "table.parquet") as f:
        assert f.size == len(data)
        assert f.read(4) == b"PAR1"
        assert f.tell() == 4
        buf = bytearray(13)
        read_count = f.readinto(buf)
        assert read_count == 13
        assert bytes(buf) == b"-column-chunk"
        assert f.read_at(len(data) - 4, 4) == b"PAR1"
        f.seek(-4, 2)
        assert f.read() == b"PAR1"


@pytest.mark.parametrize(
    ("buffer_factory", "expected"),
    [
        (lambda: memoryview(b""), b""),
        (bytearray, b""),
        (lambda: bytearray(b"mutable-payload"), b"mutable-payload"),
        (lambda: memoryview(bytearray(b"view-payload")), b"view-payload"),
    ],
)
def test_object_store_accepts_empty_and_mutable_pep3118_buffers(
    buffer_factory: Callable[[], Buffer],
    expected: bytes,
) -> None:
    """SyncObjectStore copies empty and mutable PEP 3118 buffers safely without UB."""
    store = SyncObjectStore.from_memory("buffer-safety")
    payload = buffer_factory()
    meta = store.put("blob.bin", payload)
    assert meta.size == len(expected)
    assert store.get("blob.bin") == expected
    view = store.get_view("blob.bin")
    assert bytes(view) == expected


@pytest.mark.anyio
async def test_async_object_store_roundtrip_and_buffer_safety() -> None:
    """AsyncObjectStore resolves Tokio completions and handles empty memoryviews."""
    store = AsyncObjectStore("memory://")
    meta = await store.put("empty.bin", memoryview(b""))
    assert meta.size == 0
    assert await store.get("empty.bin") == b""
    await store.put("hello.bin", bytearray(b"hello-async"))
    assert await store.get("hello.bin") == b"hello-async"
