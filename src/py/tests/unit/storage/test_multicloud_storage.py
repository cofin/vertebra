"""Unit tests for multi-cloud StorageConfig providers, OCI/Azure/R2 signing, Arrow readable file, and staging."""

from __future__ import annotations

import base64
import collections
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import vertebra.storage as storage
from vertebra.storage import (
    AzureConfig,
    GcsConfig,
    HttpConfig,
    LocalConfig,
    MemoryConfig,
    ObjectStore,
    OciConfig,
    R2Config,
    ReadableFile,
    S3Config,
    SigningConfig,
    UploadTracker,
)


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

    gcs_cfg = GcsConfig(bucket="ml-artifacts", service_account_path="/tmp/sa.json")
    gcs_url, gcs_opts = gcs_cfg.to_url_and_options()
    assert gcs_url == "gs://ml-artifacts"
    assert gcs_opts["service_account_path"] == "/tmp/sa.json"

    az_cfg = AzureConfig(container="Lake", account="devstoreaccount1", use_emulator=True)
    az_url, az_opts = az_cfg.to_url_and_options()
    assert az_url == "az://Lake"
    assert az_opts["endpoint"] == "http://127.0.0.1:4577/devstoreaccount1"

    oci_cfg = OciConfig(
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

    http_cfg = HttpConfig(url="https://cdn.example.com/files", headers={"X-Token": "abc"})
    http_url, http_opts = http_cfg.to_url_and_options()
    assert http_url == "https://cdn.example.com/files"
    assert http_opts == {"X-Token": "abc"}

    mem_cfg = MemoryConfig(namespace="unit")
    mem_url, mem_opts = mem_cfg.to_url_and_options()
    assert mem_url == "memory://"
    assert mem_opts == {"namespace": "unit"}


def test_sync_store_and_async_store_facades_removed() -> None:
    """SyncStore and AsyncStore wrappers are deleted in favor of unified ObjectStore."""
    assert not hasattr(storage, "SyncStore")
    assert not hasattr(storage, "AsyncStore")


def test_verify_presigned_url_gcs_raises_not_implemented() -> None:
    """Verifying a GCS presigned URL offline raises NotImplementedError."""
    store = ObjectStore(
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
        match="GCS V4 RSA-SHA256 presigned URL verification is not supported offline",
    ):
        store.verify_presigned_url(
            "https://storage.googleapis.com/ml-artifacts/test.bin?X-Goog-Algorithm=GOOG4-RSA-SHA256"
        )


def test_upload_tracker_logs_rollback_errors_and_uses_deque(caplog: pytest.LogCaptureFixture) -> None:
    """UploadTracker uses collections.deque, logs warnings on delete errors, and attaches after_flush."""
    store = ObjectStore.from_memory("staging-test-err")
    tracker = UploadTracker(store)

    assert isinstance(tracker._staged_uploads, collections.deque)
    assert isinstance(tracker._pending_deletes, collections.deque)

    mock_session = MagicMock()
    mock_listen = MagicMock()
    mock_event_mod = MagicMock(listen=mock_listen)
    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "sqlalchemy.event", mock_event_mod)
        tracker.attach_sqlalchemy_session(mock_session)
        mock_listen.assert_any_call(mock_session, "after_flush", tracker.before_flush)
        mock_listen.assert_any_call(mock_session, "after_commit", tracker.after_commit)
        mock_listen.assert_any_call(mock_session, "after_rollback", tracker.after_rollback)

    tracker.stage_upload("fail/item.bin", b"data")
    tracker.before_flush()
    assert store.exists("fail/item.bin")

    with pytest.MonkeyPatch.context() as mp:

        def _failing_delete(self_store: ObjectStore, k: str) -> None:
            raise OSError("disk IO failure")

        mp.setattr(ObjectStore, "delete", _failing_delete)
        with caplog.at_level("WARNING"):
            tracker.after_rollback()
        assert "Failed to delete object 'fail/item.bin' during transaction cleanup" in caplog.text


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

    payload = b"x" * (12 * 1024 * 1024)
    chunks = R2Config.chunk_uniform_parts(payload, part_size=5 * 1024 * 1024)
    assert [len(c) for c in chunks] == [5 * 1024 * 1024, 5 * 1024 * 1024, 2 * 1024 * 1024]


def test_vertebra_readable_file_seek_and_slice() -> None:
    """ReadableFile supports random-access range reads for Parquet/Arrow footers."""
    store = ObjectStore.from_memory("arrow-test")
    data = b"PAR1-column-chunk-0123456789-footer-PAR1"
    store.put("table.parquet", data)

    with ReadableFile(store, "table.parquet") as f:
        assert f.size == len(data)
        assert f.read(4) == b"PAR1"
        assert f.tell() == 4
        assert f.read_at(len(data) - 4, 4) == b"PAR1"
        f.seek(-4, 2)
        assert f.read() == b"PAR1"


@pytest.mark.anyio
async def test_file_object_session_tracker_commit_and_rollback() -> None:
    """UploadTracker flushes staged uploads on commit and deletes them on rollback."""
    store = ObjectStore.from_memory("staging-test")

    async with UploadTracker(store) as tracker:
        tracker.stage_upload("users/avatar.png", b"png-bytes")
        assert not store.exists("users/avatar.png")
        await tracker.before_flush_async()
        assert store.exists("users/avatar.png")
        await tracker.after_commit_async()

    assert store.exists("users/avatar.png")
    assert store.get("users/avatar.png") == b"png-bytes"

    with pytest.raises(RuntimeError, match="db transaction aborted"):
        async with UploadTracker(store) as failing_tracker:
            failing_tracker.stage_upload("users/orphan.png", b"orphan-bytes")
            await failing_tracker.before_flush_async()
            assert store.exists("users/orphan.png")
            raise RuntimeError("db transaction aborted")

    assert not store.exists("users/orphan.png")
