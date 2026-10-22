"""Unit tests for vertebra.storage (ObjectStore, zero-copy memoryview, and V4 presigning)."""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

import vertebra.storage as storage
from vertebra.storage import ObjectMeta, ObjectStore, PutResult, SigningConfig, StorageRegistry
from vertebra.storage._store import ReadableFile


def test_memory_store_sync_crud_and_zero_copy_memoryview() -> None:
    """Verify synchronous CRUD, range reads, and zero-copy memoryview inputs/outputs on memory://."""
    store = ObjectStore("memory://")
    payload = memoryview(b"abcdefghijklmnopqrstuvwxyz")
    result = store.put("alpha/alphabet.bin", payload)
    assert isinstance(result, PutResult)
    assert result.path == "alpha/alphabet.bin"
    assert result.size == 26

    raw = store.get("alpha/alphabet.bin")
    assert raw == b"abcdefghijklmnopqrstuvwxyz"

    view = store.get_view("alpha/alphabet.bin")
    assert isinstance(view, memoryview)
    assert bytes(view[3:10]) == b"defghij"

    range_bytes = store.get_range("alpha/alphabet.bin", offset=5, length=6)
    assert range_bytes == b"fghijk"

    meta = store.head("alpha/alphabet.bin")
    assert isinstance(meta, ObjectMeta)
    assert meta.path == "alpha/alphabet.bin"
    assert meta.size == 26

    store.copy("alpha/alphabet.bin", "alpha/alphabet_copy.bin")
    items = store.list("alpha")
    assert [item.path for item in items] == ["alpha/alphabet.bin", "alpha/alphabet_copy.bin"]

    store.delete("alpha/alphabet_copy.bin")
    assert not store.exists("alpha/alphabet_copy.bin")


def test_local_file_store_and_multipart(tmp_path: Path) -> None:
    """Verify file:// backend persistence and multipart uploads."""
    store = ObjectStore(f"file://{tmp_path}")
    meta = store.put_multipart("chunks/combined.bin", [b"chunk-01:", b"chunk-02:", memoryview(b"chunk-03")])
    assert meta.size == 26
    assert store.get("chunks/combined.bin") == b"chunk-01:chunk-02:chunk-03"


@pytest.mark.anyio
async def test_async_store_operations() -> None:
    """Verify asyncio-compatible put_async, get_async, get_range_async, head_async, list_async, delete_async."""
    store = ObjectStore("memory://")
    await store.put_async("async/item.json", b'{"ok":true}')
    assert await store.exists_async("async/item.json")

    body = await store.get_async("async/item.json")
    assert body == b'{"ok":true}'

    partial = await store.get_range_async("async/item.json", 1, 4)
    assert partial == b'"ok"'

    meta = await store.head_async("async/item.json")
    assert meta.size == 11

    listed = await store.list_async("async")
    assert len(listed) == 1

    await store.delete_async("async/item.json")
    assert not await store.exists_async("async/item.json")


def test_hmac_and_s3_v4_presigning_and_verification() -> None:
    """Verify HMAC-SHA256 and S3 Signature V4 URL signing and verification."""
    store = ObjectStore(
        "s3://media-bucket",
        signing=SigningConfig(
            provider="s3",
            bucket="media-bucket",
            region="us-west-2",
            access_key_id="AKIAEXAMPLEKEY",
            secret_access_key="secret-access-key-example-value",
            endpoint="https://s3.us-west-2.amazonaws.com",
        ),
    )
    signed_get = store.presign_get("videos/clip.mp4", expires_in_secs=1800, timestamp=1_700_000_000)
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in signed_get
    assert store.verify_presigned_url(signed_get, method="GET", timestamp=1_700_000_100)
    assert not store.verify_presigned_url(signed_get, method="GET", timestamp=1_700_003_000)

    hmac_store = ObjectStore(
        "memory://",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret="hmac-shared-secret-key-for-cdn",
            endpoint="https://files.example.org",
        ),
    )
    hmac_url = hmac_store.presign_put("uploads/avatar.png", expires_in_secs=600, timestamp=1_700_000_000)
    assert hmac_store.verify_presigned_url(hmac_url, method="PUT", timestamp=1_700_000_200)
    assert not hmac_store.verify_presigned_url(hmac_url, method="GET", timestamp=1_700_000_200)


class _MockIamHandler(BaseHTTPRequestHandler):
    """Mock HTTP handler returning a base64-encoded signedBlob payload."""

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        _ = self.rfile.read(length)
        body = b'{"keyId":"k1","signedBlob":"ZGVhZGJlZWZjYWZl"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        _ = (format, args)


def test_gcs_v4_keyless_iam_presigning_via_object_store() -> None:
    """Verify ObjectStore supports provider='gcs' keyless IAM signBlob V4 presigning."""
    server = HTTPServer(("127.0.0.1", 0), _MockIamHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    gcs_store = ObjectStore(
        "gs://ml-artifacts",
        signing=SigningConfig(
            provider="gcs",
            bucket="ml-artifacts",
            service_account_email="signer@project.iam.gserviceaccount.com",
            iam_endpoint=f"http://127.0.0.1:{port}",
            access_token="mock-token",
        ),
    )
    signed = gcs_store.presign_get("weights/model.bin", expires_in_secs=900, timestamp=1_700_000_000)
    thread.join(timeout=5)
    server.server_close()

    assert "X-Goog-Algorithm=GOOG4-RSA-SHA256" in signed
    assert "X-Goog-Signature=646561646265656663616665" in signed


def test_object_store_open_readable_file(tmp_path: Path) -> None:
    """ObjectStore.open_readable_file returns a seekable ReadableFile and wrappers are removed."""
    assert not hasattr(storage, "SyncStore")
    assert not hasattr(storage, "AsyncStore")
    assert not hasattr(StorageRegistry, "register_store")

    store = ObjectStore.from_local(tmp_path)
    store.put("data.parquet", b"PAR1-parquet-content-PAR1")

    with store.open_readable_file("data.parquet") as f:
        assert isinstance(f, ReadableFile)
        assert f.size == 25
        assert f.read(4) == b"PAR1"
        assert f.tell() == 4
        f.seek(-4, 2)
        assert f.read(4) == b"PAR1"
