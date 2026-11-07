"""Unit tests for vertebra.storage (SyncObjectStore, AsyncObjectStore, zero-copy memoryview, and V4 presigning)."""

import ast
import threading
from collections.abc import AsyncIterator, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

import vertebra.storage as storage
from vertebra._storage import StorageBuffer
from vertebra.storage import (
    AsyncObjectStore,
    AsyncReadableFile,
    LocalConfig,
    MemoryConfig,
    ObjectMeta,
    PutResult,
    SigningConfig,
    SyncObjectStore,
    SyncReadableFile,
)


def test_memory_store_sync_crud_and_zero_copy_memoryview() -> None:
    """Verify synchronous CRUD, range reads, and zero-copy memoryview inputs/outputs on memory://."""
    store = SyncObjectStore("memory://")
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
    store = SyncObjectStore(f"file://{tmp_path}")
    meta = store.put_multipart("chunks/combined.bin", [b"chunk-01:", b"chunk-02:", memoryview(b"chunk-03")])
    assert meta.size == 26
    assert store.get("chunks/combined.bin") == b"chunk-01:chunk-02:chunk-03"


@pytest.mark.anyio
async def test_async_store_operations() -> None:
    """Verify asyncio-compatible put, get, get_range, head, list, delete on AsyncObjectStore."""
    store = AsyncObjectStore("memory://")
    await store.put("async/item.json", b'{"ok":true}')
    assert await store.exists("async/item.json")

    body = await store.get("async/item.json")
    assert body == b'{"ok":true}'

    partial = await store.get_range("async/item.json", 1, 4)
    assert partial == b'"ok"'

    meta = await store.head("async/item.json")
    assert meta.size == 11

    listed = await store.list("async")
    assert len(listed) == 1

    await store.delete("async/item.json")
    assert not await store.exists("async/item.json")


def test_hmac_and_s3_v4_presigning_and_verification() -> None:
    """Verify HMAC-SHA256 and S3 Signature V4 URL signing and verification."""
    store = SyncObjectStore(
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

    hmac_store = SyncObjectStore(
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
    """Verify SyncObjectStore supports provider='gcs' keyless IAM signBlob V4 presigning."""
    server = HTTPServer(("127.0.0.1", 0), _MockIamHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    gcs_store = SyncObjectStore(
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
    """SyncObjectStore.open_readable_file returns a seekable SyncReadableFile and wrappers are removed."""
    assert not hasattr(storage, "SyncStore")
    assert not hasattr(storage, "AsyncStore")
    assert not hasattr(storage, "StorageRegistry")
    assert not hasattr(storage, "UploadTracker")

    store = SyncObjectStore.from_local(tmp_path)
    store.put("data.parquet", b"PAR1-parquet-content-PAR1")

    with store.open_readable_file("data.parquet") as f:
        assert isinstance(f, SyncReadableFile)
        assert f.size == 25
        assert f.read(4) == b"PAR1"
        assert f.tell() == 4
        f.seek(-4, 2)
        assert f.read(4) == b"PAR1"


@pytest.mark.anyio
async def test_async_put_stream_and_get_stream_roundtrip(tmp_path: Path) -> None:
    """AsyncObjectStore streams multipart chunks and yields zero-copy StorageBuffer slices."""
    store = AsyncObjectStore(LocalConfig(root=tmp_path))
    chunk_payload = b"a" * 65536

    async def async_chunks() -> AsyncIterator[bytes]:
        yield chunk_payload
        yield chunk_payload
        yield chunk_payload
        yield chunk_payload

    meta = await store.put_stream(
        "exports/data.bin",
        async_chunks(),
        content_type="application/octet-stream",
        attributes={"job": "nightly"},
    )
    assert isinstance(meta, ObjectMeta)
    assert meta.size == 262144

    collected: list[bytes] = []
    async for buf in store.get_stream("exports/data.bin", chunk_size=32768):
        assert isinstance(buf, StorageBuffer)
        view = memoryview(buf)
        assert len(view) <= 32768
        collected.append(bytes(view))

    assert b"".join(collected) == chunk_payload * 4

    sync_iterable_meta = await store.put_stream(
        "exports/sync_iter.bin",
        [b"part-1:", memoryview(b"part-2")],
    )
    assert sync_iterable_meta.size == 13
    assert await store.get("exports/sync_iter.bin") == b"part-1:part-2"


def test_sync_put_stream_and_get_stream_roundtrip(tmp_path: Path) -> None:
    """SyncObjectStore streams multipart chunks and yields zero-copy StorageBuffer slices."""
    store = SyncObjectStore(LocalConfig(root=tmp_path))
    meta = store.put_stream(
        "exports/sync.bin",
        (chunk for chunk in (b"alpha-", bytearray(b"beta-"), memoryview(b"gamma"))),
    )
    assert isinstance(meta, ObjectMeta)
    assert meta.size == 16

    buffers = list(store.get_stream("exports/sync.bin", chunk_size=5))
    assert all(isinstance(buf, StorageBuffer) for buf in buffers)
    assert b"".join(bytes(memoryview(buf)) for buf in buffers) == b"alpha-beta-gamma"


@pytest.mark.anyio
async def test_failed_put_stream_aborts_upload_without_committing(tmp_path: Path) -> None:
    """SyncObjectStore and AsyncObjectStore abort multipart uploads when chunk iterators raise."""
    sync_store = SyncObjectStore(LocalConfig(root=tmp_path / "sync"))

    def failing_sync_chunks() -> Iterator[bytes]:
        yield b"abc"
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        sync_store.put_stream("exports/partial.bin", failing_sync_chunks())
    assert not sync_store.exists("exports/partial.bin")

    async_store = AsyncObjectStore(LocalConfig(root=tmp_path / "async"))

    async def failing_async_chunks() -> AsyncIterator[bytes]:
        yield b"abc"
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await async_store.put_stream("exports/partial.bin", failing_async_chunks())
    assert not await async_store.exists("exports/partial.bin")


@pytest.mark.anyio
async def test_conditional_put_and_metadata_roundtrip() -> None:
    """SyncObjectStore and AsyncObjectStore enforce conditional puts and round-trip metadata."""
    store = SyncObjectStore(MemoryConfig(namespace="cond-sync"))
    first = store.put(
        "cfg.json",
        b"{}",
        content_type="application/json",
        attributes={"env": "prod"},
        if_not_exists=True,
    )
    assert isinstance(first, ObjectMeta)
    assert first.content_type == "application/json"
    assert first.attributes == {"env": "prod"}

    head_meta = store.head("cfg.json")
    assert head_meta.content_type == "application/json"
    assert head_meta.attributes == {"env": "prod"}

    with pytest.raises(FileExistsError):
        store.put("cfg.json", b'{"v":2}', if_not_exists=True)

    with pytest.raises(PermissionError):
        store.put("cfg.json", b'{"v":2}', if_match="non-matching-etag")

    async_store = AsyncObjectStore(MemoryConfig(namespace="cond-async"))
    async_meta = await async_store.put(
        "cfg.json",
        b"{}",
        content_type="application/json",
        attributes={"env": "staging"},
        if_not_exists=True,
    )
    assert isinstance(async_meta, ObjectMeta)
    assert async_meta.content_type == "application/json"
    assert async_meta.attributes == {"env": "staging"}

    with pytest.raises(FileExistsError):
        await async_store.put("cfg.json", b'{"v":2}', if_not_exists=True)


@pytest.mark.anyio
async def test_paginated_listing_delimiter_and_rename(tmp_path: Path) -> None:
    """SyncObjectStore and AsyncObjectStore support list_with_delimiter, list_page, and rename."""
    store = SyncObjectStore(LocalConfig(root=tmp_path))
    store.put("logs/2026/01.log", b"jan")
    store.put("logs/2026/02.log", b"feb")
    store.put("logs/root.log", b"root")

    direct_items, prefixes = store.list_with_delimiter("logs/")
    assert [item.path for item in direct_items] == ["logs/root.log"]
    assert prefixes == ["logs/2026"]

    page1, cursor1 = store.list_page("logs/", limit=2)
    assert len(page1) == 2
    assert cursor1 is not None

    page2, cursor2 = store.list_page("logs/", cursor=cursor1, limit=2)
    assert len(page2) == 1
    assert cursor2 is None
    assert sorted(item.path for item in [*page1, *page2]) == [
        "logs/2026/01.log",
        "logs/2026/02.log",
        "logs/root.log",
    ]

    store.rename("logs/root.log", "logs/moved.log", overwrite=False)
    assert not store.exists("logs/root.log")
    assert store.get("logs/moved.log") == b"root"

    with pytest.raises(FileExistsError):
        store.rename("logs/2026/01.log", "logs/moved.log", overwrite=False)

    async_store = AsyncObjectStore(LocalConfig(root=tmp_path))
    a_items, a_prefixes = await async_store.list_with_delimiter("logs/")
    assert [item.path for item in a_items] == ["logs/moved.log"]
    assert a_prefixes == ["logs/2026"]

    a_page, a_cursor = await async_store.list_page("logs/", limit=2)
    assert len(a_page) == 2
    assert a_cursor is not None

    await async_store.rename("logs/moved.log", "logs/restored.log", overwrite=False)
    assert await async_store.exists("logs/restored.log")


@pytest.mark.anyio
async def test_readable_file_streaming_iterators_and_zero_length_read() -> None:
    """SyncReadableFile and AsyncReadableFile stream chunks and short-circuit read(0) without RPCs."""
    sync_store = SyncObjectStore(MemoryConfig(namespace="readable-sync"))
    payload = b"0123456789abcdef" * 8
    sync_store.put("payload.bin", payload)

    with SyncReadableFile(sync_store, "payload.bin", chunk_size=16) as sf:
        assert sf.read(0) == b""
        assert b"".join(sf) == payload
        assert sf.tell() == len(payload)
        assert sf.read(8) == b""

    unseeded_sync = SyncReadableFile(sync_store, "missing.bin", size=100)
    assert unseeded_sync.read(0) == b""

    async_store = AsyncObjectStore(MemoryConfig(namespace="readable-async"))
    await async_store.put("payload.bin", payload)

    async with AsyncReadableFile(async_store, "missing.bin") as af_unseeded:
        assert await af_unseeded.read(0) == b""

    async with async_store.open_readable_file("payload.bin", chunk_size=16) as af:
        assert await af.read(0) == b""
        chunks = [chunk async for chunk in af]
        assert len(chunks) == 8
        assert b"".join(chunks) == payload
        assert af.tell() == len(payload)
        assert await af.read(8) == b""


def test_store_module_contains_zero_asyncio_to_thread_calls() -> None:
    """AST scan verifies vertebra.storage._store never delegates async operations to asyncio.to_thread."""
    store_file = Path(storage.__file__).resolve().parent / "_store.py"
    tree = ast.parse(store_file.read_text(encoding="utf-8"), filename=str(store_file))
    offending = [
        node.lineno
        for node in ast.walk(tree)
        if (isinstance(node, ast.Attribute) and node.attr == "to_thread")
        or (isinstance(node, ast.Name) and node.id == "to_thread")
    ]
    assert offending == []
