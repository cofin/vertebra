"""Unit tests for vertebra.storage (SyncObjectStore, AsyncObjectStore, zero-copy memoryview, and V4 presigning)."""

import ast
import asyncio
import contextlib
import sys
import threading
from collections.abc import AsyncIterator, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import pytest

import vertebra.storage as storage
from vertebra._storage import StorageBuffer
from vertebra.storage import (
    AsyncObjectStore,
    AsyncReadableFile,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    ObjectMeta,
    PutResult,
    RangeNotSatisfiableError,
    S3Config,
    SigningConfig,
    StreamingStorageResponse,
    SyncObjectStore,
    SyncReadableFile,
    parse_byte_range,
)
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, reset_dependency_cache


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


def test_fail_closed_empty_or_missing_target_raises_improper_configuration() -> None:
    """SyncObjectStore and AsyncObjectStore reject None, empty, and schemeless targets with ImproperConfigurationError."""
    with pytest.raises(TypeError):
        cast(Any, SyncObjectStore)()

    with pytest.raises(TypeError):
        cast(Any, AsyncObjectStore)()

    for invalid_target in ("", "   ", "relative/path/without/scheme", "ftp://unsupported.example.com/bucket"):
        with pytest.raises(ImproperConfigurationError):
            SyncObjectStore(invalid_target)
        with pytest.raises(ImproperConfigurationError):
            AsyncObjectStore(invalid_target)

    with pytest.raises(ImproperConfigurationError):
        cast(Any, SyncObjectStore)(None)

    with pytest.raises(ImproperConfigurationError):
        cast(Any, AsyncObjectStore)(None)

    with pytest.raises(ImproperConfigurationError):
        SyncObjectStore(HTTPConfig(url=""))


def test_fail_closed_missing_storage_extension_raises_missing_dependency(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Masking vertebra._storage causes SyncObjectStore and AsyncObjectStore to raise MissingDependencyError."""
    monkeypatch.setitem(sys.modules, "vertebra._storage", None)
    reset_dependency_cache()
    try:
        with pytest.raises(MissingDependencyError) as exc_file:
            SyncObjectStore(f"file://{tmp_path}")
        assert exc_file.value.install_package == "vertebra-core[storage]"
        assert "vertebra-core[storage]" in exc_file.value.hint

        with pytest.raises(MissingDependencyError) as exc_mem_sync:
            SyncObjectStore("memory://test")
        assert exc_mem_sync.value.install_package == "vertebra-core[storage]"

        with pytest.raises(MissingDependencyError) as exc_mem_async:
            AsyncObjectStore(MemoryConfig(namespace="masked"))
        assert exc_mem_async.value.install_package == "vertebra-core[storage]"
    finally:
        monkeypatch.undo()
        reset_dependency_cache()


@pytest.mark.anyio
async def test_explicit_memory_config_and_to_async_share_rust_in_memory_backend() -> None:
    """Explicit MemoryConfig delegates to Rust InMemory without Python _memory dict and shares state across to_async."""
    store = SyncObjectStore(MemoryConfig(namespace="t1"))
    assert not hasattr(store, "_memory")

    store.put("k", b"v")
    assert store.get("k") == b"v"

    async_store = store.to_async()
    assert not hasattr(async_store, "_memory")
    assert await async_store.get("k") == b"v"

    await async_store.put("k2", b"v2")
    assert store.get("k2") == b"v2"

    roundtrip_sync = async_store.to_sync()
    assert roundtrip_sync.get("k2") == b"v2"


@pytest.mark.parametrize("range_header", [None, "bytes=2-5", "bytes=9-", "bytes=-0"])
def test_storage_response_sync_local_ranges(tmp_path: Path, range_header: str | None) -> None:
    """Local storage exposes consistent range bodies through sync iteration and fastpath."""
    store = SyncObjectStore(LocalConfig(root=tmp_path))
    store.put("payload.bin", b"0123456789")
    response = StreamingStorageResponse.from_sync_store(store, "payload.bin", range_header=range_header)
    status, headers, body = response.to_fastpath_tuple()
    expected = {None: b"0123456789", "bytes=2-5": b"2345", "bytes=9-": b"9", "bytes=-0": b""}[range_header]
    assert status == (200 if range_header is None else 416 if range_header == "bytes=-0" else 206)
    assert body == expected
    assert b"".join(response.iter_chunks()) == expected
    assert dict(headers)[b"content-length"] == str(len(expected)).encode()


@pytest.mark.anyio
async def test_storage_response_empty_missing_and_invalid_chunks() -> None:
    """Empty ranges produce 416; missing objects and invalid chunk sizes fail explicitly."""
    store = AsyncObjectStore("memory://")
    await store.put("empty", b"")
    response = await StreamingStorageResponse.from_async_store(store, "empty", range_header="bytes=0-")
    assert (await response.to_async_fastpath_tuple())[0] == 416
    assert [chunk async for chunk in response.aiter_chunks()] == []
    with pytest.raises(FileNotFoundError):
        await StreamingStorageResponse.from_async_store(store, "missing")
    with pytest.raises(ValueError, match="chunk_size"):
        await StreamingStorageResponse.from_async_store(store, "empty", chunk_size=0)
    response = await StreamingStorageResponse.from_async_store(store, "empty")
    with pytest.raises(TypeError, match="AsyncObjectStore"):
        response.to_fastpath_tuple()


@pytest.mark.parametrize("header", ["bytes=", "bytes=-", "bytes=+1-2", "bytes=1- 2", "bytes=١-2", "bytes"])
def test_parse_byte_range_rejects_invalid_byte_syntax(header: str) -> None:
    """Malformed byte specifications fail with the representation size attached."""
    with pytest.raises(RangeNotSatisfiableError) as exc_info:
        parse_byte_range(header, 100)
    assert exc_info.value.total_size == 100


@pytest.mark.anyio
async def test_storage_response_async_native_owner_and_read_failure(tmp_path: Path) -> None:
    """Async full fastpath preserves the native buffer and propagates backend read failures."""
    store = AsyncObjectStore(LocalConfig(root=tmp_path))
    await store.put("body", b"native payload")
    response = await StreamingStorageResponse.from_async_store(
        store, "body", headers={"CONTENT-LENGTH": "999", "Content-Range": "bad", "X-Trace": "yes"}
    )
    status, headers, body = await response.to_async_fastpath_tuple()
    assert status == 200
    assert isinstance(body, memoryview)
    assert isinstance(body.obj, StorageBuffer)
    assert bytes(body) == b"native payload"
    assert dict(headers)[b"content-length"] == b"14"
    assert dict(headers)[b"x-trace"] == b"yes"
    assert b"content-range" not in dict(headers)
    await store.delete("body")
    with pytest.raises(FileNotFoundError):
        await response.to_async_fastpath_tuple()


@pytest.mark.anyio
async def test_storage_response_explicit_async_range_without_metadata() -> None:
    """Existing direct descriptors resolve an open-ended range without a supplied size."""
    store = AsyncObjectStore("memory://")
    await store.put("body", b"0123456789")
    response = StreamingStorageResponse(store=store, key="body", range_start=3, chunk_size=2)
    assert b"".join([bytes(chunk) async for chunk in response.aiter_chunks()]) == b"3456789"


@pytest.mark.anyio
@pytest.mark.parametrize("end", [5, None])
async def test_direct_async_storage_range_fastpath_matches_sync(end: int | None) -> None:
    """Direct descriptors retain HTTP range metadata when async storage resolves the body."""
    store = SyncObjectStore("memory://")
    store.put("body", b"0123456789")
    sync_response = StreamingStorageResponse(store=store, key="body", range_start=3, range_end=end)
    async_response = StreamingStorageResponse(store=store.to_async(), key="body", range_start=3, range_end=end)
    sync_status, sync_headers, sync_body = sync_response.to_fastpath_tuple()
    status, headers, body = await async_response.to_async_fastpath_tuple()
    assert status == sync_status == 206
    assert dict(headers)[b"content-range"] == dict(sync_headers)[b"content-range"]
    assert bytes(body) == sync_body
    assert dict(headers)[b"content-length"] == str(len(body)).encode()


@pytest.mark.parametrize(
    ("etag", "expected"),
    [("0", b'"0"'), ('"quoted"', b'"quoted"'), ('W/"weak"', b'W/"weak"'), ("tagé", b'"tag\xe9"')],
)
@pytest.mark.parametrize("in_headers", [False, True])
def test_storage_response_formats_http_entity_tags(etag: str, expected: bytes, in_headers: bool) -> None:
    """Raw native tokens become valid entity tags while valid strong and weak tags survive."""
    response = (
        StreamingStorageResponse(data=b"body", headers={"ETag": etag})
        if in_headers
        else StreamingStorageResponse(data=b"body", etag=etag)
    )
    assert dict(response.to_fastpath_tuple()[1])[b"etag"] == expected


@pytest.mark.parametrize("etag", ['bad"quote', '"unterminated', "bad\r\ninjected: yes", "has space", "☃"])
@pytest.mark.parametrize("in_headers", [False, True])
def test_storage_response_rejects_malformed_entity_tags(etag: str, in_headers: bool) -> None:
    """Invalid opaque tags fail before emitting an invalid or injected HTTP header."""
    with pytest.raises(ValueError, match="ETag"):
        response = (
            StreamingStorageResponse(data=b"body", headers={"ETag": etag})
            if in_headers
            else StreamingStorageResponse(data=b"body", etag=etag)
        )
        response.to_fastpath_tuple()


@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.anyio
async def test_storage_arrow_batch_readers(file_format: str) -> None:
    """Public sync and async readers decode exact batches and Parquet projections."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": range(250), "label": ["value"] * 250})
    sink = pa.BufferOutputStream()
    if file_format == "parquet":
        pq.write_table(table, sink, row_group_size=100)
    else:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table, max_chunksize=100)
    store = SyncObjectStore("memory://")
    store.put("batches", sink.getvalue())
    if file_format == "parquet":
        sync_batches = list(store.iter_parquet_batches("batches", batch_size=100, columns=["id"]))
        async_batches = [
            batch async for batch in store.to_async().iter_parquet_batches("batches", batch_size=100, columns=["id"])
        ]
        expected = table.select(["id"])
    else:
        sync_batches = list(store.iter_arrow_batches("batches"))
        async_batches = [batch async for batch in store.to_async().iter_arrow_batches("batches")]
        expected = table
    assert [batch.num_rows for batch in sync_batches] == [100, 100, 50]
    assert pa.Table.from_batches(sync_batches).equals(expected)
    assert pa.Table.from_batches(async_batches).equals(expected)


@pytest.mark.anyio
@pytest.mark.parametrize("empty", [False, True])
async def test_storage_arrow_ipc_file_table_fallback(empty: bool) -> None:
    """Seekable IPC file reads preserve data and zero-row schemas."""
    from vertebra.storage._arrow import read_table_async, read_table_sync

    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([] if empty else [1, 2], type=pa.int64())})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    store = SyncObjectStore("memory://")
    store.put("file", sink.getvalue())
    assert read_table_sync(store, "file", file_format="arrow-ipc").equals(table)
    assert (await read_table_async(store.to_async(), "file", file_format="arrow-ipc")).equals(table)


@pytest.mark.anyio
async def test_storage_arrow_dictionary_stream_and_early_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """One persistent IPC decoder retains dictionary state and closes native streams."""
    pa = pytest.importorskip("pyarrow")
    labels = pa.DictionaryArray.from_arrays(pa.array([0, 1]), pa.array(["one", "two"]))
    batch = pa.record_batch([labels], names=["label"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
        writer.write_batch(batch)
    store = SyncObjectStore("memory://")
    store.put("dictionary", sink.getvalue())
    streams: list[Any] = []
    original = AsyncObjectStore.get_stream

    def observe_stream(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> Any:
        stream = original(self, *args, **kwargs)
        streams.append(stream)
        return stream

    monkeypatch.setattr(AsyncObjectStore, "get_stream", observe_stream)
    batches = store.to_async().iter_arrow_batches("dictionary")
    assert (await anext(batches)).column(0).to_pylist() == ["one", "two"]
    assert (await anext(batches)).column(0).to_pylist() == ["one", "two"]
    await batches.aclose()
    assert len(streams) == 1
    assert streams[0].ag_frame is None
    assert [item.column(0).to_pylist() for item in store.iter_arrow_batches("dictionary")] == [
        ["one", "two"],
        ["one", "two"],
    ]


@pytest.mark.anyio
async def test_storage_parquet_reader_native_range_demand(monkeypatch: pytest.MonkeyPatch) -> None:
    """Later rowgroups remain unread until the consumer asks for their batches."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": range(300), "blob": [f"{index}:" + "x" * 2048 for index in range(300)]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=100, compression=None, use_dictionary=False)
    payload = sink.getvalue()
    metadata = pq.ParquetFile(pa.BufferReader(payload)).metadata
    second_offset = metadata.row_group(1).column(0).data_page_offset
    store = SyncObjectStore("memory://")
    store.put("groups", payload)
    ranges: list[tuple[int, int]] = []
    handles: list[AsyncReadableFile] = []
    original_range = AsyncObjectStore.get_range
    original_open = AsyncObjectStore.open_reader

    async def observe_range(self: AsyncObjectStore, key: str, offset: int, length: int) -> bytes:
        ranges.append((offset, length))
        return await original_range(self, key, offset, length)

    def observe_open(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> AsyncReadableFile:
        handle = original_open(self, *args, **kwargs)
        handles.append(handle)
        return handle

    def reject_materialization(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("batch readers must use native streams or ranges")

    monkeypatch.setattr(AsyncObjectStore, "get_range", observe_range)
    monkeypatch.setattr(AsyncObjectStore, "open_reader", observe_open)
    monkeypatch.setattr(AsyncObjectStore, "get", reject_materialization)
    monkeypatch.setattr(AsyncObjectStore, "get_view", reject_materialization)
    monkeypatch.setattr(SyncObjectStore, "get_range", reject_materialization)
    batches = store.to_async().iter_parquet_batches("groups", batch_size=100)
    first = await anext(batches)
    assert first.equals(table.slice(0, 100).to_batches()[0])
    assert payload.size > 3 * 65536
    data_ranges = [(offset, length) for offset, length in ranges if offset != payload.size - 65536]
    assert data_ranges
    assert all(offset + length <= second_offset for offset, length in data_ranges)
    checkpoint = ranges.copy()
    await asyncio.sleep(0)
    assert ranges == checkpoint
    assert (await anext(batches)).equals(table.slice(100, 100).to_batches()[0])
    assert any(offset == second_offset for offset, _ in ranges)
    await batches.aclose()
    assert len(handles) == 1
    assert handles[0].closed


@pytest.mark.anyio
async def test_storage_parquet_explicit_prefetch_option(monkeypatch: pytest.MonkeyPatch) -> None:
    """The private SQLSpec seam forwards an explicit prefetch opt-in."""
    from vertebra.storage._arrow import iter_batches_async

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": [1, 2]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    store = SyncObjectStore("memory://")
    store.put("prefetch", sink.getvalue())
    seen: list[bool] = []
    original = pq.ParquetFile

    def observe_options(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["pre_buffer"])
        return original(*args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", observe_options)
    result = [
        batch
        async for batch in iter_batches_async(
            store.to_async(), "prefetch", file_format="parquet", options={"pre_buffer": True}
        )
    ]
    assert pa.Table.from_batches(result).equals(table)
    assert seen == [True]


@pytest.mark.anyio
@pytest.mark.parametrize("batch_size", [0, -1])
async def test_storage_parquet_rejects_batch_size_before_io(batch_size: int) -> None:
    """Invalid demand sizes fail before looking up a missing native object."""
    store = SyncObjectStore("memory://")
    with pytest.raises(ValueError, match="batch_size"):
        next(store.iter_parquet_batches("missing", batch_size=batch_size))
    with pytest.raises(ValueError, match="batch_size"):
        await anext(store.to_async().iter_parquet_batches("missing", batch_size=batch_size))


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_corruption_closes_sources(file_format: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Corrupt codecs raise their original errors and close real native sources."""
    pa = pytest.importorskip("pyarrow")
    store = SyncObjectStore("memory://")
    store.put("corrupt", b"truncated invalid codec")
    sources: list[Any] = []
    original = AsyncObjectStore.open_reader if file_format == "parquet" else AsyncObjectStore.get_stream

    def observe_source(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> Any:
        source = original(self, *args, **kwargs)
        sources.append(source)
        return source

    monkeypatch.setattr(AsyncObjectStore, "open_reader" if file_format == "parquet" else "get_stream", observe_source)
    async_store = store.to_async()
    iterator = (
        async_store.iter_parquet_batches("corrupt")
        if file_format == "parquet"
        else async_store.iter_arrow_batches("corrupt")
    )
    with pytest.raises(pa.ArrowInvalid):
        await anext(iterator)
    assert len(sources) == 1
    assert sources[0].closed if file_format == "parquet" else sources[0].ag_frame is None
    sync_iterator = (
        store.iter_parquet_batches("corrupt") if file_format == "parquet" else store.iter_arrow_batches("corrupt")
    )
    with pytest.raises(pa.ArrowInvalid):
        next(sync_iterator)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_storage_arrow_native_network_barrier(
    file_format: str, cancel: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real native network reads keep the loop responsive and unwind before close."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": [1, 2]})
    sink = pa.BufferOutputStream()
    if file_format == "parquet":
        pq.write_table(table, sink)
    else:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
    payload = sink.getvalue().to_pybytes()
    entered = threading.Event()
    release = threading.Event()
    paths: list[str] = []

    class RangeHandler(BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:
            paths.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("ETag", '"fixture"')
            self.send_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
            self.end_headers()

        def do_GET(self) -> None:
            paths.append(self.path)
            entered.set()
            if not release.wait(10):
                return
            range_header = self.headers.get("Range")
            if range_header:
                start, end = range_header.removeprefix("bytes=").split("-", 1)
                offset = int(start)
                limit = int(end) + 1 if end else len(payload)
                body = payload[offset:limit]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {offset}-{limit - 1}/{len(payload)}")
            else:
                body = payload
                self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", '"fixture"')
            self.end_headers()
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            _ = (format, args)

    server = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    sources: list[Any] = []
    original = AsyncObjectStore.open_reader if file_format == "parquet" else AsyncObjectStore.get_stream

    def observe_source(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> Any:
        source = original(self, *args, **kwargs)
        sources.append(source)
        return source

    monkeypatch.setattr(AsyncObjectStore, "open_reader" if file_format == "parquet" else "get_stream", observe_source)
    store = AsyncObjectStore(
        S3Config(
            bucket="codec-fixture",
            region="us-east-1",
            endpoint=f"http://127.0.0.1:{server.server_port}",
            access_key_id="test",
            secret_access_key="test",
            allow_http=True,
        )
    )
    batches = (
        store.iter_parquet_batches("data.parquet")
        if file_format == "parquet"
        else store.iter_arrow_batches("data.parquet")
    )
    before = asyncio.all_tasks()
    task = asyncio.create_task(anext(batches))
    try:
        async with asyncio.timeout(5):
            while not entered.is_set():
                if task.done():
                    await task
                await asyncio.sleep(0.001)
        assert await asyncio.create_task(asyncio.sleep(0, result="responsive")) == "responsive"
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        else:
            release.set()
            assert (await asyncio.wait_for(task, 5)).equals(table.to_batches()[0])
            await batches.aclose()
        assert len(sources) == 1
        assert sources[0].closed if file_format == "parquet" else sources[0].ag_frame is None
        assert all(path == "/codec-fixture/data.parquet" for path in paths)
        assert not (asyncio.all_tasks() - before)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await batches.aclose()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_schema_only_batches(file_format: str) -> None:
    """Schema-only streams yield no batches without losing full-Table schema."""
    from vertebra.storage._arrow import read_table_async, read_table_sync

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": pa.array([], type=pa.int64())})
    sink = pa.BufferOutputStream()
    if file_format == "parquet":
        pq.write_table(table, sink)
    else:
        with pa.ipc.new_stream(sink, table.schema):
            pass
    store = SyncObjectStore("memory://")
    store.put("empty", sink.getvalue())
    sync_batches = (
        store.iter_parquet_batches("empty") if file_format == "parquet" else store.iter_arrow_batches("empty")
    )
    assert list(sync_batches) == []
    async_store = store.to_async()
    async_batches = (
        async_store.iter_parquet_batches("empty")
        if file_format == "parquet"
        else async_store.iter_arrow_batches("empty")
    )
    assert [batch async for batch in async_batches] == []
    assert read_table_sync(store, "empty", file_format=file_format).equals(table)
    assert (await read_table_async(async_store, "empty", file_format=file_format)).equals(table)


@pytest.mark.anyio
async def test_storage_arrow_missing_codec_before_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """Optional Arrow dependencies fail explicitly before native reads begin."""
    store = SyncObjectStore("memory://")
    with monkeypatch.context() as context:
        context.setitem(sys.modules, "pyarrow", None)
        reset_dependency_cache()
        with pytest.raises(MissingDependencyError, match="pyarrow"):
            next(store.iter_arrow_batches("missing"))
        with pytest.raises(MissingDependencyError, match="pyarrow"):
            await anext(store.to_async().iter_arrow_batches("missing"))
    reset_dependency_cache()
