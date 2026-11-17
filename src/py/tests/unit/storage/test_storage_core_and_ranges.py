"""Unit tests for vertebra.storage core CRUD, ranges, conditional checks, and atomic rename."""

import os
import sys
import threading
from collections.abc import AsyncIterator, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, cast

import pytest

import vertebra._storage as native_st
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
    SigningConfig,
    StreamingStorageResponse,
    SyncObjectStore,
    SyncReadableFile,
    parse_byte_range,
)
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    PreconditionError,
    VertebraError,
    reset_dependency_cache,
)
from vertebra.utils.serializers import decode_json


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
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_multipart_streams_parts(asynchronous: bool) -> None:
    """Consume each part before a producer reuses its mutable buffer."""
    store = SyncObjectStore(MemoryConfig())
    buffer = bytearray(b"aaaa")

    def parts() -> Iterator[bytearray]:
        for value in (b"aaaa", b"bbbb", b"cccc"):
            buffer[:] = value
            yield buffer

    if asynchronous:
        meta = await store.to_async().put_multipart("parts", parts())
    else:
        meta = store.put_multipart("parts", parts())
    assert meta.size == 12
    assert store.get("parts") == b"aaaabbbbcccc"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_multipart_empty_and_failed_iterables(asynchronous: bool) -> None:
    """Empty iterables publish empty objects and failed iterables preserve existing bytes."""
    store = SyncObjectStore(MemoryConfig())
    store.put("prior", b"original")

    def parts() -> Iterator[bytes]:
        yield b"partial"
        raise RuntimeError("boom")

    if asynchronous:
        paired = store.to_async()
        meta = await paired.put_multipart("empty", iter(()))
        with pytest.raises(RuntimeError, match="boom"):
            await paired.put_multipart("prior", parts())
        with pytest.raises(RuntimeError, match="boom"):
            await paired.put_multipart("absent", parts())
    else:
        meta = store.put_multipart("empty", iter(()))
        with pytest.raises(RuntimeError, match="boom"):
            store.put_multipart("prior", parts())
        with pytest.raises(RuntimeError, match="boom"):
            store.put_multipart("absent", parts())
    assert meta.size == 0
    assert store.get("empty") == b""
    assert store.get("prior") == b"original"
    assert not store.exists("absent")


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


def test_object_store_open_reader(tmp_path: Path) -> None:
    """SyncObjectStore.open_reader returns a seekable SyncReadableFile."""
    store = SyncObjectStore.from_local(tmp_path)
    store.put("data.parquet", b"PAR1-parquet-content-PAR1")

    with store.open_reader("data.parquet") as f:
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

    with pytest.raises(PreconditionError) as sync_prec_exc:
        store.put("cfg.json", b'{"v":2}', if_match="non-matching-etag")
    assert isinstance(sync_prec_exc.value, VertebraError)
    assert isinstance(sync_prec_exc.value, OSError)
    assert not isinstance(sync_prec_exc.value, PermissionError)

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

    with pytest.raises(PreconditionError) as async_prec_exc:
        await async_store.put("cfg.json", b'{"v":2}', if_match="non-matching-etag")
    assert isinstance(async_prec_exc.value, VertebraError)
    assert isinstance(async_prec_exc.value, OSError)
    assert not isinstance(async_prec_exc.value, PermissionError)


@pytest.mark.anyio
async def test_native_ffi_zero_copy_bytes_mutable_bytearray_copy_and_strided_rejection() -> None:
    """Native _storage wraps immutable bytes zero-copy, copies mutable bytearray synchronously, and rejects strided views."""
    sync_native = native_st.SyncObjectStore("memory://ffi-sync")
    sync_native.put("imm.bin", b"immutable-payload")
    assert bytes(sync_native.get("imm.bin")) == b"immutable-payload"

    mutable = bytearray(b"hello-world")
    sync_native.put("mut.bin", mutable)
    mutable.clear()
    assert mutable == bytearray()
    assert bytes(sync_native.get("mut.bin")) == b"hello-world"

    mp = sync_native.create_multipart("mp-mut.bin")
    part_buf = bytearray(b"part-one")
    mp.write_part(part_buf)
    part_buf.clear()
    assert part_buf == bytearray()
    mp.complete()
    assert bytes(sync_native.get("mp-mut.bin")) == b"part-one"

    with pytest.raises(BufferError, match="C-contiguous"):
        sync_native.put("strided.bin", memoryview(b"abcdef")[::2])

    async_native = sync_native.to_async()
    async_buf = bytearray(b"async-payload")
    put_awaitable = async_native.put("async-mut.bin", async_buf)
    async_buf.clear()
    assert async_buf == bytearray()
    await put_awaitable
    assert bytes(await async_native.get("async-mut.bin")) == b"async-payload"

    with pytest.raises(BufferError, match="C-contiguous"):
        async_native.put("async-strided.bin", memoryview(b"abcdef")[::2])


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

    async with async_store.open_reader("payload.bin", chunk_size=16) as af:
        assert await af.read(0) == b""
        chunks = [chunk async for chunk in af]
        assert len(chunks) == 8
        assert b"".join(chunks) == payload
        assert af.tell() == len(payload)
        assert await af.read(8) == b""


def test_fail_closed_empty_or_missing_target_raises_improper_configuration() -> None:
    """SyncObjectStore and AsyncObjectStore reject a missing target with TypeError and None with ImproperConfigurationError."""
    with pytest.raises(TypeError):
        cast(Any, SyncObjectStore)()

    with pytest.raises(TypeError):
        cast(Any, AsyncObjectStore)()

    with pytest.raises(ImproperConfigurationError):
        cast(Any, SyncObjectStore)(None)

    with pytest.raises(ImproperConfigurationError):
        cast(Any, AsyncObjectStore)(None)


@pytest.mark.parametrize(
    "invalid_target", ["", "   ", "relative/path/without/scheme", "ftp://unsupported.example.com/bucket"]
)
@pytest.mark.parametrize("store_type", [SyncObjectStore, AsyncObjectStore])
def test_fail_closed_invalid_target_raises_improper_configuration(store_type: type[Any], invalid_target: str) -> None:
    """Both stores reject empty, blank, schemeless, and unsupported-scheme targets with ImproperConfigurationError."""
    with pytest.raises(ImproperConfigurationError):
        store_type(invalid_target)

    with pytest.raises(ImproperConfigurationError):
        SyncObjectStore(HTTPConfig(url=""))


def test_http_config_encodes_headers_under_json_headers_option() -> None:
    """HTTPConfig.to_url_and_options encodes custom headers under the `headers` JSON option."""
    empty_url, empty_opts = HTTPConfig(url="https://cdn.example.com/assets").to_url_and_options()
    assert empty_url == "https://cdn.example.com/assets"
    assert empty_opts == {}

    url, opts = HTTPConfig(
        url="https://cdn.example.com/assets",
        headers={"Authorization": "Bearer tok", "X-Env": "prod"},
    ).to_url_and_options()
    assert url == "https://cdn.example.com/assets"
    assert list(opts) == ["headers"]
    assert decode_json(opts["headers"]) == {"Authorization": "Bearer tok", "X-Env": "prod"}
    store = SyncObjectStore(
        HTTPConfig(url="https://cdn.example.com/assets", headers={"Authorization": "Bearer tok", "X-Env": "prod"})
    )
    assert store.scheme == "https"


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
    """A MemoryConfig store shares its native in-memory backend with handles derived through to_async and to_sync."""
    store = SyncObjectStore(MemoryConfig(namespace="t1"))

    store.put("k", b"v")
    assert store.get("k") == b"v"

    async_store = store.to_async()
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


@pytest.fixture
def storage_read_spy(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[tuple[int, int | None]]]:
    """Forbid full-object reads on `SyncObjectStore` and record bounded `get_range` / `get_stream` calls."""
    calls: dict[str, list[tuple[int, int | None]]] = {"get_range": [], "get_stream": []}
    original_range = SyncObjectStore.get_range
    original_stream = SyncObjectStore.get_stream

    def forbidden(self: SyncObjectStore, path: str) -> bytes:
        raise AssertionError(f"full-object read of {path}")

    def spy_range(self: SyncObjectStore, path: str, offset: int, length: int) -> bytes:
        calls["get_range"].append((offset, length))
        return original_range(self, path, offset, length)

    def spy_stream(self: SyncObjectStore, path: str, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]:
        calls["get_stream"].append((0, chunk_size))
        return original_stream(self, path, chunk_size=chunk_size)

    monkeypatch.setattr(SyncObjectStore, "get", forbidden)
    monkeypatch.setattr(SyncObjectStore, "get_view", forbidden)
    monkeypatch.setattr(SyncObjectStore, "get_range", spy_range)
    monkeypatch.setattr(SyncObjectStore, "get_stream", spy_stream)
    return calls


def test_storage_response_streams_full_body_in_bounded_chunks(
    storage_read_spy: dict[str, list[tuple[int, int | None]]],
) -> None:
    """A `200` response streams native chunks bounded by `chunk_size` without a full-object read."""
    store = SyncObjectStore("memory://")
    payload = os.urandom(256 * 1024)
    store.put("large.bin", payload)
    response = StreamingStorageResponse.from_sync_store(store, "large.bin")
    chunks = list(response.iter_chunks(chunk_size=65536))
    assert [len(chunk) for chunk in chunks] == [65536] * 4
    assert all(isinstance(chunk, memoryview) for chunk in chunks)
    assert b"".join(chunks) == payload
    assert storage_read_spy == {"get_range": [], "get_stream": [(0, 65536)]}


@pytest.mark.parametrize("explicit_chunk_size", [True, False])
def test_storage_response_range_reads_only_requested_windows(
    storage_read_spy: dict[str, list[tuple[int, int | None]]], explicit_chunk_size: bool
) -> None:
    """A `206` response reads bounded `get_range` windows for chunks and one exact slice for fastpath."""
    store = SyncObjectStore("memory://")
    payload = bytes(range(64))
    store.put("range.bin", payload)
    response = StreamingStorageResponse.from_sync_store(
        store, "range.bin", range_header="bytes=10-25", chunk_size=65536 if explicit_chunk_size else 6
    )
    chunks = list(response.iter_chunks(chunk_size=6) if explicit_chunk_size else response.iter_chunks())
    status, headers, body = response.to_fastpath_tuple()
    assert [bytes(chunk) for chunk in chunks] == [payload[10:16], payload[16:22], payload[22:26]]
    assert all(isinstance(chunk, memoryview) for chunk in chunks)
    assert status == 206
    assert bytes(body) == payload[10:26]
    assert dict(headers)[b"content-range"] == b"bytes 10-25/64"
    assert dict(headers)[b"content-length"] == b"16"
    assert storage_read_spy == {"get_range": [(10, 16), (10, 16)], "get_stream": []}


@pytest.mark.anyio
async def test_storage_response_async_iteration_over_sync_store_streams_ranges(
    storage_read_spy: dict[str, list[tuple[int, int | None]]],
) -> None:
    """`aiter_chunks()` over a sync store reuses bounded range windows instead of a full-object read."""
    store = SyncObjectStore("memory://")
    store.put("range.bin", b"0123456789")
    response = StreamingStorageResponse.from_sync_store(store, "range.bin", range_header="bytes=2-8", chunk_size=4)
    assert [bytes(chunk) async for chunk in response.aiter_chunks()] == [b"2345", b"678"]
    assert storage_read_spy == {"get_range": [(2, 7)], "get_stream": []}


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_response_multi_mib_range_reads_bounded_windows(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A multi-MiB `206` body issues one `get_range` per 4 MiB window while chunks stay within `chunk_size`."""
    store = SyncObjectStore("memory://")
    payload = os.urandom(9 * 1024 * 1024 + 4096)
    store.put("big.bin", payload)
    calls: list[tuple[int, int]] = []
    if asynchronous:
        original_async = AsyncObjectStore.get_range

        async def spy_async(self: AsyncObjectStore, path: str, offset: int, length: int) -> bytes:
            calls.append((offset, length))
            return await original_async(self, path, offset, length)

        monkeypatch.setattr(AsyncObjectStore, "get_range", spy_async)
        response = await StreamingStorageResponse.from_async_store(
            store.to_async(), "big.bin", range_header=f"bytes=100-{len(payload) - 1}", chunk_size=65536
        )
        chunks = [bytes(chunk) async for chunk in response.aiter_chunks()]
    else:
        original_sync = SyncObjectStore.get_range

        def spy_sync(self: SyncObjectStore, path: str, offset: int, length: int) -> bytes:
            calls.append((offset, length))
            return original_sync(self, path, offset, length)

        monkeypatch.setattr(SyncObjectStore, "get_range", spy_sync)
        response = StreamingStorageResponse.from_sync_store(
            store, "big.bin", range_header=f"bytes=100-{len(payload) - 1}", chunk_size=65536
        )
        chunks = [bytes(chunk) for chunk in response.iter_chunks()]
    selected = len(payload) - 100
    window = 4 * 1024 * 1024
    assert len(calls) == -(-selected // window) == 3
    assert calls == [(100, window), (100 + window, window), (100 + 2 * window, selected - 2 * window)]
    assert max(len(chunk) for chunk in chunks) == 65536
    assert b"".join(chunks) == payload[100:]


def test_storage_response_sync_direct_descriptor_resolves_unknown_size() -> None:
    """A direct sync descriptor without `total_size` resolves the size with HEAD and returns the full body."""
    store = SyncObjectStore("memory://")
    store.put("body.bin", b"0123456789")
    response = StreamingStorageResponse(store=store, key="body.bin", chunk_size=4)
    assert [bytes(chunk) for chunk in response.iter_chunks()] == [b"0123", b"4567", b"89"]
    status, headers, body = response.to_fastpath_tuple()
    assert status == 200
    assert bytes(body) == b"0123456789"
    assert dict(headers)[b"content-length"] == b"10"
    assert b"content-range" not in dict(headers)


@pytest.mark.parametrize(
    ("payload", "range_start", "range_end"),
    [(b"0123456789", 10, None), (b"0123456789", 99, 120), (b"0123456789", 6, 3), (b"", 0, None)],
    ids=["start-at-end", "start-past-end", "end-before-start", "empty-object"],
)
def test_storage_response_sync_direct_unsatisfiable_range_returns_416(
    storage_read_spy: dict[str, list[tuple[int, int | None]]],
    payload: bytes,
    range_start: int,
    range_end: int | None,
) -> None:
    """Direct sync descriptors with unsatisfiable ranges return `416` without reading the body."""
    store = SyncObjectStore("memory://")
    store.put("object.bin", payload)
    response = StreamingStorageResponse(store=store, key="object.bin", range_start=range_start, range_end=range_end)
    assert list(response.iter_chunks()) == []
    status, headers, body = response.to_fastpath_tuple()
    assert status == 416
    assert body == b""
    assert dict(headers)[b"content-range"] == f"bytes */{len(payload)}".encode()
    assert dict(headers)[b"content-length"] == b"0"
    assert storage_read_spy == {"get_range": [], "get_stream": []}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("payload", "range_start", "range_end"),
    [(b"0123456789", 10, None), (b"0123456789", 99, 120), (b"0123456789", 6, 3), (b"", 0, None)],
    ids=["start-at-end", "start-past-end", "end-before-start", "empty-object"],
)
async def test_storage_response_async_direct_unsatisfiable_range_returns_416(
    payload: bytes, range_start: int, range_end: int | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct async descriptors with unsatisfiable ranges return `416` without awaiting a range read."""
    store = AsyncObjectStore("memory://")
    await store.put("object.bin", payload)

    async def forbidden(self: AsyncObjectStore, path: str, offset: int, length: int) -> bytes:
        raise AssertionError(f"range read of {path} at {offset}+{length}")

    monkeypatch.setattr(AsyncObjectStore, "get_range", forbidden)
    response = StreamingStorageResponse(store=store, key="object.bin", range_start=range_start, range_end=range_end)
    assert [chunk async for chunk in response.aiter_chunks()] == []
    status, headers, body = await response.to_async_fastpath_tuple()
    assert status == 416
    assert body == b""
    assert dict(headers)[b"content-range"] == f"bytes */{len(payload)}".encode()
    assert dict(headers)[b"content-length"] == b"0"


@pytest.mark.anyio
async def test_storage_response_async_direct_range_clamps_end_to_object_size() -> None:
    """Async iteration clamps an inclusive `range_end` past the object end to the object size."""
    store = AsyncObjectStore("memory://")
    await store.put("body", b"0123456789")
    response = StreamingStorageResponse(store=store, key="body", range_start=3, range_end=99, chunk_size=3)
    assert [bytes(chunk) async for chunk in response.aiter_chunks()] == [b"345", b"678", b"9"]
    status, headers, body = await response.to_async_fastpath_tuple()
    assert status == 206
    assert bytes(body) == b"3456789"
    assert dict(headers)[b"content-range"] == b"bytes 3-9/10"


@pytest.mark.parametrize(
    ("payload", "range_header", "expected_status"),
    [(b"", None, 200), (b"", "bytes=0-", 416), (b"0123456789", "bytes=99-", 416)],
)
def test_storage_response_empty_and_unsatisfiable_skip_storage_reads(
    storage_read_spy: dict[str, list[tuple[int, int | None]]],
    payload: bytes,
    range_header: str | None,
    expected_status: int,
) -> None:
    """Empty objects and `416` responses yield no chunks and an empty body without touching the payload."""
    store = SyncObjectStore("memory://")
    store.put("object.bin", payload)
    response = StreamingStorageResponse.from_sync_store(store, "object.bin", range_header=range_header)
    status, headers, body = response.to_fastpath_tuple()
    assert list(response.iter_chunks()) == []
    assert status == expected_status
    assert body == b""
    assert dict(headers)[b"content-length"] == b"0"
    assert storage_read_spy == {"get_range": [], "get_stream": []}


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_storage_response_iter_chunks_rejects_non_positive_chunk_size(chunk_size: int) -> None:
    """Explicit non-positive chunk sizes fail before any storage read."""
    response = StreamingStorageResponse(data=b"body")
    with pytest.raises(ValueError, match="chunk_size must be positive"):
        list(response.iter_chunks(chunk_size=chunk_size))


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


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("url", "options", "expected_atomic"),
    [
        ("memory://", None, True),
        ("oci://b@ns/prefix", {"region": "us-ashburn-1", "endpoint": "http://127.0.0.1:9"}, True),
        (
            "s3://b",
            {
                "region": "us-east-1",
                "access_key_id": "test-token-placeholder",
                "secret_access_key": "test-token-placeholder",
                "endpoint": "http://127.0.0.1:9",
                "allow_http": "true",
            },
            False,
        ),
        (
            "r2://b",
            {
                "account_id": "acct",
                "access_key_id": "test-token-placeholder",
                "secret_access_key": "test-token-placeholder",
                "endpoint": "http://127.0.0.1:9",
                "allow_http": "true",
            },
            False,
        ),
        ("gs://b", {"skip_signature": "true", "endpoint": "http://127.0.0.1:9", "allow_http": "true"}, False),
        (
            "az://c",
            {
                "account": "devstoreaccount1",
                "access_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
                "endpoint": "http://127.0.0.1:9/devstoreaccount1",
                "allow_http": "true",
            },
            False,
        ),
        ("http://127.0.0.1:9", {"allow_http": "true"}, False),
    ],
)
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_supports_atomic_rename_and_atomic_only_guard(
    url: str,
    options: dict[str, str] | None,
    expected_atomic: bool,
    asynchronous: bool,
) -> None:
    """Verify supports_atomic_rename and atomic_only=True fail-loud guards across sync and async stores."""
    if asynchronous:
        async_store = AsyncObjectStore(url, options=options)
        assert async_store.supports_atomic_rename is expected_atomic
        if url.startswith("memory://"):
            await async_store.put("a.txt", b"data")
            await async_store.rename("a.txt", "b.txt", atomic_only=True)
            assert await async_store.get("b.txt") == b"data"
        elif not expected_atomic:
            with pytest.raises(ValueError, match="atomic rename is not supported"):
                await async_store.rename("a.txt", "b.txt", atomic_only=True)
        return

    sync_store = SyncObjectStore(url, options=options)
    assert sync_store.supports_atomic_rename is expected_atomic
    if url.startswith("memory://"):
        sync_store.put("a.txt", b"data")
        sync_store.rename("a.txt", "b.txt", atomic_only=True)
        assert sync_store.get("b.txt") == b"data"
    elif not expected_atomic:
        with pytest.raises(ValueError, match="atomic rename is not supported"):
            sync_store.rename("a.txt", "b.txt", atomic_only=True)


def test_local_store_supports_atomic_rename_and_syncs_get_file(tmp_path: Path) -> None:
    """Verify LocalConfig supports atomic_only rename and durable get_file downloads without .part leftovers."""
    store = SyncObjectStore(LocalConfig(root=tmp_path))
    assert store.supports_atomic_rename is True
    store.put("src/item.bin", b"durable-bytes")
    store.rename("src/item.bin", "dst/item.bin", atomic_only=True)
    dest = tmp_path / "dl" / "item.bin"
    store.get_file("dst/item.bin", str(dest))
    assert dest.read_bytes() == b"durable-bytes"
    assert not list((tmp_path / "dl").glob("*.part"))
