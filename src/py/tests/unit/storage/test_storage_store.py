"""Unit tests for vertebra.storage (SyncObjectStore, AsyncObjectStore, zero-copy memoryview, and V4 presigning)."""

import array
import ast
import asyncio
import contextlib
import gc
import gzip
import io
import os
import sys
import tempfile
import threading
import warnings
from collections.abc import AsyncIterator, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree

import pytest

import vertebra._storage as native_st
import vertebra.storage as storage
from vertebra._storage import StorageBuffer
from vertebra.db.arrow import ArrowCursor
from vertebra.storage import (
    AsyncObjectStore,
    AsyncReadableFile,
    AsyncWritableFile,
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
    SyncWritableFile,
    _arrow,
    parse_byte_range,
)
from vertebra.storage._arrow import (
    iter_batches_async,
    read_table_async,
    read_table_sync,
    write_batches_async,
    write_batches_sync,
)
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    PreconditionError,
    VertebraError,
    reset_dependency_cache,
)


@contextlib.contextmanager
def _s3_multipart_completion_server() -> Iterator[tuple[S3Config, dict[int, bytes], list[bytes]]]:
    """Enforce S3's non-final part minimum on actual native HTTP uploads."""
    parts: dict[int, bytes] = {}
    completed: list[bytes] = []

    class CompletionHandler(BaseHTTPRequestHandler):
        def respond(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query, keep_blank_values=True)
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if parsed.path != "/codec-fixture/output":
                self.send_error(404)
            elif "uploads" in query:
                self.respond(
                    200,
                    b"<InitiateMultipartUploadResult><Bucket>codec-fixture</Bucket><Key>output</Key>"
                    b"<UploadId>fixture</UploadId></InitiateMultipartUploadResult>",
                )
            elif query.get("uploadId") == ["fixture"]:
                numbers = [int(part.findtext("PartNumber", "0")) for part in ElementTree.fromstring(body)]
                if any(len(parts[number]) < 5 * 1024 * 1024 for number in numbers[:-1]):
                    self.respond(
                        400, b"<Error><Code>EntityTooSmall</Code><Message>non-final part below 5 MiB</Message></Error>"
                    )
                    return
                completed.append(b"".join(parts[number] for number in numbers))
                self.respond(
                    200,
                    b"<CompleteMultipartUploadResult><Bucket>codec-fixture</Bucket><Key>output</Key>"
                    b'<ETag>"complete"</ETag></CompleteMultipartUploadResult>',
                )
            else:
                self.send_error(400)

        def do_PUT(self) -> None:
            query = parse_qs(urlsplit(self.path).query)
            number = int(query["partNumber"][0])
            parts[number] = self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("ETag", f'"part-{number}"')
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_DELETE(self) -> None:
            self.send_response(204)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            _ = (format, args)

    server = ThreadingHTTPServer(("127.0.0.1", 0), CompletionHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield (
            S3Config(
                bucket="codec-fixture",
                region="us-east-1",
                endpoint=f"http://127.0.0.1:{server.server_port}",
                access_key_id="test",
                secret_access_key="test",
                allow_http=True,
            ),
            parts,
            completed,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("async_write", [False, True])
@pytest.mark.parametrize("large", [False, True])
async def test_s3_codec_writers_complete_valid_multipart(file_format: str, async_write: bool, large: bool) -> None:
    """Native multipart coalescing makes real S3 codec output publish successfully."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"payload": [b"x" * (9 * 1024 * 1024)]}) if large else pa.table({"id": range(250)})
    with _s3_multipart_completion_server() as (config, parts, completed):
        store: Any = AsyncObjectStore(config) if async_write else SyncObjectStore(config)
        operation = (
            store.write_parquet("output", table, compression="NONE", row_group_size=100)
            if file_format == "parquet"
            else store.write_arrow("output", table, batch_size=100)
        )
        result = await operation if async_write else operation
    assert isinstance(result, PutResult)
    assert result.path == "output"
    assert result.etag.strip('"') == "complete"
    assert len(completed) == 1
    assert result.size == len(completed[0])
    sizes = [len(parts[number]) for number in sorted(parts)]
    assert all(size == 8 * 1024 * 1024 for size in sizes[:-1])
    assert 0 < sizes[-1] <= 8 * 1024 * 1024
    assert len(sizes) == (2 if large else 1)
    decoded = (
        pa.parquet.read_table(pa.BufferReader(completed[0]))
        if file_format == "parquet"
        else pa.ipc.open_stream(pa.BufferReader(completed[0])).read_all()
    )
    assert decoded.equals(table)


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


def test_object_store_open_reader(tmp_path: Path) -> None:
    """SyncObjectStore.open_reader returns a seekable SyncReadableFile and wrappers are removed."""
    assert not hasattr(storage, "SyncStore")
    assert not hasattr(storage, "AsyncStore")
    assert not hasattr(storage, "StorageRegistry")
    assert not hasattr(storage, "UploadTracker")

    store = SyncObjectStore.from_local(tmp_path)
    store.put("data.parquet", b"PAR1-parquet-content-PAR1")

    with store.open_reader("data.parquet") as f:
        assert isinstance(f, SyncReadableFile)
        assert f.size == 25
        assert f.read(4) == b"PAR1"
        assert f.tell() == 4
        f.seek(-4, 2)
        assert f.read(4) == b"PAR1"


@pytest.mark.parametrize("store_type", [SyncObjectStore, AsyncObjectStore])
@pytest.mark.parametrize("attribute", ["open_readable_file", "_get_native_module"])
def test_object_stores_expose_no_reader_or_native_module_shims(store_type: type[Any], attribute: str) -> None:
    """Both stores expose ``open_reader`` exclusively and resolve the native module without a shim."""
    assert hasattr(store_type, "open_reader")
    assert not hasattr(store_type, attribute)


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


def _parquet_column_encodings(store: SyncObjectStore, key: str) -> dict[str, set[str]]:
    """Return the encodings recorded for each column of the first row group."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    row_group = pq.ParquetFile(pa.BufferReader(store.get(key))).metadata.row_group(0)
    return {
        row_group.column(index).path_in_schema: set(row_group.column(index).encodings)
        for index in range(row_group.num_columns)
    }


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("use_dictionary", "use_byte_stream_split", "dictionary_columns", "split_columns"),
    [
        (False, ["metric"], set(), {"metric"}),
        (["tag"], False, {"tag"}, set()),
        ("tag", False, {"tag"}, set()),
        (False, "metric", set(), {"metric"}),
        (True, True, {"id", "tag", "metric"}, set()),
    ],
    ids=["split-list", "dictionary-list", "dictionary-str", "split-str", "all-columns"],
)
async def test_storage_parquet_encoding_options_reach_column_metadata(
    asynchronous: bool,
    use_dictionary: bool | str | list[str],
    use_byte_stream_split: bool | str | list[str],
    dictionary_columns: set[str],
    split_columns: set[str],
) -> None:
    """Dictionary and byte-stream-split choices are recorded per column in the written footer."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "id": pa.array(range(64), type=pa.int64()),
            "tag": [f"tag-{index % 4}" for index in range(64)],
            "metric": [index / 7 for index in range(64)],
        }
    )
    store = SyncObjectStore("memory://")
    if asynchronous:
        await store.to_async().write_parquet(
            "t.parquet", table, use_dictionary=use_dictionary, use_byte_stream_split=use_byte_stream_split
        )
        batches = [batch async for batch in store.to_async().iter_parquet_batches("t.parquet")]
    else:
        store.write_parquet(
            "t.parquet", table, use_dictionary=use_dictionary, use_byte_stream_split=use_byte_stream_split
        )
        batches = list(store.iter_parquet_batches("t.parquet"))
    encodings = _parquet_column_encodings(store, "t.parquet")
    assert {name for name, values in encodings.items() if "RLE_DICTIONARY" in values} == dictionary_columns
    assert {name for name, values in encodings.items() if "BYTE_STREAM_SPLIT" in values} == split_columns
    assert pa.Table.from_batches(batches).equals(table)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_parquet_options_override_explicit_keywords(asynchronous: bool) -> None:
    """Writer options replace compression and rowgroup defaults without duplicate keyword errors."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": pa.array(range(10), type=pa.int64())})
    store = SyncObjectStore("memory://")
    options = {"compression": "snappy", "row_group_size": 4, "use_dictionary": False}
    if asynchronous:
        await store.to_async().write_parquet("o.parquet", table, compression="zstd", options=options)
    else:
        store.write_parquet("o.parquet", table, compression="zstd", options=options)
    metadata = pq.ParquetFile(pa.BufferReader(store.get("o.parquet"))).metadata
    assert metadata.num_row_groups == 3
    assert metadata.row_group(0).column(0).compression == "SNAPPY"
    assert "RLE_DICTIONARY" not in metadata.row_group(0).column(0).encodings


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_parquet_reader_options_select_row_groups(asynchronous: bool) -> None:
    """Reader options forward rowgroup selection to demand-paged Parquet iteration."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": pa.array(range(30), type=pa.int64())})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=10)
    store = SyncObjectStore("memory://")
    store.put("groups.parquet", sink.getvalue())
    options = {"row_groups": [1], "use_threads": False}
    if asynchronous:
        batches = [batch async for batch in store.to_async().iter_parquet_batches("groups.parquet", options=options)]
    else:
        batches = list(store.iter_parquet_batches("groups.parquet", options=options))
    assert pa.Table.from_batches(batches).equals(table.slice(10, 10))


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_arrow_ipc_compression_options_round_trip(asynchronous: bool) -> None:
    """IPC write options compress record batch bodies and read options decode them in bounded slices."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([7] * 20_000, type=pa.int64())})
    store = SyncObjectStore("memory://")
    read_options = {"use_threads": False}
    if asynchronous:
        async_store = store.to_async()
        await async_store.write_arrow("plain.arrow", table)
        await async_store.write_arrow("t.arrow", table, options={"compression": "zstd"})
        batches = [
            batch async for batch in async_store.iter_arrow_batches("t.arrow", batch_size=8_000, options=read_options)
        ]
    else:
        store.write_arrow("plain.arrow", table)
        store.write_arrow("t.arrow", table, options={"compression": "zstd"})
        batches = list(store.iter_arrow_batches("t.arrow", batch_size=8_000, options=read_options))
    assert store.head("t.arrow").size * 10 < store.head("plain.arrow").size
    assert [batch.num_rows for batch in batches] == [8_000, 8_000, 4_000]
    assert pa.Table.from_batches(batches).equals(table)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.anyio
async def test_storage_arrow_ipc_options_reject_conflicting_write_options(asynchronous: bool) -> None:
    """A prebuilt IpcWriteOptions cannot be combined with loose IPC option keys."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": [1]})
    store = SyncObjectStore("memory://")
    options = {"options": pa.ipc.IpcWriteOptions(), "compression": "zstd"}
    with pytest.raises(ValueError, match="IpcWriteOptions"):
        if asynchronous:
            await store.to_async().write_arrow("conflict.arrow", table, options=options)
        else:
            store.write_arrow("conflict.arrow", table, options=options)
    assert not store.exists("conflict.arrow")


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("wrapper", ["pyarrow-buffer", "memoryview", "bytes", "bytearray"])
async def test_storage_writers_accept_raw_ipc_buffers(asynchronous: bool, file_format: str, wrapper: str) -> None:
    """Serialized IPC streams passed as raw buffers decode directly without a buffer_view wrapper."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64()), "tag": ["a", "b", "c"]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    ipc_buffer = sink.getvalue()
    payload: Any = {
        "pyarrow-buffer": ipc_buffer,
        "memoryview": memoryview(ipc_buffer),
        "bytes": ipc_buffer.to_pybytes(),
        "bytearray": bytearray(ipc_buffer.to_pybytes()),
    }[wrapper]
    store = SyncObjectStore("memory://")
    async_store = store.to_async()
    if file_format == "parquet":
        if asynchronous:
            await async_store.write_parquet("from_buf", payload)
        else:
            store.write_parquet("from_buf", payload)
        batches = list(store.iter_parquet_batches("from_buf"))
    else:
        if asynchronous:
            await async_store.write_arrow("from_buf", payload)
        else:
            store.write_arrow("from_buf", payload)
        batches = list(store.iter_arrow_batches("from_buf"))
    assert pa.Table.from_batches(batches).equals(table)


@pytest.mark.anyio
@pytest.mark.parametrize(("options", "expected"), [(None, True), ({"pre_buffer": False}, False)])
async def test_storage_parquet_table_reads_default_to_pre_buffer(
    options: dict[str, Any] | None, expected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Materialized Parquet reads coalesce ranges by default while honoring an explicit override."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": [1, 2]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    store = SyncObjectStore("memory://")
    store.put("table.parquet", sink.getvalue())
    seen: list[bool] = []
    original = pq.read_table

    def observe_read(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["pre_buffer"])
        return original(*args, **kwargs)

    monkeypatch.setattr(pq, "read_table", observe_read)
    assert _arrow.read_table_sync(store, "table.parquet", file_format="parquet", options=options).equals(table)
    assert (
        await _arrow.read_table_async(store.to_async(), "table.parquet", file_format="parquet", options=options)
    ).equals(table)
    assert seen == [expected, expected]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_arrow_stream_batches_decode_bodies_through_read_buffer(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Arrow IPC batch iteration decodes message bodies via `read_buffer`, leaving `read` for length prefixes."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array(range(4096), type=pa.int64()), "tag": [f"t{i % 9}" for i in range(4096)]})
    store = SyncObjectStore("memory://")
    store.write_arrow("stream.arrow", table, batch_size=512)
    read_sizes: list[int] = []
    buffer_sizes: list[int] = []
    original_read = _arrow._StreamFile.read
    original_read_buffer = _arrow._StreamFile.read_buffer

    def observe_read(self: Any, size: int | None = -1) -> bytes:
        read_sizes.append(-1 if size is None else size)
        return original_read(self, size)

    def observe_read_buffer(self: Any, nbytes: int) -> memoryview | bytes:
        buffer_sizes.append(nbytes)
        return original_read_buffer(self, nbytes)

    monkeypatch.setattr(_arrow._StreamFile, "read", observe_read)
    monkeypatch.setattr(_arrow._StreamFile, "read_buffer", observe_read_buffer)
    if asynchronous:
        batches = [batch async for batch in store.to_async().iter_arrow_batches("stream.arrow", batch_size=512)]
    else:
        batches = list(store.iter_arrow_batches("stream.arrow", batch_size=512))
    assert pa.Table.from_batches(batches).equals(table)
    assert sum(buffer_sizes) >= table.nbytes
    assert read_sizes
    assert max(read_sizes) <= 8


@pytest.mark.parametrize(
    ("chunks", "requests", "expected", "zero_copy"),
    [
        ([b"abcdefgh"], [3, 3, 3], [b"abc", b"def", b"gh"], [True, True, True]),
        ([b"abc", b"defg", b"hi"], [5, 4, 1], [b"abcde", b"fghi", b""], [False, False, False]),
        ([], [4], [b""], [False]),
    ],
    ids=["within-chunk", "spanning-chunks", "empty-stream"],
)
def test_storage_stream_file_read_buffer_slices_pending_chunk(
    chunks: list[bytes], requests: list[int], expected: list[bytes], zero_copy: list[bool]
) -> None:
    """`read_buffer` returns a view over the pending native chunk when it fits and one copy when it spans chunks."""
    pending = iter(chunks)
    stream = _arrow._StreamFile(lambda: next(pending, None))
    results = [stream.read_buffer(size) for size in requests]
    assert [bytes(result) for result in results] == expected
    assert [isinstance(result, memoryview) for result in results] == zero_copy
    assert stream.tell() == sum(len(chunk) for chunk in chunks)


def test_storage_arrow_module_layout_exports_public_codecs_first() -> None:
    """The codec module exports only its public seams, defines them first, and slots its helpers."""
    module_file = Path(_arrow.__file__)
    tree = ast.parse(module_file.read_text(encoding="utf-8"), filename=str(module_file))
    definitions = [
        node.name for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
    ]
    public = [name for name in definitions if not name.startswith("_")]
    assert _arrow.__all__ == (
        "iter_batches_async",
        "iter_batches_sync",
        "read_table_async",
        "read_table_sync",
        "write_batches_async",
        "write_batches_sync",
    )
    assert sorted(public) == list(_arrow.__all__)
    assert definitions[: len(public)] == public
    plain_helpers = (_arrow._CodecBridge, _arrow._BatchReader, _arrow._BatchWriter)
    io_helpers = (_arrow._RangeFile, _arrow._StreamFile, _arrow._OutputSink)
    assert all("__slots__" in vars(helper) for helper in (*plain_helpers, *io_helpers))
    assert not hasattr(_arrow._BatchReader(), "__dict__")
    assert not hasattr(_arrow._BatchWriter(), "__dict__")


@pytest.mark.anyio
async def test_storage_arrow_codec_bridge_instances_have_no_instance_dict() -> None:
    """The loop-bound codec bridge stores state only in declared slots."""
    assert not hasattr(_arrow._CodecBridge(), "__dict__")


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


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("key", ["sync", "async"])
async def test_storage_arrow_writers_roundtrip(file_format: str, key: str) -> None:
    """Public writers return native metadata and preserve batches and rowgroups for each writer path."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": range(250)})
    store = SyncObjectStore("memory://")
    if file_format == "parquet":
        result = store.write_parquet("sync", table, row_group_size=100)
        async_result = await store.to_async().write_parquet("async", table, row_group_size=100)
    else:
        result = store.write_arrow("sync", table, batch_size=100)
        async_result = await store.to_async().write_arrow("async", table, batch_size=100)
    assert isinstance(result, PutResult)
    assert isinstance(async_result, PutResult)
    assert result.size > 0
    assert async_result.size == result.size
    payload = pa.BufferReader(store.get(key))
    if file_format == "parquet":
        reader = pq.ParquetFile(payload)
        assert [reader.metadata.row_group(i).num_rows for i in range(3)] == [100, 100, 50]
        assert reader.read().equals(table)
    else:
        with pa.ipc.open_stream(payload) as reader:
            batches = list(reader)
        assert [batch.num_rows for batch in batches] == [100, 100, 50]
        assert pa.Table.from_batches(batches).equals(table)


class _ObservedArrowUpload:
    """Record completion while delegating every operation to a real native upload."""

    def __init__(self, native: Any, events: list[tuple[str, int]], asynchronous: bool) -> None:
        self.native = native
        self.events = events
        self.asynchronous = asynchronous

    def write_part(self, chunk: Any) -> Any:
        self.events.append(("start", len(chunk)))
        result = self.native.write_part(chunk)
        if self.asynchronous:

            async def complete() -> None:
                await result
                self.events.append(("part", len(chunk)))

            return complete()
        self.events.append(("part", len(chunk)))
        return result

    def complete(self) -> Any:
        self.events.append(("complete", 0))
        return self.native.complete()

    def abort(self) -> Any:
        self.events.append(("abort", 0))
        return self.native.abort()


class _ObservedArrowStore:
    """Observe native multipart sessions without replacing the public store helper."""

    def __init__(self, native: Any, events: list[tuple[str, int]], asynchronous: bool) -> None:
        self.native = native
        self.events = events
        self.asynchronous = asynchronous

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native, name)

    def create_multipart(self, *args: Any, **kwargs: Any) -> Any:
        upload = self.native.create_multipart(*args, **kwargs)
        self.events.append(("create", 0))
        if self.asynchronous:

            async def complete() -> Any:
                return _ObservedArrowUpload(await upload, self.events, True)

            return complete()
        return _ObservedArrowUpload(upload, self.events, False)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("fail", [False, True])
async def test_storage_arrow_writer_demand_and_abort(file_format: str, asynchronous: bool, fail: bool) -> None:
    """The first batch finishes native writes before the producer advances or fails."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"id": [1, 2]})
    events: list[tuple[str, int]] = []
    closed: list[bool] = []
    base = SyncObjectStore("memory://")
    paired = base.to_async() if asynchronous else base
    native = _ObservedArrowStore(paired._require_native(), events, asynchronous)
    store = (
        AsyncObjectStore("memory://", _native_instance=native)
        if asynchronous
        else SyncObjectStore("memory://", _native_instance=native)
    )

    def batches() -> Iterator[Any]:
        try:
            yield batch
            assert sum(size for kind, size in events if kind == "part") > 20
            if fail:
                raise ValueError("producer failed")
            yield batch
        finally:
            closed.append(True)

    async def write() -> Any:
        if asynchronous:
            return await (
                store.write_parquet("output", batches())
                if file_format == "parquet"
                else store.write_arrow("output", batches())
            )
        return (
            store.write_parquet("output", batches())
            if file_format == "parquet"
            else store.write_arrow("output", batches())
        )

    if fail:
        with pytest.raises(ValueError, match="producer failed"):
            await write()
        assert ("abort", 0) in events
        assert ("complete", 0) not in events
        assert not base.exists("output")
    else:
        assert isinstance(await write(), PutResult)
        iterator = (
            base.iter_parquet_batches("output") if file_format == "parquet" else base.iter_arrow_batches("output")
        )
        assert pa.Table.from_batches(list(iterator)).equals(pa.Table.from_batches([batch, batch]))
    assert closed == [True]


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("input_kind", ["empty-table", "batch", "cursor", "empty-iterable"])
@pytest.mark.parametrize("key", ["sync", "async"])
async def test_storage_arrow_writer_input_contracts(
    file_format: str, input_kind: str, key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tables, batches, cursors and explicit empty schemas retain Arrow values through each writer path."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([] if input_kind.startswith("empty") else [1, 2], type=pa.int64())})
    cursor = ArrowCursor.from_rows([{"id": 1}, {"id": 2}]) if input_kind == "cursor" else None
    if cursor is not None:

        def reject_rows(*args: Any, **kwargs: Any) -> Any:
            pytest.fail("Arrow writers must decode the cursor buffer without Python rows")

        monkeypatch.setattr(ArrowCursor, "to_pylist", reject_rows)
        monkeypatch.setattr(ArrowCursor, "to_arrow_table", reject_rows)
    data = (
        cursor
        if cursor is not None
        else table.to_batches()[0]
        if input_kind == "batch"
        else iter(())
        if input_kind == "empty-iterable"
        else table
    )
    store = SyncObjectStore("memory://")
    try:
        write_batches_sync(store, "sync", data, file_format=file_format, schema=table.schema, batch_size=1)
        await write_batches_async(
            store.to_async(), "async", data, file_format=file_format, schema=table.schema, batch_size=1
        )
        batches = store.iter_parquet_batches(key) if file_format == "parquet" else store.iter_arrow_batches(key)
        assert pa.Table.from_batches(list(batches), schema=table.schema).equals(table)
    finally:
        if cursor is not None:
            cursor.close()


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("case", ["zero-size", "empty", "schema-mismatch", "bad-option", "missing-codec"])
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_arrow_writer_rejections(
    file_format: str, case: str, asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalid inputs preserve their errors and never commit an object."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"id": [1]})
    store = SyncObjectStore("memory://")
    options = {"unsupported_codec_option": True} if case == "bad-option" else None
    data = [] if case == "empty" else [batch, pa.record_batch({"other": [2]})] if case == "schema-mismatch" else [batch]
    error = MissingDependencyError if case == "missing-codec" else TypeError if case == "bad-option" else ValueError
    with monkeypatch.context() as context:
        if case == "missing-codec":
            context.setitem(sys.modules, "pyarrow", None)
            reset_dependency_cache()
        with pytest.raises(error):
            if asynchronous:
                await write_batches_async(
                    store.to_async(),
                    "missing",
                    iter(data),
                    file_format=file_format,
                    batch_size=0 if case == "zero-size" else 2,
                    options=options,
                )
            else:
                write_batches_sync(
                    store,
                    "missing",
                    iter(data),
                    file_format=file_format,
                    batch_size=0 if case == "zero-size" else 2,
                    options=options,
                )
    reset_dependency_cache()
    assert not store.exists("missing")


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_writer_native_network_cancellation(file_format: str) -> None:
    """Cancel an actual native multipart request and observe abort before returning."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"payload": [b"x" * (9 * 1024 * 1024)]})
    entered = threading.Event()
    release = threading.Event()
    aborted = threading.Event()
    requests: list[tuple[str, str]] = []
    closed: list[bool] = []

    class MultipartHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(("POST", self.path))
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if "uploads" in self.path:
                body = b"<InitiateMultipartUploadResult><Bucket>codec-fixture</Bucket><Key>output</Key><UploadId>fixture</UploadId></InitiateMultipartUploadResult>"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(500, "unexpected completion")

        def do_PUT(self) -> None:
            requests.append(("PUT", self.path))
            self.rfile.read(int(self.headers["Content-Length"]))
            entered.set()
            release.wait(10)
            self.send_response(200)
            self.send_header("ETag", '"part"')
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_DELETE(self) -> None:
            requests.append(("DELETE", self.path))
            aborted.set()
            self.send_response(204)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            _ = (format, args)

    server = ThreadingHTTPServer(("127.0.0.1", 0), MultipartHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = S3Config(
        bucket="codec-fixture",
        region="us-east-1",
        endpoint=f"http://127.0.0.1:{server.server_port}",
        access_key_id="test",
        secret_access_key="test",
        allow_http=True,
    )
    native_store = AsyncObjectStore(config)
    events: list[tuple[str, int]] = []
    store = AsyncObjectStore(config, _native_instance=_ObservedArrowStore(native_store._require_native(), events, True))

    def batches() -> Iterator[Any]:
        try:
            yield batch
            yield batch
        finally:
            closed.append(True)

    before = asyncio.all_tasks()
    operation = (
        store.write_parquet("output", batches(), compression="NONE")
        if file_format == "parquet"
        else store.write_arrow("output", batches())
    )
    task = asyncio.create_task(operation)
    try:
        async with asyncio.timeout(10):
            while not entered.is_set():
                if task.done():
                    await task
                await asyncio.sleep(0.001)
        assert await asyncio.create_task(asyncio.sleep(0, result="responsive")) == "responsive"
        assert sum(kind == "start" for kind, _ in events) > sum(kind == "part" for kind, _ in events)
        assert ("complete", 0) not in events
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert aborted.is_set()
        assert closed == [True]
        assert any(method == "DELETE" and "uploadId=fixture" in path for method, path in requests)
        assert all("uploads" in path for method, path in requests if method == "POST")
        assert not (asyncio.all_tasks() - before)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_writer_dictionary_state(file_format: str) -> None:
    """Persistent writers preserve dictionary batches and native media metadata."""
    pa = pytest.importorskip("pyarrow")
    labels = pa.DictionaryArray.from_arrays(pa.array([0, 1]), pa.array(["one", "two"]))
    batch = pa.record_batch([labels], names=["label"])
    store = SyncObjectStore("memory://")
    async_store = store.to_async()
    if file_format == "parquet":
        await async_store.write_parquet("dictionary", iter([batch, batch]), row_group_size=2)
        result = list(store.iter_parquet_batches("dictionary", batch_size=2))
    else:
        await async_store.write_arrow("dictionary", iter([batch, batch]), batch_size=2)
        result = list(store.iter_arrow_batches("dictionary"))
    assert [item.column(0).to_pylist() for item in result] == [["one", "two"], ["one", "two"]]
    expected = "application/vnd.apache.parquet" if file_format == "parquet" else "application/vnd.apache.arrow.stream"
    assert store.head("dictionary").content_type == expected


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("cancellations", [1, 2, 3])
async def test_storage_arrow_writer_waits_for_cancelled_codec_worker(file_format: str, cancellations: int) -> None:
    """Cancellation retains the sink and producer until the active codec step exits."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"id": [1]})
    entered = threading.Event()
    release = threading.Event()
    closed: list[bool] = []
    store = SyncObjectStore("memory://")

    def batches() -> Iterator[Any]:
        try:
            yield batch
            entered.set()
            if not release.wait(10):
                raise TimeoutError("codec barrier was not released")
            yield batch
        finally:
            closed.append(True)

    async_store = store.to_async()
    operation = (
        async_store.write_parquet("cancelled", batches())
        if file_format == "parquet"
        else async_store.write_arrow("cancelled", batches())
    )
    task = asyncio.create_task(operation)
    try:
        async with asyncio.timeout(5):
            while not entered.is_set():
                if task.done():
                    await task
                await asyncio.sleep(0.001)
        for _ in range(cancellations):
            task.cancel()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        assert not task.done()
        assert closed == []
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert closed == [True]
        assert not store.exists("cancelled")
    finally:
        release.set()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


class _RecordingUpload:
    """Record part lengths while delegating every multipart operation to a real native upload."""

    def __init__(self, native: Any, parts: list[int]) -> None:
        self.native = native
        self.parts = parts

    def write_part(self, chunk: Any) -> Any:
        self.parts.append(memoryview(chunk).nbytes)
        return self.native.write_part(chunk)

    def complete(self) -> Any:
        return self.native.complete()

    def abort(self) -> Any:
        return self.native.abort()


class _RecordingNative:
    """Record payload objects and multipart parts forwarded to a real native store."""

    def __init__(self, native: Any, asynchronous: bool) -> None:
        self.native = native
        self.asynchronous = asynchronous
        self.payloads: list[Any] = []
        self.parts: list[int] = []
        self.uploads = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native, name)

    def put(self, key: str, data: Any, **kwargs: Any) -> Any:
        self.payloads.append(data)
        return self.native.put(key, data, **kwargs)

    def put_multipart(self, key: str, parts: list[Any]) -> Any:
        self.payloads.extend(parts)
        return self.native.put_multipart(key, parts)

    def create_multipart(self, *args: Any, **kwargs: Any) -> Any:
        upload = self.native.create_multipart(*args, **kwargs)
        self.uploads += 1
        if self.asynchronous:

            async def resolve() -> Any:
                return _RecordingUpload(await upload, self.parts)

            return resolve()
        return _RecordingUpload(upload, self.parts)


def _recording_store(namespace: str, asynchronous: bool) -> tuple[Any, SyncObjectStore, _RecordingNative]:
    """Build a store whose native backend records forwarded payloads, plus a sync verifier on the same namespace."""
    base = SyncObjectStore(MemoryConfig(namespace=namespace))
    paired = base.to_async() if asynchronous else base
    native = _RecordingNative(paired._require_native(), asynchronous)
    store: Any = (
        AsyncObjectStore(MemoryConfig(namespace=namespace), _native_instance=native)
        if asynchronous
        else SyncObjectStore(MemoryConfig(namespace=namespace), _native_instance=native)
    )
    return store, base, native


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_forwards_buffers_without_memoryview_wrapping(asynchronous: bool) -> None:
    """put and put_multipart hand caller buffers straight to native so bytes stay zero-copy."""
    store, base, native = _recording_store(f"passthrough-{asynchronous}", asynchronous)
    payload = b"immutable-payload"
    mutable = bytearray(b"mutable-payload")
    parts = [b"part-a:", bytearray(b"part-b")]
    typed = array.array("I", [1, 2, 3])

    if asynchronous:
        first = await store.put("imm.bin", payload)
        await store.put("mut.bin", mutable)
        mutable.clear()
        multi = await store.put_multipart("multi.bin", parts)
        typed_meta = await store.put("typed.bin", typed)
    else:
        first = store.put("imm.bin", payload)
        store.put("mut.bin", mutable)
        mutable.clear()
        multi = store.put_multipart("multi.bin", parts)
        typed_meta = store.put("typed.bin", typed)

    assert native.payloads[0] is payload
    assert native.payloads[2] is parts[0]
    assert native.payloads[3] is parts[1]
    assert not any(isinstance(item, memoryview) for item in native.payloads[:4])
    assert first.size == len(payload)
    assert first.etag
    assert multi.size == 13
    assert typed_meta.size == typed.itemsize * 3
    assert mutable == bytearray()
    assert base.get("mut.bin") == b"mutable-payload"
    assert base.get("multi.bin") == b"part-a:part-b"
    assert base.get("typed.bin") == typed.tobytes()


_READ_AHEAD_OBJECT = bytes(range(256)) * 4096


@pytest.mark.parametrize(("read_ahead", "after_reads", "after_seek"), [(4_194_304, 1, 1), (0, 16, 17)])
def test_sync_reader_read_ahead_coalesces_range_calls(
    monkeypatch: pytest.MonkeyPatch, read_ahead: int, after_reads: int, after_seek: int
) -> None:
    """Sixteen 4 KiB reads plus a backward seek share one cached window unless read-ahead is disabled."""
    store = SyncObjectStore(MemoryConfig(namespace=f"read-ahead-sync-{read_ahead}"))
    store.put("blob.bin", _READ_AHEAD_OBJECT)
    calls: list[tuple[int, int]] = []
    original = SyncObjectStore.get_range

    def spy(self: SyncObjectStore, path: str, offset: int, length: int) -> bytes:
        calls.append((offset, length))
        return original(self, path, offset, length)

    monkeypatch.setattr(SyncObjectStore, "get_range", spy)
    with store.open_reader("blob.bin", read_ahead=read_ahead) as reader:
        chunks = [reader.read(4096) for _ in range(16)]
        assert b"".join(chunks) == _READ_AHEAD_OBJECT[:65536]
        assert len(calls) == after_reads
        assert reader.seek(1024) == 1024
        assert reader.read(4096) == _READ_AHEAD_OBJECT[1024:5120]
        assert reader.tell() == 5120
    assert len(calls) == after_seek
    if read_ahead:
        assert calls == [(0, len(_READ_AHEAD_OBJECT))]


@pytest.mark.anyio
@pytest.mark.parametrize(("read_ahead", "after_reads", "after_seek"), [(4_194_304, 1, 1), (0, 16, 17)])
async def test_async_reader_read_ahead_coalesces_range_calls(
    monkeypatch: pytest.MonkeyPatch, read_ahead: int, after_reads: int, after_seek: int
) -> None:
    """AsyncReadableFile serves sub-window reads and backward seeks from one cached range."""
    store = AsyncObjectStore(MemoryConfig(namespace=f"read-ahead-async-{read_ahead}"))
    await store.put("blob.bin", _READ_AHEAD_OBJECT)
    calls: list[tuple[int, int]] = []
    original = AsyncObjectStore.get_range

    async def spy(self: AsyncObjectStore, path: str, offset: int, length: int) -> bytes:
        calls.append((offset, length))
        return await original(self, path, offset, length)

    monkeypatch.setattr(AsyncObjectStore, "get_range", spy)
    async with store.open_reader("blob.bin", read_ahead=read_ahead) as reader:
        chunks = [await reader.read(4096) for _ in range(16)]
        assert b"".join(chunks) == _READ_AHEAD_OBJECT[:65536]
        assert len(calls) == after_reads
        assert reader.seek(1024) == 1024
        assert await reader.read(4096) == _READ_AHEAD_OBJECT[1024:5120]
    assert len(calls) == after_seek
    if read_ahead:
        assert calls == [(0, len(_READ_AHEAD_OBJECT))]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_reader_bypasses_cache_for_large_reads_and_refills_on_miss(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    """Reads at least read_ahead long bypass the cache and window misses refetch bounded by EOF."""
    calls: list[tuple[int, int]] = []
    data = bytes(range(256)) * 16
    if asynchronous:
        async_store = AsyncObjectStore(MemoryConfig(namespace="bypass-async"))
        await async_store.put("blob.bin", data)
        async_original = AsyncObjectStore.get_range

        async def async_spy(self: AsyncObjectStore, path: str, offset: int, length: int) -> bytes:
            calls.append((offset, length))
            return await async_original(self, path, offset, length)

        monkeypatch.setattr(AsyncObjectStore, "get_range", async_spy)
        async_reader = async_store.open_reader("blob.bin", size=len(data), read_ahead=1024)
        assert await async_reader.read(1024) == data[:1024]
        assert await async_reader.read(100) == data[1024:1124]
        assert await async_reader.read(50) == data[1124:1174]
        async_reader.seek(len(data) - 10)
        assert await async_reader.read(100) == data[-10:]
    else:
        store = SyncObjectStore(MemoryConfig(namespace="bypass-sync"))
        store.put("blob.bin", data)
        original = SyncObjectStore.get_range

        def spy(self: SyncObjectStore, path: str, offset: int, length: int) -> bytes:
            calls.append((offset, length))
            return original(self, path, offset, length)

        monkeypatch.setattr(SyncObjectStore, "get_range", spy)
        reader = store.open_reader("blob.bin", read_ahead=1024)
        assert reader.read(1024) == data[:1024]
        assert reader.read(100) == data[1024:1124]
        assert reader.read(50) == data[1124:1174]
        reader.seek(len(data) - 10)
        assert reader.read(100) == data[-10:]
    assert calls == [(0, 1024), (1024, 1024), (len(data) - 10, 10)]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_open_reader_rejects_negative_read_ahead(asynchronous: bool) -> None:
    """read_ahead below zero raises ValueError on both reader flavors."""
    store: Any = AsyncObjectStore("memory://") if asynchronous else SyncObjectStore("memory://")
    with pytest.raises(ValueError, match="read_ahead"):
        store.open_reader("missing.bin", size=10, read_ahead=-1)


_WRITER_CHUNK = 1_048_576
_WRITER_TOTAL = 20 * _WRITER_CHUNK


def _writer_chunk(index: int) -> bytes:
    """Return a deterministic 1 MiB chunk distinct per index."""
    return bytes([index % 251]) * _WRITER_CHUNK


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_streams_multipart_parts_from_reusable_bytearray(asynchronous: bool) -> None:
    """A 20 MiB stream flushes 8 MiB parts, completes with an etag, and never pins the caller's bytearray."""
    store, base, native = _recording_store(f"writer-20mib-{asynchronous}", asynchronous)
    scratch = bytearray()
    if asynchronous:
        async with store.open_writer("big.bin", part_size=8_388_608, content_type="application/octet-stream") as wf:
            assert isinstance(wf, AsyncWritableFile)
            assert native.uploads == 0
            for index in range(20):
                scratch += _writer_chunk(index)
                assert await wf.write(scratch) == _WRITER_CHUNK
                scratch.clear()
            assert wf.tell() == _WRITER_TOTAL
    else:
        with store.open_writer("big.bin", part_size=8_388_608, content_type="application/octet-stream") as wf:
            assert isinstance(wf, SyncWritableFile)
            assert native.uploads == 0
            for index in range(20):
                scratch += _writer_chunk(index)
                assert wf.write(scratch) == _WRITER_CHUNK
                scratch.clear()
            assert wf.tell() == _WRITER_TOTAL
    assert wf.closed
    assert isinstance(wf.result, PutResult)
    assert wf.result.etag
    assert wf.result.size == _WRITER_TOTAL
    assert native.uploads == 1
    assert native.parts == [8_388_608, 8_388_608, 4_194_304]
    assert base.get("big.bin") == b"".join(_writer_chunk(index) for index in range(20))
    assert base.head("big.bin").content_type == "application/octet-stream"


def test_sync_writer_supports_gzip_and_buffered_writer() -> None:
    """SyncWritableFile is a RawIOBase usable by gzip.GzipFile and io.BufferedWriter."""
    store = SyncObjectStore(MemoryConfig(namespace="writer-stdlib"))
    payload = b"vertebra-streaming-writer\n" * 50_000

    with store.open_writer("data.gz", part_size=65536) as wf, gzip.GzipFile(fileobj=wf, mode="wb") as gz:
        gz.write(payload)
    assert gzip.decompress(store.get("data.gz")) == payload

    raw = store.open_writer("buffered.bin", part_size=65536)
    with io.BufferedWriter(raw, buffer_size=4096) as buffered:
        buffered.write(payload)
    assert raw.closed
    assert raw.result is not None
    assert raw.result.size == len(payload)
    assert store.get("buffered.bin") == payload


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("part_size", [8_388_608, 4])
async def test_open_writer_aborts_on_exception(asynchronous: bool, part_size: int) -> None:
    """An exception inside the writer context propagates and leaves no object behind."""
    store, base, native = _recording_store(f"writer-abort-{asynchronous}-{part_size}", asynchronous)
    with pytest.raises(ValueError, match="boom"):
        if asynchronous:
            async with store.open_writer("aborted.bin", part_size=part_size) as wf:
                await wf.write(b"partial-payload")
                raise ValueError("boom")
        else:
            with store.open_writer("aborted.bin", part_size=part_size) as wf:
                wf.write(b"partial-payload")
                raise ValueError("boom")
    assert wf.closed
    assert wf.result is None
    assert not base.exists("aborted.bin")
    assert native.parts == ([] if part_size > 4 else [4, 4, 4])


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_explicit_abort_and_closed_writes(asynchronous: bool) -> None:
    """abort() discards the upload and later writes fail as closed-file errors."""
    store, base, _native = _recording_store(f"writer-explicit-abort-{asynchronous}", asynchronous)
    wf = store.open_writer("dropped.bin", part_size=4)
    if asynchronous:
        await wf.write(b"0123456789")
        await wf.abort()
        with pytest.raises(ValueError, match="closed"):
            await wf.write(b"more")
        await wf.aclose()
    else:
        wf.write(b"0123456789")
        wf.abort()
        with pytest.raises(ValueError, match="closed"):
            wf.write(b"more")
        wf.close()
    assert wf.closed
    assert wf.result is None
    assert not base.exists("dropped.bin")


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_zero_bytes_creates_empty_object(asynchronous: bool) -> None:
    """Closing a writer that received no bytes completes a valid zero-byte object."""
    store, base, _native = _recording_store(f"writer-empty-{asynchronous}", asynchronous)
    if asynchronous:
        async with store.open_writer("empty.bin") as wf:
            assert await wf.write(b"") == 0
    else:
        with store.open_writer("empty.bin") as wf:
            assert wf.write(b"") == 0
    assert isinstance(wf.result, PutResult)
    assert wf.result.size == 0
    assert base.get("empty.bin") == b""


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_if_not_exists_preserves_existing_object(asynchronous: bool) -> None:
    """if_not_exists rejects completion over an existing key and keeps the original payload."""
    store, base, _native = _recording_store(f"writer-precondition-{asynchronous}", asynchronous)
    base.put("taken.bin", b"original")
    with pytest.raises(FileExistsError):
        if asynchronous:
            async with store.open_writer("taken.bin", if_not_exists=True) as wf:
                await wf.write(b"replacement")
        else:
            with store.open_writer("taken.bin", if_not_exists=True) as wf:
                wf.write(b"replacement")
    assert wf.closed
    assert wf.result is None
    assert base.get("taken.bin") == b"original"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("part_size", [0, -1])
def test_open_writer_rejects_non_positive_part_size(asynchronous: bool, part_size: int) -> None:
    """part_size must be positive on both writer flavors."""
    store: Any = AsyncObjectStore("memory://") if asynchronous else SyncObjectStore("memory://")
    with pytest.raises(ValueError, match="part_size"):
        store.open_writer("bad.bin", part_size=part_size)


class _FailingUpload:
    """Multipart upload double whose part writes raise a configured exception and record every call."""

    def __init__(self, error: BaseException, asynchronous: bool) -> None:
        self.error = error
        self.asynchronous = asynchronous
        self.calls: list[str] = []

    def write_part(self, chunk: Any) -> Any:
        self.calls.append("write_part")
        if self.asynchronous:
            return self._raise_async()
        raise self.error

    def complete(self) -> Any:
        self.calls.append("complete")
        return self.resolve(None)

    def abort(self) -> Any:
        self.calls.append("abort")
        return self.resolve(None)

    def resolve(self, value: Any) -> Any:
        if not self.asynchronous:
            return value

        async def resolved() -> Any:
            return value

        return resolved()

    async def _raise_async(self) -> None:
        raise self.error


class _FailingNative:
    """Native store double that hands out a single failing multipart upload."""

    def __init__(self, upload: _FailingUpload) -> None:
        self.upload = upload

    def create_multipart(self, *args: Any, **kwargs: Any) -> Any:
        return self.upload.resolve(self.upload)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("asynchronous", "error"),
    [
        pytest.param(False, OSError("part upload failed"), id="sync-error"),
        pytest.param(True, OSError("part upload failed"), id="async-error"),
        pytest.param(True, asyncio.CancelledError(), id="async-cancelled"),
    ],
)
async def test_writer_part_failure_aborts_and_blocks_later_commit(asynchronous: bool, error: BaseException) -> None:
    """A failed or cancelled part write aborts the upload so a later close cannot commit a gapped object."""
    namespace = f"writer-part-failure-{asynchronous}-{type(error).__name__}"
    base = SyncObjectStore(MemoryConfig(namespace=namespace))
    upload = _FailingUpload(error, asynchronous)
    native = _FailingNative(upload)
    store: Any = (
        AsyncObjectStore(MemoryConfig(namespace=namespace), _native_instance=native)
        if asynchronous
        else SyncObjectStore(MemoryConfig(namespace=namespace), _native_instance=native)
    )
    wf = store.open_writer("failed.bin", part_size=4)
    with pytest.raises(type(error)):
        if asynchronous:
            await wf.write(b"0123456789")
        else:
            wf.write(b"0123456789")
    assert wf.closed
    if asynchronous:
        assert await wf.aclose() is None
    else:
        wf.close()
    assert upload.calls == ["write_part", "abort"]
    assert wf.result is None
    assert not base.exists("failed.bin")


@pytest.mark.anyio
async def test_async_writer_flush_and_aclose_return_put_result() -> None:
    """flush() keeps the tail pending, and aclose() returns the stored PutResult on every call."""
    store = AsyncObjectStore(MemoryConfig(namespace="writer-aclose-result"))
    wf = store.open_writer("done.bin", part_size=4)
    await wf.write(b"abcdef")
    await wf.flush()
    assert wf.tell() == 6
    assert wf.result is None
    result = await wf.aclose()
    assert isinstance(result, PutResult)
    assert result is wf.result
    assert result.size == 6
    assert await wf.aclose() is result
    with pytest.raises(ValueError, match="closed"):
        await wf.flush()
    assert await store.get("done.bin") == b"abcdef"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_stream_normalizes_typed_buffer_chunks(asynchronous: bool) -> None:
    """put_stream uploads typed array chunks as raw bytes, matching put."""
    store, base, native = _recording_store(f"put-stream-typed-{asynchronous}", asynchronous)
    chunks = [array.array("I", [1, 2, 3]), array.array("I", [4, 5])]
    if asynchronous:
        meta = await store.put_stream("typed.bin", chunks)
    else:
        meta = store.put_stream("typed.bin", chunks)
    expected = b"".join(chunk.tobytes() for chunk in chunks)
    assert meta.size == len(expected)
    assert native.parts == [len(chunk.tobytes()) for chunk in chunks]
    assert base.get("typed.bin") == expected


@pytest.mark.anyio
async def test_async_writer_collected_open_emits_resource_warning() -> None:
    """Collecting an open AsyncWritableFile warns about the uncompleted multipart upload."""
    store = AsyncObjectStore(MemoryConfig(namespace="writer-unclosed"))
    wf = store.open_writer("leaked.bin", part_size=4)
    await wf.write(b"0123456789")
    with pytest.warns(ResourceWarning, match="leaked.bin"):
        del wf
        gc.collect()
    assert not await store.exists("leaked.bin")


@pytest.mark.anyio
@pytest.mark.parametrize("finish", ["aclose", "abort"])
async def test_async_writer_collected_after_finish_is_silent(finish: str) -> None:
    """A completed or aborted AsyncWritableFile is collected without a ResourceWarning."""
    store = AsyncObjectStore(MemoryConfig(namespace=f"writer-finished-{finish}"))
    wf = store.open_writer("finished.bin", part_size=4)
    await wf.write(b"0123456789")
    await getattr(wf, finish)()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        del wf
        gc.collect()
    assert not [item for item in caught if issubclass(item.category, ResourceWarning)]


def test_storage_exports_writers_and_precondition_error() -> None:
    """vertebra.storage re-exports the streaming writers and PreconditionError in a sorted __all__."""
    assert storage.PreconditionError is PreconditionError
    assert storage.SyncWritableFile is SyncWritableFile
    assert storage.AsyncWritableFile is AsyncWritableFile
    assert {"AsyncWritableFile", "PreconditionError", "SyncWritableFile"} <= set(storage.__all__)
    assert list(storage.__all__) == sorted(storage.__all__)
    assert issubclass(SyncWritableFile, io.RawIOBase)


@pytest.mark.parametrize("scheme", ["memory", "file"])
def test_native_sync_bulk_file_io_conditional_copy_and_compose(tmp_path: Path, scheme: str) -> None:
    """Native SyncObjectStore round-trips files in bulk, enforces conditional copy, and composes objects."""
    url = "memory://bulk" if scheme == "memory" else f"file://{tmp_path / 'root'}"
    store = native_st.SyncObjectStore(url)
    sources: list[tuple[str, str]] = []
    for idx in range(5):
        local = tmp_path / f"up-{idx}.bin"
        local.write_bytes(bytes([idx]) * (idx + 1))
        sources.append((f"bulk/k{idx}.bin", str(local)))
    put_metas = store.put_files(sources, concurrency=2)
    assert [m.path for m in put_metas] == [key for key, _ in sources]
    downloads = [(key, str(tmp_path / "down" / f"d{idx}.bin")) for idx, (key, _) in enumerate(sources)]
    get_metas = store.get_files(downloads, concurrency=3)
    assert [m.size for m in get_metas] == [1, 2, 3, 4, 5]
    assert [Path(dst).read_bytes() for _, dst in downloads] == [bytes([i]) * (i + 1) for i in range(5)]
    single = store.get_file("bulk/k4.bin", str(tmp_path / "single.bin"))
    assert single.size == 5
    assert (tmp_path / "single.bin").read_bytes() == b"\x04" * 5
    with pytest.raises(FileExistsError):
        store.copy("bulk/k0.bin", "bulk/k1.bin", if_not_exists=True)
    assert store.get("bulk/k1.bin") == b"\x01\x01"
    store.copy("bulk/k0.bin", "bulk/k1.bin")
    assert store.get("bulk/k1.bin") == b"\x00"
    composed = store.compose(["bulk/k2.bin", "bulk/k3.bin"], "bulk/merged.bin", content_type="application/octet-stream")
    assert composed.size == 7
    assert store.get("bulk/merged.bin") == b"\x02" * 3 + b"\x03" * 4
    with pytest.raises(ValueError, match="concurrency"):
        store.get_files(downloads, concurrency=0)
    with pytest.raises(ValueError, match="concurrency"):
        store.put_files(sources, concurrency=-1)
    with pytest.raises(ValueError):
        store.compose([], "bulk/empty.bin")


@pytest.mark.anyio
async def test_native_async_bulk_file_io_conditional_copy_and_compose(tmp_path: Path) -> None:
    """Native AsyncObjectStore exposes awaitable bulk file I/O, conditional copy, and compose."""
    store = native_st.AsyncObjectStore("memory://bulk-async")
    sources: list[tuple[str, str]] = []
    for idx in range(3):
        local = tmp_path / f"a-{idx}.bin"
        local.write_bytes(f"part-{idx};".encode())
        sources.append((f"a/k{idx}", str(local)))
    put_metas = await store.put_files(sources, concurrency=2)
    assert [m.path for m in put_metas] == ["a/k0", "a/k1", "a/k2"]
    downloads = [(key, str(tmp_path / "out" / f"o{idx}")) for idx, (key, _) in enumerate(sources)]
    get_metas = await store.get_files(downloads)
    assert [m.size for m in get_metas] == [7, 7, 7]
    meta = await store.get_file("a/k1", str(tmp_path / "one.bin"))
    assert meta.size == 7
    assert (tmp_path / "one.bin").read_bytes() == b"part-1;"
    with pytest.raises(FileExistsError):
        await store.copy("a/k0", "a/k1", if_not_exists=True)
    await store.copy("a/k0", "a/fresh", if_not_exists=True)
    assert await store.get("a/fresh") == b"part-0;"
    composed = await store.compose(["a/k0", "a/k1", "a/k2"], "a/all")
    assert composed.size == 21
    assert await store.get("a/all") == b"part-0;part-1;part-2;"
    with pytest.raises(ValueError, match="concurrency"):
        await store.get_files(downloads, concurrency=0)


def _bulk_store(namespace: str, asynchronous: bool) -> Any:
    """Build a sync or async memory store isolated by namespace."""
    config = MemoryConfig(namespace=namespace)
    return AsyncObjectStore(config) if asynchronous else SyncObjectStore(config)


async def _resolve(value: Any) -> Any:
    """Await coroutine results from async stores and pass sync results through."""
    return await value if asyncio.iscoroutine(value) else value


def _write_bulk_sources(root: Path) -> list[Path]:
    """Write five local files with distinct payloads for bulk transfer tests."""
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for idx in range(5):
        local = root / f"file-{idx}.bin"
        local.write_bytes(f"payload-{idx}-".encode() * (idx + 1))
        paths.append(local)
    return paths


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_upload_many_download_many_and_get_file_roundtrip(tmp_path: Path, asynchronous: bool) -> None:
    """upload_many writes prefixed keys, download_many restores basenames, and get_file streams one object."""
    store = _bulk_store(f"bulk-roundtrip-{asynchronous}", asynchronous)
    sources = _write_bulk_sources(tmp_path / "src")
    results = await _resolve(store.upload_many(sources, "bulk/run", concurrency=2))
    assert all(isinstance(result, PutResult) for result in results)
    assert [result.path for result in results] == [f"bulk/run/file-{idx}.bin" for idx in range(5)]
    assert [result.size for result in results] == [source.stat().st_size for source in sources]
    assert all(result.etag for result in results)

    keys = [result.path for result in results]
    downloaded = await _resolve(store.download_many(keys, tmp_path / "down", concurrency=3))
    assert downloaded == [tmp_path / "down" / f"file-{idx}.bin" for idx in range(5)]
    assert [path.read_bytes() for path in downloaded] == [source.read_bytes() for source in sources]

    meta = await _resolve(store.get_file("bulk/run/file-3.bin", tmp_path / "single.bin"))
    assert isinstance(meta, ObjectMeta)
    assert meta.path == "bulk/run/file-3.bin"
    assert meta.size == sources[3].stat().st_size
    assert (tmp_path / "single.bin").read_bytes() == sources[3].read_bytes()

    unprefixed = await _resolve(store.upload_many([sources[0]]))
    assert [result.path for result in unprefixed] == ["file-0.bin"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("explicit_destination", [False, True])
@pytest.mark.parametrize("fail", [False, True])
async def test_staged_download_removes_staged_files_on_exit(
    tmp_path: Path, asynchronous: bool, explicit_destination: bool, fail: bool
) -> None:
    """staged_download stages every key and deletes exactly the staged files on normal and exceptional exit."""
    store = _bulk_store(f"bulk-staged-{asynchronous}-{explicit_destination}-{fail}", asynchronous)
    sources = _write_bulk_sources(tmp_path / "src")
    keys = [result.path for result in await _resolve(store.upload_many(sources, "staged"))]
    destination = tmp_path / "stage" if explicit_destination else None
    if destination is not None:
        destination.mkdir()
        (destination / "keep.txt").write_bytes(b"unrelated")
    staged: list[Path] = []

    async def stage() -> None:
        if asynchronous:
            async with store.staged_download(keys, destination, concurrency=2) as paths:
                staged.extend(paths)
                assert [path.read_bytes() for path in paths] == [source.read_bytes() for source in sources]
                if fail:
                    raise RuntimeError("consumer failed")
        else:
            with store.staged_download(keys, destination, concurrency=2) as paths:
                staged.extend(paths)
                assert [path.read_bytes() for path in paths] == [source.read_bytes() for source in sources]
                if fail:
                    raise RuntimeError("consumer failed")

    if fail:
        with pytest.raises(RuntimeError, match="consumer failed"):
            await stage()
    else:
        await stage()

    assert [path.name for path in staged] == [f"file-{idx}.bin" for idx in range(5)]
    assert not any(path.exists() for path in staged)
    if destination is None:
        assert not staged[0].parent.exists()
    else:
        assert staged[0].parent == destination
        assert sorted(child.name for child in destination.iterdir()) == ["keep.txt"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_staged_download_defaults_to_dev_shm_or_tempdir(asynchronous: bool) -> None:
    """Without a destination, staged files land in a unique directory under /dev/shm or the system temp dir."""
    store = _bulk_store(f"bulk-staged-base-{asynchronous}", asynchronous)
    await _resolve(store.put("base/one.bin", b"one"))
    shm = Path("/dev/shm")
    expected_base = shm if shm.is_dir() and os.access(shm, os.W_OK) else Path(tempfile.gettempdir())
    if asynchronous:
        async with store.staged_download(["base/one.bin"]) as paths:
            staged = list(paths)
            assert staged[0].read_bytes() == b"one"
    else:
        with store.staged_download(["base/one.bin"]) as paths:
            staged = list(paths)
            assert staged[0].read_bytes() == b"one"
    assert staged[0].parent.parent == expected_base
    assert not staged[0].parent.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_bulk_transfers_reject_duplicates_before_io_and_accept_empty(tmp_path: Path, asynchronous: bool) -> None:
    """Duplicate basenames or keys raise ValueError before any transfer, and empty inputs return []."""
    store = _bulk_store(f"bulk-dupes-{asynchronous}", asynchronous)
    await _resolve(store.put("a/x.bin", b"a"))
    await _resolve(store.put("b/x.bin", b"b"))
    destination = tmp_path / "dupes"
    with pytest.raises(ValueError, match="x.bin"):
        await _resolve(store.download_many(["a/x.bin", "b/x.bin"], destination))
    assert not destination.exists()

    (tmp_path / "one").mkdir()
    (tmp_path / "two").mkdir()
    (tmp_path / "one" / "same.bin").write_bytes(b"1")
    (tmp_path / "two" / "same.bin").write_bytes(b"2")
    with pytest.raises(ValueError, match="same.bin"):
        await _resolve(store.upload_many([tmp_path / "one" / "same.bin", tmp_path / "two" / "same.bin"], "up"))
    assert await _resolve(store.list("up")) == []

    if asynchronous:
        with pytest.raises(ValueError, match="x.bin"):
            async with store.staged_download(["a/x.bin", "b/x.bin"], destination):
                pytest.fail("duplicate staged keys must not enter the context")
    else:
        with pytest.raises(ValueError, match="x.bin"), store.staged_download(["a/x.bin", "b/x.bin"], destination):
            pytest.fail("duplicate staged keys must not enter the context")
    assert not destination.exists()

    assert await _resolve(store.download_many([], destination)) == []
    assert await _resolve(store.upload_many([])) == []
    if asynchronous:
        async with store.staged_download([]) as paths:
            assert paths == []
    else:
        with store.staged_download([]) as paths:
            assert paths == []


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_copy_overwrite_false_raises_file_exists(asynchronous: bool) -> None:
    """copy(..., overwrite=False) refuses to replace an existing destination and keeps its payload."""
    store = _bulk_store(f"copy-overwrite-{asynchronous}", asynchronous)
    await _resolve(store.put("a", b"1"))
    await _resolve(store.put("b", b"2"))
    with pytest.raises(FileExistsError):
        await _resolve(store.copy("a", "b", overwrite=False))
    assert await _resolve(store.get("b")) == b"2"
    await _resolve(store.copy("a", "c", overwrite=False))
    assert await _resolve(store.get("c")) == b"1"
    await _resolve(store.copy("a", "b"))
    assert await _resolve(store.get("b")) == b"1"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("runs/2026/*.parquet", ["runs/2026/part-00.parquet", "runs/2026/part-01.parquet"]),
        ("runs/2026/part-0[1].parquet", ["runs/2026/part-01.parquet"]),
        ("runs/202?/notes.*", ["runs/2026/notes.txt"]),
        ("runs/2026/notes.txt", ["runs/2026/notes.txt"]),
        ("runs/2026/missing.txt", []),
        ("runs/2027/*.parquet", []),
    ],
)
async def test_glob_wildcards_literal_fast_path_and_missing(
    asynchronous: bool, pattern: str, expected: list[str]
) -> None:
    """glob filters listed keys with fnmatch, uses head for literal keys, and returns [] when nothing matches."""
    store = _bulk_store(f"glob-{asynchronous}-{abs(hash(pattern))}", asynchronous)
    for key in ("runs/2026/part-00.parquet", "runs/2026/part-01.parquet", "runs/2026/notes.txt", "runs/20x.txt"):
        await _resolve(store.put(key, key.encode()))
    matched = await _resolve(store.glob(pattern))
    assert all(isinstance(meta, ObjectMeta) for meta in matched)
    assert [meta.path for meta in matched] == expected
    assert [meta.size for meta in matched] == [len(path) for path in expected]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_glob_strips_store_uri_prefix(tmp_path: Path, asynchronous: bool) -> None:
    """glob accepts patterns prefixed with the store URL and resolves them relative to the store root."""
    config = LocalConfig(root=tmp_path / f"glob-root-{asynchronous}")
    store: Any = AsyncObjectStore(config) if asynchronous else SyncObjectStore(config)
    await _resolve(store.put("logs/a.json", b"{}"))
    await _resolve(store.put("logs/b.txt", b"b"))
    matched = await _resolve(store.glob(f"{store.url}/logs/*.json"))
    assert [meta.path for meta in matched] == ["logs/a.json"]
    literal = await _resolve(store.glob(f"{store.url}/logs/b.txt"))
    assert [meta.path for meta in literal] == ["logs/b.txt"]


class _PlaceholderListingNative:
    """Delegate to a real native store while injecting a trailing-`/` directory placeholder into listings."""

    def __init__(self, native: Any, asynchronous: bool, placeholder: str) -> None:
        self.native = native
        self.asynchronous = asynchronous
        self.placeholder = ObjectMeta(path=placeholder, size=0)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native, name)

    def list(self, prefix: str | None) -> Any:
        listed = self.native.list(prefix)
        if self.asynchronous:

            async def resolve() -> Any:
                return [*(await listed), self.placeholder]

            return resolve()
        return [*listed, self.placeholder]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_glob_skips_trailing_slash_directory_placeholders(asynchronous: bool) -> None:
    """glob never returns keys ending in `/` even when the pattern matches them."""
    namespace = f"glob-placeholder-{asynchronous}"
    base = SyncObjectStore(MemoryConfig(namespace=namespace))
    base.put("runs/2026/part-00.parquet", b"p")
    paired = base.to_async() if asynchronous else base
    native = _PlaceholderListingNative(paired._require_native(), asynchronous, "runs/2026/")
    config = MemoryConfig(namespace=namespace)
    store: Any = (
        AsyncObjectStore(config, _native_instance=native)
        if asynchronous
        else SyncObjectStore(config, _native_instance=native)
    )
    matched = await _resolve(store.glob("runs/*"))
    assert [meta.path for meta in matched] == ["runs/2026/part-00.parquet"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_compose_concatenates_sources_and_rejects_empty(asynchronous: bool) -> None:
    """compose writes the concatenation of its sources and rejects an empty source list before I/O."""
    store = _bulk_store(f"compose-{asynchronous}", asynchronous)
    for idx, payload in enumerate((b"alpha-", b"beta-", b"gamma")):
        await _resolve(store.put(f"p{idx + 1}", payload))
    result = await _resolve(store.compose(["p1", "p2", "p3"], "merged.bin", content_type="application/octet-stream"))
    assert isinstance(result, PutResult)
    assert result.path == "merged.bin"
    assert result.size == len(b"alpha-beta-gamma")
    assert await _resolve(store.get("merged.bin")) == b"alpha-beta-gamma"
    with pytest.raises(ValueError, match="source"):
        await _resolve(store.compose([], "empty.bin"))
    assert not await _resolve(store.exists("empty.bin"))
