"""Unit tests for real Advanced Alchemy StorageBackend and SQLSpec ObjectStoreProtocol backends."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import subprocess
import sys
import threading
from asyncio import CancelledError
from collections.abc import AsyncGenerator, AsyncIterator, Iterator, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from advanced_alchemy.types.file_object import FileObject, StorageBackend, StoredObject
from advanced_alchemy.types.file_object.registry import StorageRegistry as AlchemyStorageRegistry
from advanced_alchemy.types.file_object.session_tracker import FileObjectSessionTracker
from django.core.files.base import ContentFile
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.pipeline import AsyncStoragePipeline, SyncStoragePipeline
from sqlspec.storage.registry import StorageRegistry as SQLSpecStorageRegistry

import vertebra.extensions.alchemy.storage as alchemy_storage_mod
import vertebra.extensions.sqlspec.storage as sqlspec_storage_mod
from vertebra._vertebra import InterfaceError
from vertebra.db.adapters.duckdb import DuckDBSecretConfig
from vertebra.db.arrow import ArrowCursor
from vertebra.db.cdc import CDCConfig
from vertebra.db.driver.capabilities import NotificationEvent
from vertebra.extensions.alchemy.storage import ObjectStoreBackend as AlchemyObjectStoreBackend
from vertebra.extensions.alchemy.storage import register_storage_backend
from vertebra.extensions.django import DjangoStorage
from vertebra.extensions.sqlspec.adapters.sqlite import SQLiteAsyncConfig, SQLiteEventQueueStore, SQLiteSyncConfig
from vertebra.extensions.sqlspec.events import ChangeFeedAsyncEventBackend, ChangeFeedSyncEventBackend
from vertebra.extensions.sqlspec.storage import (
    ArrowFlightSerializer,
    register_storage_alias,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend as SQLSpecObjectStoreBackend
from vertebra.storage import (
    AsyncGcpTokenCache,
    AsyncObjectStore,
    AsyncReadableFile,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    S3Config,
    SigningConfig,
    StreamingStorageResponse,
    SyncGcpTokenCache,
    SyncObjectStore,
)
from vertebra.typing import ImproperConfigurationError, IntegrityError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json

native_duckdb = import_optional("vertebra._duckdb")


class _SQLSpecRangeServer:
    """Serve real native S3 reads with an optional response barrier."""

    def __init__(self, payload: bytes, *, gated: bool) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.paths: list[str] = []
        self.ranges: list[tuple[int, int]] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_HEAD(self) -> None:
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("ETag", '"fixture"')
                self.send_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                self.end_headers()

            def do_GET(self) -> None:
                owner.paths.append(self.path)
                owner.entered.set()
                if gated and not owner.release.wait(10):
                    self.send_error(504)
                    return
                range_header = self.headers.get("Range")
                start, end = 0, len(payload) - 1
                if range_header:
                    left, right = range_header.removeprefix("bytes=").split("-")
                    start, end = int(left), int(right) if right else end
                owner.ranges.append((start, end))
                body = payload[start : end + 1]
                self.send_response(206 if range_header else 200)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("ETag", '"fixture"')
                if range_header:
                    self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
                self.end_headers()
                with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                    self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self.endpoint = f"http://127.0.0.1:{self._server.server_port}"

    def __enter__(self) -> _SQLSpecRangeServer:
        self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        self.release.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


@pytest.mark.anyio
async def test_sqlspec_arrow_async_io_stays_on_caller_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async Table reads and writes use native async I/O rather than synchronous stores."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-native-async"))
    table = pa.table({"id": [1, 2]})
    backend.write_arrow_sync("input.parquet", table)

    def reject_sync(*args: object, **kwargs: object) -> Any:
        raise AssertionError("synchronous storage called by async Arrow consumer")

    monkeypatch.setattr(SyncObjectStore, "get_range", reject_sync)
    monkeypatch.setattr(SyncObjectStore, "put", reject_sync)
    monkeypatch.setattr(SyncObjectStore, "put_stream", reject_sync)
    assert (await backend.read_arrow_async("input.parquet")).equals(table)
    await backend.write_arrow_async("output.parquet", table)
    assert (await backend.read_arrow_async("output.parquet")).equals(table)


@pytest.mark.anyio
async def test_sqlspec_arrow_stream_demands_one_file_at_a_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """A first batch is delivered before reading a later matching object."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-demand"), base_path="warehouse")
    for name, first in (("a", 0), ("b", 250)):
        backend.write_arrow_sync(
            f"tables/{name}.parquet", pa.table({"id": range(first, first + 250)}), row_group_size=100
        )
    requested: list[str] = []
    sync_range = SyncObjectStore.get_range
    async_range = AsyncObjectStore.get_range

    def observe_sync(self: SyncObjectStore, key: str, offset: int, length: int) -> bytes:
        requested.append(key)
        return sync_range(self, key, offset, length)

    async def observe_async(self: AsyncObjectStore, key: str, offset: int, length: int) -> bytes:
        requested.append(key)
        return await async_range(self, key, offset, length)

    monkeypatch.setattr(SyncObjectStore, "get_range", observe_sync)
    monkeypatch.setattr(AsyncObjectStore, "get_range", observe_async)
    stream = cast(
        "AsyncGenerator[Any, None]",
        backend.stream_arrow_async("tables/*.parquet", batch_size=100, columns=["id"]),
    )
    try:
        batch = await anext(stream)
        assert batch.to_pydict() == {"id": list(range(100))}
        assert requested and set(requested) == {"warehouse/tables/a.parquet"}
    finally:
        await stream.aclose()


@pytest.mark.anyio
async def test_sqlspec_arrow_explicit_prefetch_options() -> None:
    """Accepted Parquet constructor options remain distinct from batch iteration options."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-prefetch"))
    table = pa.table({"id": range(250), "label": ["data"] * 250})
    backend.write_arrow_sync("data.parquet", table, row_group_size=100)
    batches = [
        batch
        async for batch in backend.stream_arrow_async(
            "data.parquet", batch_size=100, columns=["id"], pre_buffer=True, use_threads=False
        )
    ]
    assert [batch.num_rows for batch in batches] == [100, 100, 50]
    assert pa.Table.from_batches(batches).to_pydict() == {"id": list(range(250))}


@pytest.mark.anyio
async def test_sqlspec_arrow_default_prefetch_preserves_rowgroup_demand() -> None:
    """The public SQLSpec stream returns its first batch before native later-rowgroup reads."""
    table = pa.table({"id": range(250), "payload": [b"x" * 4096] * 250})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=100, compression="NONE", use_dictionary=False)
    payload = sink.getvalue().to_pybytes()
    metadata = pq.ParquetFile(BytesIO(payload)).metadata
    later_columns = [metadata.row_group(i).column(0) for i in (1, 2)]
    with _SQLSpecRangeServer(payload, gated=False) as server:
        backend = SQLSpecObjectStoreBackend(
            S3Config(
                bucket="sqlspec-codec",
                region="us-east-1",
                endpoint=server.endpoint,
                access_key_id="test",
                secret_access_key="test",
                allow_http=True,
            )
        )
        stream = cast(
            "AsyncGenerator[Any, None]",
            backend.stream_arrow_async("data", batch_size=100, columns=["id"]),
        )
        try:
            assert (await anext(stream)).to_pydict() == {"id": list(range(100))}
            footer_start = len(payload) - 65536
            data_ranges = [(start, end) for start, end in server.ranges if end < footer_start]
            assert data_ranges
            for column in later_columns:
                start = column.data_page_offset
                end = start + column.total_compressed_size
                assert not any(left < end and right >= start for left, right in data_ranges)
            batches = [batch async for batch in stream]
            assert [batch.num_rows for batch in batches] == [100, 50]
            assert pa.Table.from_batches(batches).to_pydict() == {"id": list(range(100, 250))}
        finally:
            await stream.aclose()


def test_sqlspec_arrow_ipc_table_reads_native_ranges(monkeypatch: pytest.MonkeyPatch) -> None:
    """Materialized IPC Tables retain file fallback and schema without fetching encoded objects whole."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-ipc-ranges"))
    schema = pa.schema([("id", pa.int64())])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, schema):
        pass
    backend.write_bytes_sync("empty.ipc", sink.getvalue())

    def reject_view(*args: object, **kwargs: object) -> Any:
        raise AssertionError("whole encoded object requested")

    monkeypatch.setattr(SyncObjectStore, "get_view", reject_view)
    result = backend.read_arrow_sync("empty.ipc")
    assert result.schema == schema
    assert result.num_rows == 0


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("file_format", ["parquet", "arrow-ipc"])
@pytest.mark.parametrize("as_view", [False, True])
async def test_sqlspec_arrow_cursor_accepts_raw_ipc_buffers(
    asynchronous: bool, file_format: str, as_view: bool
) -> None:
    """Buffer-protocol cursor inputs preserve format selection, values and None returns."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-raw-cursor"))
    table = pa.table({"id": [1, 2], "label": ["a", "b"]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    payload = sink.getvalue().to_pybytes()
    source = memoryview(payload) if as_view else payload
    if asynchronous:
        await backend.write_arrow_cursor_async("output.unknown", source, file_format=file_format)
        actual = await backend.read_arrow_async("output.unknown", file_format=file_format)
    else:
        backend.write_arrow_cursor_sync("output.unknown", source, file_format=file_format)
        actual = backend.read_arrow_sync("output.unknown", file_format=file_format)
    assert actual.equals(table)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("compression", [None, "gzip"])
async def test_sqlspec_arrow_parquet_preserves_codec_defaults(asynchronous: bool, compression: str | None) -> None:
    """Unspecified compression retains PyArrow defaults while explicit writer options survive."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-compression"))
    table = pa.table({"id": range(250)})
    options = {"compression": compression} if compression is not None else {}
    if asynchronous:
        await backend.write_arrow_async("data.parquet", table, row_group_size=100, **options)
        raw = await backend.read_bytes_async("data.parquet")
    else:
        backend.write_arrow_sync("data.parquet", table, row_group_size=100, **options)
        raw = backend.read_bytes_sync("data.parquet")
    expected_sink = pa.BufferOutputStream()
    pq.write_table(table, expected_sink, row_group_size=100, **options)
    expected = pq.ParquetFile(expected_sink.getvalue()).metadata
    actual = pq.ParquetFile(BytesIO(raw)).metadata
    assert [actual.row_group(i).num_rows for i in range(actual.num_row_groups)] == [100, 100, 50]
    assert actual.row_group(0).column(0).compression == expected.row_group(0).column(0).compression


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("cursor_input", [False, True])
@pytest.mark.parametrize(
    "options", [{}, {"row_group_size": None}, {"row_group_size": -1}], ids=["omitted", "none", "minus-one"]
)
async def test_sqlspec_parquet_default_row_groups(
    asynchronous: bool, cursor_input: bool, options: dict[str, int | None]
) -> None:
    """Default row-group sentinels match the installed PyArrow writer's output."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-rowgroup-default"))
    table = pa.table({"id": range(1_048_577)})
    source: Any = table
    if cursor_input:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        source = sink.getvalue()
    if asynchronous:
        if cursor_input:
            await backend.write_arrow_cursor_async("data.parquet", source, **options)
        else:
            await backend.write_arrow_async("data.parquet", source, **options)
        raw = await backend.read_bytes_async("data.parquet")
    else:
        if cursor_input:
            backend.write_arrow_cursor_sync("data.parquet", source, **options)
        else:
            backend.write_arrow_sync("data.parquet", source, **options)
        raw = backend.read_bytes_sync("data.parquet")
    expected_sink = pa.BufferOutputStream()
    pq.write_table(table, expected_sink, **options)
    expected = pq.ParquetFile(expected_sink.getvalue())
    expected_sizes = [expected.metadata.row_group(i).num_rows for i in range(expected.num_row_groups)]
    actual = pq.ParquetFile(BytesIO(raw))
    assert expected_sizes == [1_048_576, 1]
    assert [actual.metadata.row_group(i).num_rows for i in range(actual.num_row_groups)] == expected_sizes
    assert actual.read().equals(table)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("row_group_size", [0, -2])
async def test_sqlspec_parquet_invalid_row_group_size(asynchronous: bool, row_group_size: int) -> None:
    """Values rejected by the installed PyArrow writer remain errors in the adapter."""
    table = pa.table({"id": [1]})
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        pq.write_table(table, pa.BufferOutputStream(), row_group_size=row_group_size)
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-rowgroup-invalid"))
    with pytest.raises(ValueError, match="batch_size"):
        if asynchronous:
            await backend.write_arrow_async("invalid.parquet", pa.table({"id": [1]}), row_group_size=row_group_size)
        else:
            backend.write_arrow_sync("invalid.parquet", pa.table({"id": [1]}), row_group_size=row_group_size)
    assert not await backend.exists_async("invalid.parquet")


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlspec_arrow_ipc_invalid_options_abort(asynchronous: bool) -> None:
    """Invalid codec keywords retain PyArrow errors and leave no completed native object."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-invalid-options"))
    table = pa.table({"id": [1]})
    with pytest.raises(TypeError, match="unexpected keyword"):
        if asynchronous:
            await backend.write_arrow_async("bad.ipc", table, unsupported_option=True)
        else:
            backend.write_arrow_sync("bad.ipc", table, unsupported_option=True)
    assert not await backend.exists_async("bad.ipc")


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlspec_arrow_empty_ipc_file_retains_schema(asynchronous: bool) -> None:
    """The complete-Table contract retains typed zero-row IPC file schemas on both APIs."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(namespace="sqlspec-arrow-empty-file"))
    schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, schema):
        pass
    backend.write_bytes_sync("empty.feather", sink.getvalue())
    result = (
        await backend.read_arrow_async("empty.feather") if asynchronous else backend.read_arrow_sync("empty.feather")
    )
    assert result.schema == schema
    assert result.num_rows == 0


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_pending", [False, True])
@pytest.mark.parametrize("file_format", ["parquet", "arrow-ipc"])
async def test_sqlspec_arrow_outer_stream_closes_native_source(
    monkeypatch: pytest.MonkeyPatch, cancel_pending: bool, file_format: str
) -> None:
    """Outer close or cancellation unwinds the actual native per-file reader before returning."""
    table = pa.table({"id": range(250)})
    sink = pa.BufferOutputStream()
    if file_format == "parquet":
        pq.write_table(table, sink, row_group_size=100)
    else:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table, max_chunksize=100)
    closed: list[object] = []
    close_reader = AsyncReadableFile.close
    get_stream = AsyncObjectStore.get_stream

    def observe_close(self: AsyncReadableFile) -> None:
        close_reader(self)
        closed.append(self)

    async def observe_stream(self: AsyncObjectStore, key: str, *, chunk_size: int | None = None) -> AsyncIterator[Any]:
        source = cast("AsyncGenerator[Any, None]", get_stream(self, key, chunk_size=chunk_size))
        try:
            async for chunk in source:
                yield chunk
        finally:
            await source.aclose()
            closed.append(key)

    monkeypatch.setattr(AsyncReadableFile, "close", observe_close)
    monkeypatch.setattr(AsyncObjectStore, "get_stream", observe_stream)
    with _SQLSpecRangeServer(sink.getvalue().to_pybytes(), gated=cancel_pending) as server:
        store = AsyncObjectStore(
            S3Config(
                bucket="sqlspec-codec",
                region="us-east-1",
                endpoint=server.endpoint,
                access_key_id="test",
                secret_access_key="test",
                allow_http=True,
            )
        )
        backend = SQLSpecObjectStoreBackend(store)
        stream = cast(
            "AsyncGenerator[Any, None]",
            backend.stream_arrow_async("data", file_format=file_format, batch_size=100),
        )
        task: asyncio.Task[Any] | None = None
        try:
            if cancel_pending:
                task = asyncio.create_task(anext(stream))
                async with asyncio.timeout(5):
                    while not server.entered.is_set():
                        await asyncio.sleep(0.001)
                assert not task.done()
                task.cancel()
                with pytest.raises(CancelledError):
                    await asyncio.wait_for(task, 2)
            else:
                first = await anext(stream)
                assert first.to_pydict() == {"id": list(range(100))}
                assert not closed
                await stream.aclose()
            assert closed
            assert server.paths and set(server.paths) == {"/sqlspec-codec/data"}
        finally:
            server.release.set()
            if task is not None:
                if not task.done():
                    task.cancel()
                with contextlib.suppress(CancelledError, Exception):
                    await task
            await stream.aclose()


def test_duplicated_storage_shims_removed_from_extension_modules() -> None:
    """Shadow FileAttachment, StoredObject, SQLSpecStore, and SqlSpecStorageBridge are deleted."""
    assert not hasattr(alchemy_storage_mod, "FileAttachment")
    assert not hasattr(alchemy_storage_mod, "StoredObject")
    assert not hasattr(sqlspec_storage_mod, "SQLSpecStore")
    assert not hasattr(sqlspec_storage_mod, "SqlSpecStorageBridge")


def test_alchemy_import_preserves_registry_api() -> None:
    """Importing the adapter preserves Advanced Alchemy's registry API."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from advanced_alchemy.types.file_object import storages; "
            "before = hasattr(storages, 'get'); "
            "import vertebra.extensions.alchemy.storage; "
            "assert hasattr(storages, 'get') == before",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("target", [None, "", "  "])
def test_alchemy_requires_explicit_target(target: str | None) -> None:
    """Missing targets fail before changing the singleton registry or its default."""
    registry = AlchemyStorageRegistry()
    registered = registry.registered_backends()
    default = registry.default_backend
    with pytest.raises(ImproperConfigurationError):
        AlchemyObjectStoreBackend(fs=target)
    with pytest.raises(ImproperConfigurationError):
        register_storage_backend("alchemy_missing_target", store=target, set_default=True)
    with pytest.raises(ImproperConfigurationError):
        AlchemyObjectStoreBackend()
    assert registry.registered_backends() == registered
    assert registry.default_backend == default


@pytest.mark.parametrize("path", ["../escape.bin", "folder/../escape.bin", "folder\\..\\escape.bin"])
def test_alchemy_rejects_traversal_in_keys_and_prefixes(tmp_path: Path, path: str) -> None:
    """Namespaces reject traversal before storage or registry changes."""
    store = SyncObjectStore.from_local(tmp_path)
    backend = AlchemyObjectStoreBackend(fs=store, prefix="uploads")
    with pytest.raises(ValueError):
        backend.get_content(path)
    registry = AlchemyStorageRegistry()
    registered = registry.registered_backends()
    default = registry.default_backend
    with pytest.raises(ValueError, match="traversal"):
        register_storage_backend(store, key="alchemy_bad_prefix", prefix=path, set_default=True)
    assert registry.registered_backends() == registered
    assert registry.default_backend == default
    assert store.list() == []


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async", "async-reader"])
async def test_alchemy_file_upload_reads_bounded_chunks(tmp_path: Path, mode: str) -> None:
    """File-like uploads request bounded chunks and populate actual native metadata."""
    reads: list[int] = []

    class BoundedReader(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            assert size == 4
            reads.append(size)
            return super().read(size)

    reader = BoundedReader(b"chunk1-chunk2")

    class AsyncReader:
        async def read(self, size: int = -1) -> bytes:
            return reader.read(size)

    backend = AlchemyObjectStoreBackend(fs=SyncObjectStore.from_local(tmp_path), prefix="uploads")
    obj = FileObject(backend=backend, filename="chunks.bin")
    if mode == "sync":
        assert obj.save(reader, use_multipart=True, chunk_size=4) is obj
    else:
        upload_src: Any = AsyncReader() if mode == "async-reader" else reader
        assert await obj.save_async(upload_src, use_multipart=True, chunk_size=4) is obj
    assert reads == [4, 4, 4, 4, 4]
    assert obj.size == 13
    assert obj.etag is not None
    assert obj.get_content() == b"chunk1-chunk2"
    assert await obj.get_content_async() == b"chunk1-chunk2"


@pytest.mark.anyio
@pytest.mark.parametrize("chunk_size", [0, -1])
async def test_alchemy_invalid_chunk_size_preserves_source(tmp_path: Path, chunk_size: int) -> None:
    """Invalid sizes fail before consuming either upload source."""
    backend = AlchemyObjectStoreBackend(fs=SyncObjectStore.from_local(tmp_path))
    reader = BytesIO(b"unread")
    obj = FileObject(backend=backend, filename="invalid.bin")
    with pytest.raises(ValueError, match="chunk_size"):
        obj.save(reader, chunk_size=chunk_size)
    with pytest.raises(ValueError, match="chunk_size"):
        await obj.save_async(reader, chunk_size=chunk_size)
    assert reader.tell() == 0
    assert not backend.exists("invalid.bin")


@pytest.mark.anyio
@pytest.mark.parametrize("async_handle", [False, True])
async def test_alchemy_explicit_registration_and_shared_handles(tmp_path: Path, async_handle: bool) -> None:
    """Explicit forms resolve real registry keys and share one native engine."""
    registry = AlchemyStorageRegistry()
    default = registry.default_backend
    sync_store = SyncObjectStore.from_local(tmp_path)
    store = sync_store.to_async() if async_handle else sync_store
    try:
        backend = register_storage_backend("alchemy_explicit", store=store, prefix="uploads", set_default=True)
        assert registry.get_backend("alchemy_explicit") is backend
        assert registry.default_backend == "vertebra.extensions.alchemy.storage.ObjectStoreBackend"
        obj = FileObject(backend="alchemy_explicit", filename="uploads/one.bin")
        assert obj.save(b"one") is obj
        assert sync_store.get("uploads/one.bin") == b"one"
        await obj.save_async(b"two2")
        assert obj.size == 4
        assert obj.get_content() == b"two2"
        configured = register_storage_backend(LocalConfig(root=tmp_path), key="alchemy_config")
        assert registry.get_backend("alchemy_config") is configured
        assert configured.get_content("uploads/one.bin") == b"two2"
        assert AlchemyObjectStoreBackend(fs="", store=store).store.get("uploads/one.bin") == b"two2"
        memory = AlchemyObjectStoreBackend(fs=MemoryConfig())
        FileObject(backend=memory, filename="explicit.bin").save(b"explicit")
        assert await memory.get_content_async("explicit.bin") == b"explicit"
        await backend.delete_object_async("one.bin")
        assert not sync_store.exists("uploads/one.bin")
    finally:
        registry.unregister_backend("alchemy_explicit")
        registry.unregister_backend("alchemy_config")
        registry.set_default_backend(default)


@pytest.mark.anyio
async def test_alchemy_stream_sources_and_failure_cleanup(tmp_path: Path) -> None:
    """Iterable sources stream to native storage and failures leave no partial objects."""
    backend = AlchemyObjectStoreBackend(fs=LocalConfig(root=tmp_path), prefix="uploads")

    def chunks() -> Iterator[bytes]:
        yield b"chunk1-"
        yield b"chunk2"

    async def async_chunks() -> AsyncIterator[bytes]:
        yield b"chunk1-"
        yield b"chunk2"

    sync_obj = FileObject(backend=backend, filename="sync.bin")
    sync_obj.save(chunks(), use_multipart=True, chunk_size=4)
    assert sync_obj.size == 13
    assert sync_obj.get_content() == b"chunk1-chunk2"
    async_obj = FileObject(backend=backend, filename="async.bin")
    await async_obj.save_async(async_chunks(), use_multipart=True, chunk_size=4)
    assert async_obj.size == 13
    assert await async_obj.get_content_async() == b"chunk1-chunk2"
    await async_obj.save_async(chunks())
    assert await async_obj.get_content_async() == b"chunk1-chunk2"
    failure = RuntimeError("source failed")

    def failed_chunks() -> Iterator[bytes]:
        yield b"partial"
        raise failure

    async def failed_async_chunks() -> AsyncIterator[bytes]:
        yield b"partial"
        raise failure

    with pytest.raises(RuntimeError) as sync_error:
        FileObject(backend=backend, filename="failed-sync.bin").save(failed_chunks())
    with pytest.raises(RuntimeError) as async_error:
        await FileObject(backend=backend, filename="failed-async.bin").save_async(failed_async_chunks())
    assert sync_error.value is failure
    assert async_error.value is failure
    assert not backend.exists("failed-sync.bin")
    assert not await backend.exists_async("failed-async.bin")

    async def cancelled_chunks() -> AsyncIterator[bytes]:
        yield b"partial"
        raise CancelledError

    with pytest.raises(CancelledError):
        await FileObject(backend=backend, filename="cancelled.bin").save_async(cancelled_chunks())
    assert not await backend.exists_async("cancelled.bin")


@pytest.mark.anyio
async def test_alchemy_unconfigured_signing() -> None:
    """Unsupported native signing reports the capability and retains the upstream backend error contract."""
    backend = AlchemyObjectStoreBackend(fs=MemoryConfig())
    assert backend.supports_signing is False
    with pytest.raises(NotImplementedError, match="not configured"):
        backend.sign("one.bin")
    with pytest.raises(NotImplementedError, match="not configured"):
        await backend.sign_async(["one.bin"], for_upload=True)


@pytest.mark.parametrize("target", [None, "", "  "])
def test_sqlspec_object_store_requires_explicit_target(target: str | None) -> None:
    """Missing and blank targets fail before a storage engine is constructed."""
    with pytest.raises(ImproperConfigurationError):
        SQLSpecObjectStoreBackend(target)


def test_sqlspec_object_store_requires_target_when_omitted() -> None:
    """Omitting the target reports configuration failure."""
    with pytest.raises(ImproperConfigurationError):
        SQLSpecObjectStoreBackend()


@pytest.mark.parametrize("uri", [None, "", "  "])
def test_sqlspec_explicit_store_precedes_empty_uri(tmp_path: Path, uri: str | None) -> None:
    """An explicit store supplies the canonical address when the URI is omitted or blank."""
    backend = SQLSpecObjectStoreBackend(uri, store=SyncObjectStore.from_local(tmp_path), base_path="tenant-a")
    assert backend.resolve_uri("object.bin") == str(tmp_path / "tenant-a" / "object.bin")


@pytest.mark.parametrize("uri", ["memory://", "s3://logical-bucket"])
def test_sqlspec_backend_store_uri_resolution(tmp_path: Path, uri: str) -> None:
    """Explicit URI metadata applies to a prebuilt store, including a memory URI."""
    store = SyncObjectStore.from_local(tmp_path)
    backend = SQLSpecObjectStoreBackend(uri, store=store)
    assert backend.store_uri == uri
    assert backend.protocol == uri.split(":", 1)[0]
    backend.write_bytes_sync("object.bin", b"value")
    assert store.get("object.bin") == b"value"


@pytest.mark.anyio
@pytest.mark.parametrize("async_store", [False, True])
async def test_sqlspec_shared_store_conditional_writes_and_rename(tmp_path: Path, async_store: bool) -> None:
    """Paired handles retain bytes and native write conditions across moves."""
    store = SyncObjectStore.from_local(tmp_path)
    backend = SQLSpecObjectStoreBackend(store=store.to_async() if async_store else store, base_path="tenant-a")
    assert isinstance(backend, ObjectStoreProtocol)
    backend.write_bytes_sync("a.bin", b"123")
    await backend.move_async("a.bin", "b.bin")
    assert not backend.exists_sync("a.bin")
    assert await backend.read_bytes_async("b.bin") == b"123"
    with pytest.raises(FileExistsError):
        backend.write_bytes_sync("b.bin", b"replacement", if_not_exists=True)
    assert await backend.read_bytes_async("b.bin") == b"123"
    with pytest.raises(FileExistsError):
        await backend.write_bytes_async("b.bin", b"replacement", if_not_exists=True)
    backend.write_text_sync("c.txt", "sync")
    await backend.write_text_async("d.txt", "async")
    backend.move_sync("c.txt", "d.txt")
    assert not await backend.exists_async("c.txt")
    assert await backend.read_text_async("d.txt") == "sync"
    assert backend.sync_store.get("tenant-a/b.bin") == b"123"


@pytest.mark.anyio
async def test_sqlspec_write_metadata_and_conditions() -> None:
    """Native metadata and etag conditions survive both byte and text writes."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(), base_path="tenant-a")
    backend.write_bytes_sync("meta.bin", b"data", content_type="text/plain", attributes={"tenant": "a"})
    meta = await backend.async_store.head("tenant-a/meta.bin")
    assert meta.content_type == "text/plain"
    assert meta.attributes == {"tenant": "a"}
    await backend.write_text_async("meta.bin", "updated", if_match=meta.etag, content_type="text/markdown")
    assert backend.read_text_sync("meta.bin") == "updated"
    assert backend.sync_store.head("tenant-a/meta.bin").content_type == "text/markdown"
    with pytest.raises(OSError):
        backend.write_bytes_sync("meta.bin", b"rejected", if_match="wrong-etag")
    with pytest.raises(OSError):
        await backend.write_text_async("meta.bin", "rejected", if_none_match="*")
    assert backend.read_text_sync("meta.bin") == "updated"


@pytest.mark.anyio
async def test_sqlspec_native_streams_and_namespace_pages(tmp_path: Path) -> None:
    """Native streams retain chunk bytes and pages share opaque sync/async cursors."""
    store = SyncObjectStore.from_local(tmp_path)
    backend = SQLSpecObjectStoreBackend(store=store, base_path="tenant-a")
    store.put("tenant-other/hidden.bin", b"hidden")
    result = backend.put_stream_sync("b.bin", iter([b"123"]))
    assert result.etag
    uploaded = await backend.put_stream_async("big.bin", [b"chunk1-", b"chunk2"])
    assert uploaded.etag
    assert b"".join(bytes(chunk) for chunk in backend.get_stream_sync("big.bin", chunk_size=3)) == b"chunk1-chunk2"
    assert (
        b"".join([bytes(chunk) async for chunk in backend.get_stream_async("big.bin", chunk_size=4)])
        == b"chunk1-chunk2"
    )
    assert all(isinstance(chunk, bytes) for chunk in backend.stream_read_sync("big.bin", chunk_size=3))
    assert b"".join(backend.stream_read_sync("big.bin", chunk_size=3)) == b"chunk1-chunk2"
    stream = await backend.stream_read_async("big.bin", chunk_size=4)
    assert b"".join([chunk async for chunk in stream]) == b"chunk1-chunk2"
    page, cursor = backend.list_page_sync(limit=1)
    assert page == ["b.bin"]
    assert cursor is not None
    assert await backend.list_page_async(cursor=cursor, limit=1) == (["big.bin"], None)
    with pytest.raises(ValueError, match="limit"):
        backend.list_page_sync(limit=0)
    with pytest.raises(ValueError, match="limit"):
        await backend.list_page_async(limit=0)


@pytest.mark.anyio
async def test_sqlspec_seekable_reader_factories(tmp_path: Path) -> None:
    """Native readers preserve seek/read/close and their metadata lookup behavior."""
    backend = SQLSpecObjectStoreBackend(store=SyncObjectStore.from_local(tmp_path), base_path="tenant-a")
    backend.write_bytes_sync("big.bin", b"chunk1-chunk2")
    with backend.open_sync("big.bin", chunk_size=3) as reader:
        assert reader.read(7) == b"chunk1-"
        assert reader.seek(0) == 0
        assert reader.read() == b"chunk1-chunk2"
    assert reader.closed
    async with backend.open_async("big.bin", size=13, chunk_size=3) as async_reader:
        assert await async_reader.read(7) == b"chunk1-"
        assert async_reader.seek(0) == 0
        assert await async_reader.read() == b"chunk1-chunk2"
    assert async_reader.closed
    with pytest.raises(FileNotFoundError):
        backend.open_sync("missing.bin")
    missing = backend.open_sync("missing.bin", size=1)
    with pytest.raises(FileNotFoundError):
        missing.read(1)
    missing.close()
    async_missing = backend.open_async("missing.bin")
    with pytest.raises(FileNotFoundError):
        await async_missing.read(1)
    async_missing.close()


@pytest.mark.parametrize("path", ["../secret.txt", "safe/../secret.txt", "memory://bucket/../secret.txt"])
@pytest.mark.parametrize(
    "operation", ["read_bytes_sync", "get_stream_sync", "open_sync", "glob_sync", "list_page_sync", "sign_sync"]
)
def test_sqlspec_rejects_namespace_traversal(path: str, operation: str) -> None:
    """Every key-bearing synchronous seam refuses parent segments before namespace qualification."""
    backend = SQLSpecObjectStoreBackend("memory://bucket", base_path="tenant-a")
    with pytest.raises(ValueError, match="traversal"):
        result = getattr(backend, operation)(path)
        if operation == "get_stream_sync":
            list(result)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "operation", ["read_bytes_async", "get_stream_async", "open_async", "glob_async", "list_page_async", "sign_async"]
)
async def test_sqlspec_rejects_async_namespace_traversal(operation: str) -> None:
    """Asynchronous and reader-factory seams refuse parent key segments."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(), base_path="tenant-a")
    with pytest.raises(ValueError, match="traversal"):
        if operation == "get_stream_async":
            async for _ in backend.get_stream_async("../secret.txt"):
                pass
        elif operation == "open_async":
            backend.open_async("../secret.txt")
        else:
            await getattr(backend, operation)("../secret.txt")


@pytest.mark.parametrize("namespace", [{"base_path": "tenant/../other"}, {"prefix": "../other"}])
def test_sqlspec_rejects_configured_namespace_traversal(namespace: dict[str, Any]) -> None:
    """Parent segments in configured namespaces fail at construction."""
    with pytest.raises(ValueError, match="traversal"):
        SQLSpecObjectStoreBackend(MemoryConfig(), **namespace)


@pytest.mark.anyio
async def test_sqlspec_stream_options_and_failed_uploads() -> None:
    """Streaming uploads preserve write options and abort incomplete objects."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig(), base_path="tenant-a")
    backend.put_stream_sync("meta.bin", [b"sync"], content_type="text/plain", attributes={"tenant": "a"})
    meta = await backend.async_store.head("tenant-a/meta.bin")
    assert meta.content_type == "text/plain"
    assert meta.attributes == {"tenant": "a"}
    with pytest.raises(FileExistsError):
        backend.put_stream_sync("meta.bin", [b"rejected"], if_not_exists=True)

    async def chunks() -> AsyncIterator[bytes]:
        yield b"async-"
        yield b"payload"

    await backend.put_stream_async(
        "meta.bin", chunks(), if_match=meta.etag, content_type="text/markdown", attributes={"tenant": "b"}
    )
    assert backend.read_bytes_sync("meta.bin") == b"async-payload"
    updated = backend.sync_store.head("tenant-a/meta.bin")
    assert updated.content_type == "text/markdown"
    assert updated.attributes == {"tenant": "b"}
    with pytest.raises(OSError):
        await backend.put_stream_async("meta.bin", [b"rejected"], if_none_match="*")

    def broken_chunks() -> Iterator[bytes]:
        yield b"partial"
        raise RuntimeError("source failed")

    async def broken_async_chunks() -> AsyncIterator[bytes]:
        yield b"partial"
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        backend.put_stream_sync("failed-sync.bin", broken_chunks())
    with pytest.raises(RuntimeError, match="source failed"):
        await backend.put_stream_async("failed-async.bin", broken_async_chunks())
    assert not backend.exists_sync("failed-sync.bin")
    assert not await backend.exists_async("failed-async.bin")


@pytest.mark.anyio
async def test_sqlspec_unsupported_signing_preserves_adapter_error() -> None:
    """Unconfigured native signing is reported using SQLSpec's adapter contract."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig())
    assert backend.supports_signing is False
    with pytest.raises(NotImplementedError, match="signing not supported"):
        backend.sign_sync("object.bin")
    with pytest.raises(NotImplementedError, match="signing not supported"):
        await backend.sign_async(["object.bin"], for_upload=True)


@pytest.mark.anyio
@pytest.mark.parametrize("for_upload", [False, True], ids=["get", "put"])
async def test_sqlspec_signing_propagates_native_argument_errors(for_upload: bool) -> None:
    """Configured signing surfaces the native expiry error unchanged instead of reporting missing support."""
    signing = SigningConfig(provider="hmac", hmac_secret="sqlspec-native-error", endpoint="https://signed.example.com")
    backend = SQLSpecObjectStoreBackend("memory://", signing=signing)
    assert backend.supports_signing is True
    with pytest.raises(ValueError, match="expires_in_secs"):
        backend.sign_sync("k", expires_in=0, for_upload=for_upload)
    with pytest.raises(ValueError, match="expires_in_secs"):
        await backend.sign_async("k", expires_in=0, for_upload=for_upload)


@pytest.mark.anyio
@pytest.mark.parametrize("for_upload", [False, True], ids=["get", "put"])
async def test_alchemy_signing_propagates_native_argument_errors(for_upload: bool) -> None:
    """Configured Advanced Alchemy signing surfaces the native expiry error unchanged."""
    signing = SigningConfig(provider="hmac", hmac_secret="alchemy-native-error", endpoint="https://signed.example.com")
    backend = AlchemyObjectStoreBackend(fs="memory://", signing=signing)
    assert backend.supports_signing is True
    with pytest.raises(ValueError, match="expires_in_secs"):
        backend.sign("k", expires_in=0, for_upload=for_upload)
    with pytest.raises(ValueError, match="expires_in_secs"):
        await backend.sign_async("k", expires_in=0, for_upload=for_upload)


@pytest.mark.parametrize("async_handle", [False, True], ids=["sync-store", "async-store"])
@pytest.mark.parametrize(
    "auth_kwargs",
    [
        {"signing": SigningConfig(provider="hmac", hmac_secret="ignored", endpoint="https://ignored.example.com")},
        {"token_cache": SyncGcpTokenCache({"access_token": "ignored"}, allow_ambient_adc=False)},
    ],
    ids=["signing", "token-cache"],
)
def test_sqlspec_rejects_auth_options_with_prebuilt_store(async_handle: bool, auth_kwargs: dict[str, Any]) -> None:
    """Auth options for a prebuilt store fail loudly and leave the caller's store untouched."""
    sync_store = SyncObjectStore("memory://")
    store = sync_store.to_async() if async_handle else sync_store
    with pytest.raises(ImproperConfigurationError, match="StorageConfig or URI"):
        SQLSpecObjectStoreBackend(store=store, **auth_kwargs)
    assert store.token_cache is None
    assert store.supports_signing is False


def test_sqlspec_object_store_backend_protocol_arrow_and_sync_pipeline() -> None:
    """SQLSpec ObjectStoreBackend satisfies ObjectStoreProtocol and drives SyncStoragePipeline + PyArrow."""
    store = SyncObjectStore(
        "memory://",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret="sqlspec-export-signing-key",
            endpoint="https://exports.example.com",
        ),
    )
    registry = SQLSpecStorageRegistry()
    register_storage_alias("lake", store, registry=registry, base_path="warehouse")

    backend = registry.get("lake")
    assert isinstance(backend, SQLSpecObjectStoreBackend)
    assert isinstance(backend, ObjectStoreProtocol)
    assert backend.supports_signing is True

    pipeline = SyncStoragePipeline(registry=registry)
    rows = [
        {"id": 1, "metric": "latency_ms", "value": 1.25},
        {"id": 2, "metric": "qps", "value": 185000.0},
    ]
    row_telemetry = pipeline.write_rows(rows, "alias://lake/exports/metrics.jsonl")
    assert row_telemetry["rows_processed"] == 2
    assert row_telemetry["backend"] == "vertebra"
    raw_jsonl = b"".join(pipeline.stream_read("alias://lake/exports/metrics.jsonl", chunk_size=16))
    decoded_rows = [decode_json(line) for line in raw_jsonl.splitlines() if line]
    assert decoded_rows == rows

    table = pa.Table.from_pylist(rows)
    arrow_telemetry = pipeline.write_arrow(table, "alias://lake/exports/pipeline_metrics.parquet")
    assert arrow_telemetry["rows_processed"] == 2
    pipeline_table, read_telemetry = pipeline.read_arrow(
        "alias://lake/exports/pipeline_metrics.parquet",
        file_format="parquet",
    )
    assert read_telemetry["rows_processed"] == 2
    assert pipeline_table.to_pylist() == rows

    backend.write_arrow_sync("exports/metrics.parquet", table)
    parquet_raw = backend.read_bytes_sync("exports/metrics.parquet")
    assert parquet_raw[:4] == b"PAR1"
    assert parquet_raw[-4:] == b"PAR1"
    parquet_table = backend.read_arrow_sync("exports/metrics.parquet")
    assert parquet_table.to_pylist() == rows

    streamed_batches = list(backend.stream_arrow_sync("exports/metrics.parquet", batch_size=1))
    assert sum(batch.num_rows for batch in streamed_batches) == 2

    backend.write_arrow_sync("exports/metrics.ipc", table)
    ipc_table = backend.read_arrow_sync("exports/metrics.ipc")
    assert ipc_table.to_pylist() == rows

    signed_single = backend.sign_sync("exports/metrics.parquet", expires_in=900)
    assert "X-Vtb-Signature=" in signed_single
    signed_upload = backend.sign_sync(["exports/new.parquet"], expires_in=300, for_upload=True)
    assert len(signed_upload) == 1
    assert "X-Vtb-Method=PUT" in signed_upload[0]

    response = backend.stream_response(
        "exports/metrics.parquet",
        chunk_size=64,
        media_type="application/vnd.apache.parquet",
        filename="metrics.parquet",
    )
    assert isinstance(response, StreamingStorageResponse)
    assert response.headers["Content-Disposition"] == 'attachment; filename="metrics.parquet"'
    assert b"".join(bytes(chunk) for chunk in response.iter_chunks()) == parquet_raw

    descriptor = {"dataset": "metrics", "partition": "2026-10-31"}
    encoded = ArrowFlightSerializer.serialize_descriptor(descriptor)
    assert ArrowFlightSerializer.deserialize_descriptor(encoded) == descriptor


@pytest.mark.anyio
async def test_sqlspec_object_store_backend_async_pipeline_and_arrow_streaming(tmp_path: Path) -> None:
    """AsyncStoragePipeline streams bytes, Arrow tables, and copy/move/delete over ObjectStoreBackend."""
    local_store = SyncObjectStore.from_local(tmp_path)
    registry = SQLSpecStorageRegistry()
    register_storage_alias("async_lake", local_store, registry=registry, base_path="stage")
    pipeline = AsyncStoragePipeline(registry=registry)

    resolved_target = pipeline.resolve_destination("alias://async_lake/notes/readme.jsonl")
    assert resolved_target.protocol == "file"

    await pipeline.write_rows([{"note": "hello-sqlspec-async"}], "alias://async_lake/notes/readme.jsonl")

    chunks: list[bytes] = []
    async for chunk in await pipeline.stream_read_async("alias://async_lake/notes/readme.jsonl", chunk_size=6):
        chunks.append(chunk)
    assert decode_json(b"".join(chunks).strip()) == {"note": "hello-sqlspec-async"}

    backend = registry.get("async_lake")
    assert isinstance(backend, SQLSpecObjectStoreBackend)
    table = pa.Table.from_pylist([{"k": "a", "v": 10}, {"k": "b", "v": 20}, {"k": "c", "v": 30}])
    await pipeline.write_arrow(table, "alias://async_lake/tables/events.parquet")
    read_back, telemetry = await pipeline.read_arrow_async(
        "alias://async_lake/tables/events.parquet",
        file_format="parquet",
    )
    assert telemetry["backend"] == "vertebra"
    assert read_back.to_pylist() == table.to_pylist()

    async_rows: list[dict[str, object]] = []
    async for batch in backend.stream_arrow_async("tables/*.parquet", batch_size=2):
        async_rows.extend(batch.to_pylist())
    assert async_rows == [{"k": "a", "v": 10}, {"k": "b", "v": 20}, {"k": "c", "v": 30}]

    await backend.write_text_async("notes/plain.txt", "alpha-beta")
    assert await backend.read_text_async("notes/plain.txt") == "alpha-beta"
    await backend.copy_async("notes/plain.txt", "notes/plain_copy.txt")
    await backend.move_async("notes/plain_copy.txt", "notes/plain_moved.txt")
    listed = await backend.list_objects_async("notes")
    assert sorted(listed) == ["notes/plain.txt", "notes/plain_moved.txt", "notes/readme.jsonl"]

    meta = await backend.get_metadata_async("notes/plain_moved.txt")
    assert meta["size"] == len(b"alpha-beta")

    await backend.delete_async("notes/plain_moved.txt")
    assert not await backend.exists_async("notes/plain_moved.txt")


def test_duckdb_secret_config_sql_builder() -> None:
    """DuckDBSecretConfig renders cloud-agnostic CREATE OR REPLACE SECRET statements with escaped literals."""
    cfg = GCSConfig(bucket="lakehouse", access_token="mock-adc-bearer-token")
    token = cfg.resolve_adc_token()
    assert token == "mock-adc-bearer-token"

    sql = DuckDBSecretConfig(
        name="gcs_lake",
        secret_type="gcs",
        value={"bearer_token": token},
    ).to_create_secret_sql()
    assert sql == "CREATE OR REPLACE SECRET gcs_lake (TYPE gcs, BEARER_TOKEN 'mock-adc-bearer-token')"

    scoped_sql = DuckDBSecretConfig(
        name="gcs_lake",
        secret_type="gcs",
        value={"bearer_token": token},
        scope="gs://lakehouse/curated",
    ).to_create_secret_sql()
    assert (
        scoped_sql
        == "CREATE OR REPLACE SECRET gcs_lake (TYPE gcs, BEARER_TOKEN 'mock-adc-bearer-token', SCOPE 'gs://lakehouse/curated')"
    )

    multi_scoped_sql = DuckDBSecretConfig(
        name="gcs_lake",
        secret_type="gcs",
        value={"bearer_token": token},
        scope=("gs://lakehouse/raw", "gcs://lakehouse/raw"),
    ).to_create_secret_sql()
    assert (
        multi_scoped_sql
        == "CREATE OR REPLACE SECRET gcs_lake (TYPE gcs, BEARER_TOKEN 'mock-adc-bearer-token', SCOPE ['gs://lakehouse/raw', 'gcs://lakehouse/raw'])"
    )

    with pytest.raises(InterfaceError, match="Invalid SQL identifier"):
        DuckDBSecretConfig(
            name="bad-name;DROP TABLE x",
            secret_type="gcs",
            value={"bearer_token": token},
        ).to_create_secret_sql()


@pytest.mark.parametrize(
    "token_val",
    ["", "   "],
    ids=["empty-token", "blank-token"],
)
def test_duckdb_secret_config_rejects_blank_sensitive_values(token_val: str) -> None:
    """Blank credential strings fail loudly instead of emitting a broken DuckDB secret."""
    with pytest.raises(ImproperConfigurationError, match="must not be empty"):
        DuckDBSecretConfig(
            name="my_secret",
            secret_type="gcs",
            value={"bearer_token": token_val},
        ).to_create_secret_sql()


@pytest.mark.parametrize(
    ("token", "expected_literal"),
    [("tok'en", "'tok''en'"), ("''", "''''''"), ("mock-oauth-token.a-b_c/d+e=", "'mock-oauth-token.a-b_c/d+e='")],
    ids=["embedded-quote", "only-quotes", "oauth-charset"],
)
def test_duckdb_secret_config_escapes_quotes(token: str, expected_literal: str) -> None:
    """Single quotes inside secret values are doubled so the literal cannot terminate early."""
    sql = DuckDBSecretConfig(
        name="gcs_lake",
        secret_type="gcs",
        value={"bearer_token": token},
    ).to_create_secret_sql()
    assert sql == f"CREATE OR REPLACE SECRET gcs_lake (TYPE gcs, BEARER_TOKEN {expected_literal})"


@pytest.mark.parametrize("token", ["tok\x00en", "tok\ren", "tok\nen"], ids=["nul", "cr", "lf"])
def test_duckdb_secret_config_rejects_control_characters(token: str) -> None:
    """Secret values carrying NUL, CR, or LF characters are rejected before any SQL is rendered."""
    with pytest.raises(InterfaceError):
        DuckDBSecretConfig(
            name="gcs_lake",
            secret_type="gcs",
            value={"bearer_token": token},
        ).to_create_secret_sql()


@pytest.mark.skipif(native_duckdb is None, reason="vertebra._duckdb is not installed")
def test_duckdb_secret_config_executes_against_duckdb() -> None:
    """Rendered cloud-agnostic secrets execute cleanly against vertebra._duckdb."""
    assert native_duckdb is not None
    client = native_duckdb.SyncDuckDBClient()
    try:
        ddl = DuckDBSecretConfig(
            name="http_lake",
            secret_type="http",
            value={"bearer_token": "tok'en"},
        ).to_create_secret_sql()
        client.execute(ddl)
        secret = client.fetch_optional("SELECT name, type FROM duckdb_secrets() WHERE name = 'http_lake'")
        assert secret == {"name": "http_lake", "type": "http"}
    finally:
        client.close()


@pytest.mark.parametrize("target", [GCSConfig(bucket="lakehouse"), "gs://lakehouse"], ids=["config", "uri"])
@pytest.mark.parametrize("via_registry", [False, True], ids=["direct", "registered"])
@pytest.mark.anyio
async def test_alchemy_forwards_signing_and_token_cache(target: GCSConfig | str, via_registry: bool) -> None:
    """StorageConfig and URI targets build both stores with the forwarded SigningConfig and token cache."""
    signing = SigningConfig(provider="hmac", hmac_secret="alchemy-forward", endpoint="https://signed.example.com")
    token_cache = SyncGcpTokenCache({"access_token": "forwarded-token"}, allow_ambient_adc=False)
    registry = AlchemyStorageRegistry()
    if via_registry:
        backend = register_storage_backend(
            target, key="alchemy_forwarded", signing=signing, token_cache=token_cache, registry=registry
        )
    else:
        backend = AlchemyObjectStoreBackend("alchemy_forwarded", target, signing=signing, token_cache=token_cache)
    try:
        assert backend.sync_store.token_cache is token_cache
        assert isinstance(backend.async_store.token_cache, AsyncGcpTokenCache)
        assert backend.supports_signing is True
        assert backend.sync_store.supports_signing is True
        assert backend.async_store.supports_signing is True
        sync_url = backend.sign("one.bin")
        async_urls = await backend.sign_async(["one.bin"], for_upload=True)
        assert isinstance(sync_url, str)
        assert sync_url.startswith("https://signed.example.com/one.bin?")
        assert isinstance(async_urls, list)
        assert async_urls[0].startswith("https://signed.example.com/one.bin?")
    finally:
        if via_registry:
            registry.unregister_backend("alchemy_forwarded")


@pytest.mark.parametrize("async_handle", [False, True])
@pytest.mark.parametrize(
    "auth_kwargs",
    [
        {"signing": SigningConfig(provider="hmac", hmac_secret="ignored", endpoint="https://ignored.example.com")},
        {"token_cache": SyncGcpTokenCache({"access_token": "ignored"}, allow_ambient_adc=False)},
    ],
    ids=["signing", "token-cache"],
)
def test_alchemy_rejects_auth_options_with_prebuilt_store(async_handle: bool, auth_kwargs: dict[str, Any]) -> None:
    """Auth options for a prebuilt store fail loudly instead of being silently discarded."""
    sync_store = SyncObjectStore("memory://")
    store = sync_store.to_async() if async_handle else sync_store
    with pytest.raises(ImproperConfigurationError, match="StorageConfig or URI"):
        AlchemyObjectStoreBackend(fs=store, **auth_kwargs)


def test_sqlspec_storage_module_drops_ipc_buffer_wrapper() -> None:
    """Raw IPC buffers flow directly to the shared batch codec without a wrapper type."""
    assert not hasattr(sqlspec_storage_mod, "_IPCBuffer")


def test_sqlspec_public_methods_precede_private_helpers() -> None:
    """ObjectStoreBackend groups public sync methods, then public async methods, then private helpers."""
    names = [name for name in vars(SQLSpecObjectStoreBackend) if not name.startswith("__")]
    callables = [name for name in names if callable(getattr(SQLSpecObjectStoreBackend, name, None))]
    private_start = next(index for index, name in enumerate(callables) if name.startswith("_"))
    assert all(name.startswith("_") for name in callables[private_start:])
    public = callables[:private_start]
    first_async = next(index for index, name in enumerate(public) if name.endswith("_async"))
    assert all(name.endswith("_async") for name in public[first_async:])


@pytest.mark.anyio
async def test_alchemy_storage_backend_file_object_and_session_tracker_rollback(tmp_path: Path) -> None:
    """ObjectStoreBackend integrates with Advanced Alchemy FileObject, StoredObject, and FileObjectSessionTracker."""
    store = SyncObjectStore(
        f"file://{tmp_path}",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret="alchemy-attachment-secret",
            endpoint="https://attachments.example.com",
        ),
    )
    registry = AlchemyStorageRegistry()
    backend = register_storage_backend(
        store,
        key="vtb_attachments",
        prefix="uploads",
        set_default=True,
        registry=registry,
    )
    assert isinstance(backend, StorageBackend)
    assert isinstance(backend, AlchemyObjectStoreBackend)
    assert registry.get_backend("vtb_attachments") is backend

    col_type = StoredObject(backend=backend)
    assert col_type is not None

    avatar = FileObject(
        backend=backend,
        filename="users/42/avatar.png",
        content_type="image/png",
    )
    saved = avatar.save(b"\x89PNG\r\n\x1a\nfake-image")
    assert saved.size == 18
    assert saved.etag is not None
    assert avatar.get_content() == b"\x89PNG\r\n\x1a\nfake-image"

    signed_url = avatar.sign(expires_in=1200)
    assert "X-Vtb-Signature=" in signed_url
    signed_batch = backend.sign(["users/42/avatar.png"], expires_in=600, for_upload=True)
    assert len(signed_batch) == 1
    assert "X-Vtb-Method=PUT" in signed_batch[0]

    report = FileObject(
        backend=backend,
        filename="users/42/report.pdf",
        content_type="application/pdf",
    )
    await report.save_async(memoryview(b"%PDF-1.7-async-content"))
    assert report.size == 22
    assert await report.get_content_async() == b"%PDF-1.7-async-content"

    source_file = tmp_path / "source_doc.txt"
    source_file.write_bytes(b"path-based-async-upload")
    path_obj = FileObject(backend=backend, filename="users/42/source_doc.txt")
    await backend.save_object_async(path_obj, source_file)
    assert path_obj.size == 23
    assert await backend.get_content_async("users/42/source_doc.txt") == b"path-based-async-upload"

    class _AsyncReadable:
        __slots__ = ()

        async def read(self) -> bytes:
            return b"awaitable-read-stream"

    stream_obj = FileObject(backend=backend, filename="users/42/stream.bin")
    await backend.save_object_async(stream_obj, cast("Any", _AsyncReadable()))
    assert stream_obj.size == 21
    assert await backend.get_content_async("users/42/stream.bin") == b"awaitable-read-stream"
    assert backend.get_range("users/42/stream.bin", 0, 9) == b"awaitable"
    assert await backend.get_range_async("users/42/stream.bin", 10, 14) == b"read"
    assert backend.exists("users/42/stream.bin") is True
    assert await backend.exists_async("users/42/stream.bin") is True

    backend.copy_object("users/42/stream.bin", "users/42/stream_copy.bin")
    await backend.copy_object_async("users/42/stream_copy.bin", "users/42/stream_copy_async.bin")
    listed_sync = backend.list_objects("users/42")
    listed_async = await backend.list_objects_async("users/42")
    assert len(listed_sync) >= 5
    assert [m.path for m in listed_sync] == [m.path for m in listed_async]

    multipart_obj = FileObject(backend=backend, filename="users/42/multipart.bin")
    await backend.save_object_async(multipart_obj, b"0123456789abcdef", use_multipart=True, chunk_size=4)
    assert multipart_obj.size == 16
    assert await backend.get_content_async("users/42/multipart.bin") == b"0123456789abcdef"

    tracker = FileObjectSessionTracker(raise_on_error=True)
    staged_obj = FileObject(backend=backend, filename="users/42/tx_orphan.bin")
    tracker.add_pending_save(staged_obj, b"staged-in-tx")
    await tracker.commit_async()
    assert await backend.get_content_async("users/42/tx_orphan.bin") == b"staged-in-tx"

    rollback_tracker = FileObjectSessionTracker(raise_on_error=True)
    rollback_tracker._saved_in_transaction.add(staged_obj)
    await rollback_tracker.rollback_async()
    assert not store.exists("uploads/users/42/tx_orphan.bin")

    backend.delete_object("users/42/avatar.png")
    await backend.delete_object_async(
        [
            "users/42/report.pdf",
            "users/42/source_doc.txt",
            "users/42/stream.bin",
            "users/42/stream_copy.bin",
            "users/42/stream_copy_async.bin",
            "users/42/multipart.bin",
        ]
    )


@pytest.mark.anyio
async def test_streaming_storage_response_chunks_match_across_sync_and_async_iteration() -> None:
    """A store-backed StreamingStorageResponse yields identical bounded chunks through sync and async iteration."""
    store = SyncObjectStore.from_memory("streaming-export-test")
    store.put("blobs/sample.bin", b"abcdefghij")
    resp = StreamingStorageResponse(store=store, key="blobs/sample.bin", total_size=10, chunk_size=4)
    sync_chunks = [bytes(c) for c in resp.iter_chunks()]
    async_chunks = [bytes(c) async for c in resp.aiter_chunks()]
    assert sync_chunks == [b"abcd", b"efgh", b"ij"]
    assert async_chunks == [b"abcd", b"efgh", b"ij"]


@pytest.mark.anyio
async def test_object_store_backend_write_arrow_cursor_and_uri_qualified_globs(tmp_path: Path) -> None:
    """Verify ObjectStoreBackend write_arrow_cursor_[sync|async], URI-qualified globs, and token_cache handoff."""
    cursor_part_0 = ArrowCursor.from_rows(
        [{"id": 1, "region": "us"}, {"id": 2, "region": "eu"}],
        threshold_rows=1,
    )
    cursor_part_1 = ArrowCursor.from_rows(
        [{"id": 3, "region": "apac"}, {"id": 4, "region": "us"}],
        threshold_rows=1,
    )

    mem_backend = SQLSpecObjectStoreBackend("memory://", base_path="warehouse")
    mem_backend.write_arrow_cursor_sync("snapshots/batch.ipc", cursor_part_0)
    ipc_table = mem_backend.read_arrow_sync("snapshots/batch.ipc")
    assert ipc_table.to_pylist() == [{"id": 1, "region": "us"}, {"id": 2, "region": "eu"}]

    token_cache = SyncGcpTokenCache({"access_token": "initial-token"}, allow_ambient_adc=False)
    gcs_sim_store = SyncObjectStore(LocalConfig(root=tmp_path / "gcs-sim"), token_cache=token_cache)
    gcs_backend = SQLSpecObjectStoreBackend("gs://analytics-bucket", store=gcs_sim_store)
    assert gcs_backend.sync_store.token_cache is token_cache
    assert gcs_backend.async_store.token_cache is not None
    assert isinstance(gcs_backend.async_store.token_cache, AsyncGcpTokenCache)

    gcs_backend.write_arrow_cursor_sync(
        "gs://analytics-bucket/events/dt=2026-11-01/part-0.parquet",
        cursor_part_0,
    )
    await gcs_backend.write_arrow_cursor_async(
        "gs://analytics-bucket/events/dt=2026-11-02/part-1.parquet",
        cursor_part_1,
    )

    sync_matches = gcs_backend.glob_sync("gs://analytics-bucket/events/dt=*/part-*.parquet")
    async_matches = await gcs_backend.glob_async("gs://analytics-bucket/events/dt=*/part-*.parquet")
    assert sorted(sync_matches) == [
        "events/dt=2026-11-01/part-0.parquet",
        "events/dt=2026-11-02/part-1.parquet",
    ]
    assert sorted(async_matches) == sorted(sync_matches)

    streamed_rows: list[dict[str, object]] = []
    for batch in gcs_backend.stream_arrow_sync("gs://analytics-bucket/events/dt=*/part-*.parquet"):
        streamed_rows.extend(batch.to_pylist())
    assert sorted(streamed_rows, key=lambda r: int(str(r["id"]))) == [
        {"id": 1, "region": "us"},
        {"id": 2, "region": "eu"},
        {"id": 3, "region": "apac"},
        {"id": 4, "region": "us"},
    ]

    with pytest.raises(ValueError, match="does not match store URI"):
        gcs_backend.glob_sync("gs://other-bucket/events/*.parquet")

    cursor_part_0.close()
    cursor_part_1.close()


def test_changefeed_sync_event_backend_lifecycle() -> None:
    """SQLSpec events hydrate stored spills, redeliver nacks, and clear queued work on shutdown."""
    store = SyncObjectStore("memory://")
    backend = ChangeFeedSyncEventBackend(config=CDCConfig(spill_threshold_bytes=16), store=store)
    assert not hasattr(backend, "build_emit_message_sql")
    assert backend.dequeue("orders", 0) is None
    first_id = backend.publish("orders", {"blob": "x" * 100})
    assert store.get("cdc/spill/1.json")
    second_ids = backend.publish_many([("orders", {"id": 2}, None), ("other", {"id": 3}, None)])
    assert second_ids == ["2", "3"]
    first = backend.dequeue("orders", 0)
    assert first is not None
    assert (first.event_id, first.channel, first.payload) == (first_id, "orders", {"blob": "x" * 100})
    backend.nack(first_id)
    redelivered = backend.dequeue("orders", 0)
    assert redelivered is not None
    assert (redelivered.event_id, redelivered.payload) == (first_id, first.payload)
    backend.ack(first_id)
    backend.nack(first_id)
    second = backend.dequeue("orders", 0)
    assert second is not None
    assert (second.event_id, second.payload) == ("2", {"id": 2})
    assert backend.dequeue("orders", 0) is None
    backend.shutdown()
    backend.nack("2")
    assert backend.dequeue("orders", 0) is None
    assert backend.dequeue("other", 0) is None


@pytest.mark.anyio
async def test_changefeed_async_event_backend_lifecycle() -> None:
    """The async SQLSpec backend preserves spilled payloads and acknowledgement lifecycle."""
    store = SyncObjectStore("memory://")
    backend = ChangeFeedAsyncEventBackend(config=CDCConfig(spill_threshold_bytes=16), store=store)
    assert not hasattr(backend, "build_emit_message_sql")
    assert await backend.dequeue("orders", 0) is None
    first_id = await backend.publish("orders", {"blob": "x" * 100})
    assert store.get("cdc/spill/1.json")
    second_ids = await backend.publish_many([("orders", {"id": 2}, None), ("other", {"id": 3}, None)])
    assert second_ids == ["2", "3"]
    first = await backend.dequeue("orders", 0)
    assert first is not None
    assert (first.event_id, first.channel, first.payload) == (first_id, "orders", {"blob": "x" * 100})
    await backend.nack(first_id)
    redelivered = await backend.dequeue("orders", 0)
    assert redelivered is not None
    assert (redelivered.event_id, redelivered.payload) == (first_id, first.payload)
    await backend.ack(first_id)
    await backend.nack(first_id)
    second = await backend.dequeue("orders", 0)
    assert second is not None
    assert (second.event_id, second.payload) == ("2", {"id": 2})
    assert await backend.dequeue("orders", 0) is None
    await backend.shutdown()
    await backend.nack("2")
    assert await backend.dequeue("orders", 0) is None
    assert await backend.dequeue("other", 0) is None


_RFC3720_DIGITS = b"123456789"
_RFC3720_DIGITS_CRC32C = "e3069283"
_RFC3720_SEQ32 = bytes(range(32))
_RFC3720_SEQ32_CRC32C = "46dd794e"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlspec_integrity_read_write_and_stream_options(asynchronous: bool) -> None:
    """SQLSpec ObjectStoreBackend advertises direct Arrow writes and enforces integrity options."""
    assert SQLSpecObjectStoreBackend.supports_direct_arrow_write is True
    backend = SQLSpecObjectStoreBackend("memory://")
    assert backend.supports_direct_arrow_write is True

    payload = _RFC3720_DIGITS
    exp_sha = hashlib.sha256(payload).hexdigest()
    exp_crc = _RFC3720_DIGITS_CRC32C
    exp_md5 = hashlib.md5(payload).hexdigest()

    if asynchronous:
        await backend.write_bytes_async(
            "bytes/item.bin",
            payload,
            checksum=True,
            expected_sha256=exp_sha,
            expected_crc32c=exp_crc,
            expected_md5=exp_md5,
        )
        assert await backend.read_bytes_async("bytes/item.bin", expected_sha256=exp_sha) == payload
        with pytest.raises(IntegrityError):
            await backend.read_bytes_async("bytes/item.bin", expected_sha256="0" * 64)

        async def async_chunks() -> AsyncIterator[bytes]:
            yield payload[:4]
            yield payload[4:]

        stream_res = await backend.put_stream_async(
            "stream/item.bin", async_chunks(), checksum=True, expected_sha256=exp_sha
        )
        assert (stream_res.sha256, stream_res.crc32c, stream_res.md5) == (exp_sha, exp_crc, exp_md5)
    else:
        backend.write_bytes_sync(
            "bytes/item.bin",
            payload,
            checksum=True,
            expected_sha256=exp_sha,
            expected_crc32c=exp_crc,
            expected_md5=exp_md5,
        )
        assert backend.read_bytes_sync("bytes/item.bin", expected_sha256=exp_sha) == payload
        with pytest.raises(IntegrityError):
            backend.read_bytes_sync("bytes/item.bin", expected_sha256="0" * 64)

        stream_res = backend.put_stream_sync(
            "stream/item.bin", [payload[:4], payload[4:]], checksum=True, expected_sha256=exp_sha
        )
        assert (stream_res.sha256, stream_res.crc32c, stream_res.md5) == (exp_sha, exp_crc, exp_md5)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlspec_arrow_write_options_normalization_and_validation(asynchronous: bool) -> None:
    """SQLSpec normalizes format_hint, write_options, and None compression while failing on bad kwargs."""
    backend = SQLSpecObjectStoreBackend("memory://")
    table = pa.table({"id": [1, 2, 3, 4], "city": ["nyc", "sfo", "lax", "sea"]})

    if asynchronous:
        await backend.write_arrow_async(
            "norm/zstd.data",
            table,
            format_hint="parquet",
            compression="zstd",
            write_options={"row_group_size": 2},
            checksum=True,
        )
        raw = await backend.read_bytes_async("norm/zstd.data")
        roundtrip = await backend.read_arrow_async("norm/zstd.data", file_format="parquet")
        await backend.write_arrow_async("norm/default.parquet", table, compression=None)
        with pytest.raises(TypeError):
            await backend.write_arrow_async("norm/bad.parquet", table, unknown_parquet_codec_opt=True)
    else:
        backend.write_arrow_sync(
            "norm/zstd.data",
            table,
            format_hint="parquet",
            compression="zstd",
            write_options={"row_group_size": 2},
            checksum=True,
        )
        raw = backend.read_bytes_sync("norm/zstd.data")
        roundtrip = backend.read_arrow_sync("norm/zstd.data", file_format="parquet")
        backend.write_arrow_sync("norm/default.parquet", table, compression=None)
        with pytest.raises(TypeError):
            backend.write_arrow_sync("norm/bad.parquet", table, unknown_parquet_codec_opt=True)

    assert roundtrip.to_pydict() == table.to_pydict()
    meta = pq.ParquetFile(BytesIO(raw)).metadata
    assert meta.num_row_groups == 2
    assert meta.row_group(0).column(0).compression == "ZSTD"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("method", ["write_arrow", "write_arrow_cursor"])
async def test_sqlspec_arrow_integrity_mismatch_preserves_prior_object(asynchronous: bool, method: str) -> None:
    """SQLSpec Arrow write methods reject mismatched expected digests and preserve prior bytes."""
    backend = SQLSpecObjectStoreBackend("memory://")
    key = f"arrow/{method}.parquet"
    sentinel = b"sentinel-before-mismatch"
    backend.write_bytes_sync(key, sentinel)
    table = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    payload: Any = table if method == "write_arrow" else table.to_reader()

    with pytest.raises(IntegrityError):
        if asynchronous:
            if method == "write_arrow":
                await backend.write_arrow_async(key, payload, expected_sha256="0" * 64)
            else:
                await backend.write_arrow_cursor_async(key, payload, expected_sha256="0" * 64)
        elif method == "write_arrow":
            backend.write_arrow_sync(key, payload, expected_sha256="0" * 64)
        else:
            backend.write_arrow_cursor_sync(key, payload, expected_sha256="0" * 64)

    assert backend.read_bytes_sync(key) == sentinel


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlspec_midstream_reader_failure_aborts_multipart_upload(asynchronous: bool) -> None:
    """Mid-stream RecordBatchReader failure aborts multipart upload and keeps prior bytes intact."""
    backend = SQLSpecObjectStoreBackend("memory://")
    key = "cursor/abort.parquet"
    prior = b"existing-valid-parquet"
    backend.write_bytes_sync(key, prior)
    schema = pa.schema([("id", pa.int64())])

    def failing_stream() -> Iterator[pa.RecordBatch]:
        yield pa.record_batch([pa.array([1, 2])], schema=schema)
        raise RuntimeError("broken arrow reader")

    reader = pa.RecordBatchReader.from_batches(schema, failing_stream())
    with pytest.raises(RuntimeError, match="broken arrow reader"):
        if asynchronous:
            await backend.write_arrow_cursor_async(key, reader)
        else:
            backend.write_arrow_cursor_sync(key, reader)

    assert backend.read_bytes_sync(key) == prior


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlspec_large_arrow_write_emits_bounded_chunks(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct SQLSpec Arrow writes stream 200k rows as bounded chunks instead of a monolithic buffer."""
    backend = SQLSpecObjectStoreBackend("memory://")
    row_count = 200_000
    table = pa.table(
        {
            "id": pa.array(range(row_count), type=pa.int64()),
            "tag": pa.array([f"item-{idx:06d}" for idx in range(row_count)]),
        }
    )
    chunk_sizes: list[int] = []

    if asynchronous:
        orig_async_put_stream = AsyncObjectStore.put_stream

        async def recording_async_put_stream(self: AsyncObjectStore, path: str, chunks: Any, **kwargs: Any) -> Any:
            async def tracked() -> AsyncIterator[bytes]:
                async for chunk in chunks:
                    raw = bytes(chunk)
                    chunk_sizes.append(len(raw))
                    yield raw

            return await orig_async_put_stream(self, path, tracked(), **kwargs)

        monkeypatch.setattr(AsyncObjectStore, "put_stream", recording_async_put_stream)
        await backend.write_arrow_async("large/table.parquet", table, row_group_size=10_000)
        read_back = await backend.read_arrow_async("large/table.parquet")
    else:
        orig_sync_put_stream = SyncObjectStore.put_stream

        def recording_sync_put_stream(self: SyncObjectStore, path: str, chunks: Any, **kwargs: Any) -> Any:
            def tracked() -> Iterator[bytes]:
                for chunk in chunks:
                    raw = bytes(chunk)
                    chunk_sizes.append(len(raw))
                    yield raw

            return orig_sync_put_stream(self, path, tracked(), **kwargs)

        monkeypatch.setattr(SyncObjectStore, "put_stream", recording_sync_put_stream)
        backend.write_arrow_sync("large/table.parquet", table, row_group_size=10_000)
        read_back = backend.read_arrow_sync("large/table.parquet")

    assert read_back.num_rows == row_count
    assert len(chunk_sizes) >= 20
    assert max(chunk_sizes) < table.nbytes / 5


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("mode", ["buffer", "file", "stream"])
async def test_alchemy_save_object_integrity_options(tmp_path: Path, asynchronous: bool, mode: str) -> None:
    """Advanced Alchemy ObjectStoreBackend enforces checksum and expected digests across input modes."""
    backend = AlchemyObjectStoreBackend(key="alchemy-int", fs="memory://")
    payload = _RFC3720_SEQ32
    exp_sha = hashlib.sha256(payload).hexdigest()
    exp_crc = _RFC3720_SEQ32_CRC32C
    exp_md5 = hashlib.md5(payload).hexdigest()
    key = f"alchemy/{mode}.bin"

    def make_source(raw: bytes) -> Any:
        if mode == "buffer":
            return raw
        if mode == "file":
            src = tmp_path / f"{mode}-{len(raw)}.bin"
            src.write_bytes(raw)
            return src
        return iter((raw[:16], raw[16:]))

    file_obj = FileObject(backend="alchemy-int", filename=key, content_type="application/octet-stream")
    if asynchronous:
        saved = await backend.save_object_async(
            file_obj,
            make_source(payload),
            checksum=True,
            expected_sha256=exp_sha,
            expected_crc32c=exp_crc,
            expected_md5=exp_md5,
        )
    else:
        saved = backend.save_object(
            file_obj,
            make_source(payload),
            checksum=True,
            expected_sha256=exp_sha,
            expected_crc32c=exp_crc,
            expected_md5=exp_md5,
        )

    assert saved.size == len(payload)
    assert bytes(backend.sync_store.get(key)) == payload

    with pytest.raises(IntegrityError):
        if asynchronous:
            await backend.save_object_async(file_obj, make_source(payload), expected_sha256="0" * 64)
        else:
            backend.save_object(file_obj, make_source(payload), expected_sha256="0" * 64)

    assert bytes(backend.sync_store.get(key)) == payload


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("file_format", ["parquet", "arrow-ipc"])
async def test_alchemy_save_arrow_streaming_and_checksum(
    asynchronous: bool, file_format: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Alchemy save_arrow and save_arrow_async stream batches natively without asyncio.to_thread."""
    if asynchronous:

        async def reject_to_thread(*args: object, **kwargs: object) -> Any:
            raise AssertionError("asyncio.to_thread is forbidden on the async storage path")

        monkeypatch.setattr(asyncio, "to_thread", reject_to_thread)

    backend = AlchemyObjectStoreBackend(key="alchemy-arrow", fs="memory://")
    table = pa.table({"id": [10, 20, 30], "kind": ["a", "b", "c"]})
    ext = "parquet" if file_format == "parquet" else "arrow"
    key = f"datasets/sample.{ext}"

    if asynchronous:
        res = await backend.save_arrow_async(key, table, format=file_format, checksum=True)
    else:
        res = backend.save_arrow(key, table, format=file_format, checksum=True)

    stored = bytes(backend.sync_store.get(key))
    assert res.sha256 == hashlib.sha256(stored).hexdigest()
    assert res.md5 == hashlib.md5(stored).hexdigest()
    assert res.crc32c is not None and len(res.crc32c) == 8
    assert backend.sync_store.put("datasets/verify-crc.bin", stored, expected_crc32c=res.crc32c).crc32c == res.crc32c

    with pytest.raises(IntegrityError):
        if asynchronous:
            await backend.save_arrow_async(key, table, format=file_format, expected_sha256="0" * 64)
        else:
            backend.save_arrow(key, table, format=file_format, expected_sha256="0" * 64)

    assert bytes(backend.sync_store.get(key)) == stored


@pytest.mark.anyio
@pytest.mark.parametrize("adapter", ["sqlspec", "alchemy"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("raw_format", "expected_parquet"),
    [
        ("Parquet", True),
        (" PARQUET ", True),
        ("ARROW", False),
        ("Feather", False),
        ("IPC", False),
    ],
)
async def test_arrow_adapters_normalize_case_insensitive_formats(
    adapter: str, asynchronous: bool, raw_format: str, expected_parquet: bool
) -> None:
    """SQLSpec and Alchemy Arrow writers normalize case-insensitive format names to Parquet or Arrow IPC."""
    table = pa.table({"id": [1, 2], "tag": ["x", "y"]})
    if adapter == "sqlspec":
        sqlspec_backend = SQLSpecObjectStoreBackend("memory://")
        if asynchronous:
            await sqlspec_backend.write_arrow_async("items/out.parquet", table, file_format=raw_format)
            raw = await sqlspec_backend.read_bytes_async("items/out.parquet")
            decoded = await sqlspec_backend.read_arrow_async("items/out.parquet", file_format=raw_format)
        else:
            sqlspec_backend.write_arrow_sync("items/out.parquet", table, file_format=raw_format)
            raw = sqlspec_backend.read_bytes_sync("items/out.parquet")
            decoded = sqlspec_backend.read_arrow_sync("items/out.parquet", file_format=raw_format)
        assert decoded.equals(table)
    else:
        alchemy_backend = AlchemyObjectStoreBackend(key="alchemy-fmt", fs="memory://")
        if asynchronous:
            await alchemy_backend.save_arrow_async("items/out.parquet", table, format=raw_format)
        else:
            alchemy_backend.save_arrow("items/out.parquet", table, format=raw_format)
        raw = bytes(alchemy_backend.sync_store.get("items/out.parquet"))

    assert (raw[:4] == b"PAR1" and raw[-4:] == b"PAR1") is expected_parquet


@pytest.mark.anyio
@pytest.mark.parametrize("adapter", ["sqlspec", "alchemy"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("bad_format", ["csv", "jsonl", "json", "orc"])
async def test_arrow_adapters_reject_unsupported_formats(adapter: str, asynchronous: bool, bad_format: str) -> None:
    """SQLSpec and Alchemy Arrow methods reject non-Arrow formats instead of silently encoding IPC."""
    table = pa.table({"id": [1]})
    if adapter == "sqlspec":
        sqlspec_backend = SQLSpecObjectStoreBackend("memory://")
        with pytest.raises(ValueError, match="Unsupported Arrow format"):
            if asynchronous:
                await sqlspec_backend.write_arrow_async("items/out.parquet", table, file_format=bad_format)
            else:
                sqlspec_backend.write_arrow_sync("items/out.parquet", table, file_format=bad_format)
        with pytest.raises(ValueError, match="Unsupported Arrow format"):
            if asynchronous:
                await sqlspec_backend.read_arrow_async("items/out.parquet", file_format=bad_format)
            else:
                sqlspec_backend.read_arrow_sync("items/out.parquet", file_format=bad_format)
    else:
        alchemy_backend = AlchemyObjectStoreBackend(key="alchemy-bad-fmt", fs="memory://")
        with pytest.raises(ValueError, match="Unsupported Arrow format"):
            if asynchronous:
                await alchemy_backend.save_arrow_async("items/out.parquet", table, format=bad_format)
            else:
                alchemy_backend.save_arrow("items/out.parquet", table, format=bad_format)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("digest_attr", ["sha256", "crc32c", "md5"])
async def test_django_storage_forwards_content_digests(asynchronous: bool, digest_attr: str) -> None:
    """DjangoStorage forwards optional string sha256/crc32c/md5 attributes and ignores non-string attributes."""
    store = SyncObjectStore("memory://")
    storage = DjangoStorage(store=store, file_overwrite=True)
    payload = _RFC3720_DIGITS
    digests = {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "crc32c": _RFC3720_DIGITS_CRC32C,
        "md5": hashlib.md5(payload).hexdigest(),
    }

    valid = ContentFile(payload, name="verified.bin")
    setattr(valid, digest_attr, digests[digest_attr])
    saved_name = await storage.asave("verified.bin", valid) if asynchronous else storage.save("verified.bin", valid)
    assert bytes(store.get(saved_name)) == payload

    callable_attr = ContentFile(payload, name="callable.bin")
    setattr(callable_attr, digest_attr, lambda: "callable-method-not-a-digest")
    callable_saved = (
        await storage.asave("callable.bin", callable_attr)
        if asynchronous
        else storage.save("callable.bin", callable_attr)
    )
    assert bytes(store.get(callable_saved)) == payload

    tampered = ContentFile(b"tampered-payload", name="verified.bin")
    setattr(tampered, digest_attr, digests[digest_attr])
    with pytest.raises(IntegrityError) as exc_info:
        if asynchronous:
            await storage.asave("verified.bin", tampered)
        else:
            storage.save("verified.bin", tampered)
    assert type(exc_info.value) is IntegrityError
    assert bytes(store.get(saved_name)) == payload


class _InMemorySyncNotifySource:
    """In-memory SyncNotificationSource recording emitted notify frame sizes."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.subscribed: list[str] = []
        self._frames: list[NotificationEvent] = []

    def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        self.published.append((channel, raw))
        self._frames.append(NotificationEvent(channel=channel, payload=raw))

    def subscribe(self, channels: Sequence[str]) -> None:
        self.subscribed.extend(channels)

    def unsubscribe(self, channels: Sequence[str]) -> None:
        for ch in channels:
            if ch in self.subscribed:
                self.subscribed.remove(ch)

    def listen(self) -> Iterator[NotificationEvent]:
        while self._frames:
            yield self._frames.pop(0)


class _InMemoryAsyncNotifySource:
    """In-memory AsyncNotificationSource recording emitted notify frame sizes."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.subscribed: list[str] = []
        self._frames: list[NotificationEvent] = []

    async def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        self.published.append((channel, raw))
        self._frames.append(NotificationEvent(channel=channel, payload=raw))

    async def subscribe(self, channels: Sequence[str]) -> None:
        self.subscribed.extend(channels)

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        for ch in channels:
            if ch in self.subscribed:
                self.subscribed.remove(ch)

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        while self._frames:
            yield self._frames.pop(0)


def test_changefeed_sync_checkpoint_restart_and_notify_spill(tmp_path: Path) -> None:
    """Sync ChangeFeed backend persists checkpoints across restarts and spills >8000B notify frames."""
    db_path = tmp_path / "sync_changefeed.db"
    cfg = SQLiteSyncConfig(connection_config={"database": str(db_path)})
    queue_store = SQLiteEventQueueStore(cfg)
    notify = _InMemorySyncNotifySource()

    backend = ChangeFeedSyncEventBackend(
        queue_store=queue_store,
        notify_source=notify,
        checkpoint_key="worker-orders",
    )
    small_id = backend.publish("orders", {"id": 1}, {"source": "cdc"})
    assert len(notify.published[0][1]) <= 7999
    large_blob = "v" * 9200
    large_id = backend.publish("orders", {"blob": large_blob}, {"oversized": True})
    assert len(notify.published[1][1]) < 300

    first_msg = backend.dequeue("orders", 0.0)
    assert first_msg is not None
    assert first_msg.event_id == small_id
    assert first_msg.payload == {"id": 1}
    backend.ack(first_msg.event_id)

    second_msg = backend.dequeue("orders", 0.0)
    assert second_msg is not None
    assert second_msg.event_id == large_id
    assert second_msg.payload == {"blob": large_blob}
    assert second_msg.metadata == {"oversized": True}
    backend.ack(second_msg.event_id)
    backend.save_checkpoint("orders", "lsn:0/ABCDEF")
    backend.shutdown()

    restarted_cfg = SQLiteSyncConfig(connection_config={"database": str(db_path)})
    restarted_backend = ChangeFeedSyncEventBackend(
        queue_store=SQLiteEventQueueStore(restarted_cfg),
        notify_source=_InMemorySyncNotifySource(),
        checkpoint_key="worker-orders",
    )
    assert restarted_backend.load_checkpoint("orders") == "lsn:0/ABCDEF"


@pytest.mark.anyio
async def test_changefeed_async_checkpoint_restart_and_notify_spill(tmp_path: Path) -> None:
    """Async ChangeFeed backend persists checkpoints across restarts and spills >8000B notify frames."""
    db_path = tmp_path / "async_changefeed.db"
    cfg = SQLiteAsyncConfig(connection_config={"database": str(db_path)})
    queue_store = SQLiteEventQueueStore(cfg)
    notify = _InMemoryAsyncNotifySource()

    backend = ChangeFeedAsyncEventBackend(
        queue_store=queue_store,
        notify_source=notify,
        checkpoint_key="worker-orders-async",
    )
    small_id = await backend.publish("orders", {"id": 1}, {"source": "cdc"})
    assert len(notify.published[0][1]) <= 7999
    large_blob = "a" * 9200
    large_id = await backend.publish("orders", {"blob": large_blob}, {"oversized": True})
    assert len(notify.published[1][1]) < 300

    first_msg = await backend.dequeue("orders", 0.0)
    assert first_msg is not None
    assert first_msg.event_id == small_id
    assert first_msg.payload == {"id": 1}
    await backend.ack(first_msg.event_id)

    second_msg = await backend.dequeue("orders", 0.0)
    assert second_msg is not None
    assert second_msg.event_id == large_id
    assert second_msg.payload == {"blob": large_blob}
    assert second_msg.metadata == {"oversized": True}
    await backend.ack(second_msg.event_id)
    assert await backend.load_checkpoint("orders") == large_id
    await backend.shutdown()

    restarted_cfg = SQLiteAsyncConfig(connection_config={"database": str(db_path)})
    restarted_backend = ChangeFeedAsyncEventBackend(
        queue_store=SQLiteEventQueueStore(restarted_cfg),
        notify_source=_InMemoryAsyncNotifySource(),
        checkpoint_key="worker-orders-async",
    )
    assert await restarted_backend.load_checkpoint("orders") == large_id
