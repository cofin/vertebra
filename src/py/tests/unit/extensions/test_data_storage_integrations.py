"""Unit tests for real Advanced Alchemy StorageBackend and SQLSpec ObjectStoreProtocol backends."""

from __future__ import annotations

import subprocess
import sys
from asyncio import CancelledError
from collections.abc import AsyncIterator, Iterator
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pytest
from advanced_alchemy.types.file_object import FileObject, StorageBackend, StoredObject
from advanced_alchemy.types.file_object.registry import StorageRegistry as AlchemyStorageRegistry
from advanced_alchemy.types.file_object.session_tracker import FileObjectSessionTracker
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.pipeline import AsyncStoragePipeline, SyncStoragePipeline
from sqlspec.storage.registry import StorageRegistry as SQLSpecStorageRegistry

import vertebra.extensions.alchemy.storage as alchemy_storage_mod
import vertebra.extensions.sqlspec.storage as sqlspec_storage_mod
from vertebra.db.arrow import ArrowCursor
from vertebra.db.cdc import CDCConfig
from vertebra.extensions.alchemy.storage import ObjectStoreBackend as AlchemyObjectStoreBackend
from vertebra.extensions.alchemy.storage import register_storage_backend
from vertebra.extensions.sqlspec.events import ChangeFeedAsyncEventBackend, ChangeFeedSyncEventBackend
from vertebra.extensions.sqlspec.storage import (
    ArrowFlightSerializer,
    StreamingStorageResponse,
    build_duckdb_gcs_secret_sql,
    register_storage_alias,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend as SQLSpecObjectStoreBackend
from vertebra.storage import (
    AsyncGcpTokenCache,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    SigningConfig,
    SyncGcpTokenCache,
    SyncObjectStore,
)
from vertebra.storage import (
    StreamingStorageResponse as StorageStreamingStorageResponse,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json


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
        def read(self, size: int = -1) -> bytes:
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
        assert (
            await obj.save_async(AsyncReader() if mode == "async-reader" else reader, use_multipart=True, chunk_size=4)
            is obj
        )
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
    """Unsupported native signing retains the upstream backend error contract."""
    backend = AlchemyObjectStoreBackend(fs=MemoryConfig())
    with pytest.raises(NotImplementedError):
        backend.sign("one.bin")
    with pytest.raises(NotImplementedError):
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
    assert backend.store.get("tenant-a/b.bin") == b"123"


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
def test_sqlspec_rejects_configured_namespace_traversal(namespace: dict[str, str]) -> None:
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
    with pytest.raises(NotImplementedError, match="signing not supported"):
        backend.sign_sync("object.bin")
    with pytest.raises(NotImplementedError, match="signing not supported"):
        await backend.sign_async(["object.bin"], for_upload=True)


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


def test_duckdb_gcs_secret_sql_builder() -> None:
    """build_duckdb_gcs_secret_sql emits parameterized DuckDB httpfs GCS secret DDL."""
    cfg = GCSConfig(bucket="lakehouse", access_token="mock-adc-bearer-token")
    sql, params = build_duckdb_gcs_secret_sql("gcs_lake", cfg)
    assert sql == "CREATE OR REPLACE SECRET gcs_lake (TYPE gcs, BEARER_TOKEN ?)"
    assert params == ("mock-adc-bearer-token",)

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        build_duckdb_gcs_secret_sql("bad-name;DROP TABLE x", cfg)


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
    await backend.save_object_async(stream_obj, _AsyncReadable())
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
async def test_streaming_storage_response_exported_from_vertebra_storage() -> None:
    """Verify StreamingStorageResponse is canonically exported from vertebra.storage and re-exported in sqlspec."""
    assert StorageStreamingStorageResponse is StreamingStorageResponse
    store = SyncObjectStore.from_memory("streaming-export-test")
    store.put("blobs/sample.bin", b"abcdefghij")
    resp = StorageStreamingStorageResponse(store=store, key="blobs/sample.bin", total_size=10, chunk_size=4)
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
    gcs_sim_store = SyncObjectStore.from_local(tmp_path / "gcs-sim")
    gcs_backend = SQLSpecObjectStoreBackend(
        "gs://analytics-bucket",
        store=gcs_sim_store,
        token_cache=token_cache,
    )
    assert gcs_backend.store.token_cache is token_cache
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
