"""Unit tests for real Advanced Alchemy StorageBackend and SQLSpec ObjectStoreProtocol backends."""

from __future__ import annotations

import io
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pytest
from advanced_alchemy.types.file_object import FileObject, StorageBackend, StoredObject, storages
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
from vertebra.extensions.starlette.storage import AsyncStorageRegistry, SyncStorageRegistry
from vertebra.storage import (
    AsyncGcpTokenCache,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    S3Config,
    SigningConfig,
    SyncGcpTokenCache,
    SyncObjectStore,
)
from vertebra.storage import (
    StreamingStorageResponse as StorageStreamingStorageResponse,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json


@pytest.mark.parametrize("adapter", ["alchemy", "sqlspec", "starlette-sync", "starlette-async"])
def test_framework_storage_rejects_r2_uri(adapter: str) -> None:
    """Removed provider URIs fail through every framework storage entry point."""
    config = "r2://unit"
    with pytest.raises(ValueError, match="Unsupported storage URI scheme 'r2'"):
        if adapter == "alchemy":
            AlchemyObjectStoreBackend(fs=config)
        elif adapter == "sqlspec":
            SQLSpecObjectStoreBackend(config)
        elif adapter == "starlette-sync":
            SyncStorageRegistry().register("unit", config)
        else:
            AsyncStorageRegistry().register("unit", config)


@pytest.mark.parametrize("adapter", ["alchemy", "sqlspec", "starlette-sync", "starlette-async"])
def test_framework_storage_accepts_s3_custom_endpoint(adapter: str) -> None:
    """Generic S3 custom endpoints retain native construction and SigV4 signing."""
    config = S3Config(
        bucket="unit",
        endpoint="http://127.0.0.1:9000",
        allow_http=True,
        access_key_id="test-only-key",
        secret_access_key="test-only-secret",
    )
    if adapter == "alchemy":
        store = AlchemyObjectStoreBackend(fs=config).sync_store
    elif adapter == "sqlspec":
        store = SQLSpecObjectStoreBackend(config).sync_store
    elif adapter == "starlette-sync":
        registry = SyncStorageRegistry()
        registry.register("unit", config)
        store = registry.get("unit")
    else:
        async_registry = AsyncStorageRegistry()
        async_registry.register("unit", config)
        store = async_registry.get("unit").to_sync()
    assert store.scheme == "s3"
    assert store.url == "s3://unit"
    signed_url = store.presign_get("key")
    assert signed_url.startswith("http://127.0.0.1:9000/unit/key?")
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in signed_url


def test_duplicated_storage_shims_removed_from_extension_modules() -> None:
    """Shadow FileAttachment, StoredObject, SQLSpecStore, and SqlSpecStorageBridge are deleted."""
    assert not hasattr(alchemy_storage_mod, "FileAttachment")
    assert not hasattr(alchemy_storage_mod, "StoredObject")
    assert not hasattr(sqlspec_storage_mod, "SQLSpecStore")
    assert not hasattr(sqlspec_storage_mod, "SqlSpecStorageBridge")


def test_sqlspec_storage_requires_explicit_target() -> None:
    """Missing and blank targets cannot silently select volatile memory storage."""
    with pytest.raises(ImproperConfigurationError):
        SQLSpecObjectStoreBackend()
    for target in (None, "", "  "):
        with pytest.raises(ImproperConfigurationError):
            SQLSpecObjectStoreBackend(target)


def test_sqlspec_storage_native_sync_operations(tmp_path: Path) -> None:
    """Native streams, pagination, readable handles, and conditional puts share one namespace."""
    backend = SQLSpecObjectStoreBackend(f"file://{tmp_path}", base_path="sql")
    assert isinstance(backend, ObjectStoreProtocol)
    backend.write_bytes_sync("a.bin", b"123", if_not_exists=True)
    with pytest.raises(FileExistsError):
        backend.write_bytes_sync("a.bin", b"overwrite", if_not_exists=True)
    backend.move_sync("a.bin", "b.bin")
    assert not backend.exists_sync("a.bin")
    backend.put_stream_sync("big.bin", iter([b"chunk1-", b"chunk2"]))
    assert b"".join(bytes(chunk) for chunk in backend.get_stream_sync("big.bin", chunk_size=4)) == b"chunk1-chunk2"
    paths, cursor = backend.list_page_sync(limit=1)
    assert paths == ["b.bin"]
    assert cursor is not None
    assert backend.list_page_sync(cursor=cursor, limit=1) == (["big.bin"], None)
    with backend.open_sync("b.bin") as reader:
        assert reader.read() == b"123"
    assert reader.closed
    with pytest.raises(NotImplementedError):
        backend.sign_sync("b.bin")
    with pytest.raises(ValueError, match="limit"):
        backend.list_page_sync(limit=0)
    backend.write_text_sync("note.txt", "hello", if_not_exists=True)
    with pytest.raises(FileExistsError):
        backend.write_text_sync("note.txt", "overwrite", if_not_exists=True)


@pytest.mark.anyio
async def test_sqlspec_storage_native_async_operations(tmp_path: Path) -> None:
    """Async protocol operations preserve conditional writes and incremental stream input."""
    backend = SQLSpecObjectStoreBackend(f"file://{tmp_path}", base_path="sql")
    await backend.write_bytes_async("a.bin", b"123", if_not_exists=True)
    with pytest.raises(FileExistsError):
        await backend.write_bytes_async("a.bin", b"overwrite", if_not_exists=True)
    await backend.move_async("a.bin", "b.bin")
    assert not await backend.exists_async("a.bin")
    await backend.put_stream_async("big.bin", [b"chunk1-", b"chunk2"])
    chunks = [bytes(chunk) async for chunk in backend.get_stream_async("big.bin", chunk_size=4)]
    assert b"".join(chunks) == b"chunk1-chunk2"
    paths, cursor = await backend.list_page_async(limit=1)
    assert paths == ["b.bin"]
    assert cursor is not None
    assert await backend.list_page_async(cursor=cursor, limit=1) == (["big.bin"], None)
    async with backend.open_async("b.bin") as reader:
        assert await reader.read() == b"123"
    assert reader.closed
    with pytest.raises(NotImplementedError):
        await backend.sign_async("b.bin")
    await backend.write_text_async("note.txt", "hello", if_not_exists=True)
    with pytest.raises(FileExistsError):
        await backend.write_text_async("note.txt", "overwrite", if_not_exists=True)


@pytest.mark.parametrize("path", ["../secret.txt", "nested/../../secret.txt", "memory://sql/../secret.txt"])
def test_sqlspec_storage_rejects_traversal(path: str) -> None:
    """Keys cannot traverse outside the configured object namespace."""
    backend = SQLSpecObjectStoreBackend("memory://sql", base_path="tenant-a")
    with pytest.raises(ValueError):
        backend.read_bytes_sync(path)


def test_sqlspec_storage_rejects_traversal_prefix() -> None:
    """Invalid base prefixes fail before any object operation."""
    with pytest.raises(ValueError, match="traversal"):
        SQLSpecObjectStoreBackend("memory://sql", base_path="../tenant-a")


@pytest.fixture
def unavailable_signing_endpoint() -> Iterator[tuple[str, list[str]]]:
    """Serve actual IAM signBlob failures without external credentials."""
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            """Keep local responder access logs out of test output."""

        def do_POST(self) -> None:
            """Return a provider error after consuming the native request."""
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append(self.path)
            body = b"signing-unavailable"
            self.send_response(503)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", requests
        finally:
            server.shutdown()
            thread.join(timeout=2)
            assert not thread.is_alive()


@pytest.mark.parametrize("for_upload", [False, True])
def test_sqlspec_signing_preserves_native_validation(for_upload: bool) -> None:
    """Configured signing validation remains distinct from unsupported signing."""
    store = SyncObjectStore(MemoryConfig(), signing=SigningConfig(provider="hmac", hmac_secret="test-only-key"))
    backend = SQLSpecObjectStoreBackend(store)
    with pytest.raises(ValueError) as raised:
        backend.sign_sync("key", expires_in=0, for_upload=for_upload)
    assert type(raised.value) is ValueError


@pytest.mark.anyio
@pytest.mark.parametrize("for_upload", [False, True])
async def test_sqlspec_async_signing_preserves_native_validation(for_upload: bool) -> None:
    """Async configured signing preserves the native validation exception."""
    store = SyncObjectStore(MemoryConfig(), signing=SigningConfig(provider="hmac", hmac_secret="test-only-key"))
    backend = SQLSpecObjectStoreBackend(store)
    with pytest.raises(ValueError) as raised:
        await backend.sign_async("key", expires_in=0, for_upload=for_upload)
    assert type(raised.value) is ValueError


@pytest.mark.parametrize("for_upload", [False, True])
def test_sqlspec_signing_preserves_provider_failure(
    for_upload: bool, unavailable_signing_endpoint: tuple[str, list[str]]
) -> None:
    """A real native IAM HTTP failure retains its exact error and message."""
    endpoint, requests = unavailable_signing_endpoint
    store = SyncObjectStore(
        MemoryConfig(),
        signing=SigningConfig(
            provider="gcs",
            bucket="unit",
            service_account_email="signer@example.test",
            access_token="mock-oauth-token",
            iam_endpoint=endpoint,
        ),
    )
    backend = SQLSpecObjectStoreBackend(store)
    with pytest.raises(RuntimeError) as raised:
        backend.sign_sync("key", for_upload=for_upload)
    assert type(raised.value) is RuntimeError
    assert str(raised.value) == "signing error: IAM signBlob returned HTTP 503 Service Unavailable"
    assert requests == ["/v1/projects/-/serviceAccounts/signer@example.test:signBlob"]


@pytest.mark.anyio
@pytest.mark.parametrize("for_upload", [False, True])
async def test_sqlspec_async_signing_preserves_provider_failure(
    for_upload: bool, unavailable_signing_endpoint: tuple[str, list[str]]
) -> None:
    """Async IAM failures propagate through the native async signer unchanged."""
    endpoint, requests = unavailable_signing_endpoint
    store = SyncObjectStore(
        MemoryConfig(),
        signing=SigningConfig(
            provider="gcs",
            bucket="unit",
            service_account_email="signer@example.test",
            access_token="mock-oauth-token",
            iam_endpoint=endpoint,
        ),
    )
    backend = SQLSpecObjectStoreBackend(store)
    with pytest.raises(RuntimeError) as raised:
        await backend.sign_async("key", for_upload=for_upload)
    assert type(raised.value) is RuntimeError
    assert str(raised.value) == "signing error: IAM signBlob returned HTTP 503 Service Unavailable"
    assert requests == ["/v1/projects/-/serviceAccounts/signer@example.test:signBlob"]


@pytest.mark.parametrize("paths", ["key", ["key", "other"]])
def test_sqlspec_signing_unsupported_scalar_and_list(paths: str | list[str]) -> None:
    """Unsigned stores report their backend instead of fabricating signing support."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig())
    with pytest.raises(NotImplementedError, match="memory"):
        backend.sign_sync(paths)


@pytest.mark.anyio
@pytest.mark.parametrize("paths", ["key", ["key", "other"]])
async def test_sqlspec_async_signing_unsupported_scalar_and_list(paths: str | list[str]) -> None:
    """Unsigned async stores retain the unsupported backend contract."""
    backend = SQLSpecObjectStoreBackend(MemoryConfig())
    with pytest.raises(NotImplementedError, match="memory"):
        await backend.sign_async(paths)


@pytest.mark.anyio
@pytest.mark.parametrize("for_upload", [False, True])
async def test_sqlspec_configured_signing_retains_scalar_list_shape(for_upload: bool) -> None:
    """Signed sync and async results retain key, method, and collection shape."""
    store = SyncObjectStore(MemoryConfig(), signing=SigningConfig(provider="hmac", hmac_secret="test-only-key"))
    backend = SQLSpecObjectStoreBackend(store, base_path="tenant")
    method = "PUT" if for_upload else "GET"
    for scalar in (
        backend.sign_sync("key", for_upload=for_upload),
        await backend.sign_async("key", for_upload=for_upload),
    ):
        assert isinstance(scalar, str)
        assert "/tenant/key?" in scalar
        assert store.verify_presigned_url(scalar, method=method)
    for urls in (
        backend.sign_sync(["key", "other"], for_upload=for_upload),
        await backend.sign_async(["key", "other"], for_upload=for_upload),
    ):
        assert isinstance(urls, list)
        assert len(urls) == 2
        assert all(store.verify_presigned_url(url, method=method) for url in urls)
        assert "/tenant/key?" in urls[0]
        assert "/tenant/other?" in urls[1]


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


@pytest.mark.parametrize("target", [None, "", "   "])
def test_alchemy_storage_rejects_missing_configuration(target: str | None) -> None:
    """Backend construction and key-only registration require a configured store."""
    with pytest.raises(ImproperConfigurationError):
        AlchemyObjectStoreBackend(key="avatars", fs=target)
    with pytest.raises(ImproperConfigurationError):
        register_storage_backend("avatars", store=target)


def test_alchemy_storage_registry_and_bounded_file_upload(tmp_path: Path) -> None:
    """Configured public registry helpers preserve registry APIs and consume bounded reads."""
    registry = AlchemyStorageRegistry()
    registry_get = getattr(registry, "get", None)
    backend = register_storage_backend("avatars", store=LocalConfig(root=tmp_path), registry=registry)
    assert registry.get_backend("avatars") is backend
    assert getattr(registry, "get", None) is registry_get
    assert hasattr(storages, "get") == hasattr(AlchemyStorageRegistry, "get")

    class BoundedReader(io.BytesIO):
        """Reject unbounded reads from a caller-provided file stream."""

        def read(self, size: int = -1) -> bytes:
            assert 0 < size <= 4
            return super().read(size)

    avatar = FileObject(backend=backend, filename="u1.png", content_type="image/png")
    backend.save_object(avatar, BoundedReader(b"chunk1-chunk2"), chunk_size=4)
    assert avatar.size == 13
    assert backend.get_content("u1.png") == b"chunk1-chunk2"
    alias_backend = AlchemyObjectStoreBackend(key="direct", store=LocalConfig(root=tmp_path))
    assert alias_backend.get_content("u1.png") == b"chunk1-chunk2"


@pytest.mark.anyio
async def test_alchemy_storage_async_bounded_streams(tmp_path: Path) -> None:
    """Async file reads and iterable uploads retain bytes and clean up aborted writes."""
    backend = AlchemyObjectStoreBackend(key="avatars", fs=LocalConfig(root=tmp_path))

    class AsyncReader:
        """Expose an asynchronous bounded file input."""

        def __init__(self) -> None:
            self.source = io.BytesIO(b"chunk1-chunk2")

        async def read(self, size: int) -> bytes:
            assert 0 < size <= 4
            return self.source.read(size)

    avatar = FileObject(backend=backend, filename="u1.png", content_type="image/png")
    await backend.save_object_async(avatar, AsyncReader(), chunk_size=4)
    assert avatar.size == 13
    assert await backend.get_content_async("u1.png") == b"chunk1-chunk2"

    async def chunks():
        yield b"part1-"
        yield b"part2"

    streamed = FileObject(backend=backend, filename="iter.bin")
    await backend.save_object_async(streamed, chunks())
    assert streamed.size == 11
    assert await backend.get_content_async("iter.bin") == b"part1-part2"

    async def broken_chunks():
        yield b"first"
        raise ValueError("source failed")

    with pytest.raises(ValueError, match="source failed"):
        await backend.save_object_async(FileObject(backend=backend, filename="aborted.bin"), broken_chunks())
    assert not await backend.exists_async("aborted.bin")


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


@pytest.mark.anyio
async def test_storage_adapters_preserve_raw_keys_and_validate_prefixes() -> None:
    """Real Alchemy and SQLSpec storage surfaces preserve literal key identity."""
    store = SyncObjectStore("memory://")
    alchemy = AlchemyObjectStoreBackend(key="raw", fs=store, prefix="aa")
    sqlspec = SQLSpecObjectStoreBackend(store=store, base_path="sql")
    values = {"café%": b"unicode", "literal%2F": b"percent", "literal%252F": b"double", "dir/file": b"nested"}
    for key, value in values.items():
        alchemy.save_object(FileObject(backend=alchemy, filename=key), value)
        await alchemy.save_object_async(FileObject(backend=alchemy, filename=key), value)
        sqlspec.write_bytes_sync(key, value)
        await sqlspec.write_bytes_async(key, value)
        assert alchemy.get_content(key) == value
        assert await alchemy.get_content_async(key) == value
        assert sqlspec.read_bytes_sync(key) == value
    assert sorted(item.path for item in alchemy.list_objects("/")) == sorted(f"aa/{key}" for key in values)
    assert sorted(await sqlspec.list_objects_async("/")) == sorted(values)
    assert [item.path for item in await alchemy.list_objects_async("dir/")] == ["aa/dir/file"]
    assert sqlspec.list_objects_sync("dir/") == ["dir/file"]
    assert sqlspec.is_path_sync("dir/")
    assert sqlspec.is_path_sync("/")
    for invalid in (" café%", "café% ", "dir//file", r"dir\file", "dir/./file"):
        for operation in (alchemy.get_content, sqlspec.read_bytes_sync):
            with pytest.raises(ValueError):
                operation(invalid)
        for operation in (alchemy.get_content_async, sqlspec.read_bytes_async):
            with pytest.raises(ValueError):
                await operation(invalid)
        for source, destination in (("café%", invalid), (invalid, "literal%2F")):
            for copy_operation in (alchemy.copy_object, sqlspec.copy_sync, sqlspec.move_sync):
                with pytest.raises(ValueError):
                    copy_operation(source, destination)
            for async_copy_operation in (alchemy.copy_object_async, sqlspec.copy_async, sqlspec.move_async):
                with pytest.raises(ValueError):
                    await async_copy_operation(source, destination)
        assert alchemy.get_content("café%") == b"unicode"
        assert alchemy.get_content("literal%2F") == b"percent"
        assert sqlspec.read_bytes_sync("café%") == b"unicode"
        assert sqlspec.read_bytes_sync("literal%2F") == b"percent"
    for prefix in (" dir", "/dir", "dir//"):
        with pytest.raises(ValueError):
            alchemy.list_objects(prefix)
        with pytest.raises(ValueError):
            await sqlspec.list_objects_async(prefix)
        with pytest.raises(ValueError):
            sqlspec.is_path_sync(prefix)


@pytest.mark.anyio
async def test_sqlspec_qualified_paths_keep_raw_identity_and_reject_mismatch(tmp_path: Path) -> None:
    """Matching local/cloud qualifications retain raw names without outside-root effects."""
    local = SQLSpecObjectStoreBackend(LocalConfig(root=tmp_path))
    target = tmp_path / "dir/literal%2F"
    local.write_bytes_sync(target, b"local")
    assert await local.read_bytes_async(f"file://{target}") == b"local"
    await local.write_bytes_async("dir/literal%252F", b"distinct")
    assert local.read_bytes_sync("dir/literal%252F") == b"distinct"
    assert sorted(await local.list_objects_async("dir/")) == ["dir/literal%252F", "dir/literal%2F"]
    assert sorted(local.list_objects_sync("/")) == ["dir/literal%252F", "dir/literal%2F"]
    assert await local.list_objects_async(f"file://{tmp_path}/dir/", recursive=False) == [
        "dir/literal%252F",
        "dir/literal%2F",
    ]
    with pytest.raises(ValueError):
        local.write_bytes_sync(tmp_path.parent / "outside", b"wrong")
    with pytest.raises(ValueError):
        await local.write_bytes_async(f"file://other{target}", b"wrong")
    with pytest.raises(ValueError):
        local.write_bytes_sync(f"file://{tmp_path}/dir//alias", b"wrong")
    remote = SQLSpecObjectStoreBackend("gs://bucket/root", store=SyncObjectStore("memory://"), base_path="scope")
    remote.write_bytes_sync("gs://bucket/root/literal%2F", b"cloud")
    assert await remote.read_bytes_async("gcs://bucket/root/literal%2F") == b"cloud"
    for invalid in ("s3://bucket/root/literal%2F", "gs://other/root/literal%2F", "gs:///root/literal%2F"):
        with pytest.raises(ValueError):
            remote.copy_sync("literal%2F", invalid)
        with pytest.raises(ValueError):
            await remote.move_async(invalid, "literal%2F")
    assert remote.read_bytes_sync("literal%2F") == b"cloud"
    assert await remote.list_objects_async() == ["literal%2F"]
    remote.write_bytes_sync("literal%252F", b"double")
    assert remote.glob_sync("gs://bucket/root/literal%?F") == ["literal%2F"]
    assert await remote.glob_async("gs://bucket/root/literal%25?F") == ["literal%252F"]
