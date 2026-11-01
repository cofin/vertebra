"""Unit tests for real Advanced Alchemy StorageBackend and SQLSpec ObjectStoreProtocol backends."""

from __future__ import annotations

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
from vertebra.extensions.alchemy.storage import ObjectStoreBackend as AlchemyObjectStoreBackend
from vertebra.extensions.alchemy.storage import register_storage_backend
from vertebra.extensions.sqlspec.storage import (
    ArrowFlightSerializer,
    StreamingStorageResponse,
    build_duckdb_gcs_secret_sql,
    register_storage_alias,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend as SQLSpecObjectStoreBackend
from vertebra.storage import GCSConfig, ObjectStore, SigningConfig
from vertebra.utils.serializers import decode_json


def test_duplicated_storage_shims_removed_from_extension_modules() -> None:
    """Shadow FileAttachment, StoredObject, SQLSpecStore, and SqlSpecStorageBridge are deleted."""
    assert not hasattr(alchemy_storage_mod, "FileAttachment")
    assert not hasattr(alchemy_storage_mod, "StoredObject")
    assert not hasattr(sqlspec_storage_mod, "SQLSpecStore")
    assert not hasattr(sqlspec_storage_mod, "SqlSpecStorageBridge")


def test_sqlspec_object_store_backend_protocol_arrow_and_sync_pipeline() -> None:
    """SQLSpec ObjectStoreBackend satisfies ObjectStoreProtocol and drives SyncStoragePipeline + PyArrow."""
    store = ObjectStore(
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
    local_store = ObjectStore.from_local(tmp_path)
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
    store = ObjectStore(
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
    assert not await store.exists_async("uploads/users/42/tx_orphan.bin")

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
