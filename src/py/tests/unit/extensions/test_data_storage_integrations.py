"""Unit tests for SQLSpec and Alchemy storage adapters and plugins."""

import threading
from pathlib import Path

import pytest

from vertebra.extensions.alchemy import (
    AlchemyPlugin,
    FileAttachment,
    ObjectStoreBackend,
    SQLAlchemyState,
)
from vertebra.extensions.sqlspec import (
    SQLSPEC_STATE_KEY,
    SQLSpecPlugin,
    SQLSpecState,
    SqlSpecStorageBridge,
    SQLSpecStore,
    StreamingStorageResponse,
    SyncDatabaseConfig,
)
from vertebra.storage import ObjectStore, SigningConfig, StorageRegistry


def test_sqlspec_storage_adapter_crud_jsonl_arrow_parquet_and_streaming() -> None:
    """Verify SQLSpecStore CRUD, globbing, JSONL, Arrow IPC, Parquet export, and chunked HTTP streaming."""
    store = ObjectStore(
        "memory://",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret="sqlspec-export-signing-key",
            endpoint="https://exports.example.com",
        ),
    )
    adapter = SQLSpecStore(store, prefix="warehouse")

    adapter.write_bytes("raw/payload.bin", memoryview(b"0123456789abcdef"))
    assert adapter.exists("raw/payload.bin")
    assert adapter.read_bytes("raw/payload.bin") == b"0123456789abcdef"
    assert adapter.read_range("raw/payload.bin", offset=4, length=6) == b"456789"

    meta = adapter.get_metadata("raw/payload.bin")
    assert meta["path"] == "warehouse/raw/payload.bin"
    assert meta["size"] == 16

    rows = [{"id": 1, "metric": "latency", "value": 4.2}, {"id": 2, "metric": "qps", "value": 125000.0}]
    jsonl_meta = adapter.export_jsonl("exports/metrics.jsonl", rows)
    assert jsonl_meta.size > 0
    decoded_rows = adapter.read_jsonl("exports/metrics.jsonl")
    assert decoded_rows == rows

    arrow_buf = memoryview(b"ARROW1\x00\x00ipc-stream-payload")
    arrow_meta = adapter.export_arrow_ipc("exports/metrics.arrow", arrow_buf)
    assert arrow_meta.size == len(arrow_buf)
    assert bytes(adapter.read_arrow_ipc("exports/metrics.arrow")) == b"ARROW1\x00\x00ipc-stream-payload"

    parquet_meta = adapter.export_parquet("exports/metrics.parquet", rows)
    assert parquet_meta.size > 0
    assert adapter.read_parquet("exports/metrics.parquet") == rows

    assert adapter.glob("*.jsonl") == ["exports/metrics.jsonl"]
    signed = adapter.presign_download("exports/metrics.parquet", expires_in_secs=900, timestamp=1_700_000_000)
    assert "X-Vtb-Signature=" in signed

    response = adapter.stream_response(
        "raw/payload.bin",
        chunk_size=6,
        media_type="application/octet-stream",
        filename="payload.bin",
    )
    assert isinstance(response, StreamingStorageResponse)
    assert response.media_type == "application/octet-stream"
    assert response.headers["Content-Disposition"] == 'attachment; filename="payload.bin"'
    chunks = [bytes(chunk) for chunk in response.iter_chunks()]
    assert chunks == [b"012345", b"6789ab", b"cdef"]


@pytest.mark.anyio
async def test_sqlspec_storage_adapter_async_and_plugin() -> None:
    """Verify async SQLSpecStore methods, async chunk streaming, and SQLSpecPlugin initialization."""
    store = ObjectStore("memory://")
    adapter = SQLSpecStore(store, prefix="async_lake")

    await adapter.write_bytes_async("events/batch.bin", b"async-batch-bytes")
    assert await adapter.exists_async("events/batch.bin")
    assert await adapter.read_bytes_async("events/batch.bin") == b"async-batch-bytes"
    assert await adapter.list_objects_async("events") == ["events/batch.bin"]

    collected: list[bytes] = []
    async for chunk in adapter.stream_object_chunks_async("events/batch.bin", chunk_size=5):
        collected.append(bytes(chunk))
    assert b"".join(collected) == b"async-batch-bytes"

    await adapter.delete_async("events/batch.bin")
    assert not await adapter.exists_async("events/batch.bin")

    plugin: SQLSpecPlugin[object] = SQLSpecPlugin(store=store, storage_prefix="lake")
    app_state: dict[str, object] = {}
    state = plugin.install_state(app_state)
    assert isinstance(state, SQLSpecState)
    assert app_state[SQLSPEC_STATE_KEY] is state
    assert state.object_store is not None


@pytest.mark.anyio
async def test_alchemy_storage_backend_and_plugin(tmp_path: Path) -> None:
    """Verify ObjectStoreBackend sync/async save, get, delete, sign, and AlchemyPlugin state registration."""
    store = ObjectStore(
        f"file://{tmp_path}",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret="alchemy-attachment-secret",
            endpoint="https://attachments.example.com",
        ),
    )
    backend = ObjectStoreBackend(key="attachments", store=store, prefix="uploads")

    attachment = FileAttachment(path="users/42/avatar.png", content_type="image/png")
    saved = backend.save_object(attachment, b"\x89PNG\r\n\x1a\nfake-image")
    assert saved.size == 18
    assert saved.etag is not None
    assert backend.get_content("users/42/avatar.png") == b"\x89PNG\r\n\x1a\nfake-image"

    signed_url = backend.sign_object_url("users/42/avatar.png", expires_in_secs=1200, timestamp=1_700_000_000)
    assert "X-Vtb-Signature=" in signed_url

    async_att = FileAttachment(path="users/42/report.pdf", content_type="application/pdf")
    await backend.save_object_async(async_att, memoryview(b"%PDF-1.7-async-content"))
    assert async_att.size == 22
    assert await backend.get_content_async("users/42/report.pdf") == b"%PDF-1.7-async-content"

    source_file = tmp_path / "source_doc.txt"
    source_file.write_bytes(b"path-based-async-upload")
    path_att = FileAttachment(path="users/42/source_doc.txt", content_type="text/plain")
    await backend.save_object_async(path_att, source_file)
    assert path_att.size == 23
    assert await backend.get_content_async("users/42/source_doc.txt") == b"path-based-async-upload"

    class _AsyncReadable:
        __slots__ = ()

        async def read(self) -> bytes:
            return b"awaitable-read-stream"

    stream_att = FileAttachment(path="users/42/stream.bin")
    await backend.save_object_async(stream_att, _AsyncReadable())
    assert stream_att.size == 21
    assert await backend.get_content_async("users/42/stream.bin") == b"awaitable-read-stream"

    backend.delete_object("users/42/avatar.png")
    await backend.delete_object_async(["users/42/report.pdf", "users/42/source_doc.txt", "users/42/stream.bin"])

    plugin: AlchemyPlugin[object] = AlchemyPlugin(store=store, storage_key="attachments")
    app_state: dict[str, object] = {}
    alchemy_state = plugin.install_state(app_state)
    assert isinstance(alchemy_state, SQLAlchemyState)
    assert app_state[AlchemyPlugin.STATE_KEY] is alchemy_state
    assert isinstance(alchemy_state.storage_backend, ObjectStoreBackend)


@pytest.mark.anyio
async def test_sqlspec_plugin_shutdown() -> None:
    """Verify SQLSpecPlugin.shutdown offloads sync driver close to SyncPool."""
    closed_threads: list[str] = []

    class DummySyncPool:
        def close(self) -> None:
            closed_threads.append(threading.current_thread().name)

    dummy = DummySyncPool()
    sync_cfg = SyncDatabaseConfig(
        bind_key="primary_db",
        pool_instance=dummy,
    )
    plugin = SQLSpecPlugin(config=sync_cfg)
    app_state: dict[str, object] = {}
    plugin.install_state(app_state)

    await plugin.shutdown()
    assert len(closed_threads) == 1


def test_sqlspec_storage_bridge_delegates_to_registry() -> None:
    """Verify SqlSpecStorageBridge delegates alias and byte operations to StorageRegistry."""
    registry = StorageRegistry()
    store = ObjectStore("memory://")
    bridge = SqlSpecStorageBridge(registry)
    bridge.register_alias("primary", store)

    resolved = bridge.resolve_store("primary")
    assert resolved is store

    bridge.put_bytes("primary/data.txt", b"bridge-bytes")
    assert bridge.get_bytes("primary/data.txt") == b"bridge-bytes"

    bridge.put_json("primary/data.json", {"key": "value"})
    assert bridge.get_json("primary/data.json") == {"key": "value"}
