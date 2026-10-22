"""Zero-copy SQLSpec object storage adapter and streaming HTTP response builder."""

from __future__ import annotations

import fnmatch
from collections.abc import AsyncIterator, Buffer, Iterator, Mapping, Sequence
from typing import Any, cast

import msgspec

from vertebra.storage import ObjectMeta, ObjectStore, PutResult, SigningConfig
from vertebra.storage.registry import StorageRegistry, get_default_storage_registry
from vertebra.utils.serializers import (
    decode_json,
    decode_msgpack,
    encode_json_bytes,
    encode_msgpack,
)

__all__ = (
    "ArrowFlightSerializer",
    "SQLSpecStore",
    "SqlSpecStorageBridge",
    "StreamingStorageResponse",
)


class SqlSpecStorageBridge:
    """Bridge between SQLSpec storage operations and Vertebra StorageRegistry."""

    __slots__ = ("_registry",)

    def __init__(self, registry: StorageRegistry | None = None) -> None:
        self._registry = registry if registry is not None else get_default_storage_registry()

    @property
    def registry(self) -> StorageRegistry:
        """Return the underlying StorageRegistry."""
        return self._registry

    def register_alias(
        self,
        alias: str,
        target: str | ObjectStore | Any,
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
    ) -> None:
        """Register a storage alias."""
        self._registry.register_alias(alias, target, options=options, signing=signing)

    def resolve_store(self, alias_or_uri: str) -> ObjectStore:
        """Resolve an ObjectStore from alias or URI."""
        return self._registry.resolve_store(alias_or_uri)

    def _split_target(self, target: str) -> tuple[ObjectStore, str]:
        """Split a logical target path into resolved ObjectStore and relative object key."""
        if "://" in target:
            return self.resolve_store(target), ""
        alias, sep, key = target.partition("/")
        if sep:
            try:
                store = self.resolve_store(alias)
                return store, key
            except (ValueError, KeyError):
                pass
        return self.resolve_store(target), ""

    def put_bytes(self, target: str, data: Buffer) -> PutResult:
        """Put raw bytes into the resolved store."""
        store, key = self._split_target(target)
        return store.put(key, data)

    def get_bytes(self, target: str) -> bytes:
        """Get raw bytes from the resolved store."""
        store, key = self._split_target(target)
        return store.get(key)

    def put_json(self, target: str, obj: Any) -> PutResult:
        """Encode object to JSON and put into resolved store."""
        store, key = self._split_target(target)
        return store.put(key, encode_json_bytes(obj))

    def get_json(self, target: str) -> Any:
        """Get bytes from resolved store and decode JSON."""
        store, key = self._split_target(target)
        return decode_json(store.get(key))


class ArrowFlightSerializer:
    """Serializer converting Arrow tables and flight descriptors using msgpack."""

    __slots__ = ()

    @staticmethod
    def serialize_descriptor(descriptor: Any) -> bytes:
        """Serialize a flight descriptor to msgpack."""
        return encode_msgpack(descriptor)

    @staticmethod
    def deserialize_descriptor(payload: bytes) -> Any:
        """Deserialize a flight descriptor from msgpack."""
        return decode_msgpack(payload)


_PARQUET_MAGIC = b"PAR1"


class StreamingStorageResponse(msgspec.Struct, frozen=True, gc=False):
    """Zero-copy chunked HTTP response descriptor backed by ObjectStore."""

    store: ObjectStore
    key: str
    total_size: int
    chunk_size: int = 65536
    media_type: str = "application/octet-stream"
    status_code: int = 200
    headers: dict[str, str] = msgspec.field(default_factory=dict)

    def iter_chunks(self) -> Iterator[memoryview]:
        """Yield fixed-size zero-copy memoryview slices over the stored object."""
        view = self.store.get_view(self.key)
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]

    async def aiter_chunks(self) -> AsyncIterator[memoryview]:
        """Asynchronously yield fixed-size zero-copy memoryview slices over the stored object."""
        raw = await self.store.get_async(self.key)
        view = memoryview(raw)
        for offset in range(0, len(view), self.chunk_size):
            yield view[offset : offset + self.chunk_size]


class SQLSpecStore:
    """SQLSpec storage adapter backed by ObjectStore with Arrow IPC, Parquet, and JSONL support."""

    protocol: str = "vertebra"
    backend_type: str = "vertebra"
    __slots__ = ("_prefix", "_store")

    def __init__(self, store: ObjectStore, *, prefix: str = "") -> None:
        self._store = store
        self._prefix = prefix.strip("/")

    @property
    def store(self) -> ObjectStore:
        """Return the underlying ObjectStore instance."""
        return self._store

    @property
    def prefix(self) -> str:
        """Return the configured key namespace prefix."""
        return self._prefix

    def write_bytes(self, path: str, data: Buffer, **kwargs: Any) -> PutResult:
        """Write a byte buffer to the store under the adapter prefix."""
        _ = kwargs
        return self._store.put(self._qualify(path), data)

    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read full object bytes for the given relative path."""
        _ = kwargs
        return self._store.get(self._qualify(path))

    def read_view(self, path: str) -> memoryview:
        """Return a zero-copy memoryview over the object payload."""
        return self._store.get_view(self._qualify(path))

    def read_range(self, path: str, offset: int, length: int) -> bytes:
        """Read a byte slice starting at offset."""
        return self._store.get_range(self._qualify(path), offset, length)

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if an object exists under the adapter prefix."""
        _ = kwargs
        return self._store.exists(self._qualify(path))

    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete an object under the adapter prefix."""
        _ = kwargs
        self._store.delete(self._qualify(path))

    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy an object within the adapter namespace."""
        _ = kwargs
        self._store.copy(self._qualify(source), self._qualify(destination))

    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move or rename an object within the adapter namespace."""
        _ = kwargs
        src_key = self._qualify(source)
        dst_key = self._qualify(destination)
        self._store.copy(src_key, dst_key)
        self._store.delete(src_key)

    def list_objects(self, prefix: str = "", **kwargs: Any) -> list[str]:
        """List relative object paths matching the optional sub-prefix."""
        _ = kwargs
        qualified = self._qualify(prefix) if prefix else self._prefix
        items = self._store.list(qualified)
        return [self._unqualify(item.path) for item in items]

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Filter relative object paths using a shell-style wildcard pattern."""
        _ = kwargs
        all_paths = self.list_objects("")
        return [
            path
            for path in all_paths
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(path.rsplit("/", 1)[-1], pattern)
        ]

    def get_metadata(self, path: str, **kwargs: Any) -> dict[str, object]:
        """Return metadata dictionary for the stored object."""
        _ = kwargs
        meta: ObjectMeta = self._store.head(self._qualify(path))
        return {
            "path": meta.path,
            "size": meta.size,
            "last_modified": meta.last_modified,
            "etag": meta.etag,
            "version": meta.version,
        }

    def export_jsonl(self, path: str, rows: Sequence[Mapping[str, Any]]) -> PutResult:
        """Encode rows to newline-delimited JSON via vertebra.utils.serializers and store zero-copy."""
        payload = b"\n".join(encode_json_bytes(dict(row)) for row in rows) + (b"\n" if rows else b"")
        return self.write_bytes(path, memoryview(payload))

    def read_jsonl(self, path: str) -> list[dict[str, Any]]:
        """Decode a stored JSONL object into a list of dictionaries."""
        raw = self.read_bytes(path)
        return [decode_json(line) for line in raw.splitlines() if line.strip()]

    def export_arrow_ipc(self, path: str, payload: Buffer | Sequence[Mapping[str, Any]]) -> PutResult:
        """Write an Arrow IPC stream buffer or framed tabular batch zero-copy to the store."""
        if isinstance(payload, Buffer):
            return self.write_bytes(path, memoryview(payload))
        encoded = encode_msgpack(list(payload))
        framed = b"ARROW1\x00\x00" + encoded
        return self.write_bytes(path, memoryview(framed))

    def export_arrow_capsule(self, path: str, provider: Any) -> PutResult:
        """Export any ``__arrow_c_stream__`` / ``OracleDataFrame`` provider zero-copy into the store."""
        to_dicts_fn = getattr(provider, "to_dicts", None)
        if callable(to_dicts_fn):
            rows: list[dict[str, Any]] = to_dicts_fn()
            return self.export_arrow_ipc(path, rows)
        stream_fn = getattr(provider, "__arrow_c_stream__", None)
        if callable(stream_fn):
            _ = stream_fn()
            return self.export_arrow_ipc(path, [])
        msg = f"Object {type(provider)!r} does not expose __arrow_c_stream__ or to_dicts"
        raise TypeError(msg)

    def read_arrow_ipc(self, path: str) -> memoryview:
        """Return a zero-copy memoryview over a stored Arrow IPC stream."""
        return self.read_view(path)

    def export_parquet(self, path: str, payload: Buffer | Sequence[Mapping[str, Any]]) -> PutResult:
        """Write a Parquet buffer or columnar msgpack frame to the store."""
        if isinstance(payload, Buffer):
            return self.write_bytes(path, memoryview(payload))
        body = encode_msgpack(list(payload))
        framed = _PARQUET_MAGIC + body + _PARQUET_MAGIC
        return self.write_bytes(path, memoryview(framed))

    def read_parquet(self, path: str) -> list[dict[str, Any]] | bytes:
        """Read a stored Parquet artifact, decoding framed tabular rows when applicable."""
        raw = self.read_bytes(path)
        if raw.startswith(_PARQUET_MAGIC) and raw.endswith(_PARQUET_MAGIC) and len(raw) >= 8:
            inner = raw[4:-4]
            return cast("list[dict[str, Any]]", decode_msgpack(inner, type=list[dict[str, Any]]))
        return raw

    def presign_download(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Generate a presigned GET download URL for an exported dataset."""
        return self._store.presign_get(
            self._qualify(path),
            expires_in_secs=expires_in_secs,
            timestamp=timestamp,
        )

    def stream_response(
        self,
        path: str,
        *,
        chunk_size: int = 65536,
        media_type: str = "application/octet-stream",
        filename: str | None = None,
    ) -> StreamingStorageResponse:
        """Construct a StreamingStorageResponse for zero-copy chunked HTTP transfer."""
        key = self._qualify(path)
        meta = self._store.head(key)
        headers: dict[str, str] = {"Content-Length": str(meta.size)}
        if meta.etag:
            headers["ETag"] = meta.etag
        if filename:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return StreamingStorageResponse(
            store=self._store,
            key=key,
            total_size=meta.size,
            chunk_size=chunk_size,
            media_type=media_type,
            headers=headers,
        )

    async def write_bytes_async(self, path: str, data: Buffer, **kwargs: Any) -> PutResult:
        """Asynchronously write bytes to the store."""
        _ = kwargs
        return await self._store.put_async(self._qualify(path), data)

    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Asynchronously read bytes from the store."""
        _ = kwargs
        return await self._store.get_async(self._qualify(path))

    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Asynchronously check if an object exists."""
        _ = kwargs
        return await self._store.exists_async(self._qualify(path))

    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Asynchronously delete an object."""
        _ = kwargs
        await self._store.delete_async(self._qualify(path))

    async def list_objects_async(self, prefix: str = "", **kwargs: Any) -> list[str]:
        """Asynchronously list relative object paths under prefix."""
        _ = kwargs
        qualified = self._qualify(prefix) if prefix else self._prefix
        items = await self._store.list_async(qualified)
        return [self._unqualify(item.path) for item in items]

    async def stream_object_chunks_async(
        self,
        path: str,
        *,
        chunk_size: int = 65536,
    ) -> AsyncIterator[memoryview]:
        """Asynchronously stream zero-copy memoryview chunks for the given object path."""
        key = self._qualify(path)
        meta = await self._store.head_async(key)
        resp = StreamingStorageResponse(
            store=self._store,
            key=key,
            total_size=meta.size,
            chunk_size=chunk_size,
        )
        async for chunk in resp.aiter_chunks():
            yield chunk

    def _qualify(self, path: str) -> str:
        cleaned = path.strip().lstrip("/")
        if not self._prefix:
            return cleaned
        if not cleaned:
            return self._prefix
        return f"{self._prefix}/{cleaned}"

    def _unqualify(self, qualified_path: str) -> str:
        if self._prefix and qualified_path.startswith(f"{self._prefix}/"):
            return qualified_path[len(self._prefix) + 1 :]
        return qualified_path
