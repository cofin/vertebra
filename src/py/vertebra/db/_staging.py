"""Shared multicloud `ObjectStore` staging resolution and Arrow serialization for database drivers."""

from __future__ import annotations

import csv
import inspect
import io
import os
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Callable

from vertebra.db.arrow import ArrowCursor, write_arrow_shm
from vertebra.storage import AsyncObjectStore, SyncObjectStore
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json

__all__ = ("ObjectStoreStaging",)

_REMOTE_SCHEMES: Final[frozenset[str]] = frozenset(
    {"gs", "gcs", "s3", "r2", "az", "azure", "abfs", "abfss", "oci", "http", "https"}
)


class ObjectStoreStaging:
    """Shared `ObjectStore` target validation, store resolution, and `ArrowCursor` read/write helpers."""

    @staticmethod
    def validate_gcs_uri(uri: str) -> None:
        """Validate that `uri` is a well-formed `gs://<bucket>/<object>` URI."""
        stripped = uri.strip()
        parsed = urlparse(stripped)
        if (
            parsed.scheme.lower() not in {"gs", "gcs"}
            or not parsed.netloc.strip()
            or not parsed.path.lstrip("/").strip()
        ):
            raise ImproperConfigurationError(f"Invalid GCS staging URI {uri!r}: expected 'gs://<bucket>/<object>'")

    @classmethod
    def validate_staging_target(cls, store: Any | None, uri: str) -> None:
        """Validate `(store, uri)` before executing an upstream database export query."""
        if store is None:
            cls._parse_implicit_uri(uri)
            return
        if isinstance(store, str):
            cls._reject_ephemeral_memory_url(store)
        if not uri.strip():
            raise ImproperConfigurationError("Staging destination URI must not be empty")

    @classmethod
    def write_cursor_sync(
        cls,
        cursor: ArrowCursor,
        *,
        destination_uri: str,
        store: Any | None,
        format: str,
        compression: str = "ZSTD",
    ) -> None:
        """Write `cursor` synchronously to an `ObjectStore`, `ObjectStoreBackend`, or scheme-qualified URI."""
        if store is not None and hasattr(store, "write_arrow_cursor_sync"):
            object_key = cls._resolve_backend_path(store, destination_uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"parquet", "arrow-ipc"}:
                comp = cls._normalize_compression(compression)
                if file_fmt == "parquet" and comp is not None:
                    store.write_arrow_cursor_sync(object_key, cursor, file_format=file_fmt, compression=comp)
                else:
                    store.write_arrow_cursor_sync(object_key, cursor, file_format=file_fmt)
                return
            payload = cls._serialize_cursor(cursor, format=format, compression=compression)
            if hasattr(store, "write_bytes_sync"):
                store.write_bytes_sync(object_key, payload)
                return
            if hasattr(store, "write_text_sync"):
                store.write_text_sync(object_key, payload.decode("utf-8"))
                return

        resolved_store, object_key = cls.resolve_sync_store(store, destination_uri)
        payload = cls._serialize_cursor(cursor, format=format, compression=compression)
        put_fn: Callable[[str, bytes], Any] | None = getattr(resolved_store, "put", None)
        if callable(put_fn):
            put_fn(object_key, payload)
            return
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(resolved_store).__name__!r}")

    @classmethod
    async def write_cursor_async(
        cls,
        cursor: ArrowCursor,
        *,
        destination_uri: str,
        store: Any | None,
        format: str,
        compression: str = "ZSTD",
    ) -> None:
        """Write `cursor` asynchronously to an `ObjectStore`, `ObjectStoreBackend`, or scheme-qualified URI."""
        if store is not None and hasattr(store, "write_arrow_cursor_async"):
            object_key = cls._resolve_backend_path(store, destination_uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"parquet", "arrow-ipc"}:
                comp = cls._normalize_compression(compression)
                if file_fmt == "parquet" and comp is not None:
                    await store.write_arrow_cursor_async(object_key, cursor, file_format=file_fmt, compression=comp)
                else:
                    await store.write_arrow_cursor_async(object_key, cursor, file_format=file_fmt)
                return
            payload = cls._serialize_cursor(cursor, format=format, compression=compression)
            if hasattr(store, "write_bytes_async"):
                await store.write_bytes_async(object_key, payload)
                return
            if hasattr(store, "write_text_async"):
                await store.write_text_async(object_key, payload.decode("utf-8"))
                return

        resolved_store, object_key = cls.resolve_async_store(store, destination_uri)
        payload = cls._serialize_cursor(cursor, format=format, compression=compression)
        put_fn: Callable[[str, bytes], Any] | None = getattr(resolved_store, "put", None)
        if callable(put_fn):
            res = put_fn(object_key, payload)
            if inspect.isawaitable(res):
                await res
            return
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(resolved_store).__name__!r}")

    @classmethod
    def read_cursor_sync(
        cls,
        uri: str,
        *,
        store: Any | None,
        format: str,
    ) -> ArrowCursor:
        """Read an `ArrowCursor` synchronously from an `ObjectStore`, `ObjectStoreBackend`, or URI."""
        if store is not None and hasattr(store, "read_arrow_sync"):
            object_key = cls._resolve_backend_path(store, uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"parquet", "arrow-ipc"}:
                res = store.read_arrow_sync(object_key, file_format=file_fmt)
                return cls._coerce_arrow_result(res, format=format)
            if hasattr(store, "read_bytes_sync"):
                raw_bytes = bytes(store.read_bytes_sync(object_key))
                return cls._deserialize_bytes(raw_bytes, format=format)
            if hasattr(store, "read_text_sync"):
                raw_text = store.read_text_sync(object_key)
                return cls._deserialize_bytes(raw_text.encode("utf-8"), format=format)

        resolved_store, object_key = cls.resolve_sync_store(store, uri)
        get_fn: Callable[[str], Any] | None = getattr(resolved_store, "get_bytes", None) or getattr(
            resolved_store, "get", None
        )
        if callable(get_fn):
            raw = bytes(get_fn(object_key))
            return cls._deserialize_bytes(raw, format=format)
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(resolved_store).__name__!r}")

    @classmethod
    async def read_cursor_async(
        cls,
        uri: str,
        *,
        store: Any | None,
        format: str,
    ) -> ArrowCursor:
        """Read an `ArrowCursor` asynchronously from an `ObjectStore`, `ObjectStoreBackend`, or URI."""
        if store is not None and hasattr(store, "read_arrow_async"):
            object_key = cls._resolve_backend_path(store, uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"parquet", "arrow-ipc"}:
                res = await store.read_arrow_async(object_key, file_format=file_fmt)
                return cls._coerce_arrow_result(res, format=format)
            if hasattr(store, "read_bytes_async"):
                raw_bytes = bytes(await store.read_bytes_async(object_key))
                return cls._deserialize_bytes(raw_bytes, format=format)
            if hasattr(store, "read_text_async"):
                raw_text = await store.read_text_async(object_key)
                return cls._deserialize_bytes(raw_text.encode("utf-8"), format=format)

        resolved_store, object_key = cls.resolve_async_store(store, uri)
        get_fn: Callable[[str], Any] | None = getattr(resolved_store, "get_bytes", None) or getattr(
            resolved_store, "get", None
        )
        if callable(get_fn):
            res = get_fn(object_key)
            if inspect.isawaitable(res):
                res = await res
            raw = bytes(res)
            return cls._deserialize_bytes(raw, format=format)
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(resolved_store).__name__!r}")

    @classmethod
    def resolve_sync_store(cls, store: Any | None, uri: str) -> tuple[Any, str]:
        """Resolve a synchronous `ObjectStore` and object key from `(store, uri)`."""
        if isinstance(store, SyncObjectStore):
            return store, cls._resolve_store_key(store, uri)
        if isinstance(store, AsyncObjectStore):
            sync_store = store.to_sync()
            return sync_store, cls._resolve_store_key(sync_store, uri)
        if isinstance(store, str):
            cls._reject_ephemeral_memory_url(store)
            sync_store = SyncObjectStore(store)
            return sync_store, cls._resolve_store_key(sync_store, uri)
        if store is not None:
            if hasattr(store, "to_url_and_options"):
                sync_store = SyncObjectStore.from_config(store)
                return sync_store, cls._resolve_store_key(sync_store, uri)
            return store, cls._resolve_store_key(store, uri)
        scheme, base_or_dir, key = cls._parse_implicit_uri(uri)
        if scheme == "file":
            return SyncObjectStore.from_local(base_or_dir, create_dirs=True), key
        return SyncObjectStore(base_or_dir), key

    @classmethod
    def resolve_async_store(cls, store: Any | None, uri: str) -> tuple[Any, str]:
        """Resolve an asynchronous `ObjectStore` and object key from `(store, uri)`."""
        if isinstance(store, AsyncObjectStore):
            return store, cls._resolve_store_key(store, uri)
        if isinstance(store, SyncObjectStore):
            if getattr(store, "scheme", "") == "memory":
                return store, cls._resolve_store_key(store, uri)
            async_store = store.to_async()
            return async_store, cls._resolve_store_key(async_store, uri)
        if isinstance(store, str):
            cls._reject_ephemeral_memory_url(store)
            async_store = AsyncObjectStore(store)
            return async_store, cls._resolve_store_key(async_store, uri)
        if store is not None:
            if hasattr(store, "to_url_and_options"):
                async_store = AsyncObjectStore.from_config(store)
                return async_store, cls._resolve_store_key(async_store, uri)
            return store, cls._resolve_store_key(store, uri)
        scheme, base_or_dir, key = cls._parse_implicit_uri(uri)
        if scheme == "file":
            return AsyncObjectStore.from_local(base_or_dir, create_dirs=True), key
        return AsyncObjectStore(base_or_dir), key

    @staticmethod
    def _reject_ephemeral_memory_url(url: str) -> None:
        if url.strip().lower().startswith("memory://"):
            raise ImproperConfigurationError("memory:// staging URIs require an explicit ObjectStore instance")

    @classmethod
    def _parse_implicit_uri(cls, uri: str) -> tuple[str, str, str]:
        stripped = uri.strip()
        if not stripped or "://" not in stripped:
            raise ImproperConfigurationError(
                f"Staging URI {uri!r} requires an explicit ObjectStore instance or a scheme-qualified URI"
            )
        parsed = urlparse(stripped)
        scheme = parsed.scheme.lower()
        if scheme == "memory":
            raise ImproperConfigurationError("memory:// staging URIs require an explicit ObjectStore instance")
        if scheme == "file":
            full_path = f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
            filename = os.path.basename(full_path.rstrip("/")) if not full_path.endswith("/") else ""
            if not full_path or not filename:
                raise ImproperConfigurationError(f"Invalid file:// staging URI {uri!r}: missing target file path")
            parent_dir = os.path.dirname(full_path) or "/"
            return "file", parent_dir, filename
        if scheme in _REMOTE_SCHEMES:
            bucket = parsed.netloc.strip()
            object_key = parsed.path.lstrip("/").strip()
            if not bucket or not object_key:
                raise ImproperConfigurationError(
                    f"Invalid {scheme}:// staging URI {uri!r}: expected '{scheme}://<bucket>/<object>'"
                )
            return scheme, f"{scheme}://{bucket}", object_key
        raise ImproperConfigurationError(f"Unsupported staging URI scheme {scheme!r} in {uri!r}")

    @staticmethod
    def _normalize_protocol(scheme: str) -> str:
        lowered = scheme.lower()
        if lowered == "gcs":
            return "gs"
        if lowered == "azure":
            return "az"
        return lowered

    @classmethod
    def _resolve_backend_path(cls, store: Any, uri: str) -> str:
        stripped = uri.strip()
        if not stripped:
            raise ImproperConfigurationError("Staging destination URI must not be empty")
        if "://" not in stripped:
            return stripped.lstrip("/")
        parsed_in = urlparse(stripped)
        store_uri = getattr(store, "store_uri", None)
        if isinstance(store_uri, str) and store_uri:
            parsed_store = urlparse(store_uri)
            in_proto = cls._normalize_protocol(parsed_in.scheme)
            store_proto = cls._normalize_protocol(parsed_store.scheme)
            if in_proto == store_proto and (in_proto in {"file", "memory"} or parsed_in.netloc == parsed_store.netloc):
                return stripped
        return parsed_in.path.lstrip("/")

    @staticmethod
    def _resolve_store_key(store: Any, uri: str) -> str:
        stripped = uri.strip()
        if not stripped:
            raise ImproperConfigurationError("Staging destination URI must not be empty")
        if "://" not in stripped:
            return stripped.lstrip("/")
        store_url = getattr(store, "url", None)
        if isinstance(store_url, str) and store_url:
            prefix = store_url.rstrip("/") + "/"
            if stripped.startswith(prefix):
                return stripped[len(prefix) :].lstrip("/")
        parsed = urlparse(stripped)
        if parsed.scheme.lower() == "memory":
            return f"{parsed.netloc}/{parsed.path.lstrip('/')}".strip("/")
        return parsed.path.lstrip("/")

    @staticmethod
    def _normalize_backend_file_format(format: str) -> str:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc", "arrow-ipc"}:
            return "arrow-ipc"
        return fmt

    @staticmethod
    def _normalize_compression(compression: str) -> str | None:
        upper = compression.strip().upper()
        if not upper or upper in {"NONE", "UNCOMPRESSED"}:
            return None
        return upper

    @classmethod
    def _coerce_arrow_result(cls, res: Any, *, format: str) -> ArrowCursor:
        if isinstance(res, ArrowCursor):
            return res
        if isinstance(res, (bytes, bytearray, memoryview)):
            return cls._deserialize_bytes(bytes(res), format=format)
        return ArrowCursor(bytes(write_arrow_shm(res)))

    @classmethod
    def _serialize_cursor(
        cls,
        cursor: ArrowCursor,
        *,
        format: str,
        compression: str,
    ) -> bytes:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc", "arrow-ipc"}:
            return bytes(cursor.buffer_view())
        if fmt == "parquet":
            pa = import_optional("pyarrow")
            pq = import_optional("pyarrow.parquet")
            if pa is not None and pq is not None:
                tbl = cursor.to_arrow_table()
                sink = pa.BufferOutputStream()
                comp = cls._normalize_compression(compression)
                pq.write_table(tbl, sink, compression=comp)
                return bytes(sink.getvalue().to_pybytes())
        if fmt == "csv":
            rows = cursor.to_pylist()
            if not rows:
                return b""
            cols = list(rows[0].keys())
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            return buf.getvalue().encode("utf-8")
        rows = cursor.to_pylist()
        return ("\n".join(encode_json(r) for r in rows) + ("\n" if rows else "")).encode("utf-8")

    @staticmethod
    def _deserialize_bytes(
        payload: bytes,
        *,
        format: str,
    ) -> ArrowCursor:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc", "arrow-ipc"}:
            return ArrowCursor(payload)
        if fmt == "parquet":
            pa = import_optional("pyarrow")
            pq = import_optional("pyarrow.parquet")
            if pa is not None and pq is not None:
                tbl = pq.read_table(pa.py_buffer(payload))
                ipc_view = write_arrow_shm(tbl)
                return ArrowCursor(bytes(ipc_view))
        if fmt in {"json", "jsonl", "ndjson"}:
            lines = [ln.strip() for ln in payload.decode("utf-8", errors="replace").splitlines() if ln.strip()]
            dicts = [decode_json(ln) for ln in lines]
            if not dicts:
                return ArrowCursor.from_rows([], columns=[])
            cols = list(dicts[0].keys())
            rows = [[d.get(c) for c in cols] for d in dicts]
            return ArrowCursor.from_rows(rows, columns=cols)
        if fmt == "csv":
            text = payload.decode("utf-8", errors="replace")
            if not text.strip():
                return ArrowCursor.from_rows([], columns=[])
            reader = csv.DictReader(io.StringIO(text))
            dicts = list(reader)
            cols = list(reader.fieldnames or ())
            rows = [[d.get(c) for c in cols] for d in dicts]
            return ArrowCursor.from_rows(rows, columns=cols)
        return ArrowCursor(payload)
