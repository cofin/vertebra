"""Vertebra Store adapter for Litestar response caching and native KV stores."""

from __future__ import annotations

import math
import os
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from vertebra.framework.stores import (
    MemoryStore as _BaseMemoryStore,
)
from vertebra.framework.stores import (
    RedisStore,
    ValkeyStore,
)
from vertebra.storage import AsyncObjectStore, AsyncReadableFile, StorageConfig
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from litestar.stores.base import Store as _StoreBase
else:
    _base_store_mod = import_optional("litestar.stores.base")
    _StoreBase = _base_store_mod.Store if _base_store_mod is not None else object

__all__ = (
    "MemoryStore",
    "ObjectStoreFileSystemAdapter",
    "ObjectStoreLitestarStore",
    "RedisStore",
    "ResponseCacheStore",
    "ValkeyStore",
    "swap_response_cache_store",
)


class MemoryStore(_BaseMemoryStore):
    """Litestar store backed by Vertebra monotonic TTL cache with optional Valkey/Redis backing."""

    __slots__ = ()


class ObjectStoreLitestarStore(_StoreBase):
    """Litestar key/value store with persistent, versioned wall-clock expiration."""

    __slots__ = ("_prefix", "_store")

    def __init__(self, store: AsyncObjectStore | StorageConfig | str, *, namespace: str | None = None) -> None:
        self._store = _resolve_object_store(store)
        self._prefix = "vertebra-store/"
        if namespace is not None:
            self._prefix += validate_identifier(namespace, "namespace", allow_dots=False, allow_hyphens=True) + "/"

    def with_namespace(self, namespace: str) -> ObjectStoreLitestarStore:
        """Return an isolated child sharing this store's backend."""
        child = type(self)(self._store, namespace=namespace)
        child._prefix = self._prefix + child._prefix.removeprefix("vertebra-store/")
        return child

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Store bytes and their optional deadline in one authoritative envelope."""
        deadline = None
        if expires_in is not None:
            seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else expires_in
            deadline = self._now_ms() + math.ceil(seconds * 1000)
        payload = value.encode("utf-8") if isinstance(value, str) else value
        attributes = {} if deadline is None else {"vertebra-expires-at-ms": str(deadline)}
        await self._store.put(
            self._make_key(key), b"VTBSTORE\x01" + encode_msgpack_bytes((deadline, payload)), attributes=attributes
        )

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Read live bytes, renewing only entries with an existing expiration."""
        entry = await self._load(key)
        if entry is None:
            return None
        deadline, payload = entry
        if renew_for is not None and deadline is not None:
            await self.set(key, payload, expires_in=renew_for)
        return payload

    async def delete(self, key: str) -> None:
        """Delete a logical key, allowing absent keys."""
        await self._store.delete(self._make_key(key))

    async def delete_all(self) -> None:
        """Delete this namespace and its descendants through bounded pages."""
        cursor = None
        while True:
            items, cursor = await self._store.list_page(self._prefix, cursor=cursor, limit=1000)
            for item in items:
                if item.path.startswith(self._prefix):
                    await self._store.delete(item.path)
            if cursor is None:
                break

    async def exists(self, key: str) -> bool:
        """Return whether a live entry exists, including empty payloads."""
        return await self._load(key) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return whole remaining seconds for a live expiring entry."""
        entry = await self._load(key)
        if entry is None or entry[0] is None:
            return None
        return max(0, int((entry[0] - self._now_ms()) / 1000))

    def _make_key(self, key: str) -> str:
        return self._prefix + "~" + key.encode("utf-8").hex()

    @staticmethod
    def _now_ms() -> int:
        return math.floor(time.time() * 1000)

    async def _load(self, key: str) -> tuple[int | None, bytes] | None:
        try:
            raw = await self._store.get(self._make_key(key))
        except FileNotFoundError:
            return None
        if not raw.startswith(b"VTBSTORE\x01"):
            raise ValueError("Invalid object store envelope magic or version")
        entry: tuple[int | None, bytes] = decode_msgpack(raw[9:], type=tuple[int | None, bytes])
        if entry[0] is not None and self._now_ms() >= entry[0]:
            await self.delete(key)
            return None
        return entry


class ObjectStoreFileSystemAdapter:
    """Read-only async filesystem exposing a confined virtual object-store root."""

    __slots__ = ("_base_path", "_store")

    def __init__(self, store: AsyncObjectStore | StorageConfig | str, *, base_path: str = "") -> None:
        self._store = _resolve_object_store(store)
        if base_path.startswith("/") or "\\" in base_path or ".." in base_path.split("/"):
            raise ValueError("base_path must be a relative prefix without parent segments")
        self._base_path = "/".join(part for part in base_path.split("/") if part and part != ".")

    async def info(self, path: str | os.PathLike[str], **kwargs: Any) -> dict[str, Any]:
        """Return stat information for a file or a populated directory prefix."""
        key = self._resolve_key(path)
        size = 0
        mtime = 0.0
        kind = "directory"
        if key != self._base_path:
            try:
                meta = await self._store.head(key)
            except FileNotFoundError:
                prefix = key + "/"
                items, _ = await self._store.list_page(prefix, limit=1)
                if not any(item.path.startswith(prefix) for item in items):
                    raise FileNotFoundError(os.fspath(path)) from None
            else:
                size = meta.size
                mtime = datetime.fromisoformat(meta.last_modified).timestamp() if meta.last_modified else 0.0
                kind = "file"
        return {
            "name": os.fspath(path),
            "size": size,
            "type": kind,
            "mtime": mtime,
            "created": 0.0,
            "gid": 0,
            "ino": 0,
            "islink": False,
            "mode": 0,
            "nlink": 0,
            "uid": 0,
        }

    async def open(
        self, file: str | os.PathLike[str], mode: str = "rb", buffering: int = -1, **kwargs: Any
    ) -> AsyncReadableFile:
        """Open a native asynchronous reader with nonblocking synchronous seek/tell."""
        if mode != "rb":
            raise ValueError("Object store files support only rb mode")
        key = self._resolve_key(file)
        meta = await self._store.head(key)
        return self._store.open_reader(key, size=meta.size)

    def _resolve_key(self, path: str | os.PathLike[str]) -> str:
        raw = os.fspath(path)
        if ".." in raw.split("/") or "\\" in raw:
            raise FileNotFoundError(raw)
        relative = "/".join(part for part in raw.split("/") if part and part != ".")
        return "/".join(part for part in (self._base_path, relative) if part)


class ResponseCacheStore(MemoryStore):
    """Litestar response cache store backed by Vertebra fast memory/SHM cache."""

    __slots__ = ()


def swap_response_cache_store(
    app: Any,
    store: MemoryStore | None = None,
) -> MemoryStore | None:
    """Transparently replace Litestar's default response_cache MemoryStore with Vertebra's MemoryStore."""
    stores = getattr(app, "stores", None)
    if stores is None:
        return None
    cache_cfg = getattr(app, "response_cache_config", None)
    store_name = getattr(cache_cfg, "store", "response_cache") if cache_cfg is not None else "response_cache"
    internal_dict = getattr(stores, "_stores", None)
    if not isinstance(internal_dict, dict):
        if isinstance(stores, dict):
            internal_dict = stores
        else:
            return None
    current = internal_dict.get(store_name)
    is_memory_store = (_MemoryStoreCls is not None and isinstance(current, _MemoryStoreCls)) or (
        type(current).__name__ == "MemoryStore" and not isinstance(current, MemoryStore)
    )
    if is_memory_store or store is not None:
        replacement = store if store is not None else MemoryStore()
        internal_dict[store_name] = replacement
        return replacement
    if isinstance(current, MemoryStore):
        return current
    return None


def _resolve_object_store(store: AsyncObjectStore | StorageConfig | str) -> AsyncObjectStore:
    """Resolve an explicit backend without silently selecting a default."""
    if store is None or store == "":
        raise ImproperConfigurationError("An explicit object store configuration is required")
    return store if isinstance(store, AsyncObjectStore) else AsyncObjectStore(store)


def _load_store_types() -> type[Any] | None:
    """Load Litestar MemoryStore and register stores as Store virtual subclasses."""
    base_mod = import_optional("litestar.stores.base")
    mem_mod = import_optional("litestar.stores.memory")
    if base_mod is None or mem_mod is None:
        return None
    register_fn = getattr(base_mod.Store, "register", None)
    if callable(register_fn):
        register_fn(MemoryStore)
        register_fn(ValkeyStore)
        register_fn(RedisStore)
        register_fn(ResponseCacheStore)
    memory_cls: type[Any] = mem_mod.MemoryStore
    return memory_cls


_MemoryStoreCls = _load_store_types()
