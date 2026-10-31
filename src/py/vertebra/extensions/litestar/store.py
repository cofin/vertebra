"""Vertebra Store adapter for Litestar response caching."""

from __future__ import annotations

from typing import Any

from litestar.stores.base import Store

from vertebra.framework.stores import KeyValueBackend
from vertebra.framework.stores import ResponseCacheStore as FrameworkResponseCacheStore
from vertebra.utils.module_loader import import_optional

__all__ = (
    "KeyValueBackend",
    "MemoryStore",
    "ResponseCacheStore",
    "swap_litestar_response_cache_store",
)


class ResponseCacheStore(FrameworkResponseCacheStore, Store):
    """Litestar response cache store backed by Vertebra fast memory/SHM cache."""

    __slots__ = ()

    async def __aenter__(self) -> Any:
        """Enter async context for Litestar and Framework compatibility."""
        return self


class MemoryStore(ResponseCacheStore):
    """In-memory response cache store for Litestar applications."""

    __slots__ = ()


def swap_litestar_response_cache_store(
    app: Any,
    store: ResponseCacheStore | FrameworkResponseCacheStore | None = None,
) -> ResponseCacheStore | FrameworkResponseCacheStore | None:
    """Transparently replace Litestar's default response_cache MemoryStore with Vertebra's ResponseCacheStore."""
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
        type(current).__name__ == "MemoryStore"
        and not isinstance(current, (ResponseCacheStore, FrameworkResponseCacheStore))
    )
    if is_memory_store or store is not None:
        replacement = store if store is not None else MemoryStore()
        internal_dict[store_name] = replacement
        return replacement
    if isinstance(current, (ResponseCacheStore, FrameworkResponseCacheStore)):
        return current
    return None


def _load_store_types() -> type[Any] | None:
    """Load Litestar MemoryStore and register ResponseCacheStore and MemoryStore as Store virtual subclasses."""
    base_mod = import_optional("litestar.stores.base")
    mem_mod = import_optional("litestar.stores.memory")
    if base_mod is None or mem_mod is None:
        return None
    register_fn = getattr(base_mod.Store, "register", None)
    if callable(register_fn):
        register_fn(ResponseCacheStore)
        register_fn(MemoryStore)
    memory_cls: type[Any] = mem_mod.MemoryStore
    return memory_cls


_MemoryStoreCls = _load_store_types()
