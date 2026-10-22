"""Vertebra Store adapter for Litestar response caching."""

from __future__ import annotations

import importlib
from typing import Any

from vertebra.framework.stores import KeyValueBackend, ResponseCacheStore

MemoryStore = ResponseCacheStore

__all__ = (
    "KeyValueBackend",
    "MemoryStore",
    "ResponseCacheStore",
    "swap_litestar_response_cache_store",
)


def swap_litestar_response_cache_store(
    app: Any,
    store: ResponseCacheStore | None = None,
) -> ResponseCacheStore | None:
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
        type(current).__name__ == "MemoryStore" and not isinstance(current, ResponseCacheStore)
    )
    if is_memory_store or store is not None:
        replacement = store if store is not None else ResponseCacheStore()
        internal_dict[store_name] = replacement
        return replacement
    if isinstance(current, ResponseCacheStore):
        return current
    return None


def _load_store_types() -> type[Any] | None:
    """Load Litestar MemoryStore and register ResponseCacheStore as a Store virtual subclass."""
    try:
        base_mod = importlib.import_module("litestar.stores.base")
        mem_mod = importlib.import_module("litestar.stores.memory")
        register_fn = getattr(base_mod.Store, "register", None)
        if callable(register_fn):
            register_fn(ResponseCacheStore)
        memory_cls: type[Any] = mem_mod.MemoryStore
        return memory_cls
    except ImportError:
        return None


_MemoryStoreCls = _load_store_types()
