"""Multi-store alias and URI scheme registry for Vertebra ObjectStore."""

from __future__ import annotations

import threading
from typing import Any

from vertebra.storage._store import ObjectStore, SigningConfig

__all__ = (
    "StorageRegistry",
    "get_default_storage_registry",
)


class StorageRegistry:
    """Thread-safe registry mapping storage aliases and URI schemes to cached ObjectStore instances."""

    __slots__ = ("_aliases", "_lock", "_stores")

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._aliases: dict[str, tuple[str, dict[str, str] | None, SigningConfig | None]] = {}
        self._stores: dict[str, ObjectStore] = {}

    def register_alias(
        self,
        alias: str,
        target: str | ObjectStore | Any,
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
    ) -> None:
        """Bind a logical alias name to a storage URI, provider config, or ObjectStore instance."""
        key = alias.strip()
        if not key:
            msg = "Storage alias cannot be empty"
            raise ValueError(msg)
        with self._lock:
            if isinstance(target, ObjectStore):
                self._stores[key] = target
                self._aliases[key] = (target.url, None, None)
                return
            if isinstance(target, str):
                self._aliases[key] = (target, dict(options) if options else None, signing)
                self._stores.pop(key, None)
                return
            to_store_fn = getattr(target, "to_store", None)
            if callable(to_store_fn):
                store_obj = to_store_fn()
                if isinstance(store_obj, ObjectStore):
                    self._stores[key] = store_obj
                    self._aliases[key] = (store_obj.url, None, None)
                    return
            url = getattr(target, "url", None) or str(target)
            self._aliases[key] = (str(url), dict(options) if options else None, signing)
            self._stores.pop(key, None)

    def resolve_store(
        self,
        uri_or_alias: str = "memory://",
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
    ) -> ObjectStore:
        """Resolve an ObjectStore by registered alias or storage URI with instance caching."""
        lookup = uri_or_alias.strip() or "memory://"
        with self._lock:
            existing = self._stores.get(lookup)
            if existing is not None and options is None and signing is None:
                return existing

            alias_spec = self._aliases.get(lookup)
            if alias_spec is not None:
                target_url, alias_opts, alias_signing = alias_spec
                merged_opts = dict(alias_opts or {})
                if options:
                    merged_opts.update(options)
                effective_signing = signing or alias_signing
                store = ObjectStore(
                    target_url,
                    options=merged_opts or None,
                    signing=effective_signing,
                )
                if options is None and signing is None:
                    self._stores[lookup] = store
                return store

            cache_key = self._make_cache_key(lookup, options, signing)
            cached = self._stores.get(cache_key)
            if cached is not None:
                return cached
            created = ObjectStore(lookup, options=options, signing=signing)
            self._stores[cache_key] = created
            return created

    def get(self, alias: str) -> ObjectStore | None:
        """Return the store registered under alias, or None if not registered."""
        with self._lock:
            if alias in self._stores:
                return self._stores[alias]
            if alias in self._aliases:
                return self.resolve_store(alias)
            return None

    def aliases(self) -> tuple[str, ...]:
        """Return all registered alias names."""
        with self._lock:
            return tuple(sorted(set(self._aliases) | {k for k in self._stores if "://" not in k}))

    def clear(self) -> None:
        """Remove all registered aliases and cached stores."""
        with self._lock:
            self._aliases.clear()
            self._stores.clear()

    @staticmethod
    def _make_cache_key(
        uri: str,
        options: dict[str, str] | None,
        signing: SigningConfig | None,
    ) -> str:
        opts_repr = tuple(sorted((options or {}).items()))
        return f"{uri}|opts={opts_repr!r}|sig={signing!r}"


_DEFAULT_REGISTRY = StorageRegistry()


def get_default_storage_registry() -> StorageRegistry:
    """Return the process-wide default StorageRegistry."""
    return _DEFAULT_REGISTRY
