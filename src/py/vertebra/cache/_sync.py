"""Synchronous framework-neutral cache store across memory, SHM, Valkey, Redis, and object storage."""

from __future__ import annotations

import collections
import hashlib
import math
import threading
import time
from typing import TYPE_CHECKING, cast

from vertebra.cache._types import CacheStats
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from vertebra._vertebra import ShmCache
    from vertebra.db.adapters.redis._sync import SyncRedisDriver
    from vertebra.db.adapters.valkey._sync import SyncValkeyDriver
    from vertebra.storage._store import SyncObjectStore

__all__ = ("SyncCacheStore",)

_SHM_MAX_KEY_BYTES = 128
_SHM_MAX_VALUE_BYTES = 856
_SHM_MAX_RAW_PREFIX_BYTES = 64
_SHM_DIGEST_BYTES = 16
_SHM_NS_DIGEST_BYTES = 12
_SHM_KEY_SEP = b"~"
_ENVELOPE_MAGIC = b"VTBSTORE\x01"
_VALKEY_SCHEMES = frozenset({"valkey", "valkeys"})
_REDIS_SCHEMES = frozenset({"redis", "rediss"})
_OBJECT_SCHEMES = frozenset({"s3", "gs", "az", "r2", "oci", "file"})
_SUPPORTED_SCHEMES = frozenset({"memory", "shm"}) | _VALKEY_SCHEMES | _REDIS_SCHEMES | _OBJECT_SCHEMES
_WILDCARD_CHARS = frozenset({"*", "?", "["})


def _parse_url_scheme(url: str) -> str:
    """Validate and return the lowercase scheme from a cache URL."""
    if "://" not in url:
        msg = f"Unsupported cache URL scheme in {url!r}"
        raise ValueError(msg)
    scheme = url.split("://", 1)[0].lower()
    if scheme not in _SUPPORTED_SCHEMES:
        msg = f"Unsupported cache URL scheme: {scheme!r}"
        raise ValueError(msg)
    return scheme


def _validate_key_prefix(key_prefix: str) -> str:
    """Validate a cache namespace prefix, rejecting empty strings and glob wildcards."""
    if not key_prefix or not key_prefix.strip():
        msg = "key_prefix must not be empty"
        raise ValueError(msg)
    if any(ch in _WILDCARD_CHARS for ch in key_prefix):
        msg = f"key_prefix must not contain glob wildcards: {key_prefix!r}"
        raise ValueError(msg)
    return key_prefix


def _shm_segment_name(url: str) -> str:
    """Extract a POSIX shared-memory segment name from a ``shm://`` URL."""
    segment = url.removeprefix("shm://").strip("/").replace("/", "-")
    return segment or "vertebra-cache"


def _utf8_prefix_slice(data: bytes, max_bytes: int) -> bytes:
    """Slice ``data`` to at most ``max_bytes`` without splitting a multi-byte UTF-8 sequence."""
    if len(data) <= max_bytes:
        return data
    end = max_bytes
    while end > 0 and (data[end] & 0xC0) == 0x80:
        end -= 1
    return data[:end]


def _shm_namespace_prefix(key_prefix: str) -> bytes:
    """Return a bounded UTF-8 namespace prefix for POSIX shared-memory keys."""
    raw = key_prefix.encode("utf-8")
    if len(raw) <= _SHM_MAX_RAW_PREFIX_BYTES:
        return raw
    token = hashlib.blake2b(raw, digest_size=_SHM_NS_DIGEST_BYTES).hexdigest()
    return f"ns~{token}:".encode("ascii")


def _encode_shm_key(key_prefix: str, key: str) -> bytes:
    """Encode ``key_prefix + key`` to at most 128 UTF-8 bytes with BLAKE2b truncation."""
    ns_prefix = _shm_namespace_prefix(key_prefix)
    key_bytes = key.encode("utf-8")
    combined = ns_prefix + key_bytes
    if len(combined) <= _SHM_MAX_KEY_BYTES:
        return combined
    full_prefixed = key_prefix.encode("utf-8") + key_bytes
    digest = hashlib.blake2b(full_prefixed, digest_size=_SHM_DIGEST_BYTES).hexdigest().encode("ascii")
    max_slice = _SHM_MAX_KEY_BYTES - len(_SHM_KEY_SEP) - len(digest)
    return _utf8_prefix_slice(combined, max_slice) + _SHM_KEY_SEP + digest


def _object_store_ns_dir(key_prefix: str) -> str:
    """Return a deterministic directory namespace for object-storage cache keys."""
    digest = hashlib.blake2b(key_prefix.encode("utf-8"), digest_size=_SHM_DIGEST_BYTES).hexdigest()
    return f"vtb-cache/{digest}"


def _object_store_key(ns_dir: str, key: str) -> str:
    """Encode a logical cache key into a segment-bounded object-storage key."""
    hex_key = key.encode("utf-8").hex()
    if not hex_key:
        return f"{ns_dir}/~.vtb"
    chunks = [hex_key[i : i + 64] for i in range(0, len(hex_key), 64)]
    return f"{ns_dir}/{'/'.join(chunks)}.vtb"


def _object_store_item_hex(ns_dir: str, item_path: str) -> str | None:
    """Extract the raw UTF-8 hex key from an object-storage path under ``ns_dir``."""
    prefix = f"{ns_dir}/"
    if not item_path.startswith(prefix):
        return None
    rel = item_path[len(prefix) :]
    if rel == "~.vtb":
        return ""
    if not rel.endswith(".vtb"):
        return None
    return rel[:-4].replace("/", "")


def _escape_redis_glob(text: str) -> str:
    """Escape Redis/Valkey ``SCAN MATCH`` glob metacharacters in literal key prefixes."""
    out: list[str] = []
    for ch in text:
        if ch in {"\\", "*", "?", "[", "]"}:
            out.append("\\")
        out.append(ch)
    return "".join(out)


def _resolve_px_ms(ttl: float) -> int:
    """Convert a positive TTL in seconds to millisecond ``PX`` precision."""
    return max(1, math.ceil(ttl * 1000))


def _parse_int_bytes(raw: bytes, key: str) -> int:
    """Strictly parse UTF-8 ASCII integer bytes or raise ``ValueError``."""
    try:
        text = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        msg = f"Cache value for key {key!r} is not valid UTF-8 integer bytes"
        raise ValueError(msg) from exc
    if not text or "_" in text or not text.isascii():
        msg = f"Cache value for key {key!r} is not a valid integer"
        raise ValueError(msg)
    return int(text, 10)


def _is_redis_int_error(exc: Exception) -> bool:
    """Return True when a Redis/Valkey exception reports a non-integer value."""
    msg = str(exc).lower()
    return "not an integer" in msg or "out of range" in msg or "wrongtype" in msg


def _encode_envelope(value: bytes, deadline: float | None) -> bytes:
    """Encode an object-storage cache payload and optional deadline using centralized msgpack."""
    from vertebra.utils.serializers import encode_msgpack_bytes

    return _ENVELOPE_MAGIC + bytes(encode_msgpack_bytes((deadline, bytes(value))))


def _decode_envelope(raw: bytes) -> tuple[float | None, bytes]:
    """Decode an object-storage cache envelope into ``(deadline, payload)``."""
    if not raw.startswith(_ENVELOPE_MAGIC):
        msg = "Invalid cache object envelope magic or version"
        raise ValueError(msg)
    from vertebra.utils.serializers import decode_msgpack

    return cast(
        "tuple[float | None, bytes]", decode_msgpack(raw[len(_ENVELOPE_MAGIC) :], type=tuple[float | None, bytes])
    )


class _MemoryEntry:
    """Mutable LRU entry holding cached bytes and optional clock expiration."""

    __slots__ = ("expires_at", "value")

    def __init__(self, value: bytes, expires_at: float | None) -> None:
        self.value = value
        self.expires_at = expires_at

    def is_expired(self, now: float) -> bool:
        """Return True when the entry has an elapsed expiration timestamp."""
        return self.expires_at is not None and now >= self.expires_at


class _MemoryState:
    """Thread-safe bounded LRU state shared between sync and async memory stores."""

    __slots__ = (
        "capacity",
        "clock",
        "entries",
        "evictions",
        "expirations",
        "hits",
        "lock",
        "misses",
    )

    def __init__(self, capacity: int, clock: Callable[[], float]) -> None:
        self.capacity = capacity
        self.clock = clock
        self.entries: collections.OrderedDict[str, _MemoryEntry] = collections.OrderedDict()
        self.lock = threading.RLock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expirations = 0

    def record_hit(self) -> None:
        """Increment hit counter under lock."""
        with self.lock:
            self.hits += 1

    def record_miss(self) -> None:
        """Increment miss counter under lock."""
        with self.lock:
            self.misses += 1

    def get_with_expiry(self, key: str, *, record_stats: bool = True) -> tuple[bytes, float | None] | None:
        """Fetch live bytes and optional expiration timestamp under lock."""
        with self.lock:
            now = self.clock()
            entry = self.entries.get(key)
            if entry is None:
                if record_stats:
                    self.misses += 1
                return None
            if entry.is_expired(now):
                del self.entries[key]
                self.expirations += 1
                if record_stats:
                    self.misses += 1
                return None
            if record_stats:
                self.entries.move_to_end(key)
                self.hits += 1
            return entry.value, entry.expires_at

    def set(self, key: str, value: bytes, effective_ttl: float | None) -> None:
        """Store ``key`` and ``value`` with optional TTL, evicting LRU entries if full."""
        with self.lock:
            now = self.clock()
            if effective_ttl is not None and effective_ttl <= 0:
                self.entries.pop(key, None)
                return
            expires_at = (now + effective_ttl) if effective_ttl is not None else None
            self.entries[key] = _MemoryEntry(value=value, expires_at=expires_at)
            self.entries.move_to_end(key)
            self._evict_locked(now)

    def add(self, key: str, value: bytes, effective_ttl: float | None) -> bool:
        """Insert ``key`` only when missing or expired."""
        with self.lock:
            now = self.clock()
            entry = self.entries.get(key)
            if entry is not None:
                if entry.is_expired(now):
                    del self.entries[key]
                    self.expirations += 1
                else:
                    return False
            if effective_ttl is not None and effective_ttl <= 0:
                return True
            expires_at = (now + effective_ttl) if effective_ttl is not None else None
            self.entries[key] = _MemoryEntry(value=value, expires_at=expires_at)
            self.entries.move_to_end(key)
            self._evict_locked(now)
            return True

    def delete(self, key: str) -> bool:
        """Remove ``key`` and return True if it existed and had not expired."""
        with self.lock:
            now = self.clock()
            entry = self.entries.pop(key, None)
            if entry is None:
                return False
            if entry.is_expired(now):
                self.expirations += 1
                return False
            return True

    def has(self, key: str) -> bool:
        """Return True if ``key`` exists and is unexpired without mutating hit/miss stats."""
        return self.get_with_expiry(key, record_stats=False) is not None

    def touch(self, key: str, effective_ttl: float | None) -> bool:
        """Update TTL on an existing live entry."""
        with self.lock:
            now = self.clock()
            entry = self.entries.get(key)
            if entry is None:
                return False
            if entry.is_expired(now):
                del self.entries[key]
                self.expirations += 1
                return False
            if effective_ttl is not None and effective_ttl <= 0:
                del self.entries[key]
                return True
            entry.expires_at = (now + effective_ttl) if effective_ttl is not None else None
            self.entries.move_to_end(key)
            return True

    def incr(self, key: str, delta: int, default_ttl: float | None) -> tuple[int, float | None]:
        """Atomically increment integer UTF-8 bytes or initialize with ``delta``."""
        with self.lock:
            now = self.clock()
            entry = self.entries.get(key)
            if entry is not None and entry.is_expired(now):
                del self.entries[key]
                self.expirations += 1
                entry = None
            if entry is not None:
                cur = _parse_int_bytes(entry.value, key)
                new_val = cur + delta
                entry.value = str(new_val).encode("utf-8")
                self.entries.move_to_end(key)
                return new_val, entry.expires_at
            if default_ttl is not None and default_ttl <= 0:
                return delta, None
            expires_at = (now + default_ttl) if default_ttl is not None else None
            self.entries[key] = _MemoryEntry(value=str(delta).encode("utf-8"), expires_at=expires_at)
            self.entries.move_to_end(key)
            self._evict_locked(now)
            return delta, expires_at

    def delete_prefix(self, prefix: str) -> int:
        """Remove all live entries whose logical keys start with ``prefix``."""
        with self.lock:
            now = self.clock()
            self._purge_expired_locked(now)
            to_remove = [k for k in self.entries if k.startswith(prefix)]
            for k in to_remove:
                del self.entries[k]
            return len(to_remove)

    def clear(self) -> None:
        """Remove all entries from this memory store."""
        with self.lock:
            self.entries.clear()

    def stats(self) -> CacheStats:
        """Return a snapshot of current memory store counters."""
        with self.lock:
            self._purge_expired_locked(self.clock())
            return CacheStats(
                hits=self.hits,
                misses=self.misses,
                evictions=self.evictions,
                expirations=self.expirations,
                size=len(self.entries),
                capacity=self.capacity,
            )

    def _purge_expired_locked(self, now: float) -> None:
        expired = [k for k, entry in self.entries.items() if entry.is_expired(now)]
        for k in expired:
            del self.entries[k]
            self.expirations += 1

    def _evict_locked(self, now: float) -> None:
        if len(self.entries) <= self.capacity:
            return
        self._purge_expired_locked(now)
        while len(self.entries) > self.capacity:
            self.entries.popitem(last=False)
            self.evictions += 1


class SyncCacheStore:
    """Framework-neutral synchronous cache store supporting memory, SHM, Valkey, Redis, and object storage."""

    __slots__ = (
        "_capacity",
        "_clock",
        "_default_ttl",
        "_driver",
        "_expirations",
        "_hits",
        "_key_prefix",
        "_l1",
        "_l1_url",
        "_misses",
        "_ns_dir",
        "_raw_clock",
        "_scheme",
        "_shm",
        "_shm_name",
        "_shm_ns_prefix",
        "_state",
        "_store",
        "_url",
    )

    def __init__(
        self,
        url: str = "memory://",
        *,
        l1_url: str | None = None,
        capacity: int = 2048,
        default_ttl: float | None = None,
        key_prefix: str = "vtb:cache:",
        _clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialize a synchronous cache store for the configured backend URL and optional L1 tier.

        Args:
            url: Backend URL (``memory://``, ``shm://``, ``valkey(s)://``, ``redis(s)://``,
                ``s3://``, ``gs://``, ``az://``, ``r2://``, ``oci://``, or ``file://``).
            l1_url: Optional L1 cache URL paired with an L2 backend (including ``memory://``).
            capacity: Maximum entry capacity (must be at least 1).
            default_ttl: Default TTL in seconds applied when per-call ``ttl`` is omitted.
            key_prefix: Non-empty namespace prefix without glob wildcards.
            _clock: Optional injectable clock callable returning seconds as ``float``.

        Raises:
            ValueError: When ``url``, ``l1_url``, ``capacity``, or ``key_prefix`` is invalid.
            MissingDependencyError: When required optional native modules are unavailable.
        """
        if capacity < 1:
            msg = "capacity must be >= 1"
            raise ValueError(msg)
        scheme = _parse_url_scheme(url)
        if l1_url is not None:
            _parse_url_scheme(l1_url)
        validated_prefix = _validate_key_prefix(key_prefix)

        self._url = url
        self._l1_url = l1_url
        self._scheme = scheme
        self._capacity = capacity
        self._default_ttl = default_ttl
        self._key_prefix = validated_prefix
        self._raw_clock = _clock
        default_clock = time.time if scheme in _OBJECT_SCHEMES else time.monotonic
        self._clock: Callable[[], float] = _clock if _clock is not None else default_clock
        self._hits = 0
        self._misses = 0
        self._expirations = 0
        self._state: _MemoryState | None = None
        self._shm: ShmCache | None = None
        self._shm_name: str | None = None
        self._shm_ns_prefix = _shm_namespace_prefix(validated_prefix)
        self._driver: SyncValkeyDriver | SyncRedisDriver | None = None
        self._store: SyncObjectStore | None = None
        self._ns_dir = _object_store_ns_dir(validated_prefix)
        self._l1: SyncCacheStore | None = None

        if scheme == "memory":
            self._state = _MemoryState(capacity=capacity, clock=self._clock)
        elif scheme == "shm":
            from vertebra._vertebra import ShmCache

            segment = _shm_segment_name(url)
            self._shm_name = segment
            self._shm = ShmCache(segment, capacity_bytes=max(65536, capacity * 1024), max_entries=capacity)
        elif scheme in _VALKEY_SCHEMES:
            ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
            from vertebra.db.adapters.valkey import SyncValkeyDriver, ValkeyConfig

            self._driver = SyncValkeyDriver(ValkeyConfig(url=url))
        elif scheme in _REDIS_SCHEMES:
            ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
            from vertebra.db.adapters.redis import RedisConfig, SyncRedisDriver

            self._driver = SyncRedisDriver(RedisConfig(url=url))
        elif scheme in _OBJECT_SCHEMES:
            ensure_module("vertebra._storage", package="vertebra-core[storage]", extra="storage")
            from vertebra.storage import SyncObjectStore

            self._store = SyncObjectStore(url)

        if l1_url is not None:
            self._l1 = SyncCacheStore(
                l1_url,
                capacity=capacity,
                default_ttl=default_ttl,
                key_prefix=validated_prefix,
                _clock=_clock,
            )

    def get(self, key: str) -> bytes | None:
        """Retrieve cached bytes for ``key`` or return ``None`` on miss or expiration."""
        if self._l1 is not None:
            l1_val = self._l1.get(key)
            if l1_val is not None:
                self._record_hit()
                return l1_val

        hit = self._get_l2_with_ttl(key)
        if hit is None:
            if self._scheme != "memory":
                self._misses += 1
            return None

        value, remaining_ttl = hit
        if self._scheme != "memory":
            self._hits += 1
        if self._l1 is not None and (self._l1._scheme != "shm" or len(value) <= _SHM_MAX_VALUE_BYTES):
            self._l1.set(key, value, ttl=remaining_ttl)
        return value

    def set(self, key: str, value: bytes, *, ttl: float | None = None) -> None:
        """Store ``value`` under ``key`` with an optional TTL in seconds."""
        effective_ttl = self._effective_ttl(ttl)
        if effective_ttl is not None and effective_ttl <= 0:
            self.delete(key)
            return

        if self._l1 is not None:
            if self._l1._scheme == "shm" and len(value) > _SHM_MAX_VALUE_BYTES:
                self._l1.delete(key)
            else:
                self._l1.set(key, value, ttl=ttl)

        if self._state is not None:
            self._state.set(key, value, effective_ttl)
            return
        if self._shm is not None:
            shm_key = _encode_shm_key(self._key_prefix, key)
            if len(value) > _SHM_MAX_VALUE_BYTES:
                self._shm.delete(shm_key)
            else:
                self._shm.set(shm_key, value, ttl=effective_ttl)
            return
        if self._driver is not None:
            dkey = self._driver_key(key)
            if effective_ttl is not None:
                self._driver.set(dkey, value, px=_resolve_px_ms(effective_ttl))
            else:
                self._driver.set(dkey, value)
            return
        if self._store is not None:
            self._store_put(key, value, effective_ttl)

    def add(self, key: str, value: bytes, *, ttl: float | None = None) -> bool:
        """Store ``value`` only if ``key`` is absent or expired, returning True when inserted."""
        effective_ttl = self._effective_ttl(ttl)
        added = self._add_l2(key, value, effective_ttl)
        if added and self._l1 is not None:
            if (
                effective_ttl is not None
                and effective_ttl <= 0
                or self._l1._scheme == "shm"
                and len(value) > _SHM_MAX_VALUE_BYTES
            ):
                self._l1.delete(key)
            else:
                self._l1.set(key, value, ttl=ttl)
        return added

    def delete(self, key: str) -> bool:
        """Delete ``key`` from L1 and L2, returning True if a live entry was removed."""
        removed_l1 = self._l1.delete(key) if self._l1 is not None else False
        removed_l2 = self._delete_l2(key)
        return removed_l2 or removed_l1

    def has(self, key: str) -> bool:
        """Return True if ``key`` exists and has not expired."""
        if self._l1 is not None and self._l1.has(key):
            return True
        if self._state is not None:
            return self._state.has(key)
        if self._shm is not None:
            return bool(self._shm.has_key(_encode_shm_key(self._key_prefix, key)))
        if self._driver is not None:
            return bool(self._driver.execute_command("EXISTS", self._driver_key(key)))
        if self._store is not None:
            return self._store_get(key) is not None
        return False

    def touch(self, key: str, *, ttl: float | None = None) -> bool:
        """Refresh the expiration timestamp for ``key``, returning True if a live key existed."""
        effective_ttl = self._effective_ttl(ttl)
        if self._l1 is not None:
            self._l1.touch(key, ttl=ttl)
        if self._state is not None:
            return self._state.touch(key, effective_ttl)
        if self._shm is not None:
            shm_key = _encode_shm_key(self._key_prefix, key)
            if effective_ttl is not None and effective_ttl <= 0:
                return bool(self._shm.delete(shm_key))
            return bool(self._shm.touch(shm_key, ttl=effective_ttl))
        if self._driver is not None:
            dkey = self._driver_key(key)
            if effective_ttl is not None and effective_ttl <= 0:
                return bool(self._driver.delete(dkey))
            if effective_ttl is not None:
                return bool(self._driver.execute_command("PEXPIRE", dkey, _resolve_px_ms(effective_ttl)))
            if not self._driver.execute_command("EXISTS", dkey):
                return False
            self._driver.execute_command("PERSIST", dkey)
            return True
        if self._store is not None:
            found = self._store_get(key)
            if found is None:
                return False
            _, payload = found
            if effective_ttl is not None and effective_ttl <= 0:
                self._store.delete(_object_store_key(self._ns_dir, key))
                return True
            self._store_put(key, payload, effective_ttl)
            return True
        return False

    def incr(self, key: str, delta: int = 1) -> int:
        """Increment an integer value stored at ``key`` or initialize missing keys to ``delta``."""
        new_val, remaining_ttl = self._incr_l2(key, delta)
        if self._l1 is not None:
            encoded = str(new_val).encode("utf-8")
            if (
                remaining_ttl is not None
                and remaining_ttl <= 0
                or self._l1._scheme == "shm"
                and len(encoded) > _SHM_MAX_VALUE_BYTES
            ):
                self._l1.delete(key)
            else:
                self._l1.set(key, encoded, ttl=remaining_ttl)
        return new_val

    def get_many(self, keys: Sequence[str]) -> dict[str, bytes]:
        """Retrieve all live entries among ``keys`` as a key-to-bytes dictionary."""
        result: dict[str, bytes] = {}
        for key in keys:
            val = self.get(key)
            if val is not None:
                result[key] = val
        return result

    def set_many(self, entries: Mapping[str, bytes], *, ttl: float | None = None) -> None:
        """Store multiple ``key -> value`` pairs with an optional shared TTL."""
        for key, value in entries.items():
            self.set(key, value, ttl=ttl)

    def delete_many(self, keys: Sequence[str]) -> int:
        """Delete all ``keys`` from the cache and return the count of live keys removed."""
        unique_keys = list(dict.fromkeys(keys))
        if not unique_keys:
            return 0
        if self._l1 is None and self._driver is not None:
            dkeys = [self._driver_key(key) for key in unique_keys]
            return int(self._driver.unlink(*dkeys))
        removed = 0
        for key in unique_keys:
            if self.delete(key):
                removed += 1
        return removed

    def delete_prefix(self, prefix: str) -> int:
        """Delete all keys matching ``key_prefix + prefix`` and return the count removed."""
        if self._l1 is not None:
            self._l1.delete_prefix(prefix)
        if self._state is not None:
            return self._state.delete_prefix(prefix)
        if self._shm is not None:
            pattern = self._shm_ns_prefix + prefix.encode("utf-8")
            if len(pattern) > _SHM_MAX_KEY_BYTES:
                return 0
            return int(self._shm.invalidate_prefix(pattern))
        if self._driver is not None:
            full_prefix = self._driver_key(prefix)
            match = f"{_escape_redis_glob(full_prefix)}*"
            cursor = 0
            removed = 0
            while True:
                cursor, batch = self._driver.scan(cursor, match=match, count=500)
                matched = [item for item in batch if item.startswith(full_prefix)]
                if matched:
                    removed += int(self._driver.unlink(*matched))
                if cursor == 0:
                    break
            return removed
        if self._store is not None:
            return self._store_delete_prefix(prefix)
        return 0

    def clear(self) -> None:
        """Clear all keys within this store's ``key_prefix`` namespace."""
        if self._l1 is not None:
            self._l1.clear()
        if self._state is not None:
            self._state.clear()
            return
        if self._shm is not None:
            self._shm.invalidate_prefix(self._shm_ns_prefix)
            return
        if self._driver is not None:
            self.delete_prefix("")
            return
        if self._store is not None:
            self._store_delete_prefix("")

    def stats(self) -> CacheStats:
        """Return a snapshot of hit, miss, eviction, expiration, size, and capacity metrics."""
        if self._state is not None:
            return self._state.stats()
        if self._shm is not None:
            raw = self._shm.stats()
            return CacheStats(
                hits=raw.hits,
                misses=raw.misses,
                evictions=raw.evictions,
                expirations=raw.expirations,
                size=0,
                capacity=self._capacity,
            )
        return CacheStats(
            hits=self._hits,
            misses=self._misses,
            evictions=0,
            expirations=self._expirations,
            size=0,
            capacity=self._capacity,
        )

    def close(self) -> None:
        """Release underlying backend connections or shared-memory handles."""
        if self._l1 is not None:
            self._l1.close()
        if self._shm is not None:
            self._shm.close()
        if self._driver is not None:
            self._driver.close()
        if self._store is not None:
            self._store.close()

    def _effective_ttl(self, ttl: float | None) -> float | None:
        return self._default_ttl if ttl is None else ttl

    def _driver_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def _record_hit(self) -> None:
        if self._state is not None:
            self._state.record_hit()
        else:
            self._hits += 1

    def _get_l2_with_ttl(self, key: str) -> tuple[bytes, float | None] | None:
        if self._state is not None:
            hit = self._state.get_with_expiry(key, record_stats=True)
            if hit is None:
                return None
            value, expires_at = hit
            remaining = max(0.0, expires_at - self._clock()) if expires_at is not None else None
            return value, remaining
        if self._shm is not None:
            raw = self._shm.get(_encode_shm_key(self._key_prefix, key))
            return (raw, None) if raw is not None else None
        if self._driver is not None:
            dkey = self._driver_key(key)
            raw = self._driver.get(dkey)
            if raw is None:
                return None
            remaining = None
            if self._l1 is not None:
                pttl_ms = self._driver.pttl(dkey)
                if pttl_ms > 0:
                    remaining = pttl_ms / 1000.0
            return raw, remaining
        if self._store is not None:
            found = self._store_get(key)
            if found is None:
                return None
            deadline, payload = found
            remaining = max(0.0, deadline - self._clock()) if deadline is not None else None
            return payload, remaining
        return None

    def _add_l2(self, key: str, value: bytes, effective_ttl: float | None) -> bool:
        if self._state is not None:
            return self._state.add(key, value, effective_ttl)
        if self._shm is not None:
            shm_key = _encode_shm_key(self._key_prefix, key)
            if effective_ttl is not None and effective_ttl <= 0:
                return not self._shm.has_key(shm_key)
            if len(value) > _SHM_MAX_VALUE_BYTES:
                return False
            return bool(self._shm.add(shm_key, value, ttl=effective_ttl))
        if self._driver is not None:
            dkey = self._driver_key(key)
            if effective_ttl is not None and effective_ttl <= 0:
                return not bool(self._driver.execute_command("EXISTS", dkey))
            if effective_ttl is not None:
                res = self._driver.execute_command("SET", dkey, value, "PX", _resolve_px_ms(effective_ttl), "NX")
            else:
                res = self._driver.execute_command("SET", dkey, value, "NX")
            return res is not None and res is not False
        if self._store is not None:
            if self._store_get(key) is not None:
                return False
            if effective_ttl is not None and effective_ttl <= 0:
                return True
            self._store_put(key, value, effective_ttl)
            return True
        return False

    def _delete_l2(self, key: str) -> bool:
        if self._state is not None:
            return self._state.delete(key)
        if self._shm is not None:
            return bool(self._shm.delete(_encode_shm_key(self._key_prefix, key)))
        if self._driver is not None:
            return bool(self._driver.delete(self._driver_key(key)))
        if self._store is not None:
            existed = self._store_get(key) is not None
            if existed:
                self._store.delete(_object_store_key(self._ns_dir, key))
            return existed
        return False

    def _incr_l2(self, key: str, delta: int) -> tuple[int, float | None]:
        if self._state is not None:
            new_val, expires_at = self._state.incr(key, delta, self._default_ttl)
            remaining = max(0.0, expires_at - self._clock()) if expires_at is not None else None
            return new_val, remaining
        if self._shm is not None:
            return self._incr_shm(key, delta), self._default_ttl
        if self._driver is not None:
            return self._incr_driver(key, delta), self._default_ttl
        if self._store is not None:
            return self._incr_store(key, delta)
        return delta, self._default_ttl

    def _incr_shm(self, key: str, delta: int) -> int:
        assert self._shm is not None
        dkey = _encode_shm_key(self._key_prefix, key)
        while True:
            existing = self._shm.get(dkey)
            if existing is not None:
                _parse_int_bytes(existing, key)
                return int(self._shm.incr(dkey, delta))
            if self._default_ttl is not None and self._default_ttl <= 0:
                return delta
            if self._default_ttl is None:
                return int(self._shm.incr(dkey, delta))
            encoded = str(delta).encode("utf-8")
            if self._shm.add(dkey, encoded, ttl=self._default_ttl):
                return delta

    def _incr_driver(self, key: str, delta: int) -> int:
        assert self._driver is not None
        dkey = self._driver_key(key)
        if self._default_ttl is not None and self._default_ttl > 0:
            created = self._driver.execute_command(
                "SET", dkey, str(delta).encode("utf-8"), "PX", _resolve_px_ms(self._default_ttl), "NX"
            )
            if created is not None and created is not False:
                return delta
        elif self._default_ttl is not None and self._default_ttl <= 0:
            existing = self._driver.get(dkey)
            if existing is None:
                return delta
            _parse_int_bytes(existing, key)
        try:
            return int(self._driver.execute_command("INCRBY", dkey, delta))
        except Exception as exc:
            if _is_redis_int_error(exc):
                msg = f"Cache value for key {key!r} is not a valid integer"
                raise ValueError(msg) from exc
            raise

    def _incr_store(self, key: str, delta: int) -> tuple[int, float | None]:
        assert self._store is not None
        found = self._store_get(key)
        if found is None:
            if self._default_ttl is not None and self._default_ttl <= 0:
                return delta, None
            self._store_put(key, str(delta).encode("utf-8"), self._default_ttl)
            return delta, self._default_ttl
        deadline, payload = found
        cur = _parse_int_bytes(payload, key)
        new_val = cur + delta
        self._store_put_deadline(key, str(new_val).encode("utf-8"), deadline)
        remaining = max(0.0, deadline - self._clock()) if deadline is not None else None
        return new_val, remaining

    def _store_get(self, key: str) -> tuple[float | None, bytes] | None:
        assert self._store is not None
        store_key = _object_store_key(self._ns_dir, key)
        try:
            raw = self._store.get(store_key)
        except FileNotFoundError:
            return None
        deadline, payload = _decode_envelope(raw)
        if deadline is not None and self._clock() >= deadline:
            self._store.delete(store_key)
            self._expirations += 1
            return None
        return deadline, payload

    def _store_put(self, key: str, value: bytes, effective_ttl: float | None) -> None:
        deadline = (self._clock() + effective_ttl) if effective_ttl is not None else None
        self._store_put_deadline(key, value, deadline)

    def _store_put_deadline(self, key: str, value: bytes, deadline: float | None) -> None:
        assert self._store is not None
        store_key = _object_store_key(self._ns_dir, key)
        self._store.put(store_key, _encode_envelope(value, deadline))

    def _store_delete_prefix(self, prefix: str) -> int:
        assert self._store is not None
        prefix_hex = prefix.encode("utf-8").hex()
        cursor: str | None = None
        removed = 0
        while True:
            items, cursor = self._store.list_page(f"{self._ns_dir}/", cursor=cursor, limit=1000)
            for item in items:
                item_hex = _object_store_item_hex(self._ns_dir, item.path)
                if item_hex is not None and item_hex.startswith(prefix_hex):
                    self._store.delete(item.path)
                    removed += 1
            if cursor is None:
                break
        return removed
