"""Unit tests for SQLAlchemyCacheBackend."""

from __future__ import annotations

import time
from datetime import timedelta

import pytest
from sqlalchemy import create_engine, text

from vertebra.extensions.alchemy import (
    CacheEntry,
    CacheStats,
    SQLAlchemyCacheBackend,
)


def test_cache_entry_monotonic_expiration() -> None:
    """Verify CacheEntry is_expired correctly compares against monotonic time."""
    entry = CacheEntry(value="alpha", expires_at=100.0)
    assert entry.value == "alpha"
    assert entry.is_expired(99.9) is False
    assert entry.is_expired(100.0) is True
    assert entry.is_expired(100.1) is True

    indefinite_entry = CacheEntry(value="beta", expires_at=None)
    assert indefinite_entry.is_expired(999999.0) is False


def test_mapping_protocol_and_lru_eviction() -> None:
    """Verify MutableMapping interface and capacity-based LRU eviction."""
    cache = SQLAlchemyCacheBackend(capacity=3)
    cache["k1"] = "v1"
    cache["k2"] = "v2"
    cache["k3"] = "v3"

    assert len(cache) == 3
    assert list(cache) == ["k1", "k2", "k3"]
    assert "k1" in cache
    assert "missing" not in cache
    assert cache["k1"] == "v1"

    cache["k4"] = "v4"
    assert len(cache) == 3
    assert "k2" not in cache
    assert "k1" in cache
    assert "k3" in cache
    assert "k4" in cache

    del cache["k3"]
    assert len(cache) == 2
    assert "k3" not in cache

    with pytest.raises(KeyError):
        _ = cache["k3"]

    with pytest.raises(KeyError):
        del cache["k3"]

    popped = cache.pop("k1")
    assert popped == "v1"
    assert "k1" not in cache
    assert cache.pop("k1", "fallback") == "fallback"

    cache.clear()
    assert len(cache) == 0


def test_sync_ttl_expiration_and_stats() -> None:
    """Verify TTL expiration and stats tracking."""
    cache = SQLAlchemyCacheBackend(capacity=10, default_ttl=0.05)
    cache["fast"] = "quick"
    cache.set("slow", "steady", ttl=10.0)

    assert cache.get("fast") == "quick"
    assert cache.exists("fast") is True
    assert cache.expires_in("fast") is not None
    assert cache.expires_in("slow") is not None
    assert cache.expires_in("missing") is None

    time.sleep(0.06)

    assert cache.get("fast") is None
    assert "fast" not in cache
    assert cache.exists("fast") is False
    assert cache.get("slow") == "steady"

    stats = cache.stats()
    assert isinstance(stats, CacheStats)
    assert stats.expirations >= 1
    assert stats.hits >= 2
    assert stats.capacity == 10


@pytest.mark.anyio
async def test_async_cache_methods() -> None:
    """Verify get_async, set_async, delete_async, exists_async, and clear_async."""
    cache = SQLAlchemyCacheBackend(capacity=5)

    await cache.set_async("a_key", "a_val", ttl=timedelta(seconds=60))
    assert await cache.exists_async("a_key") is True
    assert await cache.get_async("a_key") == "a_val"
    assert await cache.get_async("missing", default="def") == "def"

    ttl = await cache.expires_in_async("a_key")
    assert ttl is not None and ttl > 0

    deleted = await cache.delete_async("a_key")
    assert deleted is True
    assert await cache.exists_async("a_key") is False
    assert await cache.delete_async("a_key") is False

    await cache.set_async("temp", "val")
    assert len(cache) == 1
    await cache.clear_async()
    assert len(cache) == 0


def test_sqlalchemy_compiled_cache_integration() -> None:
    """Verify SQLAlchemyCacheBackend operates as an execution_options compiled_cache for SQLAlchemy Engine."""
    cache = SQLAlchemyCacheBackend(capacity=128)
    engine = create_engine("sqlite:///:memory:", execution_options={"compiled_cache": cache})

    with engine.connect() as conn:
        res1 = conn.execute(text("SELECT 1 AS num")).scalar()
        assert res1 == 1

        res2 = conn.execute(text("SELECT 1 AS num")).scalar()
        assert res2 == 1

    stats = cache.stats()
    assert len(cache) > 0
    assert stats.hits >= 1


def test_shm_factory_initialization() -> None:
    """Verify create_shm_cache_backend initializes segment metadata."""
    cache = SQLAlchemyCacheBackend.create_shm_cache_backend(name="custom-shm-test", capacity=512)
    assert cache.shm_name == "custom-shm-test"
    assert cache.capacity == 512
    cache["sql"] = "SELECT 42"
    assert cache["sql"] == "SELECT 42"
