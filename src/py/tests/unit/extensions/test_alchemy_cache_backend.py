"""Unit tests for SQLAlchemyCacheBackend."""

from __future__ import annotations

import time
from datetime import timedelta

import msgspec
import pytest
from sqlalchemy import Integer, String, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.extensions.alchemy import (
    AsyncCacheManager,
    CacheEntry,
    CacheStats,
    SQLAlchemyCacheBackend,
    ZeroCopyServiceMixin,
    create_async_alchemy_config,
    create_cache_config,
    create_sync_alchemy_config,
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


class _CacheBase(DeclarativeBase):
    pass


class _CachedUser(_CacheBase):
    __tablename__ = "cached_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64))


class _UserDto(msgspec.Struct, frozen=True, gc=False):
    id: int
    name: str


class _SampleService(ZeroCopyServiceMixin):
    pass


@pytest.mark.anyio
async def test_async_cache_manager_sync_async_and_zero_copy_schema_projection() -> None:
    """AsyncCacheManager caches ORM entities, decodes directly into msgspec.Struct, and wires into configs."""
    cfg = create_cache_config(expiration_time=120, capacity=256)
    manager = AsyncCacheManager(cfg)

    user = _CachedUser(id=7, name="Ada Lovelace")
    manager.set_entity_sync("cached_users", 7, user)

    loaded_sync = manager.get_entity_sync("cached_users", 7, _CachedUser)
    assert loaded_sync is not None
    assert loaded_sync.id == 7
    assert loaded_sync.name == "Ada Lovelace"

    dto_sync = manager.get_schema_sync("cached_users", 7, _UserDto)
    assert dto_sync == _UserDto(id=7, name="Ada Lovelace")

    user_async = _CachedUser(id=8, name="Grace Hopper")
    await manager.set_entity_async("cached_users", 8, user_async)
    loaded_async = await manager.get_entity_async("cached_users", 8, _CachedUser)
    assert loaded_async is not None
    assert loaded_async.name == "Grace Hopper"

    dto_async = await manager.get_schema_async("cached_users", 8, _UserDto)
    assert dto_async == _UserDto(id=8, name="Grace Hopper")

    await manager.set_many_async("cached_users:all", [user, user_async])
    many_dtos = await manager.get_many_schema_async("cached_users:all", _UserDto)
    assert many_dtos == [
        _UserDto(id=7, name="Ada Lovelace"),
        _UserDto(id=8, name="Grace Hopper"),
    ]

    svc = _SampleService()
    assert svc.to_schema(user, schema_type=_UserDto) == _UserDto(id=7, name="Ada Lovelace")
    assert svc.to_schema([user, user_async], schema_type=_UserDto) == [
        _UserDto(id=7, name="Ada Lovelace"),
        _UserDto(id=8, name="Grace Hopper"),
    ]

    async_cfg = create_async_alchemy_config("sqlite+aiosqlite:///:memory:", cache_config="memory://")
    assert isinstance(async_cfg.cache_manager, AsyncCacheManager)

    sync_cfg = create_sync_alchemy_config("sqlite:///:memory:", cache_config=SQLAlchemyCacheBackend(capacity=64))
    assert isinstance(sync_cfg.cache_manager, AsyncCacheManager)


@pytest.mark.anyio
async def test_alchemy_cache_clear_removes_only_its_prefix() -> None:
    """SQLAlchemyCacheBackend.clear and clear_async remove only keys under the backend's key_prefix."""
    sync_driver = SyncRedisDriver(RedisConfig(url="memory://"))
    async_driver = AsyncRedisDriver(RedisConfig(url="memory://"))
    sync_driver.set("other:keep", b"1")
    await async_driver.set("other:keep", b"1")

    backend_a = SQLAlchemyCacheBackend(
        sync_driver=sync_driver,
        async_driver=async_driver,
        key_prefix="vtb:aa:",
    )
    backend_b = SQLAlchemyCacheBackend(
        sync_driver=sync_driver,
        async_driver=async_driver,
        key_prefix="vtb:other:",
    )

    backend_a.set("user:1", {"id": 1})
    backend_a.set("vtb:aa:user:2", {"id": 2})
    backend_b.set("user:9", {"id": 9})

    assert sync_driver.get("vtb:aa:user:1") is not None
    assert sync_driver.get("vtb:aa:user:2") is not None
    assert sync_driver.get("vtb:other:user:9") is not None

    backend_a.clear()
    assert sync_driver.get("other:keep") == b"1"
    assert sync_driver.get("vtb:aa:user:1") is None
    assert sync_driver.get("vtb:aa:user:2") is None
    assert sync_driver.get("vtb:other:user:9") is not None
    assert backend_b.get("user:9") == {"id": 9}

    await backend_a.set_async("user:1", {"id": 1})
    await backend_a.set_async("vtb:aa:user:2", {"id": 2})
    await backend_b.set_async("user:9", {"id": 9})

    await backend_a.clear_async()
    assert await async_driver.get("other:keep") == b"1"
    assert await async_driver.get("vtb:aa:user:1") is None
    assert await async_driver.get("vtb:aa:user:2") is None
    assert await async_driver.get("vtb:other:user:9") is not None
    assert await backend_b.get_async("user:9") == {"id": 9}


@pytest.mark.anyio
async def test_alchemy_cache_sends_sub_second_ttl_as_milliseconds() -> None:
    """Sub-second TTLs are sent to the Redis/Valkey driver as millisecond PX expiries."""
    sync_driver = SyncRedisDriver(RedisConfig(url="memory://"))
    async_driver = AsyncRedisDriver(RedisConfig(url="memory://"))
    backend = SQLAlchemyCacheBackend(
        sync_driver=sync_driver,
        async_driver=async_driver,
        key_prefix="vtb:aa:",
    )

    backend.set("ephemeral", {"ok": True}, ttl=0.25)
    assert 1 <= sync_driver.pttl("vtb:aa:ephemeral") <= 250

    await backend.set_async("ephemeral_async", {"ok": True}, ttl=timedelta(milliseconds=250))
    assert 1 <= await async_driver.pttl("vtb:aa:ephemeral_async") <= 250


@pytest.mark.parametrize("bad_prefix", ["", "bad*", "bad?key", "bad[0]"])
def test_alchemy_cache_rejects_invalid_key_prefix(bad_prefix: str) -> None:
    """Empty or glob-containing key_prefix values raise ValueError."""
    with pytest.raises(ValueError, match="key_prefix"):
        SQLAlchemyCacheBackend(key_prefix=bad_prefix)
