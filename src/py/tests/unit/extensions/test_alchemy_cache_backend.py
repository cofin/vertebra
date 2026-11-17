"""Unit tests for Advanced Alchemy cache regions, serializers, and CacheManager integration."""

from __future__ import annotations

import importlib
import importlib.util
import time
from typing import TYPE_CHECKING, Literal

import msgspec
import pytest
from advanced_alchemy.cache import CacheManager
from advanced_alchemy.cache._null import NO_VALUE, SyncCacheRegionProtocol
from advanced_alchemy.filters import LimitOffsetFilter
from advanced_alchemy.repository import SQLAlchemyAsyncRepository, SQLAlchemySyncRepository
from advanced_alchemy.service import (
    OffsetPagination,
    SQLAlchemyAsyncRepositoryService,
    SQLAlchemySyncRepositoryService,
)
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

import vertebra.extensions.alchemy as alchemy_ext
import vertebra.extensions.alchemy.cache as alchemy_cache_mod
import vertebra.extensions.django.cache as django_cache_mod
import vertebra.extensions.sqlalchemy as sqlalchemy_ext
from vertebra.cache import AsyncCacheStore, CacheStats, SyncCacheStore
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig, SyncRedisDriver
from vertebra.extensions.alchemy import (
    AsyncCacheRegion,
    SyncCacheRegion,
    ZeroCopyServiceMixin,
    create_async_alchemy_config,
    create_cache_config,
    create_sync_alchemy_config,
    msgspec_model_deserializer,
    msgspec_model_serializer,
)

if TYPE_CHECKING:
    from pathlib import Path

    from tests.unit.conftest import Resp3FakeServer


class _CacheBase(DeclarativeBase):
    pass


class _CachedUser(_CacheBase):
    __tablename__ = "cached_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64))


class _UserDto(msgspec.Struct, frozen=True, gc=False):
    id: int
    name: str


class _AsyncUserRepository(SQLAlchemyAsyncRepository[_CachedUser]):
    model_type = _CachedUser


class _AsyncUserService(ZeroCopyServiceMixin, SQLAlchemyAsyncRepositoryService[_CachedUser, _AsyncUserRepository]):
    repository_type = _AsyncUserRepository


class _SyncUserRepository(SQLAlchemySyncRepository[_CachedUser]):
    model_type = _CachedUser


class _SyncUserService(ZeroCopyServiceMixin, SQLAlchemySyncRepositoryService[_CachedUser, _SyncUserRepository]):
    repository_type = _SyncUserRepository


@pytest.mark.parametrize("scheme", ["memory", "shm", "file"])
def test_cache_manager_entity_roundtrip_and_invalidate_all(scheme: str, tmp_path: Path) -> None:
    """Upstream CacheManager(create_cache_config(...)) supports entity round trips, NO_VALUE misses, and invalidate_all_sync."""
    if scheme == "file":
        url = f"file://{tmp_path / 'aa_cache'}"
    elif scheme == "shm":
        url = f"shm://aa_test_{time.time_ns()}"
    else:
        url = "memory://"

    cfg = create_cache_config(url, expiration_time=120, capacity=256)
    manager = CacheManager(cfg)

    assert manager.get_sync("missing:key") is NO_VALUE
    assert manager.get_entity_sync("cached_users", 999, _CachedUser) is None

    ada = _CachedUser(id=7, name="Ada Lovelace")
    grace = _CachedUser(id=8, name="Grace Hopper")
    manager.set_entity_sync("cached_users", 7, ada)
    manager.set_entity_sync("cached_users", 8, grace)

    loaded_ada = manager.get_entity_sync("cached_users", 7, _CachedUser)
    assert loaded_ada is not None
    assert loaded_ada.id == 7
    assert loaded_ada.name == "Ada Lovelace"

    manager.set_many_sync("cached_users:all", [ada, grace])
    many = manager.get_many_sync("cached_users:all", _CachedUser)
    assert many is not None
    assert [u.name for u in many] == ["Ada Lovelace", "Grace Hopper"]

    manager.invalidate_entity_sync("cached_users", 7)
    assert manager.get_entity_sync("cached_users", 7, _CachedUser) is None
    assert manager.get_entity_sync("cached_users", 8, _CachedUser) is not None

    manager.invalidate_all_sync()
    assert manager.get_entity_sync("cached_users", 8, _CachedUser) is None
    assert manager.get_many_sync("cached_users:all", _CachedUser) is None


def test_create_cache_config_negative_expiration_and_eager_validation() -> None:
    """Negative or zero expiration_time maps to ttl=None (no expiry) and bad URLs fail eagerly."""
    now = 100.0
    store = SyncCacheStore("memory://", _clock=lambda: now)
    region = SyncCacheRegion(store, expiration_time=-1)
    region.set("forever", {"ok": True})
    now += 86400.0
    assert region.get("forever") == {"ok": True}

    cfg = create_cache_config("memory://", expiration_time=-1)
    manager = CacheManager(cfg)
    user = _CachedUser(id=1, name="Persistent")
    manager.set_entity_sync("cached_users", 1, user)
    loaded = manager.get_entity_sync("cached_users", 1, _CachedUser)
    assert loaded is not None
    assert loaded.name == "Persistent"

    with pytest.raises(ValueError, match="Unsupported cache URL scheme"):
        create_cache_config("invalid://bad-scheme")


def test_sync_cache_region_protocol_and_value_framing() -> None:
    """SyncCacheRegion satisfies SyncCacheRegionProtocol, frames None/bytes/str/collections, and forwards TTL."""
    now = 50.0
    store = SyncCacheStore("memory://", key_prefix="vtb:region:", _clock=lambda: now)
    region: SyncCacheRegionProtocol = SyncCacheRegion(store, expiration_time=60)

    assert region.configure("ignored", expiration_time=30, arguments={"x": 1}) is region
    assert region.get("missing") is NO_VALUE

    calls = 0

    def _create_none() -> None:
        nonlocal calls
        calls += 1
        return None

    assert region.get_or_create("none_key", _create_none) is None
    assert region.get_or_create("none_key", _create_none) is None
    assert region.get("none_key") is None
    assert calls == 1

    region.set("bytes_key", b"raw-bytes")
    region.set("str_key", "raw-text")
    region.set("list_key", ["a", "b"])
    region.set("dict_key", {"k": "v", "n": 1})
    assert isinstance(region.get("bytes_key"), bytes)
    assert region.get("bytes_key") == b"raw-bytes"
    assert isinstance(region.get("str_key"), str)
    assert region.get("str_key") == "raw-text"
    assert region.get("list_key") == ["a", "b"]
    assert region.get("dict_key") == {"k": "v", "n": 1}

    created = region.get_or_create("short_ttl", lambda: "ephemeral", expiration_time=5)
    assert created == "ephemeral"
    now += 6.0
    assert region.get("short_ttl") is NO_VALUE

    region.delete("bytes_key")
    assert region.get("bytes_key") is NO_VALUE

    region.invalidate()
    assert region.get("str_key") is NO_VALUE
    stats = store.stats()
    assert isinstance(stats, CacheStats)
    assert stats.hits >= 2


@pytest.mark.anyio
async def test_async_cache_region_methods_and_ttl_forwarding() -> None:
    """AsyncCacheRegion supports async get, get_or_create, set, delete, invalidate, and sync configure."""
    now = 200.0
    store = AsyncCacheStore("memory://", key_prefix="vtb:aregion:", _clock=lambda: now)
    region = AsyncCacheRegion(store, expiration_time=60)

    assert region.configure("ignored", expiration_time=-1) is region
    assert await region.get("missing") is NO_VALUE

    calls = 0

    async def _create_none() -> str | None:
        nonlocal calls
        calls += 1
        return None

    assert await region.get_or_create("none_key", _create_none) is None
    assert await region.get_or_create("none_key", _create_none) is None
    assert await region.get("none_key") is None
    assert calls == 1

    await region.set("forever", {"id": 42})
    now += 10000.0
    assert await region.get("forever") == {"id": 42}

    async def _create_ephemeral() -> str:
        return "short"

    assert await region.get_or_create("ephemeral", _create_ephemeral, expiration_time=4) == "short"
    now += 5.0
    assert await region.get("ephemeral") is NO_VALUE

    await region.delete("forever")
    assert await region.get("forever") is NO_VALUE

    await region.set("to_clear", "val")
    await region.invalidate()
    assert await region.get("to_clear") is NO_VALUE


@pytest.mark.parametrize("fmt", ["msgpack", "json"])
def test_msgspec_model_serializer_and_deserializer_roundtrip(fmt: Literal["msgpack", "json"]) -> None:
    """Serializer/deserializer pair handles ORM models, msgspec structs, and schema_type=None."""
    user = _CachedUser(id=11, name="Katherine Johnson")
    encoded = msgspec_model_serializer(user, format=fmt)

    decoded_model = msgspec_model_deserializer(encoded, _CachedUser, format=fmt)
    assert isinstance(decoded_model, _CachedUser)
    assert decoded_model.id == 11
    assert decoded_model.name == "Katherine Johnson"

    decoded_dto = msgspec_model_deserializer(encoded, _UserDto, format=fmt)
    assert decoded_dto == _UserDto(id=11, name="Katherine Johnson")

    decoded_raw = msgspec_model_deserializer(encoded, None, format=fmt)
    assert decoded_raw == {"id": 11, "name": "Katherine Johnson"}


@pytest.mark.anyio
async def test_zero_copy_service_mixin_async_repository_pagination() -> None:
    """ZeroCopyServiceMixin with SQLAlchemyAsyncRepositoryService returns OffsetPagination and single schemas."""
    engine = create_async_engine("sqlite+vtb_async:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(_CacheBase.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session:
            service = _AsyncUserService(session=session)
            seeded = [
                _CachedUser(id=1, name="Ada Lovelace"),
                _CachedUser(id=2, name="Grace Hopper"),
                _CachedUser(id=3, name="Katherine Johnson"),
                _CachedUser(id=4, name="Margaret Hamilton"),
                _CachedUser(id=5, name="Radia Perlman"),
            ]
            await service.create_many(seeded, auto_commit=True)

            limit_offset = LimitOffsetFilter(limit=2, offset=1)
            items, total = await service.get_many_and_count(limit_offset, order_by=[(_CachedUser.id, False)])
            assert total == 5

            dto_page = service.to_schema(items, total=total, filters=[limit_offset], schema_type=_UserDto)
            assert isinstance(dto_page, OffsetPagination)
            assert len(dto_page.items) == 2
            assert dto_page.limit == 2
            assert dto_page.offset == 1
            assert dto_page.total == total
            assert dto_page.items == [
                _UserDto(id=2, name="Grace Hopper"),
                _UserDto(id=3, name="Katherine Johnson"),
            ]

            raw_page = service.to_schema(items, total=total, filters=[limit_offset])
            assert isinstance(raw_page, OffsetPagination)
            assert raw_page.items == list(items)
            assert raw_page.limit == 2
            assert raw_page.offset == 1
            assert raw_page.total == total

            single_dto = service.to_schema(items[0], schema_type=_UserDto)
            assert single_dto == _UserDto(id=2, name="Grace Hopper")

            single_raw = service.to_schema(items[0])
            assert single_raw is items[0]

            default_bounds_page = service.to_schema(items, schema_type=_UserDto, format="json")
            assert isinstance(default_bounds_page, OffsetPagination)
            assert default_bounds_page.limit == 2
            assert default_bounds_page.offset == 0
            assert default_bounds_page.total == 2
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("filters", "expected_limit", "expected_offset"),
    [
        (None, 0, 0),
        ([LimitOffsetFilter(limit=10, offset=20)], 10, 20),
    ],
)
def test_zero_copy_service_mixin_sync_repository_empty_and_dict_fallback(
    filters: list[LimitOffsetFilter] | None,
    expected_limit: int,
    expected_offset: int,
) -> None:
    """ZeroCopyServiceMixin with SQLAlchemySyncRepositoryService preserves total on empty pages and delegates dicts."""
    engine = create_engine("sqlite:///:memory:")
    try:
        _CacheBase.metadata.create_all(engine)
        session_factory = sessionmaker(engine, expire_on_commit=False)
        with session_factory() as session:
            service = _SyncUserService(session=session)
            service.create_many(
                [_CachedUser(id=1, name="Ada Lovelace"), _CachedUser(id=2, name="Grace Hopper")],
                auto_commit=True,
            )
            limit_offset = LimitOffsetFilter(limit=1, offset=1)
            items, total = service.get_many_and_count(limit_offset, order_by=[(_CachedUser.id, False)])
            sync_page = service.to_schema(items, total=total, filters=[limit_offset], schema_type=_UserDto)
            assert isinstance(sync_page, OffsetPagination)
            assert sync_page.items == [_UserDto(id=2, name="Grace Hopper")]
            assert sync_page.limit == 1
            assert sync_page.offset == 1
            assert sync_page.total == 2

            empty_rows: list[_CachedUser] = []
            empty_raw_page = service.to_schema(empty_rows, total=5, filters=filters)
            assert isinstance(empty_raw_page, OffsetPagination)
            assert empty_raw_page.items == []
            assert empty_raw_page.limit == expected_limit
            assert empty_raw_page.offset == expected_offset
            assert empty_raw_page.total == 5

            empty_dto_page = service.to_schema([], total=5, filters=filters, schema_type=_UserDto)
            assert isinstance(empty_dto_page, OffsetPagination)
            assert empty_dto_page.items == []
            assert empty_dto_page.limit == expected_limit
            assert empty_dto_page.offset == expected_offset
            assert empty_dto_page.total == 5

            dict_dto = service.to_schema({"id": 9, "name": "Dict Row"}, schema_type=_UserDto)
            assert dict_dto == _UserDto(id=9, name="Dict Row")

            dict_page = service.to_schema(
                [{"id": 9, "name": "Dict Row"}],
                total=1,
                filters=[LimitOffsetFilter(limit=5, offset=0)],
                schema_type=_UserDto,
            )
            assert isinstance(dict_page, OffsetPagination)
            assert dict_page.items == [_UserDto(id=9, name="Dict Row")]
            assert dict_page.limit == 5
            assert dict_page.offset == 0
            assert dict_page.total == 1
    finally:
        engine.dispose()


def test_plugin_cache_wiring() -> None:
    """Plugin factories wire upstream CacheManager from URLs, stores, and regions."""
    async_cfg = create_async_alchemy_config("sqlite+aiosqlite:///:memory:", cache_config="memory://")
    assert isinstance(async_cfg.cache_manager, CacheManager)

    sync_store = SyncCacheStore("memory://")
    sync_cfg = create_sync_alchemy_config("sqlite:///:memory:", cache_config=sync_store)
    assert isinstance(sync_cfg.cache_manager, CacheManager)

    region_cfg = create_sync_alchemy_config("sqlite:///:memory:", cache_config=SyncCacheRegion(sync_store))
    assert isinstance(region_cfg.cache_manager, CacheManager)


def test_redis_backed_cache_manager_invalidate_all_scoped_to_prefix(resp3_server: Resp3FakeServer) -> None:
    """CacheManager.invalidate_all_sync clears only keys under the region key_prefix."""
    driver = SyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
    try:
        driver.set("other:keep", b"1")
        cfg_a = create_cache_config(resp3_server.redis_url, key_prefix="vtb:aa:")
        cfg_b = create_cache_config(resp3_server.redis_url, key_prefix="vtb:other:")
        mgr_a = CacheManager(cfg_a)
        mgr_b = CacheManager(cfg_b)

        mgr_a.set_entity_sync("cached_users", 1, _CachedUser(id=1, name="Alpha"))
        mgr_b.set_entity_sync("cached_users", 2, _CachedUser(id=2, name="Beta"))

        mgr_a.invalidate_all_sync()
        assert driver.get("other:keep") == b"1"
        assert mgr_a.get_entity_sync("cached_users", 1, _CachedUser) is None
        loaded_b = mgr_b.get_entity_sync("cached_users", 2, _CachedUser)
        assert loaded_b is not None
        assert loaded_b.name == "Beta"
    finally:
        driver.close()


def test_async_redis_driver_direct_cleanup_smoke(resp3_server: Resp3FakeServer) -> None:
    """Verify SyncCacheStore with resp3_server rejects invalid key prefixes cleanly."""
    with pytest.raises(ValueError, match="key_prefix"):
        create_cache_config(resp3_server.redis_url, key_prefix="bad*")
    assert AsyncRedisDriver is not None


def test_legacy_cache_classes_and_sqlalchemy_cache_module_deleted() -> None:
    """Deleted cache backends, dogpile adapters, and sqlalchemy.cache must not exist."""
    removed_alchemy = (
        "AsyncCacheBackend",
        "AsyncCacheManager",
        "CacheEntry",
        "CacheStats",
        "DogpileMemoryBackend",
        "DogpileRedisBackend",
        "DogpileShmBackend",
        "DogpileStorageBackend",
        "DogpileValkeyBackend",
        "SQLAlchemyCacheBackend",
        "SyncCacheBackend",
        "register_dogpile_backends",
    )
    for name in removed_alchemy:
        assert not hasattr(alchemy_cache_mod, name)
        assert not hasattr(alchemy_ext, name)

    removed_sqlalchemy = (
        "AsyncCacheBackend",
        "CacheEntry",
        "CacheStats",
        "CompiledCache",
        "ResultCache",
        "SQLAlchemyCacheBackend",
        "SyncCacheBackend",
    )
    for name in removed_sqlalchemy:
        assert not hasattr(sqlalchemy_ext, name)

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("vertebra.extensions.sqlalchemy.cache")

    django_source = importlib.util.find_spec(django_cache_mod.__name__)
    assert django_source is not None and django_source.origin is not None
    with open(django_source.origin, encoding="utf-8") as handle:
        assert "vertebra.extensions.alchemy" not in handle.read()
