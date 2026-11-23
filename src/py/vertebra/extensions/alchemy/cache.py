"""Advanced Alchemy cache regions over ``vertebra.cache``, serializers, and config wiring."""

from __future__ import annotations

import datetime as dt
import decimal
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Self, cast, overload

import msgspec
from sqlalchemy import inspect as sa_inspect

from vertebra.cache import AsyncCacheStore, SyncCacheStore
from vertebra.typing import MissingDependencyError
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_json,
    decode_msgpack,
    encode_json_bytes,
    encode_msgpack_bytes,
)

try:
    from advanced_alchemy.cache import CacheConfig, CacheManager, setup_cache_listeners
    from advanced_alchemy.cache._null import NO_VALUE
    from advanced_alchemy.filters import LimitOffsetFilter
    from advanced_alchemy.service import CursorPagination, OffsetPagination, ResultConverter, find_filter
except ImportError as exc:
    raise MissingDependencyError(
        "advanced-alchemy",
        install_package="advanced-alchemy",
        extra="alchemy",
    ) from exc

if TYPE_CHECKING:
    from advanced_alchemy.base import ModelProtocol
    from advanced_alchemy.filters import StatementFilter
    from advanced_alchemy.repository.typing import ModelOrRowMappingT
    from advanced_alchemy.utils.serialization import ModelDTOT
    from sqlalchemy import ColumnElement, RowMapping
    from sqlalchemy.engine.row import Row

__all__ = (
    "AsyncCacheRegion",
    "CacheConfig",
    "CacheManager",
    "NO_VALUE",
    "SyncCacheRegion",
    "ZeroCopyServiceMixin",
    "create_cache_config",
    "msgspec_model_deserializer",
    "msgspec_model_serializer",
    "setup_cache_listeners",
)

_MODEL_KEY = "__aa_model__"
_TABLE_KEY = "__aa_table__"


def _normalize_ttl(expiration_time: int | None) -> float | None:
    """Normalize upstream cache region expiration seconds into store TTL seconds."""
    if expiration_time is None or expiration_time <= 0:
        return None
    return float(expiration_time)


class _BaseCacheRegion:
    """Shared expiration configuration across sync and async cache regions."""

    __slots__ = ("_expiration_time", "_store")

    _expiration_time: int
    _store: Any

    def __init__(self, store: Any, *, expiration_time: int = 3600) -> None:
        self._store = store
        self._expiration_time = expiration_time

    def configure(
        self,
        backend: str = "",
        expiration_time: int | None = None,
        arguments: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> Self:
        """Update region expiration configuration and return ``self``."""
        _ = (backend, arguments, kwargs)
        if expiration_time is not None:
            self._expiration_time = expiration_time
        return self


class SyncCacheRegion(_BaseCacheRegion):
    """Synchronous cache region implementing Advanced Alchemy's ``SyncCacheRegionProtocol``."""

    __slots__ = ()

    _store: SyncCacheStore

    def __init__(self, store: SyncCacheStore, *, expiration_time: int = 3600) -> None:
        """Initialize a synchronous cache region backed by ``store``."""
        super().__init__(store, expiration_time=expiration_time)

    def get(self, key: str, expiration_time: int | None = None) -> object:
        """Fetch and decode ``key``, returning upstream ``NO_VALUE`` on miss."""
        _ = expiration_time
        raw = self._store.get(key)
        if raw is None:
            return NO_VALUE
        return decode_msgpack(raw)

    def get_or_create[T](
        self,
        key: str,
        creator: Callable[[], T],
        expiration_time: int | None = None,
    ) -> T:
        """Return the cached value for ``key`` or invoke ``creator`` and store its result."""
        raw = self._store.get(key)
        if raw is not None:
            return cast("T", decode_msgpack(raw))
        value = creator()
        resolved_expiration = self._expiration_time if expiration_time is None else expiration_time
        self._store.set(key, encode_msgpack_bytes(value), ttl=_normalize_ttl(resolved_expiration))
        return value

    def set(self, key: str, value: object) -> None:
        """Encode and store ``value`` under ``key`` using the region default TTL."""
        self._store.set(key, encode_msgpack_bytes(value), ttl=_normalize_ttl(self._expiration_time))

    def delete(self, key: str) -> None:
        """Remove ``key`` from the underlying store."""
        self._store.delete(key)

    def invalidate(self) -> None:
        """Clear every key under the store namespace."""
        self._store.clear()


class AsyncCacheRegion(_BaseCacheRegion):
    """Asynchronous cache region implementing Advanced Alchemy's ``AsyncCacheRegionProtocol``."""

    __slots__ = ()

    _store: AsyncCacheStore

    def __init__(self, store: AsyncCacheStore, *, expiration_time: int = 3600) -> None:
        """Initialize an asynchronous cache region backed by ``store``."""
        super().__init__(store, expiration_time=expiration_time)

    async def get(self, key: str, expiration_time: int | None = None) -> object:
        """Asynchronously fetch and decode ``key``, returning upstream ``NO_VALUE`` on miss."""
        _ = expiration_time
        raw = await self._store.get(key)
        if raw is None:
            return NO_VALUE
        return decode_msgpack(raw)

    async def get_or_create[T](
        self,
        key: str,
        creator: Callable[[], Awaitable[T]],
        expiration_time: int | None = None,
    ) -> T:
        """Asynchronously return the cached value for ``key`` or await ``creator`` and store it."""
        raw = await self._store.get(key)
        if raw is not None:
            return cast("T", decode_msgpack(raw))
        value = await creator()
        resolved_expiration = self._expiration_time if expiration_time is None else expiration_time
        await self._store.set(key, encode_msgpack_bytes(value), ttl=_normalize_ttl(resolved_expiration))
        return value

    async def set(self, key: str, value: object) -> None:
        """Asynchronously encode and store ``value`` under ``key`` using the region default TTL."""
        await self._store.set(key, encode_msgpack_bytes(value), ttl=_normalize_ttl(self._expiration_time))

    async def delete(self, key: str) -> None:
        """Asynchronously remove ``key`` from the underlying store."""
        await self._store.delete(key)

    async def invalidate(self) -> None:
        """Asynchronously clear every key under the store namespace."""
        await self._store.clear()


def _encode_column_value(value: object) -> object:
    """Convert non-primitive column values into msgspec-serializable markers."""
    if isinstance(value, uuid.UUID):
        return {"__vtb_uuid__": str(value)}
    if isinstance(value, dt.datetime):
        return {"__vtb_dt__": value.isoformat()}
    if isinstance(value, dt.date):
        return {"__vtb_date__": value.isoformat()}
    if isinstance(value, dt.time):
        return {"__vtb_time__": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__vtb_dec__": str(value)}
    return value


def _decode_column_value(value: object) -> object:
    """Restore special column markers back to Python domain types."""
    if isinstance(value, dict) and len(value) == 1:
        typed_dict = cast("dict[str, object]", value)
        if "__vtb_uuid__" in typed_dict:
            return uuid.UUID(str(typed_dict["__vtb_uuid__"]))
        if "__vtb_dt__" in typed_dict:
            return dt.datetime.fromisoformat(str(typed_dict["__vtb_dt__"]))
        if "__vtb_date__" in typed_dict:
            return dt.date.fromisoformat(str(typed_dict["__vtb_date__"]))
        if "__vtb_time__" in typed_dict:
            return dt.time.fromisoformat(str(typed_dict["__vtb_time__"]))
        if "__vtb_dec__" in typed_dict:
            return decimal.Decimal(str(typed_dict["__vtb_dec__"]))
    return value


def msgspec_model_serializer(
    value: object,
    *,
    format: Literal["msgpack", "json"] = "msgpack",
) -> bytes:
    """Serialize a SQLAlchemy model or schema value to MessagePack or JSON bytes via msgspec."""
    try:
        mapper = sa_inspect(value.__class__)
    except Exception:
        mapper = None

    if mapper is not None:
        data: dict[str, object] = {
            _MODEL_KEY: value.__class__.__name__,
            _TABLE_KEY: getattr(value.__class__, "__tablename__", value.__class__.__name__),
        }
        for column in mapper.columns:
            if getattr(column, "_insert_sentinel", False):
                continue
            data[column.key] = _encode_column_value(getattr(value, column.key))
        payload: object = data
    else:
        payload = value

    if format == "json":
        return bytes(encode_json_bytes(payload))
    return bytes(encode_msgpack_bytes(payload))


def msgspec_model_deserializer[T](
    data: bytes,
    schema_type: type[T] | None = None,
    *,
    format: Literal["msgpack", "json"] = "msgpack",
) -> T | object:
    """Deserialize MessagePack or JSON bytes into a detached SQLAlchemy model, schema, or raw value."""
    raw_obj: object = decode_json(data) if format == "json" else decode_msgpack(data)

    if schema_type is None:
        if isinstance(raw_obj, dict):
            raw_dict = cast("dict[object, object]", raw_obj)
            return {str(k): _decode_column_value(v) for k, v in raw_dict.items() if k not in (_MODEL_KEY, _TABLE_KEY)}
        return raw_obj

    if not isinstance(raw_obj, dict):
        return msgspec.convert(raw_obj, type=schema_type)

    raw_mapping = cast("dict[object, object]", raw_obj)
    parsed: dict[str, object] = {str(k): _decode_column_value(v) for k, v in raw_mapping.items()}
    serialized_model = parsed.pop(_MODEL_KEY, None)
    parsed.pop(_TABLE_KEY, None)

    try:
        sa_inspect(schema_type)
        is_sa_model = True
    except Exception:
        is_sa_model = False

    if is_sa_model:
        if serialized_model and serialized_model != schema_type.__name__:
            msg = f"Cannot deserialize {serialized_model} data as {schema_type.__name__}"
            raise ValueError(msg)
        return schema_type(**parsed)

    plan = ZeroCopyCodecRegistry.compile(schema_type)
    if plan.kind in ("msgspec", "dataclass", "attrs"):
        return cast("T", plan.hydrate_one(parsed))
    return msgspec.convert(parsed, type=schema_type)


def _build_pagination[T](
    items: list[T],
    filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None,
    total: int | None,
) -> OffsetPagination[T]:
    """Build an ``OffsetPagination`` using ``LimitOffsetFilter`` from ``filters`` and explicit ``total``."""
    limit_offset = find_filter(LimitOffsetFilter, filters=filters or ())
    limit = limit_offset.limit if limit_offset is not None else len(items)
    offset = limit_offset.offset if limit_offset is not None else 0
    resolved_total = len(items) if total is None else total
    return OffsetPagination(
        items=items,
        limit=limit,
        offset=offset,
        total=resolved_total,
    )


class ZeroCopyServiceMixin:
    """Mixin for Advanced Alchemy repository services accelerating ``to_schema`` via C-API vectorcall."""

    __slots__ = ()

    @overload
    def to_schema(
        self,
        data: ModelOrRowMappingT,
        *,
        schema_type: None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> ModelOrRowMappingT: ...

    @overload
    def to_schema(
        self,
        data: ModelProtocol | RowMapping | Row[Any] | Mapping[str, object],
        *,
        schema_type: type[ModelDTOT],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> ModelDTOT: ...

    @overload
    def to_schema(
        self,
        data: ModelOrRowMappingT,
        total: int | None = None,
        *,
        schema_type: None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> ModelOrRowMappingT: ...

    @overload
    def to_schema(
        self,
        data: ModelProtocol | RowMapping | Row[Any] | Mapping[str, object],
        total: int | None = None,
        *,
        schema_type: type[ModelDTOT],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> ModelDTOT: ...

    @overload
    def to_schema(
        self,
        data: ModelOrRowMappingT,
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> ModelOrRowMappingT: ...

    @overload
    def to_schema(
        self,
        data: ModelProtocol | RowMapping | Row[Any] | Mapping[str, object],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: type[ModelDTOT],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> ModelDTOT: ...

    @overload
    def to_schema(
        self,
        data: Sequence[Row[Any]],
        total: int,
        *,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> OffsetPagination[Row[Any]]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelOrRowMappingT],
        *,
        schema_type: None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> OffsetPagination[ModelOrRowMappingT]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelProtocol] | Sequence[RowMapping] | Sequence[Row[Any]] | Sequence[Mapping[str, object]],
        *,
        schema_type: type[ModelDTOT],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> OffsetPagination[ModelDTOT]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelOrRowMappingT],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> OffsetPagination[ModelOrRowMappingT]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelProtocol] | Sequence[RowMapping] | Sequence[Row[Any]] | Sequence[Mapping[str, object]],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: type[ModelDTOT],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> OffsetPagination[ModelDTOT]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelOrRowMappingT],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: None = None,
        pagination_type: Literal["cursor"],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> CursorPagination[ModelOrRowMappingT]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelProtocol] | Sequence[RowMapping] | Sequence[Row[Any]] | Sequence[Mapping[str, object]],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: type[ModelDTOT],
        pagination_type: Literal["cursor"],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> CursorPagination[ModelDTOT]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelOrRowMappingT],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: None = None,
        pagination_type: Literal["limit_offset"],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> OffsetPagination[ModelOrRowMappingT]: ...

    @overload
    def to_schema(
        self,
        data: Sequence[ModelProtocol] | Sequence[RowMapping] | Sequence[Row[Any]] | Sequence[Mapping[str, object]],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: type[ModelDTOT],
        pagination_type: Literal["limit_offset"],
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> OffsetPagination[ModelDTOT]: ...

    def to_schema(
        self,
        data: ModelOrRowMappingT
        | Sequence[ModelOrRowMappingT]
        | ModelProtocol
        | Sequence[ModelProtocol]
        | RowMapping
        | Sequence[RowMapping]
        | Row[Any]
        | Sequence[Row[Any]]
        | Mapping[str, object]
        | Sequence[Mapping[str, object]],
        total: int | None = None,
        filters: Sequence[StatementFilter | ColumnElement[bool]] | Sequence[StatementFilter] | None = None,
        *,
        schema_type: type[ModelDTOT] | None = None,
        pagination_type: Literal["limit_offset", "cursor"] | None = None,
        format: Literal["msgpack", "json"] = "msgpack",
    ) -> (
        ModelOrRowMappingT
        | OffsetPagination[ModelOrRowMappingT]
        | ModelDTOT
        | OffsetPagination[ModelDTOT]
        | CursorPagination[ModelOrRowMappingT]
        | CursorPagination[ModelDTOT]
        | OffsetPagination[Row[Any]]
    ):
        """Convert ORM model instance(s) into ``schema_type``, ``OffsetPagination``, or ``CursorPagination``."""
        _ = format
        resolved_filters = filters if filters is not None else ()
        cursor_page = ResultConverter._to_cursor_page(
            cast("ResultConverter", self),
            data,
            resolved_filters,
            schema_type,
            pagination_type,
        )
        if cursor_page is not None:
            return cursor_page
        if schema_type is None:
            if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
                return cast(
                    "OffsetPagination[ModelOrRowMappingT]",
                    _build_pagination(list(data), filters, total),
                )
            return cast("ModelOrRowMappingT", data)

        plan = ZeroCopyCodecRegistry.compile(schema_type)
        if plan.kind in ("msgspec", "dataclass", "attrs"):
            if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
                items = list(data)
                if not items:
                    return cast("OffsetPagination[ModelDTOT]", _build_pagination([], filters, total))
                if not isinstance(items[0], Mapping) and hasattr(items[0], "__dict__"):
                    projected = cast("list[ModelDTOT]", plan.orm_to_struct(items))
                    return cast("OffsetPagination[ModelDTOT]", _build_pagination(projected, filters, total))
            elif not isinstance(data, Mapping) and hasattr(data, "__dict__"):
                return cast("ModelDTOT", plan.orm_to_struct(data))

        converter_fn: Any = ResultConverter.to_schema
        return cast(
            "ModelDTOT | OffsetPagination[ModelDTOT] | CursorPagination[ModelDTOT]",
            converter_fn(
                self,
                data,
                total=total,
                filters=filters,
                schema_type=schema_type,
                pagination_type=pagination_type,
            ),
        )


def create_cache_config(
    url: str = "memory://",
    *,
    l1_url: str | None = None,
    expiration_time: int = 3600,
    key_prefix: str = "vtb:aa:",
    capacity: int = 2048,
) -> CacheConfig:
    """Create an Advanced Alchemy ``CacheConfig`` backed by ``SyncCacheStore`` and ``SyncCacheRegion``."""
    store = SyncCacheStore(
        url,
        l1_url=l1_url,
        capacity=capacity,
        default_ttl=_normalize_ttl(expiration_time),
        key_prefix=key_prefix,
    )
    region = SyncCacheRegion(store, expiration_time=expiration_time)

    def _region_factory(config: CacheConfig) -> SyncCacheRegion:
        return region.configure(
            config.backend,
            expiration_time=config.expiration_time,
            arguments=config.arguments,
        )

    return CacheConfig(
        backend=url,
        expiration_time=expiration_time,
        arguments={
            "url": url,
            "l1_url": l1_url,
            "capacity": capacity,
            "key_prefix": key_prefix,
        },
        key_prefix=key_prefix,
        enabled=True,
        serializer=msgspec_model_serializer,
        deserializer=msgspec_model_deserializer,
        region_factory=_region_factory,
    )
