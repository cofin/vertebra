"""Shared template-method ADK session, memory, and artifact store bases for SQLSpec adapters."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, StoredEvent, StoredSession

from vertebra.storage import ArtifactReference
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory
    from sqlspec.extensions.adk.artifact import StoredArtifact
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore as _BaseAsyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore as _BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.converters import merge_scoped_state
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore as _BaseAsyncADKMemoryStore
    from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore as _BaseSyncADKMemoryStore
else:
    _memory_store_mod = ensure_module("sqlspec.extensions.adk.memory.store", package="google-adk", extra="adk")
    _artifact_store_mod = ensure_module("sqlspec.extensions.adk.artifact.store", package="google-adk", extra="adk")
    _converters_mod = ensure_module("sqlspec.extensions.adk.converters", package="google-adk", extra="adk")
    _artifact_types_mod = ensure_module("sqlspec.extensions.adk.artifact", package="google-adk", extra="adk")
    _memory_types_mod = ensure_module("sqlspec.extensions.adk.memory", package="google-adk", extra="adk")
    _BaseAsyncADKMemoryStore = _memory_store_mod.BaseAsyncADKMemoryStore
    _BaseSyncADKMemoryStore = _memory_store_mod.BaseSyncADKMemoryStore
    _BaseAsyncADKArtifactStore = _artifact_store_mod.BaseAsyncADKArtifactStore
    _BaseSyncADKArtifactStore = _artifact_store_mod.BaseSyncADKArtifactStore
    merge_scoped_state = _converters_mod.merge_scoped_state
    StoredArtifact = _artifact_types_mod.StoredArtifact
    StoredMemory = _memory_types_mod.StoredMemory

__all__ = (
    "StoredArtifact",
    "StoredMemory",
    "AdapterAsyncADKArtifactStore",
    "AdapterAsyncADKMemoryStore",
    "AdapterAsyncADKStore",
    "CloudAsyncADKArtifactStore",
    "CloudSyncADKArtifactStore",
    "AdapterSyncADKArtifactStore",
    "AdapterSyncADKMemoryStore",
    "AdapterSyncADKStore",
    "_BaseAsyncADKArtifactStore",
    "_BaseAsyncADKMemoryStore",
    "_BaseSyncADKArtifactStore",
    "_BaseSyncADKMemoryStore",
    "adk_settings",
    "exec_adk_async",
    "exec_adk_sync",
    "is_renewal_requested",
    "merge_scoped_state",
    "resolve_json_codecs",
    "rows_affected",
    "select_adk_async",
    "select_adk_sync",
    "select_one_adk_async",
    "select_one_adk_sync",
    "select_value_adk_async",
    "select_value_adk_sync",
    "validate_event_scope",
)


def adk_settings(config: Any) -> dict[str, Any]:
    """Extract ``extension_config['adk']`` as a mutable dictionary."""
    ext = getattr(config, "extension_config", {})
    if not isinstance(ext, Mapping):
        return {}
    adk = ext.get("adk", {})
    return dict(adk) if isinstance(adk, Mapping) else {}


def resolve_json_codecs(config: Any) -> tuple[Any, Any]:
    """Resolve optional JSON serializer and deserializer hooks from driver features."""
    features = getattr(config, "driver_features", {}) or {}
    return features.get("json_serializer"), features.get("json_deserializer")


def is_renewal_requested(renew_for: int | timedelta | None) -> bool:
    """Return whether a ``renew_for`` argument specifies a positive sliding-expiration renewal."""
    if renew_for is None:
        return False
    if isinstance(renew_for, timedelta):
        return renew_for.total_seconds() > 0
    return renew_for > 0


def validate_event_scope(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
    """Validate that an ADK event record matches the target session identifiers."""
    if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
        app_name,
        user_id,
        session_id,
    ):
        msg = "event_record scope does not match target session arguments."
        raise ValueError(msg)


def rows_affected(result: Any) -> int:
    """Extract normalized non-negative integer row count from a driver execution result."""
    return int(getattr(result, "rows_affected", 0) or 0)


def _should_unpack(params: Any, unpack: bool) -> bool:
    return unpack and isinstance(params, tuple | list)


def exec_adk_sync(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> Any:
    """Execute a synchronous ADK SQL statement with either unpacked positional or single mapping binds."""
    if params is None:
        return driver.execute(sql)
    if _should_unpack(params, unpack):
        return driver.execute(sql, *params)
    return driver.execute(sql, params)


async def exec_adk_async(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> Any:
    """Execute an asynchronous ADK SQL statement with either unpacked positional or single mapping binds."""
    if params is None:
        return await driver.execute(sql)
    if _should_unpack(params, unpack):
        return await driver.execute(sql, *params)
    return await driver.execute(sql, params)


def select_adk_sync(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> list[Any]:
    """Run a synchronous ADK ``select`` with unpacked positional or single mapping binds."""
    if params is None:
        return list(driver.select(sql))
    if _should_unpack(params, unpack):
        return list(driver.select(sql, *params))
    return list(driver.select(sql, params))


async def select_adk_async(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> list[Any]:
    """Run an asynchronous ADK ``select`` with unpacked positional or single mapping binds."""
    if params is None:
        return list(await driver.select(sql))
    if _should_unpack(params, unpack):
        return list(await driver.select(sql, *params))
    return list(await driver.select(sql, params))


def select_one_adk_sync(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> Any:
    """Run a synchronous ADK ``select_one_or_none`` with positional or mapping binds."""
    if params is None:
        return driver.select_one_or_none(sql)
    if _should_unpack(params, unpack):
        return driver.select_one_or_none(sql, *params)
    return driver.select_one_or_none(sql, params)


async def select_one_adk_async(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> Any:
    """Run an asynchronous ADK ``select_one_or_none`` with positional or mapping binds."""
    if params is None:
        return await driver.select_one_or_none(sql)
    if _should_unpack(params, unpack):
        return await driver.select_one_or_none(sql, *params)
    return await driver.select_one_or_none(sql, params)


def select_value_adk_sync(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> Any:
    """Run a synchronous ADK ``select_value_or_none`` with positional or mapping binds."""
    if params is None:
        return driver.select_value_or_none(sql)
    if _should_unpack(params, unpack):
        return driver.select_value_or_none(sql, *params)
    return driver.select_value_or_none(sql, params)


async def select_value_adk_async(driver: Any, sql: str, params: Any = None, *, unpack: bool = True) -> Any:
    """Run an asynchronous ADK ``select_value_or_none`` with positional or mapping binds."""
    if params is None:
        return await driver.select_value_or_none(sql)
    if _should_unpack(params, unpack):
        return await driver.select_value_or_none(sql, *params)
    return await driver.select_value_or_none(sql, params)


class _ADKAdapterHooksMixin:
    """Shared declarative driver and dialect hook interface for ADK store bases."""

    __slots__ = ()

    _commit_after_write: ClassVar[bool] = True
    _unpack_params: ClassVar[bool] = True
    _write_session_kwargs_map: ClassVar[dict[str, Any]] = {}
    _read_session_kwargs_map: ClassVar[dict[str, Any]] = {}

    if TYPE_CHECKING:
        _config: Any
        _json_deserializer: Any
        _json_serializer: Any

    def _is_table_missing(self, exc: Exception) -> bool:
        """Return whether ``exc`` indicates an absent ADK table for this dialect."""
        _ = exc
        return False

    def _prepare_driver_sync(self, driver: Any) -> None:
        """Apply optional per-session driver setup before synchronous statements run."""
        _ = driver

    async def _prepare_driver_async(self, driver: Any) -> None:
        """Apply optional per-session driver setup before asynchronous statements run."""
        _ = driver

    def _commit_sync(self, driver: Any) -> None:
        """Commit the active driver transaction when the dialect commits explicit writes."""
        if self._commit_after_write:
            driver.commit()

    async def _commit_async(self, driver: Any) -> None:
        """Asynchronously commit the active driver transaction when enabled for the dialect."""
        if self._commit_after_write:
            await driver.commit()

    def _row_field(self, row: Any, name: str, index: int = 0) -> Any:
        """Extract a scalar column value by mapping key or positional tuple index."""
        if isinstance(row, Mapping):
            if name in row:
                return row[name]
            lowered = name.lower()
            if lowered in row:
                return row[lowered]
            upper = name.upper()
            if upper in row:
                return row[upper]
            return None
        if isinstance(row, Sequence) and not isinstance(row, str | bytes | bytearray):
            return row[index] if index < len(row) else None
        return getattr(row, name, None)


class AdapterSyncADKArtifactStore(_ADKAdapterHooksMixin, _BaseSyncADKArtifactStore[Any]):
    """Shared synchronous template-method ADK artifact metadata store."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    if TYPE_CHECKING:
        _build_artifact_table_ddl: Any
        _insert_artifact_sql_and_params: Any
        _insert_artifact_ref_sql_and_params: Any
        _get_artifact_sql_and_params: Any
        _list_artifact_keys_sql_and_params: Any
        _list_artifact_versions_sql_and_params: Any
        _delete_artifact_sql_and_params: Any
        _get_next_version_sql_and_params: Any
        _delete_older_than_sqls_and_params: Any
        _decode_artifact_row: Any
        _decode_artifact_ref_row: Any

    _include_ref_cols_kwarg: ClassVar[bool] = True

    def __init__(self, config: Any, *, read_config: Any | None = None) -> None:
        """Initialize the synchronous ADK artifact store and resolve JSON codecs."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = resolve_json_codecs(config)

    def create_table(self) -> None:
        """Create the ADK artifact metadata table idempotently."""
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            driver.execute_script(self._build_artifact_table_ddl())
            self._commit_sync(driver)

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert or upsert an ADK artifact metadata record."""
        sql, params = self._insert_artifact_sql_and_params(record)
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
            self._commit_sync(driver)

    def insert_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        reference: ArtifactReference,
        *,
        session_id: str | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Persist a ``vertebra.storage.ArtifactReference`` record into the artifact metadata table."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
            self._commit_sync(driver)

    def _resolve_get_artifact_sql(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
        version: int | None,
        *,
        include_ref_cols: bool,
    ) -> tuple[str, Any]:
        if self._include_ref_cols_kwarg:
            return cast(
                "tuple[str, Any]",
                self._get_artifact_sql_and_params(
                    app_name, user_id, filename, session_id, version, include_ref_cols=include_ref_cols
                ),
            )
        return cast(
            "tuple[str, Any]",
            cast("Any", self._get_artifact_sql_and_params)(app_name, user_id, filename, session_id, version),
        )

    def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Fetch an ADK artifact metadata dictionary by key and optional version."""
        sql, params = self._resolve_get_artifact_sql(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                row = select_one_adk_sync(driver, sql, params, unpack=self._unpack_params)
            return cast("StoredArtifact", self._decode_artifact_row(row)) if row is not None else None
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Retrieve stored object metadata as a ``vertebra.storage.ArtifactReference``."""
        sql, params = self._resolve_get_artifact_sql(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                row = select_one_adk_sync(driver, sql, params, unpack=self._unpack_params)
            return cast("ArtifactReference", self._decode_artifact_ref_row(row)) if row is not None else None
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Return distinct sorted filenames stored under an app, user, and optional session scope."""
        sql, params = self._list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                rows = select_adk_sync(driver, sql, params, unpack=self._unpack_params)
            return [str(self._row_field(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Return ascending ``StoredArtifact`` versions for a filename."""
        sql, params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                rows = select_adk_sync(driver, sql, params, unpack=self._unpack_params)
            return [self._decode_artifact_row(r) for r in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    _transactional_artifact_delete: ClassVar[bool] = False
    _skip_empty_delete_artifacts_older_than: ClassVar[bool] = False

    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Delete all stored versions of an artifact and return removed records."""
        delete_sql, delete_params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        if self._transactional_artifact_delete:
            select_sql, select_params = self._list_artifact_versions_sql_and_params(
                app_name, user_id, filename, session_id
            )
            try:
                with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                    self._prepare_driver_sync(driver)
                    driver.begin()
                    try:
                        rows = select_adk_sync(driver, select_sql, select_params, unpack=self._unpack_params)
                        exec_adk_sync(driver, delete_sql, delete_params, unpack=self._unpack_params)
                        self._commit_sync(driver)
                    except BaseException:
                        with contextlib.suppress(Exception):
                            driver.rollback()
                        raise
                return [self._decode_artifact_row(r) for r in rows]
            except Exception as exc:
                if self._is_table_missing(exc):
                    return []
                raise
        existing = self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        try:
            with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                exec_adk_sync(driver, delete_sql, delete_params, unpack=self._unpack_params)
                self._commit_sync(driver)
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise
        return existing

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Return the next sequential artifact version (``MAX(version) + 1`` or ``0``)."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=True, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                val = select_value_adk_sync(driver, sql, params, unpack=self._unpack_params)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise
        return 0 if val is None else int(val) + 1

    def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Delete artifact rows older than ``before`` and return the removed records."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                if self._transactional_artifact_delete:
                    driver.begin()
                    try:
                        rows = select_adk_sync(driver, select_sql, params, unpack=self._unpack_params)
                        if rows or not self._skip_empty_delete_artifacts_older_than:
                            exec_adk_sync(driver, delete_sql, params, unpack=self._unpack_params)
                        self._commit_sync(driver)
                    except BaseException:
                        with contextlib.suppress(Exception):
                            driver.rollback()
                        raise
                else:
                    rows = select_adk_sync(driver, select_sql, params, unpack=self._unpack_params)
                    if rows or not self._skip_empty_delete_artifacts_older_than:
                        exec_adk_sync(driver, delete_sql, params, unpack=self._unpack_params)
                        self._commit_sync(driver)
            return [self._decode_artifact_row(r) for r in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise


class AdapterAsyncADKArtifactStore(_ADKAdapterHooksMixin, _BaseAsyncADKArtifactStore[Any]):
    """Shared asynchronous template-method ADK artifact metadata store."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _transactional_artifact_delete: ClassVar[bool] = False
    _skip_empty_delete_artifacts_older_than: ClassVar[bool] = False

    if TYPE_CHECKING:
        _build_artifact_table_ddl: Any
        _insert_artifact_sql_and_params: Any
        _insert_artifact_ref_sql_and_params: Any
        _get_artifact_sql_and_params: Any
        _list_artifact_keys_sql_and_params: Any
        _list_artifact_versions_sql_and_params: Any
        _delete_artifact_sql_and_params: Any
        _get_next_version_sql_and_params: Any
        _delete_older_than_sqls_and_params: Any
        _decode_artifact_row: Any
        _decode_artifact_ref_row: Any

    _include_ref_cols_kwarg: ClassVar[bool] = True

    def __init__(self, config: Any, *, read_config: Any | None = None) -> None:
        """Initialize the asynchronous ADK artifact store and resolve JSON codecs."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = resolve_json_codecs(config)

    async def create_table(self) -> None:
        """Asynchronously create the ADK artifact metadata table idempotently."""
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await driver.execute_script(self._build_artifact_table_ddl())
            await self._commit_async(driver)

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Asynchronously insert or upsert an ADK artifact metadata record."""
        sql, params = self._insert_artifact_sql_and_params(record)
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
            await self._commit_async(driver)

    async def insert_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        reference: ArtifactReference,
        *,
        session_id: str | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Asynchronously store a ``vertebra.storage.ArtifactReference`` record."""
        sql, params = self._insert_artifact_ref_sql_and_params(
            app_name, user_id, filename, reference, session_id, custom_metadata
        )
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
            await self._commit_async(driver)

    def _resolve_get_artifact_sql(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None,
        version: int | None,
        *,
        include_ref_cols: bool,
    ) -> tuple[str, Any]:
        if self._include_ref_cols_kwarg:
            return cast(
                "tuple[str, Any]",
                self._get_artifact_sql_and_params(
                    app_name, user_id, filename, session_id, version, include_ref_cols=include_ref_cols
                ),
            )
        return cast(
            "tuple[str, Any]",
            cast("Any", self._get_artifact_sql_and_params)(app_name, user_id, filename, session_id, version),
        )

    async def get_artifact(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> StoredArtifact | None:
        """Asynchronously fetch an ADK artifact record."""
        sql, params = self._resolve_get_artifact_sql(
            app_name, user_id, filename, session_id, version, include_ref_cols=False
        )
        try:
            async with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                row = await select_one_adk_async(driver, sql, params, unpack=self._unpack_params)
            return cast("StoredArtifact", self._decode_artifact_row(row)) if row is not None else None
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    async def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Asynchronously retrieve stored object metadata as an ``ArtifactReference``."""
        sql, params = self._resolve_get_artifact_sql(
            app_name, user_id, filename, session_id, version, include_ref_cols=True
        )
        try:
            async with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                row = await select_one_adk_async(driver, sql, params, unpack=self._unpack_params)
            return cast("ArtifactReference", self._decode_artifact_ref_row(row)) if row is not None else None
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        """Asynchronously list distinct artifact filenames."""
        sql, params = self._list_artifact_keys_sql_and_params(app_name, user_id, session_id)
        try:
            async with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                rows = await select_adk_async(driver, sql, params, unpack=self._unpack_params)
            return [str(self._row_field(r, "filename", 0)) for r in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously list stored ``StoredArtifact`` versions for an artifact."""
        sql, params = self._list_artifact_versions_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                rows = await select_adk_async(driver, sql, params, unpack=self._unpack_params)
            return [self._decode_artifact_row(r) for r in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    async def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[StoredArtifact]:
        """Asynchronously delete all stored versions of an artifact and return removed records."""
        delete_sql, delete_params = self._delete_artifact_sql_and_params(app_name, user_id, filename, session_id)
        if self._transactional_artifact_delete:
            select_sql, select_params = self._list_artifact_versions_sql_and_params(
                app_name, user_id, filename, session_id
            )
            try:
                async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                    await self._prepare_driver_async(driver)
                    await driver.begin()
                    try:
                        rows = await select_adk_async(driver, select_sql, select_params, unpack=self._unpack_params)
                        await exec_adk_async(driver, delete_sql, delete_params, unpack=self._unpack_params)
                        await self._commit_async(driver)
                    except BaseException:
                        with contextlib.suppress(Exception):
                            await driver.rollback()
                        raise
                return [self._decode_artifact_row(r) for r in rows]
            except Exception as exc:
                if self._is_table_missing(exc):
                    return []
                raise
        existing = await self.list_artifact_versions(app_name, user_id, filename, session_id=session_id)
        try:
            async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                await exec_adk_async(driver, delete_sql, delete_params, unpack=self._unpack_params)
                await self._commit_async(driver)
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise
        return existing

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Asynchronously compute the next sequential artifact version."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=True, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                val = await select_value_adk_async(driver, sql, params, unpack=self._unpack_params)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise
        return 0 if val is None else int(val) + 1

    async def delete_artifacts_older_than(self, before: datetime, app_name: str | None = None) -> list[StoredArtifact]:
        """Asynchronously delete artifact metadata rows older than ``before`` and return removed records."""
        select_sql, delete_sql, params = self._delete_older_than_sqls_and_params(before, app_name)
        try:
            async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                if self._transactional_artifact_delete:
                    await driver.begin()
                    try:
                        rows = await select_adk_async(driver, select_sql, params, unpack=self._unpack_params)
                        if rows or not self._skip_empty_delete_artifacts_older_than:
                            await exec_adk_async(driver, delete_sql, params, unpack=self._unpack_params)
                        await self._commit_async(driver)
                    except BaseException:
                        with contextlib.suppress(Exception):
                            await driver.rollback()
                        raise
                else:
                    rows = await select_adk_async(driver, select_sql, params, unpack=self._unpack_params)
                    if rows or not self._skip_empty_delete_artifacts_older_than:
                        await exec_adk_async(driver, delete_sql, params, unpack=self._unpack_params)
                        await self._commit_async(driver)
            return [self._decode_artifact_row(r) for r in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise


class AdapterSyncADKMemoryStore(_ADKAdapterHooksMixin, _BaseSyncADKMemoryStore[Any]):
    """Shared synchronous template-method ADK memory store."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    if TYPE_CHECKING:
        _build_memory_table_ddl: Any
        _build_insert_sql_and_params: Any
        _build_search_sql_and_params: Any
        _delete_by_session_sql_and_params: Any
        _delete_older_than_sql_and_params: Any
        _decode_memory_row: Any

    def __init__(self, config: Any, *, read_config: Any | None = None) -> None:
        """Initialize the synchronous ADK memory store and resolve JSON codecs."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = resolve_json_codecs(config)

    def _memory_table_ddl(self) -> str | list[str]:
        """Return DDL statements creating the memory table and companion indexes."""
        return cast("str | list[str]", self._build_memory_table_ddl())

    def create_tables(self) -> None:
        """Create the ADK memory table and indexes idempotently."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            ddl = self._memory_table_ddl()
            if isinstance(ddl, str):
                driver.execute_script(ddl)
            else:
                for stmt in ddl:
                    driver.execute(stmt)
            self._commit_sync(driver)

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Bulk-insert memory entries with dialect deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        inserted = 0
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            for entry in entries:
                sql, params = self._build_insert_sql_and_params(entry, owner_id)
                res = exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
                inserted += rows_affected(res)
            self._commit_sync(driver)
        return inserted

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Search memory entries matching a text query and/or vector embedding."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                rows = select_adk_sync(driver, sql, params, unpack=self._unpack_params)
            return [self._decode_memory_row(row) for row in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries associated with ``session_id`` owned by ``app_name`` and ``user_id``."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                res = exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
                self._commit_sync(driver)
                return rows_affected(res)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than ``days`` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                res = exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
                self._commit_sync(driver)
                return rows_affected(res)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise


class AdapterAsyncADKMemoryStore(_ADKAdapterHooksMixin, _BaseAsyncADKMemoryStore[Any]):
    """Shared asynchronous template-method ADK memory store."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    if TYPE_CHECKING:
        _build_memory_table_ddl: Any
        _build_insert_sql_and_params: Any
        _build_search_sql_and_params: Any
        _delete_by_session_sql_and_params: Any
        _delete_older_than_sql_and_params: Any
        _decode_memory_row: Any

    def __init__(self, config: Any, *, read_config: Any | None = None) -> None:
        """Initialize the asynchronous ADK memory store and resolve JSON codecs."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = resolve_json_codecs(config)

    async def _memory_table_ddl(self) -> str | list[str]:
        """Return DDL statements creating the memory table and companion indexes."""
        return cast("str | list[str]", self._build_memory_table_ddl())

    async def create_tables(self) -> None:
        """Asynchronously create the ADK memory table and indexes idempotently."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            ddl = await self._memory_table_ddl()
            if isinstance(ddl, str):
                await driver.execute_script(ddl)
            else:
                for stmt in ddl:
                    await driver.execute(stmt)
            await self._commit_async(driver)

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Asynchronously bulk-insert memory entries with dialect deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0
        inserted = 0
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            for entry in entries:
                sql, params = self._build_insert_sql_and_params(entry, owner_id)
                res = await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
                inserted += rows_affected(res)
            await self._commit_async(driver)
        return inserted

    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Asynchronously search memory entries matching a text query and/or vector embedding."""
        built = self._build_search_sql_and_params(query, app_name, user_id, limit, scope_filter, embedding)
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                rows = await select_adk_async(driver, sql, params, unpack=self._unpack_params)
            return [self._decode_memory_row(row) for row in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Asynchronously delete memory entries associated with ``session_id``."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_by_session_sql_and_params(session_id, app_name, user_id)
        try:
            async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                res = await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
                await self._commit_async(driver)
                return rows_affected(res)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Asynchronously delete memory entries older than ``days`` days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        sql, params = self._delete_older_than_sql_and_params(days, app_name, scope)
        try:
            async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                res = await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
                await self._commit_async(driver)
                return rows_affected(res)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise


class AdapterSyncADKStore(_ADKAdapterHooksMixin, BaseSyncADKStore[Any]):
    """Shared synchronous template-method ADK session and event store."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _reverse_limited_events: ClassVar[bool] = True

    if TYPE_CHECKING:
        _build_sessions_ddl: Any
        _build_events_ddl: Any
        _build_app_states_ddl: Any
        _build_user_states_ddl: Any
        _build_metadata_ddl: Any
        _decode_session_row: Any
        _decode_event_row: Any
        _decode_state_row: Any
        _update_session_state_sql_and_params: Any
        _list_sessions_sql_and_params: Any
        _get_events_sql_and_params: Any
        _delete_session_sqls_and_params: Any
        _append_event_sql_and_params: Any
        _delete_before_sql_and_params: Any
        _upsert_app_state_sql_and_params: Any
        _upsert_user_state_sql_and_params: Any
        _get_metadata_sql_and_params: Any
        _set_metadata_sql_and_params: Any

    def __init__(self, config: Any, *, read_config: Any | None = None) -> None:
        """Initialize the synchronous ADK session store and resolve JSON codecs."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = resolve_json_codecs(config)

    def _sessions_table_ddl(self) -> str:
        return cast("str", self._build_sessions_ddl())

    def _events_table_ddl(self) -> str:
        return cast("str", self._build_events_ddl())

    def _app_states_table_ddl(self) -> str:
        return cast("str", self._build_app_states_ddl())

    def _user_states_table_ddl(self) -> str:
        return cast("str", self._build_user_states_ddl())

    def _metadata_table_ddl(self) -> str:
        return cast("str", self._build_metadata_ddl())

    def create_tables(self) -> None:
        """Create ADK session, event, scoped state, and metadata tables idempotently."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())
            self._commit_sync(driver)

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Update JSON session state for a single ADK session."""
        sql, params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
            self._commit_sync(driver)

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions with validated sort order, limit, and offset."""
        built = self._list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                rows = select_adk_sync(driver, sql, params, unpack=self._unpack_params)
            return [self._decode_session_row(row) for row in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete an ADK session row (and cascading/explicit events per dialect)."""
        steps = self._delete_session_sqls_and_params(app_name, user_id, session_id)
        try:
            with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                for sql, params in steps:
                    exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
                self._commit_sync(driver)
        except Exception as exc:
            if self._is_table_missing(exc) and self._ignore_missing_on_delete_session:
                return
            raise

    _ignore_missing_on_delete_session: ClassVar[bool] = False

    def append_event(self, event_record: StoredEvent) -> None:
        """Append a single ADK event row after verifying the target session exists."""
        sql, params = self._append_event_sql_and_params(event_record)
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            res = exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
            if rows_affected(res) <= 0:
                with contextlib.suppress(Exception):
                    driver.rollback()
                msg = (
                    f"Session {event_record['session_id']} not found "
                    f"for app={event_record['app_name']!r}, user={event_record['user_id']!r}"
                )
                raise ValueError(msg)
            self._commit_sync(driver)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Retrieve ADK events for a session ordered chronologically."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                rows = select_adk_sync(driver, sql, params, unpack=self._unpack_params)
            if limit is not None and self._reverse_limited_events:
                rows = list(reversed(rows))
            return [self._decode_event_row(row) for row in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    def _delete_before_sync(self, table_name: str, column_name: str, before: datetime, app_name: str | None) -> int:
        sql, params = self._delete_before_sql_and_params(table_name, column_name, before, app_name)
        try:
            with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                res = exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
                self._commit_sync(driver)
                return rows_affected(res)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than ``before``, optionally restricted to one app."""
        return self._delete_before_sync(self._events_table, "timestamp", before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose ``update_time`` is older than ``updated_before``."""
        return self._delete_before_sync(self._user_state_table, "update_time", updated_before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions whose ``update_time`` is older than ``updated_before``."""
        return self._delete_before_sync(self._session_table, "update_time", updated_before, app_name)

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Upsert app-scoped ADK state."""
        sql, params = self._upsert_app_state_sql_and_params(app_name, state)
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
            self._commit_sync(driver)

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Upsert user-scoped ADK state."""
        sql, params = self._upsert_user_state_sql_and_params(app_name, user_id, state)
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
            self._commit_sync(driver)

    def get_metadata(self, key: str) -> str | None:
        """Return a metadata value from ``adk_internal_metadata`` or ``None`` if absent."""
        sql, params = self._get_metadata_sql_and_params(key)
        try:
            with self._config.provide_session(**self._read_session_kwargs_map) as driver:
                self._prepare_driver_sync(driver)
                row = select_one_adk_sync(driver, sql, params, unpack=self._unpack_params)
            if row is None:
                return None
            val = self._row_field(row, "value", 0)
            return str(val) if val is not None else None
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Upsert a metadata key-value entry."""
        sql, params = self._set_metadata_sql_and_params(key, value)
        with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            self._prepare_driver_sync(driver)
            exec_adk_sync(driver, sql, params, unpack=self._unpack_params)
            self._commit_sync(driver)


class AdapterAsyncADKStore(_ADKAdapterHooksMixin, BaseAsyncADKStore[Any]):
    """Shared asynchronous template-method ADK session and event store."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _reverse_limited_events: ClassVar[bool] = True

    if TYPE_CHECKING:
        _build_sessions_ddl: Any
        _build_events_ddl: Any
        _build_app_states_ddl: Any
        _build_user_states_ddl: Any
        _build_metadata_ddl: Any
        _decode_session_row: Any
        _decode_event_row: Any
        _decode_state_row: Any
        _update_session_state_sql_and_params: Any
        _list_sessions_sql_and_params: Any
        _get_events_sql_and_params: Any
        _delete_session_sqls_and_params: Any
        _append_event_sql_and_params: Any
        _delete_before_sql_and_params: Any
        _upsert_app_state_sql_and_params: Any
        _upsert_user_state_sql_and_params: Any
        _get_metadata_sql_and_params: Any
        _set_metadata_sql_and_params: Any

    def __init__(self, config: Any, *, read_config: Any | None = None) -> None:
        """Initialize the asynchronous ADK session store and resolve JSON codecs."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = resolve_json_codecs(config)

    async def _sessions_table_ddl(self) -> str:
        return cast("str", self._build_sessions_ddl())

    async def _events_table_ddl(self) -> str:
        return cast("str", self._build_events_ddl())

    async def _app_states_table_ddl(self) -> str:
        return cast("str", self._build_app_states_ddl())

    async def _user_states_table_ddl(self) -> str:
        return cast("str", self._build_user_states_ddl())

    async def _metadata_table_ddl(self) -> str:
        return cast("str", self._build_metadata_ddl())

    async def create_tables(self) -> None:
        """Asynchronously create ADK session, event, scoped state, and metadata tables idempotently."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await driver.execute_script(await self._sessions_table_ddl())
            await driver.execute_script(await self._events_table_ddl())
            await driver.execute_script(await self._app_states_table_ddl())
            await driver.execute_script(await self._user_states_table_ddl())
            await driver.execute_script(await self._metadata_table_ddl())
            await self._commit_async(driver)

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Asynchronously update JSON session state for a single ADK session."""
        sql, params = self._update_session_state_sql_and_params(app_name, user_id, session_id, state)
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
            await self._commit_async(driver)

    async def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """Asynchronously list ADK sessions with validated sort order, limit, and offset."""
        built = self._list_sessions_sql_and_params(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if built is None:
            return []
        sql, params = built
        try:
            async with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                rows = await select_adk_async(driver, sql, params, unpack=self._unpack_params)
            return [self._decode_session_row(row) for row in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Asynchronously delete an ADK session row (and cascading/explicit events per dialect)."""
        steps = self._delete_session_sqls_and_params(app_name, user_id, session_id)
        try:
            async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                for sql, params in steps:
                    await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
                await self._commit_async(driver)
        except Exception as exc:
            if self._is_table_missing(exc) and self._ignore_missing_on_delete_session:
                return
            raise

    _ignore_missing_on_delete_session: ClassVar[bool] = False

    async def append_event(self, event_record: StoredEvent) -> None:
        """Asynchronously append a single ADK event row after verifying the target session exists."""
        sql, params = self._append_event_sql_and_params(event_record)
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            res = await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
            if rows_affected(res) <= 0:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                msg = (
                    f"Session {event_record['session_id']} not found "
                    f"for app={event_record['app_name']!r}, user={event_record['user_id']!r}"
                )
                raise ValueError(msg)
            await self._commit_async(driver)

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Asynchronously retrieve ADK events for a session ordered chronologically."""
        if limit == 0:
            return []
        sql, params = self._get_events_sql_and_params(app_name, user_id, session_id, after_timestamp, limit)
        try:
            async with self._provide_replica_session(primary=False, **self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                rows = await select_adk_async(driver, sql, params, unpack=self._unpack_params)
            if limit is not None and self._reverse_limited_events:
                rows = list(reversed(rows))
            return [self._decode_event_row(row) for row in rows]
        except Exception as exc:
            if self._is_table_missing(exc):
                return []
            raise

    async def _delete_before_async(
        self, table_name: str, column_name: str, before: datetime, app_name: str | None
    ) -> int:
        sql, params = self._delete_before_sql_and_params(table_name, column_name, before, app_name)
        try:
            async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                res = await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
                await self._commit_async(driver)
                return rows_affected(res)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete events older than ``before``, optionally restricted to one app."""
        return await self._delete_before_async(self._events_table, "timestamp", before, app_name)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete user state rows whose ``update_time`` is older than ``updated_before``."""
        return await self._delete_before_async(self._user_state_table, "update_time", updated_before, app_name)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Asynchronously delete sessions whose ``update_time`` is older than ``updated_before``."""
        return await self._delete_before_async(self._session_table, "update_time", updated_before, app_name)

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert app-scoped ADK state."""
        sql, params = self._upsert_app_state_sql_and_params(app_name, state)
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
            await self._commit_async(driver)

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Asynchronously upsert user-scoped ADK state."""
        sql, params = self._upsert_user_state_sql_and_params(app_name, user_id, state)
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
            await self._commit_async(driver)

    async def get_metadata(self, key: str) -> str | None:
        """Asynchronously return a metadata value from ``adk_internal_metadata`` or ``None`` if absent."""
        sql, params = self._get_metadata_sql_and_params(key)
        try:
            async with self._config.provide_session(**self._read_session_kwargs_map) as driver:
                await self._prepare_driver_async(driver)
                row = await select_one_adk_async(driver, sql, params, unpack=self._unpack_params)
            if row is None:
                return None
            val = self._row_field(row, "value", 0)
            return str(val) if val is not None else None
        except Exception as exc:
            if self._is_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Asynchronously upsert a metadata key-value entry."""
        sql, params = self._set_metadata_sql_and_params(key, value)
        async with self._config.provide_session(**self._write_session_kwargs_map) as driver:
            await self._prepare_driver_async(driver)
            await exec_adk_async(driver, sql, params, unpack=self._unpack_params)
            await self._commit_async(driver)


class CloudSyncADKArtifactStore(AdapterSyncADKArtifactStore):
    """Synchronous cloud ADK artifact store combining SQL metadata and object-storage writes."""

    __slots__ = ("_storage_backend",)

    _commit_after_write: ClassVar[bool] = False
    _unpack_params: ClassVar[bool] = False
    _include_ref_cols_kwarg: ClassVar[bool] = False

    if TYPE_CHECKING:
        _storage_backend: Any | None
        _init_storage: Any
        _object_key: Any
        _stored_artifact_to_ref: Any

    def __init__(
        self,
        config: Any,
        *,
        storage_backend: Any | None = None,
        read_config: Any | None = None,
    ) -> None:
        """Initialize the synchronous cloud artifact store with optional storage backend."""
        super().__init__(config, read_config=read_config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> Any | None:
        """Configured SQLSpec/Vertebra object storage backend for blob payloads."""
        return self._storage_backend

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Compute the next zero-based integer artifact version."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            with self._provide_replica_session(primary=True, **self._read_session_kwargs_map) as driver:
                row = select_one_adk_sync(driver, sql, params, unpack=self._unpack_params)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise
        if isinstance(row, Mapping):
            raw = row.get("max_ver")
            if raw is None and row:
                raw = next(iter(row.values()))
        else:
            raw = row
        return 0 if raw is None else int(raw) + 1

    def save_artifact_object(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        data: bytes,
        *,
        session_id: str | None = None,
        mime_type: str = "application/octet-stream",
        custom_metadata: dict[str, Any] | None = None,
    ) -> ArtifactReference:
        """Write binary payload to ``storage_backend``, record SQL metadata, and return ``ArtifactReference``."""
        version = self.get_next_version(app_name, user_id, filename, session_id)
        key = self._object_key(app_name, user_id, filename, session_id, version)
        if self._storage_backend is not None:
            self._storage_backend.write_bytes_sync(key, data)
            base_uri = str(getattr(self._storage_backend, "store_uri", "") or "").rstrip("/")
            canonical_uri = f"{base_uri}/{key}" if base_uri else key
        else:
            canonical_uri = key
        record = cast(
            "StoredArtifact",
            {
                "app_name": app_name,
                "user_id": user_id,
                "session_id": session_id,
                "filename": filename,
                "version": version,
                "mime_type": mime_type,
                "canonical_uri": canonical_uri,
                "custom_metadata": custom_metadata,
                "created_at": datetime.now(UTC),
            },
        )
        self.insert_artifact(record)
        return cast("ArtifactReference", self._stored_artifact_to_ref(record, size=len(data)))

    def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Return a storage-neutral ``ArtifactReference`` for a stored artifact."""
        record = self.get_artifact(app_name, user_id, filename, session_id=session_id, version=version)
        return cast("ArtifactReference", self._stored_artifact_to_ref(dict(record))) if record is not None else None


class CloudAsyncADKArtifactStore(AdapterAsyncADKArtifactStore):
    """Asynchronous cloud ADK artifact store combining SQL metadata and object-storage writes."""

    __slots__ = ("_storage_backend",)

    _commit_after_write: ClassVar[bool] = False
    _unpack_params: ClassVar[bool] = False
    _include_ref_cols_kwarg: ClassVar[bool] = False

    if TYPE_CHECKING:
        _storage_backend: Any | None
        _init_storage: Any
        _object_key: Any
        _stored_artifact_to_ref: Any

    def __init__(
        self,
        config: Any,
        *,
        storage_backend: Any | None = None,
        read_config: Any | None = None,
    ) -> None:
        """Initialize the asynchronous cloud artifact store with optional storage backend."""
        super().__init__(config, read_config=read_config)
        self._storage_backend = self._init_storage(config, storage_backend)

    @property
    def storage_backend(self) -> Any | None:
        """Configured SQLSpec/Vertebra object storage backend for blob payloads."""
        return self._storage_backend

    async def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        """Asynchronously compute the next zero-based integer artifact version."""
        sql, params = self._get_next_version_sql_and_params(app_name, user_id, filename, session_id)
        try:
            async with self._provide_replica_session(primary=True, **self._read_session_kwargs_map) as driver:
                row = await select_one_adk_async(driver, sql, params, unpack=self._unpack_params)
        except Exception as exc:
            if self._is_table_missing(exc):
                return 0
            raise
        if isinstance(row, Mapping):
            raw = row.get("max_ver")
            if raw is None and row:
                raw = next(iter(row.values()))
        else:
            raw = row
        return 0 if raw is None else int(raw) + 1

    async def save_artifact_object(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        data: bytes,
        *,
        session_id: str | None = None,
        mime_type: str = "application/octet-stream",
        custom_metadata: dict[str, Any] | None = None,
    ) -> ArtifactReference:
        """Asynchronously write binary payload to ``storage_backend`` and record SQL metadata."""
        version = await self.get_next_version(app_name, user_id, filename, session_id)
        key = self._object_key(app_name, user_id, filename, session_id, version)
        if self._storage_backend is not None:
            await self._storage_backend.write_bytes_async(key, data)
            base_uri = str(getattr(self._storage_backend, "store_uri", "") or "").rstrip("/")
            canonical_uri = f"{base_uri}/{key}" if base_uri else key
        else:
            canonical_uri = key
        record = cast(
            "StoredArtifact",
            {
                "app_name": app_name,
                "user_id": user_id,
                "session_id": session_id,
                "filename": filename,
                "version": version,
                "mime_type": mime_type,
                "canonical_uri": canonical_uri,
                "custom_metadata": custom_metadata,
                "created_at": datetime.now(UTC),
            },
        )
        await self.insert_artifact(record)
        return cast("ArtifactReference", self._stored_artifact_to_ref(record, size=len(data)))

    async def get_artifact_reference(
        self,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactReference | None:
        """Asynchronously return a storage-neutral ``ArtifactReference`` for a stored artifact."""
        record = await self.get_artifact(app_name, user_id, filename, session_id=session_id, version=version)
        return cast("ArtifactReference", self._stored_artifact_to_ref(dict(record))) if record is not None else None
