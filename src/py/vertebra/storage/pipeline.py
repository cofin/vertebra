"""Transactional cloud object upload staging and orphan cleanup tracker."""

from __future__ import annotations

import contextlib
import importlib
from collections.abc import Buffer
from typing import Any

import msgspec

from vertebra.storage._store import ObjectStore, PutResult

__all__ = (
    "StagedUpload",
    "UploadTracker",
)


class StagedUpload(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pending object write staged within a database transaction boundary."""

    key: str
    data: bytes
    store: ObjectStore


class UploadTracker:
    """Tracks staged uploads and deletes orphaned cloud objects on database transaction rollback."""

    __slots__ = ("_pending_deletes", "_staged_uploads", "_uploaded_keys", "default_store")

    def __init__(self, default_store: ObjectStore | None = None) -> None:
        self.default_store = default_store
        self._staged_uploads: list[StagedUpload] = []
        self._uploaded_keys: list[tuple[ObjectStore, str]] = []
        self._pending_deletes: list[tuple[ObjectStore, str]] = []

    @property
    def staged_count(self) -> int:
        """Return the number of uncommitted staged uploads."""
        return len(self._staged_uploads)

    @property
    def uploaded_count(self) -> int:
        """Return the number of flushed uploads awaiting commit or rollback."""
        return len(self._uploaded_keys)

    def stage_upload(
        self,
        key: str,
        data: Buffer,
        *,
        store: ObjectStore | None = None,
    ) -> None:
        """Queue an object upload to be written during before_flush."""
        target_store = store or self.default_store
        if target_store is None:
            msg = "An ObjectStore must be provided to stage_upload or UploadTracker"
            raise ValueError(msg)
        self._staged_uploads.append(StagedUpload(key=key, data=bytes(memoryview(data)), store=target_store))

    def stage_delete(
        self,
        key: str,
        *,
        store: ObjectStore | None = None,
    ) -> None:
        """Queue an object key to be deleted only after the database transaction commits."""
        target_store = store or self.default_store
        if target_store is None:
            msg = "An ObjectStore must be provided to stage_delete or UploadTracker"
            raise ValueError(msg)
        self._pending_deletes.append((target_store, key))

    def before_flush(self, *_args: Any) -> list[PutResult]:
        """Flush all staged uploads to their target ObjectStore before database commit."""
        results: list[PutResult] = []
        while self._staged_uploads:
            item = self._staged_uploads.pop(0)
            res = item.store.put(item.key, item.data)
            self._uploaded_keys.append((item.store, item.key))
            results.append(res)
        return results

    async def before_flush_async(self, *_args: Any) -> list[PutResult]:
        """Asynchronously flush all staged uploads to their target ObjectStore."""
        results: list[PutResult] = []
        while self._staged_uploads:
            item = self._staged_uploads.pop(0)
            res = await item.store.put_async(item.key, item.data)
            self._uploaded_keys.append((item.store, item.key))
            results.append(res)
        return results

    def after_commit(self, *_args: Any) -> None:
        """Finalize committed uploads and execute any deferred object deletions."""
        self._uploaded_keys.clear()
        while self._pending_deletes:
            store, key = self._pending_deletes.pop(0)
            with contextlib.suppress(Exception):
                store.delete(key)

    async def after_commit_async(self, *_args: Any) -> None:
        """Asynchronously finalize committed uploads and execute deferred deletions."""
        self._uploaded_keys.clear()
        while self._pending_deletes:
            store, key = self._pending_deletes.pop(0)
            with contextlib.suppress(Exception):
                await store.delete_async(key)

    def after_rollback(self, *_args: Any) -> int:
        """Delete any objects uploaded during the rolled-back transaction and clear staged state."""
        self._staged_uploads.clear()
        self._pending_deletes.clear()
        reclaimed = 0
        while self._uploaded_keys:
            store, key = self._uploaded_keys.pop()
            with contextlib.suppress(Exception):
                store.delete(key)
                reclaimed += 1
        return reclaimed

    async def after_rollback_async(self, *_args: Any) -> int:
        """Asynchronously delete any objects uploaded during the rolled-back transaction."""
        self._staged_uploads.clear()
        self._pending_deletes.clear()
        reclaimed = 0
        while self._uploaded_keys:
            store, key = self._uploaded_keys.pop()
            with contextlib.suppress(Exception):
                await store.delete_async(key)
                reclaimed += 1
        return reclaimed

    def attach_sqlalchemy_session(self, session_or_factory: Any) -> None:
        """Register SQLAlchemy before_flush, after_commit, and after_rollback listeners when SQLAlchemy is present."""
        try:
            event_mod = importlib.import_module("sqlalchemy.event")
            listen = event_mod.listen
            listen(session_or_factory, "before_flush", self.before_flush)
            listen(session_or_factory, "after_commit", self.after_commit)
            listen(session_or_factory, "after_rollback", self.after_rollback)
        except ImportError:
            return

    async def __aenter__(self) -> UploadTracker:
        """Enter an async transactional upload tracking context."""
        return self

    async def __aexit__(self, exc_type: object, _exc_val: object, _exc_tb: object) -> None:
        """Automatically roll back any flushed or staged uploads if an unhandled exception occurred."""
        if exc_type is not None:
            await self.after_rollback_async()
