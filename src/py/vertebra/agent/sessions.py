"""Pluggable AgentSessionStore implementations across memory, cache, SQL, and object storage.

This module provides the owner-scoped AgentSessionStore protocol and 5 production backends:
MemorySessionStore, CacheSessionStore, SQLSpecSessionStore, AlchemySessionStore, and
ObjectStoreSessionStore, serializing conversation history via vertebra.utils.serializers.
"""

import inspect
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

import anyio
import msgspec

from vertebra.agent.spec import AgentMessage
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes
from vertebra.utils.text import validate_identifier

ANONYMOUS_OWNER = "anonymous"


@runtime_checkable
class AgentSessionStore(Protocol):
    """Protocol for owner-scoped agent session persistence."""

    async def load(self, owner: str, session_id: str) -> list[AgentMessage] | None:
        """Load conversation history for an owner and session identifier."""
        ...

    async def save(self, owner: str, session_id: str, messages: Sequence[AgentMessage]) -> None:
        """Persist conversation history for an owner and session identifier."""
        ...

    async def delete(self, owner: str, session_id: str) -> None:
        """Delete conversation history for an owner and session identifier."""
        ...

    def lock(self, owner: str, session_id: str) -> AbstractAsyncContextManager[None]:
        """Return an async context manager acquiring a mutual exclusion lock on session."""
        ...


class MemorySessionStore:
    """Process-local monotonic-TTL LRU OrderedDict session store with per-session locking."""

    def __init__(self, max_sessions: int = 1024, ttl_seconds: float = 3600.0) -> None:
        self.max_sessions = max_sessions
        self.ttl_seconds = ttl_seconds
        self._sessions: OrderedDict[tuple[str, str], tuple[float, bytes]] = OrderedDict()
        self._locks = _LockManager()
        self._store_lock = anyio.Lock()

    def lock(self, owner: str, session_id: str) -> AbstractAsyncContextManager[None]:
        """Acquire a per-session lock for mutual exclusion."""
        return self._locks.acquire(owner, session_id)

    async def load(self, owner: str, session_id: str) -> list[AgentMessage] | None:
        """Load and deserialize conversation history for owner and session."""
        key = (owner, session_id)
        now = time.monotonic()
        async with self._store_lock:
            entry = self._sessions.get(key)
            if entry is None:
                return None
            expiry, data = entry
            if now >= expiry:
                self._sessions.pop(key, None)
                return None
            self._sessions.move_to_end(key)
        raw_list = decode_json(data)
        return [msgspec.convert(item, AgentMessage) for item in raw_list]

    async def save(self, owner: str, session_id: str, messages: Sequence[AgentMessage]) -> None:
        """Serialize and save conversation history with monotonic TTL."""
        key = (owner, session_id)
        now = time.monotonic()
        expiry = now + self.ttl_seconds
        data = encode_json_bytes(messages)
        async with self._store_lock:
            if key in self._sessions:
                self._sessions.pop(key)
            elif len(self._sessions) >= self.max_sessions:
                self._sessions.popitem(last=False)
            self._sessions[key] = (expiry, data)

    async def delete(self, owner: str, session_id: str) -> None:
        """Remove a session from memory."""
        key = (owner, session_id)
        async with self._store_lock:
            self._sessions.pop(key, None)


class CacheSessionStore:
    """Session store backed by MemoryStore or any Litestar Store implementation."""

    def __init__(
        self,
        store: Any,
        *,
        ttl_seconds: int = 3600,
        key_prefix: str = "vertebra:agent:session",
    ) -> None:
        self.store = store
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix
        self._locks = _LockManager()

    def lock(self, owner: str, session_id: str) -> AbstractAsyncContextManager[None]:
        """Acquire a per-session lock for mutual exclusion."""
        return self._locks.acquire(owner, session_id)

    async def load(self, owner: str, session_id: str) -> list[AgentMessage] | None:
        """Load conversation history from the backing cache store."""
        key = self._format_key(owner, session_id)
        val = await self.store.get(key)
        if val is None:
            return None
        raw_list = decode_json(val)
        return [msgspec.convert(item, AgentMessage) for item in raw_list]

    async def save(self, owner: str, session_id: str, messages: Sequence[AgentMessage]) -> None:
        """Store serialized conversation history with configured expiration."""
        key = self._format_key(owner, session_id)
        data = encode_json_bytes(messages)
        await self.store.set(key, data, expires_in=self.ttl_seconds)

    async def delete(self, owner: str, session_id: str) -> None:
        """Delete session key from the cache store."""
        key = self._format_key(owner, session_id)
        await self.store.delete(key)

    def _format_key(self, owner: str, session_id: str) -> str:
        return f"{self.key_prefix}:{owner}:{session_id}"


class SQLSpecSessionStore:
    """Session store persisting conversation history via parameterized SQL statements or ADK store."""

    def __init__(
        self,
        driver: Any = None,
        *,
        table_name: str = "vertebra_agent_sessions",
        adk_store: Any = None,
    ) -> None:
        self.driver = driver
        self.table_name = validate_identifier(table_name, "table_name")
        self.adk_store = adk_store
        self._locks = _LockManager()

    def lock(self, owner: str, session_id: str) -> AbstractAsyncContextManager[None]:
        """Acquire a per-session lock for mutual exclusion."""
        return self._locks.acquire(owner, session_id)

    async def load(self, owner: str, session_id: str) -> list[AgentMessage] | None:
        """Load session messages from the database table or ADK store."""
        if self.adk_store is not None:
            fn = getattr(self.adk_store, "get_session", None) or getattr(self.adk_store, "load", None)
            if callable(fn):
                try:
                    res = fn(session_id)
                except TypeError:
                    res = fn(owner, session_id)
                if inspect.isawaitable(res):
                    res = await res
                if res is None:
                    return None
                if isinstance(res, list):
                    return res
                messages = getattr(res, "messages", None) or getattr(res, "events", None)
                if messages is not None:
                    return [
                        msgspec.convert(item, AgentMessage) if not isinstance(item, AgentMessage) else item
                        for item in messages
                    ]
                return None

        query = f"SELECT messages FROM {self.table_name} WHERE owner = :owner AND session_id = :session_id"
        params = {"owner": owner, "session_id": session_id}
        row = await self._fetch_one(query, params)
        if row is None:
            return None
        data = row[0] if isinstance(row, (tuple, list)) else row.get("messages")
        if data is None:
            return None
        raw_list = decode_json(data)
        return [msgspec.convert(item, AgentMessage) for item in raw_list]

    async def save(self, owner: str, session_id: str, messages: Sequence[AgentMessage]) -> None:
        """Upsert conversation history in the database table or ADK store."""
        if self.adk_store is not None:
            fn = getattr(self.adk_store, "save_session", None) or getattr(self.adk_store, "save", None)
            if callable(fn):
                try:
                    res = fn(session_id, messages)
                except TypeError:
                    res = fn(owner, session_id, messages)
                if inspect.isawaitable(res):
                    await res
                return

        serialized = encode_json_bytes(messages).decode("utf-8")
        now_iso = datetime.now(UTC).isoformat()

        update_query = (
            f"UPDATE {self.table_name} SET messages = :messages, updated_at = :updated_at "
            f"WHERE owner = :owner AND session_id = :session_id"
        )
        params = {
            "owner": owner,
            "session_id": session_id,
            "messages": serialized,
            "updated_at": now_iso,
        }
        res = await self._execute(update_query, params)
        rows_affected = getattr(res, "rowcount", None)

        if rows_affected == 0 or rows_affected is None:
            insert_query = (
                f"INSERT INTO {self.table_name} (owner, session_id, messages, updated_at) "
                f"VALUES (:owner, :session_id, :messages, :updated_at)"
            )
            try:
                await self._execute(insert_query, params)
            except Exception:
                await self._execute(update_query, params)

    async def delete(self, owner: str, session_id: str) -> None:
        """Delete session row from the database table or ADK store."""
        if self.adk_store is not None:
            fn = getattr(self.adk_store, "delete_session", None) or getattr(self.adk_store, "delete", None)
            if callable(fn):
                try:
                    res = fn(session_id)
                except TypeError:
                    res = fn(owner, session_id)
                if inspect.isawaitable(res):
                    await res
                return

        query = f"DELETE FROM {self.table_name} WHERE owner = :owner AND session_id = :session_id"
        params = {"owner": owner, "session_id": session_id}
        await self._execute(query, params)

    async def _execute(self, query: str, params: dict[str, Any] | tuple[Any, ...]) -> Any:
        """Execute query across async or sync drivers."""
        if self.driver is None:
            return None
        if hasattr(self.driver, "execute"):
            fn = self.driver.execute
            if inspect.iscoroutinefunction(fn):
                return await fn(query, params)
            outcome = fn(query, params)
            if inspect.isawaitable(outcome):
                return await outcome
            return outcome
        return await anyio.to_thread.run_sync(self.driver, query, params)

    async def _fetch_one(self, query: str, params: dict[str, Any] | tuple[Any, ...]) -> Any:
        """Fetch single result row across async or sync drivers."""
        if self.driver is None:
            return None
        if hasattr(self.driver, "fetch_one"):
            fn = self.driver.fetch_one
            if inspect.iscoroutinefunction(fn):
                return await fn(query, params)
            outcome = fn(query, params)
            if inspect.isawaitable(outcome):
                return await outcome
            return outcome
        cursor = await self._execute(query, params)
        if hasattr(cursor, "fetchone"):
            fn = cursor.fetchone
            if inspect.iscoroutinefunction(fn):
                return await fn()
            return fn()
        return None


class AlchemySessionStore:
    """Session store persisting session rows via an SQLAlchemy async session factory."""

    def __init__(
        self,
        session_factory: Callable[[], Any],
        *,
        table_name: str = "vertebra_agent_sessions",
    ) -> None:
        self.session_factory = session_factory
        self.table_name = table_name
        self._locks = _LockManager()

    def lock(self, owner: str, session_id: str) -> AbstractAsyncContextManager[None]:
        """Acquire a per-session lock for mutual exclusion."""
        return self._locks.acquire(owner, session_id)

    async def load(self, owner: str, session_id: str) -> list[AgentMessage] | None:
        """Load conversation messages using SQLAlchemy async session."""
        query = _sql_text(f"SELECT messages FROM {self.table_name} WHERE owner = :owner AND session_id = :session_id")
        params = {"owner": owner, "session_id": session_id}

        async with self.session_factory() as session:
            result = await session.execute(query, params)
            row = result.fetchone()
            if row is None:
                return None
            data = row[0]
            if data is None:
                return None
            raw_list = decode_json(data)
            return [msgspec.convert(item, AgentMessage) for item in raw_list]

    async def save(self, owner: str, session_id: str, messages: Sequence[AgentMessage]) -> None:
        """Upsert conversation messages using SQLAlchemy async session."""
        serialized = encode_json_bytes(messages).decode("utf-8")
        now_iso = datetime.now(UTC).isoformat()
        params = {
            "owner": owner,
            "session_id": session_id,
            "messages": serialized,
            "updated_at": now_iso,
        }

        update_query = _sql_text(
            f"UPDATE {self.table_name} SET messages = :messages, updated_at = :updated_at "
            f"WHERE owner = :owner AND session_id = :session_id"
        )
        insert_query = _sql_text(
            f"INSERT INTO {self.table_name} (owner, session_id, messages, updated_at) "
            f"VALUES (:owner, :session_id, :messages, :updated_at)"
        )

        async with self.session_factory() as session:
            result = await session.execute(update_query, params)
            if result.rowcount == 0:
                try:
                    await session.execute(insert_query, params)
                except Exception:
                    await session.execute(update_query, params)
            await session.commit()

    async def delete(self, owner: str, session_id: str) -> None:
        """Delete conversation row using SQLAlchemy async session."""
        query = _sql_text(f"DELETE FROM {self.table_name} WHERE owner = :owner AND session_id = :session_id")
        params = {"owner": owner, "session_id": session_id}

        async with self.session_factory() as session:
            await session.execute(query, params)
            await session.commit()


class ObjectStoreSessionStore:
    """Session store persisting session objects in S3, GCS, or Vertebra storage."""

    def __init__(
        self,
        bucket_store: Any,
        *,
        prefix: str = "agent-sessions/",
    ) -> None:
        self.bucket_store = bucket_store
        self.prefix = prefix
        self._locks = _LockManager()

    def lock(self, owner: str, session_id: str) -> AbstractAsyncContextManager[None]:
        """Acquire a per-session lock for mutual exclusion."""
        return self._locks.acquire(owner, session_id)

    async def load(self, owner: str, session_id: str) -> list[AgentMessage] | None:
        """Load session object from bucket store."""
        path = self._format_path(owner, session_id)
        getter = getattr(self.bucket_store, "get", None) or getattr(self.bucket_store, "read", None)
        if getter is None:
            msg = "Bucket store must provide 'get' or 'read' method"
            raise AttributeError(msg)

        try:
            val = getter(path)
            if inspect.isawaitable(val):
                val = await val
            if val is None:
                return None
            raw_list = decode_json(val)
            return [msgspec.convert(item, AgentMessage) for item in raw_list]
        except Exception:
            return None

    async def save(self, owner: str, session_id: str, messages: Sequence[AgentMessage]) -> None:
        """Write session object to bucket store."""
        path = self._format_path(owner, session_id)
        data = encode_json_bytes(messages)
        putter = (
            getattr(self.bucket_store, "put", None)
            or getattr(self.bucket_store, "set", None)
            or getattr(self.bucket_store, "write", None)
        )
        if putter is None:
            msg = "Bucket store must provide 'put', 'set', or 'write' method"
            raise AttributeError(msg)

        outcome = putter(path, data)
        if inspect.isawaitable(outcome):
            await outcome

    async def delete(self, owner: str, session_id: str) -> None:
        """Delete session object from bucket store."""
        path = self._format_path(owner, session_id)
        deleter = getattr(self.bucket_store, "delete", None) or getattr(self.bucket_store, "remove", None)
        if deleter is None:
            return
        outcome = deleter(path)
        if inspect.isawaitable(outcome):
            await outcome

    def _format_path(self, owner: str, session_id: str) -> str:
        return f"{self.prefix}{owner}/{session_id}.json"


class _LockManager:
    """Manages per-session async mutual exclusion locks with refcounted idle eviction."""

    def __init__(self) -> None:
        self._locks: dict[tuple[str, str], anyio.Lock] = {}
        self._refcounts: dict[tuple[str, str], int] = {}
        self._meta_lock = anyio.Lock()

    @asynccontextmanager
    async def acquire(self, owner: str, session_id: str) -> AsyncIterator[None]:
        """Acquire the lock for a specific (owner, session_id) pair and evict when idle."""
        key = (owner, session_id)
        async with self._meta_lock:
            target_lock = self._locks.get(key)
            if target_lock is None:
                target_lock = anyio.Lock()
                self._locks[key] = target_lock
            self._refcounts[key] = self._refcounts.get(key, 0) + 1
        try:
            async with target_lock:
                yield
        finally:
            with anyio.CancelScope(shield=True):
                async with self._meta_lock:
                    remaining = self._refcounts.get(key, 0) - 1
                    if remaining <= 0:
                        self._refcounts.pop(key, None)
                        self._locks.pop(key, None)
                    else:
                        self._refcounts[key] = remaining


def _sql_text(statement: str) -> Any:
    """Safely build a SQLAlchemy text clause if sqlalchemy is installed, else return statement."""
    sa = import_optional("sqlalchemy")
    if sa is not None and hasattr(sa, "text"):
        return sa.text(statement)
    return statement


__all__ = [
    "ANONYMOUS_OWNER",
    "AgentSessionStore",
    "AlchemySessionStore",
    "CacheSessionStore",
    "MemorySessionStore",
    "ObjectStoreSessionStore",
    "SQLSpecSessionStore",
]
