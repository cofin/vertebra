"""Owner-scoped in-memory session store for legacy MCP protocol revisions.

This module provides MCPSession, SessionLimitExceededError, and MCPSessionStore
managing stateful legacy Streamable HTTP sessions, TTL expiration, capacity
bounds, and reverse resource subscription indexes.
"""

import secrets
import time
from collections.abc import Callable, Mapping
from typing import Any

import anyio
import msgspec

from vertebra.agent.mcp.models import McpLogLevel
from vertebra.typing import VertebraError

__all__ = [
    "MCPSession",
    "MCPSessionStore",
    "SessionLimitExceededError",
]


class SessionLimitExceededError(VertebraError):
    """Raised when creating a new MCP session would exceed max_sessions capacity."""


class MCPSession(msgspec.Struct, frozen=True, kw_only=True):
    """Immutable snapshot of a stateful legacy MCP protocol session."""

    session_id: str
    """Opaque session identifier exchanged via the `Mcp-Session-Id` header."""
    owner_id: str | None = None
    """Canonical caller ownership key isolating session visibility across principals."""
    protocol_version: str
    """Negotiated legacy MCP protocol version bound to this session."""
    client_capabilities: dict[str, Any] = msgspec.field(default_factory=dict)
    """Client capability dictionary advertised during `initialize`."""
    client_info: dict[str, Any] = msgspec.field(default_factory=dict)
    """Client implementation descriptor advertised during `initialize`."""
    log_level: McpLogLevel = "info"
    """Active minimum log severity configured via `logging/setLevel`."""
    subscriptions: frozenset[str] = msgspec.field(default_factory=frozenset)
    """Resource URIs currently subscribed to by this session."""
    created_at_unix_ms: int
    """Unix epoch creation timestamp in milliseconds."""
    expires_at_unix_ms: int
    """Unix epoch expiration timestamp in milliseconds."""


class MCPSessionStore:
    """Thread- and task-safe owner-scoped session store with TTL and capacity limits."""

    def __init__(
        self,
        ttl_seconds: float = 3600.0,
        max_sessions: int = 10_000,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if max_sessions < 0:
            raise ValueError("max_sessions must be non-negative")
        self.ttl_seconds = float(ttl_seconds)
        self.max_sessions = max_sessions
        self._clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self._lock = anyio.Lock()
        self._sessions: dict[str, tuple[MCPSession, float]] = {}
        self._uri_subscribers: dict[str, set[str]] = {}

    async def create(
        self,
        *,
        owner_id: str | None,
        protocol_version: str,
        client_capabilities: Mapping[str, Any] | None = None,
        client_info: Mapping[str, Any] | None = None,
        log_level: McpLogLevel = "info",
    ) -> MCPSession:
        """Create and persist a new owner-scoped session if within capacity."""
        async with self._lock:
            now_mono = self._clock()
            self._prune_expired(now_mono)
            if len(self._sessions) >= self.max_sessions:
                raise SessionLimitExceededError(f"Maximum active MCP sessions ({self.max_sessions}) exceeded")
            session_id = f"mcp_{secrets.token_hex(16)}"
            now_unix_ms = int(time.time() * 1000)
            ttl_ms = int(self.ttl_seconds * 1000)
            session = MCPSession(
                session_id=session_id,
                owner_id=owner_id,
                protocol_version=protocol_version,
                client_capabilities=dict(client_capabilities) if client_capabilities is not None else {},
                client_info=dict(client_info) if client_info is not None else {},
                log_level=log_level,
                subscriptions=frozenset(),
                created_at_unix_ms=now_unix_ms,
                expires_at_unix_ms=now_unix_ms + ttl_ms,
            )
            self._sessions[session_id] = (session, now_mono + self.ttl_seconds)
            return session

    async def get(self, session_id: str, owner_id: str | None) -> MCPSession | None:
        """Retrieve an active session if it exists and matches owner_id exactly."""
        async with self._lock:
            self._prune_expired(self._clock())
            entry = self._sessions.get(session_id)
            if entry is None:
                return None
            session, _ = entry
            if session.owner_id != owner_id:
                return None
            return session

    async def update(
        self,
        session_id: str,
        owner_id: str | None,
        *,
        log_level: McpLogLevel | None = None,
        subscriptions: frozenset[str] | None = None,
    ) -> MCPSession | None:
        """Update mutable session attributes while preserving owner isolation."""
        async with self._lock:
            self._prune_expired(self._clock())
            entry = self._sessions.get(session_id)
            if entry is None:
                return None
            current, deadline = entry
            if current.owner_id != owner_id:
                return None
            next_level = log_level if log_level is not None else current.log_level
            next_subs = subscriptions if subscriptions is not None else current.subscriptions
            if subscriptions is not None and subscriptions != current.subscriptions:
                self._remove_subscriptions_from_index(session_id, current.subscriptions - subscriptions)
                self._add_subscriptions_to_index(session_id, subscriptions - current.subscriptions)
            updated = msgspec.structs.replace(
                current,
                log_level=next_level,
                subscriptions=next_subs,
            )
            self._sessions[session_id] = (updated, deadline)
            return updated

    async def delete(self, session_id: str, owner_id: str | None) -> MCPSession | None:
        """Delete an active session and clear its subscription index entries."""
        async with self._lock:
            self._prune_expired(self._clock())
            entry = self._sessions.get(session_id)
            if entry is None:
                return None
            session, _ = entry
            if session.owner_id != owner_id:
                return None
            self._sessions.pop(session_id, None)
            self._remove_subscriptions_from_index(session_id, session.subscriptions)
            return session

    async def subscribed_session_ids(self, uri: str) -> tuple[str, ...]:
        """Return active session IDs currently subscribed to the specified resource URI."""
        async with self._lock:
            self._prune_expired(self._clock())
            subscribers = self._uri_subscribers.get(uri)
            if not subscribers:
                return ()
            return tuple(sorted(subscribers))

    async def active_session_ids(self) -> tuple[str, ...]:
        """Return all non-expired session IDs currently tracked by the store."""
        async with self._lock:
            self._prune_expired(self._clock())
            return tuple(self._sessions.keys())

    async def active_sessions(self) -> tuple[MCPSession, ...]:
        """Return all non-expired session snapshots currently tracked by the store."""
        async with self._lock:
            self._prune_expired(self._clock())
            return tuple(session for session, _ in self._sessions.values())

    def _remove_subscriptions_from_index(self, session_id: str, subscriptions: frozenset[str]) -> None:
        for uri in subscriptions:
            subscribers = self._uri_subscribers.get(uri)
            if subscribers is not None:
                subscribers.discard(session_id)
                if not subscribers:
                    self._uri_subscribers.pop(uri, None)

    def _add_subscriptions_to_index(self, session_id: str, subscriptions: frozenset[str]) -> None:
        for uri in subscriptions:
            self._uri_subscribers.setdefault(uri, set()).add(session_id)

    def _prune_expired(self, now_mono: float) -> None:
        expired_ids = [session_id for session_id, (_, deadline) in self._sessions.items() if now_mono >= deadline]
        for session_id in expired_ids:
            entry = self._sessions.pop(session_id, None)
            if entry is not None:
                self._remove_subscriptions_from_index(session_id, entry[0].subscriptions)
