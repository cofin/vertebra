"""Server-Sent Events (SSE) frame codec, bounded replay buffer, and stream broker.

This module provides SSE frame formatting compliant with the W3C EventSource
specification, bounded replay buffers supporting Last-Event-ID reconnection,
and multi-subscriber stream brokers for streaming agentic responses.
"""

import contextlib
from collections import deque
from collections.abc import AsyncIterator, Sequence

import anyio
import msgspec
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from vertebra.cache import SyncCacheStore
from vertebra.typing import VertebraError
from vertebra.utils.serializers import decode_json, encode_json_bytes


class StreamLimitExceededError(VertebraError):
    """Raised when opening an SSE stream would exceed max_streams_per_owner."""


class SseFrame(msgspec.Struct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """Server-Sent Event wire frame."""

    id: str = ""
    event: str = "message"
    data: str = ""
    retry_ms: int | None = None

    def encode(self) -> bytes:
        """Encode the frame to raw UTF-8 bytes for HTTP text/event-stream transmission."""
        chunks: list[str] = []
        if self.id:
            chunks.append(f"id: {self.id}\n")
        if self.event and self.event != "message":
            chunks.append(f"event: {self.event}\n")
        if self.retry_ms is not None:
            chunks.append(f"retry: {self.retry_ms}\n")

        for line in self.data.splitlines():
            chunks.append(f"data: {line}\n")
        if not self.data:
            chunks.append("data:\n")

        chunks.append("\n")
        return "".join(chunks).encode("utf-8")


class SseReplayBuffer:
    """Bounded ring buffer recording recent SSE frames for client reconnection replay."""

    def __init__(self, capacity: int = 1024) -> None:
        self.capacity = capacity
        self._buffer: deque[SseFrame] = deque(maxlen=capacity)

    def append(self, frame: SseFrame) -> None:
        """Append a frame to the replay ring buffer."""
        self._buffer.append(frame)

    def replay_since(self, last_event_id: str) -> list[SseFrame]:
        """Return frames recorded strictly after the given last_event_id."""
        frames = list(self._buffer)
        if not last_event_id:
            return frames

        found_index = -1
        for idx, frame in enumerate(frames):
            if frame.id == last_event_id:
                found_index = idx
                break

        if found_index == -1:
            return frames

        return frames[found_index + 1 :]

    def all_frames(self) -> tuple[SseFrame, ...]:
        """Return all currently buffered frames in chronological order."""
        return tuple(self._buffer)


class SseStreamBroker:
    """Manages SSE subscriber channels, replay buffers, and message distribution."""

    def __init__(self, replay_capacity: int = 1024) -> None:
        self.replay_capacity = replay_capacity
        self._buffers: dict[str, SseReplayBuffer] = {}
        self._subscribers: dict[str, list[MemoryObjectSendStream[SseFrame]]] = {}
        self._owner_stream_counts: dict[str | None, int] = {}

    def subscribe(
        self,
        session_id: str,
        last_event_id: str | None = None,
    ) -> tuple[MemoryObjectReceiveStream[SseFrame], SseReplayBuffer]:
        """Create or resume an SSE stream subscription for a session.

        Args:
            session_id: Unique identifier for the client session or stream.
            last_event_id: Optional event ID for replaying missed frames.

        Returns:
            A tuple of (receive_stream, replay_buffer).
        """
        _, recv_stream, replay_buf = self._subscribe_raw(session_id, last_event_id=last_event_id)
        return recv_stream, replay_buf

    def open_stream(
        self,
        session_id: str,
        *,
        owner_id: str | None = None,
        last_event_id: str | None = None,
        heartbeat_seconds: float = 15.0,
        max_streams_per_owner: int = 8,
    ) -> AsyncIterator[bytes]:
        """Open a bounded, resumable SSE byte stream enforcing per-owner concurrency limits."""
        if heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        if max_streams_per_owner < 0:
            raise ValueError("max_streams_per_owner must be non-negative")
        active_count = self._owner_stream_counts.get(owner_id, 0)
        if active_count >= max_streams_per_owner:
            raise StreamLimitExceededError(
                f"Maximum concurrent SSE streams ({max_streams_per_owner}) exceeded for owner"
            )
        self._owner_stream_counts[owner_id] = active_count + 1
        send_stream, recv_stream, _ = self._subscribe_raw(session_id, last_event_id=last_event_id)
        return _ManagedSseStream(
            broker=self,
            session_id=session_id,
            owner_id=owner_id,
            send_stream=send_stream,
            recv_stream=recv_stream,
            heartbeat_seconds=heartbeat_seconds,
        )

    def publish(self, session_id: str, frame: SseFrame) -> bool:
        """Publish a frame to a specific session's replay buffer and active streams."""
        if session_id not in self._buffers:
            self._buffers[session_id] = SseReplayBuffer(capacity=self.replay_capacity)
        replay_buf = self._buffers[session_id]
        replay_buf.append(frame)

        streams = self._subscribers.get(session_id, [])
        alive_streams: list[MemoryObjectSendStream[SseFrame]] = []
        sent = False

        for send_stream in streams:
            try:
                send_stream.send_nowait(frame)
                alive_streams.append(send_stream)
                sent = True
            except anyio.ClosedResourceError:
                pass
            except anyio.WouldBlock:
                alive_streams.append(send_stream)

        if alive_streams:
            self._subscribers[session_id] = alive_streams
        else:
            self._subscribers.pop(session_id, None)
        return sent or bool(alive_streams)

    def broadcast(self, frame: SseFrame, session_ids: Sequence[str] | None = None) -> int:
        """Broadcast a frame to all active sessions or a filtered subset."""
        target_ids = session_ids if session_ids is not None else list(self._buffers.keys())
        sent_count = 0
        for sid in target_ids:
            if self.publish(sid, frame):
                sent_count += 1
        return sent_count

    def disconnect(self, session_id: str) -> None:
        """Close and remove active SSE subscriber streams for a session."""
        streams = self._subscribers.pop(session_id, [])
        for send_stream in streams:
            with contextlib.suppress(Exception):
                send_stream.close()

    def close_session(self, session_id: str) -> None:
        """Close active SSE streams and delete replay buffers for a session."""
        self.disconnect(session_id)
        self._buffers.pop(session_id, None)

    def active_stream_ids(self) -> tuple[str, ...]:
        """Return session IDs that currently have one or more active SSE subscribers."""
        return tuple(sid for sid, streams in self._subscribers.items() if streams)

    def owner_stream_count(self, owner_id: str | None = None) -> int:
        """Return the number of currently open SSE streams tracked for owner_id."""
        return self._owner_stream_counts.get(owner_id, 0)

    def _subscribe_raw(
        self,
        session_id: str,
        last_event_id: str | None = None,
    ) -> tuple[MemoryObjectSendStream[SseFrame], MemoryObjectReceiveStream[SseFrame], SseReplayBuffer]:
        if session_id not in self._buffers:
            self._buffers[session_id] = SseReplayBuffer(capacity=self.replay_capacity)
        replay_buf = self._buffers[session_id]

        send_stream, recv_stream = anyio.create_memory_object_stream[SseFrame](128)
        self._subscribers.setdefault(session_id, []).append(send_stream)

        if last_event_id:
            missed_frames = replay_buf.replay_since(last_event_id)
            for frame in missed_frames:
                try:
                    send_stream.send_nowait(frame)
                except anyio.WouldBlock:
                    break

        return send_stream, recv_stream, replay_buf

    def _release_open_stream(
        self,
        session_id: str,
        owner_id: str | None,
        send_stream: MemoryObjectSendStream[SseFrame],
        recv_stream: MemoryObjectReceiveStream[SseFrame],
    ) -> None:
        current = self._owner_stream_counts.get(owner_id, 0)
        if current <= 1:
            self._owner_stream_counts.pop(owner_id, None)
        else:
            self._owner_stream_counts[owner_id] = current - 1

        subscribers = self._subscribers.get(session_id)
        if subscribers is not None:
            remaining = [stream for stream in subscribers if stream is not send_stream]
            if remaining:
                self._subscribers[session_id] = remaining
            else:
                self._subscribers.pop(session_id, None)

        with contextlib.suppress(Exception):
            send_stream.close()
        with contextlib.suppress(Exception):
            recv_stream.close()

        if session_id.startswith("anon-") and not self._subscribers.get(session_id):
            self._buffers.pop(session_id, None)
            self._subscribers.pop(session_id, None)


class _ManagedSseStream(AsyncIterator[bytes]):
    """Async byte iterator wrapping a broker SSE subscription with heartbeat and cleanup."""

    def __init__(
        self,
        *,
        broker: SseStreamBroker,
        session_id: str,
        owner_id: str | None,
        send_stream: MemoryObjectSendStream[SseFrame],
        recv_stream: MemoryObjectReceiveStream[SseFrame],
        heartbeat_seconds: float,
    ) -> None:
        self._broker = broker
        self._session_id = session_id
        self._owner_id = owner_id
        self._send_stream = send_stream
        self._recv_stream = recv_stream
        self._heartbeat_seconds = heartbeat_seconds
        self._closed = False
        self._released = False

    def __aiter__(self) -> "_ManagedSseStream":
        return self

    async def __anext__(self) -> bytes:
        if self._closed:
            self._release()
            raise StopAsyncIteration
        try:
            while True:
                frame: SseFrame | None = None
                try:
                    with anyio.move_on_after(self._heartbeat_seconds):
                        frame = await self._recv_stream.receive()
                except anyio.EndOfStream as exc:
                    self._closed = True
                    self._release()
                    raise StopAsyncIteration from exc
                if self._closed:
                    self._release()
                    raise StopAsyncIteration
                if frame is None:
                    return b": keepalive\n\n"
                return frame.encode()
        except StopAsyncIteration:
            raise
        except BaseException:
            self._closed = True
            self._release()
            raise

    async def aclose(self) -> None:
        """Close the stream subscription and release owner quota immediately."""
        self._closed = True
        with contextlib.suppress(Exception):
            self._recv_stream.close()
        self._release()

    def _release(self) -> None:
        if self._released:
            return
        self._released = True
        self._broker._release_open_stream(
            self._session_id,
            self._owner_id,
            self._send_stream,
            self._recv_stream,
        )


class CacheSseStreamBroker(SseStreamBroker):
    """SSE stream broker persisting bounded replay frames in SyncCacheStore across workers."""

    def __init__(
        self,
        store: SyncCacheStore | None = None,
        *,
        cache_url: str = "memory://",
        key_prefix: str = "vtb:sse:",
        replay_capacity: int = 1024,
        ttl_seconds: float | None = 3600.0,
    ) -> None:
        super().__init__(replay_capacity=replay_capacity)
        self.key_prefix = key_prefix
        self.ttl_seconds = ttl_seconds
        self._owns_store = store is None
        self._store: SyncCacheStore = (
            store if store is not None else SyncCacheStore(cache_url, key_prefix=key_prefix, default_ttl=ttl_seconds)
        )

    def publish(self, session_id: str, frame: SseFrame) -> bool:
        """Publish a frame to local streams and persist it in the shared replay cache."""
        frames = self._load_frames(session_id)
        frames.append(frame)
        if len(frames) > self.replay_capacity:
            frames = frames[-self.replay_capacity :]
        self._save_frames(session_id, frames)
        self._register_session(session_id)
        return super().publish(session_id, frame)

    def broadcast(self, frame: SseFrame, session_ids: Sequence[str] | None = None) -> int:
        """Broadcast a frame to specified sessions or all locally and remotely known sessions."""
        if session_ids is None:
            merged = list(dict.fromkeys([*self._buffers.keys(), *self._load_sessions()]))
            return super().broadcast(frame, merged)
        return super().broadcast(frame, session_ids)

    def close_session(self, session_id: str) -> None:
        """Close active SSE streams and delete persisted replay state for a session."""
        super().close_session(session_id)
        self._store.delete(self._replay_key(session_id))
        sessions = self._load_sessions()
        if session_id in sessions:
            updated = [sid for sid in sessions if sid != session_id]
            self._save_sessions(updated)

    def close(self) -> None:
        """Release the backing SyncCacheStore if owned by this stream broker."""
        if self._owns_store:
            self._store.close()

    def _subscribe_raw(
        self,
        session_id: str,
        last_event_id: str | None = None,
    ) -> tuple[MemoryObjectSendStream[SseFrame], MemoryObjectReceiveStream[SseFrame], SseReplayBuffer]:
        cached_frames = self._load_frames(session_id)
        if cached_frames:
            buf = SseReplayBuffer(capacity=self.replay_capacity)
            for frame in cached_frames:
                buf.append(frame)
            self._buffers[session_id] = buf
        return super()._subscribe_raw(session_id, last_event_id=last_event_id)

    def _replay_key(self, session_id: str) -> str:
        return f"{self.key_prefix}replay:{session_id}"

    def _sessions_key(self) -> str:
        return f"{self.key_prefix}sessions"

    def _load_frames(self, session_id: str) -> list[SseFrame]:
        raw = self._store.get(self._replay_key(session_id))
        if raw is None:
            return []
        parsed = decode_json(raw)
        if not isinstance(parsed, list):
            return []
        return [msgspec.convert(item, SseFrame) for item in parsed]

    def _save_frames(self, session_id: str, frames: list[SseFrame]) -> None:
        self._store.set(
            self._replay_key(session_id),
            encode_json_bytes(frames),
            ttl=self.ttl_seconds,
        )

    def _load_sessions(self) -> list[str]:
        raw = self._store.get(self._sessions_key())
        if raw is None:
            return []
        parsed = decode_json(raw)
        if not isinstance(parsed, list):
            return []
        return [str(item) for item in parsed]

    def _save_sessions(self, session_ids: list[str]) -> None:
        if not session_ids:
            self._store.delete(self._sessions_key())
            return
        self._store.set(
            self._sessions_key(),
            encode_json_bytes(list(dict.fromkeys(session_ids))),
            ttl=self.ttl_seconds,
        )

    def _register_session(self, session_id: str) -> None:
        sessions = self._load_sessions()
        if session_id not in sessions:
            sessions.append(session_id)
            self._save_sessions(sessions)


__all__ = [
    "CacheSseStreamBroker",
    "SseFrame",
    "SseReplayBuffer",
    "SseStreamBroker",
    "StreamLimitExceededError",
]
