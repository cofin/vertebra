"""Server-Sent Events (SSE) frame codec, bounded replay buffer, and stream broker.

This module provides SSE frame formatting compliant with the W3C EventSource
specification, bounded replay buffers supporting Last-Event-ID reconnection,
and multi-subscriber stream brokers for streaming agentic responses.
"""

import contextlib
from collections import deque
from collections.abc import Sequence

import anyio
import msgspec
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream


class SseFrame(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
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

        return recv_stream, replay_buf

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

        self._subscribers[session_id] = alive_streams
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
        """Close and remove the SSE subscriptions for a session."""
        streams = self._subscribers.pop(session_id, [])
        for send_stream in streams:
            with contextlib.suppress(Exception):
                send_stream.close()


__all__ = [
    "SseFrame",
    "SseReplayBuffer",
    "SseStreamBroker",
]
