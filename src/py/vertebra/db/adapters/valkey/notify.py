"""Valkey Pub/Sub notification transport, active subscription streaming, and message builders."""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator, Iterator, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from vertebra.db.driver import (
    AsyncNotificationSource,
    InterfaceError,
    NotificationEvent,
    SyncNotificationSource,
)
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra._valkey import (
        AsyncValkeyClient,
        AsyncValkeyListener,
        SyncValkeyClient,
        SyncValkeyListener,
    )
    from vertebra.db.adapters.valkey.driver import AsyncValkeyDriver, SyncValkeyDriver
    from vertebra.db.cdc import ChangeFeed

__all__ = (
    "AsyncValkeyNotificationSource",
    "SyncValkeyNotificationSource",
    "ValkeyNotifyConfig",
)


class ValkeyNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Valkey Pub/Sub notification configuration."""

    channels: tuple[str, ...] = ()
    """Initial channel names to subscribe to."""

    patterns: tuple[str, ...] = ()
    """Initial pattern strings to subscribe to via PSUBSCRIBE."""

    shard_channels: tuple[str, ...] = ()
    """Initial cluster shard channel names to subscribe to via SSUBSCRIBE."""

    timeout_ms: int = 250
    """Read timeout in milliseconds for polling the underlying listener."""


class SyncValkeyNotificationSource(SyncNotificationSource):
    """Publish Valkey notifications and stream live SyncValkeyListener frames."""

    __slots__ = (
        "_change_feed",
        "_client",
        "_closed",
        "_config",
        "_driver",
        "_listener",
        "_subscribed_channels",
        "_subscribed_patterns",
        "_subscribed_shard_channels",
    )

    def __init__(
        self,
        driver: SyncValkeyDriver | None = None,
        config: ValkeyNotifyConfig | None = None,
        *,
        client: SyncValkeyClient | None = None,
        change_feed: ChangeFeed | None = None,
    ) -> None:
        """Initialize a synchronous Valkey notification source.

        Args:
            driver: Optional SyncValkeyDriver instance.
            config: Optional ValkeyNotifyConfig instance.
            client: Optional pre-constructed SyncValkeyClient.
            change_feed: Optional ChangeFeed for notification hydration.

        Raises:
            MissingDependencyError: When vertebra-core[valkey] is not installed.
            ImproperConfigurationError: When neither driver nor client is provided.
        """
        ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
        self._driver = driver
        self._config = config or ValkeyNotifyConfig()
        self._change_feed = change_feed
        self._listener: SyncValkeyListener | None = None
        self._closed = False

        if client is not None:
            self._client = client
        elif driver is not None:
            resolved_sync = getattr(driver, "_client", None)
            if resolved_sync is None:
                msg = "SyncValkeyDriver has no active native client"
                raise ImproperConfigurationError(msg)
            self._client = resolved_sync
        else:
            msg = "Either driver or client must be provided to SyncValkeyNotificationSource"
            raise ImproperConfigurationError(msg)

        self._subscribed_channels: set[str] = set(self._config.channels)
        self._subscribed_patterns: set[str] = set(self._config.patterns)
        self._subscribed_shard_channels: set[str] = set(self._config.shard_channels)

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional ChangeFeed attached to this source."""
        return self._change_feed

    @property
    def subscribed_channels(self) -> list[str]:
        """Return the currently subscribed standard channels."""
        return sorted(self._subscribed_channels)

    @property
    def subscribed_patterns(self) -> list[str]:
        """Return the currently subscribed pattern subscriptions."""
        return sorted(self._subscribed_patterns)

    @property
    def subscribed_shard_channels(self) -> list[str]:
        """Return the currently subscribed shard channels."""
        return sorted(self._subscribed_shard_channels)

    def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target Valkey channel."""
        if self._closed:
            msg = "Cannot publish on closed SyncValkeyNotificationSource"
            raise InterfaceError(msg)
        self._client.publish(channel, payload)

    def spublish(self, shard_channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target cluster shard channel."""
        if self._closed:
            msg = "Cannot spublish on closed SyncValkeyNotificationSource"
            raise InterfaceError(msg)
        self._client.execute_command("SPUBLISH", shard_channel, payload)

    def subscribe(self, channels: Sequence[str]) -> None:
        """Subscribe to one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_channels = [c for c in channels if c not in self._subscribed_channels]
        self._subscribed_channels.update(channels)
        if self._listener is not None and new_channels:
            self._listener.subscribe(new_channels)

    def psubscribe(self, patterns: Sequence[str]) -> None:
        """Subscribe to one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_patterns = [p for p in patterns if p not in self._subscribed_patterns]
        self._subscribed_patterns.update(patterns)
        if self._listener is not None and new_patterns:
            self._listener.psubscribe(new_patterns)

    def ssubscribe(self, shard_channels: Sequence[str]) -> None:
        """Subscribe to one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_shards = [s for s in shard_channels if s not in self._subscribed_shard_channels]
        self._subscribed_shard_channels.update(shard_channels)
        if self._listener is not None and new_shards:
            self._listener.ssubscribe(new_shards)

    def unsubscribe(self, channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_channels = list(channels) if channels else list(self._subscribed_channels)
        for c in target_channels:
            self._subscribed_channels.discard(c)
        if self._listener is not None:
            self._listener.unsubscribe(target_channels)

    def punsubscribe(self, patterns: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_patterns = list(patterns) if patterns else list(self._subscribed_patterns)
        for p in target_patterns:
            self._subscribed_patterns.discard(p)
        if self._listener is not None:
            self._listener.punsubscribe(target_patterns)

    def sunsubscribe(self, shard_channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_shards = list(shard_channels) if shard_channels else list(self._subscribed_shard_channels)
        for s in target_shards:
            self._subscribed_shard_channels.discard(s)
        if self._listener is not None:
            self._listener.sunsubscribe(target_shards)

    def _hydrate_payload(self, raw_payload: bytes) -> bytes:
        """Decode UTF-8 notification text and hydrate ChangeFeed or b64-prefixed binary payloads."""
        if self._change_feed is not None:
            try:
                text = raw_payload.decode("utf-8")
                evt = self._change_feed.hydrate_notification(text)
                if isinstance(evt.payload, dict) and "data_b64" in evt.payload:
                    return base64.b64decode(evt.payload["data_b64"])
            except Exception:
                pass
        try:
            text = raw_payload.decode("utf-8")
            if text.startswith("b64:"):
                return base64.b64decode(text[4:])
        except Exception:
            pass
        return bytes(raw_payload)

    def listen(self) -> Iterator[NotificationEvent]:
        """Yield incoming NotificationEvent frames across subscribed channels."""
        if self._closed:
            msg = "Cannot listen on closed SyncValkeyNotificationSource"
            raise InterfaceError(msg)

        listener = self._client.listener()
        self._listener = listener
        try:
            if self._subscribed_channels:
                listener.subscribe(list(self._subscribed_channels))
            if self._subscribed_patterns:
                listener.psubscribe(list(self._subscribed_patterns))
            if self._subscribed_shard_channels:
                listener.ssubscribe(list(self._subscribed_shard_channels))

            while not self._closed and not listener.closed:
                item = listener.next_message(timeout_ms=self._config.timeout_ms)
                if item is None:
                    continue
                kind = str(item.get("type", "message"))
                ch = str(item.get("channel", ""))
                raw_payload = item.get("data", b"")
                payload = raw_payload if isinstance(raw_payload, bytes) else str(raw_payload).encode("utf-8")
                pattern = item.get("pattern")
                pattern_str = str(pattern) if pattern is not None else None
                hydrated = self._hydrate_payload(payload)
                metadata: dict[str, Any] = {
                    "kind": kind,
                    "pattern": pattern_str,
                    "shard": (kind == "smessage"),
                }
                yield NotificationEvent(
                    channel=ch,
                    payload=hydrated,
                    sender_id=None,
                    metadata=metadata,
                )
        finally:
            listener.close()
            if self._listener is listener:
                self._listener = None

    def close(self) -> None:
        """Close this notification source and release active listener handles."""
        if self._closed:
            return
        self._closed = True
        if self._listener is not None:
            self._listener.close()
            self._listener = None

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit context manager, closing active subscriptions."""
        self.close()


class AsyncValkeyNotificationSource(AsyncNotificationSource):
    """Publish Valkey notifications and stream live AsyncValkeyListener frames."""

    __slots__ = (
        "_change_feed",
        "_client",
        "_closed",
        "_config",
        "_driver",
        "_listener",
        "_subscribed_channels",
        "_subscribed_patterns",
        "_subscribed_shard_channels",
    )

    def __init__(
        self,
        driver: AsyncValkeyDriver | None = None,
        config: ValkeyNotifyConfig | None = None,
        *,
        client: AsyncValkeyClient | None = None,
        change_feed: ChangeFeed | None = None,
    ) -> None:
        """Initialize an asynchronous Valkey notification source.

        Args:
            driver: Optional AsyncValkeyDriver instance.
            config: Optional ValkeyNotifyConfig instance.
            client: Optional pre-constructed AsyncValkeyClient.
            change_feed: Optional ChangeFeed for notification hydration.

        Raises:
            MissingDependencyError: When vertebra-core[valkey] is not installed.
            ImproperConfigurationError: When neither driver nor client is provided.
        """
        ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")
        self._driver = driver
        self._config = config or ValkeyNotifyConfig()
        self._change_feed = change_feed
        self._listener: AsyncValkeyListener | None = None
        self._closed = False

        if client is not None:
            self._client = client
        elif driver is not None:
            resolved_async = getattr(driver, "_client", None)
            if resolved_async is None:
                msg = "AsyncValkeyDriver has no active native client"
                raise ImproperConfigurationError(msg)
            self._client = resolved_async
        else:
            msg = "Either driver or client must be provided to AsyncValkeyNotificationSource"
            raise ImproperConfigurationError(msg)

        self._subscribed_channels: set[str] = set(self._config.channels)
        self._subscribed_patterns: set[str] = set(self._config.patterns)
        self._subscribed_shard_channels: set[str] = set(self._config.shard_channels)

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional ChangeFeed attached to this source."""
        return self._change_feed

    @property
    def subscribed_channels(self) -> list[str]:
        """Return the currently subscribed standard channels."""
        return sorted(self._subscribed_channels)

    @property
    def subscribed_patterns(self) -> list[str]:
        """Return the currently subscribed pattern subscriptions."""
        return sorted(self._subscribed_patterns)

    @property
    def subscribed_shard_channels(self) -> list[str]:
        """Return the currently subscribed shard channels."""
        return sorted(self._subscribed_shard_channels)

    async def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target Valkey channel."""
        if self._closed:
            msg = "Cannot publish on closed AsyncValkeyNotificationSource"
            raise InterfaceError(msg)
        await self._client.publish(channel, payload)

    async def spublish(self, shard_channel: str, payload: bytes | str) -> None:
        """Publish a payload to the target cluster shard channel."""
        if self._closed:
            msg = "Cannot spublish on closed AsyncValkeyNotificationSource"
            raise InterfaceError(msg)
        await self._client.execute_command("SPUBLISH", shard_channel, payload)

    async def subscribe(self, channels: Sequence[str]) -> None:
        """Subscribe to one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_channels = [c for c in channels if c not in self._subscribed_channels]
        self._subscribed_channels.update(channels)
        if self._listener is not None and new_channels:
            await self._listener.subscribe(new_channels)

    async def psubscribe(self, patterns: Sequence[str]) -> None:
        """Subscribe to one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_patterns = [p for p in patterns if p not in self._subscribed_patterns]
        self._subscribed_patterns.update(patterns)
        if self._listener is not None and new_patterns:
            await self._listener.psubscribe(new_patterns)

    async def ssubscribe(self, shard_channels: Sequence[str]) -> None:
        """Subscribe to one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        new_shards = [s for s in shard_channels if s not in self._subscribed_shard_channels]
        self._subscribed_shard_channels.update(shard_channels)
        if self._listener is not None and new_shards:
            await self._listener.ssubscribe(new_shards)

    async def unsubscribe(self, channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more notification channels."""
        if isinstance(channels, (str, bytes)):
            msg = "channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_channels = list(channels) if channels else list(self._subscribed_channels)
        for c in target_channels:
            self._subscribed_channels.discard(c)
        if self._listener is not None:
            await self._listener.unsubscribe(target_channels)

    async def punsubscribe(self, patterns: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more channel patterns."""
        if isinstance(patterns, (str, bytes)):
            msg = "patterns must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_patterns = list(patterns) if patterns else list(self._subscribed_patterns)
        for p in target_patterns:
            self._subscribed_patterns.discard(p)
        if self._listener is not None:
            await self._listener.punsubscribe(target_patterns)

    async def sunsubscribe(self, shard_channels: Sequence[str] = ()) -> None:
        """Unsubscribe from one or more cluster shard channels."""
        if isinstance(shard_channels, (str, bytes)):
            msg = "shard_channels must be a sequence of strings, not a single string"
            raise TypeError(msg)
        target_shards = list(shard_channels) if shard_channels else list(self._subscribed_shard_channels)
        for s in target_shards:
            self._subscribed_shard_channels.discard(s)
        if self._listener is not None:
            await self._listener.sunsubscribe(target_shards)

    def _hydrate_payload(self, raw_payload: bytes) -> bytes:
        """Decode UTF-8 notification text and hydrate ChangeFeed or b64-prefixed binary payloads."""
        if self._change_feed is not None:
            try:
                text = raw_payload.decode("utf-8")
                evt = self._change_feed.hydrate_notification(text)
                if isinstance(evt.payload, dict) and "data_b64" in evt.payload:
                    return base64.b64decode(evt.payload["data_b64"])
            except Exception:
                pass
        try:
            text = raw_payload.decode("utf-8")
            if text.startswith("b64:"):
                return base64.b64decode(text[4:])
        except Exception:
            pass
        return bytes(raw_payload)

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        """Yield incoming NotificationEvent frames asynchronously across subscribed channels."""
        if self._closed:
            msg = "Cannot listen on closed AsyncValkeyNotificationSource"
            raise InterfaceError(msg)

        listener = self._client.listener()
        self._listener = listener
        try:
            if self._subscribed_channels:
                await listener.subscribe(list(self._subscribed_channels))
            if self._subscribed_patterns:
                await listener.psubscribe(list(self._subscribed_patterns))
            if self._subscribed_shard_channels:
                await listener.ssubscribe(list(self._subscribed_shard_channels))

            while not self._closed and not listener.closed:
                item = await listener.next_message(timeout_ms=self._config.timeout_ms)
                if item is None:
                    continue
                kind = str(item.get("type", "message"))
                ch = str(item.get("channel", ""))
                raw_payload = item.get("data", b"")
                payload = raw_payload if isinstance(raw_payload, bytes) else str(raw_payload).encode("utf-8")
                pattern = item.get("pattern")
                pattern_str = str(pattern) if pattern is not None else None
                hydrated = self._hydrate_payload(payload)
                metadata: dict[str, Any] = {
                    "kind": kind,
                    "pattern": pattern_str,
                    "shard": (kind == "smessage"),
                }
                yield NotificationEvent(
                    channel=ch,
                    payload=hydrated,
                    sender_id=None,
                    metadata=metadata,
                )
        finally:
            await listener.close()
            if self._listener is listener:
                self._listener = None

    async def aclose(self) -> None:
        """Close this notification source asynchronously and release active listener handles."""
        if self._closed:
            return
        self._closed = True
        if self._listener is not None:
            await self._listener.close()
            self._listener = None

    async def __aenter__(self) -> Self:
        """Enter asynchronous context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit asynchronous context manager, closing active subscriptions."""
        await self.aclose()
