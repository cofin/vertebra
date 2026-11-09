"""Oracle 26ai TxEventQ and DBMS_AQ notification transport with ChangeFeed and reconnect backoff."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from vertebra.db.cdc import ChangeEvent, ChangeFeed
from vertebra.db.driver import (
    AsyncDatabaseDriver,
    InterfaceError,
    NotificationEvent,
    NotificationSource,
    OperationalError,
    ProgrammingError,
)
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from vertebra.db.adapters.oracle.driver import AsyncOracleDriver, SyncOracleDriver

__all__ = (
    "OracleNotificationSource",
    "OracleNotifyConfig",
    "validate_oracle_channel_name",
)

_TRANSIENT_ORA_CODES: frozenset[int] = frozenset({3113, 3114, 3135, 4061, 4068, 12170, 12514, 12537, 12541, 12545})


def _is_valid_aq_identifier(value: str) -> bool:
    if not value or len(value) > 128:
        return False
    first = value[0]
    if not (first.isascii() and (first.isalpha() or first == "_")):
        return False
    return all(ch.isascii() and (ch.isalnum() or ch in ("_", "$", "#")) for ch in value[1:])


def validate_oracle_channel_name(channel: str, *, queue_prefix: str = "VTB_AQ_") -> str:
    """Validate a logical channel or queue name and return its uppercase Oracle AQ queue identifier.

    Args:
        channel: Logical notification channel or AQ queue name.
        queue_prefix: Prefix prepended when ``channel`` does not already start with the prefix.

    Returns:
        Normalized uppercase Oracle AQ identifier matching ``^[A-Za-z_][A-Za-z0-9_$#]{0,127}$``.

    Raises:
        ProgrammingError: With ``sqlstate="42602"`` when the channel or queue prefix is invalid.
    """
    cleaned = channel.strip() if isinstance(channel, str) else ""
    prefix_cleaned = queue_prefix.strip() if isinstance(queue_prefix, str) else ""
    if not _is_valid_aq_identifier(cleaned):
        err = ProgrammingError(
            f"Invalid Oracle AQ channel/queue identifier {channel!r}; expected ^[A-Za-z_][A-Za-z0-9_$#]{{0,127}}$"
        )
        err.sqlstate = "42602"
        raise err
    if prefix_cleaned and not _is_valid_aq_identifier(prefix_cleaned):
        err = ProgrammingError(
            f"Invalid Oracle AQ queue_prefix {queue_prefix!r}; expected ^[A-Za-z_][A-Za-z0-9_$#]{{0,127}}$"
        )
        err.sqlstate = "42602"
        raise err

    upper_channel = cleaned.upper()
    upper_prefix = prefix_cleaned.upper()
    resolved = (
        upper_channel
        if not upper_prefix or upper_channel.startswith(upper_prefix)
        else f"{upper_prefix}{upper_channel}"
    )
    if not _is_valid_aq_identifier(resolved):
        err = ProgrammingError(
            f"Invalid Oracle AQ resolved queue identifier {resolved!r}; exceeds 128-byte identifier limit"
        )
        err.sqlstate = "42602"
        raise err
    return resolved


class OracleNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Configuration for Oracle 26ai TxEventQ and DBMS_AQ notification streaming."""

    queue_prefix: str = "VTB_AQ_"
    """Prefix prepended to logical channel names to form Oracle AQ queue identifiers."""

    consumer_name: str | None = None
    """Optional multi-consumer subscriber name passed to ``DBMS_AQ.DEQUEUE``."""

    wait_timeout_seconds: int = 5
    """Per-iteration ``DBMS_AQ.DEQUEUE`` wait timeout in seconds on the dedicated worker thread."""

    reconnect_initial_delay: float = 0.25
    """Initial exponential backoff delay in seconds after a transient listener disconnect."""

    reconnect_max_delay: float = 15.0
    """Maximum exponential backoff delay in seconds between listener reconnect attempts."""

    max_reconnect_attempts: int = 5
    """Maximum consecutive transient reconnect attempts before surfacing the error."""

    poll_timeout: float = 1.0
    """Per-queue poll slice timeout in seconds when multiplexing multiple subscribed queues."""

    def __post_init__(self) -> None:
        if self.queue_prefix:
            validate_oracle_channel_name(" probe ", queue_prefix=self.queue_prefix)
        if self.consumer_name is not None and not _is_valid_aq_identifier(self.consumer_name.strip()):
            err = ProgrammingError(
                f"Invalid Oracle AQ consumer_name {self.consumer_name!r}; expected ^[A-Za-z_][A-Za-z0-9_$#]{{0,127}}$"
            )
            err.sqlstate = "42602"
            raise err
        if self.wait_timeout_seconds < 1:
            raise ValueError("wait_timeout_seconds must be >= 1")
        if self.reconnect_initial_delay < 0:
            raise ValueError("reconnect_initial_delay must be >= 0")
        if self.reconnect_max_delay < self.reconnect_initial_delay:
            raise ValueError("reconnect_max_delay must be >= reconnect_initial_delay")
        if self.max_reconnect_attempts < 0:
            raise ValueError("max_reconnect_attempts must be >= 0")
        if self.poll_timeout <= 0:
            raise ValueError("poll_timeout must be > 0")


class OracleNotificationSource(NotificationSource):
    """Oracle 26ai TxEventQ / DBMS_AQ notification publisher and streaming listener."""

    __slots__ = (
        "_active_listeners",
        "_change_feed",
        "_closed",
        "_config",
        "_driver",
        "_subscriptions",
    )

    def __init__(
        self,
        driver: AsyncOracleDriver | SyncOracleDriver,
        config: OracleNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
    ) -> None:
        self._driver = driver
        self._config = config or OracleNotifyConfig()
        self._change_feed = change_feed
        self._subscriptions: dict[str, tuple[str, str | None]] = {}
        self._active_listeners: dict[str, Any] = {}
        self._closed: bool = False

    @property
    def config(self) -> OracleNotifyConfig:
        """Return the ``OracleNotifyConfig`` governing this notification source."""
        return self._config

    @property
    def change_feed(self) -> ChangeFeed | None:
        """Return the optional ``ChangeFeed`` used to wrap and hydrate notification envelopes."""
        return self._change_feed

    async def publish(self, channel: str, payload: bytes | str) -> None:
        """Publish a payload or ``ChangeFeed`` envelope to ``channel`` via ``DBMS_AQ.ENQUEUE``."""
        queue_name = validate_oracle_channel_name(channel, queue_prefix=self._config.queue_prefix)
        cleaned_channel = channel.strip()
        raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        if self._change_feed is not None:
            wire_json, _ = self._change_feed.publish_event(
                "PUBLISH",
                {
                    "channel": cleaned_channel,
                    "queue_name": queue_name,
                    "data_b64": base64.b64encode(raw_bytes).decode("ascii"),
                },
            )
            wire_bytes = wire_json.encode("utf-8")
        else:
            wire_bytes = raw_bytes
        await self._enqueue_raw(
            queue_name=queue_name,
            payload=wire_bytes,
            consumer_name=self._config.consumer_name,
            correlation_id=cleaned_channel,
        )

    async def subscribe(self, channels: Sequence[str]) -> None:
        """Register one or more logical notification channels for subsequent ``listen()`` streams."""
        for channel in channels:
            queue_name = validate_oracle_channel_name(channel, queue_prefix=self._config.queue_prefix)
            self._subscriptions[queue_name] = (channel.strip(), self._config.consumer_name)
        self._closed = False

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        """Unsubscribe from one or more logical channels and close any active listener sessions."""
        for channel in channels:
            queue_name = validate_oracle_channel_name(channel, queue_prefix=self._config.queue_prefix)
            self._subscriptions.pop(queue_name, None)
            listener = self._active_listeners.pop(queue_name, None)
            if listener is not None:
                with suppress(Exception):
                    await listener.close()

    async def subscribe_aq(self, queue_name: str, *, consumer_name: str | None = None) -> None:
        """Subscribe directly to an Oracle AQ / TxEventQ queue with an optional consumer name."""
        resolved_queue = validate_oracle_channel_name(queue_name, queue_prefix=self._config.queue_prefix)
        resolved_consumer = consumer_name if consumer_name is not None else self._config.consumer_name
        if resolved_consumer is not None and not _is_valid_aq_identifier(resolved_consumer.strip()):
            err = ProgrammingError(
                f"Invalid Oracle AQ consumer_name {resolved_consumer!r}; expected ^[A-Za-z_][A-Za-z0-9_$#]{{0,127}}$"
            )
            err.sqlstate = "42602"
            raise err
        self._subscriptions[resolved_queue] = (
            queue_name.strip(),
            resolved_consumer.strip() if resolved_consumer is not None else None,
        )
        self._closed = False

    async def publish_aq(
        self,
        queue_name: str,
        payload: bytes | str,
        *,
        correlation_id: str | None = None,
    ) -> str:
        """Enqueue a raw payload directly to an Oracle AQ / TxEventQ queue and return its hex/ASCII message ID."""
        resolved_queue = validate_oracle_channel_name(queue_name, queue_prefix=self._config.queue_prefix)
        raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        msg_id_bytes = await self._enqueue_raw(
            queue_name=resolved_queue,
            payload=raw_bytes,
            consumer_name=self._config.consumer_name,
            correlation_id=correlation_id,
        )
        return msg_id_bytes.decode("ascii", errors="replace")

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        """Yield ``NotificationEvent`` items across all subscribed channels with exponential reconnect backoff."""
        if not self._subscriptions:
            err = ProgrammingError("At least one notification channel must be subscribed before calling listen()")
            err.sqlstate = "42602"
            raise err

        self._closed = False
        attempt = 0
        try:
            while not self._closed and self._subscriptions:
                snapshot = list(self._subscriptions.items())
                for queue_name, (logical_channel, consumer_name) in snapshot:
                    if self._closed or queue_name not in self._subscriptions:
                        continue
                    try:
                        listener = await self._get_or_open_listener(queue_name, consumer_name)
                        raw_frame = await listener.next_notification(timeout=self._config.poll_timeout)
                        attempt = 0
                    except asyncio.CancelledError:
                        raise
                    except (OperationalError, InterfaceError) as exc:
                        await self._discard_listener(queue_name)
                        if self._closed or not self._is_transient_disconnect(exc):
                            raise
                        attempt += 1
                        if attempt > self._config.max_reconnect_attempts:
                            raise
                        delay = min(
                            self._config.reconnect_max_delay,
                            self._config.reconnect_initial_delay * (2 ** (attempt - 1)),
                        )
                        await asyncio.sleep(delay)
                        continue

                    if raw_frame is None:
                        continue
                    yield self._frame_to_event(logical_channel, queue_name, raw_frame)
        finally:
            await self._close_all_listeners()

    async def listen_aq(
        self,
        queue_name: str | None = None,
        *,
        consumer_name: str | None = None,
    ) -> AsyncIterator[NotificationEvent]:
        """Subscribe (when ``queue_name`` is provided) and stream ``NotificationEvent`` items from AQ / TxEventQ."""
        if queue_name is not None:
            await self.subscribe_aq(queue_name, consumer_name=consumer_name)
        async for event in self.listen():
            yield event

    async def close(self) -> None:
        """Stop active ``listen()`` loops and close all open ``AsyncOracleListener`` sessions immediately."""
        self._closed = True
        await self._close_all_listeners()

    async def _enqueue_raw(
        self,
        *,
        queue_name: str,
        payload: bytes,
        consumer_name: str | None,
        correlation_id: str | None,
    ) -> bytes:
        if isinstance(self._driver, AsyncDatabaseDriver):
            client = getattr(self._driver, "client", None)
            if client is not None and hasattr(client, "enqueue_notification"):
                res = await client.enqueue_notification(
                    queue_name=queue_name,
                    payload=payload,
                    consumer_name=consumer_name,
                    correlation_id=correlation_id,
                )
                return bytes(res)
            msg_id_str = await self._driver.publish_notification(
                queue_name,
                payload,
                queue_prefix=self._config.queue_prefix,
                consumer_name=consumer_name,
                correlation_id=correlation_id,
            )
            return msg_id_str.encode("ascii")

        sync_client = getattr(self._driver, "client", None)
        if sync_client is not None and hasattr(sync_client, "enqueue_notification"):
            res = await asyncio.to_thread(
                sync_client.enqueue_notification,
                queue_name=queue_name,
                payload=payload,
                consumer_name=consumer_name,
                correlation_id=correlation_id,
            )
            return bytes(res)
        msg_id_str = await asyncio.to_thread(
            self._driver.publish_notification,
            queue_name,
            payload,
            queue_prefix=self._config.queue_prefix,
            consumer_name=consumer_name,
            correlation_id=correlation_id,
        )
        return msg_id_str.encode("ascii")

    async def _get_or_open_listener(self, queue_name: str, consumer_name: str | None) -> Any:
        existing = self._active_listeners.get(queue_name)
        if existing is not None and not bool(getattr(existing, "is_closed", False)):
            return existing
        client = getattr(self._driver, "client", None)
        if client is None or not hasattr(client, "open_listener"):
            raise InterfaceError("Oracle driver client does not support open_listener()")
        listener = await client.open_listener(
            queue_name=queue_name,
            consumer_name=consumer_name,
            wait_timeout_seconds=self._config.wait_timeout_seconds,
        )
        self._active_listeners[queue_name] = listener
        return listener

    async def _discard_listener(self, queue_name: str) -> None:
        listener = self._active_listeners.pop(queue_name, None)
        if listener is not None:
            with suppress(Exception):
                await listener.close()

    async def _close_all_listeners(self) -> None:
        listeners = list(self._active_listeners.values())
        self._active_listeners.clear()
        for listener in listeners:
            with suppress(Exception):
                await listener.close()

    @staticmethod
    def _is_transient_disconnect(exc: BaseException) -> bool:
        if type(exc).__name__ == "ConnectionFailure" and getattr(exc, "vendor_code", None) in (1017, 28000, 12154):
            return False
        if bool(getattr(exc, "is_transient", False)):
            return True
        vendor_code = getattr(exc, "vendor_code", None)
        return isinstance(vendor_code, int) and vendor_code in _TRANSIENT_ORA_CODES

    def _frame_to_event(
        self,
        logical_channel: str,
        queue_name: str,
        raw_frame: Any,
    ) -> NotificationEvent:
        raw_payload = bytes(raw_frame.payload)
        raw_msg_id = bytes(raw_frame.msg_id)
        msg_id_str = raw_msg_id.decode("ascii", errors="replace")
        metadata: dict[str, Any] = {
            "queue_name": str(getattr(raw_frame, "queue_name", queue_name)),
            "consumer_name": getattr(raw_frame, "consumer_name", None),
            "msg_id": msg_id_str,
            "enqueued_at_epoch_ms": getattr(raw_frame, "enqueued_at_epoch_ms", None),
        }
        if self._change_feed is not None:
            try:
                evt: ChangeEvent = self._change_feed.hydrate_notification(raw_payload)
                metadata["change_event_id"] = evt.event_id
                metadata["spilled"] = evt.spilled
                data_b64 = evt.payload.get("data_b64")
                if isinstance(data_b64, str):
                    raw_payload = base64.b64decode(data_b64.encode("ascii"))
                else:
                    raw_payload = encode_json(evt.payload).encode("utf-8")
            except (KeyError, TypeError, ValueError):
                decoded_any = decode_json(raw_payload)
                if isinstance(decoded_any, dict) and isinstance(decoded_any.get("data_b64"), str):
                    raw_payload = base64.b64decode(decoded_any["data_b64"].encode("ascii"))
        return NotificationEvent(
            channel=logical_channel,
            payload=raw_payload,
            sender_id=msg_id_str,
            metadata=metadata,
        )
