"""In-memory email transport for testing and local development."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.framework.email.base import EmailReceipt

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.framework.email.base import EmailConfig, EmailMessage

__all__ = ("MemoryEmailTransport",)


class MemoryEmailTransport:
    """In-memory mock transport capturing sent messages with idempotency support."""

    __slots__ = (
        "_by_idempotency_key",
        "_config",
        "_messages",
        "_receipts",
        "_seq",
    )

    def __init__(self, config: EmailConfig | None = None) -> None:
        """Initialize in-memory transport."""
        self._config = config
        self._messages: list[EmailMessage] = []
        self._receipts: list[EmailReceipt] = []
        self._by_idempotency_key: dict[str, EmailReceipt] = {}
        self._seq = 0

    @property
    def outbox(self) -> tuple[EmailMessage, ...]:
        """Return captured messages in order of sending."""
        return tuple(self._messages)

    @property
    def receipts(self) -> tuple[EmailReceipt, ...]:
        """Return captured receipts in order of generation."""
        return tuple(self._receipts)

    def find_by_recipient(self, email: str) -> list[EmailMessage]:
        """Filter captured messages where the recipient appears in to, cc, or bcc."""
        target = email.strip().lower()
        matched: list[EmailMessage] = []
        for msg in self._messages:
            recipients = {r.strip().lower() for r in (*msg.to, *msg.cc, *msg.bcc)}
            if target in recipients:
                matched.append(msg)
        return matched

    def clear(self) -> None:
        """Reset captured outbox, receipts, and idempotency cache."""
        self._messages.clear()
        self._receipts.clear()
        self._by_idempotency_key.clear()
        self._seq = 0

    async def send(self, message: EmailMessage) -> EmailReceipt:
        """Send message by appending to memory outbox or returning cached receipt."""
        if message.idempotency_key and message.idempotency_key in self._by_idempotency_key:
            return self._by_idempotency_key[message.idempotency_key]

        self._seq += 1
        msg_id = f"msg-{self._seq}"
        receipt = EmailReceipt(
            message_id=msg_id,
            provider="memory",
            accepted_recipients=tuple(message.to),
            raw_response={"seq": self._seq, "idempotency_key": message.idempotency_key},
        )
        self._messages.append(message)
        self._receipts.append(receipt)
        if message.idempotency_key:
            self._by_idempotency_key[message.idempotency_key] = receipt
        return receipt

    async def send_batch(self, messages: Sequence[EmailMessage]) -> list[EmailReceipt]:
        """Send a sequence of messages sequentially."""
        results: list[EmailReceipt] = []
        for msg in messages:
            receipt = await self.send(msg)
            results.append(receipt)
        return results

    async def close(self) -> None:
        """No-op close for in-memory transport."""
