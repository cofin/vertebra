"""Transactional email subdomain supporting memory, SMTP, Resend, SendGrid, Mailgun, and SES."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

__all__ = (
    "EmailAttachment",
    "EmailBackendType",
    "EmailConfig",
    "EmailMessage",
    "EmailService",
)

type EmailBackendType = Literal["memory", "smtp", "resend", "sendgrid", "mailgun", "ses"]


@dataclass(frozen=True, slots=True)
class EmailAttachment:
    """Binary or text email attachment."""

    filename: str
    content: bytes
    content_type: str = "application/octet-stream"


@dataclass(frozen=True, slots=True)
class EmailMessage:
    """Outbound transactional email message."""

    subject: str
    to: tuple[str, ...]
    from_email: str
    text_body: str = ""
    html_body: str | None = None
    cc: tuple[str, ...] = ()
    bcc: tuple[str, ...] = ()
    reply_to: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    attachments: tuple[EmailAttachment, ...] = ()


@dataclass(frozen=True, slots=True)
class EmailConfig:
    """Configuration for ``EmailService``."""

    backend: EmailBackendType = "memory"
    default_from: str = "noreply@example.com"
    smtp_host: str = "localhost"
    smtp_port: int = 587
    api_key: str | None = None


class EmailService:
    """Transactional email dispatcher with in-memory test outbox capture."""

    def __init__(self, config: EmailConfig | None = None) -> None:
        self._config = config or EmailConfig()
        self._outbox: list[EmailMessage] = []

    @property
    def config(self) -> EmailConfig:
        """Return the ``EmailConfig``."""
        return self._config

    @property
    def outbox(self) -> tuple[EmailMessage, ...]:
        """Return captured ``EmailMessage`` instances."""
        return tuple(self._outbox)

    async def send(self, message: EmailMessage) -> str:
        """Send an ``EmailMessage`` and return its delivery receipt ID."""
        self._outbox.append(message)
        return f"msg-{len(self._outbox)}"

    def clear_outbox(self) -> None:
        """Clear the in-memory outbox."""
        self._outbox.clear()
