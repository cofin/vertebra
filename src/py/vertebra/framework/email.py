"""Transactional email subdomain supporting memory and SMTP backends."""

from __future__ import annotations

import email.message
import smtplib
import uuid
from typing import Literal

import msgspec

from vertebra._typing import UnsupportedBackendError
from vertebra.utils.sync_tools import SyncPool

__all__ = (
    "EmailAttachment",
    "EmailBackendType",
    "EmailConfig",
    "EmailMessage",
    "EmailService",
)

type EmailBackendType = Literal["memory", "smtp", "resend", "sendgrid", "mailgun", "ses"]


class EmailAttachment(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Binary or text email attachment."""

    filename: str
    content: bytes
    content_type: str = "application/octet-stream"


class EmailMessage(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Outbound transactional email message."""

    subject: str
    to: tuple[str, ...]
    from_email: str
    text_body: str = ""
    html_body: str | None = None
    cc: tuple[str, ...] = ()
    bcc: tuple[str, ...] = ()
    reply_to: str | None = None
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    attachments: tuple[EmailAttachment, ...] = ()


class EmailConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for EmailService."""

    backend: EmailBackendType = "memory"
    default_from: str = "noreply@example.com"
    smtp_host: str = "localhost"
    smtp_port: int = 587
    api_key: str | None = None


class EmailService:
    """Transactional email dispatcher with in-memory test capture and SMTP delivery."""

    __slots__ = ("_config", "_outbox")

    def __init__(self, config: EmailConfig | None = None) -> None:
        """Initialize email service and validate that the configured backend is supported."""
        self._config = config or EmailConfig()
        if self._config.backend not in ("memory", "smtp"):
            msg = f"Email backend {self._config.backend!r} is not supported; use 'memory' or 'smtp'"
            raise UnsupportedBackendError(msg)
        self._outbox: list[EmailMessage] = []

    @property
    def config(self) -> EmailConfig:
        """Return the EmailConfig."""
        return self._config

    @property
    def outbox(self) -> tuple[EmailMessage, ...]:
        """Return captured EmailMessage instances."""
        return tuple(self._outbox)

    async def send(self, message: EmailMessage) -> str:
        """Send an EmailMessage and return its delivery receipt ID."""
        if self._config.backend == "memory":
            self._outbox.append(message)
            return f"msg-{len(self._outbox)}"

        return await SyncPool.default().submit_async(self._send_via_smtp, message)

    def _send_via_smtp(self, message: EmailMessage) -> str:
        msg = email.message.EmailMessage()
        msg["Subject"] = message.subject
        msg["From"] = message.from_email
        msg["To"] = ", ".join(message.to)
        if message.cc:
            msg["Cc"] = ", ".join(message.cc)
        if message.bcc:
            msg["Bcc"] = ", ".join(message.bcc)
        if message.reply_to:
            msg["Reply-To"] = message.reply_to
        for header_key, header_val in message.headers.items():
            msg[header_key] = header_val

        msg.set_content(message.text_body)
        if message.html_body:
            msg.add_alternative(message.html_body, subtype="html")

        for att in message.attachments:
            maintype, _, subtype = att.content_type.partition("/")
            msg.add_attachment(
                att.content,
                maintype=maintype or "application",
                subtype=subtype or "octet-stream",
                filename=att.filename,
            )

        with smtplib.SMTP(self._config.smtp_host, self._config.smtp_port) as client:
            client.send_message(msg)
        return f"smtp-{uuid.uuid4().hex}"

    def clear_outbox(self) -> None:
        """Clear the in-memory outbox."""
        self._outbox.clear()
